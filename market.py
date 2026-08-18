"""Polymarket discovery, public CLOB books, and depth-aware execution math."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import httpx

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:  # pragma: no cover
    websockets = None
    ConnectionClosed = Exception


UTC = timezone.utc


def as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return as_utc(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return as_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def optional_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


@dataclass(frozen=True)
class NormalizedMarketEvent:
    """Canonical representation of a raw or enveloped market event."""

    event_type: str
    asset_id: str | None
    timestamp: Any
    market: str | None
    raw_data: dict[str, Any]
    data: dict[str, Any]
    bids: list[Any] | None = None
    asks: list[Any] | None = None
    price_changes: list[dict[str, Any]] = field(default_factory=list)

    def as_orderbook_event(self) -> dict[str, Any]:
        event = dict(self.data)
        event["event_type"] = self.event_type
        if self.asset_id is not None:
            event["asset_id"] = self.asset_id
        if self.timestamp is not None:
            event["timestamp"] = self.timestamp
        if self.market is not None:
            event["market"] = self.market
        if self.bids is not None:
            event["bids"] = self.bids
        if self.asks is not None:
            event["asks"] = self.asks
        if self.price_changes:
            event["price_changes"] = self.price_changes
        return event


def _first_value(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return None


def _normalize_price_change(change: Any) -> dict[str, Any] | None:
    if not isinstance(change, dict):
        return None
    normalized = dict(change)
    asset_id = _first_value(
        normalized,
        "asset_id",
        "assetId",
        "token_id",
        "tokenId",
    )
    if asset_id is not None:
        normalized["asset_id"] = str(asset_id)
    return normalized


def _normalize_market_event(message: dict[str, Any]) -> NormalizedMarketEvent:
    """Normalize raw endpoint events and typed ``payload`` envelopes."""

    event_type = str(
        _first_value(message, "event_type", "eventType", "type") or "unknown"
    ).lower()
    payload = message.get("payload")
    if isinstance(payload, dict):
        data = dict(payload)
    else:
        data = dict(message)

    asset_id = _first_value(data, "asset_id", "assetId", "token_id", "tokenId")
    timestamp = _first_value(data, "timestamp", "ts")
    market = _first_value(data, "market", "condition_id", "conditionId")
    if asset_id is not None:
        data["asset_id"] = str(asset_id)
    if timestamp is not None:
        data["timestamp"] = timestamp
    if market is not None:
        data["market"] = str(market)

    bids = data.get("bids") if isinstance(data.get("bids"), list) else None
    asks = data.get("asks") if isinstance(data.get("asks"), list) else None
    raw_changes = _first_value(data, "price_changes", "priceChanges")
    if isinstance(raw_changes, dict):
        raw_changes = [raw_changes]
    price_changes = [
        normalized
        for item in (raw_changes or [])
        if (normalized := _normalize_price_change(item)) is not None
    ]
    if price_changes:
        data["price_changes"] = price_changes

    if event_type == "market_resolved":
        winning_asset_id = _first_value(data, "winning_asset_id", "winningAssetId", "winningTokenId")
        winning_outcome = _first_value(data, "winning_outcome", "winningOutcome")
        if winning_asset_id is not None:
            data["winning_asset_id"] = str(winning_asset_id)
        if winning_outcome is not None:
            data["winning_outcome"] = str(winning_outcome)

    return NormalizedMarketEvent(
        event_type=event_type,
        asset_id=str(asset_id) if asset_id is not None else None,
        timestamp=timestamp,
        market=str(market) if market is not None else None,
        raw_data=dict(message),
        data=data,
        bids=bids,
        asks=asks,
        price_changes=price_changes,
    )


def parse_ws_message(message: Any) -> list[NormalizedMarketEvent]:
    """Return normalized events from dict, list, envelope, or JSON text.

    Invalid JSON and unsupported root structures are ignored as data errors;
    the caller can log them and keep the WebSocket connection alive.
    """

    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError:
            return []
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except json.JSONDecodeError:
            return []
    if isinstance(message, list):
        events: list[NormalizedMarketEvent] = []
        for item in message:
            events.extend(parse_ws_message(item))
        return events
    if isinstance(message, dict):
        # Be tolerant of a transport wrapper carrying an event array.
        if not any(key in message for key in ("event_type", "eventType", "type", "payload")):
            nested = message.get("data")
            if isinstance(nested, list):
                return parse_ws_message(nested)
        return [_normalize_market_event(message)]
    return []


@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    quantity: float


@dataclass(frozen=True)
class ExecutionEstimate:
    """Depth execution result.

    ``fills`` contains the actually consumed ``(quantity, price)`` pairs.
    It is kept so downstream fee estimates can apply the fee formula at each
    executed level instead of approximating a multi-level fill at its VWAP.
    Slippage fields are explicit: ``slippage_per_share`` is a price/share
    delta, while ``slippage_total`` is a USDC notional amount.
    """

    requested_quantity: float
    filled_quantity: float
    notional: float
    average_price: float | None
    best_price: float | None
    slippage_per_share: float
    slippage_total: float
    complete: bool
    levels_consumed: int
    fills: tuple[tuple[float, float], ...] = ()


@dataclass
class OrderBook:
    asset_id: str
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)
    updated_at: datetime | None = None
    sequence: int | None = None

    def __post_init__(self) -> None:
        self._sort_and_clean()

    @property
    def best_bid(self) -> OrderBookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> OrderBookLevel | None:
        return self.asks[0] if self.asks else None

    @property
    def spread(self) -> float | None:
        if not self.best_bid or not self.best_ask:
            return None
        return max(0.0, self.best_ask.price - self.best_bid.price)

    @property
    def available_buy_quantity(self) -> float:
        return sum(level.quantity for level in self.asks)

    @property
    def available_sell_quantity(self) -> float:
        return sum(level.quantity for level in self.bids)

    def age_ms(self, now: datetime | None = None) -> float:
        if self.updated_at is None:
            return float("inf")
        current = as_utc(now or datetime.now(UTC))
        return max(0.0, (current - as_utc(self.updated_at)).total_seconds() * 1000.0)

    def coherent(self) -> bool:
        return (
            bool(self.bids)
            and bool(self.asks)
            and all(level.price > 0 and level.quantity >= 0 for level in self.bids + self.asks)
            and self.best_bid is not None
            and self.best_ask is not None
            and self.best_bid.price <= self.best_ask.price
        )

    def clone(self) -> "OrderBook":
        return copy.deepcopy(self)

    def _sort_and_clean(self) -> None:
        bid_by_price: dict[float, float] = {}
        ask_by_price: dict[float, float] = {}
        for level in self.bids:
            if level.price > 0 and level.quantity > 0:
                bid_by_price[level.price] = bid_by_price.get(level.price, 0.0) + level.quantity
        for level in self.asks:
            if level.price > 0 and level.quantity > 0:
                ask_by_price[level.price] = ask_by_price.get(level.price, 0.0) + level.quantity
        self.bids = [OrderBookLevel(p, q) for p, q in sorted(bid_by_price.items(), reverse=True)]
        self.asks = [OrderBookLevel(p, q) for p, q in sorted(ask_by_price.items())]

    @staticmethod
    def _levels(raw: Any) -> list[OrderBookLevel]:
        levels: list[OrderBookLevel] = []
        for item in raw or []:
            if isinstance(item, dict):
                price = item.get("price")
                quantity = item.get("size", item.get("quantity", item.get("qty")))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                price, quantity = item[0], item[1]
            else:
                continue
            try:
                levels.append(OrderBookLevel(float(price), float(quantity)))
            except (TypeError, ValueError):
                continue
        return levels

    @classmethod
    def from_api(cls, asset_id: str, payload: dict[str, Any]) -> "OrderBook":
        timestamp = payload.get("timestamp") or payload.get("ts")
        updated_at = None
        if timestamp is not None:
            try:
                value = float(timestamp)
                if value > 10_000_000_000:
                    value /= 1000.0
                updated_at = datetime.fromtimestamp(value, tz=UTC)
            except (TypeError, ValueError, OSError):
                updated_at = None
        return cls(
            asset_id=str(payload.get("asset_id") or payload.get("assetId") or asset_id),
            bids=cls._levels(payload.get("bids")),
            asks=cls._levels(payload.get("asks")),
            updated_at=updated_at or datetime.now(UTC),
            sequence=int(payload["sequence"]) if payload.get("sequence") is not None else None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "bids": [[level.price, level.quantity] for level in self.bids],
            "asks": [[level.price, level.quantity] for level in self.asks],
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "sequence": self.sequence,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OrderBook":
        return cls(
            asset_id=str(payload["asset_id"]),
            bids=cls._levels(payload.get("bids")),
            asks=cls._levels(payload.get("asks")),
            updated_at=parse_datetime(payload.get("updated_at")),
            sequence=payload.get("sequence"),
        )

    def estimate_buy_cost(self, quantity: float) -> ExecutionEstimate:
        return self._estimate(quantity, self.asks, is_buy=True)

    def estimate_sell_proceeds(self, quantity: float) -> ExecutionEstimate:
        return self._estimate(quantity, self.bids, is_buy=False)

    @staticmethod
    def _estimate(
        quantity: float,
        levels: list[OrderBookLevel],
        is_buy: bool,
    ) -> ExecutionEstimate:
        requested = max(0.0, float(quantity))
        if requested == 0.0 or not levels:
            return ExecutionEstimate(requested, 0.0, 0.0, None, None, 0.0, 0.0, requested == 0.0, 0)
        remaining = requested
        notional = 0.0
        filled = 0.0
        levels_used = 0
        fills: list[tuple[float, float]] = []
        best_price = levels[0].price
        for level in levels:
            if remaining <= 1e-12:
                break
            amount = min(remaining, level.quantity)
            filled += amount
            notional += amount * level.price
            remaining -= amount
            levels_used += 1
            fills.append((amount, level.price))
        average = notional / filled if filled else None
        slippage = (average - best_price) if is_buy and average is not None else 0.0
        if not is_buy and average is not None:
            slippage = best_price - average
        return ExecutionEstimate(
            requested_quantity=requested,
            filled_quantity=filled,
            notional=notional,
            average_price=average,
            best_price=best_price,
            slippage_per_share=max(0.0, slippage),
            slippage_total=max(0.0, slippage) * filled,
            complete=remaining <= 1e-12,
            levels_consumed=levels_used,
            fills=tuple(fills),
        )

    def apply_event(self, event: dict[str, Any]) -> bool:
        """Apply documented ``book`` or ``price_change`` market events."""

        event_type = str(event.get("event_type", event.get("type", ""))).lower()
        event_asset = str(event.get("asset_id", event.get("assetId", self.asset_id)))
        if event_asset != self.asset_id:
            return False
        if event_type == "book" or (event.get("bids") is not None and event.get("asks") is not None):
            self.bids = self._levels(event.get("bids"))
            self.asks = self._levels(event.get("asks"))
            self.sequence = int(event["sequence"]) if event.get("sequence") is not None else self.sequence
            self.updated_at = _event_time(event) or datetime.now(UTC)
            self._sort_and_clean()
            return True
        changes = event.get("price_changes", event.get("priceChanges", []))
        if event_type == "price_change" and isinstance(changes, dict):
            changes = [changes]
        if event_type == "price_change" and changes:
            for change in changes:
                if str(change.get("asset_id", change.get("assetId", self.asset_id))) != self.asset_id:
                    continue
                try:
                    price = float(change["price"])
                    size = float(change.get("size", change.get("quantity", 0.0)))
                except (KeyError, TypeError, ValueError):
                    continue
                side = str(change.get("side", "")).upper()
                target = self.bids if side in {"BUY", "BID"} else self.asks
                target[:] = [level for level in target if abs(level.price - price) > 1e-12]
                if size > 0:
                    target.append(OrderBookLevel(price, size))
            self.updated_at = _event_time(event) or datetime.now(UTC)
            self._sort_and_clean()
            return True
        return False


def _event_time(event: dict[str, Any]) -> datetime | None:
    value = event.get("timestamp") or event.get("ts")
    if value is None:
        return None
    try:
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000.0
        return datetime.fromtimestamp(number, tz=UTC)
    except (TypeError, ValueError):
        return parse_datetime(value)
    except OSError:
        return None


@dataclass(frozen=True)
class PolymarketMarket:
    market_id: str
    condition_id: str
    question: str
    slug: str
    up_token_id: str
    down_token_id: str
    start_time: datetime | None
    end_time: datetime
    resolution_source: str | None
    price_to_beat: float | None
    active: bool
    closed: bool
    accepting_orders: bool | None
    fees_enabled: bool | None
    fee_rate: float | None
    fee_exponent: float | None
    raw: dict[str, Any] = field(repr=False, compare=False)

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, (self.end_time - datetime.now(UTC)).total_seconds())

    def remaining_seconds_at(self, now: datetime) -> float:
        return max(0.0, (self.end_time - as_utc(now)).total_seconds())

    @property
    def token_ids(self) -> tuple[str, str]:
        return self.up_token_id, self.down_token_id


class MarketDiscovery:
    """Read-only Gamma API market discovery with local BTC/5m validation."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        gamma_url: str,
        logger_: logging.Logger | None = None,
    ) -> None:
        self.http = http
        self.gamma_url = gamma_url.rstrip("/")
        self.logger = logger_ or logging.getLogger(__name__)

    async def list_active_markets(self, max_pages: int = 3) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        cursor: str | None = None
        keyset_error: Exception | None = None
        for _ in range(max_pages):
            params: dict[str, Any] = {"closed": "false", "limit": 100}
            if cursor:
                params["after_cursor"] = cursor
            try:
                response = await self.http.get(f"{self.gamma_url}/markets/keyset", params=params)
                response.raise_for_status()
                payload = response.json()
            except (httpx.HTTPError, ValueError) as exc:
                keyset_error = exc
                break
            if isinstance(payload, list):
                page = payload
                cursor = None
            elif isinstance(payload, dict):
                page = payload.get("markets", payload.get("data", []))
                cursor = payload.get("next_cursor") or payload.get("nextCursor")
            else:
                page = []
                cursor = None
            markets.extend(item for item in page if isinstance(item, dict))
            if not cursor or not page:
                break
        if markets:
            return markets
        # The documented non-keyset endpoint is a compatibility fallback for
        # deployments that have not exposed the keyset route yet.
        try:
            response = await self.http.get(
                f"{self.gamma_url}/markets",
                params={"active": "true", "closed": "false", "limit": 100},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            detail = f"; keyset={keyset_error}" if keyset_error else ""
            raise RuntimeError(
                f"Gamma API did not return JSON from its public market endpoints: {exc}{detail}"
            ) from exc
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            markets = payload.get("markets", payload.get("data", []))
            return markets if isinstance(markets, list) else []
        return []

    async def list_current_markets(
        self,
        now: datetime,
        window_seconds: float = 30.0 * 60.0,
    ) -> list[dict[str, Any]]:
        """Fetch the small Gamma window that can contain the current interval.

        The current crypto market schema uses ``endDate`` for the actual
        interval end but ``startDate`` for creation time.  Gamma's keyset
        endpoint can filter on ``end_date_min``/``end_date_max``; using that
        window avoids scanning unrelated historical markets and avoids an
        assumption about sequential market slugs or IDs.
        """

        current = as_utc(now)
        params: dict[str, Any] = {
            "closed": "false",
            "limit": 100,
            "order": "endDate",
            "ascending": "true",
            "end_date_min": current.isoformat().replace("+00:00", "Z"),
            "end_date_max": (
                current + timedelta(seconds=max(60.0, window_seconds))
            ).isoformat().replace("+00:00", "Z"),
        }
        try:
            response = await self.http.get(
                f"{self.gamma_url}/markets/keyset", params=params
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            self.logger.warning("[MARKET] current Gamma window unavailable: %s", exc)
            return []

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            page = payload.get("markets", payload.get("data", []))
            return [item for item in page if isinstance(item, dict)] if isinstance(page, list) else []
        return []

    async def search_active_markets(
        self,
        query: str = "bitcoin up or down",
        max_pages: int = 2,
    ) -> list[dict[str, Any]]:
        """Use Gamma's public search as a compatibility fallback."""

        markets: list[dict[str, Any]] = []
        for page_number in range(1, max_pages + 1):
            try:
                response = await self.http.get(
                    f"{self.gamma_url}/public-search",
                    params={
                        "q": query,
                        "limit_per_type": 50,
                        "page": page_number,
                        "events_status": "active",
                        "search_profiles": "false",
                        "search_tags": "false",
                    },
                )
                response.raise_for_status()
                payload = response.json()
            except (httpx.HTTPError, ValueError) as exc:
                self.logger.warning("[MARKET] Gamma public search unavailable: %s", exc)
                return []

            if not isinstance(payload, dict):
                return markets
            events = payload.get("events")
            page_markets = payload.get("markets")
            if isinstance(page_markets, list):
                markets.extend(item for item in page_markets if isinstance(item, dict))
            if isinstance(events, list):
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    nested = event.get("markets")
                    if isinstance(nested, list):
                        markets.extend(item for item in nested if isinstance(item, dict))
            pagination = payload.get("pagination")
            if not isinstance(pagination, dict) or not pagination.get("hasMore"):
                break

        deduplicated: dict[str, dict[str, Any]] = {}
        for item in markets:
            key = str(item.get("id") or item.get("slug") or len(deduplicated))
            deduplicated[key] = item
        return list(deduplicated.values())

    @staticmethod
    def _token_mapping(raw: dict[str, Any]) -> tuple[list[str], dict[str, str]]:
        outcomes = [str(item) for item in parse_json_list(raw.get("outcomes"))]
        token_values = parse_json_list(raw.get("clobTokenIds") or raw.get("clob_token_ids"))
        token_by_outcome: dict[str, str] = {}
        if isinstance(raw.get("tokens"), list):
            for token in raw["tokens"]:
                if not isinstance(token, dict):
                    continue
                token_id = token.get("token_id") or token.get("tokenId") or token.get("id")
                outcome = token.get("outcome") or token.get("name")
                if token_id and outcome:
                    token_by_outcome[str(outcome).strip().lower()] = str(token_id)
        if not token_by_outcome and outcomes and len(outcomes) == len(token_values):
            token_by_outcome = {
                outcome.strip().lower(): str(token)
                for outcome, token in zip(outcomes, token_values)
            }
        return outcomes, token_by_outcome

    @classmethod
    def _parse_failure_reason(cls, raw: Any) -> str:
        if not isinstance(raw, dict):
            return "invalid_payload"
        if not (raw.get("conditionId") or raw.get("condition_id")):
            return "missing_condition_id"
        if not (raw.get("id") or raw.get("marketId")):
            return "missing_market_id"
        if not parse_datetime(
            raw.get("endDate") or raw.get("end_date") or raw.get("endDateIso")
        ):
            return "missing_end_time"
        outcomes, token_by_outcome = cls._token_mapping(raw)
        if not outcomes:
            return "missing_outcomes"
        if not token_by_outcome:
            return "missing_token_ids"
        up = {"up", "yes", "true"}.intersection(token_by_outcome)
        down = {"down", "no", "false"}.intersection(token_by_outcome)
        if not up or not down:
            return "missing_up_down_tokens"
        return "invalid_payload"

    @classmethod
    def parse_market(cls, raw: dict[str, Any]) -> PolymarketMarket | None:
        condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "")
        market_id = str(raw.get("id") or raw.get("marketId") or "")
        end_time = parse_datetime(
            raw.get("endDate") or raw.get("end_date") or raw.get("endDateIso")
        )
        if not condition_id or not market_id or not end_time:
            return None

        _, token_by_outcome = cls._token_mapping(raw)

        def find_token(names: set[str]) -> str | None:
            for outcome, token in token_by_outcome.items():
                if outcome in names:
                    return token
            return None

        up_token = find_token({"up", "yes", "true"})
        down_token = find_token({"down", "no", "false"})
        if not up_token or not down_token:
            return None

        fee_schedule = raw.get("feeSchedule") or raw.get("fee_schedule") or {}
        fee_rate = optional_float(fee_schedule.get("rate") if isinstance(fee_schedule, dict) else None)
        fee_exponent = optional_float(
            fee_schedule.get("exponent") if isinstance(fee_schedule, dict) else None
        )
        if fee_rate is None:
            fee_rate = optional_float(raw.get("feeRate") or raw.get("fee_rate"))
        price_to_beat = None
        price_sources = [raw]
        crypto_config = raw.get("cryptoMarketConfig")
        if isinstance(crypto_config, dict):
            price_sources.append(crypto_config)
        for source in price_sources:
            for key in ("priceToBeat", "price_to_beat", "strikePrice", "strike_price"):
                price_to_beat = optional_float(source.get(key))
                if price_to_beat is not None:
                    break
            if price_to_beat is not None:
                break

        start_time = parse_datetime(
            raw.get("eventStartTime")
            or raw.get("event_start_time")
            or raw.get("startTime")
            or raw.get("start_time")
            or raw.get("startDate")
            or raw.get("start_date")
        )

        return PolymarketMarket(
            market_id=market_id,
            condition_id=condition_id,
            question=str(raw.get("question") or raw.get("title") or ""),
            slug=str(raw.get("slug") or ""),
            up_token_id=up_token,
            down_token_id=down_token,
            start_time=start_time,
            end_time=end_time,
            resolution_source=raw.get("resolutionSource") or raw.get("resolution_source"),
            price_to_beat=price_to_beat,
            active=bool(raw.get("active", True)),
            closed=bool(raw.get("closed", False)),
            accepting_orders=(
                bool(raw["acceptingOrders"]) if raw.get("acceptingOrders") is not None else None
            ),
            fees_enabled=(
                bool(raw["feesEnabled"]) if raw.get("feesEnabled") is not None else None
            ),
            fee_rate=fee_rate,
            fee_exponent=fee_exponent,
            raw=raw,
        )

    @staticmethod
    def _market_text(raw: dict[str, Any]) -> str:
        text_values = [
            raw.get(key, "")
            for key in (
                "question",
                "title",
                "slug",
                "description",
                "marketType",
                "seriesSlug",
                "groupItemTitle",
                "cryptoMarketConfig",
            )
        ]
        for relation_key in ("events", "series"):
            relation = raw.get(relation_key)
            if isinstance(relation, list):
                text_values.extend(item for item in relation if isinstance(item, dict))
        return " ".join(str(value) for value in text_values).lower()

    @classmethod
    def is_btc_5m(cls, market: PolymarketMarket) -> bool:
        raw_text = cls._market_text(market.raw)
        bitcoin = bool(re.search(r"\b(btc|bitcoin)\b", raw_text))
        duration_seconds = None
        if market.start_time:
            duration_seconds = (market.end_time - market.start_time).total_seconds()
        five_minute_label = bool(
            re.search(r"(?<!\d)5\s*[- ]?(?:m|min|minute)s?\b", raw_text)
        )
        five_minute_duration = duration_seconds is not None and 240.0 <= duration_seconds <= 360.0
        return bitcoin and (five_minute_label or five_minute_duration)

    @classmethod
    def evaluate_market(
        cls,
        raw: dict[str, Any],
        now: datetime | None = None,
    ) -> tuple[PolymarketMarket | None, str]:
        """Return a candidate and a stable read-only diagnostic reason."""

        current = as_utc(now or datetime.now(UTC))
        market = cls.parse_market(raw)
        if market is None:
            return None, cls._parse_failure_reason(raw)
        if not re.search(r"\b(btc|bitcoin)\b", cls._market_text(market.raw)):
            return None, "not_btc"
        if not cls.is_btc_5m(market):
            return None, "not_btc_5m"
        if market.closed:
            return None, "closed"
        if not market.active:
            return None, "inactive"
        if market.accepting_orders is False:
            return None, "not_accepting_orders"
        if market.start_time and market.start_time > current:
            return None, "not_started"
        if market.end_time <= current:
            return None, "expired"
        return market, "eligible"

    async def discover_btc_5m(self, now: datetime | None = None) -> PolymarketMarket | None:
        current = as_utc(now or datetime.now(UTC))
        candidates: list[PolymarketMarket] = []
        raw_markets = await self.list_current_markets(current)
        if not raw_markets:
            raw_markets = await self.search_active_markets()
        if not raw_markets:
            raw_markets = await self.list_active_markets()
        for raw in raw_markets:
            market, reason = self.evaluate_market(raw, current)
            if market is not None and reason == "eligible":
                candidates.append(market)
        candidates.sort(key=lambda item: item.end_time)
        return candidates[0] if candidates else None


class PolymarketClient:
    """Public, read-only Gamma and CLOB client."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        gamma_url: str,
        clob_url: str,
        logger_: logging.Logger | None = None,
    ) -> None:
        self.http = http
        self.discovery = MarketDiscovery(http, gamma_url, logger_)
        self.clob_url = clob_url.rstrip("/")
        self.logger = logger_ or logging.getLogger(__name__)

    async def get_order_book(self, token_id: str) -> OrderBook:
        response = await self.http.get(f"{self.clob_url}/book", params={"token_id": token_id})
        response.raise_for_status()
        return OrderBook.from_api(token_id, response.json())

    async def get_order_books(self, token_ids: list[str]) -> dict[str, OrderBook]:
        result: dict[str, OrderBook] = {}
        for token_id in token_ids:
            try:
                result[token_id] = await self.get_order_book(token_id)
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                self.logger.warning("[ORDERBOOK] unable to bootstrap %s: %s", token_id, exc)
        return result


@dataclass
class BookHealth:
    connected: bool = False
    last_message_at: datetime | None = None
    reconnects: int = 0
    last_error: str | None = None


class PolymarketBookFeed:
    """Reconnectable public market-channel feed with REST bootstrap."""

    def __init__(
        self,
        client: PolymarketClient,
        ws_url: str,
        refresh_seconds: float = 5.0,
        logger_: logging.Logger | None = None,
    ) -> None:
        self.client = client
        self.ws_url = ws_url
        self.refresh_seconds = refresh_seconds
        self.logger = logger_ or logging.getLogger(__name__)
        self.books: dict[str, OrderBook] = {}
        self.resolved_outcomes: dict[str, str] = {}
        self.resolved_asset_ids: dict[str, str] = {}
        self.health = BookHealth()
        self._stop = asyncio.Event()

    async def stop(self) -> None:
        self._stop.set()

    async def _bootstrap(self, asset_ids: list[str]) -> None:
        fresh = await self.client.get_order_books(asset_ids)
        for token_id, book in fresh.items():
            self.books[token_id] = book

    async def run(self, assets_provider: Callable[[], Awaitable[list[str]] | list[str]]) -> None:
        if websockets is None:
            raise RuntimeError("websockets dependency is required for PolymarketBookFeed")
        delay = 1.0
        while not self._stop.is_set():
            try:
                assets = assets_provider()
                if asyncio.iscoroutine(assets):
                    assets = await assets
                assets = sorted({str(item) for item in assets if item})
                if not assets:
                    await asyncio.sleep(self.refresh_seconds)
                    continue
                await self._bootstrap(assets)
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,
                    close_timeout=5,
                ) as websocket:
                    await websocket.send(
                        json.dumps(
                            {
                                "assets_ids": assets,
                                "type": "market",
                                "custom_feature_enabled": True,
                            }
                        )
                    )
                    self.health.connected = True
                    delay = 1.0
                    heartbeat = asyncio.create_task(self._heartbeat(websocket))
                    self.logger.info("[ORDERBOOK] subscribed to %d assets", len(assets))
                    try:
                        while not self._stop.is_set():
                            try:
                                raw = await asyncio.wait_for(
                                    websocket.recv(), timeout=self.refresh_seconds
                                )
                            except asyncio.TimeoutError:
                                latest_assets = assets_provider()
                                if asyncio.iscoroutine(latest_assets):
                                    latest_assets = await latest_assets
                                if sorted({str(item) for item in latest_assets if item}) != assets:
                                    break
                                continue
                            if raw in {"PONG", "pong"}:
                                continue
                            try:
                                events = self._decode_events(raw)
                            except (TypeError, ValueError) as exc:
                                self.logger.warning("[ORDERBOOK] invalid WebSocket message: %s", exc)
                                continue
                            for event in events:
                                try:
                                    self._apply_event(event)
                                except (TypeError, ValueError, KeyError) as exc:
                                    self.logger.warning(
                                        "[ORDERBOOK] invalid WebSocket event: %s", exc
                                    )
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError, asyncio.TimeoutError, httpx.HTTPError, Exception) as exc:
                self.health.connected = False
                self.health.reconnects += 1
                self.health.last_error = str(exc)
                self.logger.warning("[ORDERBOOK] feed unavailable: %s; retrying", exc)
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2.0)

    async def _heartbeat(self, websocket: Any) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(10.0)
            try:
                await websocket.send("PING")
            except Exception:
                return

    @staticmethod
    def _decode_events(raw: Any) -> list[NormalizedMarketEvent]:
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError("WebSocket message is not valid UTF-8") from exc
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError("WebSocket message is not valid JSON") from exc
        return parse_ws_message(raw)

    def _apply_event(self, event: NormalizedMarketEvent | dict[str, Any]) -> None:
        if isinstance(event, NormalizedMarketEvent):
            normalized_events = [event]
        else:
            normalized_events = parse_ws_message(event)
        for normalized in normalized_events:
            self._apply_normalized_event(normalized)

    def _apply_normalized_event(self, event: NormalizedMarketEvent) -> None:
        event_type = event.event_type
        event_data = event.as_orderbook_event()
        self.health.last_message_at = datetime.now(UTC)
        if event_type == "market_resolved":
            market_id = str(event.market or "")
            winning_outcome = event_data.get("winning_outcome")
            winning_asset_id = event_data.get("winning_asset_id")
            if market_id and winning_outcome:
                self.resolved_outcomes[market_id] = str(winning_outcome).upper()
            if market_id and winning_asset_id:
                self.resolved_asset_ids[market_id] = str(winning_asset_id)
            return
        if event_type == "price_change":
            applied = False
            for book in self.books.values():
                applied = book.apply_event(event_data) or applied
            if applied:
                self.health.last_message_at = datetime.now(UTC)
            return
        asset_id = str(event.asset_id or "")
        if not asset_id and event_type == "book":
            return
        book = self.books.get(asset_id)
        if not book:
            return
        if book.apply_event(event_data):
            self.health.last_message_at = datetime.now(UTC)
