"""Realtime BTC market data and health tracking."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:  # pragma: no cover - dependency is declared in requirements.txt
    websockets = None
    ConnectionClosed = Exception


UTC = timezone.utc
TickCallback = Callable[["MarketTick"], Awaitable[None] | None]


def utc_now() -> datetime:
    return datetime.now(UTC)


def as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


@dataclass(frozen=True)
class MarketTick:
    local_timestamp: datetime
    exchange_timestamp: datetime | None
    symbol: str
    price: float
    bid: float
    ask: float
    bid_quantity: float
    ask_quantity: float
    spread: float
    latency_ms: float | None

    @property
    def data_age_ms(self) -> float:
        return self.age_ms()

    def age_ms(self, now: datetime | None = None) -> float:
        current = as_utc(now or utc_now())
        return max(0.0, (current - as_utc(self.local_timestamp)).total_seconds() * 1000.0)

    @property
    def coherent(self) -> bool:
        return (
            self.price > 0
            and self.bid > 0
            and self.ask > 0
            and self.bid <= self.ask
            and self.bid_quantity >= 0
            and self.ask_quantity >= 0
        )

    @classmethod
    def from_binance_payload(
        cls,
        payload: dict[str, Any] | str,
        received_at: datetime | None = None,
    ) -> "MarketTick":
        if isinstance(payload, str):
            payload = json.loads(payload)
        raw = payload.get("data", payload)
        local_timestamp = as_utc(received_at or utc_now())
        exchange_ms = raw.get("E") or raw.get("T")
        exchange_timestamp = (
            datetime.fromtimestamp(float(exchange_ms) / 1000.0, tz=UTC)
            if exchange_ms is not None
            else None
        )
        bid = float(raw["b"])
        ask = float(raw["a"])
        price = (bid + ask) / 2.0
        latency_ms = (
            max(0.0, (local_timestamp - exchange_timestamp).total_seconds() * 1000.0)
            if exchange_timestamp
            else None
        )
        return cls(
            local_timestamp=local_timestamp,
            exchange_timestamp=exchange_timestamp,
            symbol=str(raw.get("s", "BTCUSDT")),
            price=price,
            bid=bid,
            ask=ask,
            bid_quantity=float(raw.get("B", 0.0)),
            ask_quantity=float(raw.get("A", 0.0)),
            spread=max(0.0, ask - bid),
            latency_ms=latency_ms,
        )


@dataclass
class DataHealth:
    connected: bool = False
    last_tick_at: datetime | None = None
    last_exchange_timestamp: datetime | None = None
    last_latency_ms: float | None = None
    last_processing_latency_ms: float | None = None
    reconnects: int = 0
    last_error: str | None = None

    def age_ms(self, now: datetime | None = None) -> float:
        if self.last_tick_at is None:
            return float("inf")
        current = as_utc(now or utc_now())
        return max(0.0, (current - as_utc(self.last_tick_at)).total_seconds() * 1000.0)


class MarketDataStore:
    """Bounded in-memory history used by the model without future data."""

    def __init__(self, max_history: int = 2_000) -> None:
        self._latest: MarketTick | None = None
        self._history: deque[MarketTick] = deque(maxlen=max_history)
        self.health = DataHealth()

    def update(self, tick: MarketTick) -> None:
        self._latest = tick
        self._history.append(tick)
        self.health.last_tick_at = tick.local_timestamp
        self.health.last_exchange_timestamp = tick.exchange_timestamp
        self.health.last_latency_ms = tick.latency_ms
        self.health.last_error = None

    @property
    def latest(self) -> MarketTick | None:
        return self._latest

    def history(self, as_of: datetime | None = None) -> list[MarketTick]:
        """Return only observations available at ``as_of`` (anti-lookahead)."""

        if as_of is None:
            return list(self._history)
        cutoff = as_utc(as_of)
        return [tick for tick in self._history if as_utc(tick.local_timestamp) <= cutoff]

    def data_age_ms(self, now: datetime | None = None) -> float:
        return self.health.age_ms(now)

    def mark_disconnected(self, error: Exception | str | None = None) -> None:
        self.health.connected = False
        self.health.reconnects += 1
        self.health.last_error = str(error) if error else None

    def mark_processed(self, elapsed_ms: float) -> None:
        self.health.last_processing_latency_ms = max(0.0, elapsed_ms)


class BinanceSpotFeed:
    """Reconnectable public Binance ``bookTicker`` feed.

    It has no authentication and never exposes an order placement method.
    """

    def __init__(
        self,
        url: str,
        store: MarketDataStore,
        reconnect_seconds: float = 1.0,
        max_reconnect_seconds: float = 30.0,
        logger_: logging.Logger | None = None,
    ) -> None:
        self.url = url
        self.store = store
        self.reconnect_seconds = reconnect_seconds
        self.max_reconnect_seconds = max_reconnect_seconds
        self.logger = logger_ or logging.getLogger(__name__)
        self._stop = asyncio.Event()

    async def stop(self) -> None:
        self._stop.set()

    async def run(self, callback: TickCallback | None = None) -> None:
        if websockets is None:
            raise RuntimeError("websockets dependency is required for BinanceSpotFeed")
        delay = self.reconnect_seconds
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                ) as websocket:
                    self.store.health.connected = True
                    delay = self.reconnect_seconds
                    self.logger.info("[DATA] Binance WebSocket connected")
                    async for raw in websocket:
                        if self._stop.is_set():
                            break
                        try:
                            tick = MarketTick.from_binance_payload(raw)
                            self.store.update(tick)
                            processing_started = time.perf_counter()
                            if callback:
                                result = callback(tick)
                                if inspect.isawaitable(result):
                                    await result
                            self.store.mark_processed(
                                (time.perf_counter() - processing_started) * 1000.0
                            )
                        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                            self.store.mark_disconnected(exc)
                            self.logger.warning("[DATA] invalid Binance message: %s", exc)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError, asyncio.TimeoutError, Exception) as exc:
                self.store.mark_disconnected(exc)
                self.logger.warning("[DATA] Binance disconnected: %s; retrying", exc)
                await asyncio.sleep(delay)
                delay = min(self.max_reconnect_seconds, delay * 2.0)
