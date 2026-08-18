"""Entry point for the V1 paper-only research loop."""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import statistics
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from analytics import analyze
from config import AppConfig, load_config
from data import BinanceSpotFeed, MarketDataStore, MarketTick, utc_now
from database import Database
from execution import PaperExecutor, PaperTrade
from market import OrderBook, PolymarketBookFeed, PolymarketClient, PolymarketMarket
from monitoring import default_status_path, write_runtime_status
from risk import RiskEngine
from strategy import FeeModel, Opportunity, StrategyEngine


UTC = timezone.utc


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def _fee_model(market: PolymarketMarket, config: AppConfig) -> FeeModel:
    enabled = market.fees_enabled if market.fees_enabled is not None else config.polymarket_fees_enabled
    rate = market.fee_rate if market.fee_rate is not None else config.polymarket_taker_fee_rate
    exponent = market.fee_exponent if market.fee_exponent is not None else config.polymarket_fee_exponent
    return FeeModel(rate=max(0.0, rate), exponent=max(0.0, exponent), enabled=bool(enabled))


def _market_changed(old: PolymarketMarket | None, new: PolymarketMarket | None) -> bool:
    return bool(new and (old is None or old.market_id != new.market_id))


def _numeric_summary(values: list[float]) -> dict[str, float | int | None]:
    finite = [value for value in values if math.isfinite(value)]
    return {
        "count": len(values),
        "finite_count": len(finite),
        "mean": statistics.fmean(finite) if finite else None,
        "median": statistics.median(finite) if finite else None,
        "max": max(finite) if finite else None,
    }


def _annotate_opportunity_analytics(opportunity: Opportunity) -> None:
    """Copy the final risk decision into the persisted analytics payload."""

    analytics = opportunity.features.get("analytics")
    if not isinstance(analytics, dict):
        return
    if opportunity.strategy == "STRUCTURAL_ARB":
        structural = analytics.get("structural_arb")
        if isinstance(structural, dict):
            structural["decision"] = opportunity.decision
            structural["decision_reason"] = opportunity.decision_reason
    elif opportunity.strategy == "LATE_MARKET":
        late = analytics.get("late_market")
        if isinstance(late, dict):
            late["decision"] = opportunity.decision
            late["rejection_reason"] = (
                opportunity.decision_reason
                if opportunity.decision != "ACCEPT"
                else None
            )


def _runtime_status_payload(
    *,
    session_id: str,
    started_at: datetime,
    runtime_state: dict[str, Any],
    market: PolymarketMarket | None,
    data_store: MarketDataStore,
    book_feed: PolymarketBookFeed,
    risk: RiskEngine,
    running: bool = True,
) -> dict[str, Any]:
    """Build a non-sensitive status snapshot for the local read-only UI."""

    now = utc_now()
    tick = data_store.latest
    books = book_feed.books

    def book_status(token_id: str | None) -> dict[str, Any]:
        book = books.get(token_id) if token_id else None
        if book is None:
            return {
                "available": False,
                "bid": None,
                "ask": None,
                "bid_depth": None,
                "ask_depth": None,
                "age_ms": None,
                "coherent": False,
            }
        return {
            "available": True,
            "bid": book.best_bid.price if book.best_bid else None,
            "ask": book.best_ask.price if book.best_ask else None,
            "bid_depth": book.available_sell_quantity,
            "ask_depth": book.available_buy_quantity,
            "age_ms": book.age_ms(now),
            "coherent": book.coherent(),
        }

    last_tick_at = tick.local_timestamp.isoformat() if tick else None
    binance_age = data_store.data_age_ms(now)
    binance_connected = bool(data_store.health.connected and tick)
    current = runtime_state.get("last_opportunity")
    current = current if isinstance(current, dict) else {}
    market_payload = None
    if market:
        market_payload = {
            "market_id": market.market_id,
            "question": market.question,
            "slug": market.slug,
            "condition_id": market.condition_id,
            "start": market.start_time.isoformat() if market.start_time else None,
            "end": market.end_time.isoformat(),
            "remaining_seconds": market.remaining_seconds_at(now),
        }
    return {
        "mode": "PAPER",
        "paper_only": True,
        "running": running,
        "session_id": session_id,
        "started_at": started_at.isoformat(),
        "ended_at": now.isoformat() if not running else None,
        "uptime_seconds": max(0.0, (now - started_at).total_seconds()),
        "last_message_at": last_tick_at or book_feed.health.last_message_at.isoformat()
        if book_feed.health.last_message_at
        else None,
        "binance": {
            "status": "OK" if binance_connected and binance_age <= risk.config.maximum_data_age_ms else "STALE",
            "connected": data_store.health.connected,
            "last_tick_at": last_tick_at,
            "age_ms": binance_age if tick else None,
            "latency_ms": tick.latency_ms if tick else None,
            "reconnects": data_store.health.reconnects,
        },
        "polymarket_websocket": {
            "status": "OK" if book_feed.health.connected else "WAITING",
            "connected": book_feed.health.connected,
            "last_message_at": (
                book_feed.health.last_message_at.isoformat()
                if book_feed.health.last_message_at
                else None
            ),
            "reconnects": book_feed.health.reconnects,
        },
        "clob": {
            "status": runtime_state.get("clob_status", "WAITING"),
            "last_success_at": runtime_state.get("clob_last_success_at"),
        },
        "market": market_payload,
        "btc": {
            "price": tick.price if tick else None,
            "timestamp": last_tick_at,
            "recent_variation": runtime_state.get("btc_recent_variation"),
            "realized_volatility": runtime_state.get("realized_volatility"),
            "volatility_observations": runtime_state.get("volatility_observations"),
        },
        "order_book": {
            "UP": book_status(market.up_token_id if market else None),
            "DOWN": book_status(market.down_token_id if market else None),
        },
        "current_opportunity": current,
        "risk": {
            "circuit_breaker": risk.state.circuit_breaker,
            "breaker_reason": risk.state.breaker_reason,
            "market_data_breaker": risk.state.market_data_breaker,
            "market_data_breaker_reason": risk.state.market_data_breaker_reason,
        },
    }


def _book_failure_reasons(book: OrderBook) -> list[str]:
    """Classify the predicates used by OrderBook.coherent()."""

    reasons: list[str] = []
    if book.best_bid is None:
        reasons.append("missing_bid")
    if book.best_ask is None:
        reasons.append("missing_ask")
    if any(
        level.price <= 0 or level.quantity < 0
        for level in book.bids + book.asks
    ):
        reasons.append("invalid_level")
    if (
        book.best_bid is not None
        and book.best_ask is not None
        and book.best_bid.price > book.best_ask.price
    ):
        reasons.append("crossed_book")
    return reasons


@dataclass
class RuntimeDiagnostics:
    """Read-only runtime diagnostics; never feeds values back into decisions."""

    incoherent_orderbook_rejections: int = 0
    incoherent_reason_counts: Counter[str] = field(default_factory=Counter)
    timestamp_deltas_ms: list[float] = field(default_factory=list)
    up_ages_ms: list[float] = field(default_factory=list)
    down_ages_ms: list[float] = field(default_factory=list)
    crossed_books: int = 0
    stale_rejections: int = 0
    structural_incoherent_rejections: int = 0
    insufficient_edge_count: int = 0
    insufficient_edge_gross: list[float] = field(default_factory=list)
    insufficient_edge_net: list[float] = field(default_factory=list)
    insufficient_edge_fees: list[float] = field(default_factory=list)
    insufficient_edge_slippage: list[float] = field(default_factory=list)
    insufficient_edge_prices: list[float] = field(default_factory=list)
    insufficient_edge_time_remaining: list[float] = field(default_factory=list)
    insufficient_edge_sides: Counter[str] = field(default_factory=Counter)

    def record_orderbook_rejection(
        self,
        *,
        up_book: OrderBook,
        down_book: OrderBook,
        up_age_ms: float,
        down_age_ms: float,
        orderbook_coherent: bool,
        fresh_books: bool,
        logger: logging.Logger,
    ) -> None:
        up_reasons = _book_failure_reasons(up_book)
        down_reasons = _book_failure_reasons(down_book)
        structural_reasons = sorted(set(up_reasons + down_reasons))
        self.incoherent_orderbook_rejections += 1
        self.up_ages_ms.append(up_age_ms)
        self.down_ages_ms.append(down_age_ms)
        if up_book.updated_at is not None and down_book.updated_at is not None:
            self.timestamp_deltas_ms.append(
                abs((up_book.updated_at - down_book.updated_at).total_seconds() * 1000.0)
            )

        crossed = "crossed_book" in structural_reasons
        if crossed:
            self.crossed_books += 1
        if not fresh_books:
            self.stale_rejections += 1
        if not orderbook_coherent:
            self.structural_incoherent_rejections += 1

        if orderbook_coherent and fresh_books:
            # The risk decision can still carry this reason after the
            # RiskEngine circuit breaker was latched by an earlier rejection.
            primary_reason = "circuit_breaker_latched"
        elif not fresh_books and not orderbook_coherent:
            primary_reason = "stale_and_structurally_incoherent"
        elif not fresh_books:
            primary_reason = "stale"
        elif structural_reasons:
            primary_reason = "+".join(structural_reasons)
        else:
            primary_reason = "other"
        self.incoherent_reason_counts[primary_reason] += 1

        logger.debug(
            "[DIAGNOSTIC] incoherent_orderbook primary=%s reasons_up=%s reasons_down=%s "
            "up_bid=%s up_ask=%s down_bid=%s down_ask=%s up_timestamp=%s down_timestamp=%s "
            "up_age_ms=%.3f down_age_ms=%.3f timestamp_delta_ms=%s "
            "up_coherent=%s down_coherent=%s fresh_books=%s orderbook_coherent=%s",
            primary_reason,
            "+".join(up_reasons) or "none",
            "+".join(down_reasons) or "none",
            up_book.best_bid.price if up_book.best_bid else None,
            up_book.best_ask.price if up_book.best_ask else None,
            down_book.best_bid.price if down_book.best_bid else None,
            down_book.best_ask.price if down_book.best_ask else None,
            up_book.updated_at.isoformat() if up_book.updated_at else None,
            down_book.updated_at.isoformat() if down_book.updated_at else None,
            up_age_ms,
            down_age_ms,
            self.timestamp_deltas_ms[-1] if self.timestamp_deltas_ms else None,
            up_book.coherent(),
            down_book.coherent(),
            fresh_books,
            orderbook_coherent,
        )

    def record_insufficient_edge(self, opportunity: Opportunity) -> None:
        self.insufficient_edge_count += 1
        self.insufficient_edge_gross.append(opportunity.gross_edge)
        self.insufficient_edge_net.append(opportunity.net_edge)
        self.insufficient_edge_fees.append(opportunity.estimated_fees)
        self.insufficient_edge_slippage.append(opportunity.estimated_slippage)
        if opportunity.executable_price is not None:
            self.insufficient_edge_prices.append(opportunity.executable_price)
        self.insufficient_edge_time_remaining.append(opportunity.time_remaining)
        self.insufficient_edge_sides[opportunity.side] += 1

    def summary(self) -> dict[str, Any]:
        return {
            "incoherent_orderbook_rejections": self.incoherent_orderbook_rejections,
            "incoherent_reason_counts": dict(sorted(self.incoherent_reason_counts.items())),
            "timestamp_delta_ms": _numeric_summary(self.timestamp_deltas_ms),
            "up_age_ms": _numeric_summary(self.up_ages_ms),
            "down_age_ms": _numeric_summary(self.down_ages_ms),
            "crossed_books": self.crossed_books,
            "stale_rejections": self.stale_rejections,
            "structural_incoherent_rejections": self.structural_incoherent_rejections,
            "insufficient_edge": {
                "count": self.insufficient_edge_count,
                "gross_edge": _numeric_summary(self.insufficient_edge_gross),
                "net_edge": _numeric_summary(self.insufficient_edge_net),
                "fees": _numeric_summary(self.insufficient_edge_fees),
                "slippage": _numeric_summary(self.insufficient_edge_slippage),
                "executable_price": _numeric_summary(self.insufficient_edge_prices),
                "time_remaining_s": _numeric_summary(self.insufficient_edge_time_remaining),
                "sides": dict(sorted(self.insufficient_edge_sides.items())),
            },
        }


def _settle_matured_trades(
    open_trades: list[PaperTrade],
    market: PolymarketMarket,
    resolved_outcome: str | None,
    timestamp: datetime,
    database: Database,
    risk: RiskEngine,
    logger: logging.Logger,
    session_id: str | None = None,
) -> None:
    """Settle only from Polymarket's explicit market resolution event."""

    if resolved_outcome not in {"UP", "DOWN", "YES", "NO"}:
        return
    outcome = {"YES": "UP", "NO": "DOWN"}.get(resolved_outcome, resolved_outcome)
    remaining: list[PaperTrade] = []
    for trade in open_trades:
        if trade.market != market.market_id:
            remaining.append(trade)
            continue
        PaperExecutor.settle(trade, outcome, timestamp)
        database.insert_trade(trade, session_id=session_id)
        database.update_positions_for_trade(trade)
        risk.register_closed(trade.net_pnl or 0.0, trade.capital_required)
        logger.info(
            "[PAPER] settled market=%s outcome=%s net_pnl=%.5f",
            market.market_id,
            outcome,
            trade.net_pnl or 0.0,
        )
    open_trades[:] = remaining


async def discover_once(config: AppConfig) -> int:
    async with httpx.AsyncClient(
        timeout=config.network_timeout_seconds, verify=config.http_verify
    ) as http:
        client = PolymarketClient(http, config.gamma_api_url, config.clob_api_url)
        market = await client.discovery.discover_btc_5m()
        if not market:
            print("[MARKET] no active BTC 5m market found")
            return 1
        print(
            f"[MARKET] id={market.market_id} condition={market.condition_id} "
            f"UP={market.up_token_id} DOWN={market.down_token_id} "
            f"end={market.end_time.isoformat()} price_to_beat={market.price_to_beat}"
        )
        books = await client.get_order_books(list(market.token_ids))
        for outcome, token_id in (("UP", market.up_token_id), ("DOWN", market.down_token_id)):
            book = books.get(token_id)
            if not book:
                print(f"[ORDERBOOK] {outcome}=unavailable")
                continue
            print(
                f"[ORDERBOOK] {outcome} bid={book.best_bid.price if book.best_bid else None} "
                f"ask={book.best_ask.price if book.best_ask else None} "
                f"liquidity={book.available_buy_quantity:.4f}"
            )
    return 0


async def _process_tick(
    tick: MarketTick,
    *,
    market: PolymarketMarket,
    books: dict[str, OrderBook],
    data_store: MarketDataStore,
    database: Database,
    risk: RiskEngine,
    executor: PaperExecutor,
    strategy: StrategyEngine,
    open_trades: list[PaperTrade],
    logger: logging.Logger,
    diagnostics: RuntimeDiagnostics | None = None,
    session_id: str | None = None,
    runtime_state: dict[str, Any] | None = None,
) -> None:
    decision_time = utc_now()
    processing_started = time.perf_counter()
    data_age = tick.age_ms(decision_time)
    data_store.history(decision_time)  # makes the anti-lookahead boundary explicit
    if runtime_state is not None:
        previous_price = runtime_state.get("last_btc_price")
        runtime_state["last_btc_price"] = tick.price
        runtime_state["btc_recent_variation"] = (
            tick.price / previous_price - 1.0
            if isinstance(previous_price, (int, float)) and previous_price > 0
            else None
        )
        runtime_state["last_message_at"] = decision_time.isoformat()
    database.insert_observation(
        market,
        tick,
        books,
        decision_time,
        session_id=session_id,
    )
    logger.info(
        "[DATA] BTC=%.2f bid=%.2f ask=%.2f spread=%.4f age_ms=%.1f latency_ms=%s",
        tick.price,
        tick.bid,
        tick.ask,
        tick.spread,
        data_age,
        f"{tick.latency_ms:.1f}" if tick.latency_ms is not None else "n/a",
    )
    logger.info(
        "[MARKET] %s remaining=%.1fs price_to_beat=%s",
        market.market_id,
        market.remaining_seconds_at(decision_time),
        market.price_to_beat,
    )
    for outcome, token_id in (("UP", market.up_token_id), ("DOWN", market.down_token_id)):
        book = books.get(token_id)
        logger.info(
            "[ORDERBOOK] %s bid=%s ask=%s spread=%s liquidity=%s age_ms=%s",
            outcome,
            book.best_bid.price if book and book.best_bid else None,
            book.best_ask.price if book and book.best_ask else None,
            book.spread if book else None,
            book.available_buy_quantity if book else 0.0,
            book.age_ms(decision_time) if book else float("inf"),
        )

    up_book = books.get(market.up_token_id)
    down_book = books.get(market.down_token_id)
    if up_book is None or down_book is None:
        missing = [
            outcome for outcome, book in (("UP", up_book), ("DOWN", down_book)) if book is None
        ]
        logger.info("[ORDERBOOK] market data not ready; missing=%s", ",".join(missing))
        return
    required_books = (up_book, down_book)

    opportunities = strategy.evaluate(
        market,
        tick,
        books,
        data_store.history(decision_time),
        decision_time,
    )
    if runtime_state is not None:
        momentum = opportunities[0].features.get("momentum", {}) if opportunities else {}
        runtime_state["realized_volatility"] = momentum.get("realized_volatility")
        runtime_state["volatility_observations"] = momentum.get("volatility_observations")
    for opportunity in opportunities:
        orderbook_coherent = all(book is not None and book.coherent() for book in required_books)
        book_ages = [book.age_ms(decision_time) for book in required_books if book is not None]
        fresh_books = all(
            age_ms <= risk.config.maximum_data_age_ms for age_ms in book_ages
        )
        decision = risk.evaluate(
            opportunity,
            data_age_ms=max((data_age, *book_ages)),
            execution_latency_ms=max(
                tick.latency_ms or 0.0,
                (time.perf_counter() - processing_started) * 1000.0,
            ),
            now=decision_time,
            data_coherent=tick.coherent,
            orderbook_coherent=orderbook_coherent and fresh_books,
            market_open=(
                not market.closed
                and market.active
                and market.accepting_orders is not False
                and market.remaining_seconds_at(decision_time) > 0
            ),
        )
        if diagnostics and decision.reason == "incoherent_orderbook":
            diagnostics.record_orderbook_rejection(
                up_book=up_book,
                down_book=down_book,
                up_age_ms=up_book.age_ms(decision_time),
                down_age_ms=down_book.age_ms(decision_time),
                orderbook_coherent=orderbook_coherent,
                fresh_books=fresh_books,
                logger=logger,
            )
        elif diagnostics and decision.reason == "insufficient_edge":
            diagnostics.record_insufficient_edge(opportunity)
        opportunity.decision = "ACCEPT" if decision.accepted else "REJECT"
        opportunity.decision_reason = decision.reason
        _annotate_opportunity_analytics(opportunity)
        database.insert_opportunity(opportunity, session_id=session_id)
        if runtime_state is not None:
            runtime_state["last_opportunity"] = {
                "timestamp": opportunity.timestamp.isoformat(),
                "strategy": opportunity.strategy,
                "decision": opportunity.decision,
                "decision_reason": opportunity.decision_reason,
                "gross_edge": opportunity.gross_edge,
                "net_edge": opportunity.net_edge,
                "estimated_fees": opportunity.estimated_fees,
                "estimated_slippage": opportunity.estimated_slippage,
                "capital_required": opportunity.capital_required,
                "quantity": opportunity.quantity,
                "analytics": opportunity.features.get("analytics", {}),
            }
        logger.info(
            "[STRATEGY] strategy=%s side=%s gross_edge=%.5f fees=%.5f slippage=%.5f net_edge=%.5f",
            opportunity.strategy,
            opportunity.side,
            opportunity.gross_edge,
            opportunity.estimated_fees,
            opportunity.estimated_slippage,
            opportunity.net_edge,
        )
        logger.info("[RISK] decision=%s reason=%s", opportunity.decision, opportunity.decision_reason)
        if not decision.accepted:
            continue
        trade = executor.execute(opportunity, books, decision_time)
        database.insert_trade(trade, session_id=session_id)
        database.insert_positions(trade)
        if trade.status == "FAILED":
            risk.register_failed_order()
            logger.warning("[PAPER] failed side=%s reason=%s", trade.side, trade.failure_reason)
            continue
        risk.register_open(trade.capital_required)
        open_trades.append(trade)
        logger.info(
            "[PAPER] BUY %s quantity=%.4f price=%.5f fees=%.5f status=%s",
            opportunity.side,
            trade.quantity,
            trade.entry_price,
            trade.fees,
            trade.status,
        )


async def run_live(config: AppConfig, duration: float | None = None) -> None:
    logger = logging.getLogger("paper-bot")
    session_id = uuid.uuid4().hex
    started_at = utc_now()
    status_path = default_status_path(config.database_path)
    database = Database(config.database_path)
    database.prune_before(utc_now().replace(microsecond=0) - timedelta(days=config.retention_days))
    data_store = MarketDataStore()
    risk = RiskEngine(config.risk)
    executor = PaperExecutor(
        mode=config.mode,
        fee_model=FeeModel(
            rate=config.polymarket_taker_fee_rate,
            exponent=config.polymarket_fee_exponent,
            enabled=config.polymarket_fees_enabled,
        ),
    )
    state: dict[str, PolymarketMarket | None] = {"market": None}
    open_trades: list[PaperTrade] = []
    diagnostics = RuntimeDiagnostics()
    runtime_state: dict[str, Any] = {
        "clob_status": "STARTING",
        "clob_last_success_at": None,
        "last_opportunity": None,
        "last_btc_price": None,
        "btc_recent_variation": None,
        "realized_volatility": None,
        "volatility_observations": None,
    }

    async with httpx.AsyncClient(
        timeout=config.network_timeout_seconds,
        verify=config.http_verify,
        headers={"User-Agent": "polymarket-edge-bot-paper/1.0"},
    ) as http:
        client = PolymarketClient(http, config.gamma_api_url, config.clob_api_url, logger)
        book_feed = PolymarketBookFeed(
            client,
            config.polymarket_ws_url,
            config.book_refresh_seconds,
            logger,
        )
        binance_feed = BinanceSpotFeed(config.binance_ws_url, data_store, logger_=logger)
        strategy: StrategyEngine | None = None
        stop = asyncio.Event()
        last_status_write = 0.0

        def publish_status(*, force: bool = False, running: bool = True) -> None:
            nonlocal last_status_write
            current_monotonic = time.monotonic()
            if not force and current_monotonic - last_status_write < 0.5:
                return
            try:
                write_runtime_status(
                    status_path,
                    _runtime_status_payload(
                        session_id=session_id,
                        started_at=started_at,
                        runtime_state=runtime_state,
                        market=state["market"],
                        data_store=data_store,
                        book_feed=book_feed,
                        risk=risk,
                        running=running,
                    ),
                )
                last_status_write = current_monotonic
            except (OSError, TypeError, ValueError) as exc:
                logger.debug("[DASHBOARD] status publication failed: %s", exc)

        publish_status(force=True)

        async def asset_provider() -> list[str]:
            market = state["market"]
            return list(market.token_ids) if market else []

        async def discover_loop() -> None:
            nonlocal strategy
            while not stop.is_set():
                try:
                    market = await client.discovery.discover_btc_5m()
                    runtime_state["clob_status"] = "OK"
                    runtime_state["clob_last_success_at"] = utc_now().isoformat()
                    previous_market = state["market"]
                    if previous_market and (
                        market is None or market.market_id != previous_market.market_id
                    ):
                        resolved_outcome = book_feed.resolved_outcomes.pop(
                            previous_market.condition_id, None
                        )
                        if resolved_outcome is None:
                            winning_asset_id = book_feed.resolved_asset_ids.pop(
                                previous_market.condition_id, None
                            )
                            if winning_asset_id == previous_market.up_token_id:
                                resolved_outcome = "UP"
                            elif winning_asset_id == previous_market.down_token_id:
                                resolved_outcome = "DOWN"
                        _settle_matured_trades(
                            open_trades,
                            previous_market,
                            resolved_outcome,
                            utc_now(),
                            database,
                            risk,
                            logger,
                            session_id=session_id,
                        )
                    if _market_changed(state["market"], market):
                        state["market"] = market
                        strategy = StrategyEngine(
                            config.strategy,
                            _fee_model(market, config),  # type: ignore[arg-type]
                        )
                        executor.fee_model = strategy.fee_model
                        logger.info(
                            "[MARKET] discovered BTC-5M id=%s UP=%s DOWN=%s remaining=%.1fs",
                            market.market_id,
                            market.up_token_id,
                            market.down_token_id,
                            market.remaining_seconds_at(utc_now()),
                        )
                    elif market is None:
                        logger.warning("[MARKET] no active BTC-5M market; waiting")
                    publish_status(force=True)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    runtime_state["clob_status"] = "ERROR"
                    logger.exception("[MARKET] discovery failed: %s", exc)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=config.market_poll_seconds)
                except asyncio.TimeoutError:
                    pass

        async def on_tick(tick: MarketTick) -> None:
            market = state["market"]
            if not market or not strategy:
                publish_status()
                return
            await _process_tick(
                tick,
                market=market,
                books=book_feed.books,
                data_store=data_store,
                database=database,
                risk=risk,
                executor=executor,
                strategy=strategy,
                open_trades=open_trades,
                logger=logger,
                diagnostics=diagnostics,
                session_id=session_id,
                runtime_state=runtime_state,
            )
            publish_status()

        async def stop_after_duration() -> None:
            if duration is not None:
                await asyncio.sleep(max(0.0, duration))
                stop.set()

        tasks = [
            asyncio.create_task(discover_loop(), name="market-discovery"),
            asyncio.create_task(book_feed.run(asset_provider), name="polymarket-books"),
            asyncio.create_task(binance_feed.run(on_tick), name="binance-data"),
        ]
        if duration is not None:
            tasks.append(asyncio.create_task(stop_after_duration(), name="duration"))
        try:
            await stop.wait()
        except asyncio.CancelledError:
            raise
        finally:
            await book_feed.stop()
            await binance_feed.stop()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            publish_status(force=True, running=False)
            report = analyze(
                database.fetch_opportunities(session_id=session_id),
                database.fetch_trades(session_id=session_id),
            )
            logger.info("[ANALYTICS] %s", report.as_dict())
            logger.info("[RISK] final_status=%s", risk.status())
            logger.info("[DIAGNOSTIC] %s", diagnostics.summary())
            database.close()


def run_replay(config: AppConfig, limit: int | None = None) -> int:
    database = Database(config.database_path)
    risk = RiskEngine(config.risk)
    executor = PaperExecutor(mode="PAPER")
    decisions: list[Opportunity] = []
    trades: list[PaperTrade] = []
    try:
        replay_history: list[MarketTick] = []
        for observation in database.replay_observations(limit):
            books = {
                token_id: book
                for token_id, book in (
                    (observation.market.up_token_id, observation.up_book),
                    (observation.market.down_token_id, observation.down_book),
                )
                if book
            }
            strategy = StrategyEngine(
                config.strategy,
                _fee_model(observation.market, config),
            )
            executor.fee_model = strategy.fee_model
            replay_history.append(observation.tick)
            for opportunity in strategy.evaluate(
                observation.market,
                observation.tick,
                books,
                replay_history,
                observation.timestamp,
            ):
                decision = risk.evaluate(
                    opportunity,
                    data_age_ms=0.0,
                    execution_latency_ms=0.0,
                    now=observation.timestamp,
                    data_coherent=observation.tick.coherent,
                    orderbook_coherent=all(book.coherent() for book in books.values()),
                )
                opportunity.decision = "ACCEPT" if decision.accepted else "REJECT"
                opportunity.decision_reason = decision.reason
                _annotate_opportunity_analytics(opportunity)
                decisions.append(opportunity)
                if decision.accepted:
                    trade = executor.execute(opportunity, books, observation.timestamp)
                    trades.append(trade)
        report = analyze(decisions, trades)
        print(f"[REPLAY] observations={len(decisions)} metrics={report.as_dict()}")
        return 0
    finally:
        database.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket BTC 5m paper research bot")
    parser.add_argument("--once", action="store_true", help="discover and fetch one live market snapshot")
    parser.add_argument("--duration", type=float, help="run realtime paper mode for N seconds")
    parser.add_argument("--replay", action="store_true", help="replay observations stored in SQLite")
    parser.add_argument("--replay-limit", type=int, help="limit replay observations")
    parser.add_argument("--database", help="override DATABASE_PATH")
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    config = load_config()
    if args.database:
        config = replace(config, database_path=args.database)
    config.assert_paper_only()
    print("MODE=PAPER (real trading is intentionally unavailable in V1)", flush=True)
    if args.replay:
        return run_replay(config, args.replay_limit)
    if args.once:
        return await discover_once(config)
    await run_live(config, args.duration)
    return 0


def main() -> None:
    args = parse_args()
    try:
        config = load_config()
        configure_logging(config.log_level)
        raise_code = asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("\nStopped; no real orders were possible.")
        raise_code = 0
    except (RuntimeError, httpx.HTTPError) as exc:
        logging.getLogger("paper-bot").error("%s", exc)
        raise_code = 1
    raise SystemExit(raise_code)


if __name__ == "__main__":
    main()
