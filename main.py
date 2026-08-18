"""Entry point for the V1 paper-only research loop."""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from analytics import analyze
from config import AppConfig, load_config
from data import BinanceSpotFeed, MarketDataStore, MarketTick, utc_now
from database import Database
from execution import PaperExecutor, PaperTrade
from market import OrderBook, PolymarketBookFeed, PolymarketClient, PolymarketMarket
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


def _settle_matured_trades(
    open_trades: list[PaperTrade],
    market: PolymarketMarket,
    resolved_outcome: str | None,
    timestamp: datetime,
    database: Database,
    risk: RiskEngine,
    logger: logging.Logger,
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
        database.insert_trade(trade)
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
) -> None:
    decision_time = utc_now()
    processing_started = time.perf_counter()
    data_age = tick.age_ms(decision_time)
    data_store.history(decision_time)  # makes the anti-lookahead boundary explicit
    database.insert_observation(market, tick, books, decision_time)
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

    opportunities = strategy.evaluate(
        market,
        tick,
        books,
        data_store.history(decision_time),
        decision_time,
    )
    for opportunity in opportunities:
        orderbook_coherent = all(
            books.get(token_id) is not None and books[token_id].coherent()
            for token_id in market.token_ids
        )
        fresh_books = all(
            books.get(token_id) is not None
            and books[token_id].age_ms(decision_time) <= risk.config.maximum_data_age_ms
            for token_id in market.token_ids
        )
        decision = risk.evaluate(
            opportunity,
            data_age_ms=max(data_age, *(books[token_id].age_ms(decision_time) for token_id in market.token_ids if token_id in books)),
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
        opportunity.decision = "ACCEPT" if decision.accepted else "REJECT"
        opportunity.decision_reason = decision.reason
        database.insert_opportunity(opportunity)
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
        database.insert_trade(trade)
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

        async def asset_provider() -> list[str]:
            market = state["market"]
            return list(market.token_ids) if market else []

        async def discover_loop() -> None:
            nonlocal strategy
            while not stop.is_set():
                try:
                    market = await client.discovery.discover_btc_5m()
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
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception("[MARKET] discovery failed: %s", exc)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=config.market_poll_seconds)
                except asyncio.TimeoutError:
                    pass

        async def on_tick(tick: MarketTick) -> None:
            market = state["market"]
            if not market or not strategy:
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
            )

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
            report = analyze(database.fetch_opportunities(), database.fetch_trades())
            logger.info("[ANALYTICS] %s", report.as_dict())
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
