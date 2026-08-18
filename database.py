"""Small SQLite persistence layer for observations, decisions, and paper fills."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from data import MarketTick, as_utc
from execution import PaperTrade
from market import OrderBook, PolymarketMarket, parse_datetime
from strategy import Opportunity


UTC = timezone.utc


def _iso(value: datetime | None) -> str | None:
    return as_utc(value).isoformat() if value else None


@dataclass(frozen=True)
class ReplayObservation:
    timestamp: datetime
    market: PolymarketMarket
    tick: MarketTick
    up_book: OrderBook | None
    down_book: OrderBook | None


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS market_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                market TEXT NOT NULL,
                btc_price REAL,
                btc_bid REAL,
                btc_ask REAL,
                btc_spread REAL,
                exchange_timestamp TEXT,
                data_age_ms REAL,
                price_to_beat REAL,
                time_remaining_s REAL,
                market_json TEXT NOT NULL,
                up_book_json TEXT,
                down_book_json TEXT
            );
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                market TEXT NOT NULL,
                token_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                bids_json TEXT NOT NULL,
                asks_json TEXT NOT NULL,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS opportunities (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                market TEXT NOT NULL,
                strategy TEXT NOT NULL,
                side TEXT NOT NULL,
                btc_price REAL,
                price_to_beat REAL,
                time_remaining REAL,
                executable_price REAL,
                executable_probability REAL,
                model_probability REAL,
                gross_edge REAL NOT NULL,
                estimated_fees REAL NOT NULL,
                estimated_slippage REAL NOT NULL,
                estimated_execution_risk REAL NOT NULL,
                net_edge REAL NOT NULL,
                available_liquidity REAL NOT NULL,
                signal_score REAL NOT NULL,
                decision TEXT NOT NULL,
                decision_reason TEXT NOT NULL,
                quantity REAL NOT NULL,
                capital_required REAL NOT NULL,
                features_json TEXT NOT NULL,
                up_token_id TEXT,
                down_token_id TEXT
            );
            CREATE TABLE IF NOT EXISTS paper_trades (
                id TEXT PRIMARY KEY,
                opportunity_id TEXT NOT NULL,
                entry_timestamp TEXT NOT NULL,
                market TEXT NOT NULL,
                strategy TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                fees REAL NOT NULL,
                slippage REAL NOT NULL,
                capital_required REAL NOT NULL,
                latency_ms REAL NOT NULL,
                legs_json TEXT NOT NULL,
                exit_timestamp TEXT,
                exit_price REAL,
                gross_pnl REAL,
                net_pnl REAL,
                status TEXT NOT NULL,
                failure_reason TEXT
            );
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT NOT NULL,
                market TEXT NOT NULL,
                token_id TEXT NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                status TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                closed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS daily_stats (
                day TEXT PRIMARY KEY,
                opportunities INTEGER NOT NULL DEFAULT 0,
                accepted INTEGER NOT NULL DEFAULT 0,
                trades INTEGER NOT NULL DEFAULT 0,
                gross_pnl REAL NOT NULL DEFAULT 0,
                net_pnl REAL NOT NULL DEFAULT 0,
                max_drawdown REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_observations_timestamp ON market_observations(timestamp);
            CREATE INDEX IF NOT EXISTS idx_observations_market ON market_observations(market);
            CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp ON orderbook_snapshots(timestamp);
            CREATE INDEX IF NOT EXISTS idx_orderbook_market ON orderbook_snapshots(market);
            CREATE INDEX IF NOT EXISTS idx_opportunities_timestamp ON opportunities(timestamp);
            CREATE INDEX IF NOT EXISTS idx_opportunities_market ON opportunities(market);
            CREATE INDEX IF NOT EXISTS idx_opportunities_strategy ON opportunities(strategy);
            CREATE INDEX IF NOT EXISTS idx_opportunities_decision ON opportunities(decision);
            CREATE INDEX IF NOT EXISTS idx_trades_entry_timestamp ON paper_trades(entry_timestamp);
            """
        )
        self.connection.commit()

    def insert_observation(
        self,
        market: PolymarketMarket,
        tick: MarketTick,
        books: dict[str, OrderBook],
        now: datetime | None = None,
    ) -> None:
        timestamp = as_utc(now or tick.local_timestamp)
        up_book = books.get(market.up_token_id)
        down_book = books.get(market.down_token_id)
        self.connection.execute(
            """
            INSERT INTO market_observations
            (timestamp, market, btc_price, btc_bid, btc_ask, btc_spread,
             exchange_timestamp, data_age_ms, price_to_beat, time_remaining_s,
             market_json, up_book_json, down_book_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _iso(timestamp),
                market.market_id,
                tick.price,
                tick.bid,
                tick.ask,
                tick.spread,
                _iso(tick.exchange_timestamp),
                tick.age_ms(timestamp),
                market.price_to_beat,
                market.remaining_seconds_at(timestamp),
                json.dumps(_market_to_json(market), sort_keys=True),
                json.dumps(up_book.to_dict(), sort_keys=True) if up_book else None,
                json.dumps(down_book.to_dict(), sort_keys=True) if down_book else None,
            ),
        )
        for outcome, book in (("UP", up_book), ("DOWN", down_book)):
            if book:
                self.connection.execute(
                    """
                    INSERT INTO orderbook_snapshots
                    (timestamp, market, token_id, outcome, bids_json, asks_json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _iso(timestamp),
                        market.market_id,
                        book.asset_id,
                        outcome,
                        json.dumps(book.to_dict()["bids"]),
                        json.dumps(book.to_dict()["asks"]),
                        _iso(book.updated_at),
                    ),
                )
        self.connection.commit()

    def insert_opportunity(self, opportunity: Opportunity) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO opportunities
            (id, timestamp, market, strategy, side, btc_price, price_to_beat,
             time_remaining, executable_price, executable_probability,
             model_probability, gross_edge, estimated_fees, estimated_slippage,
             estimated_execution_risk, net_edge, available_liquidity, signal_score,
             decision, decision_reason, quantity, capital_required, features_json,
             up_token_id, down_token_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                opportunity.id,
                _iso(opportunity.timestamp),
                opportunity.market,
                opportunity.strategy,
                opportunity.side,
                opportunity.btc_price,
                opportunity.price_to_beat,
                opportunity.time_remaining,
                opportunity.executable_price,
                opportunity.executable_probability,
                opportunity.model_probability,
                opportunity.gross_edge,
                opportunity.estimated_fees,
                opportunity.estimated_slippage,
                opportunity.estimated_execution_risk,
                opportunity.net_edge,
                opportunity.available_liquidity,
                opportunity.signal_score,
                opportunity.decision,
                opportunity.decision_reason,
                opportunity.quantity,
                opportunity.capital_required,
                json.dumps(opportunity.features, sort_keys=True),
                opportunity.up_token_id,
                opportunity.down_token_id,
            ),
        )
        self.connection.commit()

    def insert_trade(self, trade: PaperTrade) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO paper_trades
            (id, opportunity_id, entry_timestamp, market, strategy, side, quantity,
             entry_price, fees, slippage, capital_required, latency_ms, legs_json,
             exit_timestamp, exit_price, gross_pnl, net_pnl, status, failure_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade.id,
                trade.opportunity_id,
                _iso(trade.entry_timestamp),
                trade.market,
                trade.strategy,
                trade.side,
                trade.quantity,
                trade.entry_price,
                trade.fees,
                trade.slippage,
                trade.capital_required,
                trade.latency_ms,
                json.dumps([leg.__dict__ for leg in trade.legs], sort_keys=True),
                _iso(trade.exit_timestamp),
                trade.exit_price,
                trade.gross_pnl,
                trade.net_pnl,
                trade.status,
                trade.failure_reason,
            ),
        )
        self.connection.commit()

    def insert_positions(self, trade: PaperTrade) -> None:
        for leg in trade.legs:
            self.connection.execute(
                """
                INSERT INTO positions
                (trade_id, market, token_id, quantity, entry_price, status, opened_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.id,
                    trade.market,
                    leg.token_id,
                    leg.filled_quantity,
                    leg.entry_price,
                    trade.status,
                    _iso(trade.entry_timestamp),
                    _iso(trade.exit_timestamp),
                ),
            )
        self.connection.commit()

    def update_positions_for_trade(self, trade: PaperTrade) -> None:
        """Keep position rows synchronized when a paper trade is settled."""

        self.connection.execute(
            """
            UPDATE positions
            SET status = ?, closed_at = ?
            WHERE trade_id = ?
            """,
            (trade.status, _iso(trade.exit_timestamp), trade.id),
        )
        self.connection.commit()

    def fetch_opportunities(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM opportunities ORDER BY timestamp")]

    def fetch_trades(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM paper_trades ORDER BY entry_timestamp")]

    def replay_observations(self, limit: int | None = None) -> Iterator[ReplayObservation]:
        query = "SELECT * FROM market_observations ORDER BY timestamp"
        if limit is not None:
            query += " LIMIT ?"
            rows = self.connection.execute(query, (limit,))
        else:
            rows = self.connection.execute(query)
        for row in rows:
            raw_market = json.loads(row["market_json"])
            market = _market_from_json(raw_market)
            tick_timestamp = parse_datetime(row["timestamp"]) or datetime.now(UTC)
            exchange_timestamp = parse_datetime(row["exchange_timestamp"])
            bid = float(row["btc_bid"] or row["btc_price"] or 0.0)
            ask = float(row["btc_ask"] or row["btc_price"] or 0.0)
            tick = MarketTick(
                local_timestamp=tick_timestamp,
                exchange_timestamp=exchange_timestamp,
                symbol="BTCUSDT",
                price=float(row["btc_price"] or 0.0),
                bid=bid,
                ask=ask,
                bid_quantity=0.0,
                ask_quantity=0.0,
                spread=float(row["btc_spread"] or max(0.0, ask - bid)),
                latency_ms=None,
            )
            up_book = OrderBook.from_dict(json.loads(row["up_book_json"])) if row["up_book_json"] else None
            down_book = OrderBook.from_dict(json.loads(row["down_book_json"])) if row["down_book_json"] else None
            yield ReplayObservation(tick_timestamp, market, tick, up_book, down_book)

    def prune_before(self, cutoff: datetime) -> None:
        value = _iso(cutoff)
        for table, column in (
            ("market_observations", "timestamp"),
            ("orderbook_snapshots", "timestamp"),
            ("opportunities", "timestamp"),
            ("paper_trades", "entry_timestamp"),
        ):
            self.connection.execute(f"DELETE FROM {table} WHERE {column} < ?", (value,))
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _market_to_json(market: PolymarketMarket) -> dict[str, Any]:
    return {
        "market_id": market.market_id,
        "condition_id": market.condition_id,
        "question": market.question,
        "slug": market.slug,
        "up_token_id": market.up_token_id,
        "down_token_id": market.down_token_id,
        "start_time": _iso(market.start_time),
        "end_time": _iso(market.end_time),
        "resolution_source": market.resolution_source,
        "price_to_beat": market.price_to_beat,
        "active": market.active,
        "closed": market.closed,
        "accepting_orders": market.accepting_orders,
        "fees_enabled": market.fees_enabled,
        "fee_rate": market.fee_rate,
        "fee_exponent": market.fee_exponent,
        "raw": market.raw,
    }


def _market_from_json(raw: dict[str, Any]) -> PolymarketMarket:
    return PolymarketMarket(
        market_id=raw["market_id"],
        condition_id=raw["condition_id"],
        question=raw.get("question", ""),
        slug=raw.get("slug", ""),
        up_token_id=raw["up_token_id"],
        down_token_id=raw["down_token_id"],
        start_time=parse_datetime(raw.get("start_time")),
        end_time=parse_datetime(raw["end_time"]) or datetime.now(UTC),
        resolution_source=raw.get("resolution_source"),
        price_to_beat=raw.get("price_to_beat"),
        active=bool(raw.get("active", True)),
        closed=bool(raw.get("closed", False)),
        accepting_orders=raw.get("accepting_orders"),
        fees_enabled=raw.get("fees_enabled"),
        fee_rate=raw.get("fee_rate"),
        fee_exponent=raw.get("fee_exponent"),
        raw=raw.get("raw", {}),
    )
