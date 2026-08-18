from datetime import datetime, timedelta, timezone

from analytics import analyze
from database import Database
from data import MarketTick
from execution import PaperExecutor
from market import OrderBook, OrderBookLevel, PolymarketMarket
from strategy import FeeModel, Opportunity


UTC = timezone.utc


def make_market(now: datetime) -> PolymarketMarket:
    return PolymarketMarket(
        "market-db",
        "condition-db",
        "Bitcoin Up or Down 5m",
        "btc-updown-5m-db",
        "up-db",
        "down-db",
        now - timedelta(minutes=4),
        now + timedelta(minutes=1),
        None,
        100.0,
        True,
        False,
        True,
        True,
        0.07,
        None,
        {"question": "Bitcoin Up or Down 5m"},
    )


def make_opportunity(timestamp: datetime, decision: str, reason: str, net_edge: float) -> Opportunity:
    return Opportunity(
        id=f"opp-{decision}-{reason}",
        timestamp=timestamp,
        market="market-db",
        strategy="TEST",
        side="BUY_UP",
        btc_price=100.0,
        price_to_beat=100.0,
        time_remaining=45.0,
        executable_price=0.4,
        executable_probability=0.4,
        model_probability=0.8,
        gross_edge=net_edge,
        estimated_fees=0.1,
        estimated_slippage=0.01,
        estimated_execution_risk=0.0,
        net_edge=net_edge,
        available_liquidity=20.0,
        signal_score=net_edge,
        decision=decision,
        decision_reason=reason,
        quantity=5.0,
        capital_required=2.0,
        up_token_id="up-db",
        down_token_id="down-db",
    )


def test_sqlite_roundtrip_and_replay(tmp_path) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    market = make_market(now)
    tick = MarketTick(now, now, "BTCUSDT", 100.0, 99.9, 100.1, 1, 1, 0.2, 2)
    books = {
        "up-db": OrderBook("up-db", [OrderBookLevel(0.39, 10)], [OrderBookLevel(0.4, 10)], now),
        "down-db": OrderBook("down-db", [OrderBookLevel(0.39, 10)], [OrderBookLevel(0.4, 10)], now),
    }
    with Database(tmp_path / "test.sqlite3") as database:
        database.insert_observation(market, tick, books, now)
        accepted = make_opportunity(now, "ACCEPT", "risk_checks_passed", 0.2)
        rejected = make_opportunity(now + timedelta(seconds=1), "REJECT", "stale_data", 0.2)
        database.insert_opportunity(accepted)
        database.insert_opportunity(rejected)
        trade = PaperExecutor(mode="PAPER").execute(accepted, books, now)
        database.insert_trade(trade)
        assert len(database.fetch_opportunities()) == 2
        assert len(database.fetch_trades()) == 1
        replay = list(database.replay_observations())
        assert len(replay) == 1
        assert replay[0].market.up_token_id == "up-db"
        assert replay[0].up_book.best_ask.price == 0.4

        trade.exit_timestamp = now + timedelta(minutes=1)
        trade.status = "CLOSED"
        database.insert_positions(trade)
        database.update_positions_for_trade(trade)
        position = database.connection.execute(
            "SELECT status, closed_at FROM positions WHERE trade_id = ?", (trade.id,)
        ).fetchone()
        assert position["status"] == "CLOSED"
        assert position["closed_at"] is not None


def test_analytics_reports_rejected_opportunities_and_drawdown() -> None:
    opportunities = [
        {"id": "a", "decision": "ACCEPT", "decision_reason": "ok", "strategy": "ARB", "net_edge": 0.02, "available_liquidity": 20, "time_remaining": 20},
        {"id": "b", "decision": "REJECT", "decision_reason": "insufficient_edge", "strategy": "ARB", "net_edge": 0.005, "available_liquidity": 2, "time_remaining": 80},
    ]
    trades = [
        {"opportunity_id": "a", "status": "CLOSED", "gross_pnl": 4.0, "net_pnl": 3.0, "entry_timestamp": "2026-01-01T00:00:00+00:00"},
        {"opportunity_id": "a", "status": "CLOSED", "gross_pnl": -5.0, "net_pnl": -4.0, "entry_timestamp": "2026-01-01T01:00:00+00:00"},
    ]
    report = analyze(opportunities, trades).as_dict()
    assert report["total_opportunities"] == 2
    assert report["rejected_opportunities"] == 1
    assert report["net_pnl"] == -1.0
    assert report["max_drawdown"] == 4.0
    assert report["not_taken"]["by_reason"]["insufficient_edge"] == 1
    assert report["pnl_by_edge_bucket"]["<0.05"] == -1.0
