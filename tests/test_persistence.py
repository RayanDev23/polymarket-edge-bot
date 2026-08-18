import json
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


def test_strategy_analytics_report_reads_and_deduplicates_persisted_metrics() -> None:
    late_metrics = {
        "evaluation_id": "late-1",
        "price_to_beat": None,
        "spot_price": 100.0,
        "realized_volatility": None,
        "volatility_observations": 2,
        "model_probability": None,
        "candidate_up": None,
        "candidate_down": None,
        "gross_edge": None,
        "net_edge": None,
        "decision": None,
        "rejection_reason": "price_to_beat_missing",
        "price_to_beat_available": False,
        "enough_volatility_observations": False,
        "probability_calculated": False,
        "candidate_signal": False,
    }
    structural_metrics = {
        "combined_best_ask": 0.98,
        "gross_edge_signed": 0.02,
        "net_edge": 0.01,
        "fees_total": 0.2,
        "slippage_total": 0.1,
        "remaining_seconds": 40.0,
        "decision": "REJECT",
        "decision_reason": "insufficient_edge",
    }
    late_candidate = dict(late_metrics)
    late_candidate.update(
        {
            "decision": "REJECT",
            "rejection_reason": "insufficient_edge",
            "price_to_beat": 100.0,
            "price_to_beat_available": True,
            "probability_calculated": True,
            "candidate_signal": True,
            "model_probability": 0.6,
            "gross_edge": 0.02,
            "net_edge": 0.01,
        }
    )
    structural = {
        "id": "structural-1",
        "strategy": "STRUCTURAL_ARB",
        "decision": "REJECT",
        "features_json": json.dumps(
            {"analytics": {"structural_arb": structural_metrics, "late_market": late_metrics}}
        ),
    }
    late = {
        "id": "late-1",
        "strategy": "LATE_MARKET",
        "decision": "REJECT",
        "features_json": json.dumps({"analytics": {"late_market": late_candidate}}),
    }

    strategy_report = analyze([structural, late], []).as_dict()["strategy_analytics"]
    assert strategy_report["structural_arb"]["counters"]["total_evaluations"] == 1
    assert strategy_report["structural_arb"]["counters"]["combined_ask_lt_0.99"] == 1
    assert strategy_report["structural_arb"]["counters"]["ACCEPT"] == 0
    assert strategy_report["late_market"]["counters"]["evaluations"] == 1
    assert strategy_report["late_market"]["counters"]["price_to_beat_available"] == 1
    assert strategy_report["late_market"]["counters"]["probability_calculated"] == 1
    assert strategy_report["late_market"]["counters"]["candidate_signal"] == 1
    assert strategy_report["late_market"]["counters"]["ACCEPT"] == 0
    assert strategy_report["late_market"]["distributions"]["model_probability"]["median"] == 0.6
