import csv
import json
import threading
from datetime import datetime, timezone
from http.client import HTTPConnection

from dashboard import DashboardServer
from database import Database
from monitoring import build_dashboard_payload, write_runtime_status
from scripts.export_paper_report import export_session
from strategy import Opportunity


UTC = timezone.utc


def opportunity(timestamp: datetime, identifier: str, session: str, decision: str) -> Opportunity:
    structural = {
        "timestamp": timestamp.isoformat(),
        "market_id": "market-1",
        "remaining_seconds": 42.0,
        "best_up_bid": 0.39,
        "best_up_ask": 0.40,
        "best_down_bid": 0.39,
        "best_down_ask": 0.41,
        "combined_best_ask": 0.81,
        "gross_edge_signed": 0.19,
        "target_quantity": 5.0,
        "matched_quantity": 5.0,
        "average_up_price": 0.40,
        "average_down_price": 0.41,
        "combined_average_price": 0.81,
        "execution_cost": 4.05,
        "fees_total": 0.02,
        "slippage_total": 0.01,
        "execution_buffer": 0.005,
        "net_edge": 0.18,
        "capital_required": 4.07,
        "available_up_liquidity": 10.0,
        "available_down_liquidity": 10.0,
        "decision": decision,
        "decision_reason": "risk_checks_passed" if decision == "ACCEPT" else "insufficient_edge",
    }
    late = {
        "evaluation_id": f"late-{identifier}",
        "price_to_beat": 100.0,
        "spot_price": 101.0,
        "realized_volatility": 0.5,
        "volatility_observations": 10,
        "model_probability": 0.6,
        "candidate_up": {"net_edge": 0.1},
        "candidate_down": None,
        "gross_edge": 0.2,
        "net_edge": 0.1,
        "decision": decision,
        "rejection_reason": None if decision == "ACCEPT" else "insufficient_edge",
        "price_to_beat_available": True,
        "enough_volatility_observations": True,
        "probability_calculated": True,
        "candidate_signal": True,
    }
    return Opportunity(
        id=identifier,
        timestamp=timestamp,
        market="market-1",
        strategy="STRUCTURAL_ARB",
        side="BUY_UP_AND_DOWN",
        btc_price=101.0,
        price_to_beat=100.0,
        time_remaining=42.0,
        executable_price=0.81,
        executable_probability=0.81,
        model_probability=None,
        gross_edge=0.19,
        estimated_fees=0.02,
        estimated_slippage=0.01,
        estimated_execution_risk=0.0,
        net_edge=0.18,
        available_liquidity=10.0,
        signal_score=0.18,
        decision=decision,
        decision_reason=structural["decision_reason"],
        quantity=5.0,
        capital_required=4.07,
        features={"analytics": {"structural_arb": structural, "late_market": late}},
    )


def test_dashboard_is_session_filtered_and_endpoint_is_read_only(tmp_path) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    database_path = tmp_path / "paper.sqlite3"
    with Database(database_path) as database:
        database.insert_opportunity(opportunity(now, "one", "session-one", "REJECT"), "session-one")
        database.insert_opportunity(opportunity(now, "two", "session-two", "ACCEPT"), "session-two")
    status_path = tmp_path / "paper_status.json"
    write_runtime_status(
        status_path,
        {"session_id": "session-one", "mode": "PAPER", "paper_only": True},
    )

    payload = build_dashboard_payload(database_path, status_path)
    assert payload["paper_only"] is True
    assert payload["session_id"] == "session-one"
    assert payload["analytics"]["structural_arb"]["counters"]["total_evaluations"] == 1
    assert payload["summary"]["accepted_opportunities"] == 0

    server = DashboardServer(("127.0.0.1", 0), database_path, status_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        connection.request("GET", "/api/state")
        response = connection.getresponse()
        body = json.loads(response.read())
        assert response.status == 200
        assert body["session_id"] == "session-one"
        connection.request("POST", "/api/state")
        assert connection.getresponse().status == 405
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_export_contains_statistics_and_usable_csv(tmp_path) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    database_path = tmp_path / "paper.sqlite3"
    with Database(database_path) as database:
        database.insert_opportunity(opportunity(now, "one", "export-session", "REJECT"), "export-session")
    status_path = tmp_path / "paper_status.json"
    write_runtime_status(
        status_path,
        {
            "session_id": "export-session",
            "started_at": now.isoformat(),
            "ended_at": now.isoformat(),
            "mode": "PAPER",
            "paper_only": True,
        },
    )
    json_path, csv_path, payload = export_session(
        database_path,
        session_id="export-session",
        output_dir=tmp_path / "reports",
    )
    saved = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved["session_id"] == "export-session"
    assert saved["parameters"]["mode"] == "PAPER"
    assert saved["statistics"]["strategy_analytics"]["structural_arb"]["counters"]["REJECT"] == 1
    assert payload["row_counts"]["opportunities"] == 1
    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["session_id"] == "export-session"
    assert rows[0]["combined_best_ask"] == "0.81"
