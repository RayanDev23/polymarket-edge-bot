"""Read-only PAPER monitoring helpers.

This module deliberately contains no trading controls.  It reads persisted
decisions and a small, non-sensitive runtime status file for local dashboards
and research exports.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from analytics import analyze


def default_status_path(database_path: str | Path) -> Path:
    return Path(database_path).expanduser().with_name("paper_status.json")


def _json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def write_runtime_status(path: str | Path, payload: dict[str, Any]) -> None:
    """Atomically publish a non-sensitive status snapshot for local readers."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            json.dump(_json_safe(payload), handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        os.replace(temporary, destination)
        temporary = None
    finally:
        if temporary:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def read_runtime_status(path: str | Path) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_only_connection(path: str | Path) -> sqlite3.Connection | None:
    database = Path(path).expanduser()
    if not database.exists():
        return None
    resolved = database.resolve().as_posix()
    uri = f"file:///{resolved}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _has_column(connection: sqlite3.Connection, table: str, column: str) -> bool:
    return any(
        row[1] == column
        for row in connection.execute(f"PRAGMA table_info({table})")
    )


def latest_session_id(database_path: str | Path) -> str | None:
    connection = _read_only_connection(database_path)
    if connection is None:
        return None
    try:
        candidates: list[tuple[str, str]] = []
        if _has_column(connection, "opportunities", "session_id"):
            row = connection.execute(
                """SELECT session_id, timestamp FROM opportunities
                   WHERE session_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1"""
            ).fetchone()
            if row:
                candidates.append((str(row["timestamp"]), str(row["session_id"])))
        if _has_column(connection, "market_observations", "session_id"):
            row = connection.execute(
                """SELECT session_id, timestamp FROM market_observations
                   WHERE session_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1"""
            ).fetchone()
            if row:
                candidates.append((str(row["timestamp"]), str(row["session_id"])))
        return max(candidates)[1] if candidates else None
    finally:
        connection.close()


def resolve_session_id(
    database_path: str | Path,
    status_path: str | Path | None = None,
    requested: str | None = None,
) -> str | None:
    if requested:
        return requested
    status = read_runtime_status(status_path or default_status_path(database_path))
    status_session = status.get("session_id")
    if isinstance(status_session, str) and status_session:
        return status_session
    return latest_session_id(database_path)


def read_session_rows(
    database_path: str | Path,
    session_id: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read only one session; legacy rows without a session are excluded."""

    connection = _read_only_connection(database_path)
    if connection is None or not session_id:
        if connection is not None:
            connection.close()
        return [], []
    try:
        opportunities: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        if _has_column(connection, "opportunities", "session_id"):
            opportunities = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM opportunities WHERE session_id = ? ORDER BY timestamp",
                    (session_id,),
                )
            ]
        if _has_column(connection, "paper_trades", "session_id"):
            trades = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM paper_trades WHERE session_id = ? ORDER BY entry_timestamp",
                    (session_id,),
                )
            ]
        return opportunities, trades
    finally:
        connection.close()


def _metric(row: dict[str, Any], name: str) -> dict[str, Any]:
    try:
        features = json.loads(row.get("features_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        features = {}
    analytics = features.get("analytics", {}) if isinstance(features, dict) else {}
    if not isinstance(analytics, dict):
        return {}
    strategy = row.get("strategy")
    key = "structural_arb" if strategy == "STRUCTURAL_ARB" else "late_market"
    value = analytics.get(key)
    return value if isinstance(value, dict) else {}


def _recent_observations(rows: list[dict[str, Any]], limit: int = 50) -> list[dict[str, Any]]:
    recent: list[dict[str, Any]] = []
    for row in rows[-limit:]:
        metric = _metric(row, "analytics")
        recent.append(
            {
                "timestamp": row.get("timestamp"),
                "remaining": metric.get("remaining_seconds", row.get("time_remaining")),
                "combined_ask": metric.get("combined_best_ask"),
                "gross_edge": metric.get("gross_edge_signed", row.get("gross_edge")),
                "net_edge": metric.get("net_edge", row.get("net_edge")),
                "fees": metric.get("fees_total", row.get("estimated_fees")),
                "slippage": metric.get("slippage_total", row.get("estimated_slippage")),
                "decision": row.get("decision"),
                "reason": row.get("decision_reason"),
                "strategy": row.get("strategy"),
            }
        )
    return recent


def _rejection_analytics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(str(row.get("decision_reason") or "unknown") for row in rows)
    total = len(rows)
    return [
        {
            "reason": reason,
            "count": count,
            "percentage": (count / total * 100.0) if total else 0.0,
        }
        for reason, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def build_dashboard_payload(
    database_path: str | Path,
    status_path: str | Path | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    status_file = Path(status_path or default_status_path(database_path))
    status = read_runtime_status(status_file)
    selected_session = resolve_session_id(database_path, status_file, session_id)
    opportunities, trades = read_session_rows(database_path, selected_session)
    report = analyze(opportunities, trades).as_dict()
    strategy_report = report.get("strategy_analytics", {})
    return _json_safe(
        {
            "mode": "PAPER",
            "paper_only": True,
            "session_id": selected_session,
            "status": status,
            "analytics": strategy_report,
            "summary": {
                "total_opportunities": report.get("total_opportunities", 0),
                "accepted_opportunities": report.get("accepted_opportunities", 0),
                "rejected_opportunities": report.get("rejected_opportunities", 0),
                "paper_trades": report.get("trades", 0),
                "net_pnl": report.get("net_pnl", 0.0),
            },
            "rejections": _rejection_analytics(opportunities),
            "recent_observations": _recent_observations(opportunities),
        }
    )

