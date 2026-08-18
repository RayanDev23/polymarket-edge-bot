"""Export one PAPER session as reproducible JSON statistics and CSV rows."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analytics import analyze  # noqa: E402
from config import load_config  # noqa: E402
from monitoring import (  # noqa: E402
    _json_safe,
    _metric,
    default_status_path,
    read_runtime_status,
    read_session_rows,
    resolve_session_id,
)


UTC = timezone.utc


def _safe_parameters(config: Any) -> dict[str, Any]:
    """Expose research parameters without URLs, environment data, or secrets."""

    return {
        "mode": "PAPER",
        "strategy": {
            "volatility_lookback": config.strategy.volatility_lookback,
            "minimum_volatility_observations": config.strategy.minimum_volatility_observations,
            "volatility_fallback_annualized": config.strategy.volatility_fallback_annualized,
            "annualization_seconds": config.strategy.annualization_seconds,
            "execution_buffer": config.strategy.execution_buffer,
            "sizing_capital": config.strategy.sizing_capital,
            "max_time_remaining_for_late_market_s": config.strategy.max_time_remaining_for_late_market_s,
        },
        "fees": {
            "rate": config.polymarket_taker_fee_rate,
            "exponent": config.polymarket_fee_exponent,
            "enabled": config.polymarket_fees_enabled,
        },
        "risk": {
            "starting_capital": config.risk.starting_capital,
            "max_capital_per_trade": config.risk.max_capital_per_trade,
            "max_simultaneous_exposure": config.risk.max_simultaneous_exposure,
            "max_daily_loss": config.risk.max_daily_loss,
            "max_consecutive_losses": config.risk.max_consecutive_losses,
            "minimum_net_edge": config.risk.minimum_net_edge,
            "minimum_liquidity": config.risk.minimum_liquidity,
            "maximum_data_age_ms": config.risk.maximum_data_age_ms,
            "maximum_execution_latency_ms": config.risk.maximum_execution_latency_ms,
            "market_data_recovery_observations": config.risk.market_data_recovery_observations,
        },
    }


def _duration_seconds(status: dict[str, Any], rows: list[dict[str, Any]]) -> float | None:
    started = status.get("started_at")
    ended = status.get("ended_at") or status.get("last_message_at")
    if isinstance(started, str) and isinstance(ended, str):
        try:
            start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(ended.replace("Z", "+00:00"))
            return max(0.0, (end_dt - start_dt).total_seconds())
        except ValueError:
            pass
    if len(rows) >= 2:
        try:
            first = datetime.fromisoformat(str(rows[0]["timestamp"]).replace("Z", "+00:00"))
            last = datetime.fromisoformat(str(rows[-1]["timestamp"]).replace("Z", "+00:00"))
            return max(0.0, (last - first).total_seconds())
        except (KeyError, TypeError, ValueError):
            pass
    return None


CSV_FIELDS = [
    "session_id", "timestamp", "market_id", "strategy", "side", "remaining_seconds",
    "best_up_bid", "best_up_ask", "best_down_bid", "best_down_ask", "combined_best_ask",
    "gross_edge_signed", "target_quantity", "matched_quantity", "average_up_price",
    "average_down_price", "combined_average_price", "execution_cost", "fees_total",
    "slippage_total", "execution_buffer", "net_edge", "capital_required",
    "available_up_liquidity", "available_down_liquidity", "price_to_beat", "spot_price",
    "realized_volatility", "volatility_observations", "model_probability", "late_gross_edge",
    "late_net_edge", "decision", "decision_reason", "late_rejection_reason",
]


def _csv_row(session_id: str, row: dict[str, Any]) -> dict[str, Any]:
    structural = _metric(row, "structural_arb") if row.get("strategy") == "STRUCTURAL_ARB" else {}
    late = {}
    try:
        features = json.loads(row.get("features_json") or "{}")
        analytics = features.get("analytics", {}) if isinstance(features, dict) else {}
        late = analytics.get("late_market", {}) if isinstance(analytics, dict) else {}
    except (TypeError, json.JSONDecodeError):
        pass
    values = {
        "session_id": session_id,
        "timestamp": row.get("timestamp"),
        "market_id": row.get("market"),
        "strategy": row.get("strategy"),
        "side": row.get("side"),
        "remaining_seconds": structural.get("remaining_seconds", row.get("time_remaining")),
        "best_up_bid": structural.get("best_up_bid"),
        "best_up_ask": structural.get("best_up_ask"),
        "best_down_bid": structural.get("best_down_bid"),
        "best_down_ask": structural.get("best_down_ask"),
        "combined_best_ask": structural.get("combined_best_ask"),
        "gross_edge_signed": structural.get("gross_edge_signed", row.get("gross_edge")),
        "target_quantity": structural.get("target_quantity"),
        "matched_quantity": structural.get("matched_quantity", row.get("quantity")),
        "average_up_price": structural.get("average_up_price"),
        "average_down_price": structural.get("average_down_price"),
        "combined_average_price": structural.get("combined_average_price"),
        "execution_cost": structural.get("execution_cost"),
        "fees_total": structural.get("fees_total", row.get("estimated_fees")),
        "slippage_total": structural.get("slippage_total", row.get("estimated_slippage")),
        "execution_buffer": structural.get("execution_buffer"),
        "net_edge": structural.get("net_edge", row.get("net_edge")),
        "capital_required": structural.get("capital_required", row.get("capital_required")),
        "available_up_liquidity": structural.get("available_up_liquidity"),
        "available_down_liquidity": structural.get("available_down_liquidity"),
        "price_to_beat": late.get("price_to_beat", row.get("price_to_beat")),
        "spot_price": late.get("spot_price", row.get("btc_price")),
        "realized_volatility": late.get("realized_volatility"),
        "volatility_observations": late.get("volatility_observations"),
        "model_probability": late.get("model_probability", row.get("model_probability")),
        "late_gross_edge": late.get("gross_edge"),
        "late_net_edge": late.get("net_edge"),
        "decision": row.get("decision"),
        "decision_reason": row.get("decision_reason"),
        "late_rejection_reason": late.get("rejection_reason"),
    }
    return _json_safe(values)


def export_session(
    database_path: str | Path = "paper_trading.sqlite3",
    session_id: str | None = None,
    output_dir: str | Path = "reports",
) -> tuple[Path, Path, dict[str, Any]]:
    database = Path(database_path)
    status_path = default_status_path(database)
    status = read_runtime_status(status_path)
    selected = resolve_session_id(database, status_path, session_id)
    if not selected:
        raise RuntimeError("No PAPER session_id was found")
    opportunities, trades = read_session_rows(database, selected)
    config = load_config()
    report = analyze(opportunities, trades).as_dict()
    payload = _json_safe(
        {
            "session_id": selected,
            "mode": "PAPER",
            "paper_only": True,
            "started_at": status.get("started_at"),
            "ended_at": status.get("ended_at"),
            "duration_seconds": _duration_seconds(status, opportunities),
            "parameters": _safe_parameters(config),
            "statistics": report,
            "row_counts": {
                "opportunities": len(opportunities),
                "paper_trades": len(trades),
            },
        }
    )
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    json_path = destination / f"paper_report_{stamp}.json"
    csv_path = destination / f"paper_report_{stamp}.csv"
    # Avoid overwriting a same-second report.
    if json_path.exists() or csv_path.exists():
        stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
        json_path = destination / f"paper_report_{stamp}.json"
        csv_path = destination / f"paper_report_{stamp}.csv"
    json_path.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in opportunities:
            writer.writerow(_csv_row(selected, row))
    return json_path, csv_path, payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a read-only PAPER session report")
    config = load_config()
    parser.add_argument("--database", default=str(config.database_path))
    parser.add_argument("--session-id")
    parser.add_argument("--output-dir", default="reports")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    json_path, csv_path, payload = export_session(args.database, args.session_id, args.output_dir)
    print(
        json.dumps(
            {
                "session_id": payload["session_id"],
                "opportunities": payload["row_counts"]["opportunities"],
                "paper_trades": payload["row_counts"]["paper_trades"],
                "json": str(json_path),
                "csv": str(csv_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
