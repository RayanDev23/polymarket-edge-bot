"""Performance analytics, including the opportunities that were rejected."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable


@dataclass(frozen=True)
class AnalyticsReport:
    metrics: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return self.metrics


def _value(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _closed_pnls(trades: Iterable[Any]) -> list[float]:
    return [
        float(pnl)
        for trade in trades
        if (pnl := _value(trade, "net_pnl")) is not None
        and _value(trade, "status", "") == "CLOSED"
    ]


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def _profit_factor(pnls: list[float]) -> float:
    wins = sum(pnl for pnl in pnls if pnl > 0)
    losses = abs(sum(pnl for pnl in pnls if pnl < 0))
    if losses == 0:
        return math.inf if wins > 0 else 0.0
    return wins / losses


def _sharpe_approx(pnls: list[float]) -> float | None:
    if len(pnls) < 2:
        return None
    deviation = statistics.stdev(pnls)
    if deviation == 0:
        return None
    return statistics.mean(pnls) / deviation * math.sqrt(len(pnls))


def _bucket(value: float | None, boundaries: tuple[float, ...]) -> str:
    if value is None:
        return "unknown"
    for boundary in boundaries:
        if value < boundary:
            return f"<{boundary:g}"
    return f">={boundaries[-1]:g}"


def _pnl_by(
    trades: Iterable[Any],
    key_fn: Any,
) -> dict[str, float]:
    result: defaultdict[str, float] = defaultdict(float)
    for trade in trades:
        if _value(trade, "status", "") != "CLOSED" or _value(trade, "net_pnl") is None:
            continue
        result[str(key_fn(trade))] += float(_value(trade, "net_pnl"))
    return dict(sorted(result.items()))


def analyze(opportunities: Iterable[Any], trades: Iterable[Any]) -> AnalyticsReport:
    opportunities = list(opportunities)
    raw_trades = list(trades)
    opportunity_by_id = {
        str(_value(item, "id")): item for item in opportunities if _value(item, "id") is not None
    }
    # Paper-trade rows intentionally stay compact. Enrich them in memory with
    # the opportunity fields needed for edge/liquidity/time buckets.
    trades: list[Any] = []
    for trade in raw_trades:
        enriched = dict(trade) if isinstance(trade, dict) else dict(vars(trade))
        source = opportunity_by_id.get(str(enriched.get("opportunity_id")))
        for field in ("net_edge", "available_liquidity", "time_remaining"):
            if field not in enriched and source is not None:
                enriched[field] = _value(source, field)
        trades.append(enriched)
    pnls = _closed_pnls(trades)
    accepted = [item for item in opportunities if _value(item, "decision") == "ACCEPT"]
    rejected = [item for item in opportunities if _value(item, "decision") != "ACCEPT"]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    entry_times = [
        _value(trade, "entry_timestamp")
        for trade in trades
        if _value(trade, "status", "") == "CLOSED"
    ]

    def timestamp_hour(trade: Any) -> str:
        value = _value(trade, "entry_timestamp")
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return "unknown"
        return f"{value.hour:02d}" if isinstance(value, datetime) else "unknown"

    metrics: dict[str, Any] = {
        "total_opportunities": len(opportunities),
        "accepted_opportunities": len(accepted),
        "rejected_opportunities": len(rejected),
        "trades": len([trade for trade in trades if _value(trade, "status", "") != "FAILED"]),
        "closed_trades": len(pnls),
        "win_rate": (len(wins) / len(pnls)) if pnls else None,
        "gross_pnl": sum(
            float(_value(trade, "gross_pnl"))
            for trade in trades
            if _value(trade, "status", "") == "CLOSED" and _value(trade, "gross_pnl") is not None
        ),
        "net_pnl": sum(pnls),
        "average_trade": statistics.mean(pnls) if pnls else None,
        "median_trade": statistics.median(pnls) if pnls else None,
        "profit_factor": _profit_factor(pnls),
        "max_drawdown": _max_drawdown(pnls),
        "sharpe_approx": _sharpe_approx(pnls),
        "pnl_by_strategy": _pnl_by(trades, lambda item: _value(item, "strategy", "unknown")),
        "pnl_by_hour": _pnl_by(trades, timestamp_hour),
        "pnl_by_time_to_expiration": _pnl_by(
            trades,
            lambda item: _bucket(_value(item, "time_remaining"), (30.0, 60.0, 120.0, 300.0)),
        ),
        "pnl_by_edge_bucket": _pnl_by(
            trades,
            lambda item: _bucket(_value(item, "net_edge"), (0.005, 0.01, 0.02, 0.05)),
        ),
        "pnl_by_liquidity_bucket": _pnl_by(
            trades,
            lambda item: _bucket(_value(item, "available_liquidity"), (5.0, 10.0, 50.0, 100.0)),
        ),
        "not_taken": {
            "count": len(rejected),
            "average_rejected_net_edge": (
                statistics.mean(float(_value(item, "net_edge", 0.0)) for item in rejected)
                if rejected
                else None
            ),
            "by_reason": _counts(rejected, lambda item: _value(item, "decision_reason", "unknown")),
            "by_strategy": _counts(rejected, lambda item: _value(item, "strategy", "unknown")),
        },
    }
    return AnalyticsReport(metrics)


def _counts(items: Iterable[Any], key_fn: Any) -> dict[str, int]:
    result: defaultdict[str, int] = defaultdict(int)
    for item in items:
        result[str(key_fn(item))] += 1
    return dict(sorted(result.items()))
