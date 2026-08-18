"""Performance analytics, including the opportunities that were rejected."""

from __future__ import annotations

import math
import json
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


def _features(item: Any) -> dict[str, Any]:
    raw = _value(item, "features")
    if isinstance(raw, dict):
        return raw
    raw = _value(item, "features_json")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _strategy_metric(item: Any, name: str) -> dict[str, Any] | None:
    analytics = _features(item).get("analytics")
    if not isinstance(analytics, dict):
        return None
    metric = analytics.get(name)
    return metric if isinstance(metric, dict) else None


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _distribution(values: Iterable[Any]) -> dict[str, float | int | None]:
    ordered = sorted(
        value
        for raw in values
        if (value := _finite_float(raw)) is not None
    )
    if not ordered:
        return {"count": 0, "min": None, "p5": None, "median": None, "p95": None, "max": None}

    def percentile(fraction: float) -> float:
        position = (len(ordered) - 1) * fraction
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return ordered[lower]
        weight = position - lower
        return ordered[lower] + (ordered[upper] - ordered[lower]) * weight

    return {
        "count": len(ordered),
        "min": ordered[0],
        "p5": percentile(0.05),
        "median": percentile(0.50),
        "p95": percentile(0.95),
        "max": ordered[-1],
    }


def _thresholds(
    values: Iterable[Any],
    total: int,
    boundaries: tuple[tuple[str, float], ...],
) -> dict[str, dict[str, float | int]]:
    numeric = [value for raw in values if (value := _finite_float(raw)) is not None]
    return {
        label: {
            "count": sum(value < boundary for value in numeric),
            "percentage": (
                100.0 * sum(value < boundary for value in numeric) / total
                if total
                else 0.0
            ),
        }
        for label, boundary in boundaries
    }


def analyze_strategy_observations(opportunities: Iterable[Any]) -> dict[str, Any]:
    """Summarize persisted strategy instrumentation without affecting decisions."""

    items = list(opportunities)
    structural_rows: list[tuple[Any, dict[str, Any]]] = []
    for item in items:
        if _value(item, "strategy") != "STRUCTURAL_ARB":
            continue
        structural_rows.append((item, _strategy_metric(item, "structural_arb") or {}))

    structural_metrics = [metric for _, metric in structural_rows]
    combined_asks = [metric.get("combined_best_ask") for metric in structural_metrics]
    gross_edges = [metric.get("gross_edge_signed") for metric in structural_metrics]
    net_edges = [metric.get("net_edge") for metric in structural_metrics]
    fees = [metric.get("fees_total") for metric in structural_metrics]
    slippage = [metric.get("slippage_total") for metric in structural_metrics]
    remaining = [metric.get("remaining_seconds") for metric in structural_metrics]

    def decision_for(item: Any, metric: dict[str, Any]) -> str | None:
        return metric.get("decision") or _value(item, "decision")

    structural_total = len(structural_rows)
    structural_counters = {
        "total_evaluations": structural_total,
        "combined_ask_lt_1.00": sum(
            value is not None and value < 1.0
            for value in (_finite_float(raw) for raw in combined_asks)
        ),
        "combined_ask_lt_0.995": sum(
            value is not None and value < 0.995
            for value in (_finite_float(raw) for raw in combined_asks)
        ),
        "combined_ask_lt_0.99": sum(
            value is not None and value < 0.99
            for value in (_finite_float(raw) for raw in combined_asks)
        ),
        "combined_ask_lt_0.98": sum(
            value is not None and value < 0.98
            for value in (_finite_float(raw) for raw in combined_asks)
        ),
        "gross_edge_gt_0": sum(
            value is not None and value > 0.0
            for value in (_finite_float(raw) for raw in gross_edges)
        ),
        "net_edge_gt_0": sum(
            value is not None and value > 0.0
            for value in (_finite_float(raw) for raw in net_edges)
        ),
        "net_edge_gt_0.001": sum(
            value is not None and value > 0.001
            for value in (_finite_float(raw) for raw in net_edges)
        ),
        "net_edge_gt_0.005": sum(
            value is not None and value > 0.005
            for value in (_finite_float(raw) for raw in net_edges)
        ),
        "net_edge_gt_0.01": sum(
            value is not None and value > 0.01
            for value in (_finite_float(raw) for raw in net_edges)
        ),
        "ACCEPT": sum(decision_for(item, metric) == "ACCEPT" for item, metric in structural_rows),
        "REJECT": sum(decision_for(item, metric) == "REJECT" for item, metric in structural_rows),
    }

    late_records: dict[str, dict[str, Any]] = {}
    for item in items:
        metric = _strategy_metric(item, "late_market")
        if not metric:
            continue
        evaluation_id = str(
            metric.get("evaluation_id")
            or f"{_value(item, 'market', '')}|{_value(item, 'timestamp', '')}"
        )
        current = late_records.setdefault(evaluation_id, {})
        for key, value in metric.items():
            if value is not None:
                current[key] = value
        # Only an actual LATE_MARKET opportunity carries its final risk
        # decision; the copy on STRUCTURAL_ARB is used for deduplicated
        # prerequisite/evaluation statistics.
        if _value(item, "strategy") == "LATE_MARKET":
            current["decision"] = metric.get("decision") or _value(item, "decision")
            if metric.get("rejection_reason") is not None:
                current["rejection_reason"] = metric["rejection_reason"]

    late_metrics = list(late_records.values())

    def late_bool(metric: dict[str, Any], field: str, fallback: bool = False) -> bool:
        value = metric.get(field)
        return bool(value) if isinstance(value, bool) else fallback

    late_evaluations = len(late_metrics)
    late_counters = {
        "evaluations": late_evaluations,
        "price_to_beat_available": sum(
            late_bool(metric, "price_to_beat_available", metric.get("price_to_beat") is not None)
            for metric in late_metrics
        ),
        "price_to_beat_missing": sum(
            not late_bool(metric, "price_to_beat_available", metric.get("price_to_beat") is not None)
            for metric in late_metrics
        ),
        "enough_volatility_observations": sum(
            late_bool(metric, "enough_volatility_observations") for metric in late_metrics
        ),
        "probability_calculated": sum(
            late_bool(metric, "probability_calculated", metric.get("model_probability") is not None)
            for metric in late_metrics
        ),
        "candidate_signal": sum(
            late_bool(
                metric,
                "candidate_signal",
                metric.get("candidate_up") is not None or metric.get("candidate_down") is not None,
            )
            for metric in late_metrics
        ),
        "positive_gross_edge": sum(
            value is not None and value > 0.0
            for value in (_finite_float(metric.get("gross_edge")) for metric in late_metrics)
        ),
        "positive_net_edge": sum(
            value is not None and value > 0.0
            for value in (_finite_float(metric.get("net_edge")) for metric in late_metrics)
        ),
        "ACCEPT": sum(metric.get("decision") == "ACCEPT" for metric in late_metrics),
    }

    return {
        "structural_arb": {
            "counters": structural_counters,
            "combined_ask_thresholds": _thresholds(
                combined_asks,
                structural_total,
                (
                    ("<1.00", 1.0),
                    ("<0.995", 0.995),
                    ("<0.99", 0.99),
                    ("<0.98", 0.98),
                ),
            ),
            "distributions": {
                "combined_best_ask": _distribution(combined_asks),
                "gross_edge_signed": _distribution(gross_edges),
                "net_edge": _distribution(net_edges),
                "fees_total": _distribution(fees),
                "slippage_total": _distribution(slippage),
                "remaining_seconds": _distribution(remaining),
            },
        },
        "late_market": {
            "counters": late_counters,
            "rejection_reasons": _counts(
                late_metrics,
                lambda metric: metric.get("rejection_reason") or "none",
            ),
            "distributions": {
                "model_probability": _distribution(
                    metric.get("model_probability") for metric in late_metrics
                ),
                "gross_edge": _distribution(metric.get("gross_edge") for metric in late_metrics),
                "net_edge": _distribution(metric.get("net_edge") for metric in late_metrics),
            },
        },
    }


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
        "strategy_analytics": analyze_strategy_observations(opportunities),
    }
    return AnalyticsReport(metrics)


def _counts(items: Iterable[Any], key_fn: Any) -> dict[str, int]:
    result: defaultdict[str, int] = defaultdict(int)
    for item in items:
        result[str(key_fn(item))] += 1
    return dict(sorted(result.items()))
