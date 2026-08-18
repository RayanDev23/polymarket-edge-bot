"""Configurable risk limits and circuit breakers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from config import RiskConfig
from strategy import Opportunity


UTC = timezone.utc


@dataclass(frozen=True)
class RiskDecision:
    accepted: bool
    reason: str
    capital_required: float


@dataclass
class RiskState:
    exposure: float = 0.0
    daily_pnl: float = 0.0
    consecutive_losses: int = 0
    failed_orders: int = 0
    circuit_breaker: bool = False
    breaker_reason: str | None = None
    session_day: date | None = None


class RiskEngine:
    def __init__(self, config: RiskConfig) -> None:
        self.config = config
        self.state = RiskState()

    def _roll_day(self, now: datetime) -> None:
        current_day = now.astimezone(UTC).date()
        if self.state.session_day is None:
            self.state.session_day = current_day
        elif self.state.session_day != current_day:
            self.state.daily_pnl = 0.0
            self.state.consecutive_losses = 0
            self.state.failed_orders = 0
            self.state.circuit_breaker = False
            self.state.breaker_reason = None
            self.state.session_day = current_day

    def evaluate(
        self,
        opportunity: Opportunity,
        *,
        data_age_ms: float,
        execution_latency_ms: float | None,
        now: datetime | None = None,
        data_coherent: bool = True,
        orderbook_coherent: bool = True,
        market_open: bool = True,
    ) -> RiskDecision:
        current = now or datetime.now(UTC)
        current = (
            current.replace(tzinfo=UTC)
            if current.tzinfo is None
            else current.astimezone(UTC)
        )
        self._roll_day(current)
        capital = max(0.0, opportunity.capital_required)
        checks = (
            (self.state.circuit_breaker, self.state.breaker_reason or "circuit_breaker"),
            (not data_coherent, "incoherent_spot_data"),
            (not orderbook_coherent, "incoherent_orderbook"),
            (not market_open, "market_closed"),
            (data_age_ms > self.config.maximum_data_age_ms, "stale_data"),
            (
                execution_latency_ms is not None
                and execution_latency_ms > self.config.maximum_execution_latency_ms,
                "execution_latency_too_high",
            ),
            (opportunity.net_edge < self.config.minimum_net_edge, "insufficient_edge"),
            (opportunity.available_liquidity < self.config.minimum_liquidity, "insufficient_liquidity"),
            (capital > self.config.starting_capital, "insufficient_starting_capital"),
            (capital > self.config.max_capital_per_trade, "max_capital_per_trade"),
            (
                self.state.exposure + capital > self.config.max_simultaneous_exposure,
                "max_simultaneous_exposure",
            ),
            (self.state.daily_pnl <= -abs(self.config.max_daily_loss), "daily_loss_limit"),
            (
                self.state.consecutive_losses >= self.config.max_consecutive_losses,
                "consecutive_loss_limit",
            ),
        )
        for failed, reason in checks:
            if failed:
                if reason in {
                    "stale_data",
                    "execution_latency_too_high",
                    "daily_loss_limit",
                    "consecutive_loss_limit",
                    "incoherent_spot_data",
                    "incoherent_orderbook",
                }:
                    self.trigger(reason)
                return RiskDecision(False, reason, capital)
        return RiskDecision(True, "risk_checks_passed", capital)

    def register_open(self, capital_required: float) -> None:
        self.state.exposure += max(0.0, capital_required)

    def register_closed(self, net_pnl: float, capital_released: float) -> None:
        self.state.exposure = max(0.0, self.state.exposure - max(0.0, capital_released))
        self.state.daily_pnl += net_pnl
        if net_pnl < 0:
            self.state.consecutive_losses += 1
        elif net_pnl > 0:
            self.state.consecutive_losses = 0
        if self.state.daily_pnl <= -abs(self.config.max_daily_loss):
            self.trigger("daily_loss_limit")
        if self.state.consecutive_losses >= self.config.max_consecutive_losses:
            self.trigger("consecutive_loss_limit")

    def register_failed_order(self) -> None:
        self.state.failed_orders += 1
        if self.state.failed_orders >= self.config.failed_orders_before_breaker:
            self.trigger("failed_order_limit")

    def trigger(self, reason: str) -> None:
        self.state.circuit_breaker = True
        self.state.breaker_reason = reason

    def status(self) -> dict[str, float | int | bool | str | None]:
        return {
            "exposure": self.state.exposure,
            "daily_pnl": self.state.daily_pnl,
            "consecutive_losses": self.state.consecutive_losses,
            "failed_orders": self.state.failed_orders,
            "circuit_breaker": self.state.circuit_breaker,
            "breaker_reason": self.state.breaker_reason,
        }
