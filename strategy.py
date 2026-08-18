"""Explicit, testable opportunity models and quantitative strategies."""

from __future__ import annotations

import math
import statistics
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from config import StrategyConfig
from data import MarketTick, as_utc
from market import ExecutionEstimate, OrderBook, PolymarketMarket


UTC = timezone.utc


@dataclass(frozen=True)
class FeeModel:
    """Polymarket taker fee model from the documented crypto formula."""

    rate: float
    exponent: float = 1.0
    enabled: bool = True

    def fee_for_fill(self, quantity: float, price: float) -> float:
        if not self.enabled or quantity <= 0 or not 0.0 <= price <= 1.0:
            return 0.0
        # Polymarket charges fees in USDC rounded to five decimal places.
        return round(
            max(0.0, quantity * self.rate * (price * (1.0 - price)) ** self.exponent),
            5,
        )

    def fee_for_estimate(self, estimate: ExecutionEstimate) -> float:
        if estimate.filled_quantity <= 0 or estimate.average_price is None:
            return 0.0
        return self.fee_for_fill(estimate.filled_quantity, estimate.average_price)


@dataclass(frozen=True)
class SignalFeatures:
    short_term_return: float | None
    medium_term_return: float | None
    realized_volatility: float | None
    distance_from_recent_range: float | None
    volatility_observations: int = 0

    def as_dict(self) -> dict[str, float | int | None]:
        return {
            "short_term_return": self.short_term_return,
            "medium_term_return": self.medium_term_return,
            "realized_volatility": self.realized_volatility,
            "distance_from_recent_range": self.distance_from_recent_range,
            "volatility_observations": self.volatility_observations,
        }


@dataclass
class Opportunity:
    id: str
    timestamp: datetime
    market: str
    strategy: str
    side: str
    btc_price: float | None
    price_to_beat: float | None
    time_remaining: float
    executable_price: float | None
    executable_probability: float | None
    model_probability: float | None
    gross_edge: float
    estimated_fees: float
    estimated_slippage: float
    estimated_execution_risk: float
    net_edge: float
    available_liquidity: float
    signal_score: float
    decision: str = "REJECT"
    decision_reason: str = "not_evaluated"
    quantity: float = 0.0
    capital_required: float = 0.0
    features: dict[str, Any] = field(default_factory=dict)
    up_token_id: str | None = None
    down_token_id: str | None = None

    @property
    def accepted(self) -> bool:
        return self.decision == "ACCEPT"


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def annualized_realized_volatility(
    ticks: Iterable[MarketTick],
    as_of: datetime,
    lookback: int,
    annualization_seconds: float,
) -> float | None:
    """Estimate volatility using only ticks timestamped at or before ``as_of``."""

    cutoff = as_utc(as_of)
    ordered = sorted(
        (tick for tick in ticks if as_utc(tick.local_timestamp) <= cutoff and tick.price > 0),
        key=lambda tick: as_utc(tick.local_timestamp),
    )[-lookback:]
    returns: list[float] = []
    intervals: list[float] = []
    for previous, current in zip(ordered, ordered[1:]):
        dt = (as_utc(current.local_timestamp) - as_utc(previous.local_timestamp)).total_seconds()
        if dt <= 0:
            continue
        returns.append(math.log(current.price / previous.price))
        intervals.append(dt)
    if len(returns) < 2:
        return None
    interval = statistics.median(intervals)
    if interval <= 0:
        return None
    return statistics.stdev(returns) * math.sqrt(annualization_seconds / interval)


def _return(prices: list[float], periods_back: int) -> float | None:
    if len(prices) <= periods_back or prices[-1] <= 0 or prices[-1 - periods_back] <= 0:
        return None
    return prices[-1] / prices[-1 - periods_back] - 1.0


def momentum_features(
    ticks: Iterable[MarketTick],
    as_of: datetime,
    lookback: int,
    annualization_seconds: float,
) -> SignalFeatures:
    cutoff = as_utc(as_of)
    prices = [
        tick.price
        for tick in sorted(
            (item for item in ticks if as_utc(item.local_timestamp) <= cutoff),
            key=lambda item: as_utc(item.local_timestamp),
        )[-lookback:]
    ]
    recent_volatility = annualized_realized_volatility(
        ticks, cutoff, lookback, annualization_seconds
    )
    if not prices:
        return SignalFeatures(None, None, recent_volatility, None, 0)
    recent_min = min(prices)
    recent_max = max(prices)
    distance = (
        (prices[-1] - recent_min) / (recent_max - recent_min)
        if recent_max > recent_min
        else 0.5
    )
    return SignalFeatures(
        short_term_return=_return(prices, 1),
        medium_term_return=_return(prices, min(5, max(1, len(prices) - 1))),
        realized_volatility=recent_volatility,
        distance_from_recent_range=distance,
        volatility_observations=max(0, len(prices) - 1),
    )


class StrategyEngine:
    """Runs structural arbitrage and late-market probability strategies.

    Momentum is intentionally returned as a feature set only; it cannot create
    an autonomous trade in V1.
    """

    def __init__(self, config: StrategyConfig, fee_model: FeeModel) -> None:
        self.config = config
        self.fee_model = fee_model

    def evaluate(
        self,
        market: PolymarketMarket,
        tick: MarketTick,
        books: dict[str, OrderBook],
        history: Iterable[MarketTick],
        now: datetime | None = None,
    ) -> list[Opportunity]:
        as_of = as_utc(now or tick.local_timestamp)
        features = momentum_features(
            history,
            as_of,
            self.config.volatility_lookback,
            self.config.annualization_seconds,
        )
        opportunities: list[Opportunity] = []
        up_book = books.get(market.up_token_id)
        down_book = books.get(market.down_token_id)
        if up_book and down_book and tick.coherent and up_book.coherent() and down_book.coherent():
            opportunities.append(
                self._structural_arbitrage(market, tick, up_book, down_book, features, as_of)
            )
            late = self._late_market(market, tick, up_book, down_book, features, as_of)
            if late:
                opportunities.append(late)
        else:
            opportunities.append(
                self._rejected_placeholder(
                    market,
                    tick,
                    features,
                    as_of,
                    "missing_or_incoherent_orderbook_or_spot",
                )
            )
        return opportunities

    def _base(
        self,
        market: PolymarketMarket,
        tick: MarketTick,
        strategy: str,
        side: str,
        features: SignalFeatures,
        as_of: datetime,
        **values: Any,
    ) -> Opportunity:
        return Opportunity(
            id=str(uuid.uuid4()),
            timestamp=as_of,
            market=market.market_id,
            strategy=strategy,
            side=side,
            btc_price=tick.price,
            price_to_beat=market.price_to_beat,
            time_remaining=market.remaining_seconds_at(as_of),
            features={"momentum": features.as_dict()},
            up_token_id=market.up_token_id,
            down_token_id=market.down_token_id,
            **values,
        )

    def _structural_arbitrage(
        self,
        market: PolymarketMarket,
        tick: MarketTick,
        up_book: OrderBook,
        down_book: OrderBook,
        features: SignalFeatures,
        as_of: datetime,
    ) -> Opportunity:
        best_up = up_book.best_ask.price if up_book.best_ask else 0.0
        best_down = down_book.best_ask.price if down_book.best_ask else 0.0
        target = (
            self.config.sizing_capital / (best_up + best_down)
            if best_up + best_down > 0
            else 0.0
        )
        target = min(target, up_book.available_buy_quantity, down_book.available_buy_quantity)
        up_estimate = up_book.estimate_buy_cost(target)
        down_estimate = down_book.estimate_buy_cost(target)
        matched = min(up_estimate.filled_quantity, down_estimate.filled_quantity)
        if matched > 0:
            up_estimate = up_book.estimate_buy_cost(matched)
            down_estimate = down_book.estimate_buy_cost(matched)
        combined_price = (
            up_estimate.average_price + down_estimate.average_price
            if up_estimate.average_price is not None and down_estimate.average_price is not None
            else None
        )
        fees = self.fee_model.fee_for_estimate(up_estimate) + self.fee_model.fee_for_estimate(
            down_estimate
        )
        gross_edge = max(0.0, 1.0 - (best_up + best_down))
        slippage = up_estimate.slippage_per_share + down_estimate.slippage_per_share
        cost_per_share = combined_price or 0.0
        net_edge = 1.0 - cost_per_share - (fees / matched if matched else 0.0)
        net_edge -= self.config.execution_buffer
        liquidity = min(up_book.available_buy_quantity, down_book.available_buy_quantity)
        execution_risk = self._execution_risk(matched, target, tick)
        opportunity = self._base(
            market,
            tick,
            "STRUCTURAL_ARB",
            "BUY_UP_AND_DOWN",
            features,
            as_of,
            executable_price=combined_price,
            executable_probability=combined_price,
            model_probability=None,
            gross_edge=gross_edge,
            estimated_fees=fees,
            estimated_slippage=slippage,
            estimated_execution_risk=execution_risk,
            net_edge=net_edge,
            available_liquidity=liquidity,
            signal_score=net_edge,
            quantity=matched,
            capital_required=(combined_price or 0.0) * matched + fees,
        )
        opportunity.features["structural"] = {
            "up_best_ask": best_up,
            "down_best_ask": best_down,
            "up_levels_consumed": up_estimate.levels_consumed,
            "down_levels_consumed": down_estimate.levels_consumed,
        }
        return opportunity

    def _late_market(
        self,
        market: PolymarketMarket,
        tick: MarketTick,
        up_book: OrderBook,
        down_book: OrderBook,
        features: SignalFeatures,
        as_of: datetime,
    ) -> Opportunity | None:
        remaining = market.remaining_seconds_at(as_of)
        if market.price_to_beat is None or remaining <= 0:
            return None
        if remaining > self.config.max_time_remaining_for_late_market_s:
            return None
        volatility = features.realized_volatility
        if features.volatility_observations < self.config.minimum_volatility_observations:
            volatility = self.config.volatility_fallback_annualized
        if volatility <= 0:
            return None
        model_up = self._probability_above_barrier(
            tick.price,
            market.price_to_beat,
            volatility,
            remaining,
            self.config.annualization_seconds,
        )
        candidates: list[Opportunity] = []
        for side, book, probability in (
            ("BUY_UP", up_book, model_up),
            ("BUY_DOWN", down_book, 1.0 - model_up),
        ):
            if not book.best_ask:
                continue
            best_ask = book.best_ask.price
            target = min(
                self.config.sizing_capital / best_ask if best_ask > 0 else 0.0,
                book.available_buy_quantity,
            )
            estimate = book.estimate_buy_cost(target)
            if estimate.filled_quantity <= 0 or estimate.average_price is None:
                continue
            fees = self.fee_model.fee_for_estimate(estimate)
            gross_edge = probability - best_ask
            net_edge = probability - estimate.average_price
            net_edge -= fees / estimate.filled_quantity
            net_edge -= self.config.execution_buffer
            opportunity = self._base(
                market,
                tick,
                "LATE_MARKET",
                side,
                features,
                as_of,
                executable_price=estimate.average_price,
                executable_probability=estimate.average_price,
                model_probability=probability,
                gross_edge=gross_edge,
                estimated_fees=fees,
                estimated_slippage=estimate.slippage_per_share,
                estimated_execution_risk=self._execution_risk(
                    estimate.filled_quantity, target, tick
                ),
                net_edge=net_edge,
                available_liquidity=book.available_buy_quantity,
                signal_score=net_edge,
                quantity=estimate.filled_quantity,
                capital_required=estimate.notional + fees,
            )
            opportunity.features["late_market"] = {
                "volatility_annualized": volatility,
                "remaining_seconds": remaining,
                "barrier_distance": tick.price - market.price_to_beat,
            }
            candidates.append(opportunity)
        if not candidates:
            return None
        # Only the stronger side is a candidate; this prevents a binary market
        # from creating two opposing late-market trades at the same timestamp.
        return max(candidates, key=lambda item: item.net_edge)

    @staticmethod
    def _probability_above_barrier(
        spot: float,
        barrier: float,
        annualized_volatility: float,
        remaining_seconds: float,
        annualization_seconds: float = 365.0 * 24.0 * 60.0 * 60.0,
    ) -> float:
        if remaining_seconds <= 0:
            return 1.0 if spot > barrier else 0.0
        if spot <= 0 or barrier <= 0:
            return 0.5
        horizon_years = remaining_seconds / annualization_seconds
        sigma_sqrt_t = annualized_volatility * math.sqrt(horizon_years)
        if sigma_sqrt_t <= 0:
            return 1.0 if spot > barrier else 0.0
        d2 = (math.log(spot / barrier) - 0.5 * annualized_volatility**2 * horizon_years) / sigma_sqrt_t
        return min(1.0, max(0.0, normal_cdf(d2)))

    @staticmethod
    def _execution_risk(quantity: float, requested: float, tick: MarketTick) -> float:
        if requested <= 0:
            return 1.0
        non_fill = max(0.0, 1.0 - min(1.0, quantity / requested))
        latency_risk = min(1.0, max(0.0, tick.latency_ms or 0.0) / 1_500.0)
        return min(1.0, non_fill + latency_risk)

    def _rejected_placeholder(
        self,
        market: PolymarketMarket,
        tick: MarketTick,
        features: SignalFeatures,
        as_of: datetime,
        reason: str,
    ) -> Opportunity:
        opportunity = self._base(
            market,
            tick,
            "DATA_QUALITY",
            "NONE",
            features,
            as_of,
            executable_price=None,
            executable_probability=None,
            model_probability=None,
            gross_edge=0.0,
            estimated_fees=0.0,
            estimated_slippage=0.0,
            estimated_execution_risk=1.0,
            net_edge=0.0,
            available_liquidity=0.0,
            signal_score=0.0,
            quantity=0.0,
            capital_required=0.0,
            decision_reason=reason,
        )
        return opportunity
