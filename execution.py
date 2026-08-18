"""Paper-only execution using current order-book depth."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal
import uuid

from market import OrderBook
from strategy import FeeModel, Opportunity


UTC = timezone.utc


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@dataclass
class PaperLeg:
    token_id: str
    side: Literal["BUY", "SELL"]
    requested_quantity: float
    filled_quantity: float
    entry_price: float
    notional: float
    fees: float
    slippage: float  # USDC notional slippage for this leg.


@dataclass
class PaperTrade:
    id: str
    opportunity_id: str
    entry_timestamp: datetime
    market: str
    strategy: str
    side: str
    quantity: float
    entry_price: float
    fees: float
    slippage: float  # USDC notional slippage across all filled legs.
    capital_required: float
    latency_ms: float
    legs: list[PaperLeg] = field(default_factory=list)
    exit_timestamp: datetime | None = None
    exit_price: float | None = None
    gross_pnl: float | None = None
    net_pnl: float | None = None
    status: str = "OPEN"
    failure_reason: str | None = None


class PaperExecutor:
    """No private key, signer, POST, or live order endpoint exists here."""

    def __init__(
        self,
        *,
        mode: str = "PAPER",
        latency_ms: float = 150.0,
        fee_model: FeeModel | None = None,
    ) -> None:
        if mode.upper() != "PAPER":
            raise RuntimeError("PaperExecutor refuses every mode other than PAPER")
        self.latency_ms = latency_ms
        self.fee_model = fee_model

    def execute(
        self,
        opportunity: Opportunity,
        books: dict[str, OrderBook],
        now: datetime | None = None,
    ) -> PaperTrade:
        timestamp = _as_utc(now or datetime.now(UTC))
        if opportunity.side == "BUY_UP_AND_DOWN":
            return self._execute_pair(opportunity, books, timestamp)
        if opportunity.side == "BUY_UP":
            return self._execute_single(
                opportunity,
                books.get(opportunity.up_token_id or ""),
                opportunity.up_token_id or "",
                timestamp,
            )
        if opportunity.side == "BUY_DOWN":
            return self._execute_single(
                opportunity,
                books.get(opportunity.down_token_id or ""),
                opportunity.down_token_id or "",
                timestamp,
            )
        return self._failed(opportunity, timestamp, "unsupported_side")

    def _execute_single(
        self,
        opportunity: Opportunity,
        book: OrderBook | None,
        token_id: str,
        timestamp: datetime,
    ) -> PaperTrade:
        if not book:
            return self._failed(opportunity, timestamp, "missing_orderbook")
        estimate = book.estimate_buy_cost(opportunity.quantity)
        if estimate.filled_quantity <= 0 or estimate.average_price is None:
            return self._failed(opportunity, timestamp, "no_ask_liquidity")
        fees = (
            self.fee_model.fee_for_estimate(estimate)
            if self.fee_model is not None
            else opportunity.estimated_fees
            * estimate.filled_quantity
            / max(opportunity.quantity, 1e-12)
        )
        leg = PaperLeg(
            token_id=token_id,
            side="BUY",
            requested_quantity=opportunity.quantity,
            filled_quantity=estimate.filled_quantity,
            entry_price=estimate.average_price,
            notional=estimate.notional,
            fees=fees,
            slippage=estimate.slippage_total,
        )
        status = "OPEN" if estimate.complete else "PARTIAL"
        return PaperTrade(
            id=str(uuid.uuid4()),
            opportunity_id=opportunity.id,
            entry_timestamp=timestamp,
            market=opportunity.market,
            strategy=opportunity.strategy,
            side=opportunity.side,
            quantity=estimate.filled_quantity,
            entry_price=estimate.average_price,
            fees=fees,
            slippage=estimate.slippage_total,
            capital_required=estimate.notional + fees,
            latency_ms=self.latency_ms,
            legs=[leg],
            status=status,
            failure_reason=None if estimate.complete else "partial_fill",
        )

    def _execute_pair(
        self,
        opportunity: Opportunity,
        books: dict[str, OrderBook],
        timestamp: datetime,
    ) -> PaperTrade:
        up_book = books.get(opportunity.up_token_id or "")
        down_book = books.get(opportunity.down_token_id or "")
        if not up_book or not down_book:
            return self._failed(opportunity, timestamp, "missing_pair_orderbook")
        up_probe = up_book.estimate_buy_cost(opportunity.quantity)
        down_probe = down_book.estimate_buy_cost(opportunity.quantity)
        matched = min(up_probe.filled_quantity, down_probe.filled_quantity)
        if matched <= 0:
            return self._failed(opportunity, timestamp, "no_matched_pair_liquidity")
        up_estimate = up_book.estimate_buy_cost(matched)
        down_estimate = down_book.estimate_buy_cost(matched)
        if self.fee_model is not None:
            up_fees = self.fee_model.fee_for_estimate(up_estimate)
            down_fees = self.fee_model.fee_for_estimate(down_estimate)
        else:
            up_fees = self._proportional_fee(opportunity, up_estimate, matched, 2)
            down_fees = self._proportional_fee(opportunity, down_estimate, matched, 2)
        legs = [
            PaperLeg(
                token_id=opportunity.up_token_id or "",
                side="BUY",
                requested_quantity=opportunity.quantity,
                filled_quantity=matched,
                entry_price=up_estimate.average_price or 0.0,
                notional=up_estimate.notional,
                fees=up_fees,
                slippage=up_estimate.slippage_total,
            ),
            PaperLeg(
                token_id=opportunity.down_token_id or "",
                side="BUY",
                requested_quantity=opportunity.quantity,
                filled_quantity=matched,
                entry_price=down_estimate.average_price or 0.0,
                notional=down_estimate.notional,
                fees=down_fees,
                slippage=down_estimate.slippage_total,
            ),
        ]
        complete = up_probe.complete and down_probe.complete
        return PaperTrade(
            id=str(uuid.uuid4()),
            opportunity_id=opportunity.id,
            entry_timestamp=timestamp,
            market=opportunity.market,
            strategy=opportunity.strategy,
            side=opportunity.side,
            quantity=matched,
            entry_price=(up_estimate.notional + down_estimate.notional) / matched,
            fees=up_fees + down_fees,
            slippage=up_estimate.slippage_total + down_estimate.slippage_total,
            capital_required=up_estimate.notional + down_estimate.notional + up_fees + down_fees,
            latency_ms=self.latency_ms,
            legs=legs,
            status="OPEN" if complete else "PARTIAL",
            failure_reason=None if complete else "partial_pair_fill",
        )

    @staticmethod
    def _proportional_fee(
        opportunity: Opportunity,
        estimate: object,
        matched: float,
        legs: int,
    ) -> float:
        # Strategy fees are already computed from executable depth.  Splitting
        # them across pair legs keeps paper accounting consistent with the
        # opportunity that was accepted, while execution price still comes from
        # the live snapshot.
        total = opportunity.estimated_fees
        return total * (1.0 / legs) if matched > 0 else 0.0

    @staticmethod
    def _failed(opportunity: Opportunity, timestamp: datetime, reason: str) -> PaperTrade:
        return PaperTrade(
            id=str(uuid.uuid4()),
            opportunity_id=opportunity.id,
            entry_timestamp=timestamp,
            market=opportunity.market,
            strategy=opportunity.strategy,
            side=opportunity.side,
            quantity=0.0,
            entry_price=0.0,
            fees=0.0,
            slippage=0.0,
            capital_required=0.0,
            latency_ms=0.0,
            status="FAILED",
            failure_reason=reason,
        )

    @staticmethod
    def settle(
        trade: PaperTrade,
        outcome: Literal["UP", "DOWN"],
        timestamp: datetime | None = None,
    ) -> PaperTrade:
        if trade.status == "FAILED" or trade.quantity <= 0:
            return trade
        payout = 0.0
        if trade.side == "BUY_UP_AND_DOWN":
            # A complete UP+DOWN pair pays exactly one dollar per matched
            # share, not one dollar per leg.
            payout = trade.quantity
        else:
            wins = (trade.side == "BUY_UP" and outcome == "UP") or (
                trade.side == "BUY_DOWN" and outcome == "DOWN"
            )
            if wins:
                payout = trade.quantity
        trade.exit_timestamp = _as_utc(timestamp or datetime.now(UTC))
        trade.exit_price = 1.0 if payout > 0 else 0.0
        trade.gross_pnl = payout - sum(leg.notional for leg in trade.legs)
        trade.net_pnl = trade.gross_pnl - trade.fees
        trade.status = "CLOSED"
        return trade
