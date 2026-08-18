import logging
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from config import RiskConfig, StrategyConfig
from data import MarketDataStore, MarketTick
from database import Database
from execution import PaperExecutor
from main import RuntimeDiagnostics, _process_tick
from market import (
    MarketDiscovery,
    OrderBook,
    OrderBookLevel,
    PolymarketBookFeed,
    PolymarketMarket,
    parse_ws_message,
)
from risk import RiskEngine
from strategy import FeeModel, Opportunity, StrategyEngine, annualized_realized_volatility


UTC = timezone.utc


def tick(at: datetime, price: float = 100.0) -> MarketTick:
    return MarketTick(at, at, "BTCUSDT", price, price - 0.1, price + 0.1, 1.0, 1.0, 0.2, 4.0)


def book(token: str, asks=((0.40, 10.0),), bids=((0.39, 10.0),)) -> OrderBook:
    return OrderBook(
        token,
        [OrderBookLevel(price, size) for price, size in bids],
        [OrderBookLevel(price, size) for price, size in asks],
        datetime.now(UTC),
    )


def market(now: datetime, price_to_beat: float | None = 100.0) -> PolymarketMarket:
    return PolymarketMarket(
        "m1",
        "condition-1",
        "Bitcoin Up or Down 5m",
        "btc-updown-5m-test",
        "up-token",
        "down-token",
        now - timedelta(minutes=4),
        now + timedelta(seconds=60),
        None,
        price_to_beat,
        True,
        False,
        True,
        True,
        0.07,
        None,
        {"question": "Bitcoin Up or Down 5m", "slug": "btc-updown-5m-test"},
    )


def opportunity(side: str = "BUY_UP", quantity: float = 10.0, capital: float = 4.0) -> Opportunity:
    return Opportunity(
        id="opp-1",
        timestamp=datetime.now(UTC),
        market="m1",
        strategy="TEST",
        side=side,
        btc_price=100.0,
        price_to_beat=100.0,
        time_remaining=30.0,
        executable_price=0.4,
        executable_probability=0.4,
        model_probability=0.8,
        gross_edge=0.4,
        estimated_fees=0.0,
        estimated_slippage=0.0,
        estimated_execution_risk=0.0,
        net_edge=0.2,
        available_liquidity=10.0,
        signal_score=0.2,
        decision="ACCEPT",
        decision_reason="test",
        quantity=quantity,
        capital_required=capital,
        up_token_id="up-token",
        down_token_id="down-token",
    )


class RecordingStrategy:
    def __init__(self) -> None:
        self.calls = 0

    def evaluate(self, *args: object, **kwargs: object) -> list[Opportunity]:
        self.calls += 1
        return []


def test_runtime_diagnostics_distinguishes_latched_breaker_from_book_failure() -> None:
    diagnostics = RuntimeDiagnostics()
    up = book("up-token")
    down = book("down-token")

    diagnostics.record_orderbook_rejection(
        up_book=up,
        down_book=down,
        up_age_ms=25.0,
        down_age_ms=25.0,
        orderbook_coherent=True,
        fresh_books=True,
        logger=logging.getLogger("test-diagnostics"),
    )

    assert diagnostics.incoherent_reason_counts == {"circuit_breaker_latched": 1}
    assert diagnostics.crossed_books == 0
    assert diagnostics.stale_rejections == 0


async def process_tick_for_test(
    books: dict[str, OrderBook],
    strategy: object,
    risk: RiskEngine | None = None,
) -> Database:
    timestamp = datetime.now(UTC)
    database = Database(":memory:")
    await _process_tick(
        tick(timestamp),
        market=market(timestamp),
        books=books,
        data_store=MarketDataStore(),
        database=database,
        risk=risk or RiskEngine(RiskConfig()),
        executor=PaperExecutor(mode="PAPER"),
        strategy=strategy,  # type: ignore[arg-type]
        open_trades=[],
        logger=logging.getLogger("test-process-tick"),
    )
    return database


def test_orderbook_depth_and_sell_proceeds() -> None:
    order_book = book("t", asks=((0.40, 10.0), (0.50, 20.0)), bids=((0.35, 5.0), (0.30, 20.0)))
    buy = order_book.estimate_buy_cost(20)
    assert buy.filled_quantity == pytest.approx(20)
    assert buy.notional == pytest.approx(9.0)
    assert buy.average_price == pytest.approx(0.45)
    assert buy.slippage_total == pytest.approx(1.0)
    assert buy.complete

    sell = order_book.estimate_sell_proceeds(10)
    assert sell.notional == pytest.approx(3.25)
    assert sell.average_price == pytest.approx(0.325)
    assert sell.slippage_total == pytest.approx(0.25)

    example = book("example", asks=((0.40, 10.0), (0.41, 20.0), (0.42, 100.0)))
    thirty = example.estimate_buy_cost(30)
    assert thirty.complete
    assert thirty.notional == pytest.approx(12.2)
    assert thirty.average_price == pytest.approx(12.2 / 30)


def test_fee_formula_is_explicit() -> None:
    assert FeeModel(0.07).fee_for_fill(100, 0.5) == pytest.approx(1.75)
    assert FeeModel(0.07, enabled=False).fee_for_fill(100, 0.5) == 0.0
    assert FeeModel(0.25, exponent=2).fee_for_fill(100, 0.5) == pytest.approx(1.5625)
    assert FeeModel(0.07).fee_for_fill(0.001, 0.5) == 0.00002


def test_polymarket_resolution_event_is_explicit() -> None:
    feed = PolymarketBookFeed(object(), "wss://example.invalid")
    feed._apply_event(
        {
            "event_type": "market_resolved",
            "market": "condition-1",
            "winning_asset_id": "up-token",
            "winning_outcome": "Up",
        }
    )
    assert feed.resolved_outcomes["condition-1"] == "UP"
    assert feed.resolved_asset_ids["condition-1"] == "up-token"


def test_polymarket_price_change_updates_the_nested_asset_book() -> None:
    now = datetime.now(UTC)
    feed = PolymarketBookFeed(object(), "wss://example.invalid")
    feed.books["up-token"] = book("up-token", asks=((0.5, 10),), bids=((0.4, 10),))
    feed.books["down-token"] = book("down-token", asks=((0.6, 10),), bids=((0.3, 10),))
    feed._apply_event(
        {
            "event_type": "price_change",
            "market": "condition-1",
            "timestamp": str(int(now.timestamp() * 1000)),
            "price_changes": [
                {"asset_id": "up-token", "price": "0.45", "size": "7", "side": "BUY"},
                {"asset_id": "down-token", "price": "0.65", "size": "8", "side": "SELL"},
            ],
        }
    )
    assert feed.books["up-token"].best_bid.price == pytest.approx(0.45)
    assert feed.books["down-token"].best_ask.price == pytest.approx(0.6)
    assert any(level.price == pytest.approx(0.65) for level in feed.books["down-token"].asks)


def test_websocket_parser_handles_dict_list_empty_unknown_and_unexpected() -> None:
    raw_book = {
        "event_type": "book",
        "asset_id": "up-token",
        "timestamp": "1767225600000",
        "bids": [{"price": "0.40", "size": "10"}],
        "asks": [{"price": "0.41", "size": "12"}],
    }
    events = parse_ws_message(raw_book)
    assert len(events) == 1
    assert events[0].event_type == "book"
    assert events[0].asset_id == "up-token"
    assert events[0].raw_data == raw_book

    listed = parse_ws_message([raw_book, {"event_type": "mystery", "value": 1}])
    assert [event.event_type for event in listed] == ["book", "mystery"]
    assert parse_ws_message([]) == []
    assert parse_ws_message({"unexpected": True})[0].event_type == "unknown"
    assert parse_ws_message("not-json") == []


def test_websocket_envelope_snapshot_updates_depth_and_timestamps() -> None:
    feed = PolymarketBookFeed(object(), "wss://example.invalid")
    feed.books["up-token"] = book("up-token")
    feed._apply_event(
        {
            "topic": "market",
            "type": "book",
            "payload": {
                "market": "condition-1",
                "tokenId": "up-token",
                "timestamp": "2026-01-01T00:00:00Z",
                "bids": [{"price": "0.40", "size": "10"}],
                "asks": [
                    {"price": "0.41", "size": "10"},
                    {"price": "0.42", "size": "20"},
                ],
            },
        }
    )
    assert feed.books["up-token"].best_bid.price == pytest.approx(0.4)
    assert feed.books["up-token"].best_ask.price == pytest.approx(0.41)
    assert feed.books["up-token"].available_buy_quantity == pytest.approx(30)
    assert feed.books["up-token"].updated_at == datetime(2026, 1, 1, tzinfo=UTC)


def test_websocket_envelope_price_change_keeps_up_and_down_separate() -> None:
    feed = PolymarketBookFeed(object(), "wss://example.invalid")
    feed.books["up-token"] = book("up-token", asks=((0.5, 10),), bids=((0.4, 10),))
    feed.books["down-token"] = book("down-token", asks=((0.6, 10),), bids=((0.3, 10),))
    feed._apply_event(
        {
            "topic": "market",
            "type": "price_change",
            "payload": {
                "market": "condition-1",
                "timestamp": "2026-01-01T00:00:01Z",
                "priceChanges": [
                    {"tokenId": "down-token", "price": "0.65", "size": "8", "side": "SELL"}
                ],
            },
        }
    )
    assert feed.books["up-token"].best_ask.price == pytest.approx(0.5)
    assert feed.books["down-token"].best_ask.price == pytest.approx(0.6)
    assert any(level.price == pytest.approx(0.65) for level in feed.books["down-token"].asks)


def test_websocket_envelope_resolution_uses_official_winning_fields() -> None:
    feed = PolymarketBookFeed(object(), "wss://example.invalid")
    feed._apply_event(
        {
            "topic": "market",
            "type": "market_resolved",
            "payload": {
                "market": "condition-1",
                "winningTokenId": "up-token",
                "winningOutcome": "Yes",
            },
        }
    )
    assert feed.resolved_outcomes["condition-1"] == "YES"
    assert feed.resolved_asset_ids["condition-1"] == "up-token"


def test_binance_payload_and_stale_age() -> None:
    received = datetime(2026, 1, 1, tzinfo=UTC)
    parsed = MarketTick.from_binance_payload(
        {"u": 1, "s": "BTCUSDT", "b": "100.0", "B": "2", "a": "100.2", "A": "3"},
        received,
    )
    assert parsed.price == pytest.approx(100.1)
    assert parsed.spread == pytest.approx(0.2)
    store = MarketDataStore()
    store.update(parsed)
    assert store.data_age_ms(received + timedelta(seconds=3)) == pytest.approx(3000)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "books",
    [
        {},
        {"up-token": book("up-token")},
        {"down-token": book("down-token")},
    ],
    ids=["no-books", "up-only", "down-only"],
)
async def test_process_tick_waits_for_both_books_before_strategy(books: dict[str, OrderBook]) -> None:
    strategy = RecordingStrategy()
    database = await process_tick_for_test(books, strategy)

    assert strategy.calls == 0
    assert database.fetch_opportunities() == []
    assert database.connection.execute("SELECT count(*) FROM market_observations").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_process_tick_evaluates_strategy_after_both_books_arrive() -> None:
    strategy = RecordingStrategy()
    database = await process_tick_for_test(
        {"up-token": book("up-token"), "down-token": book("down-token")},
        strategy,
    )

    assert strategy.calls == 1
    assert database.fetch_opportunities() == []


@pytest.mark.asyncio
async def test_process_tick_rejects_incoherent_books_without_trade() -> None:
    incoherent = OrderBook("up-token", bids=[], asks=[])
    strategy = StrategyEngine(StrategyConfig(), FeeModel(0.07))
    database = await process_tick_for_test(
        {"up-token": incoherent, "down-token": book("down-token")},
        strategy,
    )

    opportunities = database.fetch_opportunities()
    assert len(opportunities) == 1
    assert opportunities[0]["decision"] == "REJECT"
    assert opportunities[0]["decision_reason"] == "incoherent_orderbook"
    assert database.fetch_trades() == []


@pytest.mark.asyncio
async def test_process_tick_rejects_stale_books_without_trade() -> None:
    stale_at = datetime.now(UTC) - timedelta(seconds=10)
    stale_up = OrderBook(
        "up-token",
        [OrderBookLevel(0.39, 10)],
        [OrderBookLevel(0.40, 10)],
        stale_at,
    )
    stale_down = OrderBook(
        "down-token",
        [OrderBookLevel(0.39, 10)],
        [OrderBookLevel(0.40, 10)],
        stale_at,
    )
    strategy = StrategyEngine(StrategyConfig(), FeeModel(0.07))
    database = await process_tick_for_test(
        {"up-token": stale_up, "down-token": stale_down},
        strategy,
        RiskEngine(RiskConfig(maximum_data_age_ms=2_000)),
    )

    opportunities = database.fetch_opportunities()
    assert len(opportunities) == 1
    assert opportunities[0]["decision"] == "REJECT"
    assert opportunities[0]["decision_reason"] == "incoherent_orderbook"
    assert database.fetch_trades() == []


def test_market_discovery_maps_dynamic_outcomes_and_filters_5m() -> None:
    raw = {
        "id": "dynamic-market",
        "conditionId": "condition-dynamic",
        "question": "Bitcoin Up or Down - 5m",
        "slug": "btc-updown-5m-dynamic",
        "startDate": "2026-01-01T00:00:00Z",
        "endDate": "2026-01-01T00:05:00Z",
        "outcomes": '["Down", "Up"]',
        "clobTokenIds": '["down-dynamic", "up-dynamic"]',
        "feesEnabled": True,
        "feeSchedule": {"rate": 0.02, "exponent": 2, "takerOnly": True},
        "active": True,
        "closed": False,
    }
    parsed = MarketDiscovery.parse_market(raw)
    assert parsed is not None
    assert parsed.up_token_id == "up-dynamic"
    assert parsed.down_token_id == "down-dynamic"
    assert parsed.fee_rate == pytest.approx(0.02)
    assert parsed.fee_exponent == pytest.approx(2)
    assert MarketDiscovery.is_btc_5m(parsed)


def _gamma_crypto_market(
    *,
    market_id: str,
    slug: str,
    question: str,
    start: datetime,
    end: datetime,
    asset: str = "btc",
    active: bool = True,
    closed: bool = False,
) -> dict:
    return {
        "id": market_id,
        "conditionId": f"condition-{market_id}",
        "question": question,
        "slug": slug,
        # startDate is creation time in the current Gamma crypto schema.
        "startDate": (start - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "eventStartTime": start.isoformat().replace("+00:00", "Z"),
        "endDate": end.isoformat().replace("+00:00", "Z"),
        "outcomes": '["Down", "Up"]',
        "clobTokenIds": f'["down-{market_id}", "up-{market_id}"]',
        "cryptoMarketConfig": {"asset": asset, "duration": "5m"},
        "active": active,
        "closed": closed,
        "acceptingOrders": True,
    }


@pytest.mark.asyncio
async def test_discovery_selects_current_gamma_btc_5m_and_maps_tokens() -> None:
    now = datetime(2026, 8, 18, 21, 22, tzinfo=UTC)
    current = _gamma_crypto_market(
        market_id="current",
        slug="btc-updown-5m-1787088000",
        question="Bitcoin Up or Down - August 18, 5:20PM-5:25PM ET",
        start=now - timedelta(minutes=2),
        end=now + timedelta(minutes=3),
    )
    future = _gamma_crypto_market(
        market_id="future",
        slug="btc-updown-5m-future",
        question="Bitcoin Up or Down - future",
        start=now + timedelta(minutes=5),
        end=now + timedelta(minutes=10),
    )
    fifteen = _gamma_crypto_market(
        market_id="fifteen",
        slug="btc-updown-15m-1787087700",
        question="Bitcoin Up or Down - August 18, 5:15PM-5:30PM ET",
        start=now - timedelta(minutes=7),
        end=now + timedelta(minutes=8),
    )
    fifteen["cryptoMarketConfig"] = {"asset": "btc", "duration": "15m"}
    eth = _gamma_crypto_market(
        market_id="eth",
        slug="eth-updown-5m-test",
        question="Ethereum Up or Down - 5m",
        start=now - timedelta(minutes=1),
        end=now + timedelta(minutes=4),
        asset="eth",
    )
    expired = _gamma_crypto_market(
        market_id="expired",
        slug="btc-updown-5m-expired",
        question="Bitcoin Up or Down - expired",
        start=now - timedelta(minutes=10),
        end=now - timedelta(seconds=1),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/markets/keyset")
        assert request.url.params.get("end_date_min")
        assert request.url.params.get("end_date_max")
        return httpx.Response(200, json={"markets": [future, fifteen, eth, expired, current]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        discovery = MarketDiscovery(client, "https://gamma.example")
        found = await discovery.discover_btc_5m(now)

    assert found is not None
    assert found.market_id == "current"
    assert found.up_token_id == "up-current"
    assert found.down_token_id == "down-current"
    assert found.start_time == now - timedelta(minutes=2)
    assert found.end_time == now + timedelta(minutes=3)


def test_discovery_rejects_15m_eth_and_finished_markets() -> None:
    now = datetime(2026, 8, 18, 21, 22, tzinfo=UTC)
    fifteen = _gamma_crypto_market(
        market_id="fifteen",
        slug="btc-updown-15m-test",
        question="Bitcoin Up or Down - 15m",
        start=now - timedelta(minutes=1),
        end=now + timedelta(minutes=14),
    )
    fifteen["cryptoMarketConfig"] = {"asset": "btc", "duration": "15m"}
    eth = _gamma_crypto_market(
        market_id="eth",
        slug="eth-updown-5m-test",
        question="Ethereum Up or Down - 5m",
        start=now - timedelta(minutes=1),
        end=now + timedelta(minutes=4),
        asset="eth",
    )
    expired = _gamma_crypto_market(
        market_id="expired",
        slug="btc-updown-5m-expired",
        question="Bitcoin Up or Down - expired",
        start=now - timedelta(minutes=10),
        end=now - timedelta(seconds=1),
    )

    for raw, reason in ((fifteen, "not_btc_5m"), (eth, "not_btc"), (expired, "expired")):
        parsed, actual_reason = MarketDiscovery.evaluate_market(raw, now)
        assert parsed is None
        assert actual_reason == reason


def test_discovery_reports_incomplete_gamma_payload() -> None:
    parsed, reason = MarketDiscovery.evaluate_market(
        {
            "id": "incomplete",
            "conditionId": "condition-incomplete",
            "question": "Bitcoin Up or Down - 5m",
            "slug": "btc-updown-5m-incomplete",
            "eventStartTime": "2026-08-18T21:20:00Z",
            "endDate": "2026-08-18T21:25:00Z",
            "outcomes": '["Up", "Down"]',
        },
        datetime(2026, 8, 18, 21, 22, tzinfo=UTC),
    )
    assert parsed is None
    assert reason == "missing_token_ids"


@pytest.mark.asyncio
async def test_discovery_returns_none_when_no_current_btc_5m_is_available() -> None:
    now = datetime(2026, 8, 18, 21, 22, tzinfo=UTC)
    eth = _gamma_crypto_market(
        market_id="eth",
        slug="eth-updown-5m-test",
        question="Ethereum Up or Down - 5m",
        start=now - timedelta(minutes=1),
        end=now + timedelta(minutes=4),
        asset="eth",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"markets": [eth]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        discovery = MarketDiscovery(client, "https://gamma.example")
        found = await discovery.discover_btc_5m(now)

    assert found is None


def test_structural_arb_uses_executable_depth_and_risk_accepts() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    up = book("up-token", asks=((0.40, 10.0), (0.60, 10.0)))
    down = book("down-token", asks=((0.40, 10.0), (0.60, 10.0)))
    engine = StrategyEngine(StrategyConfig(sizing_capital=10, execution_buffer=0.0), FeeModel(0.0))
    opportunity_result = engine.evaluate(
        market(now), tick(now), {"up-token": up, "down-token": down}, [tick(now)], now
    )[0]
    assert opportunity_result.strategy == "STRUCTURAL_ARB"
    assert opportunity_result.quantity == pytest.approx(12.5)
    assert opportunity_result.executable_price == pytest.approx(0.88)
    assert opportunity_result.net_edge == pytest.approx(0.12)
    risk = RiskEngine(RiskConfig(minimum_net_edge=0.1, minimum_liquidity=5))
    decision = risk.evaluate(
        opportunity_result,
        data_age_ms=0,
        execution_latency_ms=1,
        now=now,
        orderbook_coherent=True,
    )
    assert decision.accepted


def test_late_probability_is_testable_and_no_future_volatility_leak() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    history = [tick(now - timedelta(seconds=i), 100 + i * 0.01) for i in range(7, 0, -1)]
    current = tick(now, 100.2)
    future = tick(now + timedelta(seconds=1), 10_000)
    before_future = annualized_realized_volatility(history + [current], now, 60, 365 * 24 * 3600)
    with_future = annualized_realized_volatility(
        history + [current, future], now, 60, 365 * 24 * 3600
    )
    assert with_future == pytest.approx(before_future)
    probability = StrategyEngine._probability_above_barrier(101, 100, 0.2, 30)
    assert probability > 0.5
    assert StrategyEngine._probability_above_barrier(101, 100, 0.2, 0) == 1.0
    assert StrategyEngine._probability_above_barrier(99, 100, 0.2, 0) == 0.0


def test_partial_fill_and_paper_pnl() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    trade = PaperExecutor(mode="PAPER").execute(
        opportunity(), {"up-token": book("up-token", asks=((0.4, 2.0),))}, now
    )
    assert trade.status == "PARTIAL"
    assert trade.quantity == pytest.approx(2.0)
    PaperExecutor.settle(trade, "UP", now + timedelta(seconds=30))
    assert trade.gross_pnl == pytest.approx(1.2)
    assert trade.net_pnl == pytest.approx(1.2)

    pair = PaperExecutor(mode="PAPER").execute(
        opportunity("BUY_UP_AND_DOWN", quantity=5, capital=4),
        {
            "up-token": book("up-token", asks=((0.4, 5.0),)),
            "down-token": book("down-token", asks=((0.4, 5.0),)),
        },
        now,
    )
    PaperExecutor.settle(pair, "DOWN", now + timedelta(seconds=30))
    assert pair.gross_pnl == pytest.approx(1.0)


def test_risk_stale_data_daily_loss_and_failed_order_breaker() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    risk = RiskEngine(
        RiskConfig(
            max_daily_loss=10,
            failed_orders_before_breaker=2,
            minimum_net_edge=0.1,
            minimum_liquidity=1,
        )
    )
    rejected = risk.evaluate(
        opportunity(), data_age_ms=10_000, execution_latency_ms=0, now=now
    )
    assert not rejected.accepted and rejected.reason == "stale_data"
    assert risk.state.market_data_breaker
    assert not risk.state.circuit_breaker

    risk = RiskEngine(RiskConfig(max_daily_loss=10))
    risk.register_closed(-11.0, 0.0)
    assert risk.state.circuit_breaker
    assert risk.state.breaker_reason == "daily_loss_limit"

    risk = RiskEngine(RiskConfig(failed_orders_before_breaker=2))
    risk.register_failed_order()
    risk.register_failed_order()
    assert risk.state.breaker_reason == "failed_order_limit"

    risk = RiskEngine(RiskConfig(starting_capital=1.0, max_capital_per_trade=10.0))
    decision = risk.evaluate(opportunity(capital=2.0), data_age_ms=0, execution_latency_ms=0, now=now)
    assert not decision.accepted and decision.reason == "insufficient_starting_capital"


@pytest.mark.parametrize(
    ("config", "kwargs", "reason"),
    [
        (RiskConfig(maximum_execution_latency_ms=1), {"execution_latency_ms": 2}, "execution_latency_too_high"),
        (RiskConfig(minimum_liquidity=20), {"execution_latency_ms": 0}, "insufficient_liquidity"),
        (RiskConfig(minimum_net_edge=0.3), {"execution_latency_ms": 0}, "insufficient_edge"),
        (RiskConfig(max_capital_per_trade=3), {"execution_latency_ms": 0}, "max_capital_per_trade"),
        (RiskConfig(max_simultaneous_exposure=3), {"execution_latency_ms": 0}, "max_simultaneous_exposure"),
    ],
)
def test_risk_rejects_operational_limits(config: RiskConfig, kwargs: dict, reason: str) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    risk = RiskEngine(config)
    if reason == "max_simultaneous_exposure":
        risk.register_open(1.0)
    decision = risk.evaluate(opportunity(), data_age_ms=0, now=now, **kwargs)
    assert not decision.accepted
    assert decision.reason == reason


def test_risk_rejects_expired_market() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    decision = RiskEngine(RiskConfig()).evaluate(
        opportunity(), data_age_ms=0, execution_latency_ms=0, now=now, market_open=False
    )
    assert not decision.accepted
    assert decision.reason == "market_closed"


def test_market_data_breaker_requires_three_safe_observations_to_recover() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    risk = RiskEngine(RiskConfig(market_data_recovery_observations=3))

    stale = risk.evaluate(
        opportunity(), data_age_ms=2_001, execution_latency_ms=0, now=now
    )
    assert not stale.accepted
    assert stale.reason == "stale_data"
    assert risk.state.market_data_breaker
    assert not risk.state.circuit_breaker

    for streak in (1, 2):
        pending = risk.evaluate(
            opportunity(), data_age_ms=0, execution_latency_ms=0, now=now
        )
        assert not pending.accepted
        assert pending.reason == "market_data_recovery_pending"
        assert risk.state.market_data_recovery_streak == streak

    recovered = risk.evaluate(
        opportunity(), data_age_ms=0, execution_latency_ms=0, now=now
    )
    assert recovered.accepted
    assert not risk.state.market_data_breaker
    assert risk.state.market_data_recoveries == 1


def test_incoherent_orderbook_triggers_only_market_data_breaker() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    risk = RiskEngine(RiskConfig())

    decision = risk.evaluate(
        opportunity(),
        data_age_ms=0,
        execution_latency_ms=0,
        now=now,
        orderbook_coherent=False,
    )

    assert not decision.accepted
    assert decision.reason == "incoherent_orderbook"
    assert risk.state.market_data_breaker
    assert not risk.state.circuit_breaker


def test_failed_order_breaker_is_separate_and_not_released_by_data_recovery() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    risk = RiskEngine(
        RiskConfig(failed_orders_before_breaker=2, market_data_recovery_observations=2)
    )
    risk.evaluate(opportunity(), data_age_ms=2_001, execution_latency_ms=0, now=now)
    risk.register_failed_order()
    risk.register_failed_order()

    assert risk.state.market_data_breaker
    assert risk.state.circuit_breaker
    for _ in range(3):
        blocked = risk.evaluate(
            opportunity(), data_age_ms=0, execution_latency_ms=0, now=now
        )
        assert not blocked.accepted
        assert blocked.reason == "failed_order_limit"
    assert risk.state.market_data_breaker
    assert risk.state.market_data_recoveries == 0


def test_daily_loss_breaker_is_separate_from_market_data_breaker() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    risk = RiskEngine(RiskConfig(max_daily_loss=10))
    risk.register_closed(-11.0, 0.0)
    stale = risk.evaluate(
        opportunity(), data_age_ms=2_001, execution_latency_ms=0, now=now
    )

    assert not stale.accepted
    assert stale.reason == "daily_loss_limit"
    assert risk.state.circuit_breaker
    assert not risk.state.market_data_breaker
