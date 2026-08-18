"""Configuration for the paper-only research bot.

The configuration deliberately contains no credentials.  V1 has no private
key, signer, or live execution setting by design.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value in (None, "") else float(value)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value in (None, "") else int(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class RiskConfig:
    starting_capital: float = 1_000.0
    max_capital_per_trade: float = 25.0
    max_simultaneous_exposure: float = 100.0
    max_daily_loss: float = 50.0
    max_consecutive_losses: int = 5
    minimum_net_edge: float = 0.01
    minimum_liquidity: float = 5.0
    maximum_data_age_ms: float = 2_000.0
    maximum_execution_latency_ms: float = 1_500.0
    failed_orders_before_breaker: int = 3


@dataclass(frozen=True)
class StrategyConfig:
    # These are explicit research parameters, not hidden model constants.
    volatility_lookback: int = 60
    minimum_volatility_observations: int = 5
    volatility_fallback_annualized: float = 0.0
    annualization_seconds: float = 365.0 * 24.0 * 60.0 * 60.0
    execution_buffer: float = 0.005
    sizing_capital: float = 25.0
    max_time_remaining_for_late_market_s: float = 90.0


@dataclass(frozen=True)
class AppConfig:
    mode: str = "PAPER"
    database_path: Path = Path("paper_trading.sqlite3")
    log_level: str = "INFO"
    retention_days: int = 14
    market_poll_seconds: float = 15.0
    book_refresh_seconds: float = 5.0
    network_timeout_seconds: float = 10.0
    http_verify: bool = True
    binance_symbol: str = "BTCUSDT"
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    clob_api_url: str = "https://clob.polymarket.com"
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    # Current Polymarket docs describe the crypto taker rate as 0.07.  The
    # market-specific value is preferred when the discovery payload supplies it.
    polymarket_taker_fee_rate: float = 0.07
    polymarket_fee_exponent: float = 1.0
    polymarket_fees_enabled: bool = True
    risk: RiskConfig = RiskConfig()
    strategy: StrategyConfig = StrategyConfig()

    def assert_paper_only(self) -> None:
        if self.mode.upper() != "PAPER":
            raise RuntimeError("V1 is paper-only; MODE must be PAPER")


def load_config(env_file: str | Path | None = ".env") -> AppConfig:
    """Load visible configuration from environment variables and .env."""

    if env_file:
        load_dotenv(Path(env_file), override=False)

    risk = RiskConfig(
        starting_capital=_env_float("STARTING_CAPITAL", 1_000.0),
        max_capital_per_trade=_env_float("MAX_CAPITAL_PER_TRADE", 25.0),
        max_simultaneous_exposure=_env_float("MAX_SIMULTANEOUS_EXPOSURE", 100.0),
        max_daily_loss=_env_float("MAX_DAILY_LOSS", 50.0),
        max_consecutive_losses=_env_int("MAX_CONSECUTIVE_LOSSES", 5),
        minimum_net_edge=_env_float("MINIMUM_NET_EDGE", 0.01),
        minimum_liquidity=_env_float("MINIMUM_LIQUIDITY", 5.0),
        maximum_data_age_ms=_env_float("MAXIMUM_DATA_AGE_MS", 2_000.0),
        maximum_execution_latency_ms=_env_float("MAXIMUM_EXECUTION_LATENCY_MS", 1_500.0),
        failed_orders_before_breaker=_env_int("FAILED_ORDERS_BEFORE_BREAKER", 3),
    )
    strategy = StrategyConfig(
        volatility_lookback=_env_int("VOLATILITY_LOOKBACK", 60),
        minimum_volatility_observations=_env_int("MINIMUM_VOLATILITY_OBSERVATIONS", 5),
        volatility_fallback_annualized=_env_float("VOLATILITY_FALLBACK_ANNUALIZED", 0.0),
        execution_buffer=_env_float("EXECUTION_BUFFER", 0.005),
        sizing_capital=_env_float("SIZING_CAPITAL", risk.max_capital_per_trade),
        max_time_remaining_for_late_market_s=_env_float(
            "MAX_TIME_REMAINING_FOR_LATE_MARKET_S", 90.0
        ),
    )
    binance_symbol = os.getenv("BINANCE_SYMBOL", "BTCUSDT").upper()
    config = AppConfig(
        mode=os.getenv("MODE", "PAPER").upper(),
        database_path=Path(os.getenv("DATABASE_PATH", "paper_trading.sqlite3")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        retention_days=_env_int("RETENTION_DAYS", 14),
        market_poll_seconds=_env_float("MARKET_POLL_SECONDS", 15.0),
        book_refresh_seconds=_env_float("BOOK_REFRESH_SECONDS", 5.0),
        network_timeout_seconds=_env_float("NETWORK_TIMEOUT_SECONDS", 10.0),
        http_verify=_env_bool("HTTP_VERIFY", True),
        binance_symbol=binance_symbol,
        binance_ws_url=os.getenv(
            "BINANCE_WS_URL",
            f"wss://stream.binance.com:9443/ws/{binance_symbol.lower()}@bookTicker",
        ),
        gamma_api_url=os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com"),
        clob_api_url=os.getenv("CLOB_API_URL", "https://clob.polymarket.com"),
        polymarket_ws_url=os.getenv(
            "POLYMARKET_WS_URL",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        ),
        polymarket_taker_fee_rate=_env_float("POLYMARKET_TAKER_FEE_RATE", 0.07),
        polymarket_fee_exponent=_env_float("POLYMARKET_FEE_EXPONENT", 1.0),
        polymarket_fees_enabled=_env_bool("POLYMARKET_FEES_ENABLED", True),
        risk=risk,
        strategy=strategy,
    )
    config.assert_paper_only()
    return config
