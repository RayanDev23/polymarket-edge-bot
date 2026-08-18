"""Read-only Polymarket connectivity diagnostic.

This module intentionally does not import the trading bot and contains no
authentication, wallet, signing, or order-placement code.  It diagnoses the
public Gamma API, public CLOB order book endpoint, and public market WebSocket.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import socket
import ssl
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

try:
    import websockets
except ImportError:  # pragma: no cover - dependency is declared in requirements.txt
    websockets = None


GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_TIMEOUT_SECONDS = 10.0
MAX_PREVIEW_LENGTH = 260


@dataclass(frozen=True)
class JsonInspection:
    payload: Any | None
    valid_json: bool
    content_type_is_json: bool
    kind: str
    preview: str


@dataclass
class HttpCheck:
    label: str
    requested_url: str
    status_code: int | None = None
    response_time_ms: float | None = None
    content_type: str = ""
    content_length: str = "n/a"
    final_url: str | None = None
    redirect_chain: list[str] | None = None
    server: str | None = None
    cf_ray: str | None = None
    location: str | None = None
    inspection: JsonInspection | None = None
    error: str | None = None

    @property
    def http_ok(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 300

    @property
    def json_ok(self) -> bool:
        return bool(
            self.inspection
            and self.inspection.valid_json
            and self.inspection.content_type_is_json
        )


def _safe_preview(text: str, limit: int = MAX_PREVIEW_LENGTH) -> str:
    compact = re.sub(r"\s+", " ", text).strip()
    compact = re.sub(
        r"(?i)(api[_-]?key|secret|password|private[_-]?key|authorization|bearer)"
        r"\s*[:=]\s*[^,; ]+",
        r"\1=<redacted>",
        compact,
    )
    return compact[:limit]


def inspect_json_body(body: bytes, content_type: str | None) -> JsonInspection:
    """Inspect a response body without hiding HTML or malformed JSON."""

    text = body.decode("utf-8", errors="replace").strip()
    preview = _safe_preview(text)
    normalized_content_type = (content_type or "").lower()
    content_type_is_json = (
        "application/json" in normalized_content_type
        or "+json" in normalized_content_type
    )
    if not text:
        return JsonInspection(None, False, content_type_is_json, "empty", preview)

    looks_html = bool(re.match(r"(?is)^(<!doctype\s+html|<html|<head|<body|<script)", text))
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        kind = "html" if looks_html or "html" in normalized_content_type else "invalid-json"
        return JsonInspection(None, False, content_type_is_json, kind, preview)

    if not content_type_is_json:
        return JsonInspection(payload, True, False, "wrong-content-type", preview)
    return JsonInspection(payload, True, True, "json", preview)


def classify_request_exception(exc: BaseException) -> str:
    """Return a stable, testable category for network exceptions."""

    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, (ssl.SSLError,)):  # pragma: no cover - usually wrapped by httpx
        return "tls"
    if isinstance(exc, socket.gaierror):
        return "dns"
    if isinstance(exc, httpx.HTTPError):
        return "http-client"
    return type(exc).__name__


def _format_params(url: str, params: dict[str, Any] | None) -> str:
    return str(httpx.URL(url, params=params or {}))


def request_json(
    client: httpx.Client,
    label: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
) -> HttpCheck:
    requested_url = _format_params(url, params)
    check = HttpCheck(label=label, requested_url=requested_url)
    print(f"\n[{label}]")
    print(f"URL: {requested_url}")
    started = time.perf_counter()
    try:
        response = client.get(url, params=params)
    except Exception as exc:  # noqa: BLE001 - diagnostic must report all network failures
        check.response_time_ms = (time.perf_counter() - started) * 1000.0
        check.error = f"{classify_request_exception(exc)}: {exc}"
        print(f"HTTP: FAIL ({check.error})")
        print(f"response time: {check.response_time_ms:.1f} ms")
        return check

    check.response_time_ms = (time.perf_counter() - started) * 1000.0
    check.status_code = response.status_code
    check.content_type = response.headers.get("content-type", "")
    check.content_length = response.headers.get("content-length", str(len(response.content)))
    check.final_url = str(response.url)
    check.redirect_chain = [str(item.url) for item in response.history]
    check.server = response.headers.get("server")
    check.cf_ray = response.headers.get("cf-ray")
    check.location = response.headers.get("location")
    check.inspection = inspect_json_body(response.content, check.content_type)

    print(f"HTTP status: {check.status_code}")
    print(f"Content-Type: {check.content_type or 'n/a'}")
    print(f"Content-Length: {check.content_length}")
    print(f"response time: {check.response_time_ms:.1f} ms")
    print(f"final URL: {check.final_url}")
    if check.redirect_chain:
        print(f"redirect chain: {' -> '.join(check.redirect_chain)}")
    if check.server:
        print(f"Server: {check.server}")
    if check.cf_ray:
        print(f"CF-Ray: {check.cf_ray}")
    if check.location:
        print(f"Location: {check.location}")

    inspection = check.inspection
    assert inspection is not None
    if inspection.valid_json:
        print(
            "JSON parsing: PASS"
            + (" (Content-Type is not JSON)" if not inspection.content_type_is_json else "")
        )
    else:
        print(f"JSON parsing: FAIL ({inspection.kind})")
        if inspection.kind == "html":
            print("EXPECTED JSON")
            print("RECEIVED HTML")
        if inspection.kind == "empty":
            print("RECEIVED EMPTY RESPONSE")
        if inspection.preview:
            print(f"response preview: {inspection.preview}")
    return check


def dns_probe(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme in {"https", "wss"} else 80)
    started = time.perf_counter()
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        addresses = sorted({str(info[4][0]) for info in infos})
        elapsed = (time.perf_counter() - started) * 1000.0
        print(f"DNS {host}: PASS ({elapsed:.1f} ms; {', '.join(addresses[:4])})")
        return True
    except Exception as exc:  # noqa: BLE001 - diagnostic must expose resolver failures
        elapsed = (time.perf_counter() - started) * 1000.0
        print(f"DNS {host}: FAIL ({type(exc).__name__}: {exc}; {elapsed:.1f} ms)")
        return False


def tls_probe(url: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme in {"https", "wss"} else 80)
    started = time.perf_counter()
    raw_socket: socket.socket | None = None
    try:
        context = ssl.create_default_context()
        raw_socket = socket.create_connection((host, port), timeout=timeout_seconds)
        with context.wrap_socket(raw_socket, server_hostname=host) as tls_socket:
            elapsed = (time.perf_counter() - started) * 1000.0
            print(f"TLS {host}: PASS ({tls_socket.version()}; {elapsed:.1f} ms)")
        return True
    except Exception as exc:  # noqa: BLE001 - diagnostic must expose TLS failures
        elapsed = (time.perf_counter() - started) * 1000.0
        print(f"TLS {host}: FAIL ({type(exc).__name__}: {exc}; {elapsed:.1f} ms)")
        if raw_socket is not None:
            raw_socket.close()
        return False


def extract_markets(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        nested = payload.get("markets", payload.get("data", []))
        return [item for item in nested if isinstance(item, dict)] if isinstance(nested, list) else []
    return []


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def market_token_ids(market: dict[str, Any]) -> list[str]:
    outcomes = [str(item).strip().lower() for item in parse_json_list(market.get("outcomes"))]
    token_values = [str(item) for item in parse_json_list(market.get("clobTokenIds"))]
    by_outcome: dict[str, str] = {}
    tokens = market.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            token_id = token.get("token_id") or token.get("tokenId") or token.get("id")
            outcome = token.get("outcome") or token.get("name")
            if token_id and outcome:
                by_outcome[str(outcome).strip().lower()] = str(token_id)
    if not by_outcome and len(outcomes) == len(token_values):
        by_outcome = dict(zip(outcomes, token_values))
    ordered = [
        by_outcome[name]
        for name in ("up", "down", "yes", "no", "true", "false")
        if name in by_outcome
    ]
    return ordered or list(by_outcome.values()) or token_values


def print_market_summary(markets: list[dict[str, Any]], limit: int) -> str | None:
    print(f"markets found: {len(markets)}")
    selected_token: str | None = None
    for market in markets[:limit]:
        tokens = market_token_ids(market)
        question = _safe_preview(str(market.get("question") or ""), 150)
        print(
            "market id={id} question={question!r} active={active} closed={closed} "
            "start={start} end={end} tokens={tokens}".format(
                id=market.get("id") or market.get("marketId") or "n/a",
                question=question,
                active=market.get("active"),
                closed=market.get("closed"),
                start=market.get("startDate") or market.get("start_date") or "n/a",
                end=market.get("endDate") or market.get("end_date") or "n/a",
                tokens=tokens[:4],
            )
        )
        if selected_token is None and tokens and market.get("closed") is not True:
            selected_token = tokens[0]
    if selected_token is None:
        for market in markets:
            tokens = market_token_ids(market)
            if tokens:
                selected_token = tokens[0]
                break
    print(f"market discovery token for CLOB test: {selected_token or 'none'}")
    return selected_token


def _orderbook_level(level: Any) -> tuple[float, float] | None:
    if not isinstance(level, dict):
        return None
    try:
        return float(level["price"]), float(level.get("size", level.get("quantity")))
    except (KeyError, TypeError, ValueError):
        return None


def inspect_orderbook(payload: Any) -> bool:
    if not isinstance(payload, dict):
        print("order book structure: FAIL (JSON root is not an object)")
        return False
    bids = payload.get("bids")
    asks = payload.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list):
        print("order book structure: FAIL (bids/asks are not arrays)")
        return False
    bid_levels = [level for item in bids if (level := _orderbook_level(item)) is not None]
    ask_levels = [level for item in asks if (level := _orderbook_level(item)) is not None]
    if len(bid_levels) != len(bids) or len(ask_levels) != len(asks):
        print("order book structure: FAIL (invalid price/size level)")
        return False
    best_bid = max(bid_levels, default=None, key=lambda item: item[0])
    best_ask = min(ask_levels, default=None, key=lambda item: item[0])
    print(f"order book timestamp: {payload.get('timestamp', 'n/a')}")
    print(f"best bid: {best_bid}")
    print(f"best ask: {best_ask}")
    print(f"top bids: {bid_levels[:3]}")
    print(f"top asks: {ask_levels[:3]}")
    print(f"order book structure: PASS (bids={len(bid_levels)} asks={len(ask_levels)})")
    return True


@dataclass(frozen=True)
class WebSocketCheck:
    connected: bool
    message_received: bool
    error: str | None = None


def parse_websocket_event_types(message: Any) -> list[str]:
    """Extract event types without assuming the WebSocket root is a dict."""

    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError:
            return []
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except json.JSONDecodeError:
            return []
    if isinstance(message, list):
        event_types: list[str] = []
        for item in message:
            event_types.extend(parse_websocket_event_types(item))
        return event_types
    if not isinstance(message, dict):
        return []
    event_type = message.get("event_type") or message.get("eventType") or message.get("type")
    if not event_type and isinstance(message.get("payload"), dict):
        event_type = message.get("type")
    return [str(event_type).lower()] if event_type else []


async def websocket_probe(token_id: str, timeout_seconds: float) -> WebSocketCheck:
    if websockets is None:
        error = "websockets dependency is not installed"
        print(f"WebSocket: FAIL ({error})")
        return WebSocketCheck(False, False, error)

    print("\n[Polymarket WebSocket]")
    print(f"connecting... {POLYMARKET_WS_URL}")
    started = time.perf_counter()
    try:
        async with websockets.connect(
            POLYMARKET_WS_URL,
            ping_interval=None,
            open_timeout=timeout_seconds,
            close_timeout=3,
        ) as websocket:
            print("connected")
            subscription = {
                "assets_ids": [token_id],
                "type": "market",
                "custom_feature_enabled": True,
            }
            await websocket.send(json.dumps(subscription))
            print("subscription sent")
            deadline = asyncio.get_running_loop().time() + timeout_seconds
            raw_messages_printed = 0
            while asyncio.get_running_loop().time() < deadline:
                remaining = max(0.1, deadline - asyncio.get_running_loop().time())
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=min(2.0, remaining))
                except asyncio.TimeoutError:
                    await websocket.send("PING")
                    continue
                if raw in {"PONG", "pong"}:
                    continue
                text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
                if raw_messages_printed < 3:
                    print("raw message received:")
                    print(f"RAW WS MESSAGE: {_safe_preview(text, 1000)}")
                    raw_messages_printed += 1
                event_types = parse_websocket_event_types(raw)
                if event_types:
                    print(f"parsed event type: {', '.join(event_types)}")
                    elapsed = (time.perf_counter() - started) * 1000.0
                    print(f"WebSocket elapsed before message: {elapsed:.1f} ms")
                    print("WebSocket: PASS")
                    return WebSocketCheck(True, True)
                print("raw message received but no valid event type was parsed")
            print("connected but no live message received before timeout")
            return WebSocketCheck(True, False, "no message before timeout")
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - diagnostic must expose handshake failures
        elapsed = (time.perf_counter() - started) * 1000.0
        error = f"{type(exc).__name__}: {exc}; failed after {elapsed:.1f} ms"
        print(f"WebSocket: FAIL ({error})")
        return WebSocketCheck(False, False, error)


def _payload_from_check(check: HttpCheck) -> Any | None:
    return check.inspection.payload if check.inspection and check.inspection.valid_json else None


def _print_summary(statuses: dict[str, bool], blockers: list[str]) -> int:
    print("\nRESULT SUMMARY")
    for label, passed in statuses.items():
        print(f"{label:<20} {'PASS' if passed else 'FAIL'}")
    overall = all(statuses.values())
    print("\nOVERALL RESULT")
    print("READY" if overall else "BLOCKED")
    if blockers:
        print("blocking layers:")
        for blocker in blockers:
            print(f"- {blocker}")
    return 0 if overall else 1


def run_diagnostic(max_markets: int, timeout_seconds: float) -> int:
    print("Polymarket smoke test: READ-ONLY diagnostics; no orders, keys, or wallet")
    endpoints = {"Gamma": GAMMA_URL, "CLOB": CLOB_URL, "WebSocket": POLYMARKET_WS_URL}
    dns_status = {name: dns_probe(url) for name, url in endpoints.items()}
    tls_status = {
        name: tls_probe(url, timeout_seconds) for name, url in endpoints.items()
    }

    gamma_keyset: HttpCheck
    gamma_fallback: HttpCheck | None = None
    markets: list[dict[str, Any]] = []
    with httpx.Client(
        timeout=timeout_seconds,
        follow_redirects=True,
        headers={"User-Agent": "polymarket-edge-bot-smoke-test/1.0"},
    ) as client:
        gamma_keyset = request_json(
            client,
            "Gamma markets/keyset",
            f"{GAMMA_URL}/markets/keyset",
            params={"closed": "false", "limit": 100},
        )
        markets = extract_markets(_payload_from_check(gamma_keyset))
        if not markets:
            gamma_fallback = request_json(
                client,
                "Gamma markets fallback",
                f"{GAMMA_URL}/markets",
                params={"active": "true", "closed": "false", "limit": 100},
            )
            markets = extract_markets(_payload_from_check(gamma_fallback))

        token_id = print_market_summary(markets, max_markets) if markets else None
        clob_check: HttpCheck | None = None
        orderbook_ok = False
        if token_id:
            clob_check = request_json(
                client,
                "CLOB order book",
                f"{CLOB_URL}/book",
                params={"token_id": token_id},
            )
            payload = _payload_from_check(clob_check)
            if payload is not None:
                orderbook_ok = inspect_orderbook(payload)
        else:
            print("\n[CLOB order book]\nSKIPPED: Gamma did not provide a valid token ID")

    ws_check = asyncio.run(websocket_probe(token_id, timeout_seconds)) if token_id else WebSocketCheck(
        False, False, "skipped because Gamma did not provide a token ID"
    )

    gamma_http_ok = gamma_keyset.http_ok or bool(gamma_fallback and gamma_fallback.http_ok)
    gamma_json_ok = gamma_keyset.json_ok or bool(gamma_fallback and gamma_fallback.json_ok)
    clob_http_ok = bool(clob_check and clob_check.http_ok)
    clob_json_ok = bool(clob_check and clob_check.json_ok)
    blockers: list[str] = []
    if not all(dns_status.values()):
        blockers.append("DNS resolution failed for at least one Polymarket host")
    if not all(tls_status.values()):
        blockers.append("TLS certificate/handshake failed for at least one Polymarket host")
    if not gamma_http_ok:
        blockers.append("Gamma HTTP endpoint unavailable")
    if not gamma_json_ok:
        blockers.append("Gamma response is not usable JSON")
    if not markets:
        blockers.append("No market/token ID was discovered from Gamma")
    if token_id and not clob_http_ok:
        blockers.append("CLOB order-book HTTP endpoint unavailable")
    if token_id and not clob_json_ok:
        blockers.append("CLOB order-book response is not usable JSON")
    if token_id and not orderbook_ok:
        blockers.append("CLOB order-book structure is invalid")
    if not ws_check.connected:
        blockers.append(f"Polymarket WebSocket connection failed: {ws_check.error}")
    if token_id and not ws_check.message_received:
        blockers.append(f"No live WebSocket message received: {ws_check.error}")

    statuses = {
        "DNS": all(dns_status.values()),
        "TLS": all(tls_status.values()),
        "Gamma HTTP": gamma_http_ok,
        "Gamma JSON": gamma_json_ok,
        "Market discovery": bool(markets),
        "CLOB HTTP": clob_http_ok if token_id else False,
        "CLOB JSON": clob_json_ok if token_id else False,
        "Order book": orderbook_ok if token_id else False,
        "WebSocket": ws_check.connected if token_id else False,
        "Live message": ws_check.message_received if token_id else False,
    }
    return _print_summary(statuses, blockers)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Polymarket network smoke test")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--max-markets", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.timeout <= 0 or args.max_markets <= 0:
        print("timeout and max-markets must be positive")
        return 2
    return run_diagnostic(args.max_markets, args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
