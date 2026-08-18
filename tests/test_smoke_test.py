import httpx

from scripts.polymarket_smoke_test import (
    classify_request_exception,
    inspect_json_body,
    parse_websocket_event_types,
)


def test_smoke_parser_accepts_json_with_json_content_type() -> None:
    inspection = inspect_json_body(b'{"markets": []}', "application/json; charset=utf-8")
    assert inspection.valid_json
    assert inspection.content_type_is_json
    assert inspection.kind == "json"
    assert inspection.payload == {"markets": []}


def test_smoke_parser_identifies_html_instead_of_json() -> None:
    inspection = inspect_json_body(b"<!doctype html><html><body>blocked</body></html>", "text/html")
    assert not inspection.valid_json
    assert inspection.kind == "html"
    assert "blocked" in inspection.preview


def test_smoke_parser_identifies_empty_response() -> None:
    inspection = inspect_json_body(b"", "application/json")
    assert not inspection.valid_json
    assert inspection.kind == "empty"


def test_smoke_parser_flags_json_with_wrong_content_type() -> None:
    inspection = inspect_json_body(b'{"ok": true}', "text/plain")
    assert inspection.valid_json
    assert not inspection.content_type_is_json
    assert inspection.kind == "wrong-content-type"


def test_smoke_parser_classifies_timeout() -> None:
    assert classify_request_exception(httpx.ReadTimeout("diagnostic timeout")) == "timeout"


def test_smoke_websocket_type_parser_handles_dict_and_list_roots() -> None:
    assert parse_websocket_event_types({"event_type": "book"}) == ["book"]
    assert parse_websocket_event_types(
        [{"type": "book", "payload": {}}, {"event_type": "price_change"}]
    ) == ["book", "price_change"]
    assert parse_websocket_event_types([]) == []
    assert parse_websocket_event_types("not-json") == []
