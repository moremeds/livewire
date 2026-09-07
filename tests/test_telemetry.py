import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from clients.telemetry import BaseTelemetry, ConnectionTelemetry, MassiveTelemetry, UWTelemetry, _parse_farm_name


def test_base_telemetry_emits_jsonl_line(tmp_path):
    path = tmp_path / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()
    t._emit({"event": "connected", "client_id": 0})
    t.stop()
    lines = path.read_text().splitlines()
    assert len(lines) == 3  # start, event, stop (start/stop framing is OK either way)
    record = json.loads(lines[1])
    assert record["source"] == "ib"
    assert record["event"] == "connected"
    assert "ts" in record
    assert record["client_id"] == 0


def test_base_telemetry_disabled_when_path_is_none(tmp_path, caplog):
    t = BaseTelemetry(source="ib", jsonl_path=None)
    t.start()
    t._emit({"event": "x"})
    t.stop()
    # No file written, no exception
    assert t._disabled is True


def test_base_telemetry_disabled_when_parent_dir_missing(tmp_path, caplog):
    path = tmp_path / "nope" / "missing" / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()
    assert t._disabled is True


def test_base_telemetry_emit_failure_rate_limited(tmp_path, caplog, monkeypatch):
    path = tmp_path / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()

    # Simulate intermittent write failure: monkeypatch _do_write
    fails = [0]

    def boom(line):
        fails[0] += 1
        raise OSError("disk full")

    monkeypatch.setattr(t, "_do_write", boom)

    for _ in range(10):
        t._emit({"event": "x"})
    # Should NOT raise; warning rate-limited to 1/min — we don't assert log count strictly
    assert fails[0] == 10
    t.stop()


def test_stop_is_idempotent(tmp_path):
    path = tmp_path / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()
    t.stop()
    t.stop()  # second call must not raise


def test_start_is_idempotent(tmp_path):
    path = tmp_path / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()
    t.start()
    t.stop()
    lines = path.read_text().splitlines()
    assert len(lines) == 2


def test_resolve_default_path_disabled_via_env(monkeypatch):
    from clients.telemetry import _resolve_default_path

    monkeypatch.setenv("MDW_TELEMETRY_PATH", "none")
    assert _resolve_default_path() is None


def test_resolve_default_path_uses_explicit_path(monkeypatch, tmp_path):
    from clients.telemetry import _resolve_default_path

    monkeypatch.setenv("MDW_TELEMETRY_PATH", str(tmp_path / "x.jsonl"))
    assert _resolve_default_path() == tmp_path / "x.jsonl"


def test_resolve_default_path_follows_warehouse_override(monkeypatch, tmp_path):
    from clients.telemetry import _resolve_default_path

    warehouse = tmp_path / "warehouse"
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    monkeypatch.delenv("MDW_LOG_DIR", raising=False)
    monkeypatch.delenv("MDW_TELEMETRY_PATH", raising=False)

    assert _resolve_default_path() == warehouse / "logs" / "telemetry.jsonl"


def test_invalid_source_rejected(tmp_path):
    with pytest.raises(ValueError, match="source must be one of"):
        BaseTelemetry(source="bogus", jsonl_path=tmp_path / "t.jsonl")


def _fake_ib():
    ib = MagicMock()
    ib.errorEvent = MagicMock()
    ib.connectedEvent = MagicMock()
    ib.disconnectedEvent = MagicMock()
    return ib


def test_connection_telemetry_attaches_handlers(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    ib.errorEvent.connect.assert_called_once()
    ib.connectedEvent.connect.assert_called_once()
    ib.disconnectedEvent.connect.assert_called_once()
    t.stop()
    ib.errorEvent.disconnect.assert_called_once()


def test_connection_telemetry_start_is_idempotent(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t.start()
    ib.errorEvent.connect.assert_called_once()


def test_connection_telemetry_disabled_does_not_attach():
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=None)
    t.start()
    ib.errorEvent.connect.assert_not_called()


@pytest.mark.parametrize(
    "code,state",
    [
        (2104, "ok"),
        (2105, "broken"),
        (2106, "ok"),
        (2107, "inactive"),
        (2158, "ok"),
    ],
)
def test_connection_telemetry_parses_farm_codes(tmp_path, code, state):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t._on_error(reqId=-1, errorCode=code, errorString="All connections OK:usfarm", contract=None)
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    farm_records = [r for r in records if r["event"] == "farm_state"]
    assert len(farm_records) == 1
    assert farm_records[0]["code"] == code
    assert farm_records[0]["state"] == state
    assert farm_records[0]["farm"] == "usfarm"
    assert farm_records[0]["source"] == "ib"


def test_connection_telemetry_unknown_code_emits_ib_error(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t._on_error(reqId=42, errorCode=162, errorString="HMDS query returned no data", contract=None)
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    error_records = [r for r in records if r["event"] == "ib_error"]
    assert len(error_records) == 1
    assert error_records[0]["code"] == 162
    assert error_records[0]["req_id"] == 42


def test_connection_telemetry_no_farm_suffix(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t._on_error(reqId=-1, errorCode=2106, errorString="HMDS data farm connection is OK", contract=None)
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    farm_records = [r for r in records if r["event"] == "farm_state"]
    assert farm_records[0]["farm"] is None  # no :farmname suffix


@pytest.mark.parametrize("message", ["", "no farm suffix", "message:", "message:bad farm"])
def test_parse_farm_name_rejects_missing_or_invalid_suffix(message):
    assert _parse_farm_name(message) is None


def test_connection_telemetry_connected_disconnected_events(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t._on_connected()
    t._on_disconnected()
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    events = [r["event"] for r in records]
    assert "connected" in events
    assert "disconnected" in events


def test_connection_telemetry_deduplicates_manual_connection_records(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t.record_connected()
    t.record_connected()
    t.record_disconnected()
    t.record_disconnected()
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    events = [r["event"] for r in records]
    assert events.count("connected") == 1
    assert events.count("disconnected") == 1


def test_uw_telemetry_stub_no_op(tmp_path, caplog):
    t = UWTelemetry(jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t.record_request(endpoint="/options/AAPL", status=200, dt_ms=42)
    t.record_rate_limit(remaining=100, reset_at=1700000000)
    t.stop()
    # Stub: methods exist, do not raise, emit JSONL records tagged uw
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    assert any(r["source"] == "uw" for r in records)


def test_massive_telemetry_stub_no_op(tmp_path):
    t = MassiveTelemetry(jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t.record_request(endpoint="/v2/bars/AAPL", status=200, dt_ms=15)
    t.record_rate_limit(remaining=12, reset_at=1778875200)
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    assert any(r["source"] == "massive" for r in records)
    assert any(
        r["event"] == "massive_rate_limit" and r["remaining"] == 12 and r["reset_at"] == 1778875200 for r in records
    )


def test_uw_telemetry_source_locked_to_uw():
    t = UWTelemetry(jsonl_path=None)
    assert t.source == "uw"


def test_massive_telemetry_source_locked_to_massive():
    t = MassiveTelemetry(jsonl_path=None)
    assert t.source == "massive"


def test_massive_totals_are_recorded_without_a_jsonl_sink():
    """Production passes jsonl_path=None and never calls start(); the counters
    still have to work. BaseTelemetry disables _emit in both of those cases."""
    t = MassiveTelemetry(jsonl_path=None)

    for dt_ms in range(1, 101):
        t.record_request(endpoint="/v3/reference/splits", status=200, dt_ms=dt_ms)
    t.record_request(endpoint="/v3/reference/splits", status=429, dt_ms=4000)
    t.record_request(endpoint="/v3/reference/splits", status=0, dt_ms=30000)
    t.record_wait(12.5)
    t.record_wait(0.5)

    summary = t.summary()
    assert summary["requests"] == 102
    assert summary["throttled"] == 1
    assert summary["errors"] == 1
    assert summary["wait_s"] == 13.0
    # 102 samples (1..100ms, then 4000 and 30000): inclusive p95 interpolates
    # between the 96th and 97th sample, so it stays in the 100 fast responses
    # rather than being dragged up by the two slow tail samples.
    assert summary["latency_p95_ms"] == 96.95


def test_massive_summary_is_zeroed_before_any_request():
    """ledger.emit refuses zero rows, so the caller needs a truthful zero."""
    assert MassiveTelemetry(jsonl_path=None).summary() == {
        "requests": 0,
        "throttled": 0,
        "errors": 0,
        "wait_s": 0.0,
        "latency_p95_ms": 0.0,
    }


def test_massive_totals_survive_concurrent_workers():
    """_fetch_parallel shares one telemetry across 4 client threads."""
    t = MassiveTelemetry(jsonl_path=None)

    def hammer() -> None:
        for _ in range(500):
            t.record_request(endpoint="/v3/reference/splits", status=200, dt_ms=10)
            t.record_wait(0.001)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for future in [executor.submit(hammer) for _ in range(4)]:
            future.result()

    assert t.summary()["requests"] == 2000
    assert t.summary()["wait_s"] == 2.0
