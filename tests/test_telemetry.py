import json
import os
from pathlib import Path

import pytest

from clients.telemetry import BaseTelemetry


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
    t.stop()    # second call must not raise


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


def test_invalid_source_rejected(tmp_path):
    with pytest.raises(ValueError, match="source must be one of"):
        BaseTelemetry(source="bogus", jsonl_path=tmp_path / "t.jsonl")
