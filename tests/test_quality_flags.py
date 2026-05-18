import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from clients.quality_detector import QualityFlag
from clients.quality_flags import alert_on_flag, append_audit, _resolve_audit_path, write_sidecar


def _flag(category="range_shortfall", severity="critical"):
    return QualityFlag(
        category=category,
        severity=severity,
        detail={"k": "v"},
        ts="2026-05-17T00:00:00Z",
    )


def test_write_sidecar_atomic_temp_then_replace(tmp_path):
    parquet = tmp_path / "1d.parquet"
    parquet.write_bytes(b"")  # placeholder
    metadata = {
        "ticker": "SMH",
        "timeframe": "1d",
        "source": "ib",
        "bars_received": 1758,
    }
    ok = write_sidecar(parquet, [_flag()], metadata)
    assert ok is True
    sidecar = parquet.with_suffix(".parquet.meta.json")
    assert sidecar.exists()
    payload = json.loads(sidecar.read_text())
    assert payload["ticker"] == "SMH"
    assert payload["flags"][0]["category"] == "range_shortfall"
    assert payload["bars_received"] == 1758


def test_write_sidecar_includes_parquet_path_relative(tmp_path):
    parquet = tmp_path / "symbol=SMH" / "1d.parquet"
    parquet.parent.mkdir()
    parquet.write_bytes(b"")
    write_sidecar(parquet, [_flag()], {"ticker": "SMH", "timeframe": "1d", "source": "ib"})
    payload = json.loads(parquet.with_suffix(".parquet.meta.json").read_text())
    assert payload["parquet_path"].endswith("symbol=SMH/1d.parquet")


def test_write_sidecar_oserror_returns_false(tmp_path, monkeypatch, caplog):
    parquet = tmp_path / "1d.parquet"
    parquet.write_bytes(b"")

    def boom(*a, **kw):
        raise OSError("disk full")

    monkeypatch.setattr("os.replace", boom)
    ok = write_sidecar(parquet, [_flag()], {"ticker": "X", "timeframe": "1d", "source": "ib"})
    assert ok is False


def test_append_audit_writes_one_jsonl_line(tmp_path, monkeypatch):
    audit = tmp_path / "audit.jsonl"
    monkeypatch.setenv("MDW_QUALITY_AUDIT_PATH", str(audit))
    ok = append_audit(_flag(), source="ib", ticker="SMH", timeframe="1d", parquet_path=tmp_path / "1d.parquet")
    assert ok is True
    lines = audit.read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["source"] == "ib"
    assert record["ticker"] == "SMH"
    assert record["category"] == "range_shortfall"


def test_append_audit_rejects_invalid_source(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_QUALITY_AUDIT_PATH", str(tmp_path / "audit.jsonl"))
    with pytest.raises(ValueError, match="source must be one of"):
        append_audit(_flag(), source="bogus", ticker="SMH", timeframe="1d", parquet_path=tmp_path / "1d.parquet")


def test_append_audit_oserror_returns_false(tmp_path, monkeypatch):
    audit = tmp_path / "nope" / "audit.jsonl"
    monkeypatch.setenv("MDW_QUALITY_AUDIT_PATH", str(audit))

    def boom(*a, **kw):
        raise OSError("readonly fs")

    monkeypatch.setattr("pathlib.Path.open", boom)
    ok = append_audit(_flag(), source="ib", ticker="SMH", timeframe="1d", parquet_path=tmp_path / "1d.parquet")
    assert ok is False


def test_alert_below_threshold_skipped(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "critical")
    called = []
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: called.append(a) or _ok())
    ok = alert_on_flag(_flag(severity="warning"), source="ib", ticker="SMH")
    assert ok is False
    assert called == []    # below threshold -> never spawned


def test_alert_above_threshold_spawns(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "warning")
    called = []

    def fake_run(*a, **kw):
        called.append(a)
        return _ok()

    monkeypatch.setattr("subprocess.run", fake_run)
    ok = alert_on_flag(_flag(severity="critical"), source="ib", ticker="SMH")
    assert ok is True
    assert called, "subprocess.run should have been invoked"
    cmd = called[0][0]
    assert "send_daily_update_failure_email.mjs" in " ".join(cmd)
    assert "flag-alert" in cmd


def test_alert_rate_limit_dedupes_within_window(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "warning")
    monkeypatch.setenv("MDW_ALERT_RATE_LIMIT_SECONDS", "300")
    counts = [0]

    def fake_run(*a, **kw):
        counts[0] += 1
        return _ok()

    monkeypatch.setattr("subprocess.run", fake_run)
    from clients import quality_flags

    quality_flags._RATE_LIMIT_CACHE.clear()    # ensure clean state
    alert_on_flag(_flag(severity="critical"), source="ib", ticker="SMH")
    alert_on_flag(_flag(severity="critical"), source="ib", ticker="SMH")    # duplicate
    assert counts[0] == 1


def test_alert_smtp_failure_preserves_html(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "warning")
    monkeypatch.setenv("MDW_UNDELIVERED_DIR", str(tmp_path / "undelivered"))

    def fake_run(*a, **kw):
        return _fail("SMTP timeout")

    monkeypatch.setattr("subprocess.run", fake_run)
    from clients import quality_flags

    quality_flags._RATE_LIMIT_CACHE.clear()
    ok = alert_on_flag(_flag(severity="critical"), source="ib", ticker="HOOD")
    assert ok is False
    saved = list((tmp_path / "undelivered").glob("*HOOD*"))
    assert saved, "undelivered HTML should be preserved"


def test_alert_invalid_rate_limit_env_uses_default(monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "warning")
    monkeypatch.setenv("MDW_ALERT_RATE_LIMIT_SECONDS", "bad")
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: _ok())
    ok = alert_on_flag(_flag(severity="critical"), source="ib", ticker="SMH")
    assert ok is True


def test_alert_spawn_exception_preserves_html(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "warning")
    monkeypatch.setenv("MDW_UNDELIVERED_DIR", str(tmp_path / "undelivered"))

    def boom(*a, **kw):
        raise OSError("node missing")

    monkeypatch.setattr("subprocess.run", boom)
    ok = alert_on_flag(_flag(severity="critical"), source="ib", ticker="TSLA")
    assert ok is False
    assert list((tmp_path / "undelivered").glob("*TSLA*"))


def _ok():
    from subprocess import CompletedProcess

    return CompletedProcess(args=[], returncode=0, stdout=b"", stderr=b"")


def _fail(msg):
    from subprocess import CompletedProcess

    return CompletedProcess(args=[], returncode=1, stdout=b"", stderr=msg.encode())
