import json

import pytest

from clients.quality_detector import QualityFlag
from clients.quality_flags import append_audit, write_sidecar


@pytest.fixture(autouse=True)
def _ledger_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setenv("LW_RUN_ID", "quality-flags-test")


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


def test_quality_paths_follow_warehouse_override(tmp_path, monkeypatch):
    from clients.quality_flags import _resolve_audit_path

    warehouse = tmp_path / "warehouse"
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    monkeypatch.delenv("MDW_LOG_DIR", raising=False)
    monkeypatch.delenv("MDW_QUALITY_AUDIT_PATH", raising=False)

    assert _resolve_audit_path() == warehouse / "logs" / "quality_audit.jsonl"


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


def test_flags_are_written_without_any_send(tmp_path, monkeypatch):
    """A detector flag lands as a sidecar plus audit line — no process, no email.

    Quality flags are findings, not pages: notify.py owns every send, and a
    critical flag must still never spawn one.
    """
    import subprocess
    from unittest.mock import patch

    from clients.quality_detector import run_detection

    audit_path = tmp_path / "audit.jsonl"
    monkeypatch.setenv("MDW_QUALITY_AUDIT_PATH", str(audit_path))
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("a flag must never spawn a process")),
    )
    parquet = tmp_path / "symbol=T" / "1d.parquet"

    with patch("clients.quality_detector.detect_all", return_value=[_flag()]):
        run_detection(
            ticker="T",
            asset_class="equity",
            timeframe="1d",
            bars=[{"trade_date": "2026-05-17"}],
            parquet_path=parquet,
            source="ib",
        )

    sidecar = json.loads((tmp_path / "symbol=T" / "1d.parquet.meta.json").read_text())
    assert sidecar["flags"][0]["category"] == "range_shortfall"
    audit = [json.loads(line) for line in audit_path.read_text().splitlines()]
    assert [row["category"] for row in audit] == ["range_shortfall"]
