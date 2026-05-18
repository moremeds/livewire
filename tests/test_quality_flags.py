import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from clients.quality_detector import QualityFlag
from clients.quality_flags import write_sidecar


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
