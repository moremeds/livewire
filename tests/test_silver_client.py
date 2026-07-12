from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from clients.adjustment_engine import FactorInterval
from clients.silver_client import SilverClient


def _daily_rows():
    return [
        {
            "trade_date": date(2026, 1, 2),
            "symbol_id": 7,
            "open": 101.0,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "adj_close": 101.5,
            "volume": 1_000,
            "price_adjustment_factor": 1.0,
            "split_volume_factor": 1.0,
            "adjustment_revision": 3,
        },
        {
            "trade_date": date(2026, 1, 1),
            "symbol_id": 7,
            "open": 99.0,
            "high": 101.0,
            "low": 98.0,
            "close": 100.0,
            "adj_close": 100.0,
            "volume": 2_000,
            "price_adjustment_factor": 0.5,
            "split_volume_factor": 2.0,
            "adjustment_revision": 3,
        },
    ]


def _intervals():
    return [
        FactorInterval(date(2026, 1, 2), date(2026, 1, 2), Decimal("1"), Decimal("1"), 3),
        FactorInterval(date(2026, 1, 1), date(2026, 1, 1), Decimal("0.5"), Decimal("2"), 3),
    ]


def test_publish_daily_uses_canonical_schema_path_sort_and_checksum(tmp_path):
    client = SilverClient(tmp_path)
    artifact = client.publish_daily("BRK.B", _daily_rows())

    assert artifact.path == tmp_path / "asset_class=equity/symbol=BRK.B/1d.parquet"
    parquet = pq.ParquetFile(artifact.path)
    assert parquet.schema_arrow == client.daily_schema
    table = parquet.read()
    assert table.column("trade_date").to_pylist() == [date(2026, 1, 1), date(2026, 1, 2)]
    assert artifact.row_count == 2
    assert artifact.sha256 == hashlib.sha256(artifact.path.read_bytes()).hexdigest()


def test_publish_factors_preserves_identity_and_revision(tmp_path):
    client = SilverClient(tmp_path)
    artifact = client.publish_factors("NVDA", _intervals())

    assert artifact.path == tmp_path / "adjustments/asset_class=equity/symbol=NVDA/factors.parquet"
    table = pq.ParquetFile(artifact.path).read()
    assert table.schema == client.factor_schema
    assert table.column("effective_start").to_pylist() == [date(2026, 1, 1), date(2026, 1, 2)]
    assert table.column("price_adjustment_factor").to_pylist()[-1] == 1.0
    assert table.column("split_volume_factor").to_pylist()[-1] == 1.0
    assert table.column("adjustment_revision").to_pylist() == [3, 3]


def test_publish_rejects_duplicate_daily_dates(tmp_path):
    rows = _daily_rows()
    rows[1]["trade_date"] = rows[0]["trade_date"]
    with pytest.raises(ValueError, match="duplicate"):
        SilverClient(tmp_path).publish_daily("NVDA", rows)


def test_publish_rejects_mixed_daily_revisions(tmp_path):
    rows = _daily_rows()
    rows[1]["adjustment_revision"] = 4
    with pytest.raises(ValueError, match="revision"):
        SilverClient(tmp_path).publish_daily("NVDA", rows)


def test_publish_rejects_duplicate_factor_starts(tmp_path):
    intervals = _intervals()
    intervals[1] = FactorInterval(
        intervals[0].effective_start,
        intervals[1].effective_end,
        intervals[1].price_adjustment_factor,
        intervals[1].split_volume_factor,
        3,
    )
    with pytest.raises(ValueError, match="duplicate"):
        SilverClient(tmp_path).publish_factors("NVDA", intervals)


def test_publish_rejects_overlapping_factor_intervals(tmp_path):
    intervals = _intervals()
    intervals[1] = FactorInterval(
        date(2026, 1, 1),
        date(2026, 1, 3),
        intervals[1].price_adjustment_factor,
        intervals[1].split_volume_factor,
        3,
    )
    with pytest.raises(ValueError, match="overlap"):
        SilverClient(tmp_path).publish_factors("NVDA", intervals)


def test_failed_republish_leaves_existing_daily_artifact_unchanged(tmp_path, monkeypatch):
    client = SilverClient(tmp_path)
    artifact = client.publish_daily("NVDA", _daily_rows())
    original = artifact.path.read_bytes()

    def fail_publish(*args, **kwargs):
        raise RuntimeError("staged validation failed")

    monkeypatch.setattr("clients.silver_client.publish_parquet", fail_publish)
    with pytest.raises(RuntimeError, match="validation"):
        client.publish_daily("NVDA", _daily_rows()[:1])
    assert artifact.path.read_bytes() == original


def test_zero_revision_is_not_publishable(tmp_path):
    intervals = [FactorInterval(date(2026, 1, 1), date(2026, 1, 2), Decimal("1"), Decimal("1"))]
    with pytest.raises(ValueError, match="revision"):
        SilverClient(tmp_path).publish_factors("NVDA", intervals)
