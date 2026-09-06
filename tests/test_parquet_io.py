"""Tests for clients/parquet_io.py — shared parquet publish and validation."""

from __future__ import annotations

from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.parquet_io import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    publish_parquet,
    validate_parquet_file,
)

_SCHEMA = pa.schema(
    [
        ("trade_date", pa.date32()),
        ("symbol_id", pa.int64()),
        ("value", pa.float64()),
    ]
)


def _table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=_SCHEMA)


class TestPublishParquet:
    def test_writes_file_atomically(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 6), "symbol_id": 1, "value": 2.0},
        ]
        publish_parquet(out, _table(rows), sort_column="trade_date")
        assert out.exists()
        loaded = pq.read_table(out)
        assert loaded.num_rows == 2

    def test_symbol_lock_creates_parent_and_is_reusable(self, tmp_path):
        from clients.parquet_io import symbol_lock

        parquet_path = tmp_path / "symbol=NEW" / "1d.parquet"

        with symbol_lock(parquet_path):
            assert parquet_path.with_suffix(".parquet.lock").exists()

        with symbol_lock(parquet_path):
            pass

        with pytest.raises(RuntimeError, match="probe"):
            with symbol_lock(parquet_path):
                raise RuntimeError("probe")

        with symbol_lock(parquet_path):
            pass

    def test_no_temp_file_remains_on_success(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0}]
        publish_parquet(out, _table(rows), sort_column="trade_date")
        tmps = list(tmp_path.glob(".data.parquet.*.tmp"))
        assert tmps == []

    def test_temp_file_cleaned_on_validation_failure(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0}]
        with pytest.raises(KeyError):
            publish_parquet(out, _table(rows), sort_column="nonexistent_column")
        tmps = list(tmp_path.glob(".data.parquet.*.tmp"))
        assert tmps == []
        assert not out.exists()

    def test_creates_parent_dirs(self, tmp_path):
        out = tmp_path / "deeply" / "nested" / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0}]
        publish_parquet(out, _table(rows), sort_column="trade_date")
        assert out.exists()

    def test_published_file_uses_zstd_codec(self, tmp_path):
        # zstd is the cheap, lossless storage win: every column of every row group
        # must be written with the configured codec, not snappy.
        assert PARQUET_COMPRESSION == "zstd"
        assert PARQUET_COMPRESSION_LEVEL == 3
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 6), "symbol_id": 1, "value": 2.0},
        ]
        publish_parquet(out, _table(rows), sort_column="trade_date")
        metadata = pq.read_metadata(out)
        codecs = {
            metadata.row_group(rg).column(col).compression
            for rg in range(metadata.num_row_groups)
            for col in range(metadata.num_columns)
        }
        assert codecs == {"ZSTD"}

    def test_published_file_round_trips_losslessly(self, tmp_path):
        # zstd must not alter any value: bytes in == bytes out.
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 5), "symbol_id": 7, "value": 192.34},
            {"trade_date": date(2026, 1, 6), "symbol_id": 7, "value": 0.0001},
        ]
        table = _table(rows)
        publish_parquet(out, table, sort_column="trade_date")
        assert pq.read_table(out).equals(table)


class TestValidateParquetFile:
    def test_valid_file_passes(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 6), "symbol_id": 1, "value": 2.0},
        ]
        pq.write_table(_table(rows), out)
        validate_parquet_file(out, expected_rows=2, sort_column="trade_date")

    def test_wrong_row_count_raises(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0}]
        pq.write_table(_table(rows), out)
        with pytest.raises(ValueError, match="expected 5 rows"):
            validate_parquet_file(out, expected_rows=5, sort_column="trade_date")

    def test_unsorted_raises(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 6), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 2.0},
        ]
        pq.write_table(_table(rows), out)
        with pytest.raises(ValueError, match="not sorted"):
            validate_parquet_file(out, expected_rows=2, sort_column="trade_date")

    def test_an_integer_sort_column_is_compared_as_a_number_not_as_text(self, tmp_path):
        """11 ascending ints used to fail: str() sorts "10" before "2"."""
        out = tmp_path / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": i, "value": 1.0} for i in range(11)]
        pq.write_table(_table(rows), out)
        validate_parquet_file(out, expected_rows=11, sort_column="symbol_id")

    def test_an_out_of_order_integer_sort_column_still_raises(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [{"trade_date": date(2026, 1, 5), "symbol_id": i, "value": 1.0} for i in (1, 10, 2)]
        pq.write_table(_table(rows), out)
        with pytest.raises(ValueError, match="not sorted"):
            validate_parquet_file(out, expected_rows=3, sort_column="symbol_id")

    def test_duplicates_raise(self, tmp_path):
        out = tmp_path / "data.parquet"
        rows = [
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 1.0},
            {"trade_date": date(2026, 1, 5), "symbol_id": 1, "value": 2.0},
        ]
        pq.write_table(_table(rows), out)
        with pytest.raises(ValueError, match="duplicate"):
            validate_parquet_file(out, expected_rows=2, sort_column="trade_date")


def test_fsync_directory_opens_and_closes_the_directory(tmp_path, monkeypatch):
    from clients import parquet_io

    synced: list[int] = []
    monkeypatch.setattr(parquet_io.os, "fsync", synced.append)

    parquet_io.fsync_directory(tmp_path)

    assert len(synced) == 1


def test_every_directory_fsync_comes_from_parquet_io():
    """Four hand-rolled copies preceded this (pm:2026-09-05-source-evidence-flat-exfat-directory)."""
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    offenders = [
        path.name
        for path in sorted((root / "clients").glob("*.py"))
        if path.name != "parquet_io.py" and "def fsync_directory" in path.read_text(encoding="utf-8")
    ]

    assert offenders == []
