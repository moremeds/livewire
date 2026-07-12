from __future__ import annotations

import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from livewire_scripts import migrate_equity_price_basis


def _legacy(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "trade_date": pa.array(["2025-01-02"], pa.string()).cast(pa.date32()),
            "symbol_id": pa.array([1], pa.int64()),
            "open": [99.0],
            "high": [101.0],
            "low": [98.0],
            "close": [100.0],
            "adj_close": [100.0],
            "volume": pa.array([1000], pa.int64()),
        }
    )
    pq.write_table(table, path)


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_dry_run_reports_migration_without_writing(tmp_path, capsys):
    path = tmp_path / "symbol=AAPL/1d.parquet"
    _legacy(path)
    before = _sha(path)

    assert migrate_equity_price_basis.run(["--tickers", "AAPL", "--dry-run"], bronze_root=tmp_path) == 0

    report = json.loads(capsys.readouterr().out)
    assert report["migrated"] == 1
    assert report["dry_run"] is True
    assert _sha(path) == before


def test_migration_preserves_values_and_adds_legacy_metadata(tmp_path, capsys):
    path = tmp_path / "symbol=AAPL/1d.parquet"
    _legacy(path)
    before = pq.read_table(path).to_pylist()

    assert migrate_equity_price_basis.run(["--tickers", "AAPL"], bronze_root=tmp_path) == 0

    after = pq.read_table(path).to_pylist()
    assert [{k: row[k] for k in before[0]} for row in after] == before
    assert after[0]["source"] == "legacy"
    assert after[0]["price_basis"] == "unknown"
    report = json.loads(capsys.readouterr().out)
    assert report["artifacts"][0]["source_sha256"] != report["artifacts"][0]["target_sha256"]


def test_second_run_is_noop(tmp_path, capsys):
    path = tmp_path / "symbol=AAPL/1d.parquet"
    _legacy(path)
    migrate_equity_price_basis.run(["--tickers", "AAPL"], bronze_root=tmp_path)
    capsys.readouterr()
    before = _sha(path)

    assert migrate_equity_price_basis.run(["--tickers", "AAPL"], bronze_root=tmp_path) == 0

    assert _sha(path) == before
    assert json.loads(capsys.readouterr().out)["unchanged"] == 1


def test_failed_publish_preserves_original_bytes(tmp_path, monkeypatch):
    path = tmp_path / "symbol=AAPL/1d.parquet"
    _legacy(path)
    before = path.read_bytes()

    def fail(*args, **kwargs):
        raise RuntimeError("publish failed")

    monkeypatch.setattr("clients.bronze_client.BronzeClient._publish_symbol_rows", fail)
    with pytest.raises(RuntimeError, match="publish failed"):
        migrate_equity_price_basis.run(["--tickers", "AAPL"], bronze_root=tmp_path)
    assert path.read_bytes() == before
