from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow.parquet as pq

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveDividend, MassiveSplit
from livewire_scripts import rebuild_silver


def _bronze(root, symbol, closes=(100.0, 100.0, 50.0)):
    rows = [
        {
            "trade_date": date(2026, 1, index + 1).isoformat(),
            "symbol_id": 7,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "adj_close": close,
            "volume": 1_000,
            "source": "massive",
            "price_basis": "raw",
        }
        for index, close in enumerate(closes)
    ]
    BronzeClient(root / "bronze/asset_class=equity", "equity").replace_ticker_rows(symbol, rows)


def _split(root, symbol="NVDA"):
    event = MassiveSplit(
        provider_event_id=f"{symbol}-split",
        ticker=symbol,
        execution_date=date(2026, 1, 3),
        split_from=Decimal("1"),
        split_to=Decimal("2"),
        payload_hash="split-hash",
    )
    CorporateActionStore(root).reconcile(symbol, [event], datetime(2026, 1, 4, tzinfo=UTC))


def test_targeted_rebuild_publishes_daily_factors_and_manifest(tmp_path, capsys):
    _bronze(tmp_path, "NVDA")
    _split(tmp_path)
    silver = tmp_path / "silver"

    assert rebuild_silver.run(["--tickers", "NVDA"], data_lake_root=tmp_path, silver_root=silver) == 0

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["rebuilt"] == 1
    assert summary["action_count"] == 1
    assert summary["earliest_affected_date"] == "2026-01-01"
    assert summary["revision"] == 1
    daily = pq.ParquetFile(silver / "asset_class=equity/symbol=NVDA/1d.parquet").read()
    assert daily.column("adjustment_revision").to_pylist() == [1, 1, 1]
    assert daily.column("close").to_pylist()[0] == 50.0
    factors = pq.ParquetFile(silver / "adjustments/asset_class=equity/symbol=NVDA/factors.parquet").read()
    assert factors.column("adjustment_revision").to_pylist() == [1, 1]
    assert (silver / "revisions/current.json").exists()


def test_targeted_rebuild_excludes_announced_future_dividend(tmp_path, capsys):
    _bronze(tmp_path, "MSFT")
    dividend = MassiveDividend(
        provider_event_id="future-dividend",
        ticker="MSFT",
        ex_dividend_date=date(2026, 1, 4),
        cash_amount=Decimal("1"),
        currency="USD",
        declaration_date=date(2026, 1, 1),
        record_date=None,
        pay_date=None,
        payload_hash="future-dividend-hash",
    )
    CorporateActionStore(tmp_path).reconcile(
        "MSFT",
        [dividend],
        datetime(2026, 1, 2, tzinfo=UTC),
    )
    silver = tmp_path / "silver"

    assert (
        rebuild_silver.run(
            ["--tickers", "MSFT"],
            data_lake_root=tmp_path,
            silver_root=silver,
            as_of_date=date(2026, 1, 3),
        )
        == 0
    )

    daily = pq.ParquetFile(silver / "asset_class=equity/symbol=MSFT/1d.parquet").read()
    assert daily.column("close").to_pylist() == [100.0, 100.0, 50.0]
    assert daily.column("price_adjustment_factor").to_pylist() == [1.0, 1.0, 1.0]
    factors = pq.ParquetFile(silver / "adjustments/asset_class=equity/symbol=MSFT/factors.parquet").read()
    assert factors.column("price_adjustment_factor").to_pylist() == [1.0]
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["as_of_date"] == "2026-01-03"
    assert summary["action_count"] == 1
    assert summary["effective_action_count"] == 0
    assert summary["future_action_count"] == 1


def test_multi_symbol_rebuild_uses_injected_cutoff_for_every_symbol(tmp_path):
    for symbol in ("MSFT", "AAPL"):
        _bronze(tmp_path, symbol)
        split = MassiveSplit(
            provider_event_id=f"{symbol}-future-split",
            ticker=symbol,
            execution_date=date(2026, 1, 4),
            split_from=Decimal("1"),
            split_to=Decimal("2"),
            payload_hash=f"{symbol}-future-split-hash",
        )
        CorporateActionStore(tmp_path).reconcile(
            symbol,
            [split],
            datetime(2026, 1, 2, tzinfo=UTC),
        )

    assert (
        rebuild_silver.run(
            ["--tickers", "MSFT", "AAPL"],
            data_lake_root=tmp_path,
            silver_root=tmp_path / "silver",
            as_of_date=date(2026, 1, 3),
        )
        == 0
    )

    for symbol in ("MSFT", "AAPL"):
        factors = pq.ParquetFile(
            tmp_path / f"silver/adjustments/asset_class=equity/symbol={symbol}/factors.parquet"
        ).read()
        assert factors.column("price_adjustment_factor").to_pylist() == [1.0]
        assert factors.column("split_volume_factor").to_pylist() == [1.0]


def test_full_rebuild_discovers_all_equity_bronze_symbols(tmp_path, capsys):
    _bronze(tmp_path, "NVDA")
    _bronze(tmp_path, "AAPL", closes=(10.0, 10.0, 10.0))
    _split(tmp_path)

    assert rebuild_silver.run(["--full"], data_lake_root=tmp_path, silver_root=tmp_path / "silver") == 0

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["rebuilt"] == 2
    assert (tmp_path / "silver/asset_class=equity/symbol=AAPL/1d.parquet").exists()


def test_unchanged_second_run_is_manifest_noop(tmp_path, capsys):
    _bronze(tmp_path, "NVDA")
    _split(tmp_path)
    silver = tmp_path / "silver"
    rebuild_silver.run(["--tickers", "NVDA"], data_lake_root=tmp_path, silver_root=silver)
    capsys.readouterr()
    current = (silver / "revisions/current.json").read_bytes()

    assert rebuild_silver.run(["--tickers", "NVDA"], data_lake_root=tmp_path, silver_root=silver) == 0

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["rebuilt"] == 0
    assert summary["unchanged"] == 1
    assert summary["revision"] == 1
    assert (silver / "revisions/current.json").read_bytes() == current
    assert not (silver / "revisions/revision=2.json").exists()


def test_one_symbol_validation_failure_blocks_entire_batch_manifest(tmp_path, capsys):
    _bronze(tmp_path, "NVDA")
    _bronze(tmp_path, "BAD")
    _split(tmp_path)
    bad_dividend = MassiveDividend(
        provider_event_id="bad-dividend",
        ticker="BAD",
        ex_dividend_date=date(2026, 1, 1),
        cash_amount=Decimal("1"),
        currency="USD",
        declaration_date=None,
        record_date=None,
        pay_date=None,
        payload_hash="bad",
    )
    CorporateActionStore(tmp_path).reconcile("BAD", [bad_dividend], datetime(2026, 1, 4, tzinfo=UTC))
    silver = tmp_path / "silver"

    assert rebuild_silver.run(["--tickers", "NVDA", "BAD"], data_lake_root=tmp_path, silver_root=silver) == 1

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["failed"] == 1
    assert not (silver / "revisions/current.json").exists()
    assert not (silver / "asset_class=equity/symbol=NVDA/1d.parquet").exists()


def test_dry_run_reports_changes_without_creating_silver_root(tmp_path, capsys):
    _bronze(tmp_path, "NVDA")
    _split(tmp_path)
    silver = tmp_path / "silver"

    assert (
        rebuild_silver.run(
            ["--tickers", "NVDA", "--dry-run"],
            data_lake_root=tmp_path,
            silver_root=silver,
        )
        == 0
    )

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["rebuilt"] == 1
    assert summary["revision"] == 1
    assert not silver.exists()
