from __future__ import annotations

import hashlib
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


def _bad_action(root, symbol="BAD"):
    bad_dividend = MassiveDividend(
        provider_event_id=f"{symbol}-dividend",
        ticker=symbol,
        ex_dividend_date=date(2026, 1, 3),
        cash_amount=Decimal("1"),
        currency="CAD",
        declaration_date=None,
        record_date=None,
        pay_date=None,
        payload_hash=f"{symbol}-bad",
    )
    CorporateActionStore(root).reconcile(symbol, [bad_dividend], datetime(2026, 1, 4, tzinfo=UTC))


def test_one_symbol_failure_still_publishes_healthy_symbols(tmp_path, capsys):
    _bronze(tmp_path, "NVDA")
    _bronze(tmp_path, "BAD")
    _split(tmp_path)
    _bad_action(tmp_path)
    silver = tmp_path / "silver"

    # 1 failure out of 2 processed is below the systemic threshold, so the run
    # publishes the healthy symbol and reports the failure without failing.
    assert rebuild_silver.run(["--tickers", "NVDA", "BAD"], data_lake_root=tmp_path, silver_root=silver) == 0

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["failed"] == 1
    assert summary["rebuilt"] == 1
    assert (silver / "revisions/current.json").exists()
    assert (silver / "asset_class=equity/symbol=NVDA/1d.parquet").exists()
    assert not (silver / "asset_class=equity/symbol=BAD/1d.parquet").exists()


def test_total_staging_failure_fails_the_run_and_publishes_nothing(tmp_path, capsys):
    _bronze(tmp_path, "BAD")
    _bad_action(tmp_path)
    silver = tmp_path / "silver"

    # Zero successful symbols with an error is a systemic failure.
    assert rebuild_silver.run(["--tickers", "BAD"], data_lake_root=tmp_path, silver_root=silver) == 1

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["failed"] == 1
    assert summary["rebuilt"] == 0
    assert not (silver / "revisions/current.json").exists()


def test_mixed_basis_symbol_is_quarantined_not_published(tmp_path):
    # Seed a legacy/raw symbol whose series mixes already-adjusted and true-raw
    # rows around a split, so the adjusted output has a >6x residual jump.
    from datetime import UTC, date, datetime
    from decimal import Decimal

    from clients.bronze_client import BronzeClient
    from clients.corporate_action_store import CorporateActionStore
    from clients.massive_client import MassiveSplit
    from livewire_scripts import rebuild_silver

    bronze_root = tmp_path / "bronze/asset_class=equity"
    # NVDA-style: true-raw ~713 and already-adjusted ~17 interleaved, all labeled raw.
    rows = []
    for d, close in (
        ("2021-06-17", 746.29),  # true-raw
        ("2021-06-18", 18.64),  # already-adjusted, mislabeled raw (lone bad bar)
        ("2021-06-21", 737.09),  # true-raw
    ):
        rows.append(
            {
                "trade_date": d,
                "symbol_id": 1,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "adj_close": close,
                "volume": 100,
                "source": "legacy",
                "price_basis": "raw",
            }
        )
    BronzeClient(bronze_root, "equity").replace_ticker_rows("NVDA", rows)
    split = MassiveSplit(
        provider_event_id="nvda-2021",
        ticker="NVDA",
        execution_date=date(2021, 7, 20),
        split_from=Decimal("1"),
        split_to=Decimal("4"),
        payload_hash="s",
    )
    CorporateActionStore(tmp_path).reconcile("NVDA", [split], datetime(2021, 7, 20, tzinfo=UTC))

    # Also seed a CLEAN symbol (no split) so the run has updated>0. A lone rejected
    # symbol makes updated==0 → resolve_exit_code returns 1; a second published symbol
    # proves quarantine doesn't fail the whole batch.
    clean = [
        {
            "trade_date": d,
            "symbol_id": 2,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "legacy",
            "price_basis": "raw",
        }
        for d, c in (("2021-06-17", 258.0), ("2021-06-18", 259.4), ("2021-06-21", 259.9))
    ]
    BronzeClient(bronze_root, "equity").replace_ticker_rows("MSFT", clean)

    failure_output = tmp_path / "failures.json"
    rc = rebuild_silver.run(
        ["--tickers", "NVDA", "MSFT", "--failure-output", str(failure_output)],
        data_lake_root=tmp_path,
        silver_root=tmp_path / "silver",
    )
    # NVDA quarantined (no artifact); MSFT published.
    assert not (tmp_path / "silver/asset_class=equity/symbol=NVDA/1d.parquet").exists()
    assert (tmp_path / "silver/asset_class=equity/symbol=MSFT/1d.parquet").exists()
    import json

    failures = json.loads(failure_output.read_text())["failures"]
    assert any(f["symbol"] == "NVDA" and "continuity" in f["error"] for f in failures)
    assert rc == 0  # quarantining one symbol while another publishes is not systemic failure


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


def test_dry_run_preserves_existing_bronze_and_silver_bytes(tmp_path, capsys):
    _bronze(tmp_path, "NVDA")
    _split(tmp_path)
    silver = tmp_path / "silver"
    assert rebuild_silver.run(["--tickers", "NVDA"], data_lake_root=tmp_path, silver_root=silver) == 0
    capsys.readouterr()

    watched = [
        tmp_path / "bronze/asset_class=equity/symbol=NVDA/1d.parquet",
        silver / "asset_class=equity/symbol=NVDA/1d.parquet",
        silver / "adjustments/asset_class=equity/symbol=NVDA/factors.parquet",
        silver / "revisions/current.json",
        silver / "revisions/revision=1.json",
    ]
    before = {path: path.read_bytes() for path in watched}

    assert (
        rebuild_silver.run(
            ["--tickers", "NVDA", "--dry-run"],
            data_lake_root=tmp_path,
            silver_root=silver,
        )
        == 0
    )

    assert {path: path.read_bytes() for path in watched} == before
    assert not (silver / "revisions/revision=2.json").exists()


def test_successful_dry_run_atomically_replaces_stale_failure_report(tmp_path):
    _bronze(tmp_path, "NVDA")
    _split(tmp_path)
    silver = tmp_path / "silver"
    output = tmp_path / "reports/rebuild-failures.json"
    output.parent.mkdir(parents=True)
    output.write_text('{"failures":[{"symbol":"STALE"}]}')

    assert (
        rebuild_silver.run(
            ["--tickers", "NVDA", "--dry-run", "--failure-output", str(output)],
            data_lake_root=tmp_path,
            silver_root=silver,
        )
        == 0
    )

    payload = json.loads(output.read_text())
    assert payload["schema_version"] == 2
    assert payload["failures"] == []
    assert list(output.parent.glob(f".{output.name}.*.tmp")) == []


def test_dry_run_writes_evidence_grade_failure_report(tmp_path):
    _bronze(tmp_path, "BAD")
    _bad_action(tmp_path)
    silver = tmp_path / "silver"
    output = tmp_path / "reports/rebuild-failures.json"
    bronze_path = tmp_path / "bronze/asset_class=equity/symbol=BAD/1d.parquet"
    expected_digest = hashlib.sha256(bronze_path.read_bytes()).hexdigest()
    expected_action = CorporateActionStore(tmp_path).latest_active("BAD")[0]

    assert (
        rebuild_silver.run(
            [
                "--tickers",
                "BAD",
                "--dry-run",
                "--failure-output",
                str(output),
            ],
            data_lake_root=tmp_path,
            silver_root=silver,
            as_of_date=date(2026, 1, 3),
        )
        == 1
    )

    payload = json.loads(output.read_text())
    assert payload.keys() == {
        "schema_version",
        "generated_at",
        "data_lake_root",
        "silver_root",
        "as_of_date",
        "failures",
    }
    assert payload["schema_version"] == 2
    assert datetime.fromisoformat(payload["generated_at"]).tzinfo is not None
    assert payload["data_lake_root"] == str(tmp_path.resolve())
    assert payload["silver_root"] == str(silver.resolve())
    assert payload["as_of_date"] == "2026-01-03"
    assert payload["failures"] == [
        {
            "symbol": "BAD",
            "error_type": "ValueError",
            "error": "dividend currency does not match bronze currency",
            "bronze_path": str(bronze_path.resolve()),
            "source_sha256": expected_digest,
            "earliest_trade_date": "2026-01-01",
            "latest_trade_date": "2026-01-03",
            "active_actions": [
                {
                    "action_id": expected_action.action_id,
                    "action_type": "cash_dividend",
                    "ex_date": "2026-01-03",
                    "status": "active",
                }
            ],
        }
    ]
    assert not silver.exists()
