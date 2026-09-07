from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from clients import ledger
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveDividend, MassiveSplit
from livewire_scripts import rebuild_silver
from livewire_scripts.daily_outcomes import parse_all_summary_json


def _summary_from(capsys) -> dict:
    """Parse the run summary exactly the way the nightly digest does.

    Going through the real parser is the point. It skips every line without
    SUMMARY_PREFIX, so a bare-JSON summary fails loudly here instead of
    silently vanishing from the digest's Silver section, which is what
    happened in production up to 2026-08-01.
    """
    summaries = parse_all_summary_json(capsys.readouterr().out)
    assert summaries, "rebuild-silver emitted no SUMMARY_JSON line the digest can parse"
    return summaries[-1]


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


def _seed_bronze(root, symbol, rows_spec, *, source="massive", price_basis="raw"):
    """Bronze rows at explicit ISO dates. `rows_spec` is [(iso_date, close), ...]."""
    rows = [
        {
            "trade_date": iso_date,
            "symbol_id": 7,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "adj_close": close,
            "volume": 1_000,
            "source": source,
            "price_basis": price_basis,
        }
        for iso_date, close in rows_spec
    ]
    BronzeClient(root / "bronze/asset_class=equity", "equity").replace_ticker_rows(symbol, rows)


def _seed_split(root, symbol, ex_date, split_from, split_to):
    """One active split at an explicit ex-date."""
    event = MassiveSplit(
        provider_event_id=f"{symbol}-{ex_date}",
        ticker=symbol,
        execution_date=date.fromisoformat(ex_date),
        split_from=Decimal(str(split_from)),
        split_to=Decimal(str(split_to)),
        payload_hash=f"{symbol}-{ex_date}-hash",
    )
    CorporateActionStore(root).reconcile(symbol, [event], datetime(2026, 1, 4, tzinfo=UTC))


def test_continuity_allowlist_exempts_an_evidenced_date(tmp_path):
    """Without an override a >6x step makes a symbol unpublishable forever.

    Real EQIX bronze closes (frozen 2026-07-17): a 24.95x step at 2003-01-02 that no
    corporate action explains — EQIX has zero split events in the store.
    """
    root = tmp_path / "lake"
    _seed_bronze(root, "EQIX", [("2002-12-30", 0.21), ("2003-01-02", 5.24), ("2003-01-03", 5.08)])
    silver = tmp_path / "silver"

    # Baseline: unexplained, so the window trims everything before the step.
    assert (
        rebuild_silver.run(["--tickers", "EQIX"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
        == 0
    )
    trimmed = pq.ParquetFile(silver / "asset_class=equity/symbol=EQIX/1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in trimmed] == ["2003-01-02", "2003-01-03"]

    assert (
        rebuild_silver.run(
            ["--tickers", "EQIX", "--continuity-allowlist", "2003-01-02"],
            data_lake_root=root,
            silver_root=silver,
            as_of_date=date(2026, 7, 17),
        )
        == 0
    )

    kept = pq.ParquetFile(silver / "asset_class=equity/symbol=EQIX/1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in kept] == ["2002-12-30", "2003-01-02", "2003-01-03"]


def test_targeted_rebuild_publishes_daily_factors_and_manifest(tmp_path, capsys):
    _bronze(tmp_path, "NVDA")
    _split(tmp_path)
    silver = tmp_path / "silver"

    assert rebuild_silver.run(["--tickers", "NVDA"], data_lake_root=tmp_path, silver_root=silver) == 0

    summary = _summary_from(capsys)
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
    summary = _summary_from(capsys)
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

    summary = _summary_from(capsys)
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

    summary = _summary_from(capsys)
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

    summary = _summary_from(capsys)
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

    summary = _summary_from(capsys)
    assert summary["failed"] == 1
    assert summary["rebuilt"] == 0
    assert not (silver / "revisions/current.json").exists()


def test_mixed_basis_symbol_publishes_its_window_not_nothing(tmp_path):
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
    import json

    # NVDA is no longer dropped whole: the lone bad 2021-06-18 bar breaks continuity
    # twice (into it and out of it), so the window starts after the second break and
    # publishes only the true-raw 2021-06-21 row — shorter, but every row correct.
    published = pq.ParquetFile(tmp_path / "silver/asset_class=equity/symbol=NVDA/1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in published] == ["2021-06-21"]
    assert (tmp_path / "silver/asset_class=equity/symbol=MSFT/1d.parquet").exists()
    assert json.loads(failure_output.read_text())["failures"] == []
    assert rc == 0


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

    summary = _summary_from(capsys)
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
        "window_regressions",
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


def test_seed_corrupt_symbol_publishes_its_post_seed_window_rather_than_quarantining(tmp_path):
    """The 2x class the 6.0 scan cannot see: APH must publish from the seed date on,
    NOT be dropped. Real APH closes + its real 2024-06-12 1:2 split."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(
        root, "APH", [("2021-06-09", 34.07), ("2021-06-10", 34.13), ("2021-06-11", 68.45), ("2021-06-14", 68.31)]
    )
    _seed_split(root, "APH", "2024-06-12", 1, 2)
    failures = tmp_path / "failures.json"

    assert (
        rebuild_silver.run(
            ["--tickers", "APH", "--failure-output", str(failures)],
            data_lake_root=root,
            silver_root=silver,
            as_of_date=date(2026, 7, 17),
        )
        == 0
    )

    assert json.loads(failures.read_text())["failures"] == []  # published, not quarantined
    published = pq.ParquetFile(silver / "asset_class=equity/symbol=APH/1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in published] == ["2021-06-11", "2021-06-14"]


def test_seed_trim_does_not_narrow_factor_coverage(tmp_path):
    """The SEED trim reassigns `rows`, so building factors after it bounds them at the
    floor and leaves every pre-floor bronze intraday bar uncovered -> apex 500. Live,
    not theoretical: production APH's bronze 1m starts 2021-06-03 against its
    2021-06-11 floor (TSLA/GE/WMT/CSX identical). The EQIX case below covers only the
    WINDOW trim, which never narrowed the factor input — which is why this broke
    undetected. Real APH closes + its real 2024-06-12 1:2 split."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(
        root, "APH", [("2021-06-09", 34.07), ("2021-06-10", 34.13), ("2021-06-11", 68.45), ("2021-06-14", 68.31)]
    )
    _seed_split(root, "APH", "2024-06-12", 1, 2)

    rebuild_silver.run(["--tickers", "APH"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    daily = pq.ParquetFile(silver / "asset_class=equity/symbol=APH/1d.parquet").read().to_pylist()
    factors = pq.ParquetFile(silver / "adjustments/asset_class=equity/symbol=APH/factors.parquet").read().to_pylist()
    assert min(str(r["trade_date"]) for r in daily) == "2021-06-11"  # daily IS floored
    assert min(str(f["effective_start"]) for f in factors) == "2021-06-09"  # factors are NOT


def test_factor_intervals_still_cover_dates_trimmed_out_of_the_daily_window(tmp_path):
    """Factors must NOT be narrowed to the daily window. Apex LEFT JOINs bronze
    intraday onto these intervals and 500s on any uncovered bronze bar
    (apex ohlc_provider.py:236-240); bronze intraday predates the trimmed window."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "EQIX", [("2002-12-30", 0.21), ("2003-01-02", 5.24), ("2003-01-03", 5.08)])

    rebuild_silver.run(["--tickers", "EQIX"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    daily = pq.ParquetFile(silver / "asset_class=equity/symbol=EQIX/1d.parquet").read().to_pylist()
    factors = pq.ParquetFile(silver / "adjustments/asset_class=equity/symbol=EQIX/factors.parquet").read().to_pylist()
    assert min(str(r["trade_date"]) for r in daily) == "2003-01-02"  # daily IS trimmed
    assert min(str(f["effective_start"]) for f in factors) == "2002-12-30"  # factors are NOT


def test_triage_confirmed_real_move_is_not_trimmed(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "MRNA", [("2020-11-13", 89.39), ("2020-11-16", 900.00), ("2020-11-17", 905.00)])
    triage = tmp_path / "triage.json"
    triage.write_text(json.dumps({"verdicts": [{"symbol": "MRNA", "date": "2020-11-16", "verdict": "real_move"}]}))

    rebuild_silver.run(
        ["--tickers", "MRNA", "--triage-manifest", str(triage)],
        data_lake_root=root,
        silver_root=silver,
        as_of_date=date(2026, 7, 17),
    )

    published = pq.ParquetFile(silver / "asset_class=equity/symbol=MRNA/1d.parquet").read().to_pylist()
    assert len(published) == 3


def test_verdicts_at_the_default_path_are_honoured_without_any_flag(tmp_path):
    """The nightly job passes no flags (run_daily_update_job.py:129). If the verdicts
    are not found by default, every real move is re-trimmed the night after rev-3."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "MRNA", [("2020-11-13", 89.39), ("2020-11-16", 900.00), ("2020-11-17", 905.00)])
    default = root / "repairs" / "triage" / "current.json"
    default.parent.mkdir(parents=True)
    default.write_text(json.dumps({"verdicts": [{"symbol": "MRNA", "date": "2020-11-16", "verdict": "real_move"}]}))

    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    published = pq.ParquetFile(silver / "asset_class=equity/symbol=MRNA/1d.parquet").read().to_pylist()
    assert len(published) == 3


def test_an_explicitly_named_missing_triage_manifest_is_an_error_not_silence(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    with pytest.raises(SystemExit, match="triage manifest not found"):
        rebuild_silver.run(
            ["--tickers", "AAPL", "--triage-manifest", str(tmp_path / "nope.json")],
            data_lake_root=root,
            silver_root=silver,
            as_of_date=date(2026, 7, 17),
        )


def test_targeted_rebuild_keeps_previously_published_symbols_in_the_manifest(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 370.87), ("2024-01-03", 370.60)])
    rebuild_silver.run(
        ["--tickers", "AAPL", "MSFT"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17)
    )
    _seed_bronze(root, "MSFT", [("2024-01-02", 370.87), ("2024-01-03", 370.60), ("2024-01-04", 367.94)])

    rebuild_silver.run(["--tickers", "MSFT"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    current = json.loads((silver / "revisions/current.json").read_text())
    symbols = {a["path"].split("symbol=")[1].split("/")[0] for a in current["artifacts"]}
    assert symbols == {"AAPL", "MSFT"}  # AAPL must not vanish
    assert current["revision"] == 2


# A corrupt bar the way this warehouse actually produces one: an IB back-adjusted
# close leaking in among true-raw rows. 181.91 / 40 = 4.548 — the same mechanism as
# the 2021-06 seed artifact, not an invented price.
_BACK_ADJUSTED_LEAK = 4.548


def test_a_new_bad_bar_that_shortens_the_window_does_not_publish(tmp_path):
    """THE core invariant. A corrupt bar arriving at the newest edge is the LAST
    break, so the suffix rule would start the window at it and publish that single
    garbage row, dropping all real history. It must fail closed instead: keep serving
    what was published, and alert."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])
    rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(
        root,
        "AAPL",
        [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91), ("2024-01-05", _BACK_ADJUSTED_LEAK)],
    )
    failures = tmp_path / "failures.json"

    rebuild_silver.run(
        ["--tickers", "AAPL", "--failure-output", str(failures)],
        data_lake_root=root,
        silver_root=silver,
        as_of_date=date(2026, 7, 17),
    )

    payload = json.loads(failures.read_text())
    regression = payload["window_regressions"][0]
    assert regression["symbol"] == "AAPL"
    assert regression["previous_start"] == "2024-01-02"
    assert regression["new_start"] == "2024-01-05"
    assert payload["failures"] == []  # a regression is an alert, not a staging failure
    # The published artifact is UNCHANGED — the garbage singleton never shipped.
    published = pq.ParquetFile(silver / "asset_class=equity/symbol=AAPL/1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in published] == ["2024-01-02", "2024-01-03", "2024-01-04"]
    # ...and the symbol is still in the manifest, not evicted.
    current = json.loads((silver / "revisions/current.json").read_text())
    assert any("symbol=AAPL" in a["path"] for a in current["artifacts"])


def test_allow_window_regression_publishes_the_shorter_window(tmp_path):
    """The rev-3 bootstrap: rev-2 published untrimmed history, so the intentional
    mass trim must be able to land once, under operator review."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])
    rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(
        root,
        "AAPL",
        [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91), ("2024-01-05", _BACK_ADJUSTED_LEAK)],
    )

    rebuild_silver.run(
        ["--tickers", "AAPL", "--allow-window-regression"],
        data_lake_root=root,
        silver_root=silver,
        as_of_date=date(2026, 7, 17),
    )

    published = pq.ParquetFile(silver / "asset_class=equity/symbol=AAPL/1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in published] == ["2024-01-05"]


def test_no_regression_reported_when_the_window_is_stable(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])
    failures = tmp_path / "failures.json"

    rebuild_silver.run(
        ["--tickers", "AAPL", "--failure-output", str(failures)],
        data_lake_root=root,
        silver_root=silver,
        as_of_date=date(2026, 7, 17),
    )

    assert json.loads(failures.read_text())["window_regressions"] == []
    published = pq.ParquetFile(silver / "asset_class=equity/symbol=AAPL/1d.parquet").read().to_pylist()
    assert len(published) == 3  # the good new bar published normally


def test_a_quarantined_symbols_stale_artifact_is_moved_not_just_unmanifested(tmp_path):
    """Apex resolves symbols by path construction and never consults the manifest
    (apex ohlc_provider.py:141-145). Un-manifesting a symbol leaves it serving stale
    corrupt data forever; moving the file is the only eviction apex can perceive."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    _seed_bronze(root, "INTC", [("2000-07-28", 129.13), ("2000-07-31", 66.75)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    assert (silver / "asset_class=equity/symbol=INTC/1d.parquet").exists()

    # INTC now fails staging: unknown-basis rows against its real 2000-07-31 1:2 split.
    _seed_bronze(root, "INTC", [("2000-07-28", 129.13), ("2000-07-31", 66.75)], price_basis="unknown")
    _seed_split(root, "INTC", "2000-07-31", 1, 2)

    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    assert not (silver / "asset_class=equity/symbol=INTC/1d.parquet").exists()
    assert (silver / "asset_class=equity/symbol=AAPL/1d.parquet").exists()
    current = json.loads((silver / "revisions/current.json").read_text())
    assert not any("symbol=INTC" in a["path"] for a in current["artifacts"])
    # Moved, not destroyed: an eviction must be reversible.
    assert (silver / "evicted/2/asset_class=equity/symbol=INTC/1d.parquet").is_file()


def test_eviction_leaves_the_factor_artifact_in_place(tmp_path):
    """Apex joins bronze intraday onto factors independently of the daily file, so
    removing the factor artifact is its own 500 rather than a clean fail-closed."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    _seed_bronze(root, "INTC", [("2000-07-28", 129.13), ("2000-07-31", 66.75)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(root, "INTC", [("2000-07-28", 129.13), ("2000-07-31", 66.75)], price_basis="unknown")
    _seed_split(root, "INTC", "2000-07-31", 1, 2)

    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    assert (silver / "adjustments/asset_class=equity/symbol=INTC/factors.parquet").is_file()


def test_a_dry_run_never_evicts(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    _seed_bronze(root, "INTC", [("2000-07-28", 129.13), ("2000-07-31", 66.75)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(root, "INTC", [("2000-07-28", 129.13), ("2000-07-31", 66.75)], price_basis="unknown")
    _seed_split(root, "INTC", "2000-07-31", 1, 2)

    rebuild_silver.run(["--full", "--dry-run"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    assert (silver / "asset_class=equity/symbol=INTC/1d.parquet").is_file()  # untouched


def test_an_all_quarantined_universe_refuses_to_publish_rather_than_evicting(tmp_path):
    """Degenerate but load-bearing. The publisher rejects an empty revision, so if we
    evicted anyway the current manifest would still name a file that is gone — and
    apex verifies every artifact's sha256 on every poll and rejects the WHOLE revision
    on a mismatch. One symbol going dark must never blank the entire service."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "INTC", [("2000-07-28", 129.13), ("2000-07-31", 66.75)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(root, "INTC", [("2000-07-28", 129.13), ("2000-07-31", 66.75)], price_basis="unknown")
    _seed_split(root, "INTC", "2000-07-31", 1, 2)

    with pytest.raises(SystemExit, match="refusing to publish an empty revision"):
        rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    # Nothing moved: the manifest and the tree still agree.
    assert (silver / "asset_class=equity/symbol=INTC/1d.parquet").is_file()


def test_a_vanished_artifact_is_not_carried_into_the_manifest(tmp_path):
    """Apex verifies every manifested artifact's sha256 on each poll and rejects the
    whole revision on a mismatch, so manifesting a file that is no longer on disk
    would blank the service rather than drop one symbol."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 370.87), ("2024-01-03", 370.60)])
    rebuild_silver.run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    # MSFT's artifact disappears out from under us (operator action, disk fault).
    (silver / "asset_class=equity/symbol=MSFT/1d.parquet").unlink()
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])

    rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    current = json.loads((silver / "revisions/current.json").read_text())
    paths = [a["path"] for a in current["artifacts"]]
    assert any("symbol=AAPL" in p for p in paths)
    assert not any("symbol=MSFT" in p for p in paths)  # not manifested — it is gone


def test_an_unreadable_published_artifact_is_treated_as_changed(tmp_path):
    """_matches_existing must not crash the rebuild on a truncated parquet."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    (silver / "asset_class=equity/symbol=AAPL/1d.parquet").write_bytes(b"not a parquet")

    assert (
        rebuild_silver.run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
        == 0
    )

    published = pq.ParquetFile(silver / "asset_class=equity/symbol=AAPL/1d.parquet").read().to_pylist()
    assert len(published) == 2  # republished over the corruption


def test_eviction_is_retried_after_a_previous_run_left_the_file_behind(tmp_path):
    """A committed manifest omitting Q plus a failed eviction is a reachable state, and
    every later run then assembles that SAME manifest. The publisher dedupes an
    identical manifest to the current revision (silver_revision.py:110), which trips
    the transaction's reserved-revision guard (:65) — so the run would crash before
    reaching the eviction retry and Q would serve its stale artifact forever.
    Real INTC closes around its real 2000-07-31 1:2 split; NVDA is the control."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "INTC", [("2000-07-27", 137.00), ("2000-07-28", 129.13), ("2000-08-01", 64.63)])
    _seed_bronze(root, "NVDA", [("2000-07-27", 1.71), ("2000-07-28", 1.66), ("2000-08-01", 1.80)])
    args = ["--tickers", "INTC", "NVDA"]
    assert rebuild_silver.run(args, data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17)) == 0
    served = silver / "asset_class=equity/symbol=INTC/1d.parquet"
    assert served.is_file()

    # INTC becomes unstageable: `unknown` basis against a split it cannot resolve.
    _seed_bronze(
        root,
        "INTC",
        [("2000-07-27", 137.00), ("2000-07-28", 129.13), ("2000-08-01", 64.63)],
        source="legacy",
        price_basis="unknown",
    )
    _seed_split(root, "INTC", "2000-07-31", 1, 2)
    rebuild_silver.run(args, data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    assert not served.is_file()  # evicted, manifest committed without it

    # Simulate that run's eviction having failed AFTER the commit (os.replace can fail
    # on a full or read-only volume): the manifest is already right, only the file is
    # stale. The next run must retry the eviction rather than abort.
    evicted_copy = next((silver / "evicted").rglob("symbol=INTC/1d.parquet"))
    served.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(evicted_copy, served)

    rebuild_silver.run(args, data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    assert not served.is_file()  # re-evicted, not left serving stale data


def test_full_rebuild_remanifests_orphaned_silver_files(tmp_path, capsys):
    # Two clean symbols → a full rebuild publishes both.
    _bronze(tmp_path, "AAA")
    _bronze(tmp_path, "BBB")
    silver = tmp_path / "silver"
    assert rebuild_silver.run(["--full"], data_lake_root=tmp_path, silver_root=silver) == 0
    capsys.readouterr()

    # Simulate manifest drift a past --tickers rebuild could leave: drop BBB from the
    # manifest but keep its (still-correct) file on disk → an orphan apex can't serve.
    current_path = silver / "revisions/current.json"
    manifest = json.loads(current_path.read_text())
    manifest["affected"] = [a for a in manifest["affected"] if a["symbol"] != "BBB"]
    manifest["artifacts"] = [a for a in manifest["artifacts"] if "symbol=BBB" not in a["path"]]
    current_path.write_text(json.dumps(manifest))
    assert (silver / "asset_class=equity/symbol=BBB/1d.parquet").is_file()

    # A full rebuild must re-manifest the orphan by reference (no rewrite) and advance rev.
    assert rebuild_silver.run(["--full"], data_lake_root=tmp_path, silver_root=silver) == 0
    summary = _summary_from(capsys)
    assert summary["orphans_remanifested"] == 1
    assert summary["rebuilt"] == 0  # BBB carried by reference, AAA unchanged
    remanifested = json.loads(current_path.read_text())
    assert {a["symbol"] for a in remanifested["affected"]} == {"AAA", "BBB"}


def test_full_rebuild_heartbeats_progress_to_the_ledger(tmp_path, monkeypatch):
    """A --full walk killed at its lane budget prints nothing; the ledger says how far it got."""
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260907T060000Z-1")
    monkeypatch.setattr(rebuild_silver, "_PROGRESS_EVERY", 2)
    for symbol in ("AAA", "BBB", "CCC"):
        _bronze(tmp_path, symbol)

    assert rebuild_silver.run(["--full"], data_lake_root=tmp_path, silver_root=tmp_path / "silver") == 0

    rows = ledger.query("select name, value, unit, run_id from measurements where scope = 'silver'")
    # Beat at symbol 2, then the closing beat at 3 — never a stale multiple of the cadence.
    assert sorted(row["value"] for row in rows if row["name"] == "progress") == [2.0, 3.0]
    assert {row["value"] for row in rows if row["name"] == "progress_total"} == {3.0}
    assert {row["unit"] for row in rows} == {"symbols"}
    assert {row["run_id"] for row in rows} == {"daily-update-20260907T060000Z-1"}
