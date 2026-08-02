"""Tests for the `livewire_store.py duckdb` command surface.

Drives the CLI end to end against a temporary lake built from the same frozen
real bars as `test_duckdb_catalog.py`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from livewire_scripts.duckdb_catalog_cli import main
from tests.test_duckdb_catalog import _write_symbol


@pytest.fixture
def wired(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the CLI's default path resolution at a disposable lake."""
    lake = tmp_path / "data-lake"
    equity = lake / "bronze" / "asset_class=equity"
    for symbol in ("NVDA", "HON", "ALLpI"):
        _write_symbol(equity, symbol)
    # Silver deliberately omits HON — the "absent from silver" case.
    _write_symbol(lake / "silver" / "asset_class=equity", "NVDA")

    monkeypatch.setenv("MDW_DATA_LAKE", str(lake))
    monkeypatch.setenv("MDW_SILVER_DIR", str(lake / "silver"))
    monkeypatch.setenv("MDW_DUCKDB_PATH", str(tmp_path / "analytics.duckdb"))
    return tmp_path


def test_views_lists_every_view(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["views"]) == 0
    out = capsys.readouterr().out
    assert "bronze_equity_1d" in out
    assert "silver_factors" in out


def test_build_reports_counts_and_publishes(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["build"]) == 0
    out = capsys.readouterr().out
    assert "bronze_equity_1d: 3 symbols" in out
    assert (wired / "analytics.duckdb").exists()


def test_freshness_requires_a_built_catalog(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["freshness"]) == 2
    assert "run `duckdb build` first" in capsys.readouterr().err


def test_freshness_buckets_by_staleness(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["build"])
    capsys.readouterr()
    assert main(["freshness", "--target-date", "2026-07-31"]) == 0
    out = capsys.readouterr().out
    assert "as of 2026-07-31" in out
    # NVDA and HON are current; ALLpI last traded 2021 and is >30d stale.
    assert "bronze_equity_1d" in out


def test_lag_reports_absent_and_trailing_symbols(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["build"])
    capsys.readouterr()
    assert main(["lag", "--target-date", "2026-07-31"]) == 0
    out = capsys.readouterr().out
    assert "absent from silver entirely: 2" in out
    assert "HON" in out


def test_lag_json_output_is_machine_readable(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    import json

    main(["build"])
    capsys.readouterr()
    assert main(["lag", "--target-date", "2026-07-31", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["as_of"] == "2026-07-31"
    assert {entry["symbol"] for entry in payload["absent_from_silver"]} == {"HON", "ALLpI"}


def test_stale_lists_symbols_with_no_recent_bar(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["build"])
    capsys.readouterr()
    assert main(["stale", "--target-date", "2026-07-31", "--days", "30"]) == 0
    out = capsys.readouterr().out
    assert "ALLpI" in out
    assert "NVDA" not in out


def test_stale_requires_a_built_catalog(wired: Path) -> None:
    assert main(["stale"]) == 2


def test_lag_requires_a_built_catalog(wired: Path) -> None:
    assert main(["lag"]) == 2


def test_bars_reads_named_symbols_without_the_glob(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["bars", "--symbols", "NVDA", "--limit", "5"]) == 0
    out = capsys.readouterr().out
    assert "NVDA" in out
    assert "HON" not in out


def test_bars_resolves_a_percent_encoded_symbol(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["bars", "--symbols", "ALLpI", "--limit", "5"]) == 0
    assert "ALLpI" in capsys.readouterr().out


def test_bars_accepts_an_extra_predicate(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["bars", "--symbols", "NVDA", "--where", "trade_date = DATE '2026-07-31'"]) == 0
    out = capsys.readouterr().out
    assert "2026-07-31" in out
    assert "2026-07-30" not in out


def test_sql_binds_only_the_views_the_query_names(
    wired: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["sql", "SELECT count(*) AS n FROM bronze_equity_1d"]) == 0
    captured = capsys.readouterr()
    assert "binding views: bronze_equity_1d" in captured.err
    assert "silver_equity_1d" not in captured.err
    assert "6" in captured.out  # 3 symbols x 2 bars


def test_sql_without_a_known_view_binds_nothing(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["sql", "SELECT 1 AS one"]) == 0
    captured = capsys.readouterr()
    assert "binding views" not in captured.err


def test_target_date_defaults_to_the_last_trading_day(wired: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["build"])
    capsys.readouterr()
    assert main(["freshness"]) == 0
    assert "as of " in capsys.readouterr().out
