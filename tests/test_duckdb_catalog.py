"""Tests for the DuckDB analytical catalog.

Fixtures are real bars for real tickers, captured from the warehouse on
2026-08-02 and frozen here. No synthetic prices, no network at runtime.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.duckdb_catalog import (
    COVERAGE_SOURCES,
    build_coverage,
    connect,
    ensure_view,
    read_symbols,
    symbol_files,
    view_names,
    view_spec,
    view_specs,
)
from clients.symbol_paths import canonical_symbol, encode_symbol

# Frozen snapshot of real bronze rows (source=massive, price_basis=raw),
# captured 2026-08-02 from ~/market-warehouse.
FROZEN_BARS: dict[str, list[tuple]] = {
    "NVDA": [
        (date(2026, 7, 30), 2817081741272460, 193.45, 197.25, 191.52, 195.04, 195.04, 129017841),
        (date(2026, 7, 31), 2817081741272460, 198.4405, 202.0, 194.95, 200.75, 200.75, 140011034),
    ],
    "HON": [
        (date(2026, 7, 30), 8613838052609349, 241.875, 242.42, 237.36, 241.91, 241.91, 2839931),
        (date(2026, 7, 31), 8613838052609349, 240.52, 246.0, 238.02, 243.05, 243.05, 3882162),
    ],
    # A real preferred share. Its partition directory is `symbol=ALL%70I`
    # because lowercase `p` is not filesystem-case-safe — 504 equity symbols in
    # the warehouse look like this.
    "ALLpI": [
        (date(2021, 6, 11), 677581891970577, 27.37, 27.47, 27.34, 27.46, 27.46, 8534),
        (date(2021, 6, 14), 677581891970577, 27.46, 27.62, 27.4, 27.62, 27.62, 24536),
    ],
}

_SCHEMA = pa.schema(
    [
        ("trade_date", pa.date32()),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adj_close", pa.float64()),
        ("volume", pa.int64()),
        ("source", pa.string()),
        ("price_basis", pa.string()),
    ]
)


def _write_symbol(directory: Path, symbol: str, filename: str = "1d.parquet") -> Path:
    rows = FROZEN_BARS[symbol]
    # Mirror the writers: partition names go through encode_symbol.
    target = directory / f"symbol={encode_symbol(canonical_symbol(symbol))}"
    target.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "trade_date": [r[0] for r in rows],
            "symbol_id": [r[1] for r in rows],
            "open": [r[2] for r in rows],
            "high": [r[3] for r in rows],
            "low": [r[4] for r in rows],
            "close": [r[5] for r in rows],
            "adj_close": [r[6] for r in rows],
            "volume": [r[7] for r in rows],
            "source": ["massive"] * len(rows),
            "price_basis": ["raw"] * len(rows),
        },
        schema=_SCHEMA,
    )
    path = target / filename
    pq.write_table(table, path)
    return path


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    """A minimal lake holding real NVDA and HON daily bars."""
    equity = tmp_path / "bronze" / "asset_class=equity"
    for symbol in ("NVDA", "HON"):
        _write_symbol(equity, symbol)
    return tmp_path


@pytest.fixture
def silver(tmp_path: Path) -> Path:
    """Silver holding only NVDA — HON is absent, mirroring the real gap."""
    root = tmp_path / "silver"
    _write_symbol(root / "asset_class=equity", "NVDA")
    return root


def test_view_specs_resolve_against_supplied_roots(lake: Path, silver: Path) -> None:
    specs = {spec.name: spec for spec in view_specs(lake_root=lake, silver_root=silver)}
    assert specs["bronze_equity_1d"].glob == str(lake / "bronze" / "asset_class=equity" / "*" / "1d.parquet")
    assert specs["silver_equity_1d"].glob == str(silver / "asset_class=equity" / "*" / "1d.parquet")
    assert specs["bronze_equity_1d"].path_for("NVDA").endswith("symbol=NVDA/1d.parquet")


def test_view_names_covers_every_spec() -> None:
    assert set(view_names()) == {spec.name for spec in view_specs()}
    assert "bronze_equity_1m" in view_names()  # intraday is view-only, never materialised


def test_unknown_view_raises() -> None:
    with pytest.raises(KeyError):
        view_spec("bronze_equity_7y")


def test_connect_registers_no_views_by_default(lake: Path, silver: Path) -> None:
    """The empty default is load-bearing: registering a view enumerates its glob."""
    con = connect(lake_root=lake, silver_root=silver)
    try:
        with pytest.raises(duckdb.CatalogException):
            con.sql("SELECT * FROM bronze_equity_1d")
    finally:
        con.close()


def test_connect_registers_only_named_views(lake: Path, silver: Path) -> None:
    con = connect(views=["bronze_equity_1d"], lake_root=lake, silver_root=silver)
    try:
        assert con.sql("SELECT count(*) FROM bronze_equity_1d").fetchone()[0] == 4
        with pytest.raises(duckdb.CatalogException):
            con.sql("SELECT * FROM silver_equity_1d")
    finally:
        con.close()


def test_ensure_view_registers_after_connect(lake: Path, silver: Path) -> None:
    con = connect(lake_root=lake, silver_root=silver)
    try:
        ensure_view(con, "bronze_equity_1d", lake_root=lake, silver_root=silver)
        assert con.sql("SELECT count(*) FROM bronze_equity_1d").fetchone()[0] == 4
    finally:
        con.close()


def test_symbol_files_skips_absent_symbols(lake: Path, silver: Path) -> None:
    files = symbol_files("bronze_equity_1d", ["NVDA", "NOT_A_SYMBOL"], lake_root=lake, silver_root=silver)
    assert len(files) == 1
    assert files[0].endswith("symbol=NVDA/1d.parquet")


def test_symbol_files_can_fail_closed(lake: Path, silver: Path) -> None:
    with pytest.raises(FileNotFoundError):
        symbol_files("bronze_equity_1d", ["NOT_A_SYMBOL"], lake_root=lake, silver_root=silver, missing_ok=False)


def test_read_symbols_returns_only_named_symbols_with_real_values(lake: Path, silver: Path) -> None:
    con = connect(lake_root=lake, silver_root=silver)
    try:
        relation = read_symbols(con, "bronze_equity_1d", ["NVDA"], lake_root=lake, silver_root=silver)
        con.register("selected", relation)
        rows = con.sql("SELECT symbol, trade_date, close, volume FROM selected ORDER BY trade_date").fetchall()
    finally:
        con.close()
    assert rows == [
        ("NVDA", date(2026, 7, 30), 195.04, 129017841),
        ("NVDA", date(2026, 7, 31), 200.75, 140011034),
    ]


def test_read_symbols_raises_when_nothing_resolves(lake: Path, silver: Path) -> None:
    con = connect(lake_root=lake, silver_root=silver)
    try:
        with pytest.raises(FileNotFoundError):
            read_symbols(con, "bronze_equity_1d", ["NOT_A_SYMBOL"], lake_root=lake, silver_root=silver)
    finally:
        con.close()


def test_build_coverage_publishes_expected_rows(tmp_path: Path, lake: Path, silver: Path) -> None:
    dest = tmp_path / "analytics.duckdb"
    counts = build_coverage(dest, lake_root=lake, silver_root=silver)

    assert counts["bronze_equity_1d"] == 2
    assert counts["silver_equity_1d"] == 1
    assert dest.exists()

    con = connect(dest, read_only=True)
    try:
        rows = con.sql(
            "SELECT view_name, symbol, n_rows, first_date, last_date FROM coverage ORDER BY view_name, symbol"
        ).fetchall()
    finally:
        con.close()
    assert rows == [
        ("bronze_equity_1d", "HON", 2, date(2026, 7, 30), date(2026, 7, 31)),
        ("bronze_equity_1d", "NVDA", 2, date(2026, 7, 30), date(2026, 7, 31)),
        ("silver_equity_1d", "NVDA", 2, date(2026, 7, 30), date(2026, 7, 31)),
    ]


def test_build_coverage_surfaces_symbol_absent_from_silver(tmp_path: Path, lake: Path, silver: Path) -> None:
    """The cross-tier question that motivated the table: HON has bronze, no silver."""
    dest = tmp_path / "analytics.duckdb"
    build_coverage(dest, lake_root=lake, silver_root=silver)
    con = connect(dest, read_only=True)
    try:
        absent = con.sql(
            """
            SELECT b.symbol FROM coverage b
            WHERE b.view_name = 'bronze_equity_1d'
              AND NOT EXISTS (SELECT 1 FROM coverage s
                              WHERE s.view_name = 'silver_equity_1d' AND s.symbol = b.symbol)
            """
        ).fetchall()
    finally:
        con.close()
    assert absent == [("HON",)]


def test_build_coverage_leaves_no_staging_artifacts(tmp_path: Path, lake: Path, silver: Path) -> None:
    dest = tmp_path / "analytics.duckdb"
    build_coverage(dest, lake_root=lake, silver_root=silver)
    assert not dest.with_name(dest.name + ".building").exists()
    assert not dest.with_name(dest.name + ".building.wal").exists()
    assert not dest.with_name(dest.name + ".wal").exists()


def test_build_coverage_replaces_previous_database(tmp_path: Path, lake: Path, silver: Path) -> None:
    dest = tmp_path / "analytics.duckdb"
    build_coverage(dest, lake_root=lake, silver_root=silver)
    _write_symbol(lake / "bronze" / "asset_class=equity", "HON", filename="1d.parquet")
    counts = build_coverage(dest, lake_root=lake, silver_root=silver)
    assert counts["bronze_equity_1d"] == 2
    con = connect(dest, read_only=True)
    try:
        assert con.sql("SELECT count(*) FROM coverage WHERE view_name='bronze_equity_1d'").fetchone()[0] == 2
    finally:
        con.close()


def test_build_coverage_fails_when_every_source_is_empty(tmp_path: Path) -> None:
    empty_lake = tmp_path / "empty-lake"
    empty_silver = tmp_path / "empty-silver"
    empty_lake.mkdir()
    empty_silver.mkdir()
    with pytest.raises(RuntimeError, match="no rows"):
        build_coverage(tmp_path / "out.duckdb", lake_root=empty_lake, silver_root=empty_silver)


def test_build_coverage_tolerates_individually_absent_asset_classes(tmp_path: Path, lake: Path, silver: Path) -> None:
    """cmdty/fx are legitimately absent on a fresh lake and must not abort the build."""
    counts = build_coverage(tmp_path / "analytics.duckdb", lake_root=lake, silver_root=silver)
    assert counts["bronze_cmdty_1d"] == 0
    assert counts["bronze_equity_1d"] == 2


def test_coverage_sources_are_daily_only() -> None:
    """Intraday must stay out: a coverage pass over it would scan 23.57 GB."""
    names = [name for name, _ in COVERAGE_SOURCES]
    assert not any(name.endswith(("_1m", "_5m", "_30m", "_1h")) for name in names)
    assert "bronze_equity_1d" in names
    assert "silver_equity_1d" in names


def test_concurrent_read_only_connections_are_allowed(tmp_path: Path, lake: Path, silver: Path) -> None:
    dest = tmp_path / "analytics.duckdb"
    build_coverage(dest, lake_root=lake, silver_root=silver)
    first = connect(dest, read_only=True)
    second = connect(dest, read_only=True)
    try:
        assert first.sql("SELECT count(*) FROM coverage").fetchone()[0] == 3
        assert second.sql("SELECT count(*) FROM coverage").fetchone()[0] == 3
    finally:
        first.close()
        second.close()


def test_path_for_encodes_partition_names(tmp_path: Path) -> None:
    """504 real equity symbols live under percent-escaped directories."""
    spec = view_spec("bronze_equity_1d", lake_root=tmp_path, silver_root=tmp_path)
    assert spec.path_for("ALLpI").endswith("symbol=ALL%70I/1d.parquet")
    assert spec.path_for("NVDA").endswith("symbol=NVDA/1d.parquet")


def test_path_for_normalises_lowercase_input(tmp_path: Path) -> None:
    spec = view_spec("bronze_equity_1d", lake_root=tmp_path, silver_root=tmp_path)
    assert spec.path_for("nvda") == spec.path_for("NVDA")


def test_read_symbols_resolves_an_encoded_symbol(tmp_path: Path) -> None:
    """The bug this guards: raw interpolation missed every preferred share."""
    equity = tmp_path / "bronze" / "asset_class=equity"
    _write_symbol(equity, "ALLpI")
    con = connect(lake_root=tmp_path, silver_root=tmp_path)
    try:
        relation = read_symbols(con, "bronze_equity_1d", ["ALLpI"], lake_root=tmp_path, silver_root=tmp_path)
        con.register("selected", relation)
        rows = con.sql("SELECT symbol, trade_date, close FROM selected ORDER BY trade_date").fetchall()
    finally:
        con.close()
    # hive_partitioning unescapes on the way back, so the symbol reads as ALLpI.
    assert rows == [("ALLpI", date(2021, 6, 11), 27.46), ("ALLpI", date(2021, 6, 14), 27.62)]


def test_coverage_headline_reports_symbols_and_newest_date(tmp_path: Path, lake: Path, silver: Path) -> None:
    from clients.duckdb_catalog import coverage_headline

    database = tmp_path / "analytics.duckdb"
    build_coverage(database, lake_root=lake, silver_root=silver)
    headline = coverage_headline(database)

    assert "bronze_equity_1d" in headline
    symbols, last = headline["bronze_equity_1d"]
    assert symbols > 0
    assert last is not None


def test_coverage_headline_raises_when_the_catalog_was_never_built(tmp_path: Path) -> None:
    from clients.duckdb_catalog import coverage_headline

    with pytest.raises(FileNotFoundError):
        coverage_headline(tmp_path / "absent.duckdb")


def test_coverage_headline_raises_when_the_file_holds_no_coverage_table(tmp_path: Path) -> None:
    """What an interrupted `duckdb build` leaves behind. The caller cannot
    catch DuckDB's own exception — importing duckdb is what the containment
    test forbids it — so the translation has to happen here."""
    from clients.duckdb_catalog import connect, coverage_headline

    database = tmp_path / "empty.duckdb"
    connect(database, read_only=False).close()

    with pytest.raises(FileNotFoundError):
        coverage_headline(database)
