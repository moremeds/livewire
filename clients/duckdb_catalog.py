"""DuckDB analytical catalog over the Livewire parquet lake.

Parquet stays the system of record. This module never copies bar data: it
registers *views* over the lake so SQL can address it, plus a small coverage
table whose rows are per-symbol file statistics — not bars.

Everything here is shaped by one measured fact: **enumerating a glob over the
lake is the dominant cost, and it dwarfs reading the data.** Measured
2026-08-02 against the real lake (13,270 equity 1d files, 19.75M rows):

===================================================  ========
Open one known parquet file                            0.04s
``CREATE VIEW`` over the equity ``1h`` glob          221.04s
Whole-universe ``count(*)``, filesystem cache warm      0.86s
The same query once the cache had been evicted       283.84s
===================================================  ========

Three consequences drive the API:

1. **Views are registered on demand, never eagerly.** ``CREATE VIEW`` binds the
   schema, and binding enumerates the glob — so registering all 13 views costs
   13 full enumerations before a single query runs. :func:`connect` therefore
   registers nothing by default; call :func:`ensure_view` for what you need.
2. **Symbol-scoped reads bypass views entirely.** The layout is the contract
   (``symbol=<TICKER>/<timeframe>.parquet``) and Apex already resolves symbols
   by construction, so :func:`read_symbols` builds an explicit path list —
   0.04s per file against 221s to bind the glob it would otherwise sit behind.
3. **The coverage table is durable.** The filesystem metadata cache does not
   survive the night: the nightly job writes 23.57 GB of intraday, which is
   what evicted the cache between the 0.86s and 283.84s readings above. Cold
   is the normal morning state, so freshness questions get their own table
   rather than being re-derived from 13,270 parquet footers on every ask.

Intraday is deliberately view-only. Equity ``1m`` alone is 23.57 GB against
~20 GiB of free disk, so it cannot be materialised even if we wanted to.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb

from clients.symbol_paths import canonical_symbol, encode_symbol
from livewire_scripts.paths import data_lake_dir, silver_dir, warehouse_dir

# ponytail: TEMP views, not persisted ones. A read_only connection cannot
# CREATE VIEW in the database but can create temporary ones, so this single
# form works for in-memory, read-write and read_only alike.
_VIEW_SQL = "CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM read_parquet({glob!r}, hive_partitioning=1)"

_EQUITY_INTRADAY = ("1m", "5m", "30m", "1h")
_DAILY_ASSET_CLASSES = ("equity", "volatility", "futures", "rates", "fx", "cmdty")


@dataclass(frozen=True)
class ViewSpec:
    """One SQL view over a set of parquet files in the lake.

    ``directory`` is the parent holding the ``symbol=<TICKER>`` partitions and
    ``filename`` the per-symbol file inside them. Keeping the halves separate
    rather than storing a finished glob is what lets :func:`symbol_files`
    address files directly instead of paying for enumeration.
    """

    name: str
    directory: str
    filename: str

    @property
    def glob(self) -> str:
        return str(Path(self.directory) / "*" / self.filename)

    def path_for(self, symbol: str) -> str:
        """Resolve one symbol's file, encoding the partition name as the writers do.

        Partition directories are written through ``encode_symbol``, so 504 real
        equity symbols live under percent-escaped names (``ALLpI`` ->
        ``symbol=ALL%70I``). Interpolating the raw ticker would miss every one of
        them. Reading back needs no matching decode: DuckDB's ``hive_partitioning``
        already unescapes, so the ``symbol`` column yields ``ALLpI``.
        """
        partition = encode_symbol(canonical_symbol(symbol))
        return str(Path(self.directory) / f"symbol={partition}" / self.filename)


def view_specs(
    lake_root: Path | None = None,
    silver_root: Path | None = None,
) -> list[ViewSpec]:
    """Return every view the catalog knows about, resolved against the lake."""
    lake = Path(lake_root) if lake_root is not None else data_lake_dir()
    silver = Path(silver_root) if silver_root is not None else silver_dir()
    bronze = lake / "bronze"

    specs = [
        ViewSpec(f"bronze_{asset}_1d", str(bronze / f"asset_class={asset}"), "1d.parquet")
        for asset in _DAILY_ASSET_CLASSES
    ]
    specs += [
        ViewSpec(f"bronze_equity_{timeframe}", str(bronze / "asset_class=equity"), f"{timeframe}.parquet")
        for timeframe in _EQUITY_INTRADAY
    ]
    specs.append(ViewSpec("corporate_actions", str(bronze / "asset_class=corporate_action"), "events.parquet"))
    specs.append(ViewSpec("silver_equity_1d", str(silver / "asset_class=equity"), "1d.parquet"))
    specs.append(ViewSpec("silver_factors", str(silver / "adjustments" / "asset_class=equity"), "factors.parquet"))
    return specs


def view_spec(
    name: str,
    *,
    lake_root: Path | None = None,
    silver_root: Path | None = None,
) -> ViewSpec:
    """Return one view spec by name."""
    for spec in view_specs(lake_root=lake_root, silver_root=silver_root):
        if spec.name == name:
            return spec
    raise KeyError(f"unknown view {name!r}")


def view_names() -> list[str]:
    """Return the registered view names without resolving any paths."""
    return [spec.name for spec in view_specs()]


def default_database() -> Path:
    """Return the catalog database path."""
    return Path(os.environ.get("MDW_DUCKDB_PATH", warehouse_dir() / "analytics.duckdb")).expanduser()


def connect(
    database: Path | str | None = None,
    *,
    read_only: bool = False,
    views: Iterable[str] = (),
    lake_root: Path | None = None,
    silver_root: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Open a catalog connection.

    ``views`` is empty by default and that default is load-bearing: registering
    a view enumerates its glob (221s for equity ``1h``), so eagerly registering
    all of them would make every connection unusable. Name only what the query
    touches, or call :func:`ensure_view` later.

    ``database`` defaults to in-memory. ``read_only=True`` is safe to run
    concurrently — four simultaneous readers measured 0.00s each — but a
    *writer* cannot open a database that readers hold, which is why
    :func:`build_coverage` publishes by replacing the file.
    """
    target = ":memory:" if database is None else str(database)
    con = duckdb.connect(target, read_only=read_only)
    for name in views:
        ensure_view(con, name, lake_root=lake_root, silver_root=silver_root)
    return con


def ensure_view(
    con: duckdb.DuckDBPyConnection,
    name: str,
    *,
    lake_root: Path | None = None,
    silver_root: Path | None = None,
) -> None:
    """Register one lake view on *con*.

    Costs a full glob enumeration of that view's files — seconds when the
    filesystem cache is warm, minutes when it is not. Register deliberately.
    """
    spec = view_spec(name, lake_root=lake_root, silver_root=silver_root)
    con.execute(_VIEW_SQL.format(name=spec.name, glob=spec.glob))


def symbol_files(
    view_name: str,
    symbols: Iterable[str],
    *,
    lake_root: Path | None = None,
    silver_root: Path | None = None,
    missing_ok: bool = True,
) -> list[str]:
    """Resolve explicit parquet paths for *symbols*, skipping absent ones.

    Querying a handful of symbols *through* a glob view still enumerates every
    file behind it. Constructing the paths instead turns a 221s bind into a
    0.04s open per file. Use this for any query that names its symbols.
    """
    spec = view_spec(view_name, lake_root=lake_root, silver_root=silver_root)
    resolved: list[str] = []
    for symbol in symbols:
        path = spec.path_for(symbol)
        if Path(path).exists():
            resolved.append(path)
        elif not missing_ok:
            raise FileNotFoundError(path)
    return resolved


def read_symbols(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    symbols: Iterable[str],
    *,
    lake_root: Path | None = None,
    silver_root: Path | None = None,
) -> duckdb.DuckDBPyRelation:
    """Read only the named symbols by direct path. See :func:`symbol_files`.

    Needs no registered view, which is the point — it never pays enumeration.
    """
    files = symbol_files(view_name, symbols, lake_root=lake_root, silver_root=silver_root)
    if not files:
        raise FileNotFoundError(f"no parquet files for {list(symbols)!r} in view {view_name!r}")
    return con.sql(f"SELECT * FROM read_parquet({files!r}, hive_partitioning=1)")


# Views the coverage build summarises, with the column holding each row's date.
# Daily only. A coverage pass over the intraday tier would enumerate and scan
# 23.57 GB, and footer-only extraction is not the shortcut it appears to be:
# parquet_metadata() over the equity 1d glob measured 471s against 118s for
# this plain aggregate.
COVERAGE_SOURCES: tuple[tuple[str, str], ...] = tuple(
    [(f"bronze_{asset}_1d", "trade_date") for asset in _DAILY_ASSET_CLASSES] + [("silver_equity_1d", "trade_date")]
)

_COVERAGE_DDL = """
CREATE TABLE coverage (
    view_name  VARCHAR NOT NULL,
    symbol     VARCHAR NOT NULL,
    n_rows     BIGINT  NOT NULL,
    first_date DATE,
    last_date  DATE
)
"""


def _coverage_insert(view_name: str, date_column: str) -> str:
    return f"""
    INSERT INTO coverage
    SELECT {view_name!r} AS "view_name",
           CAST(symbol AS VARCHAR) AS "symbol",
           count(*) AS "n_rows",
           CAST(min({date_column}) AS DATE) AS "first_date",
           CAST(max({date_column}) AS DATE) AS "last_date"
    FROM {view_name}
    GROUP BY symbol
    """


def build_coverage(
    database: Path | str | None = None,
    *,
    sources: tuple[tuple[str, str], ...] = COVERAGE_SOURCES,
    lake_root: Path | None = None,
    silver_root: Path | None = None,
) -> dict[str, int]:
    """Rebuild the coverage table and publish it atomically.

    Builds into a sibling temp database, then :func:`os.replace` swaps it into
    place. Readers holding the previous file keep serving from it; new
    connections see the new one. This mirrors the temp -> validate -> replace
    contract bronze publication already uses, and it is *required* rather than
    stylistic: DuckDB is single-writer, so rebuilding in place fails outright
    whenever a reader is connected.

    A source whose asset class has no parquet at all contributes zero rows and
    is skipped rather than failing the build — ``cmdty`` and ``fx`` are legitimately
    absent on a fresh lake. The build fails only if *every* source is empty.

    Returns row counts per source view.
    """
    dest = Path(database) if database is not None else default_database()
    dest.parent.mkdir(parents=True, exist_ok=True)
    staging = dest.with_name(f"{dest.name}.building")
    for leftover in (staging, staging.with_name(staging.name + ".wal")):
        leftover.unlink(missing_ok=True)

    counts: dict[str, int] = {}
    con = connect(staging, lake_root=lake_root, silver_root=silver_root)
    try:
        con.execute(_COVERAGE_DDL)
        for view_name, date_column in sources:
            try:
                ensure_view(con, view_name, lake_root=lake_root, silver_root=silver_root)
                con.execute(_coverage_insert(view_name, date_column))
            except duckdb.IOException:
                # No files behind this view yet; leave it out rather than abort.
                counts[view_name] = 0
                continue
            counts[view_name] = con.execute(
                "SELECT count(*) FROM coverage WHERE view_name = ?", [view_name]
            ).fetchone()[0]
        if not any(counts.values()):
            raise RuntimeError(f"coverage build produced no rows for any of {[s[0] for s in sources]}")
    finally:
        con.close()

    # A clean close checkpoints the WAL away; refuse to publish if one survives,
    # because os.replace moves only the database file and would strand it.
    stray_wal = staging.with_name(staging.name + ".wal")
    if stray_wal.exists():
        raise RuntimeError(f"refusing to publish: uncheckpointed WAL at {stray_wal}")

    os.replace(staging, dest)
    return counts


@contextmanager
def open_catalog(
    database: Path | str | None = None,
    *,
    read_only: bool = True,
    views: Iterable[str] = (),
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Context-managed catalog connection, read-only by default."""
    con = connect(
        database if database is not None else default_database(),
        read_only=read_only,
        views=views,
    )
    try:
        yield con
    finally:
        con.close()
