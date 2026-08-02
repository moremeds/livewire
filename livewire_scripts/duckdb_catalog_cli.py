#!/usr/bin/env python3
"""Query the lake through DuckDB and report coverage / freshness."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.duckdb_catalog import (
    build_coverage,
    connect,
    default_database,
    read_symbols,
    view_names,
    view_specs,
)
from clients.trading_calendar import is_trading_day, previous_trading_day


def _resolve_target(value: str | None) -> date:
    if value:
        return date.fromisoformat(value)
    today = date.today()
    return today if is_trading_day(today) else previous_trading_day(today)


def _cmd_build(args: argparse.Namespace) -> int:
    counts = build_coverage(args.database)
    for view_name, rows in counts.items():
        print(f"{view_name}: {rows:,} symbols")
    print(f"published -> {args.database or default_database()}")
    return 0


def _cmd_views(args: argparse.Namespace) -> int:
    for spec in view_specs():
        print(f"{spec.name:24s} {spec.glob}")
    return 0


def _cmd_sql(args: argparse.Namespace) -> int:
    # Register only the views the query actually names. Registering a view
    # enumerates its glob (221s for equity 1h), so registering all of them
    # would cost minutes before the query even starts.
    needed = [name for name in view_names() if name in args.query]
    database = args.database or (default_database() if default_database().exists() else None)
    if needed:
        print(f"-- binding views: {', '.join(needed)} (enumerates their files)", file=sys.stderr)
    con = connect(database, read_only=bool(database), views=needed)
    try:
        result = con.sql(args.query)
        if result is not None:
            print(result)
    finally:
        con.close()
    return 0


def _cmd_bars(args: argparse.Namespace) -> int:
    """Read named symbols by direct path — never through the glob."""
    con = connect()
    try:
        relation = read_symbols(con, args.view, args.symbols)
        where = f" WHERE {args.where}" if args.where else ""
        con.register("_selected", relation)
        print(con.sql(f"SELECT * FROM _selected{where} ORDER BY 1 LIMIT {args.limit}"))
    finally:
        con.close()
    return 0


def _require_coverage(database: Path | None):
    path = Path(database) if database else default_database()
    if not path.exists():
        print(f"no coverage database at {path} — run `duckdb build` first", file=sys.stderr)
        return None
    return connect(path, read_only=True)


def _cmd_freshness(args: argparse.Namespace) -> int:
    con = _require_coverage(args.database)
    if con is None:
        return 2
    target = _resolve_target(args.target_date)
    try:
        rows = con.execute(
            """
            SELECT view_name,
                   count(*) FILTER (WHERE last_date >= ?)                         AS current,
                   count(*) FILTER (WHERE last_date < ? AND last_date >= ? - 7)   AS within_7d,
                   count(*) FILTER (WHERE last_date < ? - 7 AND last_date >= ? - 30) AS within_30d,
                   count(*) FILTER (WHERE last_date < ? - 30)                     AS over_30d,
                   count(*)                                                       AS symbols
            FROM coverage GROUP BY view_name ORDER BY view_name
            """,
            [target] * 6,
        ).fetchall()
    finally:
        con.close()

    print(f"as of {target}")
    header = f"{'view':24s} {'symbols':>9s} {'current':>9s} {'<=7d':>7s} {'<=30d':>7s} {'>30d':>7s}"
    print(header)
    print("-" * len(header))
    for view_name, current, within_7d, within_30d, over_30d, symbols in rows:
        print(f"{view_name:24s} {symbols:>9,} {current:>9,} {within_7d:>7,} {within_30d:>7,} {over_30d:>7,}")
    return 0


def _cmd_lag(args: argparse.Namespace) -> int:
    """Symbols whose silver trails its own bronze, or is missing entirely."""
    con = _require_coverage(args.database)
    if con is None:
        return 2
    target = _resolve_target(args.target_date)
    try:
        lagging = con.execute(
            """
            SELECT b.symbol, b.last_date, s.last_date
            FROM coverage b JOIN coverage s ON b.symbol = s.symbol
            WHERE b.view_name = 'bronze_equity_1d' AND s.view_name = 'silver_equity_1d'
              AND b.last_date >= ? AND s.last_date < ?
            ORDER BY s.last_date, b.symbol
            """,
            [target, target],
        ).fetchall()
        absent = con.execute(
            """
            SELECT b.symbol, b.n_rows, b.last_date
            FROM coverage b
            WHERE b.view_name = 'bronze_equity_1d'
              AND NOT EXISTS (
                SELECT 1 FROM coverage s
                WHERE s.view_name = 'silver_equity_1d' AND s.symbol = b.symbol)
            ORDER BY b.n_rows DESC
            """
        ).fetchall()
    finally:
        con.close()

    if args.json:
        print(
            json.dumps(
                {
                    "as_of": target.isoformat(),
                    "silver_lagging_bronze": [
                        {"symbol": s, "bronze_last": str(b), "silver_last": str(v)} for s, b, v in lagging
                    ],
                    "absent_from_silver": [
                        {"symbol": s, "rows": n, "bronze_last": str(d)} for s, n, d in absent
                    ],
                },
                indent=2,
            )
        )
        return 0

    print(f"as of {target}")
    print(f"\nsilver lagging bronze: {len(lagging):,}")
    for symbol, bronze_last, silver_last in lagging[: args.limit]:
        print(f"  {symbol:10s} bronze={bronze_last} silver={silver_last}")
    print(f"\nabsent from silver entirely: {len(absent):,}")
    for symbol, n_rows, last in absent[: args.limit]:
        print(f"  {symbol:10s} rows={n_rows:>7,} bronze_last={last}")
    return 0


def _cmd_stale(args: argparse.Namespace) -> int:
    con = _require_coverage(args.database)
    if con is None:
        return 2
    target = _resolve_target(args.target_date)
    try:
        rows = con.execute(
            """
            SELECT symbol, last_date, n_rows FROM coverage
            WHERE view_name = ? AND last_date < ? - ?
            ORDER BY last_date, symbol
            """,
            [args.view, target, args.days],
        ).fetchall()
    finally:
        con.close()
    print(f"{args.view}: {len(rows):,} symbols stale by more than {args.days} days as of {target}")
    for symbol, last, n_rows in rows[: args.limit]:
        print(f"  {symbol:10s} last={last} rows={n_rows:,}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=None, help="catalog path; defaults to MDW_DUCKDB_PATH")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("build", help="rebuild and publish the coverage table").set_defaults(func=_cmd_build)
    sub.add_parser("views", help="list registered views").set_defaults(func=_cmd_views)

    p_sql = sub.add_parser("sql", help="run a SQL query against the lake (whole-universe; enumerates the glob)")
    p_sql.add_argument("query")
    p_sql.set_defaults(func=_cmd_sql)

    p_bars = sub.add_parser("bars", help="read named symbols by direct path (fast; skips the glob)")
    p_bars.add_argument("--view", default="bronze_equity_1d")
    p_bars.add_argument("--symbols", nargs="+", required=True)
    p_bars.add_argument("--where", default=None, help="extra SQL predicate")
    p_bars.add_argument("--limit", type=int, default=20)
    p_bars.set_defaults(func=_cmd_bars)

    p_fresh = sub.add_parser("freshness", help="per-view staleness buckets")
    p_fresh.add_argument("--target-date", default=None)
    p_fresh.set_defaults(func=_cmd_freshness)

    p_lag = sub.add_parser("lag", help="silver trailing or missing vs bronze")
    p_lag.add_argument("--target-date", default=None)
    p_lag.add_argument("--limit", type=int, default=25)
    p_lag.add_argument("--json", action="store_true")
    p_lag.set_defaults(func=_cmd_lag)

    p_stale = sub.add_parser("stale", help="symbols with no recent bar")
    p_stale.add_argument("--view", default="bronze_equity_1d")
    p_stale.add_argument("--days", type=int, default=30)
    p_stale.add_argument("--target-date", default=None)
    p_stale.add_argument("--limit", type=int, default=50)
    p_stale.set_defaults(func=_cmd_stale)

    args = parser.parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
