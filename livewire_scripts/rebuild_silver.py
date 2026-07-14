#!/usr/bin/env python3
"""Rebuild adjusted Silver bars and factor intervals from canonical bronze."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from clients.adjustment_engine import FactorInterval, adjust_daily_rows, build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateAction, CorporateActionStore
from clients.silver_client import SilverClient
from clients.silver_revision import AffectedSymbol, SilverRevisionPublisher
from livewire_scripts.daily_outcomes import resolve_exit_code
from livewire_scripts.paths import data_lake_dir

TIMEFRAMES = ("1d", "1m", "5m", "30m", "1h")
NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class StagedSymbol:
    symbol: str
    rows: list[dict]
    intervals: list[FactorInterval]
    actions: list[CorporateAction]
    earliest_date: date


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+", help="Explicit equity symbols")
    scope.add_argument("--full", action="store_true", help="Discover all equity bronze symbols")
    parser.add_argument("--dry-run", action="store_true", help="Compute and compare without publishing")
    return parser.parse_args(list(argv) if argv is not None else None)


def default_silver_root(root: Path) -> Path:
    return Path(os.environ.get("MDW_SILVER_DIR", root / "silver")).expanduser()


def _trade_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _daily_semantics(rows: list[dict]) -> list[tuple]:
    columns = (
        "trade_date",
        "symbol_id",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "price_adjustment_factor",
        "split_volume_factor",
    )
    return [
        tuple(_trade_date(row[column]) if column == "trade_date" else row[column] for column in columns)
        for row in sorted(rows, key=lambda row: _trade_date(row["trade_date"]))
    ]


def _factor_semantics(intervals: list[FactorInterval]) -> list[tuple]:
    return [
        (
            item.effective_start,
            item.effective_end,
            float(item.price_adjustment_factor),
            float(item.split_volume_factor),
        )
        for item in sorted(intervals, key=lambda item: item.effective_start)
    ]


def _matches_existing(client: SilverClient, staged: StagedSymbol) -> bool:
    daily_path = client.daily_path(staged.symbol)
    factor_path = client.factor_path(staged.symbol)
    if not daily_path.exists() or not factor_path.exists():
        return False
    try:
        daily_rows = pq.ParquetFile(daily_path).read().to_pylist()
        factor_rows = pq.ParquetFile(factor_path).read().to_pylist()
    except Exception:
        return False
    existing_intervals = [
        FactorInterval(
            row["effective_start"],
            row["effective_end"],
            row["price_adjustment_factor"],
            row["split_volume_factor"],
            row["adjustment_revision"],
        )
        for row in factor_rows
    ]
    candidate_daily = adjust_daily_rows(staged.rows, staged.intervals, revision=1)
    return _daily_semantics(daily_rows) == _daily_semantics(candidate_daily) and _factor_semantics(
        existing_intervals
    ) == _factor_semantics(staged.intervals)


def _summary(**values) -> None:
    print(json.dumps(values, sort_keys=True))


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    silver_root: Path | None = None,
    as_of_date: date | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else data_lake_dir()
    silver_path = Path(silver_root) if silver_root is not None else default_silver_root(root)
    bronze = BronzeClient(root / "bronze" / "asset_class=equity", "equity")
    action_store = CorporateActionStore(root)
    client = SilverClient(silver_path)
    publisher = SilverRevisionPublisher(silver_path)
    symbols = (
        sorted(bronze.get_existing_symbols())
        if args.full
        else list(dict.fromkeys(symbol.upper() for symbol in args.tickers))
    )
    if not symbols:
        raise SystemExit("no equity bronze symbols found")
    effective_as_of = as_of_date or datetime.now(NEW_YORK).date()

    staged: list[StagedSymbol] = []
    failed = 0
    for symbol in symbols:
        try:
            rows = bronze.read_symbol_rows(symbol)
            if not rows:
                raise ValueError("missing equity bronze rows")
            actions = action_store.latest_active(symbol)
            intervals = build_factor_intervals(rows, actions, effective_as_of)
            staged.append(
                StagedSymbol(
                    symbol,
                    rows,
                    intervals,
                    actions,
                    min(_trade_date(row["trade_date"]) for row in rows),
                )
            )
        except Exception as exc:
            failed += 1
            print(f"{symbol}: {exc}", file=sys.stderr)

    current = publisher.read_current()
    current_revision = 0 if current is None else current.revision
    action_count = sum(len(item.actions) for item in staged)
    effective_action_count = sum(action.ex_date <= effective_as_of for item in staged for action in item.actions)
    future_action_count = action_count - effective_action_count
    earliest = min((item.earliest_date for item in staged), default=None)
    # Publish the successfully staged subset even when some symbols fail: a small,
    # stable set of unresolved symbols must not block the rest of the universe.
    # Exit code fails only on systemic breakage (all symbols failed, or the failure
    # rate exceeds the daily-command threshold), so persistent known-unresolved
    # symbols don't trigger a nightly alert storm.
    exit_code = resolve_exit_code(updated=len(staged), no_trade=0, partial=0, errors=failed)

    changed = [item for item in staged if not _matches_existing(client, item)]
    unchanged = len(staged) - len(changed)
    predicted_revision = current_revision + 1 if changed else current_revision
    if args.dry_run:
        _summary(
            action_count=action_count,
            as_of_date=effective_as_of.isoformat(),
            earliest_affected_date=None if earliest is None else earliest.isoformat(),
            effective_action_count=effective_action_count,
            failed=failed,
            future_action_count=future_action_count,
            rebuilt=len(changed),
            revision=predicted_revision,
            unchanged=unchanged,
        )
        return exit_code

    if not changed:
        _summary(
            action_count=action_count,
            as_of_date=effective_as_of.isoformat(),
            earliest_affected_date=None if earliest is None else earliest.isoformat(),
            effective_action_count=effective_action_count,
            failed=failed,
            future_action_count=future_action_count,
            rebuilt=0,
            revision=current_revision,
            unchanged=unchanged,
        )
        return exit_code

    with publisher.transaction() as transaction:
        changed = [item for item in staged if not _matches_existing(client, item)]
        if not changed:
            revision = 0 if transaction.current is None else transaction.current.revision
            rebuilt = 0
            unchanged = len(staged)
        else:
            artifacts = []
            affected = []
            actions_as_of = datetime.now(UTC)
            for item in changed:
                revision = transaction.revision
                daily_rows = adjust_daily_rows(item.rows, item.intervals, revision=revision)
                intervals = [replace(interval, adjustment_revision=revision) for interval in item.intervals]
                artifacts.append(client.publish_daily(item.symbol, daily_rows))
                artifacts.append(client.publish_factors(item.symbol, intervals))
                affected.append(AffectedSymbol(item.symbol, item.earliest_date, TIMEFRAMES))
                if item.actions:
                    actions_as_of = max(actions_as_of, *(action.fetched_at for action in item.actions))
            revision = transaction.commit(artifacts, affected, actions_as_of).revision
            rebuilt = len(changed)
            unchanged = len(staged) - rebuilt

    _summary(
        action_count=action_count,
        as_of_date=effective_as_of.isoformat(),
        earliest_affected_date=None if earliest is None else earliest.isoformat(),
        effective_action_count=effective_action_count,
        failed=failed,
        future_action_count=future_action_count,
        rebuilt=rebuilt,
        revision=revision,
        unchanged=unchanged,
    )
    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
