#!/usr/bin/env python3
"""Fetch EIA grid-monitor daily electricity data into bronze parquet.

Layout: bronze/asset_class=energy/product=electricity/dataset=<name>/month=<YYYY-MM>/1d.parquet

One file per dataset-month, keyed by (period, *facets). Every value is published
once per timezone day-boundary (Eastern, Central, Mountain, Pacific, Arizona), so
`timezone` is part of the key, not a duplicate. Rows are upserted by key and
never deleted: a row EIA stops serving stays, and the raw page it came from is
in source evidence. `BronzeClient` is not used: it keys on `trade_date` alone,
which would collapse ~1,400 rows per day into one.

The scheduled run (a `sync_runner` phase) re-fetches the last
`eia_electricity_lookback_days`; a backfill is the same command with `--start`.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import socket
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients import ledger
from clients.constants import declared
from clients.eia_client import EiaClient, EiaPage
from clients.parquet_io import publish_parquet, symbol_lock
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from livewire_scripts.paths import data_lake_dir

JOB = "eia-electricity"
EARLIEST = date(2019, 1, 1)  # first period of every electricity/rto daily route (probed 2026-09-23)


@dataclass(frozen=True)
class Dataset:
    route: str
    keys: tuple[str, ...]
    names: dict[str, str]  # EIA label field -> column


DATASETS = {
    "region": Dataset(
        "electricity/rto/daily-region-data",
        ("respondent", "type", "timezone"),
        {"respondent-name": "respondent_name", "type-name": "type_name"},
    ),
    "fuel_type": Dataset(
        "electricity/rto/daily-fuel-type-data",
        ("respondent", "fueltype", "timezone"),
        {"respondent-name": "respondent_name", "type-name": "fueltype_name"},
    ),
    "sub_ba": Dataset(
        "electricity/rto/daily-region-sub-ba-data",
        ("subba", "parent", "timezone"),
        {"subba-name": "subba_name", "parent-name": "parent_name"},
    ),
    "interchange": Dataset(
        "electricity/rto/daily-interchange-data",
        ("fromba", "toba", "timezone"),
        {"fromba-name": "fromba_name", "toba-name": "toba_name"},
    ),
}


def schema_for(dataset: Dataset) -> pa.Schema:
    return pa.schema(
        [("period", pa.date32())]
        + [(key, pa.string()) for key in dataset.keys]
        + [(column, pa.string()) for column in dataset.names.values()]
        + [("value", pa.float64()), ("units", pa.string()), ("source", pa.string())]
    )


def normalize(dataset: Dataset, raw: dict) -> dict:
    """One EIA row -> one bronze row. A missing key field raises: it cannot be keyed."""
    row: dict = {"period": date.fromisoformat(raw["period"])}
    for key in dataset.keys:
        if raw.get(key) in (None, ""):
            raise ValueError(f"{dataset.route}: row without {key!r}: {raw}")
        row[key] = str(raw[key])
    for field, column in dataset.names.items():
        row[column] = raw.get(field)
    # EIA sends numbers as strings on this route; null stays null, never 0.
    row["value"] = None if raw.get("value") is None else float(raw["value"])
    row["units"] = raw.get("value-units")
    row["source"] = "eia"
    return row


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Calendar months intersected with [start, end]; one window = one file."""
    windows = []
    cursor = start
    while cursor <= end:
        next_month = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
        windows.append((cursor, min(end, next_month - timedelta(days=1))))
        cursor = next_month
    return windows


def partition_path(root: Path, name: str, month_start: date) -> Path:
    return root / f"dataset={name}" / f"month={month_start:%Y-%m}" / "1d.parquet"


def upsert(path: Path, dataset: Dataset, rows: list[dict]) -> int:
    """Merge `rows` into the month file by key; returns the file's row count."""
    key_of = lambda row: (row["period"], *(row[key] for key in dataset.keys))  # noqa: E731
    with symbol_lock(path):
        merged = {key_of(row): row for row in pq.ParquetFile(path).read().to_pylist()} if path.exists() else {}
        merged.update((key_of(row), row) for row in rows)
        ordered = [merged[key] for key in sorted(merged)]
        publish_parquet(path, pa.Table.from_pylist(ordered, schema=schema_for(dataset)), ("period", *dataset.keys))
    return len(ordered)


def _evidence(store: SourceEvidenceStore, pages: list[EiaPage]) -> list[SourceEvidence]:
    records = []
    for page in pages:
        artifact = store.persist_raw(page.body_gzip, hashlib.sha256(page.body_gzip).hexdigest())
        records.append(
            SourceEvidence(
                ref=artifact.ref,
                sha256=artifact.sha256,
                source_url=page.url,
                retrieved_at=page.retrieved_at,
                publication_time=None,
                mediawiki_revision_id=None,
                mediawiki_revision_time=None,
                content_type="application/gzip",
            )
        )
    return records


def _measure(name: str, scope: str, value: float, unit: str, run_id: str) -> dict:
    return {
        "name": name,
        "scope": scope,
        "measured_at": datetime.now(UTC),
        "value": float(value),
        "unit": unit,
        "source": "measured",
        "run_id": run_id,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="livewire_ingest.py eia-electricity", description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", nargs="+", choices=sorted(DATASETS), default=list(DATASETS))
    parser.add_argument("--start", type=date.fromisoformat, help=f"First period, YYYY-MM-DD (backfill: {EARLIEST})")
    parser.add_argument("--end", type=date.fromisoformat, help="Last period, YYYY-MM-DD (default: today UTC)")
    return parser.parse_args(list(argv) if argv is not None else None)


def run(argv: Sequence[str] | None = None, *, client: EiaClient | None = None) -> int:
    args = parse_args(argv)
    today = datetime.now(UTC).date()
    end = args.end or today
    start = max(EARLIEST, args.start or end - timedelta(days=int(declared("eia_electricity_lookback_days"))))
    lake = data_lake_dir()
    root = lake / "bronze" / "asset_class=energy" / "product=electricity"

    # Under sync_runner the phase's lane_results row is the run record and
    # LW_RUN_ID is the parent's; opening a second `runs` row under that id would
    # close the parent. Standalone (a backfill), this process owns its run.
    inherited = os.environ.get("LW_RUN_ID")
    run_id = inherited or ledger.new_run_id(JOB)
    run_row = {
        "run_id": run_id,
        "job": JOB,
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": datetime.now(UTC),
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    if not inherited:
        ledger.open_run(run_row)

    store = SourceEvidenceStore(lake)
    evidence: list[SourceEvidence] = []
    measurements: list[dict] = []
    failures: list[str] = []

    def finish(exit_code: int) -> int:
        # One evidence commit per run, success or not (pm:2026-08-31-source-evidence-per-response-cost).
        store.record_many(evidence)
        if measurements:
            ledger.emit("measurements", measurements, run_id=run_id)
        if not inherited:
            verdict = "OK" if exit_code == 0 else "FAILED"
            ledger.emit(
                "runs",
                [run_row | {"ended": datetime.now(UTC), "exit_code": exit_code, "verdict": verdict}],
                run_id=run_id,
            )
        return exit_code

    try:
        eia = client or EiaClient()
        print(f"EIA electricity {start} -> {end}: {', '.join(args.dataset)} (run {run_id})", flush=True)
        for name in args.dataset:
            dataset = DATASETS[name]
            latest: date | None = None
            for window_start, window_end in month_windows(start, end):
                scope = f"electricity/{name}:{window_start:%Y-%m}"
                try:
                    raw_rows, pages = eia.fetch(
                        dataset.route,
                        frequency="daily",
                        start=window_start.isoformat(),
                        end=window_end.isoformat(),
                        sort_columns=("period", *dataset.keys),
                    )
                    evidence += _evidence(store, pages)
                    rows = [normalize(dataset, raw) for raw in raw_rows]
                    path = partition_path(root, name, window_start.replace(day=1))
                    file_rows = upsert(path, dataset, rows) if rows else 0
                except Exception as exc:
                    # One dataset-month failing never costs the others, and never
                    # exits 0. Named by subject so the operator reruns this month,
                    # not the range (pm:2026-09-16-fetch-failures-had-no-subject).
                    status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
                    failures.append(f"{scope}:{status}")
                    measurements.append(_measure("eia_fetch_failed", f"{scope}:{status}", 1, "count", run_id))
                    print(f"  {scope}: FAILED {status}: {exc}", flush=True)
                    continue
                if rows:
                    latest = max(latest or rows[0]["period"], max(row["period"] for row in rows))
                measurements.append(_measure("eia_rows_fetched", scope, len(rows), "rows", run_id))
                print(f"  {scope}: {len(rows)} rows, {len(pages)} pages, file now {file_rows}", flush=True)
            if latest is not None:
                measurements.append(
                    _measure("eia_staleness_days", f"electricity/{name}", (today - latest).days, "days", run_id)
                )
    except BaseException:
        # BaseException, not Exception: a backfill is exactly the run an operator
        # interrupts, and Ctrl-C must still close it (pm:2026-09-16-interrupted-runs-never-closed).
        finish(1)
        raise
    if failures:
        print(f"Unfetched: {', '.join(failures)}", flush=True)
    return finish(1 if failures else 0)


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
