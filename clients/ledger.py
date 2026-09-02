"""The append-only run ledger: what happened, as rows, not as log prose.

Three readers used to reconstruct "what happened last night" from one prose
artifact — status.py, the watchdog, the digest — which is why a fix in one
left the other two broken (spec 2026-09-02-ledger §0). They now read here.
Append-only: a file is NEVER rewritten. A correction is a new row; a second
emit from the same run is a new numbered file. Readers dedupe by taking the
latest row per key.

``seq`` orders rows within ONE file only. Every emit writes its own file and
restarts ``seq`` at 0, so ordering by ``seq`` across files is meaningless — an
entry row (seq 0) and its terminal row (also seq 0) are indistinguishable by
it. Order across files by ``ended nulls first``, never by ``seq``.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from clients.parquet_io import publish_parquet, symbol_lock
from livewire_scripts.paths import data_lake_dir

_TS = pa.timestamp("us", tz="UTC")

# Appended by emit, never by a caller: the row's index inside its file.
# publish_parquet validates its sort column ascending and duplicate-free, and
# no spec column is unique per row.
SEQ_COLUMN = "seq"

LEDGER_TABLES: dict[str, pa.Schema] = {
    "runs": pa.schema(
        [
            ("run_id", pa.string()),
            ("job", pa.string()),
            ("host", pa.string()),
            ("release_sha", pa.string()),
            ("presets_sha", pa.string()),
            ("registry_sha", pa.string()),
            ("started", _TS),
            ("ended", _TS),
            ("exit_code", pa.int64()),
            ("verdict", pa.string()),
            (SEQ_COLUMN, pa.int64()),
        ]
    ),
    "lane_results": pa.schema(
        [
            ("run_id", pa.string()),
            ("lane", pa.string()),
            ("started", _TS),
            ("ended", _TS),
            ("exit_code", pa.int64()),
            ("budget_s", pa.float64()),
            ("elapsed_s", pa.float64()),
            ("outcome", pa.string()),
            ("blocker", pa.string()),
            (SEQ_COLUMN, pa.int64()),
        ]
    ),
    "measurements": pa.schema(
        [
            ("name", pa.string()),
            ("scope", pa.string()),
            ("measured_at", _TS),
            ("value", pa.float64()),
            ("unit", pa.string()),
            ("source", pa.string()),
            ("run_id", pa.string()),
            (SEQ_COLUMN, pa.int64()),
        ]
    ),
    "findings": pa.schema(
        [
            ("finding_hash", pa.string()),
            ("gap_class", pa.string()),
            ("symbol", pa.string()),
            ("asset_class", pa.string()),
            ("timeframe", pa.string()),
            ("sessions", pa.list_(pa.string())),
            ("tier", pa.string()),
            ("source", pa.string()),
            ("run_id", pa.string()),
            (SEQ_COLUMN, pa.int64()),
        ]
    ),
    "evidence": pa.schema(
        [
            ("evidence_hash", pa.string()),
            ("kind", pa.string()),
            ("subject", pa.string()),
            ("payload_json", pa.string()),
            ("source_url", pa.string()),
            ("fetched_at", _TS),
            ("proposer", pa.string()),
            ("run_id", pa.string()),
            (SEQ_COLUMN, pa.int64()),
        ]
    ),
    "executions": pa.schema(
        [
            ("evidence_hash", pa.string()),
            ("script", pa.string()),
            ("attempt", pa.int64()),
            ("args_json", pa.string()),
            ("release_sha", pa.string()),
            ("started", _TS),
            ("ended", _TS),
            ("exit_code", pa.int64()),
            ("receipt_json", pa.string()),
            ("run_id", pa.string()),
            (SEQ_COLUMN, pa.int64()),
        ]
    ),
}


def ledger_root() -> Path:
    override = os.environ.get("LW_LEDGER_ROOT")
    return Path(override).expanduser() if override else data_lake_dir() / "ledger"


def new_run_id(job: str) -> str:
    """Return ``<job>-<utc-ts>-<pid>``; children read LW_RUN_ID instead."""
    return f"{job}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{os.getpid()}"


def example_row(table: str) -> dict:
    """Return a schema-complete row of defaults for tests and CLI docs."""
    now = datetime.now(UTC)
    defaults = {
        pa.string(): None,
        pa.int64(): None,
        pa.float64(): None,
        _TS: now,
        pa.list_(pa.string()): [],
    }
    return {field.name: defaults[field.type] for field in LEDGER_TABLES[table] if field.name != SEQ_COLUMN}


def _validate(table: str, rows: list[dict]) -> None:
    expected = {field.name for field in LEDGER_TABLES[table]} - {SEQ_COLUMN}
    for index, row in enumerate(rows):
        keys = set(row)
        if extra := sorted(keys - expected):
            raise ValueError(f"{table} row {index}: unexpected column(s) {extra}")
        if missing := sorted(expected - keys):
            raise ValueError(f"{table} row {index}: missing column(s) {missing}")


def _next_path(directory: Path, run_id: str) -> Path:
    """Return ``<run_id>.parquet``, then numbered paths; never rewrite."""
    candidate = directory / f"{run_id}.parquet"
    suffix = 0
    while candidate.exists():
        suffix += 1
        candidate = directory / f"{run_id}-{suffix}.parquet"
    return candidate


def emit(table: str, rows: list[dict], *, run_id: str) -> Path:
    """Publish rows as one parquet file, rejecting unknown or bad columns."""
    if table not in LEDGER_TABLES:
        raise ValueError(f"unknown ledger table {table!r}")
    if not rows:
        raise ValueError(f"{table}: refusing to emit zero rows")
    _validate(table, rows)
    numbered = [row | {SEQ_COLUMN: index} for index, row in enumerate(rows)]
    arrow = pa.Table.from_pylist(numbered, schema=LEDGER_TABLES[table])
    directory = ledger_root() / table / f"date={datetime.now(UTC):%Y-%m-%d}"
    directory.mkdir(parents=True, exist_ok=True)
    with symbol_lock(directory / f"{table}.dir"):
        return publish_parquet(_next_path(directory, run_id), arrow, sort_column=SEQ_COLUMN)
