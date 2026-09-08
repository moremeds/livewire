#!/usr/bin/env python3
"""Rebuild adjusted Silver bars and factor intervals from canonical bronze."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import shutil
import sys
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from clients import constants, ledger
from clients.adjustment_engine import FactorInterval, adjust_daily_rows, build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateAction, CorporateActionStore
from clients.parquet_io import PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL, silver_input_lock, write_json_atomic
from clients.seed_boundary import classify_seed_boundary
from clients.silver_client import PublishedArtifact, SilverClient
from clients.silver_revision import AffectedSymbol, ManifestArtifact, SilverRevision, SilverRevisionPublisher
from clients.silver_window import resolve_window
from clients.symbol_paths import canonical_symbol, encode_symbol
from livewire_scripts.daily_outcomes import SUMMARY_PREFIX, resolve_exit_code
from livewire_scripts.job_runner_common import emit_progress
from livewire_scripts.paths import data_lake_dir

TIMEFRAMES = ("1d", "1m", "5m", "30m", "1h")
NEW_YORK = ZoneInfo("America/New_York")
CONTINUITY_THRESHOLD = 6.0
# Resolved against the data-lake root. The nightly job passes no flags
# (run_daily_update_job.py:129), so the verdicts must be found, not passed.
DEFAULT_TRIAGE_MANIFEST = "repairs/triage/current.json"


@dataclass(frozen=True)
class StagedSymbol:
    symbol: str
    rows_path: Path
    intervals: list[FactorInterval]
    actions: list[CorporateAction]
    earliest_date: date
    window: dict

    @property
    def rows(self) -> list[dict]:
        return pq.ParquetFile(self.rows_path).read().to_pylist()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+", help="Explicit equity symbols")
    scope.add_argument("--full", action="store_true", help="Discover all equity bronze symbols")
    parser.add_argument("--dry-run", action="store_true", help="Compute and compare without publishing")
    parser.add_argument(
        "--failure-output",
        type=Path,
        help="Write evidence-grade per-symbol staging failures as JSON",
    )
    parser.add_argument(
        "--continuity-threshold",
        type=float,
        default=CONTINUITY_THRESHOLD,
        help="max adjacent-day adjusted close ratio before a symbol is quarantined",
    )
    parser.add_argument(
        "--continuity-allowlist",
        nargs="*",
        default=[],
        metavar="ISO_DATE",
        help="iso dates exempt from the continuity gate (evidence-backed halts/relistings)",
    )
    parser.add_argument(
        "--triage-manifest",
        type=Path,
        help=(
            f"break-triage verdicts; real_move dates are kept rather than trimmed "
            f"(default: <data-lake-root>/{DEFAULT_TRIAGE_MANIFEST} when present)"
        ),
    )
    parser.add_argument(
        "--allow-window-regression",
        action="store_true",
        help="publish symbols whose window start moved later (required once, for the rev-3 bootstrap)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


# Heartbeat cadence, in symbols. Matches the corporate-actions lane: a --full run
# walks ~13k symbols for hours and prints nothing an operator can count.
_PROGRESS_EVERY = 500


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


def _load_keep_dates(root: Path, explicit: Path | None) -> dict[str, frozenset[str]]:
    """Triage-confirmed real_move dates, per symbol.

    Read from the default path when no flag is given: the nightly job passes none
    (run_daily_update_job.py:129), and without the verdicts every confirmed real move
    is re-read as an unexplained break and its history trimmed away the next night.
    """
    triage_path = explicit or (root / DEFAULT_TRIAGE_MANIFEST)
    keep_by_symbol: dict[str, frozenset[str]] = {}
    if triage_path.is_file():
        payload = json.loads(triage_path.read_text())
        for verdict in payload.get("verdicts", []):
            if verdict.get("verdict") == "real_move":
                symbol = canonical_symbol(verdict["symbol"])
                keep_by_symbol[symbol] = keep_by_symbol.get(symbol, frozenset()) | {str(verdict["date"])}
    elif explicit is not None:
        # An explicitly-named manifest that does not exist is an operator error, not
        # "no verdicts" — silently trimming every real move is the failure we are
        # trying to prevent.
        raise SystemExit(f"triage manifest not found: {triage_path}")
    return keep_by_symbol


def _copy_snapshot_file(source: Path, destination: Path) -> None:
    """Copy one canonical input while its shared input boundary is exclusive."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as reader, destination.open("xb") as writer:
        shutil.copyfileobj(reader, writer, length=1024 * 1024)


def _require_snapshot_capacity(paths: list[Path], scratch_root: Path) -> None:
    """Refuse a whole-snapshot attempt that would consume the existing disk reserve."""
    source_bytes = sum(path.stat().st_size for path in paths if path.is_file())
    reserve_bytes = int(constants.declared("flatfile_min_free_gb") * 1024**3)
    free_bytes = shutil.disk_usage(scratch_root).free
    if free_bytes - source_bytes < reserve_bytes:
        raise RuntimeError(
            "insufficient scratch capacity for Silver input snapshot: "
            f"source={source_bytes} free={free_bytes} reserve={reserve_bytes}"
        )


def _stage_rows(path: Path, rows: list[dict]) -> None:
    """Persist a symbol's kept daily rows so full runs never retain all rows in RAM."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows),
        path,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )
    # Decode before retaining the path: a staging write failure is a symbol failure,
    # never a claim that the copied snapshot was usable.
    pq.ParquetFile(path).read()


def _artifact_index(current: SilverRevision | None) -> dict[str, list[ManifestArtifact]]:
    """Index one committed manifest by its canonical symbol partition."""
    if current is None:
        return {}
    symbols = {encode_symbol(item.symbol): item.symbol for item in current.affected}
    indexed = {symbol: [] for symbol in symbols.values()}
    for artifact in current.artifacts:
        partition = next((part[7:] for part in Path(artifact.path).parts if part.startswith("symbol=")), None)
        symbol = symbols.get(partition)
        if symbol is not None:
            indexed[symbol].append(artifact)
    return indexed


def _copy_verified_legacy_artifact(
    source: Path,
    artifact: ManifestArtifact,
    generation_client: SilverClient,
) -> PublishedArtifact:
    """Copy an old fixed-path artifact into this attempt without changing its bytes."""
    expected = artifact.sha256
    if _sha256(source) != expected:
        raise ValueError(f"committed Silver artifact checksum mismatch: {artifact.path}")
    # A checksum proves identity; a full decode proves this legacy source remains a
    # usable Parquet artifact before it enters a new immutable snapshot.
    pq.ParquetFile(source).read()
    destination = generation_client.output_root / artifact.path
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as reader, destination.open("xb") as writer:
        while chunk := reader.read(1024 * 1024):
            writer.write(chunk)
    if _sha256(destination) != expected:
        raise ValueError(f"legacy Silver artifact changed while copying: {artifact.path}")
    return PublishedArtifact(destination, expected, 0)


def _carry_forward(
    client: SilverClient,
    current: SilverRevision | None,
    staged: list[StagedSymbol],
    replaced: set[str],
    scope: set[str],
    artifact_index: dict[str, list[ManifestArtifact]],
    generation_client: SilverClient,
) -> tuple[list[PublishedArtifact], list[AffectedSymbol]]:
    """Re-list still-valid symbols this run did not republish.

    Carried: symbols outside ``scope`` (a targeted rebuild must not evict the
    universe) and in-scope symbols that staged cleanly but were byte-identical to
    what is published. NOT carried: symbols republished here (already added), and
    in-scope symbols that failed staging — dropping them is the quarantine.
    """
    if current is None:
        return [], []
    staged_ok = {item.symbol for item in staged}
    previous_affected = {item.symbol: item for item in current.affected}
    artifacts: list[PublishedArtifact] = []
    affected: list[AffectedSymbol] = []
    for symbol, previous in sorted(previous_affected.items()):
        if symbol in replaced:
            continue
        if symbol in scope and symbol not in staged_ok:
            continue
        entries = artifact_index.get(symbol, [])
        resolved: list[PublishedArtifact] = []
        for artifact in entries:
            path = client.root / artifact.path
            if artifact.path.startswith("generations/"):
                if _sha256(path) != artifact.sha256:
                    raise ValueError(f"committed Silver artifact checksum mismatch: {artifact.path}")
                resolved.append(PublishedArtifact(path, artifact.sha256, 0))
            else:
                resolved.append(_copy_verified_legacy_artifact(path, artifact, generation_client))
        if len(resolved) != 2:
            raise ValueError(f"committed Silver symbol requires daily and factor artifacts: {symbol}")
        artifacts.extend(resolved)
        affected.append(previous)
    return artifacts, affected


def _matches_existing(
    client: SilverClient,
    staged: StagedSymbol,
    current: SilverRevision | None,
    artifact_index: dict[str, list[ManifestArtifact]],
) -> bool:
    if current is None:
        return False
    entries = artifact_index.get(staged.symbol, [])
    if len(entries) != 2 or any(not artifact.path.startswith("generations/") for artifact in entries):
        return False
    daily_entry = next((artifact for artifact in entries if artifact.path.endswith("/1d.parquet")), None)
    factor_entry = next((artifact for artifact in entries if artifact.path.endswith("/factors.parquet")), None)
    if daily_entry is None or factor_entry is None:
        return False
    daily_path = client.root / daily_entry.path
    factor_path = client.root / factor_entry.path
    if _sha256(daily_path) != daily_entry.sha256 or _sha256(factor_path) != factor_entry.sha256:
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
    staged_rows = staged.rows
    candidate_daily = adjust_daily_rows(staged_rows, staged.intervals, revision=1)
    return _daily_semantics(daily_rows) == _daily_semantics(candidate_daily) and _factor_semantics(
        existing_intervals
    ) == _factor_semantics(staged.intervals)


def _summary(**values) -> None:
    """Emit the machine-readable run summary on the shared SUMMARY_JSON contract.

    This line used to print as bare JSON. `parse_all_summary_json` skips every
    line without the prefix, so the ledger measurement writer never found it
    and rendered "(not found)" on nights the rebuild had in fact succeeded —
    taking the `window_regressions` warning with it. That warning is the ONLY
    alert for a symbol whose window shrank (the run still exits 0), so the
    missing prefix silently disabled it: rev-19 withheld 41 symbols and the
    2026-08-01 digest reported no Silver rebuild at all.
    """
    print(SUMMARY_PREFIX + json.dumps(values, sort_keys=True))


def _sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _action_identity(action: CorporateAction) -> dict[str, str]:
    return {
        "action_id": action.action_id,
        "action_type": action.action_type,
        "ex_date": action.ex_date.isoformat(),
        "status": action.status,
    }


def _failure(
    symbol: str,
    exc: Exception,
    bronze: BronzeClient,
    rows: list[dict],
    actions: list[CorporateAction],
    snapshot_path: Path | None,
) -> dict:
    dates = []
    for row in rows:
        try:
            dates.append(_trade_date(row["trade_date"]))
        except (KeyError, TypeError, ValueError):
            pass  # A malformed date must not prevent reporting the original failure.
    dates.sort()
    path = bronze.symbol_path(symbol).resolve()
    try:
        checksum = _sha256(snapshot_path) if snapshot_path is not None else None
    except OSError:
        checksum = None
    return {
        "symbol": symbol,
        "error_type": type(exc).__name__,
        "error": str(exc),
        "bronze_path": str(path),
        "source_sha256": checksum,
        "earliest_trade_date": dates[0].isoformat() if dates else None,
        "latest_trade_date": dates[-1].isoformat() if dates else None,
        "active_actions": [_action_identity(action) for action in actions],
    }


def _run_snapshot(
    args: argparse.Namespace,
    *,
    root: Path,
    silver_path: Path,
    as_of_date: date | None,
    scratch_root: Path,
) -> int:
    bronze = BronzeClient(root / "bronze" / "asset_class=equity", "equity")
    action_store = CorporateActionStore(root)
    client = SilverClient(silver_path)
    publisher = SilverRevisionPublisher(silver_path)
    baseline = publisher.read_current()
    baseline_revision = 0 if baseline is None else baseline.revision
    input_errors: dict[str, Exception] = {}
    snapshot_root = scratch_root / "inputs"
    snapshot_bronze = BronzeClient(snapshot_root / "bronze" / "asset_class=equity", "equity")
    snapshot_actions = CorporateActionStore(snapshot_root)
    # Freeze the canonical files only while discovering scope and copying bytes into
    # owned scratch. Decode, adjustment and output happen after the exclusive lock.
    with silver_input_lock(root / "bronze"):
        symbols = (
            sorted(bronze.get_existing_symbols())
            if args.full
            else list(dict.fromkeys(canonical_symbol(symbol) for symbol in args.tickers))
        )
        if not symbols:
            raise SystemExit("no equity bronze symbols found")
        source_paths = [bronze.symbol_path(symbol) for symbol in symbols]
        source_paths.extend(action_store.path_for(symbol) for symbol in symbols)
        _require_snapshot_capacity(source_paths, scratch_root)
        for symbol in symbols:
            try:
                _copy_snapshot_file(bronze.symbol_path(symbol), snapshot_bronze.symbol_path(symbol))
                action_source = action_store.path_for(symbol)
                if action_source.is_file():
                    _copy_snapshot_file(action_source, snapshot_actions.path_for(symbol))
            except OSError as exc:
                # The preflight is necessarily only a point-in-time check.  If a
                # competing process consumes the reserve while we copy, a partial
                # snapshot must never be treated like a symbol-local source error:
                # that would publish a revision from an incompletely frozen input
                # set.  Scratch loss has the same all-or-nothing outcome.
                if exc.errno == errno.ENOSPC or not scratch_root.is_dir():
                    raise RuntimeError(f"Silver scratch became unavailable while copying {symbol}") from exc
                input_errors[symbol] = exc
            except Exception as exc:
                input_errors[symbol] = exc
        input_actions_as_of = datetime.now(UTC)
    effective_as_of = as_of_date or datetime.now(NEW_YORK).date()
    threshold = args.continuity_threshold
    keep_by_symbol = _load_keep_dates(root, args.triage_manifest)

    staged: list[StagedSymbol] = []
    failures: list[dict] = []
    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("silver")
    for position, symbol in enumerate(symbols, start=1):
        rows: list[dict] = []
        actions: list[CorporateAction] = []
        writing_stage = False
        snapshot_path = None if symbol in input_errors else snapshot_bronze.symbol_path(symbol)
        try:
            if symbol in input_errors:
                raise input_errors[symbol]
            rows = snapshot_bronze.read_symbol_rows(symbol)
            if not rows:
                raise ValueError("missing equity bronze rows")
            actions = snapshot_actions.latest_active(symbol)
            # Trim 1 — the seed floor, applied to RAW bronze before adjustment. The
            # only detector that sees the 2x-5x class; a corrupt symbol's pre-window
            # rows are IB back-adjusted, its rows on/after the window are true raw.
            # Trim rather than quarantine: the post-seed years are perfectly good.
            seed = classify_seed_boundary(rows, actions)
            # Factors FIRST, over the untrimmed raw range — see the note on `intervals`
            # below. build_factor_intervals bounds its intervals by the first supplied
            # bar, and every seed-corrupt symbol's bronze intraday starts ~2021-06-03,
            # BEFORE its 2021-06-11 floor. Building after the trim would leave those
            # bars uncovered, which is the exact hard-fail that note warns about. The
            # pre-seed rows' prices are corrupt but their factors are not: the interval
            # covering the pre-floor gap is the product of actions with a later ex_date,
            # all of which fall after the seed window where closes are genuine raw.
            intervals = build_factor_intervals(rows, actions, effective_as_of)
            if seed["verdict"] == "corrupt":
                rows = [row for row in rows if str(row["trade_date"])[:10] >= seed["date"]]
            adjusted = adjust_daily_rows(rows, intervals, revision=1)
            # Trim 2 — the blind window scan over the ADJUSTED series, for every other
            # unexplained break. Keep triage-confirmed real moves and allowlisted dates.
            window = resolve_window(
                adjusted,
                threshold=threshold,
                allowlist=frozenset(args.continuity_allowlist),
                keep_dates=keep_by_symbol.get(symbol, frozenset()),
            )
            if window["start"] is None:
                # No suffix excludes the offending row — it is the newest one. Fail
                # closed rather than publishing a series that starts on a bad bar.
                raise ValueError(f"no silver-grade window: {window['reason']}")
            kept = [row for row in rows if str(row["trade_date"])[:10] >= window["start"]]
            # NOTE: `intervals` stay built over the FULL pre-trim raw range (both trims).
            # Do NOT rebuild them over `kept` to "make the factor file match" — that is
            # a correctness trap: raw intraday can precede the trimmed daily window,
            # and every adjusted bar still requires exactly one factor interval.
            rows_path = scratch_root / "staged" / f"{encode_symbol(symbol)}.parquet"
            writing_stage = True
            _stage_rows(rows_path, kept)
            writing_stage = False
            staged.append(
                StagedSymbol(
                    symbol,
                    rows_path,
                    intervals,
                    actions,
                    min(_trade_date(row["trade_date"]) for row in kept),
                    window,
                )
            )
        except OSError as exc:
            if writing_stage and (exc.errno == errno.ENOSPC or not scratch_root.is_dir()):
                raise RuntimeError(f"Silver scratch became unavailable while staging {symbol}") from exc
            failures.append(_failure(symbol, exc, bronze, rows, actions, snapshot_path))
            print(f"{symbol}: {exc}", file=sys.stderr)
        except Exception as exc:
            failures.append(_failure(symbol, exc, bronze, rows, actions, snapshot_path))
            print(f"{symbol}: {exc}", file=sys.stderr)
        if position % _PROGRESS_EVERY == 0:
            emit_progress(scope="silver", completed=position, total=len(symbols), run_id=run_id)
    # The loop is the hours-long part; a final beat so the last partial batch is
    # counted and `status` reads N of N rather than the previous multiple of 500.
    emit_progress(scope="silver", completed=len(symbols), total=len(symbols), run_id=run_id)

    action_count = sum(len(item.actions) for item in staged)
    effective_action_count = sum(action.ex_date <= effective_as_of for item in staged for action in item.actions)
    future_action_count = action_count - effective_action_count
    earliest = min((item.earliest_date for item in staged), default=None)
    trimmed = sum(1 for item in staged if item.window["trimmed_at"])
    # Publish the successfully staged subset even when some symbols fail: a small,
    # stable set of unresolved symbols must not block the rest of the universe.
    # Exit code fails only on systemic breakage (all symbols failed, or the failure
    # rate exceeds the daily-command threshold), so persistent known-unresolved
    # symbols don't trigger a nightly alert storm.
    failed = len(failures)
    exit_code = resolve_exit_code(updated=len(staged), no_trade=0, partial=0, errors=failed)

    def publication_state(current: SilverRevision | None):
        artifact_index = _artifact_index(current)
        previous_start = {item.symbol: item.earliest_date for item in (current.affected if current else ())}
        regressions = [
            {
                "symbol": item.symbol,
                "previous_start": previous_start[item.symbol].isoformat(),
                "new_start": item.window["start"],
                "reason": item.window["reason"],
            }
            for item in staged
            if item.symbol in previous_start and item.window["start"] > previous_start[item.symbol].isoformat()
        ]
        regressed = set() if args.allow_window_regression else {item["symbol"] for item in regressions}
        publishable = [item for item in staged if item.symbol not in regressed]
        changed = [item for item in publishable if not _matches_existing(client, item, current, artifact_index)]
        current_symbols = {item.symbol for item in (current.affected if current else ())}
        scope = {canonical_symbol(symbol) for symbol in symbols}
        if args.full:
            # Symbols removed from canonical Bronze are part of a full rebuild's scope
            # and must disappear from the next manifest without moving historical bytes.
            scope |= current_symbols
        omitted = (scope & current_symbols) - {item.symbol for item in staged}
        return regressions, publishable, changed, scope, omitted, artifact_index

    def write_failure_output(regressions: list[dict]) -> None:
        if args.failure_output is not None:
            write_json_atomic(
                args.failure_output,
                {
                    "schema_version": 2,
                    "generated_at": datetime.now(UTC).isoformat(),
                    "data_lake_root": str(root.expanduser().resolve()),
                    "silver_root": str(silver_path.expanduser().resolve()),
                    "as_of_date": effective_as_of.isoformat(),
                    "failures": sorted(failures, key=lambda item: item["symbol"]),
                    "window_regressions": sorted(regressions, key=lambda item: item["symbol"]),
                },
            )

    if args.dry_run:
        current = publisher.read_current()
        current_revision = 0 if current is None else current.revision
        regressions, _, changed, _, omitted, _ = publication_state(current)
        write_failure_output(regressions)
        unchanged = len(staged) - len(changed)
        predicted_revision = current_revision + 1 if (changed or omitted) else current_revision
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
            trimmed=trimmed,
            window_regressions=len(regressions),
            evicted=0,
            orphans_remanifested=0,
        )
        return exit_code

    with publisher.transaction() as transaction:
        current_revision = 0 if transaction.current is None else transaction.current.revision
        if current_revision != baseline_revision:
            raise RuntimeError(
                f"Silver advanced from revision {baseline_revision} to {current_revision} while inputs were processed; "
                "this stale attempt published nothing; retry from fresh inputs"
            )
        regressions, publishable, changed, scope, omitted, artifact_index = publication_state(transaction.current)
        write_failure_output(regressions)
        if not changed and not omitted:
            revision = 0 if transaction.current is None else transaction.current.revision
            rebuilt = 0
            unchanged = len(staged)
        else:
            revision = transaction.revision
            attempt_id = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%S.%fZ')}-{uuid.uuid4().hex}"
            generation_client = client.for_generation(attempt_id)
            artifacts: list[PublishedArtifact] = []
            affected: list[AffectedSymbol] = []
            actions_as_of = input_actions_as_of
            for item in changed:
                daily_rows = adjust_daily_rows(item.rows, item.intervals, revision=revision)
                intervals = [replace(interval, adjustment_revision=revision) for interval in item.intervals]
                artifacts.append(generation_client.publish_daily(item.symbol, daily_rows))
                artifacts.append(generation_client.publish_factors(item.symbol, intervals))
                affected.append(AffectedSymbol(item.symbol, item.earliest_date, TIMEFRAMES))
                if item.actions:
                    actions_as_of = max(actions_as_of, *(action.fetched_at for action in item.actions))
            # The publisher writes exactly what it is handed and never merges the
            # previous revision, so a targeted rebuild would manifest only its own
            # symbols and drop the rest of the universe.
            carried_artifacts, carried_affected = _carry_forward(
                client,
                transaction.current,
                staged,
                {item.symbol for item in changed},
                scope,
                artifact_index,
                generation_client,
            )
            artifacts.extend(carried_artifacts)
            affected.extend(carried_affected)
            if not artifacts:
                # Every in-scope symbol is quarantined. Keep the prior commit intact;
                # schema 1 deliberately has no representation for an empty revision.
                raise SystemExit("every in-scope symbol failed staging: refusing to publish an empty revision")
            revision = transaction.commit(
                artifacts,
                affected,
                actions_as_of,
                generation_id=attempt_id,
            ).revision
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
        trimmed=trimmed,
        window_regressions=len(regressions),
        evicted=0,
        orphans_remanifested=0,
    )
    return exit_code


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
    # The default temporary directory is deliberately outside canonical Bronze/Silver.
    # Its lifetime owns every input and staged artifact, including dry runs and failures.
    with tempfile.TemporaryDirectory(prefix="livewire-silver-") as scratch:
        return _run_snapshot(
            args, root=root, silver_path=silver_path, as_of_date=as_of_date, scratch_root=Path(scratch)
        )


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
