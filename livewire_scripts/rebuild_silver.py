#!/usr/bin/env python3
"""Rebuild adjusted Silver bars and factor intervals from canonical bronze."""

from __future__ import annotations

import argparse
import hashlib
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
from clients.seed_boundary import classify_seed_boundary
from clients.silver_client import PublishedArtifact, SilverClient
from clients.silver_revision import AffectedSymbol, ManifestArtifact, SilverRevision, SilverRevisionPublisher
from clients.silver_window import resolve_window
from livewire_scripts.daily_outcomes import resolve_exit_code
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
    rows: list[dict]
    intervals: list[FactorInterval]
    actions: list[CorporateAction]
    earliest_date: date
    window: dict


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
                symbol = str(verdict["symbol"]).upper()
                keep_by_symbol[symbol] = keep_by_symbol.get(symbol, frozenset()) | {str(verdict["date"])}
    elif explicit is not None:
        # An explicitly-named manifest that does not exist is an operator error, not
        # "no verdicts" — silently trimming every real move is the failure we are
        # trying to prevent.
        raise SystemExit(f"triage manifest not found: {triage_path}")
    return keep_by_symbol


def _carry_forward(
    client: SilverClient,
    current: SilverRevision | None,
    staged: list[StagedSymbol],
    changed: list[StagedSymbol],
    scope: set[str],
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
    republished = {item.symbol for item in changed}
    previous_affected = {item.symbol: item for item in current.affected}
    by_symbol: dict[str, list[ManifestArtifact]] = {}
    for artifact in current.artifacts:
        if "symbol=" not in artifact.path:
            continue
        by_symbol.setdefault(artifact.path.split("symbol=")[1].split("/")[0], []).append(artifact)

    artifacts: list[PublishedArtifact] = []
    affected: list[AffectedSymbol] = []
    for symbol, entries in sorted(by_symbol.items()):
        if symbol in republished:
            continue
        if symbol in scope and symbol not in staged_ok:
            continue
        previous = previous_affected.get(symbol)
        if previous is None:
            continue
        resolved: list[PublishedArtifact] = []
        for artifact in entries:
            path = client.root / artifact.path
            if not path.is_file():
                resolved = []
                break
            # row_count is not serialized into the manifest but PublishedArtifact
            # requires it — read the footer rather than inventing a number.
            resolved.append(PublishedArtifact(path, artifact.sha256, pq.ParquetFile(path).metadata.num_rows))
        if not resolved:
            continue  # a vanished artifact must not be manifested
        artifacts.extend(resolved)
        affected.append(previous)  # one per symbol: _validate_affected rejects dupes
    return artifacts, affected


def _evict_unmanifested(
    client: SilverClient,
    current: SilverRevision | None,
    manifested: list[PublishedArtifact],
    scope: set[str],
    revision: int,
) -> list[str]:
    """Move a quarantined symbol's daily artifact out of the served tree.

    Apex resolves a symbol by constructing <root>/asset_class=equity/symbol=<S>/1d.parquet
    and reading whatever is there (apex ohlc_provider.py:141-145); the manifest is a
    reseed signal, not a view definition, and its per-symbol revision maps are
    write-only with no eviction path. So dropping a symbol from the manifest leaves it
    serving its stale corrupt artifact forever — moving the file is the only removal
    signal apex can perceive, after which it fails closed with AdjustedDataUnavailable.

    Moved, not unlinked, so an eviction is reversible. The FACTOR artifact stays: apex
    joins bronze intraday onto factors independently of the daily file, and a missing
    factor file is its own 500.
    """
    if current is None:
        return []
    kept = {artifact.path.resolve() for artifact in manifested}
    evicted: list[str] = []
    for artifact in current.artifacts:
        if "symbol=" not in artifact.path or not artifact.path.endswith("1d.parquet"):
            continue
        symbol = artifact.path.split("symbol=")[1].split("/")[0]
        if symbol not in scope:
            continue
        path = client.root / artifact.path
        if not path.is_file() or path.resolve() in kept:
            continue
        destination = client.root / "evicted" / str(revision) / artifact.path
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(path, destination)
        evicted.append(symbol)
    return sorted(evicted)


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
) -> dict:
    dates = sorted(_trade_date(row["trade_date"]) for row in rows)
    path = bronze.symbol_path(symbol).resolve()
    return {
        "symbol": symbol,
        "error_type": type(exc).__name__,
        "error": str(exc),
        "bronze_path": str(path),
        "source_sha256": _sha256(path),
        "earliest_trade_date": dates[0].isoformat() if dates else None,
        "latest_trade_date": dates[-1].isoformat() if dates else None,
        "active_actions": [_action_identity(action) for action in actions],
    }


def _write_json_atomic(path: Path, payload: dict) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


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
    threshold = args.continuity_threshold
    keep_by_symbol = _load_keep_dates(root, args.triage_manifest)

    staged: list[StagedSymbol] = []
    failures: list[dict] = []
    for symbol in symbols:
        rows: list[dict] = []
        actions: list[CorporateAction] = []
        try:
            rows = bronze.read_symbol_rows(symbol)
            if not rows:
                raise ValueError("missing equity bronze rows")
            actions = action_store.latest_active(symbol)
            # Trim 1 — the seed floor, applied to RAW bronze before adjustment. The
            # only detector that sees the 2x-5x class; a corrupt symbol's pre-window
            # rows are IB back-adjusted, its rows on/after the window are true raw.
            # Trim rather than quarantine: the post-seed years are perfectly good.
            seed = classify_seed_boundary(rows, actions)
            if seed["verdict"] == "corrupt":
                rows = [row for row in rows if str(row["trade_date"])[:10] >= seed["date"]]
            intervals = build_factor_intervals(rows, actions, effective_as_of)
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
            # NOTE: `intervals` stay built over the FULL pre-trim `rows`. Do NOT rebuild
            # them over `kept` to "make the factor file match the daily file" — that is
            # a correctness trap. Apex's adjusted-intraday path LEFT JOINs BRONZE
            # intraday bars onto these factor intervals and hard-fails when any bronze
            # bar has no interval (apex `ohlc_provider.py:236-240`,
            # "incomplete or overlapping factor coverage" -> HTTP 500). Bronze intraday
            # extends before the trimmed daily window, so narrowing the factors to the
            # daily window breaks intraday for exactly the symbols we just trimmed.
            # Factors WIDER than the daily rows are harmless; narrower is fatal.
            staged.append(
                StagedSymbol(
                    symbol,
                    kept,
                    intervals,
                    actions,
                    min(_trade_date(row["trade_date"]) for row in kept),
                    window,
                )
            )
        except Exception as exc:
            failures.append(_failure(symbol, exc, bronze, rows, actions))
            print(f"{symbol}: {exc}", file=sys.stderr)

    current = publisher.read_current()
    current_revision = 0 if current is None else current.revision

    # A window that moved LATER than what we already serve means new data cost us
    # published history. resolve_window cannot tell which side of a one-sided
    # boundary is trustworthy — it always trusts the newer one — so a corrupt bar
    # arriving tonight would become the last break and collapse the window onto
    # itself. Only this comparison against the published revision can catch that.
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
    # Fail closed: withhold the regressed symbols from republication. They stay in
    # `staged`, so _carry_forward re-lists their previous artifacts and they keep
    # serving the older, longer, still-valid window.
    regressed = set() if args.allow_window_regression else {item["symbol"] for item in regressions}
    publishable = [item for item in staged if item.symbol not in regressed]
    evicted: list[str] = []  # only a real publish can evict; a dry run never moves a file
    # A symbol that failed staging but still has a published artifact must force a
    # publish even when nothing else changed — otherwise the run early-returns and
    # apex keeps serving the stale file until some unrelated symbol happens to move.
    staged_ok = {item.symbol for item in staged}
    quarantined = sorted(
        symbol
        for symbol in {s.upper() for s in symbols}
        if symbol not in staged_ok and client.daily_path(symbol).is_file()
    )

    if args.failure_output is not None:
        _write_json_atomic(
            args.failure_output,
            {
                "schema_version": 2,
                "generated_at": datetime.now(UTC).isoformat(),
                "data_lake_root": str(root.expanduser().resolve()),
                "silver_root": str(silver_path.expanduser().resolve()),
                "as_of_date": effective_as_of.isoformat(),
                "failures": sorted(failures, key=lambda item: item["symbol"]),
                # Alongside `failures`, not inside it: a regressed symbol still
                # publishes (its previous window), so it must not inflate `failed`
                # or reach resolve_exit_code. It is an alert, not a job failure.
                "window_regressions": sorted(regressions, key=lambda item: item["symbol"]),
            },
        )

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

    changed = [item for item in publishable if not _matches_existing(client, item)]
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
            trimmed=trimmed,
            window_regressions=len(regressions),
            evicted=len(evicted),
        )
        return exit_code

    if not changed and not quarantined:
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
            trimmed=trimmed,
            window_regressions=len(regressions),
            evicted=len(evicted),
        )
        return exit_code

    with publisher.transaction() as transaction:
        changed = [item for item in publishable if not _matches_existing(client, item)]
        if not changed and not quarantined:
            revision = 0 if transaction.current is None else transaction.current.revision
            rebuilt = 0
            unchanged = len(staged)
        else:
            revision = transaction.revision
            artifacts = []
            affected = []
            actions_as_of = datetime.now(UTC)
            for item in changed:
                daily_rows = adjust_daily_rows(item.rows, item.intervals, revision=revision)
                intervals = [replace(interval, adjustment_revision=revision) for interval in item.intervals]
                artifacts.append(client.publish_daily(item.symbol, daily_rows))
                artifacts.append(client.publish_factors(item.symbol, intervals))
                affected.append(AffectedSymbol(item.symbol, item.earliest_date, TIMEFRAMES))
                if item.actions:
                    actions_as_of = max(actions_as_of, *(action.fetched_at for action in item.actions))
            # The publisher writes exactly what it is handed and never merges the
            # previous revision, so a targeted rebuild would manifest only its own
            # symbols and drop the rest of the universe.
            carried_artifacts, carried_affected = _carry_forward(
                client, transaction.current, staged, changed, {s.upper() for s in symbols}
            )
            artifacts.extend(carried_artifacts)
            affected.extend(carried_affected)
            if not artifacts:
                # Every in-scope symbol is quarantined. The publisher rejects an empty
                # revision, and evicting against a manifest that still names the file
                # would fail apex's sha256 check and reject the WHOLE revision.
                raise SystemExit("every in-scope symbol failed staging: refusing to publish an empty revision")
            revision = transaction.commit(artifacts, affected, actions_as_of).revision
            # Evict only AFTER the manifest that omits them is committed. Apex verifies
            # every manifested artifact's sha256 on every 30s poll and rejects the whole
            # revision atomically on a mismatch, so a moved file still named by the
            # current manifest would take the entire service down, not just one symbol.
            evicted = _evict_unmanifested(
                client, transaction.current, artifacts, {s.upper() for s in symbols}, revision
            )
            for symbol in evicted:
                print(f"{symbol}: evicted — quarantined, artifact moved out of the served tree", file=sys.stderr)
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
        evicted=len(evicted),
    )
    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
