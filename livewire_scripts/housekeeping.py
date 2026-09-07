#!/usr/bin/env python3
"""Retention sweeps for warehouse artifacts that nothing else prunes.

Deliberately narrow. Four categories are unrecoverable and are protected by
name, never by a size or age rule:

  data-lake/raw/       anything older than the rolling 5-year provider GET floor
                       can never be refetched. LIST advertises 2003; GET 403s.
  repairs/triage/      a triage verdict obtainable today may be unobtainable next
                       year, because the entitlement floor rolls forward.
  repairs/*/backup/    the only basis rollback-legacy-basis has.
  the release `current` points at — promote short-circuits on the symlink, not
                       the directory, so deleting the target leaves current
                       dangling and promote then refuses to rebuild it.

data-lake/repairs/ as a whole is out of scope. It is 26 GB, 21 GB of which is
12,636 verbatim .parquet.bak files from the 2026-07-15 cutover — rollback
material whose disposal is an operator decision, not a retention rule. It also
lives on the volume with 6.6 TiB free, so nothing forces the call.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from livewire_scripts.paths import data_lake_dir, log_dir

# Bound at module level so a test can replace it. A late `from … import prune`
# inside main() is unpatchable, which would mean every main() test ran the real
# pruner against the operator's real warehouse.
from livewire_scripts.release import prune as prune_releases

log = logging.getLogger(__name__)

#: Lake subtrees no retention rule may ever enter. Matched on the path's parts,
#: so `repairs/triage/anything/deeper` is protected too. The ledger is the run
#: record every reader grades against, so pruning it would erase a night.
PROTECTED_LAKE_DIRS = frozenset({"raw", "repairs", "ledger"})

LOG_RETENTION_DAYS = 60
KEEP_RELEASES = 3
KEEP_EVICTED = 2


def _is_protected(path: Path, lake: Path) -> bool:
    try:
        relative = path.relative_to(lake)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] in PROTECTED_LAKE_DIRS


def plan_housekeeping(
    log_dir_path: Path,
    data_lake: Path,
    *,
    log_retention_days: int = LOG_RETENTION_DAYS,
    keep_evicted: int = KEEP_EVICTED,
    now: date | None = None,
) -> list[tuple[str, Path]]:
    """Return (reason, path) pairs this run would delete. Never mutates.

    No `keep_releases` here on purpose: releases are pruned by `release.prune()`
    in main(), which alone knows not to collect what `current` points at. A
    parameter this function never reads would be a promise it does not keep.
    """
    now = now or datetime.now().date()
    planned: list[tuple[str, Path]] = []

    # Logs. 395 files back to 2026-06 with no rotation at the time of writing.
    cutoff = now.toordinal() - log_retention_days
    for path in sorted(log_dir_path.glob("*.log")):
        try:
            # is_file, not just glob: a DIRECTORY named `*.log` would otherwise
            # be planned and main() would rmtree the whole tree. This prunes
            # log files; anything else is out of scope by construction.
            if not path.is_file():
                continue
            modified = date.fromtimestamp(path.stat().st_mtime)
        except OSError:
            continue
        if modified.toordinal() < cutoff:
            planned.append((f"log older than {log_retention_days}d", path))

    # Evicted silver revisions, keeping the newest `keep_evicted` by revision
    # number. Sorted numerically: lexical order puts "10" before "9".
    evicted = data_lake / "silver" / "evicted"
    if evicted.is_dir():
        revisions = sorted(
            (d for d in evicted.iterdir() if d.is_dir() and d.name.isdigit()),
            key=lambda d: int(d.name),
        )
        for directory in revisions[: max(0, len(revisions) - keep_evicted)]:
            planned.append(("superseded evicted revision", directory))

    # AppleDouble sidecars are NOT swept here. `plan_appledouble` is a full
    # recursive walk of a 13 TiB exFAT volume — the exact operation this branch is
    # fixing everywhere else (a single-timeframe glob measured 281s cold; `du -sh`
    # over bronze never returned). Putting it inside a nightly 600s budget would
    # reintroduce the bug one task over — and worse than "the sidecars survive":
    # planning completes before anything is deleted, so a traversal that blows
    # the budget deletes NOTHING, logs and evicted revisions included. The whole
    # sweep would be permanently ineffective while reporting only a warning.
    # They are a one-off artifact of the exFAT move, not recurring garbage, so
    # `--appledouble` runs it deliberately instead.
    return planned


def _sweep_roots(data_lake: Path) -> tuple[Path, ...]:
    """Resolved directories the sidecar walk is allowed inside: the lake itself
    plus whatever the lake's own first-level entries point at. Since 2026-09-06
    the lake root is a real internal directory and bronze/, gold/, quarantine/,
    repairs/, security_master/ … are each a symlink onto /Volumes/DATA_LAKE, so
    the volume is reached through those declared entries and nowhere else. A
    deeper symlink resolving outside all of them leaves the lake and is not ours
    to sweep.
    """
    roots = [data_lake.resolve()]
    try:
        entries = sorted(data_lake.iterdir())
    except OSError:
        return tuple(roots)
    roots += [entry.resolve() for entry in entries if entry.is_symlink() and entry.is_dir()]
    return tuple(roots)


def _iter_sidecars(data_lake: Path, entered: set[str]) -> Iterator[Path]:
    """Yield every `._*` file under the lake, recording the first-level subtrees
    the walk actually got inside.

    `os.walk(..., followlinks=True)` rather than `Path.rglob("._*")`: rglob does
    not descend into a symlinked directory, and since the 2026-09-06 layout
    change every large subtree of the lake is one. Paths are yielded unresolved
    so `_is_protected` still matches them against the lake root it was handed.
    """
    roots = _sweep_roots(data_lake)
    seen: set[tuple[int, int]] = set()
    try:
        root_stat = data_lake.stat()
        seen.add((root_stat.st_dev, root_stat.st_ino))
    except OSError:
        return
    for dirpath, dirnames, filenames in os.walk(data_lake, followlinks=True):
        parts = Path(dirpath).relative_to(data_lake).parts
        if parts:
            entered.add(parts[0])
        followable = []
        for name in dirnames:
            child = Path(dirpath) / name
            # Only a symlink can escape the lake or close a loop; a real
            # subdirectory needs neither check and both cost a stat apiece.
            if not child.is_symlink():
                followable.append(name)
                continue
            try:
                if not any(child.resolve().is_relative_to(root) for root in roots):
                    continue  # points off the lake
                stat = child.stat()
            except OSError:
                continue  # dangling, or unreadable: nothing to sweep behind it
            key = (stat.st_dev, stat.st_ino)
            if key in seen:
                continue  # a loop, or the same subtree reachable twice
            seen.add(key)
            followable.append(name)
        dirnames[:] = followable
        for name in filenames:
            if name.startswith("._"):
                yield Path(dirpath) / name


def _warn_on_subtrees_the_walk_missed(data_lake: Path, entered: set[str]) -> None:
    """Rule 9: a sweep that covered nothing must not read as a clean sweep.

    `os.walk` yields a dirpath for every directory it enters, empty ones
    included, so a first-level subtree that is missing from `entered` was not
    walked at all — a dangling symlink (the volume is unmounted), a target off
    the lake, or an unreadable directory. That is the 2026-09-06 shape: 71
    deletions, 0 failures, and every sidecar still on disk.
    """
    try:
        children = sorted(data_lake.iterdir())
    except OSError:
        return
    for child in children:
        if child.name in entered or _is_protected(child, data_lake):
            continue
        if child.is_dir() or (child.is_symlink() and not child.exists()):
            log.warning("appledouble sweep never entered %s — it swept nothing there", child)


def plan_appledouble(data_lake: Path) -> list[tuple[str, Path]]:
    """Opt-in sweep. Walks the whole lake — minutes, not seconds. Never nightly."""
    entered: set[str] = set()
    planned = [
        ("AppleDouble sidecar", path)
        for path in sorted(_iter_sidecars(data_lake, entered))
        if not _is_protected(path, data_lake)
    ]
    _warn_on_subtrees_the_walk_missed(data_lake, entered)
    return planned


def plan_evidence_locks(data_lake: Path) -> list[tuple[str, Path]]:
    """Opt-in sweep of orphan `.<digest>.lock` files in the flat evidence CAS.

    The one sanctioned exception to the `raw/` protection, and scoped by name to
    `.*.lock` files sitting directly in `raw/shepherd/sha256/`: those are not
    provider bytes, they are the lock files `persist_raw` created and never
    unlinked -- 137,504 of them, half the 275,006 entries in that one directory,
    on 2026-09-05. An artifact is never matched, and no subdirectory (a shard) is
    entered.

    Opt-in, like `--appledouble` and for the same reason: listing a
    275k-entry directory on exFAT is minutes of cold I/O, and `main()` plans
    everything before it deletes anything, so a glob that blows the nightly
    tail's 600s budget would delete nothing at all.
    """
    flat = data_lake / "raw" / "shepherd" / "sha256"
    if not flat.is_dir():
        return []
    return [("orphan source-evidence lock", path) for path in sorted(flat.glob(".*.lock")) if path.is_file()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Warehouse retention sweeps")
    parser.add_argument("--apply", action="store_true", help="Actually delete (default: dry run)")
    parser.add_argument(
        "--appledouble",
        action="store_true",
        help="Also sweep exFAT ._* sidecars. Walks the whole lake — minutes. Not for the nightly job.",
    )
    parser.add_argument(
        "--evidence-locks",
        action="store_true",
        help="Also sweep orphan source-evidence .lock files. Lists a 275k-entry directory — minutes.",
    )
    parser.add_argument("--log-retention-days", type=int, default=LOG_RETENTION_DAYS)
    parser.add_argument("--keep-releases", type=int, default=KEEP_RELEASES)
    parser.add_argument("--keep-evicted", type=int, default=KEEP_EVICTED)
    parser.add_argument("--log-dir", type=Path, default=None)
    parser.add_argument("--data-lake", type=Path, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    resolved_logs = args.log_dir or log_dir()
    resolved_lake = args.data_lake or data_lake_dir()

    planned = plan_housekeeping(
        resolved_logs,
        resolved_lake,
        log_retention_days=args.log_retention_days,
        keep_evicted=args.keep_evicted,
    )
    if args.appledouble:
        planned += plan_appledouble(resolved_lake)
    if args.evidence_locks:
        planned += plan_evidence_locks(resolved_lake)
    for reason, path in planned:
        log.info("%s %s (%s)", "DELETE" if args.apply else "would delete", path, reason)

    # release.prune never collects the release `current` points at. Previewed in
    # dry run too: the operator review this command exists for is worthless if
    # the one category that deletes 422 MB at a time is invisible until --apply.
    for name in prune_releases(args.keep_releases, dry_run=not args.apply):
        log.info("%s release %s", "pruned" if args.apply else "would prune", name)

    deleted = 0
    failed = 0
    if args.apply:
        for _, path in planned:
            try:
                if path.is_dir():
                    shutil.rmtree(path)  # not ignore_errors: see below
                else:
                    path.unlink(missing_ok=True)
                deleted += 1
            except OSError as exc:
                # `ignore_errors=True` would let this report a clean sweep while
                # the artifacts are still there — the exact "green while wrong"
                # shape the rest of this branch exists to remove.
                failed += 1
                log.warning("could not delete %s: %s", path, exc)
        log.info("%d item(s) deleted, %d failed", deleted, failed)
        return 1 if failed else 0

    log.info("%d item(s) would be deleted", len(planned))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
