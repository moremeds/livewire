#!/usr/bin/env python3
"""Build the single nightly digest for Livewire.

Assembles one plain-text report from the machine-readable SUMMARY_JSON lines the
daily jobs now emit, plus the day's coverage line and disk headroom. This
replaces the noisy per-warrant daily-summary email: one digest on success, and
a rare truthful failure mail only on systemic failure.

Every section renders "(not found)" for missing inputs — build_digest never
raises, so a missing log can never suppress the whole digest.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from livewire_scripts.daily_outcomes import parse_all_summary_json, parse_last_summary_json
from livewire_scripts.paths import data_lake_dir, log_dir

_LOG_DIR: Path | None = None
_DATA_LAKE: Path | None = None
_FAILURE_EMAIL_SCRIPT = _PROJECT_ROOT / "livewire_node" / "send_daily_update_failure_email.mjs"
_MIN_FREE_GB = float(os.getenv("MDW_FLATFILE_MIN_FREE_GB", "25"))
_GIB = 1024**3
#: Coverage is daily and the digest reads yesterday's by design, so 3 absorbs
#: one missed run without absorbing a job that has stopped firing entirely.
_COVERAGE_STALE_DAYS = 3


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def _outcomes_section(run_date: str, log_dir: Path) -> list[str]:
    text = _read_text(log_dir / f"daily_update_{run_date}.log")
    lines = ["Daily update outcomes:"]
    summaries = [s for s in parse_all_summary_json(text or "") if s.get("job") == "daily_update"]
    if not summaries:
        lines.append("  (not found)")
        return lines
    for s in summaries:
        lines.append(
            f"  {s.get('asset_class', '?'):<10} "
            f"updated={s.get('updated', 0)}  "
            f"no_trade={s.get('no_trade', 0)}  "
            f"partial={s.get('partial', 0)}  "
            f"errors={s.get('errors', 0)}"
        )
    return lines


def _phases_section(run_date: str, log_dir: Path) -> list[str]:
    text = _read_text(log_dir / f"intraday_catchup_{run_date}.log")
    lines = ["Intraday catch-up phases:"]
    summary = parse_last_summary_json(text or "") if text else None
    if summary is None or summary.get("job") != "daily_backfill":
        lines.append("  (not found)")
        return lines
    degraded = set(summary.get("degraded", []))
    for p in summary.get("phases", []):
        label = p.get("label", "?")
        if p.get("exit") == 0:
            status = "ok"
        elif label in degraded:
            # A Gateway outage is not a failure. Naming it "DEGRADED" rather
            # than "FAILED (exit 86)" is the whole point of the new field —
            # a page-shaped word for a non-page trains the reader to ignore it.
            status = "DEGRADED (IB down)"
        else:
            status = f"FAILED (exit {p.get('exit')})"
        lines.append(f"  {label:<44} {status:<18} {p.get('duration_s', '?')}s")
    if summary.get("degraded"):
        lines.append(f"  degraded: {', '.join(summary['degraded'])}")
    if summary.get("failed"):
        lines.append(f"  failed: {', '.join(summary['failed'])}")
    return lines


def _silver_section(run_date: str, log_dir: Path) -> list[str]:
    """Silver rebuild outcome, and loudly whenever new data cost published history.

    The rebuild lane logs into the same daily-update log; its SUMMARY_JSON carries no
    `job` key, which is what tells it apart from the per-asset-class outcome lines.
    """
    text = _read_text(log_dir / f"daily_update_{run_date}.log")
    lines = ["Silver rebuild:"]
    summaries = [s for s in parse_all_summary_json(text or "") if "window_regressions" in s]
    if not summaries:
        lines.append("  (not found)")
        return lines
    s = summaries[-1]
    lines.append(
        f"  revision={s.get('revision', '?')}  rebuilt={s.get('rebuilt', 0)}  "
        f"unchanged={s.get('unchanged', 0)}  trimmed={s.get('trimmed', 0)}  failed={s.get('failed', 0)}"
    )
    regressions = s.get("window_regressions", 0)
    if regressions:
        lines.append(
            f"  ⚠ {regressions} symbol(s) withheld: their silver-grade window SHRANK, so new data "
            f"cost published history. They still serve their previous window — investigate the "
            f"newest bars before the next publish."
        )
    return lines


#: `_spawn_post_success_quality` writes exactly this on a failure. Matching the
#: existing line means no parallel marker format to keep in sync — three weeks of
#: real logs are already in this shape.
_QUALITY_WARNING_RE = re.compile(r"^WARNING: (?P<label>.+?) failed: (?P<reason>.+)$", re.MULTILINE)


def _quality_jobs_section(run_date: str, log_dir: Path) -> list[str]:
    """Report post-success quality jobs that failed.

    `_spawn_post_success_quality` swallows these into a WARNING by design — they
    must never flip a successful daily run to failure. But nothing counted them,
    so `coverage` timed out at its 600s budget every single night from
    2026-07-07 to at least 07-27 and the digest never mentioned it. Three weeks
    with no coverage data and no signal that coverage was the thing broken.
    """
    text = _read_text(log_dir / f"daily_update_{run_date}.log") or ""
    # Dedup by label: one job failing on all three retry passes is one problem.
    seen = {m["label"]: m["reason"].strip() for m in _QUALITY_WARNING_RE.finditer(text)}
    if not seen:
        # Deliberately not "(not found)": a run with no failures is a pass, and
        # a missing log is already reported by every other section.
        return ["Quality jobs: all green"]
    return [f"Quality jobs: {len(seen)} FAILED"] + [f"  {label}: {reason}" for label, reason in sorted(seen.items())]


def _coverage_section(run_date: str, log_dir: Path) -> list[str]:
    """Report the newest coverage measurement, whatever day it measured.

    Coverage runs as its own launchd job now, so its log will rarely carry the
    run date. Matching on an exact filename would report "(not found)" every
    night — indistinguishable from the detector being dead, which is exactly
    how the real outage hid for four weeks. The measured date is in the line
    itself, so a lag is visible rather than silent.
    """
    lines = ["Coverage:"]
    logs = sorted(log_dir.glob("coverage_*.log"))
    for path in reversed(logs):
        text = _read_text(path)
        if not text or not text.strip():
            continue
        lines.append("  " + text.splitlines()[0].strip())
        # Decoupling the schedules removes the ordering bug but opens a new
        # silence: if com.livewire.coverage stops firing, the newest log simply
        # stops advancing and the digest keeps printing a green line forever —
        # the same dead-detector shape, one level up. Age is the only thing that
        # distinguishes "measured yesterday" from "has not run since July".
        measured = path.stem.removeprefix("coverage_")
        try:
            age = (date.fromisoformat(run_date) - date.fromisoformat(measured)).days
        except ValueError:
            return lines
        if age > _COVERAGE_STALE_DAYS:
            lines.append(
                f"  ⚠ newest coverage log is {age} days old — has the coverage job run?"
            )
        return lines
    lines.append("  (not found)")
    return lines


def _disk_section(data_lake: Path, warehouse: Path | None = None) -> list[str]:
    """Report every distinct volume the warehouse depends on.

    `data-lake` is a symlink to an external volume, so measuring it alone read
    "6752.4 GiB free" every night while the internal volume holding releases,
    logs, cursors and the venv sat below its own reserve, unreported. One
    symlink silently swapped the monitored object.

    Deduplicated on the usage triple, not on st_dev: when both paths live on
    one filesystem — any deployment without the external drive — disk_usage
    returns identical numbers and this prints a single line. Read field by
    field rather than `tuple(usage)` so any object exposing total/used/free
    works, which is what the existing tripwire test patches in.

    # ponytail: two genuinely distinct volumes with byte-identical
    # total/used/free would collapse to one line. Cosmetic, astronomically
    # unlikely, and it keeps the dedup to data this function already has.
    """
    paths = [("lake", data_lake)]
    if warehouse is not None:
        paths.append(("warehouse", warehouse))

    volumes: list[tuple[str, tuple[int, int, int]]] = []
    seen: set[tuple[int, int, int]] = set()
    for label, path in paths:
        try:
            usage = shutil.disk_usage(path)
        except OSError:
            continue
        key = (usage.total, usage.used, usage.free)
        if key in seen:
            continue
        seen.add(key)
        volumes.append((label, key))

    # Label only once there is something to distinguish. A single-filesystem
    # deployment keeps reading plain "Disk:", which is also what it means.
    lines: list[str] = []
    for label, (total, used, free) in volumes:
        free_gib = free / _GIB
        suffix = "" if len(volumes) == 1 else f" [{label}]"
        line = f"Disk{suffix}: {free_gib:.1f} GiB free ({100.0 * used / total:.0f}% used)"
        if free_gib < 2 * _MIN_FREE_GB:
            line += f"  ⚠ raw retention deferred — free space under {2 * _MIN_FREE_GB:.0f} GiB"
        lines.append(line)
    return lines


def build_digest(run_date: date, log_dir: Path, data_lake: Path) -> str:
    """Assemble the nightly digest text. Never raises on missing inputs."""
    run = run_date.isoformat()
    sections: list[list[str]] = [
        [f"Livewire nightly digest — {run}"],
        _outcomes_section(run, log_dir),
        _phases_section(run, log_dir),
        _silver_section(run, log_dir),
        _quality_jobs_section(run, log_dir),
        _coverage_section(run, log_dir),
        _disk_section(data_lake, log_dir.parent),
    ]
    return "\n\n".join("\n".join(section) for section in sections) + "\n"


def _send_email(body: str, run_date: date, node_bin: str, log_dir: Path, runner) -> int:
    body_file = log_dir / f"nightly_digest_{run_date.isoformat()}.txt"
    body_file.parent.mkdir(parents=True, exist_ok=True)
    body_file.write_text(body, encoding="utf-8")
    result = runner(
        [
            node_bin,
            str(_FAILURE_EMAIL_SCRIPT),
            "--mode",
            "digest",
            "--run-date",
            run_date.isoformat(),
            "--body-file",
            str(body_file),
        ],
        check=False,
    )
    return int(result.returncode or 0)


def main(argv=None, runner=subprocess.run) -> int:
    parser = argparse.ArgumentParser(description="Build the Livewire nightly digest")
    parser.add_argument("--run-date", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--email", action="store_true", help="Send the digest via Nodemailer")
    parser.add_argument("--log-dir", type=Path, default=_LOG_DIR or log_dir())
    parser.add_argument("--data-lake", type=Path, default=_DATA_LAKE or data_lake_dir())
    args = parser.parse_args(argv)

    body = build_digest(args.run_date, args.log_dir, args.data_lake)
    print(body)
    if args.email:
        node_bin = os.getenv("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"
        rc = _send_email(body, args.run_date, node_bin, args.log_dir, runner)
        if rc == 0:
            # The digest is the post-daily quality artifact; write the marker
            # only after the email path succeeds so the watchdog cannot mask a failed send.
            marker = args.log_dir / f"quality_summary_{args.run_date.isoformat()}.marker"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(f"nightly digest {args.run_date.isoformat()}\n", encoding="utf-8")
        return rc
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
