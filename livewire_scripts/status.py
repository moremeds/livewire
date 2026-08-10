#!/usr/bin/env python3
"""Assess Livewire's operational state and grade every item.

The reports this replaces stated facts and never judged them: warehouse-wide
zero coverage rendered in the same font as a routine trim, and "(not found)"
— meaning the run's log could not be located at all — read exactly like a
healthy line. Every check here carries a verdict and, when it is not OK, the
command that addresses it.

Nothing here scans parquet. `coverage` costs 1400-2860s and `warehouse` reads
footers; this module reads only what the nightly jobs already produced, and
grades how old that is as its own signal.
"""

from __future__ import annotations

import enum
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from livewire_scripts.daily_outcomes import (
    parse_all_summary_json,
    parse_last_summary_json,
    resolve_exit_code,
)

_MIN_FREE_GB = float(os.getenv("MDW_FLATFILE_MIN_FREE_GB", "25"))
_GIB = 1024**3
#: Coverage is daily and the digest reads yesterday's by design, so 3 absorbs
#: one missed run without absorbing a job that has stopped firing entirely.
_COVERAGE_STALE_DAYS = 3
_SILVER_FIX = "python scripts/livewire_store.py rebuild-silver --full --dry-run --failure-output /tmp/silver-dry.json"


class Verdict(enum.IntEnum):
    """Ordered worst-last so `max()` over a run of checks is the run's verdict.

    UNKNOWN outranks OK deliberately. A check that could not measure has not
    passed — rendering it green is exactly how coverage stayed dead for four
    weeks while the digest printed a line every night.

    IntEnum, not Enum: plain Enum members are unorderable and `max()` over them
    raises TypeError, so the ordering above would have been documentation with
    no mechanism behind it. Identity comparison (`is Verdict.OK`) still works.
    """

    OK = 0
    UNKNOWN = 1
    WARN = 2
    BAD = 3

    @property
    def glyph(self) -> str:
        return {Verdict.OK: "OK ", Verdict.UNKNOWN: "?? ", Verdict.WARN: "WARN", Verdict.BAD: "BAD "}[self]

    @property
    def style(self) -> str:
        return {Verdict.OK: "green", Verdict.UNKNOWN: "magenta", Verdict.WARN: "yellow", Verdict.BAD: "red"}[self]


@dataclass(frozen=True)
class Section:
    """One graded check: the prose the digest already printed, plus a judgement."""

    name: str
    verdict: Verdict
    lines: list[str] = field(default_factory=list)
    fix: str | None = None


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def _outcomes_section(run_date: str, log_dir: Path) -> Section:
    path = log_dir / f"daily_update_{run_date}.log"
    text = _read_text(path)
    lines = ["Daily update outcomes:"]
    if text is None:
        # BAD, not UNKNOWN: no log at all means the nightly run appears never to
        # have started. "(not found)" used to render exactly like a healthy line.
        lines.append("  (not found)")
        return Section("Daily update outcomes", Verdict.BAD, lines, fix="launchctl start com.livewire.daily-update")
    summaries = [s for s in parse_all_summary_json(text) if s.get("job") == "daily_update"]
    if not summaries:
        # The job ran and wrote something but produced no machine-readable
        # outcome. Cannot measure is not the same as did not run.
        lines.append("  (not found)")
        return Section("Daily update outcomes", Verdict.UNKNOWN, lines, fix=f"tail -50 {path}")
    for s in summaries:
        lines.append(
            f"  {s.get('asset_class', '?'):<10} "
            f"updated={s.get('updated', 0)}  "
            f"no_trade={s.get('no_trade', 0)}  "
            f"partial={s.get('partial', 0)}  "
            f"errors={s.get('errors', 0)}"
        )
    # Reuse the repo's own measured rule rather than inventing a second one.
    # Grading every nonzero `errors` as WARN would put updated=0/errors=13311 at
    # the same severity as one flaky warrant — the exact disease this module
    # exists to cure, reproduced inside the cure.
    systemic = [
        s
        for s in summaries
        if resolve_exit_code(
            updated=int(s.get("updated", 0)),
            no_trade=int(s.get("no_trade", 0)),
            partial=int(s.get("partial", 0)),
            errors=int(s.get("errors", 0)),
        )
        != 0
    ]
    if systemic:
        target = systemic[0].get("target_date") or run_date
        return Section(
            "Daily update outcomes",
            Verdict.BAD,
            lines,
            fix=f"python scripts/livewire_ingest.py daily --target-date {target}",
        )
    if any(int(s.get("errors", 0)) for s in summaries):
        return Section("Daily update outcomes", Verdict.WARN, lines, fix=f"grep -c ERROR {path}")
    return Section("Daily update outcomes", Verdict.OK, lines)


def _phases_section(run_date: str, log_dir: Path) -> Section:
    path = log_dir / f"intraday_catchup_{run_date}.log"
    text = _read_text(path)
    lines = ["Intraday catch-up phases:"]
    if text is None:
        lines.append("  (not found)")
        return Section(
            "Intraday catch-up phases",
            Verdict.BAD,
            lines,
            fix="launchctl start com.livewire.intraday-catchup",
        )
    summary = parse_last_summary_json(text)
    if summary is None or summary.get("job") != "daily_backfill":
        lines.append("  (not found)")
        return Section("Intraday catch-up phases", Verdict.UNKNOWN, lines, fix=f"tail -50 {path}")
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
    if summary.get("failed"):
        return Section(
            "Intraday catch-up phases",
            Verdict.BAD,
            lines,
            fix="python scripts/livewire_ingest.py daily-backfill",
        )
    if summary.get("degraded"):
        # A Gateway outage is degraded, not failed, and livewire must not try to
        # recover it: 2FA and IBKR maintenance are not this repo's to fix.
        return Section(
            "Intraday catch-up phases",
            Verdict.WARN,
            lines,
            fix="nc -z 127.0.0.1 4001  # IB Gateway: 2FA/maintenance, do not restart from this repo",
        )
    return Section("Intraday catch-up phases", Verdict.OK, lines)


def _silver_section(run_date: str, log_dir: Path) -> Section:
    """Silver rebuild outcome, and loudly whenever new data cost published history.

    The rebuild lane logs into the same daily-update log; its SUMMARY_JSON carries no
    `job` key, which is what tells it apart from the per-asset-class outcome lines.
    """
    text = _read_text(log_dir / f"daily_update_{run_date}.log")
    lines = ["Silver rebuild:"]
    summaries = [s for s in parse_all_summary_json(text or "") if "window_regressions" in s]
    if not summaries:
        lines.append("  (not found)")
        return Section("Silver rebuild", Verdict.UNKNOWN, lines, fix=_SILVER_FIX)
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
        return Section("Silver rebuild", Verdict.WARN, lines, fix=_SILVER_FIX)
    return Section("Silver rebuild", Verdict.OK, lines)


#: `_spawn_post_success_quality` writes exactly this on a failure. Matching the
#: existing line means no parallel marker format to keep in sync — three weeks of
#: real logs are already in this shape.
_QUALITY_WARNING_RE = re.compile(r"^WARNING: (?P<label>.+?) failed: (?P<reason>.+)$", re.MULTILINE)


def _quality_jobs_section(run_date: str, log_dir: Path) -> Section:
    """Report post-success quality jobs that failed.

    `_spawn_post_success_quality` swallows these into a WARNING by design — they
    must never flip a successful daily run to failure. But nothing counted them,
    so `coverage` timed out at its 600s budget every single night from
    2026-07-07 to at least 07-27 and the digest never mentioned it. Three weeks
    with no coverage data and no signal that coverage was the thing broken.
    """
    path = log_dir / f"daily_update_{run_date}.log"
    text = _read_text(path) or ""
    # Dedup by label: one job failing on all three retry passes is one problem.
    seen = {m["label"]: m["reason"].strip() for m in _QUALITY_WARNING_RE.finditer(text)}
    if not seen:
        # Deliberately not "(not found)": a run with no failures is a pass, and
        # a missing log is already reported by every other section.
        return Section("Quality jobs", Verdict.OK, ["Quality jobs: all green"])
    lines = [f"Quality jobs: {len(seen)} FAILED"] + [f"  {label}: {reason}" for label, reason in sorted(seen.items())]
    return Section("Quality jobs", Verdict.WARN, lines, fix=f"grep '^WARNING:' {path}")


#: Matches one timeframe in `format_one_liner`'s output, e.g. "1d=0/13311 (0.00%)".
_COVERAGE_TF_RE = re.compile(r"(?P<tf>\w+)=(?P<present>\d+)/(?P<total>\d+)")
#: The same knob coverage_report.py already uses; adopting it adds no new judgement.
_THRESHOLD = float(os.getenv("MDW_COVERAGE_ALERT_THRESHOLD", "0.95"))


def _coverage_ratios(measurement: str) -> dict[str, float]:
    """Per-timeframe ratio from a coverage one-liner.

    total == 0 is 1.0, matching CoverageResult.ratio — an asset class with no
    files is not a gap, and dividing by it would raise out of a section that
    promises never to.
    """
    return {
        m["tf"]: (1.0 if int(m["total"]) == 0 else int(m["present"]) / int(m["total"]))
        for m in _COVERAGE_TF_RE.finditer(measurement)
    }


def _coverage_section(run_date: str, log_dir: Path) -> Section:
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
        if not text:
            continue
        # First NON-BLANK line, not first line. A partially-flushed write whose
        # first line is empty would otherwise print a bare "  " and return —
        # a blank line that reads as "coverage ran fine" while saying nothing,
        # and it would mask the older log that does carry a measurement.
        measurement = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
        if not measurement:
            continue
        lines.append("  " + measurement)
        # Decoupling the schedules removes the ordering bug but opens a new
        # silence: if com.livewire.coverage stops firing, the newest log simply
        # stops advancing and the digest keeps printing a green line forever —
        # the same dead-detector shape, one level up. Age is the only thing that
        # distinguishes "measured yesterday" from "has not run since July".
        measured = path.stem.removeprefix("coverage_")
        try:
            age = (date.fromisoformat(run_date) - date.fromisoformat(measured)).days
        except ValueError:
            # Cannot date the measurement, so cannot judge its staleness. Saying
            # OK here would be the dead-detector shape one level up.
            return Section(
                "Coverage",
                Verdict.UNKNOWN,
                lines,
                fix=f"ls -l {path}   # log name carries no parseable date",
            )
        if age > _COVERAGE_STALE_DAYS:
            lines.append(f"  ⚠ newest coverage log is {age} days old — has the coverage job run?")
        ratios = _coverage_ratios(measurement)
        if not ratios:
            return Section("Coverage", Verdict.UNKNOWN, lines, fix=f"head -1 {path}   # coverage line did not parse")
        below = {tf: ratio for tf, ratio in ratios.items() if ratio < _THRESHOLD}
        if below:
            lines.append(
                "  below threshold: "
                + ", ".join(f"{tf}={ratio:.2%}" for tf, ratio in sorted(below.items()))
                + f" (< {_THRESHOLD:.0%})"
            )
            return Section(
                "Coverage",
                Verdict.BAD,
                lines,
                fix=f"python scripts/livewire_quality.py coverage --target-date {measured}",
            )
        if age > _COVERAGE_STALE_DAYS:
            # Green numbers from a frozen detector are the four-week outage.
            return Section("Coverage", Verdict.BAD, lines, fix="launchctl start com.livewire.coverage")
        return Section("Coverage", Verdict.OK, lines)
    lines.append("  (not found)")
    return Section("Coverage", Verdict.UNKNOWN, lines, fix="launchctl start com.livewire.coverage")


def _disk_section(data_lake: Path, warehouse: Path | None = None) -> Section:
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
        # A filesystem reporting total=0 tells us nothing, and dividing by it
        # would raise out of _disk_section — killing the WHOLE digest, which
        # this module's docstring promises never happens on a missing input.
        if not usage.total:
            continue
        key = (usage.total, usage.used, usage.free)
        if key in seen:
            continue
        seen.add(key)
        volumes.append((label, key))

    if not volumes:
        return Section("Disk", Verdict.UNKNOWN, ["Disk: (unavailable)"], fix="df -h")

    # Label only once there is something to distinguish. A single-filesystem
    # deployment keeps reading plain "Disk:", which is also what it means.
    lines: list[str] = []
    tightest = None
    for label, (total, used, free) in volumes:
        free_gib = free / _GIB
        tightest = free_gib if tightest is None else min(tightest, free_gib)
        suffix = "" if len(volumes) == 1 else f" [{label}]"
        line = f"Disk{suffix}: {free_gib:.1f} GiB free ({100.0 * used / total:.0f}% used)"
        if free_gib < 2 * _MIN_FREE_GB:
            line += f"  ⚠ raw retention deferred — free space under {2 * _MIN_FREE_GB:.0f} GiB"
        lines.append(line)
    # The existing numbers, newly graded. Today the digest prints a ⚠ below 2×
    # the reserve and says nothing at all below 1× — the more serious state was
    # the quieter one.
    if tightest is not None and tightest < _MIN_FREE_GB:
        return Section("Disk", Verdict.BAD, lines, fix="python scripts/livewire_ops.py housekeeping")
    if tightest is not None and tightest < 2 * _MIN_FREE_GB:
        return Section("Disk", Verdict.WARN, lines, fix="python scripts/livewire_ops.py housekeeping")
    return Section("Disk", Verdict.OK, lines)


def _safe(name: str, builder) -> Section:
    """Run one check, degrading a crash to UNKNOWN.

    Both renderers go through this: nightly_digest's contract is that a missing
    input cannot suppress the whole report, and a check that *crashes* must be
    visible rather than silently absent.
    """
    try:
        return builder()
    except Exception as exc:  # a broken check must not kill the report
        return Section(
            name=name,
            verdict=Verdict.UNKNOWN,
            lines=[f"{name}: check failed — {exc}"],
            fix="python scripts/livewire_ops.py status   # reproduce, then read the traceback",
        )


def collect(
    run_date: date,
    log_dir: Path,
    data_lake: Path,
    *,
    runner=subprocess.run,
    database: Path | None = None,
) -> list[Section]:
    """Assess every cheap signal. Never raises. Never scans parquet.

    `runner` and `database` exist so tests reach no real machine state. Without
    them the launchd check shells out to the operator's real `launchctl` and the
    catalog check opens the operator's real analytics.duckdb — both from a unit
    test, which the repo's testing rules forbid. They are declared here from the
    start so the signature never changes underneath a written test.
    """
    run = run_date.isoformat()
    return [
        _safe("Daily update outcomes", lambda: _outcomes_section(run, log_dir)),
        _safe("Intraday catch-up phases", lambda: _phases_section(run, log_dir)),
        _safe("Silver rebuild", lambda: _silver_section(run, log_dir)),
        _safe("Quality jobs", lambda: _quality_jobs_section(run, log_dir)),
        _safe("Coverage", lambda: _coverage_section(run, log_dir)),
        _safe("Disk", lambda: _disk_section(data_lake, log_dir.parent)),
    ]
