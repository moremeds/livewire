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

import argparse
import enum
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from rich.console import Console
from rich.markup import escape

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from livewire_scripts.daily_outcomes import (
    parse_all_summary_json,
    parse_last_summary_json,
    resolve_exit_code,
)
from livewire_scripts.paths import data_lake_dir
from livewire_scripts.paths import log_dir as default_log_dir

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
        line = (
            f"  {s.get('asset_class', '?'):<10} "
            f"updated={s.get('updated', 0)}  "
            f"no_trade={s.get('no_trade', 0)}  "
            f"partial={s.get('partial', 0)}  "
            f"errors={s.get('errors', 0)}"
        )
        # The denominator, when the producer recorded it. Only gapped symbols
        # are fetched, so `no_trade=974` alone cannot be told apart from "we
        # looked at 974 of 13,385". Older log lines have no such keys.
        scanned, current = s.get("scanned"), s.get("up_to_date")
        if scanned is not None:
            line += f"   [of {int(scanned):,} scanned"
            if current is not None:
                line += f", {int(current):,} already current"
            line += "]"
        lines.append(line)
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


def _previous_silver_summary(run_date: str, log_dir: Path) -> dict | None:
    """The most recent Silver SUMMARY_JSON from a log older than *run_date*.

    Reverse-sorted filenames mean this normally reads exactly one file. There is
    no absolute threshold for `failed` anywhere in this module: 233 may be
    normal or catastrophic and nothing measured tells us which, so the baseline
    is the previous run and the signal is the change.

    The date is PARSED rather than string-compared. `daily_update_*.log` also
    matches `daily_update_watchdog_<date>.log`, which is a different job's log
    — those sort first under `reverse=True` and only fall out of a `>=` string
    comparison because "w" happens to exceed "2". Right answer, wrong reason.
    """
    try:
        cutoff = date.fromisoformat(run_date)
    except ValueError:
        return None
    for path in sorted(log_dir.glob("daily_update_*.log"), reverse=True):
        try:
            stamp = date.fromisoformat(path.stem.removeprefix("daily_update_"))
        except ValueError:
            continue  # daily_update_watchdog_*.log, or anything else undated
        if stamp >= cutoff:
            continue
        summaries = [s for s in parse_all_summary_json(_read_text(path) or "") if "window_regressions" in s]
        if summaries:
            return summaries[-1]
    return None


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
    previous = _previous_silver_summary(run_date, log_dir)
    if previous is None:
        lines.append("  no previous run to compare against")
        # A missing baseline makes the DELTA unmeasurable, not the regressions.
        # Returning UNKNOWN here would DOWNGRADE a known warning — UNKNOWN
        # ranks below WARN — which is the shape this module exists to prevent.
        verdict = Verdict.WARN if regressions else Verdict.UNKNOWN
        return Section("Silver rebuild", verdict, lines, fix=_SILVER_FIX)
    delta = int(s.get("failed", 0)) - int(previous.get("failed", 0))
    lines.append(f"  failed {delta:+d} vs revision {previous.get('revision', '?')}")
    if regressions or delta > 0:
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
#: Identifies a measurement line inside a coverage log. The log also carries
#: `  1d missing: …` and `MISSING_JSON …` detail lines per run, so "the last
#: non-blank line" is not the last measurement.
_COVERAGE_LINE_RE = re.compile(r"\bcoverage:\s")
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
        # LAST measurement line, not the first. The log is opened in append
        # mode, so a day on which coverage ran twice holds both runs — and the
        # first is the OLDER one. Measured 2026-08-16: an aborted run wrote
        # `1d=0/0 (100.00%)`, the real run two lines below wrote 99.93%, and
        # this section reported the 0/0. Selecting on the `coverage:` marker
        # rather than "non-blank" also skips the `1d missing:`/`MISSING_JSON`
        # detail lines that follow each measurement.
        measurements = [ln.strip() for ln in text.splitlines() if _COVERAGE_LINE_RE.search(ln)]
        if measurements:
            measurement = measurements[-1]
        else:
            # Nothing recognisable as a measurement. Fall back to the first
            # non-blank line so a log that says only "coverage run aborted"
            # is still SHOWN and still grades UNKNOWN below, rather than being
            # skipped in favour of an older log that looks healthier.
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
        if all(int(m["total"]) == 0 for m in _COVERAGE_TF_RE.finditer(measurement)):
            # Every timeframe 0/0 means the run enumerated no files at all, not
            # that the warehouse is perfectly covered. `_coverage_ratios` maps
            # 0/0 to 1.0 to match CoverageResult, which is right per-timeframe
            # (an asset class with no files is not a gap) and catastrophic
            # across all of them: it renders `(100.00%)` for a run that
            # measured nothing. UNKNOWN is the honest grade for a failure to
            # measure — the whole point of this command.
            return Section(
                "Coverage",
                Verdict.UNKNOWN,
                lines,
                fix=f"python scripts/livewire_quality.py coverage --target-date {measured}",
            )
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
        # The coverage job also runs the windowed classifier, and that half is
        # deliberately allowed to fail without taking coverage down. Isolating
        # it is right; letting it fail invisibly is the swallowed-WARNING shape
        # with a different log level. The job exits 0 either way and no alert
        # fires, so this line is the only place an operator can learn of it.
        scan_failure = next(
            (ln.strip() for ln in reversed(text.splitlines()) if ln.strip().startswith("scan: FAILED")), None
        )
        if scan_failure:
            lines.append(f"  ⚠ {scan_failure}")
            return Section(
                "Coverage",
                Verdict.WARN,
                lines,
                fix=f"python scripts/livewire_quality.py coverage --target-date {measured} --no-recover",
            )
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


#: Every plist under launchd/. A job that is absent cannot run and cannot
#: recover on its own, which is the only BAD this check ever reports.
_LAUNCHD_JOBS: tuple[str, ...] = (
    "com.livewire.daily-update",
    "com.livewire.daily-update-watchdog",
    "com.livewire.intraday-catchup",
    "com.livewire.coverage",
    "com.livewire.release-promote",
)


def _launchd_section(runner=subprocess.run) -> Section:
    """Grade the scheduled jobs from `launchctl list`.

    `launchctl list` prints "PID\\tStatus\\tLabel" and the status is the LAST
    exit code with no indication of when it happened. This check therefore
    caps a nonzero exit at WARN: right now the watchdog shows 1 and
    intraday-catchup shows 86, both residue from runs predating the fix now in
    production. Overstating a stale red trains the reader to ignore the surface.
    """
    try:
        result = runner(["launchctl", "list"], capture_output=True, text=True, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return Section(
            "launchd jobs",
            Verdict.UNKNOWN,
            [f"launchd jobs: launchctl unavailable — {exc}"],
            fix="launchctl list | grep com.livewire",
        )

    loaded: dict[str, str] = {}
    for line in (result.stdout or "").splitlines():
        parts = line.split("\t")
        if len(parts) >= 3 and parts[2] in _LAUNCHD_JOBS:
            loaded[parts[2]] = parts[1]

    missing = [label for label in _LAUNCHD_JOBS if label not in loaded]
    nonzero = {label: code for label, code in loaded.items() if code not in {"0", "-"}}

    lines = ["launchd jobs:"]
    for label in _LAUNCHD_JOBS:
        lines.append(f"  {label:<38} {loaded.get(label, 'NOT LOADED')}")
    if missing:
        # Name EVERY missing job, not just the first — an operator who runs the
        # printed command and sees the section still red learns to distrust it.
        # And check the plist actually exists: the repo ships `.plist.example`
        # templates that must be rendered first, so `launchctl load` on an
        # uninstalled label fails with a message that explains nothing.
        agents = Path.home() / "Library/LaunchAgents"
        installed = [label for label in missing if (agents / f"{label}.plist").exists()]
        uninstalled = [label for label in missing if label not in installed]
        lines.append(f"  missing: {', '.join(missing)}")
        if uninstalled:
            fix = (
                f"render the plist first — no {agents}/{uninstalled[0]}.plist exists; "
                f"see launchd/{uninstalled[0]}.plist.example and the CLAUDE.md scheduling block"
            )
        else:
            fix = " && ".join(f"launchctl load {agents}/{label}.plist" for label in installed)
        return Section("launchd jobs", Verdict.BAD, lines, fix=fix)
    if nonzero:
        # A FACT, not a grade. The exit code has no timestamp, so this cannot be
        # acted on — and a WARN nobody can clear is worse than silence: it
        # pinned every nightly digest to yellow regardless of what happened, and
        # a mail that is always yellow trains the reader to stop opening it.
        # The failure this used to stand in for is measured elsewhere, against
        # real artifacts: each job's own outcome/phase section.
        lines.append("  note: last exit code, no timestamp — the outcome sections below grade the actual runs")
    return Section("launchd jobs", Verdict.OK, lines)


#: Two nightly cycles. A queue with nothing newer than this has been proven
#: drainable by every send that succeeded since, so it is a chore, not an alert.
_UNDELIVERED_ACTIVE_DAYS = 2


def _undelivered_queues(log_dir: Path) -> list[Path]:
    """BOTH queues. The repo keeps two, deliberately and separately.

    `MDW_UNDELIVERED_DIR` (default `quality_alerts_undelivered`) is per-flag
    quality alerts — the 4,408 files. `run_daily_update_job.undelivered_dir`
    writes scheduled-job alerts to `<log_dir>/alerts_undelivered` and its
    docstring says the split is intentional. A section called "Undelivered
    alerts" that counts only one of them is misnamed, and the one it would omit
    is the *job failure* page.
    """
    return [
        Path(os.environ.get("MDW_UNDELIVERED_DIR", log_dir / "quality_alerts_undelivered")),
        log_dir / "alerts_undelivered",
    ]


def _undelivered_section(log_dir: Path) -> Section:
    """Count alerts that could not be sent, and grade only the ACTIVE ones.

    An earlier version of this docstring claimed telling a pile-up from a live
    failure "would be guessing". It is not. The queue is written only when a
    send fails, and the jobs mail nightly — so if nothing has landed in it for
    two cycles, delivery has demonstrably worked since and what remains is a
    cleanup chore, not an incident. The 4,408 files from 2026-07-19 held the
    section at WARN every night for a month, which is how a permanently-yellow
    mail stops being read at all.

    `_UNDELIVERED_ACTIVE_DAYS` is derived from the job cadence, not guessed at
    like the coverage budgets this repo retired twice: a daily job either
    failed to mail within the last two days or it did not.
    """
    lines, total, newest_ts, unreadable = ["Undelivered alerts:"], 0, 0.0, []
    for directory in _undelivered_queues(log_dir):
        try:
            entries = [p for p in directory.iterdir() if p.is_file()]
        except FileNotFoundError:
            lines.append(f"  {directory.name:<28} none")
            continue
        except OSError as exc:
            unreadable.append(f"{directory.name}: {exc}")
            lines.append(f"  {directory.name:<28} unreadable — {exc}")
            continue
        if not entries:
            lines.append(f"  {directory.name:<28} none")
            continue
        stamp = max(p.stat().st_mtime for p in entries)
        newest_ts = max(newest_ts, stamp)
        total += len(entries)
        lines.append(
            f"  {directory.name:<28} {len(entries):>6,} file(s), newest {date.fromtimestamp(stamp).isoformat()}"
        )

    if unreadable:
        return Section("Undelivered alerts", Verdict.UNKNOWN, lines, fix=f"ls -ld {log_dir}")
    if not total:
        return Section("Undelivered alerts", Verdict.OK, ["Undelivered alerts: none"])
    newest = date.fromtimestamp(newest_ts)
    stale_days = (date.today() - newest).days
    active = stale_days <= _UNDELIVERED_ACTIVE_DAYS
    headline = f"Undelivered alerts: {total:,} across 2 queues, newest {newest.isoformat()}"
    if not active:
        # Named as a backlog so the reader is not hunting for tonight's failure.
        # The cleanup command still prints here, because `render` shows `fix`
        # only for a non-OK verdict and this one is deliberately OK.
        headline += f" — backlog, {stale_days}d since the last failed send (not an active incident)"
        lines = [headline] + lines[1:]
        lines.append(f"  clear when reviewed: cat $(ls -t {log_dir}/quality_alerts_undelivered/* | head -1)")
        return Section("Undelivered alerts", Verdict.OK, lines)
    return Section(
        "Undelivered alerts",
        Verdict.WARN,
        [headline] + lines[1:],
        # An honest instruction: read one to learn WHY delivery failed, then
        # delete the batch. `ls | head` alone neither diagnoses nor clears, and
        # a fix that overpromises is a fix nobody trusts twice.
        fix=f"cat $(ls -t {log_dir}/quality_alerts_undelivered/* | head -1)   # then rm the batch once understood",
    )


#: Indirection so tests can replace the catalog read without importing duckdb,
#: and so the ImportError guard has exactly one place to live.
def _coverage_headline(database: Path | None):
    from clients.duckdb_catalog import coverage_headline

    return coverage_headline(database)


#: Deliberately the same NUMBER as _COVERAGE_STALE_DAYS and deliberately a
#: separate constant: that one counts calendar days, this one counts trading
#: sessions. Sharing the name would make a future edit to one silently change
#: the other's meaning. The value itself is a starting guess, not a
#: measurement — correct it the first time it misfires.
_CATALOG_STALE_SESSIONS = 3

#: View -> the lane that WRITES it. The catalog only reports; a stale view means
#: its writer is behind, so the fix has to name the writer. Views are
#: `bronze_<asset_class>_1d` over `duckdb_catalog._DAILY_ASSET_CLASSES` plus
#: `silver_equity_1d`; an unmapped name falls back to rebuilding the table.
_CATALOG_LANE_FIX: dict[str, str] = {
    "bronze_equity_1d": "python scripts/livewire_ingest.py daily --asset-class equity --source massive",
    "bronze_futures_1d": "python scripts/livewire_ingest.py daily --asset-class futures",
    "bronze_cmdty_1d": "python scripts/livewire_ingest.py daily --asset-class cmdty",
    "bronze_rates_1d": "python scripts/livewire_ingest.py fred-rates",
    "bronze_volatility_1d": "python scripts/livewire_ingest.py cboe-vol",
    "bronze_fx_1d": "python scripts/livewire_ingest.py fx --days 7",
    "silver_equity_1d": "python scripts/livewire_store.py rebuild-silver --full",
}


def _sessions_behind(newest: date, target: date, limit: int = 10) -> int:
    """Trading sessions between *newest* and *target*, saturating at *limit*.

    Sessions, not calendar days: newest=Friday against target=Monday is one
    session behind but three days, and a calendar-day rule would flag every
    Monday morning as stale.
    """
    from clients.trading_calendar import previous_trading_day

    cursor, count = target, 0
    while cursor > newest and count < limit:
        cursor = previous_trading_day(cursor)
        count += 1
    return count


def _duckdb_section(target: date, database: Path | None = None) -> Section:
    """Grade the DuckDB coverage table's own staleness.

    The table is refreshed by the last phase of `daily-backfill`. When that
    orchestrator stopped running, the table quietly froze — on 2026-08-10 it
    still read 2026-08-07 — and nothing anywhere said so. Catalog staleness is
    a symptom of an upstream lane, which is exactly why it belongs here.
    """
    try:
        headline = _coverage_headline(database)
    except ImportError as exc:
        return Section(
            "DuckDB catalog",
            Verdict.UNKNOWN,
            [f"DuckDB catalog: duckdb unavailable in this environment — {exc}"],
            fix="use the release venv: ~/market-warehouse/current/.venv/bin/python",
        )
    except FileNotFoundError:
        return Section(
            "DuckDB catalog",
            Verdict.UNKNOWN,
            ["DuckDB catalog: never built"],
            fix="python scripts/livewire_store.py duckdb build",
        )
    # No broad `except Exception` here: collect() wraps every check in _safe(),
    # which already degrades an unexpected crash to UNKNOWN. The two caught
    # above are caught because each has a SPECIFIC, actionable message.

    dated = [(name, last) for name, (_count, last) in headline.items() if last is not None]
    if not dated:
        return Section(
            "DuckDB catalog",
            Verdict.UNKNOWN,
            ["DuckDB catalog: table holds no dated rows"],
            fix="python scripts/livewire_store.py duckdb build",
        )

    # The WORST view, not the freshest. `max(dates)` would let one current view
    # green the whole check while bronze_equity_1d sat frozen — the detail lines
    # would print the stale view under an OK headline that carries no fix, which
    # is the same "a fact nobody grades" shape this module exists to kill.
    laggard, oldest = min(dated, key=lambda item: item[1])
    behind = _sessions_behind(oldest, target)
    lines = [
        "DuckDB catalog:",
        f"  oldest view {laggard} last_date={oldest.isoformat()}  ({behind} session(s) behind {target})",
    ]
    for view_name, (count, last) in sorted(headline.items()):
        lines.append(f"  {view_name:<24} {count:>7,} symbols  last={last}")
    # The fix must name the lane that OWNS the laggard, not the catalog. This
    # docstring already says catalog staleness is a symptom of an upstream lane,
    # and then every branch prescribed `duckdb build` — which on 2026-08-16
    # meant "rebuild the table" for `bronze_rates_1d last=2026-08-13`, where the
    # table was reporting correctly and FRED was the one behind. Rebuilding
    # would reproduce the same date and the reader would conclude the surface
    # lies. `duckdb build` stays the fallback: when the owner is unknown, a
    # stale table really is the only thing we can name.
    fix = _CATALOG_LANE_FIX.get(laggard, "python scripts/livewire_store.py duckdb build")
    if behind > _CATALOG_STALE_SESSIONS:
        return Section("DuckDB catalog", Verdict.BAD, lines, fix=fix)
    if behind > 1:
        return Section("DuckDB catalog", Verdict.WARN, lines, fix=fix)
    return Section("DuckDB catalog", Verdict.OK, lines)


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
        _safe("launchd jobs", lambda: _launchd_section(runner=runner)),
        _safe("Undelivered alerts", lambda: _undelivered_section(log_dir)),
        _safe("Daily update outcomes", lambda: _outcomes_section(run, log_dir)),
        _safe("Intraday catch-up phases", lambda: _phases_section(run, log_dir)),
        _safe("Silver rebuild", lambda: _silver_section(run, log_dir)),
        _safe("Quality jobs", lambda: _quality_jobs_section(run, log_dir)),
        _safe("Coverage", lambda: _coverage_section(run, log_dir)),
        _safe("DuckDB catalog", lambda: _duckdb_section(run_date, database)),
        _safe("Disk", lambda: _disk_section(data_lake, log_dir.parent)),
    ]


def render(sections: list[Section]) -> str:
    """Render for a terminal. Returns rich markup; Console() applies it.

    EVERY line here is log-derived text and MUST go through `escape()`.
    Measured 2026-08-10 against rich: a line containing "[/]" raises
    MarkupError and takes the whole command down, and a line containing
    "[bold red]" is silently consumed as a style — the text vanishes from the
    report. Both shapes occur in real log output (error payloads, path
    fragments, `top_errors` reprs).

    Note that a bare "[BAD ]" is NOT a hazard — rich leaves unrecognised tags
    literal. The verdict keeps its brackets so the terminal and the email read
    identically; colour is added on top, not instead.
    """
    lines = ["Livewire status"]
    for section in sections:
        # `lines` defaults to [] on the dataclass and render() is the one path
        # with no try/except above it — an empty-lines Section must not be the
        # thing that kills the report it was added to.
        headline = section.lines[0] if section.lines else f"{section.name}: (no detail)"
        lines.append(f"[{section.verdict.style}][{section.verdict.glyph}][/] {escape(headline)}")
        lines.extend(f"  {escape(line.lstrip())}" for line in section.lines[1:])
        if section.fix and section.verdict is not Verdict.OK:
            lines.append(f"  [dim]fix:[/] {escape(section.fix)}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Assess Livewire's operational state")
    parser.add_argument("--run-date", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--log-dir", type=Path, default=None)
    parser.add_argument("--data-lake", type=Path, default=None)
    args = parser.parse_args(argv)
    sections = collect(
        args.run_date,
        args.log_dir or default_log_dir(),
        args.data_lake or data_lake_dir(),
    )
    # Exit 0 always: see the module docstring. rich strips markup when not a TTY.
    #
    # soft_wrap=True because the fix commands are meant to be COPIED. rich's
    # default word-wrap inserts real newlines at the console width, so
    # `rebuild-silver --full --dry-run --failure-output …` came back as two
    # lines and pasted as two commands. Let the terminal wrap visually instead.
    Console(soft_wrap=True).print(render(sections))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
