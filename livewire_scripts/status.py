#!/usr/bin/env python3
"""Assess Livewire's operational state from the append-only ledger."""

from __future__ import annotations

import argparse
import enum
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

from clients import constants, ledger
from livewire_scripts.paths import data_lake_dir
from livewire_scripts.paths import log_dir as default_log_dir

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


def _lane_values(lanes: tuple[str, ...]) -> str:
    """Render a lane tuple as a DuckDB VALUES list: `('futures'), ('cmdty')`."""
    return ", ".join(f"('{lane}')" for lane in lanes)


#: Every operational check is one SQL statement over the ledger plus one test.
CHECKS: list[tuple[str, str]] = [
    (
        "Daily update ran",
        "select case verdict when 'FAILED' then 'BAD' when 'DEGRADED' then 'WARN' "
        "when 'OK' then 'OK' else 'UNKNOWN' end as verdict, run_id, started "
        "from runs where job = 'daily-update' and date(started) = date '$today' "
        "and ended is not null order by started desc limit 1",
    ),
    (
        "Daily update finished",
        "select 'WARN' as verdict, run_id, "
        "date_diff('minute', min(started), now()) as running_minutes "
        "from runs where run_id = '$open_run' "
        "group by run_id having max(ended) is null",
    ),
    (
        "Intraday catch-up ran",
        "select case verdict when 'FAILED' then 'BAD' when 'DEGRADED' then 'WARN' "
        "when 'OK' then 'OK' else 'UNKNOWN' end as verdict, run_id, started "
        "from runs where job = 'intraday-catchup' and date(started) = date '$today' "
        "and ended is not null order by started desc limit 1",
    ),
    (
        "Intraday catch-up finished",
        "select 'WARN' as verdict, run_id, date_diff('minute', min(started), now()) as running_minutes "
        "from runs where job = 'intraday-catchup' and date(started) = date '$today' "
        "group by run_id having max(ended) is null order by min(started) desc limit 1",
    ),
    (
        "Lanes terminal",
        "select case when '$run' = '' then 'UNKNOWN' "
        "when count(*) = 0 then 'OK' else 'BAD' end as verdict, "
        "count(*) as unterminated, string_agg(lane, ', ') as lanes from ("
        f"  select expected.lane from (values {_lane_values(constants.LANE_ORDER)}) as expected(lane) "
        "  left join (select lane, outcome from lane_results where run_id = '$run' "
        "    qualify row_number() over (partition by lane order by started desc, ended desc nulls last) = 1"
        "  ) latest using (lane) where latest.outcome is null"
        ")",
    ),
    (
        "Lanes blocked",
        "select 'WARN' as verdict, lane, blocker from lane_results "
        "where run_id = '$run' and outcome = 'blocked' and blocker = 'lake_lock' "
        "order by lane",
    ),
    (
        "Corporate-action progress",
        "select 'OK' as verdict, "
        "max(case when name = 'progress' then value end) as symbols, "
        "max(case when name = 'progress_total' then value end) as universe "
        "from measurements where scope = 'corporate-actions' "
        "and name in ('progress', 'progress_total') and run_id = '$run' "
        "having count(*) > 0",
    ),
    (
        "Silver advanced",
        "select case when outcome = 'done' then 'OK' else 'BAD' end as verdict, "
        "outcome, blocker from lane_results "
        "where run_id = '$run' and lane = 'silver' and outcome is not null "
        "order by ended desc limit 1",
    ),
    (
        "Post-success tail",
        "select case when outcome = 'done' then 'OK' else 'BAD' end as verdict, outcome, exit_code "
        "from lane_results where run_id = '$run' and lane = 'digest' and outcome is not null "
        "order by ended desc limit 1",
    ),
    (
        "Undelivered alerts",
        "select 'WARN' as verdict, script, count(*) as failed_sends from executions "
        "where script = 'send_alert' and exit_code <> 0 and date(started) = date '$today' "
        "group by script",
    ),
    (
        "Release matches main",
        "select case when '$main_sha' = '__missing__' then 'UNKNOWN' "
        "when release_sha = '$main_sha' then 'OK' else 'BAD' end as verdict, "
        "release_sha, '$main_sha' as main_sha from runs where run_id = '$run'",
    ),
    (
        "Lanes within budget",
        "select 'WARN' as verdict, lane, elapsed_s, budget_s from lane_results "
        "where run_id = '$run' and elapsed_s is not null and elapsed_s > budget_s "
        "union all select 'UNKNOWN', null, null, null where '$run' = '' "
        "order by elapsed_s desc",
    ),
    (
        "Silver failures did not grow",
        "select case when count(*) < 2 then 'UNKNOWN' "
        "when max(case when rn = 1 then value end) > max(case when rn = 2 then value end) "
        "then 'WARN' else 'OK' end as verdict, "
        "max(case when rn = 1 then value end) as failed_now, "
        "max(case when rn = 2 then value end) as failed_before from ("
        "  select value, row_number() over (order by measured_at desc) as rn "
        "  from measurements where name = 'silver_failed'"
        ") where rn <= 2",
    ),
    (
        "Silver window regressions",
        "select case when value > 0 then 'WARN' else 'OK' end as verdict, value as symbols "
        "from measurements where name = 'silver_window_regressions' "
        "order by measured_at desc limit 1",
    ),
    (
        "Coverage",
        "select case when count(*) filter (where name = 'coverage_pct') < 5 "
        "or count(*) filter (where name = 'coverage_total') < 5 "
        "or sum(value) filter (where name = 'coverage_total') = 0 then 'UNKNOWN' "
        "when min(value) filter (where name = 'coverage_pct') < $coverage_threshold then 'BAD' "
        "when date_diff('day', date(max(measured_at)), date '$today') > $coverage_stale_days then 'BAD' "
        "else 'OK' end as verdict, count(*) filter (where name = 'coverage_pct') as timeframes, "
        "min(value) filter (where name = 'coverage_pct') as worst_ratio, "
        "max(measured_at) as measured_at from ("
        "  select name, scope, value, measured_at from measurements "
        "  where name in ('coverage_pct', 'coverage_total') "
        "  and scope in ('1d', '1m', '1h', '5m', '30m') "
        "  qualify row_number() over (partition by name, scope order by measured_at desc) = 1"
        ")",
    ),
    (
        "Coverage scan",
        "select case when value = 1 then 'OK' else 'WARN' end as verdict, value as succeeded "
        "from measurements where name = 'coverage_scan_ok' order by measured_at desc limit 1",
    ),
    (
        "IB-only lanes behind",
        f"select case when count(last_session) < {len(constants.IB_ONLY_LANES)} then 'UNKNOWN' "
        "when max(behind) > $ib_slack_days then 'WARN' else 'OK' end as verdict, "
        "string_agg(lane || '@' || last_session || case when blocker is null then '' "
        "else ' (' || blocker || ')' end, ', ') as lanes, max(behind) as sessions_behind from ("
        "  select expected.lane, date '1970-01-01' + cast(m.value as int) as last_session, "
        "         date_diff('day', date '1970-01-01' + cast(m.value as int), date '$today') as behind, "
        "         (select l.blocker from lane_results l where l.lane = expected.lane "
        "          and l.outcome is not null order by l.ended desc limit 1) as blocker "
        f"  from (values {_lane_values(constants.IB_ONLY_LANES)}) as expected(lane) left join measurements m "
        "    on m.name = 'last_session' and m.scope = expected.lane "
        "  qualify row_number() over (partition by expected.lane order by m.measured_at desc) = 1"
        ")",
    ),
    (
        "Declared constants match reality",
        "select case when _n = 0 then 'UNKNOWN' "
        "when declared_value > 2 * measured_p95 or measured_p95 > 2 * declared_value "
        "then 'WARN' else 'OK' end as verdict, "
        "name, scope, declared_value, measured_p95 from ("
        "  select name, case when name = 'lake_lock_wait_s' then '' else scope end as scope, "
        "    arg_max(value, measured_at) filter (where source = 'declared') as declared_value, "
        "    quantile_cont(value, 0.95) filter (where source = 'measured') as measured_p95, "
        "    count(*) filter (where source = 'measured') as _n "
        "  from measurements "
        "  where measured_at >= today() - interval 14 day "
        "    and ((name = 'lane_budget_s' and scope <> 'default') or name = 'lake_lock_wait_s') "
        "  group by all"
        ") "
        "where declared_value is not null "
        "order by case when _n = 0 then 1 "
        "  when declared_value > 2 * measured_p95 or measured_p95 > 2 * declared_value "
        "  then 0 else 2 end, name, scope limit 1",
    ),
]

_EMPTY_IS_OK = {
    "Undelivered alerts",
    "Lanes blocked",
    "Lanes within budget",
    "Daily update finished",
    "Intraday catch-up finished",
}
IB_LANE_SLACK_DAYS = 4
_FIXES = {
    "Daily update ran": "launchctl list | grep livewire.daily-update   # then read <log_dir>/daily_update_$today.log",
    "Daily update finished": (
        'python scripts/livewire_ops.py ledger query "select lane, outcome, elapsed_s '
        "from lane_results where run_id = '$open_run'\"   # which lane is still open"
    ),
    "Intraday catch-up ran": "launchctl start com.livewire.intraday-catchup",
    "Silver failures did not grow": _SILVER_FIX,
    "Silver window regressions": _SILVER_FIX,
    "Coverage": "launchctl start com.livewire.coverage",
    "Coverage scan": "python scripts/livewire_quality.py coverage --no-recover",
    "Lanes terminal": (
        "python scripts/livewire_ops.py ledger query \"select lane, outcome from lane_results where run_id = '$run'\""
    ),
    "Lanes blocked": (
        'python scripts/livewire_ops.py ledger query "select scope, value from measurements '
        "where name = 'lake_lock_wait_s' order by value desc\"   # who held the lake, and for how long"
    ),
    "Silver advanced": _SILVER_FIX,
    "Post-success tail": "python scripts/livewire_quality.py digest --email",
    "Undelivered alerts": (
        'python scripts/livewire_ops.py ledger query "select receipt_json from executions '
        "where script = 'send_alert' and exit_code <> 0\""
    ),
    "Release matches main": "python scripts/livewire_ops.py release promote",
    "Lanes within budget": (
        "raise the lane's budget only after measuring it cold; see clients/constants.py (lane_budget_s/<lane>)"
    ),
    "Declared constants match reality": (
        "re-measure cold on the real lake, then change the value in "
        "clients/constants.py (lane_budget_s/<lane>, lake_lock_wait_s) -- not an LW_DECLARED_* override"
    ),
    "IB-only lanes behind": (
        "nc -z 127.0.0.1 4001 && echo up || echo down   # then 2FA by hand; rerun: "
        "python scripts/livewire_ingest.py daily --asset-class futures / --asset-class cmdty"
    ),
}


def _substitute(sql: str, params: dict[str, str]) -> str:
    for key, value in params.items():
        sql = sql.replace(f"${key}", value)
    return sql


def run_check(name: str, sql: str, params: dict[str, str]) -> Section:
    """Execute one ledger check; missing evidence is never silently green."""
    rows = [row for row in ledger.query(_substitute(sql, params)) if any(value is not None for value in row.values())]
    fix = _substitute(_FIXES.get(name, ""), params) or None
    if not rows:
        if name in _EMPTY_IS_OK:
            return Section(name, Verdict.OK, [f"{name}: none"])
        return Section(name, Verdict.UNKNOWN, [f"{name}: no rows — nothing measured"], fix=fix)
    verdict = max(Verdict[str(row["verdict"])] if row.get("verdict") else Verdict.OK for row in rows)
    lines = [f"{name}:"] + [
        "  " + "  ".join(f"{key}={value}" for key, value in row.items() if key != "verdict") for row in rows
    ]
    return Section(name, verdict, lines, fix=fix if verdict is not Verdict.OK else None)


def _last_run_id(today: str, *, closed: bool) -> str:
    """Return today's latest daily-update run id, optionally requiring closure."""
    clause = "and ended is not null " if closed else ""
    rows = ledger.query(
        "select run_id from runs where job = 'daily-update' "
        f"and date(started) = date '{today}' {clause}order by started desc limit 1"
    )
    return str(rows[0]["run_id"]) if rows else ""


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
    # Read at call time, never at import: an import-time read is fixed for the
    # life of the process, so an operator's one-run LW_DECLARED_ override (and
    # any test that sets it after importing this module) is silently ignored.
    min_free_gb = constants.declared("flatfile_min_free_gb")
    for label, (total, used, free) in volumes:
        free_gib = free / _GIB
        tightest = free_gib if tightest is None else min(tightest, free_gib)
        suffix = "" if len(volumes) == 1 else f" [{label}]"
        line = f"Disk{suffix}: {free_gib:.1f} GiB free ({100.0 * used / total:.0f}% used)"
        if free_gib < 2 * min_free_gb:
            line += f"  ⚠ raw retention deferred — free space under {2 * min_free_gb:.0f} GiB"
        lines.append(line)
    # The existing numbers, newly graded. Today the digest prints a ⚠ below 2×
    # the reserve and says nothing at all below 1× — the more serious state was
    # the quieter one.
    if tightest is not None and tightest < min_free_gb:
        return Section("Disk", Verdict.BAD, lines, fix="python scripts/livewire_ops.py housekeeping")
    if tightest is not None and tightest < 2 * min_free_gb:
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
    main_sha: str | None = None,
) -> list[Section]:
    """Assess every cheap signal without scanning bar parquet."""
    today = run_date.isoformat()
    try:
        closed_run = _last_run_id(today, closed=True)
        open_run = _last_run_id(today, closed=False)
    except Exception:
        closed_run = open_run = ""
    params = {
        "today": today,
        "run": closed_run,
        "open_run": open_run,
        "main_sha": main_sha or "__missing__",
        "ib_slack_days": str(IB_LANE_SLACK_DAYS),
        "coverage_threshold": str(constants.declared("coverage_alert_threshold")),
        "coverage_stale_days": str(_COVERAGE_STALE_DAYS),
    }
    return [
        _safe("launchd jobs", lambda: _launchd_section(runner=runner)),
        *[_safe(name, lambda n=name, sql=sql: run_check(n, sql, params)) for name, sql in CHECKS],
        _safe("DuckDB catalog", lambda: _duckdb_section(run_date, database)),
        _safe("Disk", lambda: _disk_section(data_lake, log_dir.parent)),
    ]


def render(sections: list[Section]) -> str:
    """Render for a terminal. Returns rich markup; Console() applies it.

    EVERY line here may contain operator-controlled or external text and MUST
    go through `escape()`.
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
    parser.add_argument("--main-sha", default=None)
    args = parser.parse_args(argv)
    sections = collect(
        args.run_date,
        args.log_dir or default_log_dir(),
        args.data_lake or data_lake_dir(),
        main_sha=args.main_sha,
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
