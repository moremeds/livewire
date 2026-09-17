#!/usr/bin/env python3
"""Assess Livewire's operational state from the append-only ledger."""

from __future__ import annotations

import argparse
import enum
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TypeGuard

from rich.console import Console
from rich.markup import escape

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients import constants, ledger
from livewire_scripts.paths import cursor_dir, data_lake_dir, resolve_capacity_target, warehouse_dir
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
    notification_key: str | None = None
    #: The run this check is a statement about, when its rows name exactly one.
    #: Lets a pager tell "the lane already reported this" from "nobody has".
    run_id: str | None = None


def _lane_values(lanes: tuple[str, ...]) -> str:
    """Render a lane tuple as a DuckDB VALUES list: `('futures'), ('cmdty')`."""
    return ", ".join(f"('{lane}')" for lane in lanes)


#: Every operational check is one SQL statement over the ledger plus one test.
CHECKS: list[tuple[str, str]] = [
    (
        "Daily update ran",
        "select case verdict when 'FAILED' then 'BAD' when 'DEGRADED' then 'WARN' "
        "when 'ABANDONED' then 'WARN' "
        "when 'OK' then 'OK' else 'UNKNOWN' end as verdict, run_id, started "
        "from runs where job = 'daily-update' and date(started) = date '$today' "
        "and ended is not null "
        "union all select case when timestamp '$now' > timestamp '$today 07:00:00' "
        "then 'BAD' else 'UNKNOWN' end, 'no run today', null "
        "where not exists (select 1 from runs where job = 'daily-update' "
        "and date(started) = date '$today') "
        "order by started desc nulls last limit 1",
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
        "when 'ABANDONED' then 'WARN' "
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
        # Aggregated, so the check always returns exactly one row: a zero-row
        # shape here read `none` in _EMPTY_IS_OK even when no run had resolved.
        # Both scheduled jobs write the lake, so both can be the deferred one.
        "select case when '$run' = '' then 'UNKNOWN' "
        "when count(*) = 0 then 'OK' else 'WARN' end as verdict, "
        "count(*) as blocked, string_agg(job || ':' || lane, ', ' order by job, lane) as lanes "
        "from ("
        "  select 'daily-update' as job, lane from lane_results where run_id = '$run' "
        "    and outcome = 'blocked' and blocker = 'lake_lock' "
        "  union all "
        "  select 'intraday-catchup' as job, lane from lane_results where run_id = '$intraday_run' "
        "    and outcome = 'blocked' and blocker = 'lake_lock'"
        ")",
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
        "Silver progress",
        "select 'OK' as verdict, "
        "max(case when name = 'progress' then value end) as symbols, "
        "max(case when name = 'progress_total' then value end) as universe "
        "from measurements where scope = 'silver' "
        "and name in ('progress', 'progress_total') and run_id = '$run' "
        "having count(*) > 0",
    ),
    (
        "Silver lane completed",
        "select case when outcome = 'done' then 'OK' else 'BAD' end as verdict, "
        "outcome, blocker from lane_results "
        "where run_id = '$run' and lane = 'silver' and outcome is not null "
        "order by ended desc limit 1",
    ),
    (
        "Catalog build",
        # The digest runs before its enclosing daily run closes. Read the
        # terminal build directly, including the intraday writer's build.
        "select case outcome when 'done' then 'OK' when 'blocked' then 'WARN' "
        "else 'BAD' end as verdict, run_id, lane, outcome, exit_code, ended "
        "from lane_results where lane in ('catalog', 'daily_backfill_duckdb_coverage') "
        "and outcome is not null order by ended desc, started desc limit 1",
    ),
    (
        "Post-success tail",
        "select case when outcome = 'done' then 'OK' else 'BAD' end as verdict, outcome, exit_code "
        "from lane_results where run_id = '$run' and lane = 'tail' and outcome is not null "
        "order by ended desc limit 1",
    ),
    (
        "Undelivered notifications",
        "select 'WARN' as verdict, count(*) as failed_sends, "
        "string_agg(json_extract_string(receipt_json,'$.subject'), '; ') as subjects "
        "from executions where script = 'notify' and exit_code <> 0 "
        "and date(started) = date '$today' having count(*) > 0",
    ),
    (
        "Digest sent today",
        "select 'OK' as verdict, started, json_extract_string(receipt_json,'$.subject') as subject "
        "from executions where script = 'notify' and exit_code = 0 "
        "and json_extract_string(receipt_json,'$.kind') = 'digest' "
        "and date(started) = date '$today' "
        "union all select case when timestamp '$now' > timestamp '$today 20:00:00' "
        "then 'BAD' else 'UNKNOWN' end, null, 'not sent yet' "
        "where not exists (select 1 from executions where script = 'notify' and exit_code = 0 "
        "and json_extract_string(receipt_json,'$.kind') = 'digest' "
        "and date(started) = date '$today') "
        "order by started desc nulls last limit 1",
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
        "Silver failures",
        "select case when count(*) = 0 then 'UNKNOWN' "
        "when max(case when rn = 1 then value end) > 0 "
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
        "select case when count(*) < 5 then 'UNKNOWN' "
        "when min(pct) filter (where total > 0) < $coverage_threshold then 'BAD' "
        "when date_diff('day', date(max(measured_at)), date '$today') > $coverage_stale_days then 'BAD' "
        "when count(*) filter (where total = 0) > 0 then 'UNKNOWN' else 'OK' end as verdict, "
        "string_agg(scope || '=' || case when total = 0 then 'UNKNOWN(expected=0)' "
        "else format('{:.1f}%', 100*pct) end, ' ' order by scope) as scopes, "
        "min(pct) filter (where total > 0) as worst_ratio, max(measured_at) as measured_at from ("
        "  select p.scope, p.value as pct, t.value as total, p.measured_at from "
        "  (select scope, value, measured_at from measurements where name = 'coverage_pct' "
        "   and scope in ('1d','1m','1h','5m','30m') "
        "   qualify row_number() over (partition by scope order by measured_at desc) = 1) p "
        "  join (select scope, value from measurements where name = 'coverage_total' "
        "   and scope in ('1d','1m','1h','5m','30m') "
        "   qualify row_number() over (partition by scope order by measured_at desc) = 1) t "
        "  using (scope))",
    ),
    (
        "Coverage ran today",
        # A deliberate stand-down is a fact too: coverage_skipped reads UNKNOWN
        # with its reason, and a later real scan supersedes it. With no fact at
        # all, BAD only after 17:30Z — coverage starts 15:05Z and may wait on
        # upstreams, so a no-row afternoon is pending, not absent.
        "select case when value = 1 then 'OK' else 'WARN' end as verdict, measured_at, null as reason "
        "from measurements where name = 'coverage_scan_ok' and date(measured_at) = date '$today' "
        "union all select 'UNKNOWN', measured_at, scope as reason "
        "from measurements where name = 'coverage_skipped' and date(measured_at) = date '$today' "
        "and not exists (select 1 from measurements where name = 'coverage_scan_ok' "
        "and date(measured_at) = date '$today') "
        "union all select case when timestamp '$now' > timestamp '$today 17:30:00' "
        "then 'BAD' else 'UNKNOWN' end, null, null "
        "where not exists (select 1 from measurements where name = 'coverage_scan_ok' "
        "and date(measured_at) = date '$today') "
        "and not exists (select 1 from measurements where name = 'coverage_skipped' "
        "and date(measured_at) = date '$today') "
        "order by measured_at desc nulls last limit 1",
    ),
    (
        "Coverage recovery",
        "select case when count(*) = 0 then 'UNKNOWN' when max(twice) = 1 then 'BAD' "
        "when max(latest) = 1 then 'WARN' else 'OK' end as verdict, "
        "string_agg(scope || case when twice = 1 then '=deferred x2' when latest = 1 then '=deferred' "
        "else '=ok' end, ' ' order by scope) as scopes from ("
        "  select scope, max(case when rn = 1 then value end) as latest, "
        "  case when max(case when rn = 1 then value end) = 1 "
        "  and max(case when rn = 2 then value end) = 1 then 1 else 0 end as twice from ("
        "    select scope, value, row_number() over (partition by scope order by measured_at desc) as rn "
        "    from measurements where name = 'coverage_recovery_deferred') where rn <= 2 group by scope)",
    ),
    (
        "Stale non-equity",
        "select 'WARN' as verdict, string_agg(scope || '=' || cast(value as int), ' ') as classes from ("
        "  select scope, value from measurements where name = 'stale_non_equity' "
        "  qualify row_number() over (partition by scope order by measured_at desc) = 1) "
        "where value > 0 having count(*) > 0",
    ),
    (
        "Foreign-currency dividends",
        # Today's dividend-fx fact only: a repaired lake reads OK while older
        # WARN rows still exist, and a day with no conversion at all reads
        # UNKNOWN rather than inheriting yesterday's green. 2026-09-14: the
        # conversion was wired to no lane, so the newest row was Sunday's manual
        # run (mismatch=0) and the check stayed OK through a Monday that failed
        # ~30 symbols in Silver. A detector with no output is dead, not healthy.
        # scope='all' only: a targeted `--tickers` repair measures a handful of
        # symbols and would otherwise erase the night's whole-scope WARN as the
        # newest row of the day. 'all' is the lane's own pass over the universe
        # it reconciles, which is what Silver consumes.
        # A zero is whole-scope evidence only if today's corporate-actions lane
        # finished: the lane converts at the end of every cycle, so a lane
        # SIGKILLed at its budget after cycle one's conversion leaves cycle
        # two's foreign-currency dividends unconverted behind that cycle's
        # already-filed zero. No lane row today at all is a manual run and
        # grades as before.
        # ... and a failing conversion files `dividend_fx_error` instead of a
        # count. The lane swallows that exception, which can be raised before
        # the `runs` row is opened or after it closed OK, so one rule -- the
        # newest whole-scope row of the day -- covers every failure mode a gate
        # per mode would keep missing.
        "select case when name = 'dividend_fx_error' then 'UNKNOWN' "
        "when value > 0 then 'WARN' "
        "when coalesce(("
        "  select coalesce(outcome, 'running') from lane_results "
        "  where lane = 'corporate-actions' and date(started) = date '$today' "
        "  order by started desc, ended desc nulls last limit 1), 'done') <> 'done' then 'UNKNOWN' "
        "else 'OK' end as verdict, "
        "name as fact, value as mismatched, measured_at, coalesce(("
        "  select coalesce(outcome, 'running') from lane_results "
        "  where lane = 'corporate-actions' and date(started) = date '$today' "
        "  order by started desc, ended desc nulls last limit 1), 'none') as lane_outcome "
        "from measurements where name in ('dividend_currency_mismatch', 'dividend_fx_error') "
        "and scope = 'all' and date(measured_at) = date '$today' "
        "order by measured_at desc limit 1",
    ),
    (
        "Membership sync ran today",
        # Weekday-only job (01:00Z): on Sat/Sun the check reads OK regardless.
        # A missing run on a weekday is UNKNOWN, not BAD — the job pages on its
        # own fetch failure, so this check is only "did it run at all".
        "select case when isodow(date '$today') > 5 then 'OK' "
        "when verdict = 'FAILED' then 'BAD' else 'OK' end as verdict, run_id, started "
        "from runs where job = 'membership-sync' and date(started) = date '$today' "
        "and ended is not null "
        "union all select case when isodow(date '$today') > 5 then 'OK' else 'UNKNOWN' end, "
        "'no run today', null "
        "where not exists (select 1 from runs where job = 'membership-sync' "
        "and date(started) = date '$today') "
        "order by started desc nulls last limit 1",
    ),
    (
        "Unresolved memberships",
        # Latest membership_unresolved per index; never measured is UNKNOWN,
        # never green (same contract as "Foreign-currency dividends").
        "select case when max(value) is null then 'UNKNOWN' "
        "when max(value) > 0 then 'WARN' else 'OK' end as verdict, "
        "coalesce(sum(value), 0) as unresolved, "
        "string_agg(scope || '=' || cast(value as int), ' ' order by scope) as indexes "
        "from (select scope, value from measurements where name = 'membership_unresolved' "
        "  qualify row_number() over (partition by scope order by measured_at desc) = 1)",
    ),
    (
        "IB-only lanes behind",
        f"select case when count(last_session) < {len(constants.IB_ONLY_LANES)} then 'UNKNOWN' "
        "when max(behind) > $ib_slack_days then 'WARN' else 'OK' end as verdict, "
        "string_agg(lane || '@' || last_session || case when blocker is null then '' "
        "else ' (' || blocker || ')' end, ', ') as lanes, max(behind) as sessions_behind, "
        "string_agg(lane, ', ' order by lane) filter (where last_session is null or behind > $ib_slack_days) "
        "as affected_lanes, string_agg(blocker, ', ' order by blocker) "
        "filter (where last_session is null or behind > $ib_slack_days) as blockers from ("
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
    "Undelivered notifications",
    "Stale non-equity",
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
    "Silver failures": _SILVER_FIX,
    "Silver window regressions": _SILVER_FIX,
    "Coverage": "launchctl start com.livewire.coverage",
    "Coverage ran today": "python scripts/livewire_quality.py coverage --no-recover",
    "Coverage recovery": (
        "launchctl start com.livewire.coverage   # recovery defers only while its precondition holds; "
        "the coverage log names which"
    ),
    "Stale non-equity": (
        'python scripts/livewire_ops.py ledger query "select scope, value from measurements '
        "where name = 'stale_non_equity'\"   # then query that symbol's last observation upstream"
    ),
    "Foreign-currency dividends": (
        "python scripts/livewire_ingest.py corporate-actions convert-dividend-currency   "
        "# dry-run first; add --apply --output-dir <dir> to repair"
    ),
    "Membership sync ran today": "launchctl start com.livewire.membership-sync   # then read logs/launchd/com.livewire.membership-sync.stderr.log",
    "Unresolved memberships": (
        "python scripts/livewire_ingest.py security-master sync --index <id>   "
        "# fetch identities, then: "
        "python scripts/livewire_ingest.py membership-sync reresolve --index <id> --confidence B"
    ),
    "Digest sent today": "launchctl start com.livewire.digest",
    "Lanes terminal": (
        "python scripts/livewire_ops.py ledger query \"select lane, outcome from lane_results where run_id = '$run'\""
    ),
    "Lanes blocked": (
        'python scripts/livewire_ops.py ledger query "select scope, value from measurements '
        "where name = 'lake_lock_wait_s' order by value desc\"   # who held the lake, and for how long"
    ),
    "Silver lane completed": _SILVER_FIX,
    "Post-success tail": (
        'python scripts/livewire_ops.py ledger query "select * from lane_results '
        "where run_id = '$run' and lane = 'tail'\""
    ),
    "Undelivered notifications": (
        'python scripts/livewire_ops.py ledger query "select receipt_json from executions '
        "where script = 'notify' and exit_code <> 0\""
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


def _notification_key(name: str, verdict: Verdict, rows: list[dict]) -> str:
    """Stable fault identity plus its measured impact, never attempt chronology."""
    fields = {
        "scope",
        "lane",
        "lanes",
        "affected_lanes",
        "blocker",
        "blockers",
        "outcome",
        "asset_class",
        "symbol",
        "symbols",
        "ticker",
        "tickers",
        "timeframe",
        "timeframes",
        "name",
        "script",
        "failed_now",
        "failed",
        "failed_symbols",
        "failed_count",
        "mismatched",
        "missing",
        "missing_count",
        "unterminated",
        "unresolved",
        "blocked",
        "silver_failed",
        "silver_window_regressions",
        "worst_ratio",
    }
    identities = set()
    for row in rows:
        if row.get("verdict") == "OK":
            continue
        values = {key: value for key, value in row.items() if key in fields}
        if "affected_lanes" in values:
            values.pop("lanes", None)  # The human detail may include dated session evidence.
        for key in ("lanes", "affected_lanes", "blockers"):
            if isinstance(values.get(key), str):
                values[key] = sorted({item.strip() for item in values[key].split(",")})
        identities.add(json.dumps(values, sort_keys=True, separators=(",", ":")))
    return f"{name}:{verdict.name}:" + "|".join(sorted(identities))


def _warning_context(name: str, fix: str | None) -> list[str]:
    return [
        f"  Impact: {name} did not pass; affected scope and counts are limited to the ledger evidence above.",
        "  Evidence: this named ledger check; absent fields remain unknown.",
        "  Last valid: unknown; this check does not establish a previous valid result.",
        "  Automatic handling: unknown; this status check is read-only and performs no recovery.",
        f"  Next action: {fix or 'inspect the named check and its latest ledger evidence before changing data.'}",
        "  Clear condition: the named check returns OK with sufficient current evidence.",
    ]


def run_check(name: str, sql: str, params: dict[str, str]) -> Section:
    """Execute one ledger check; missing evidence is never silently green."""
    rows = [row for row in ledger.query(_substitute(sql, params)) if any(value is not None for value in row.values())]
    fix = _substitute(_FIXES.get(name, ""), params) or None
    if not rows:
        if name in _EMPTY_IS_OK:
            return Section(name, Verdict.OK, [f"{name}: none"])
        return Section(
            name,
            Verdict.UNKNOWN,
            [f"{name}: no rows — nothing measured", *_warning_context(name, fix)],
            fix=fix,
            notification_key=_notification_key(name, Verdict.UNKNOWN, []),
        )
    verdict = max(Verdict[str(row["verdict"])] if row.get("verdict") else Verdict.OK for row in rows)
    lines = [f"{name}:"] + [
        "  " + "  ".join(f"{key}={value}" for key, value in row.items() if key != "verdict") for row in rows
    ]
    if verdict is not Verdict.OK:
        lines.extend(_warning_context(name, fix))
    return Section(
        name,
        verdict,
        lines,
        fix=fix if verdict is not Verdict.OK else None,
        notification_key=_notification_key(name, verdict, rows),
        run_id=_single_run_id(rows),
    )


def _single_run_id(rows: list[dict]) -> str | None:
    """The one run these rows describe, or None when they describe several."""
    ids = {str(row["run_id"]) for row in rows if row.get("run_id")}
    return ids.pop() if len(ids) == 1 else None


def _last_run_id(today: str, *, closed: bool, job: str = "daily-update") -> str:
    """Return today's latest run id for `job`, optionally requiring closure."""
    clause = "and ended is not null " if closed else ""
    rows = ledger.query(
        f"select run_id from runs where job = '{job}' "
        f"and date(started) = date '{today}' {clause}order by started desc limit 1"
    )
    return str(rows[0]["run_id"]) if rows else ""


def _silver_publication_section(data_lake: Path) -> Section:
    """Describe the served Silver revision separately from the latest attempt.

    A committed ``current.json`` answers only what readers may select.  It does
    not turn a failed later rebuild into a success, nor prove a reader actually
    consumed that revision.  The terminal lane row supplies the latter attempt
    fact. A partial rebuild may publish a healthy subset and still exit nonzero;
    no linkage between these facts is inferred without a publication receipt.
    """
    from clients.silver_revision import SilverRevisionPublisher

    pointer = _configured_silver_path(data_lake)
    try:
        committed = SilverRevisionPublisher(pointer).read_current()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return Section(
            "Silver publication",
            Verdict.BAD,
            [
                "Silver publication: committed pointer is not readable",
                "  Impact: Silver readers cannot safely select this manifest reference.",
                f"  Evidence: {pointer / 'revisions/current.json'} — {exc}",
                "  Last valid: manifest reference unknown; artifact hashes have not been checked here.",
                "  Automatic handling: no replacement pointer is selected.",
                "  Next action: inspect the manifest and restore only through rebuild-silver publication.",
                "  Clear condition: current.json matches a valid immutable manifest and the next rebuild completes.",
            ],
            fix=_SILVER_FIX,
            notification_key="silver-publication:pointer-invalid",
        )

    if committed is None:
        return Section(
            "Silver publication",
            Verdict.UNKNOWN,
            [
                "Silver publication: no committed revision",
                "  Impact: Silver readers have no snapshot to select.",
                f"  Evidence: {pointer / 'revisions/current.json'} is absent.",
                "  Last valid: manifest reference unknown; this is not a successful first publication.",
                "  Automatic handling: no candidate is served.",
                "  Next action: run the approved rebuild after its Bronze and corporate-action inputs are valid.",
                "  Clear condition: a complete manifest is committed and readers can select it.",
            ],
            fix=_SILVER_FIX,
            notification_key="silver-publication:pointer-missing",
        )

    # Verified attempt linkage: a decoded receipt that names THIS revision and
    # whose immutable manifest hash/identity verifies against silver_path. A
    # matching revision alone is insufficient — roots, hash and manifest
    # payload must all agree.
    lake_root = str(data_lake.expanduser().resolve())
    silver_root = str(pointer.resolve())
    receipts, receipts_malformed = _silver_receipts(lake_root, silver_root, pointer)
    linked = next(
        (
            receipt
            for receipt in reversed(receipts)
            if receipt["result"] in ("committed", "noop")
            and receipt.get("published_revision") == committed.revision
            and receipt["manifest"] is not None
        ),
        None,
    )
    linkage = (
        f"  Attempt linkage: verified — receipt run={linked['run_id']} names revision={committed.revision} "
        "with a matching immutable manifest."
        if linked is not None
        else "  Attempt linkage: unknown; no verified publication receipt ties an attempt to the current reference."
    )
    # The newest DECODED receipt is reported on its own — a standalone
    # rebuild-silver run produces one without any terminal lane_results row, and
    # an undecodable newer row must not let an older decoded one pose as latest.
    latest_receipt = receipts[-1] if receipts else None
    receipt_line = (
        "  Latest decoded rebuild receipt: "
        + (
            f"{latest_receipt['result']} ended={latest_receipt['ended']} "
            f"selected={len(latest_receipt['selected_symbols'])} "
            f"validated={len(latest_receipt['validated_symbols'])} "
            f"failed={len(latest_receipt['failed_symbols'])} withheld={len(latest_receipt['withheld_symbols'])}"
        )
        if latest_receipt is not None
        else "  Latest decoded rebuild receipt: none on record for these roots"
    )
    receipt_notes = (
        [
            f"  Receipt coverage: {receipts_malformed} rebuild-silver row(s) could not be decoded — "
            "the latest decoded receipt may not be the actual latest."
        ]
        if receipts_malformed
        else []
    )

    rows = ledger.query(
        "select run_id, outcome, blocker, exit_code, started, ended from lane_results "
        "where lane = 'silver' and outcome is not null "
        "order by ended desc nulls last, started desc limit 1"
    )
    latest = rows[0] if rows else None
    incidents = ledger.query(
        "select count(*) as attempts, min(started) as first_seen, max(ended) as last_seen "
        "from lane_results where lane = 'silver' and outcome is not null and outcome <> 'done' "
        "and ended > coalesce((select max(ended) from lane_results where lane = 'silver' and outcome = 'done' "
        "and ended < (select max(ended) from lane_results where lane = 'silver' and outcome is not null)), "
        "timestamptz '1970-01-01 00:00:00+00')"
    )
    incident = incidents[0] if incidents and incidents[0].get("attempts") else None
    committed_line = (
        f"  Current manifest reference: revision={committed.revision} published_at={committed.published_at.isoformat()} "
        f"actions_as_of={committed.corporate_actions_as_of.isoformat()}; artifact hashes were not checked by status."
    )
    if latest is None:
        return Section(
            "Silver publication",
            Verdict.UNKNOWN,
            [
                f"Silver publication: committed revision={committed.revision}; no terminal lane attempt is recorded.",
                "  Impact: readers may select the committed snapshot, but its current freshness is unmeasured.",
                f"  Evidence: {pointer / 'revisions/current.json'} matches immutable revision={committed.revision}.json.",
                committed_line,
                receipt_line,
                "  Last valid: data snapshot unknown; a matching manifest reference does not establish artifact validity.",
                "  Automatic handling: no later candidate is inferred from files on disk.",
                linkage,
                "  Next action: run the normal daily rebuild; do not adopt uncommitted artifacts.",
                "  Clear condition: a terminal rebuild fact and committed manifest agree.",
            ],
            fix=_SILVER_FIX,
            notification_key="silver-publication:attempt-unmeasured",
        )

    outcome = str(latest["outcome"])
    attempt = (
        f"run_id={latest['run_id']} outcome={outcome} exit_code={latest['exit_code']} "
        f"blocker={latest['blocker']} ended={latest['ended']}"
    )
    if outcome == "done":
        lane_time = latest["ended"] or latest["started"]
        receipt_time = (latest_receipt["ended"] or latest_receipt["started"]) if latest_receipt is not None else None
        if latest_receipt is not None and receipt_time is not None and (lane_time is None or receipt_time > lane_time):
            # A standalone rebuild-silver attempt is NEWER than the terminal
            # lane row: the receipt's own result — not the lane's — describes
            # the latest attempt. The receipt stays a receipt; nothing here
            # pretends it is a lane row, and the committed manifest remains a
            # separate fact.
            if latest_receipt["result"] == "attempt_only" or (
                latest_receipt["failed_symbols"] or latest_receipt["withheld_symbols"]
            ):
                degraded = latest_receipt["result"] == "attempt_only"
                headline = (
                    "the latest rebuild attempt's outcome is unknown (attempt-only receipt)"
                    if degraded
                    else "the latest rebuild receipt lists failed/withheld symbols after the last lane record"
                )
                return Section(
                    "Silver publication",
                    Verdict.UNKNOWN if degraded else Verdict.BAD,
                    [
                        f"Silver publication: committed revision={committed.revision}; {headline}.",
                        f"  Impact: the newest attempt did not prove a clean rebuild; "
                        f"revision={committed.revision} remains the served reference.",
                        f"  Evidence: {attempt}; receipt run={latest_receipt['run_id']} "
                        f"result={latest_receipt['result']} ended={latest_receipt['ended']}.",
                        committed_line,
                        receipt_line,
                        *receipt_notes,
                        "  Last valid: data snapshot unknown; a matching manifest reference does not establish artifact validity.",
                        linkage,
                        "  Next action: inspect the recorded failure, repair the named input through its normal publisher, then rerun rebuild-silver.",
                        "  Clear condition: a later rebuild completes and commits a valid manifest; a retry alone is not evidence.",
                    ],
                    fix=_SILVER_FIX,
                    notification_key=(
                        "silver-publication:receipt-attempt-unknown"
                        if degraded
                        else "silver-publication:receipt-attempt-failed"
                    ),
                )
        recovery = []
        if incident:
            recovery.append(
                "  Recovery: latest successful attempt follows "
                f"{incident['attempts']} prior non-success attempt(s), first_seen={incident['first_seen']} "
                f"last_seen={incident['last_seen']}."
            )
        lines = [
            f"Silver publication: committed revision={committed.revision}; latest terminal lane rebuild completed.",
            f"  Evidence: {attempt}; pointer={pointer / 'revisions/current.json'}.",
            committed_line,
            receipt_line,
            *receipt_notes,
            "  Last valid: data snapshot unknown; artifact hashes were not checked by status.",
            "  Reader state: this is a committed publication fact, not proof that a consumer has run on it.",
            linkage,
            *recovery,
        ]
        if receipts_malformed:
            # UNKNOWN floor: an undecodable row may hide a newer failed attempt.
            return Section(
                "Silver publication",
                Verdict.UNKNOWN,
                lines,
                fix=_SILVER_FIX,
                notification_key="silver-publication:receipts-undecodable",
            )
        return Section(
            "Silver publication",
            Verdict.OK,
            lines,
            notification_key="silver-publication:healthy",
        )

    measurements = ledger.query(
        "select name, value from measurements where name in ('silver_failed', 'silver_window_regressions') "
        "and scope = 'silver' and run_id = (select run_id from lane_results where lane = 'silver' "
        "and outcome is not null order by ended desc nulls last, started desc limit 1) "
        "and measured_at >= (select started from lane_results where lane = 'silver' and outcome is not null "
        "order by ended desc nulls last, started desc limit 1) "
        "qualify row_number() over (partition by name order by measured_at desc) = 1"
    )
    impact_counts = {str(row["name"]): row["value"] for row in measurements}
    impact = (
        "a healthy subset may have advanced current.json despite the failed attempt; remaining failures need repair"
    )
    if outcome == "blocked":
        impact = "the recorded attempt was blocked; the pointer is an independent publication fact"
    return Section(
        "Silver publication",
        Verdict.BAD,
        [
            f"Silver publication: committed revision={committed.revision}; latest terminal lane attempt {outcome}.",
            f"  Impact: {impact} (revision={committed.revision}).",
            f"  Evidence: {attempt}; pointer={pointer / 'revisions/current.json'}.",
            "  Measured impact: "
            + (
                ", ".join(f"{key}={value}" for key, value in sorted(impact_counts.items()))
                or "unknown; no counts for this run"
            ),
            receipt_line,
            (
                "  Sustained incident: "
                f"attempts={incident['attempts']} first_seen={incident['first_seen']} last_seen={incident['last_seen']}."
                if incident
                else "  Sustained incident: unknown; no aggregate attempt evidence is available."
            ),
            committed_line,
            "  Last valid: data snapshot unknown; a matching manifest reference does not establish artifact validity.",
            "  Automatic handling: normal publication may commit a healthy subset; this status check changes nothing.",
            linkage,
            "  Next action: inspect the recorded failure, repair the named input through its normal publisher, then rerun rebuild-silver.",
            "  Clear condition: a later rebuild completes and commits a valid manifest; a retry alone is not evidence.",
        ],
        fix=_SILVER_FIX,
        notification_key=_notification_key(
            "Silver publication", Verdict.BAD, [{"outcome": outcome, "blocker": latest["blocker"], **impact_counts}]
        ),
    )


def _is_int(value) -> TypeGuard[int]:
    """Strict int — ``True`` is an int in Python and must not pass a version field."""
    return isinstance(value, int) and not isinstance(value, bool)


def _opt_str(value) -> bool:
    return value is None or isinstance(value, str)


def _opt_int(value) -> bool:
    return value is None or _is_int(value)


def _decode_silver_fault(payload_text) -> dict | None:
    """Validate one evidence payload; None when it is not a usable v1 fault fact."""
    try:
        payload = json.loads(payload_text)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("kind") != "silver_symbol_failure":
        return None
    if not _is_int(payload.get("schema_version")) or payload["schema_version"] != 1:
        return None
    if payload.get("stage") not in ("staging", "withheld_window_regression"):
        return None
    if not isinstance(payload.get("symbol"), str):
        return None
    if not isinstance(payload.get("data_lake_root"), str) or not isinstance(payload.get("silver_root"), str):
        return None
    if not _is_int(payload.get("baseline_revision")):
        return None
    for key in ("error", "error_type", "reason"):
        if not _opt_str(payload.get(key)):
            return None
    bounds = payload.get("input_date_bounds")
    if bounds is not None:
        if not isinstance(bounds, dict) or not (_opt_str(bounds.get("earliest")) and _opt_str(bounds.get("latest"))):
            return None
    source = payload.get("input")
    if source is not None:
        if not isinstance(source, dict) or not (_opt_str(source.get("path")) and _opt_str(source.get("sha256"))):
            return None
    artifacts = payload.get("baseline_artifacts")
    if artifacts is not None and not (
        isinstance(artifacts, list)
        and all(
            isinstance(item, dict) and _opt_str(item.get("path")) and _opt_str(item.get("sha256")) for item in artifacts
        )
    ):
        return None
    return payload


def _decode_executions_receipt(row: dict, *, script: str, kind: str) -> dict | None:
    """Validate one executions receipt; None when absent/malformed/old-version."""
    if row.get("script") != script:
        return None
    try:
        receipt = json.loads(row.get("receipt_json"))
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(receipt, dict) or receipt.get("kind") != kind:
        return None
    if not _is_int(receipt.get("schema_version")) or receipt["schema_version"] != 1:
        return None
    return receipt


def _decode_silver_receipt(row: dict) -> dict | None:
    receipt = _decode_executions_receipt(row, script="rebuild-silver", kind="silver_publication")
    if receipt is None or receipt.get("result") not in ("committed", "noop", "attempt_only"):
        return None
    for key in ("selected_symbols", "staged_symbols", "validated_symbols", "failed_symbols", "withheld_symbols"):
        value = receipt.get(key)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            return None
    if not isinstance(receipt.get("data_lake_root"), str) or not isinstance(receipt.get("silver_root"), str):
        return None
    for key in ("manifest_ref", "manifest_sha256", "generation_id", "attempt_reason", "error"):
        if not _opt_str(receipt.get(key)):
            return None
    if not _opt_int(receipt.get("baseline_revision")) or not _opt_int(receipt.get("published_revision")):
        return None
    return receipt


def _verified_manifest(receipt: dict, silver_path: Path) -> set[str] | None:
    """Verify the immutable manifest a committed/noop receipt names.

    Returns the manifest's affected-symbol set: a closer must then name the
    symbol in BOTH the receipt's ``validated_symbols`` and the manifest it
    claims. The filename is rebuilt from a positive int ``published_revision``
    (never trusted verbatim), the resolved path must stay under ``silver_path``,
    and the payload's schema/revision/generation must match the receipt — so an
    arbitrary file with a lucky hash, a traversal, or a foreign manifest cannot
    close a fault.
    """
    revision = receipt.get("published_revision")
    if receipt.get("result") not in ("committed", "noop") or not _is_int(revision) or revision <= 0:
        return None
    ref = f"revisions/revision={revision}.json"
    if receipt.get("manifest_ref") != ref or not isinstance(receipt.get("manifest_sha256"), str):
        return None
    try:
        resolved = (silver_path / ref).resolve()
        resolved.relative_to(silver_path.resolve())
        body = resolved.read_bytes()
    except (OSError, ValueError):
        return None
    if hashlib.sha256(body).hexdigest() != receipt["manifest_sha256"]:
        return None
    try:
        manifest = json.loads(body)
    except json.JSONDecodeError:
        return None
    if (
        not isinstance(manifest, dict)
        or not _is_int(manifest.get("schema_version"))
        or manifest["schema_version"] != 1
        or not _is_int(manifest.get("revision"))
        or manifest["revision"] != revision
    ):
        return None
    generation = receipt.get("generation_id")
    if not (isinstance(generation, str) and generation and manifest.get("generation_id") == generation):
        return None  # a receipt without a nonempty matching generation proves nothing
    affected = manifest.get("affected")
    if not isinstance(affected, list):
        return None
    return {item["symbol"] for item in affected if isinstance(item, dict) and isinstance(item.get("symbol"), str)}


def _silver_receipts(lake_root: str, silver_root: str, silver_path: Path) -> tuple[list[dict], int]:
    """Decoded rebuild-silver receipts for THESE roots, manifests verified once.

    ``_verified_manifest`` runs once per unique (manifest_ref, sha, generation)
    — a run of noop receipts all naming the same immutable file must not
    re-read and re-hash a multi-MB manifest N times. ``cache`` is a plain local
    dict for this projection call only — no persistent or query cache. The
    affected-symbol set rides on each receipt under ``manifest`` (None when
    unverifiable).
    """
    receipts: list[dict] = []
    malformed = 0
    cache: dict[tuple, set | None] = {}
    for row in ledger.query(
        "select run_id, script, started, ended, receipt_json "
        # Completion order, not start order: overlapping invocations let an
        # earlier-started attempt finish last, and that one is the latest fact.
        # coalesce keeps a missing ended conservative (falls back to started).
        "from executions where script = 'rebuild-silver' "
        "order by coalesce(ended, started), started"
    ):
        receipt = _decode_silver_receipt(row)
        if receipt is None:
            malformed += 1
            continue
        if receipt["data_lake_root"] != lake_root or receipt["silver_root"] != silver_root:
            continue  # another deployment's attempt; roots gate matching AND closure
        # Every input _verified_manifest consumes — a contradictory revision or
        # result must never inherit another receipt's verified set.
        cache_key = (
            receipt.get("result"),
            receipt.get("published_revision"),
            receipt.get("manifest_ref"),
            receipt.get("manifest_sha256"),
            receipt.get("generation_id"),
        )
        if cache_key not in cache:
            cache[cache_key] = _verified_manifest(receipt, silver_path)
        receipt["manifest"] = cache[cache_key]
        receipt["run_id"] = row["run_id"]
        receipt["started"] = row["started"]
        receipt["ended"] = row["ended"]
        receipts.append(receipt)
    return receipts, malformed


def _seen_key(value) -> datetime:
    return value if value is not None else datetime.min.replace(tzinfo=UTC)


def _observe_receipt_fault(issues: dict, symbol: str, stage: str, receipt: dict) -> None:
    """Fold a receipt's failed/withheld scope into the projection.

    Every failed/withheld receipt is its own observation — inherited run ids
    cannot tell invocations apart, so nothing dedupes by run. The observation
    lands on the most recent same-symbol+stage issue (detail-bearing evidence
    or a prior receipt-only entry), advancing ``receipt_failures`` and
    ``last_seen`` at the receipt's ``ended`` — which is also what lets a
    post-recovery failure sharing an old run id reopen the issue.
    """
    observed = receipt["ended"] or receipt["started"]
    issue = None
    for candidate in issues.values():
        if candidate["symbol"] == symbol and candidate["stage"] == stage:
            if issue is None or _seen_key(candidate["last_seen"]) > _seen_key(issue["last_seen"]):
                issue = candidate
    if issue is None:
        issue = issues[symbol, stage, "receipt_scope", "UNKNOWN"] = {
            "symbol": symbol,
            "stage": stage,
            "classification": "receipt_scope",
            "scope": "UNKNOWN",
            "evidence_facts": 0,
            "receipt_failures": 0,
            "last_seen": None,
            "evidence_refs": [],
            "baseline_revision": receipt.get("baseline_revision"),
            "baseline_artifacts": None,
            "detail": "",
            "input": None,
            "bounds": None,
            "run": None,
        }
    issue["receipt_failures"] += 1
    issue["run"] = receipt["run_id"]
    issue["detail"] = issue["detail"] or f"listed in {receipt['result']} receipt {receipt['run_id']}"
    if observed is not None:
        issue["last_seen"] = observed if issue["last_seen"] is None else max(issue["last_seen"], observed)


def _configured_silver_path(data_lake: Path) -> Path:
    """The silver root the publisher resolves for this lake — MDW_SILVER_DIR wins."""
    return Path(os.environ.get("MDW_SILVER_DIR", data_lake / "silver")).expanduser()


_SILVER_FAULTS_LIMIT = 8
_SILVER_FAULTS_QUERY = (
    'python scripts/livewire_ops.py ledger query "select subject, payload_json, fetched_at, run_id '
    "from evidence where kind = 'silver_symbol_failure' order by fetched_at\""
)


def _silver_faults_section(data_lake: Path) -> Section:
    """Project per-symbol Silver faults from evidence rows and publication receipts.

    Committed manifest, latest attempt and consumer observation stay distinct —
    this section claims the first two only. A fault closes only when a later
    ``committed``/``noop`` receipt on the SAME resolved lake+silver roots lists
    the symbol in ``validated_symbols`` AND in the verified manifest it names —
    never on a bare exit 0, a ``--tickers`` run over other symbols, an
    ``attempt_only`` row, or an unscoped ``silver_failed=0``. The closer must
    start at/after the fault's latest observation (``ended`` for receipt-only
    facts), so a delayed older success cannot erase a later failure. Malformed
    or wrong-version rows are counted, never erased and never fatal; input date
    bounds are context — the exact failing session was never recorded.
    """
    lake_root = str(data_lake.expanduser().resolve())
    silver_path = _configured_silver_path(data_lake)
    silver_root = str(silver_path.resolve())

    rows = ledger.query(
        "select evidence_hash, subject, payload_json, fetched_at, run_id "
        "from evidence where kind = 'silver_symbol_failure' order by fetched_at"
    )
    facts: list[tuple[dict, dict]] = []
    malformed = foreign = 0
    for row in rows:
        payload = _decode_silver_fault(row["payload_json"])
        if payload is None:
            malformed += 1
            continue
        if payload["data_lake_root"] != lake_root or payload["silver_root"] != silver_root:
            foreign += 1
            continue
        facts.append((row, payload))

    receipts, receipts_malformed = _silver_receipts(lake_root, silver_root, silver_path)

    # Group recurring facts: symbol + stage + classification + KNOWN scope is
    # one issue. Input bounds stay context-only — they widen daily and would
    # relabel the same fault as new; failing sessions were never recorded.
    issues: dict[tuple, dict] = {}
    for row, payload in facts:
        if payload["stage"] == "staging":
            classification = payload.get("error_type") or "unknown"
            scope = "UNKNOWN"
        else:
            classification = f"window_regression:{payload.get('reason') or 'unknown'}"
            scope = (
                f"{payload['previous_start']}->{payload['new_start']}"
                if isinstance(payload.get("previous_start"), str) and isinstance(payload.get("new_start"), str)
                else "UNKNOWN"
            )
        key = (payload["symbol"], payload["stage"], classification, scope)
        issue = issues.setdefault(
            key,
            {
                "symbol": payload["symbol"],
                "stage": payload["stage"],
                "classification": classification,
                "scope": scope,
                "evidence_facts": 0,
                "receipt_failures": 0,
                "last_seen": None,
                "evidence_refs": [],
                "baseline_revision": payload["baseline_revision"],
                "baseline_artifacts": payload.get("baseline_artifacts"),
                "detail": "",
                "input": None,
                "bounds": None,
                "run": None,
            },
        )
        issue["evidence_facts"] += 1
        seen = row["fetched_at"]
        if seen is not None:
            issue["last_seen"] = seen if issue["last_seen"] is None else max(issue["last_seen"], seen)
        if isinstance(row["evidence_hash"], str):
            issue["evidence_refs"].append(row["evidence_hash"])
        issue["run"] = row["run_id"]
        issue["detail"] = payload.get("error") or payload.get("reason") or issue["detail"]
        issue["input"] = payload.get("input") or issue["input"]
        issue["bounds"] = payload.get("input_date_bounds") or issue["bounds"]

    # A receipt's own failed/withheld scope is a fault fact even when its
    # evidence row never landed (the guarded emit may have failed).
    for receipt in receipts:
        for symbol in receipt["failed_symbols"]:
            _observe_receipt_fault(issues, symbol, "staging", receipt)
        for symbol in receipt["withheld_symbols"]:
            _observe_receipt_fault(issues, symbol, "withheld_window_regression", receipt)

    open_issues: list[dict] = []
    closed = 0
    for issue in issues.values():
        symbol = issue["symbol"]
        closer = None
        for receipt in receipts:
            if receipt["result"] not in ("committed", "noop"):
                continue
            if (
                symbol not in receipt["validated_symbols"]
                or symbol in receipt["failed_symbols"]
                or symbol in receipt["withheld_symbols"]
            ):
                continue  # not validated, or a contradictory scope — never a closer
            if receipt["manifest"] is None or symbol not in receipt["manifest"]:
                continue  # unverified manifest, or it does not carry the symbol
            if receipt["started"] is None or issue["last_seen"] is None:
                continue
            if receipt["started"] < issue["last_seen"]:
                continue  # a delayed older success cannot erase a later-seen failure
            closer = receipt
            break
        if closer is None:
            open_issues.append(issue)
        else:
            closed += 1

    # Legacy unscoped counters stay visible as UNKNOWN forever: the counter has
    # no symbol scope and no later run — full or targeted — can establish which
    # symbols it meant. Nothing here ever clears one; a same-run id match would
    # only pretend to (run ids are inherited, not invocation-unique).
    legacy_positives = ledger.query(
        "select name, value, measured_at from measurements "
        "where name in ('silver_failed', 'silver_window_regressions') and value > 0 "
        "qualify row_number() over (partition by name order by measured_at desc) = 1"
    )
    uncleared_legacy = []
    for row in legacy_positives:
        try:
            count = int(row["value"])
        except (TypeError, ValueError):
            continue
        if count > 0:
            uncleared_legacy.append(row)

    proven = any(receipt["result"] in ("committed", "noop") and receipt["manifest"] is not None for receipt in receipts)
    if open_issues:
        verdict = Verdict.WARN
    elif uncleared_legacy or malformed or receipts_malformed or not proven:
        # UNKNOWN, not proof healthy: an unscoped positive can never be proven
        # cleared, undecodable rows can hide a fault, and without one VERIFIED
        # committed/noop receipt nothing shows a symbol ever got healthy —
        # attempt_only rows and unverifiable manifests prove nothing.
        verdict = Verdict.UNKNOWN
    else:
        verdict = Verdict.OK

    receipt_failures = sum(issue["receipt_failures"] for issue in issues.values())
    counts = (
        f"{len(open_issues)} unresolved, {closed} resolved by verified receipt, "
        f"{len(facts)} evidence facts, {receipt_failures} receipt-listed failures"
    )
    if foreign:
        counts += f", {foreign} for other roots"
    if malformed or receipts_malformed:
        counts += f", {malformed + receipts_malformed} undecodable"
    lines = [f"Silver symbol faults: {counts}"]
    if not issues and not receipts:
        lines.append("  no symbol-level fault evidence or rebuild receipts on record for these roots")
    for row in sorted(uncleared_legacy, key=lambda item: item["name"]):
        lines.append(
            f"  legacy unscoped {row['name']}={int(row['value'])} last positive {row['measured_at']} —"
            " symbol scope unknown; never auto-cleared by a later run"
        )
    for issue in sorted(open_issues, key=lambda i: (i["symbol"], i["stage"], i["classification"]))[
        :_SILVER_FAULTS_LIMIT
    ]:
        lines.append(
            f"  {issue['symbol']} {issue['stage']} {issue['classification']} scope={issue['scope']}"
            f" facts={issue['evidence_facts']} receipt_failures={issue['receipt_failures']}"
            f" last_seen={issue['last_seen']}"
        )
        context = []
        bounds = issue["bounds"]
        if isinstance(bounds, dict) and bounds.get("earliest") and bounds.get("latest"):
            context.append(f"input_bounds={bounds['earliest']}..{bounds['latest']} (context, not failing sessions)")
        source = issue["input"]
        if isinstance(source, dict) and isinstance(source.get("path"), str):
            sha = source.get("sha256")
            context.append(f"input={source['path']}" + (f"@{sha[:12]}" if isinstance(sha, str) else ""))
        if issue["evidence_refs"]:
            extra = f"+{len(issue['evidence_refs']) - 1}" if len(issue["evidence_refs"]) > 1 else ""
            context.append(f"evidence={issue['evidence_refs'][-1][:12]}{extra}")
        if issue["run"]:
            context.append(f"run={issue['run']}")
        if context:
            lines.append("    " + " ".join(context))
        if issue["detail"]:
            lines.append(f"    detail: {issue['detail'][:200]}")
        artifacts = issue["baseline_artifacts"]
        if isinstance(artifacts, list) and artifacts:
            refs = ", ".join(
                f"{item['path']}@{str(item.get('sha256'))[:8]}"
                for item in artifacts[:3]
                if isinstance(item, dict) and isinstance(item.get("path"), str)
            )
            lines.append(
                f"    baseline rev={issue['baseline_revision']} artifacts=[{refs}] (references, not proof bytes exist)"
            )
        elif _is_int(issue["baseline_revision"]):
            lines.append(f"    baseline rev={issue['baseline_revision']} (artifact references unavailable)")
        else:
            lines.append("    last-valid baseline: UNKNOWN")
        if issue["stage"] == "withheld_window_regression":
            action = (
                "review the withheld regression against the baseline above; repair the input and rerun — "
                "--allow-window-regression adopts the shortening and needs separate evidence/authorization"
            )
        else:
            action = (
                "repair the staged input, then python scripts/livewire_store.py rebuild-silver "
                f"--tickers {shlex.quote(issue['symbol'])}"
            )
        lines.append(f"    next: {action}")
        lines.append(
            "    closes: a rebuild-silver receipt on these roots lists "
            f"{issue['symbol']} in validated_symbols and its verified manifest"
        )
    if len(open_issues) > _SILVER_FAULTS_LIMIT:
        lines.append(f"  …and {len(open_issues) - _SILVER_FAULTS_LIMIT} more — full detail: {_SILVER_FAULTS_QUERY}")

    # The page key carries issue identity only — timestamps, run ids and
    # malformed counts change daily and would re-page an unchanged fault.
    if open_issues or uncleared_legacy:
        key = "silver-symbol-faults:" + "|".join(
            sorted(f"{i['symbol']}|{i['stage']}|{i['classification']}|{i['scope']}" for i in open_issues)
        )
        key += "".join(
            f"|legacy:{row['name']}={int(row['value'])}" for row in sorted(uncleared_legacy, key=lambda r: r["name"])
        )
    else:
        key = "silver-symbol-faults:" + ("uncertain" if verdict is Verdict.UNKNOWN else "none")
    return Section("Silver symbol faults", verdict, lines, fix=_SILVER_FIX, notification_key=key)


def _filesystem_id(path: Path) -> int:
    """st_dev of the filesystem holding an existing, already-resolved path."""
    return path.stat().st_dev


def _disk_targets(data_lake: Path, warehouse: Path | None) -> list[tuple[str, Path]]:
    """Every configured write destination, one entry per logical target.

    Resolved against the env the same way the writers do — MDW_SILVER_DIR,
    log/cursor dirs, LW_LEDGER_ROOT, the local catalog path and the temp dir
    are all independent destinations, not children of whatever the caller's
    log_dir.parent happens to be. Nothing here may create a path.
    """
    from clients.duckdb_catalog import default_database

    targets = [
        ("lake", data_lake),
        ("bronze", data_lake / "bronze"),
        ("lake-catalog", data_lake / "catalog"),
        ("silver", _configured_silver_path(data_lake)),
        ("logs", default_log_dir()),
        ("cursors", cursor_dir()),
        ("ledger", ledger.ledger_root()),
        ("local-catalog", default_database().parent),
        ("staging-tmp", Path(tempfile.gettempdir())),
    ]
    if warehouse is not None:
        # Internal disk: releases and anything the env resolvers did not cover.
        targets.append(("warehouse", warehouse))
        # The flat-file writers root at <warehouse>/data-lake regardless of the
        # configured lake, and raw/massive may itself be a child symlink.
        targets.append(("raw-massive", warehouse / "data-lake" / "raw" / "massive"))
    return targets


def _disk_section(data_lake: Path, warehouse: Path | None = None) -> Section:
    """Report every distinct volume the warehouse depends on.

    `data-lake` is a symlink to an external volume, so measuring it alone read
    "6752.4 GiB free" every night while the internal volume holding releases,
    logs, cursors and the venv sat below its own reserve, unreported. One
    symlink silently swapped the monitored object.

    Every real destination is resolved first — a lake child like `raw/massive`
    or `silver` may itself be a symlink onto a third volume — and targets are
    deduplicated on st_dev, the physical filesystem, not the usage triple: two
    targets on one disk collapse to one line; the same numbers on two disks do
    not. A missing destination (or a dangling configured link) is UNKNOWN, not
    its ancestor's free space — the parent may live on the wrong disk. Read
    field by field rather than `tuple(usage)` so any object exposing
    total/used/free works, which is what the existing tripwire test patches in.
    """
    try:
        targets = _disk_targets(data_lake, warehouse)
    except Exception as exc:  # a resolver must never kill the section
        return Section("Disk", Verdict.UNKNOWN, [f"Disk: could not resolve destinations — {exc}"], fix="df -h")

    volumes: dict[int, dict] = {}
    unknown: list[str] = []
    for label, path in targets:
        try:
            # Resolution itself can raise OSError on a hostile filesystem — it
            # belongs inside the guard with the two calls that follow.
            resolved = resolve_capacity_target(path, allow_missing=False)
            if resolved is None:
                unknown.append(label)
                continue
            usage = shutil.disk_usage(resolved)
            fs = _filesystem_id(resolved)
        except OSError:
            # A volume that vanished or rejected resolution is UNKNOWN for that
            # target only — other destinations still report their own.
            unknown.append(label)
            continue
        # A filesystem reporting total=0 tells us nothing, and dividing by it
        # would raise out of _disk_section — killing the WHOLE digest, which
        # this module's docstring promises never happens on a missing input.
        if not usage.total:
            unknown.append(label)
            continue
        if fs in volumes:
            volumes[fs]["labels"].append(label)
        else:
            volumes[fs] = {"labels": [label], "usage": usage}

    if not volumes and not unknown:
        return Section("Disk", Verdict.UNKNOWN, ["Disk: (unavailable)"], fix="df -h")

    # Label only once there is something to distinguish. A single-filesystem
    # deployment keeps reading plain "Disk:", which is also what it means.
    lines: list[str] = []
    tightest = None
    # Read at call time, never at import: an import-time read is fixed for the
    # life of the process, so an operator's one-run LW_DECLARED_ override (and
    # any test that sets it after importing this module) is silently ignored.
    min_free_gb = constants.declared("flatfile_min_free_gb")
    for entry in sorted(volumes.values(), key=lambda item: item["usage"].free):
        total, used, free = entry["usage"].total, entry["usage"].used, entry["usage"].free
        free_gib = free / _GIB
        tightest = free_gib if tightest is None else min(tightest, free_gib)
        suffix = "" if len(volumes) == 1 and not unknown else f" [{'+'.join(entry['labels'])}]"
        line = f"Disk{suffix}: {free_gib:.1f} GiB free ({100.0 * used / total:.0f}% used)"
        if free_gib < 2 * min_free_gb:
            line += f"  ⚠ raw retention deferred — free space under {2 * min_free_gb:.0f} GiB"
        lines.append(line)
    for label in unknown:
        lines.append(f"Disk [{label}]: UNKNOWN — destination missing or dangling; not its parent's free space")
    # The existing numbers, newly graded. Today the digest prints a ⚠ below 2×
    # the reserve and says nothing at all below 1× — the more serious state was
    # the quieter one. An unmeasurable destination can never green the check.
    if tightest is not None and tightest < min_free_gb:
        verdict, fix = Verdict.BAD, "python scripts/livewire_ops.py housekeeping"
    elif tightest is not None and tightest < 2 * min_free_gb:
        verdict, fix = Verdict.WARN, "python scripts/livewire_ops.py housekeeping"
    else:
        verdict, fix = Verdict.OK, None
    if unknown:
        verdict = max(verdict, Verdict.UNKNOWN)
        fix = fix or "df -h   # then check the named destination's mount or symlink"
    return Section("Disk", verdict, lines, fix=fix)


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


def _latest_catalog_receipt(lake_root: str, database: Path | None) -> dict | None:
    """The newest duckdb-build receipt, only when it verifiably describes THIS catalog.

    The newest executions row is the latest — scanning past an undecodable or
    foreign row would present an older success as the latest state. A receipt
    attaches to this section only when its resolved ``data_lake_root`` equals
    this lake AND its local/lake destinations equal this deployment's actual
    configured paths; a receipt from another warehouse cannot describe this
    database. Both ``committed`` and ``copy_failed`` mean the local file
    committed — the receipt carries no ``local.result``.
    """
    from clients.duckdb_catalog import default_database, lake_snapshot_path

    rows = ledger.query(
        # Completion order: an earlier-started build that finishes last is the
        # latest fact, and a missing ended falls back to started, never zero.
        "select script, receipt_json from executions where script = 'duckdb-build' "
        "order by coalesce(ended, started) desc, started desc limit 1"
    )
    if not rows:
        return None
    receipt = _decode_executions_receipt(rows[0], script="duckdb-build", kind="catalog_publication")
    if receipt is None or receipt.get("result") not in ("committed", "copy_failed"):
        return None
    if not isinstance(receipt.get("data_lake_root"), str) or receipt["data_lake_root"] != lake_root:
        return None
    local, lake = receipt.get("local"), receipt.get("lake")
    if not isinstance(local, dict) or not isinstance(lake, dict):
        return None
    if not isinstance(local.get("path"), str) or not isinstance(lake.get("path"), str):
        return None
    if not (_opt_str(local.get("sha256")) and _opt_str(lake.get("expected_sha256"))):
        return None
    if not (_opt_str(lake.get("result")) and _opt_str(lake.get("error"))):
        return None
    if local["path"] != str((database or default_database()).expanduser().resolve()):
        return None
    if lake["path"] != str(lake_snapshot_path(Path(lake_root)).expanduser().resolve()):
        return None
    return receipt


def _catalog_receipt_summary(receipt: dict | None) -> str:
    """One line of local-vs-lake outcome — never a claim Apex consumed the target."""
    if receipt is None:
        return "none describing this catalog — publication linkage UNKNOWN"
    local, lake = receipt["local"], receipt["lake"]
    sha = local.get("sha256")
    expected = lake.get("expected_sha256")
    line = (
        f"{receipt['result']}: local committed {local['path']}"
        + (f" sha={sha[:12]}" if isinstance(sha, str) else "")
        + f"; lake copy {lake.get('result', 'unknown')} → {lake['path']}"
        + (f" expected_sha={expected[:12]}" if isinstance(expected, str) else "")
    )
    if lake.get("error"):
        line += f" ({lake['error']})"
    return line + " — records the local commit and copy outcome, not Apex consumption"


#: Deliberately the same NUMBER as _COVERAGE_STALE_DAYS and deliberately a
#: separate constant: that one counts calendar days, this one counts trading
#: sessions. Sharing the name would make a future edit to one silently change
#: the other's meaning. The value itself is a starting guess, not a
#: measurement — correct it the first time it misfires.
_CATALOG_STALE_SESSIONS = 3

#: View -> the lane that WRITES it. The catalog only reports; a stale view means
#: its writer is behind, so the fix has to name the writer. Views are
#: `bronze_<asset_class>_1d` over `duckdb_catalog._DAILY_ASSET_CLASSES` plus
#: `silver_equity_1d`. These are the production catalog's expected views, even
#: when an earlier partial build omitted one; never infer that set from rows.
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
    from clients.trading_calendar import is_trading_day, previous_trading_day

    cursor = target if is_trading_day(target) else previous_trading_day(target)
    count = 0
    while cursor > newest and count < limit:
        cursor = previous_trading_day(cursor)
        count += 1
    return count


def _duckdb_section(target: date, database: Path | None = None, data_lake: Path | None = None) -> Section:
    """Grade the DuckDB coverage table's own staleness.

    The table is refreshed by the last phase of `daily-backfill`. When that
    orchestrator stopped running, the table quietly froze — on 2026-08-10 it
    still read 2026-08-07 — and nothing anywhere said so. Catalog staleness is
    a symptom of an upstream lane, which is exactly why it belongs here. The
    publication receipt line is separate from freshness: it reports the local
    commit and lake copy outcome of the latest build, never whether Apex read
    the current target.
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

    lake_root = str((data_lake or data_lake_dir()).expanduser().resolve())
    receipt = _latest_catalog_receipt(lake_root, database)
    receipt_line = f"  latest build receipt: {_catalog_receipt_summary(receipt)}"
    # A copy_failed receipt is a failed publication of THIS catalog — freshness
    # of the local table cannot green a copy that never reached the lake.
    copy_failed = receipt is not None and receipt["result"] == "copy_failed"

    missing = sorted(name for name in _CATALOG_LANE_FIX if name not in headline)
    empty = sorted(name for name, (count, last) in headline.items() if count <= 0 or last is None)
    if headline and (missing or empty):
        return Section(
            "DuckDB catalog",
            Verdict.BAD,
            [
                "DuckDB catalog: incomplete",
                f"  missing views: {', '.join(missing) or 'none'}",
                f"  empty or undated views: {', '.join(empty) or 'none'}",
                receipt_line,
            ],
            fix="python scripts/livewire_store.py duckdb build   # repair any reported source error before retrying",
        )

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
    lines.append(receipt_line)
    # The fix must name the lane that OWNS the laggard, not the catalog. This
    # docstring already says catalog staleness is a symptom of an upstream lane,
    # and then every branch prescribed `duckdb build` — which on 2026-08-16
    # meant "rebuild the table" for `bronze_rates_1d last=2026-08-13`, where the
    # table was reporting correctly and FRED was the one behind. Rebuilding
    # would reproduce the same date and the reader would conclude the surface
    # lies. `duckdb build` stays the fallback: when the owner is unknown, a
    # stale table really is the only thing we can name.
    fix = _CATALOG_LANE_FIX.get(laggard, "python scripts/livewire_store.py duckdb build")
    if copy_failed:
        return Section(
            "DuckDB catalog",
            Verdict.BAD,
            lines,
            fix="python scripts/livewire_store.py duckdb build   # local commit already landed; rerun retries the lake copy",
        )
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
    now: datetime | None = None,
) -> list[Section]:
    """Assess every cheap signal without scanning bar parquet."""
    today = run_date.isoformat()
    try:
        closed_run = _last_run_id(today, closed=True)
        open_run = _last_run_id(today, closed=False)
        # Not closure-gated: an intraday phase that lost the lock is recorded
        # while the run is still in flight, and that is when it needs surfacing.
        intraday_run = _last_run_id(today, closed=False, job="intraday-catchup")
    except Exception:
        closed_run = open_run = intraday_run = ""
    params = {
        "today": today,
        "run": closed_run,
        "open_run": open_run,
        "intraday_run": intraday_run,
        "main_sha": main_sha or "__missing__",
        "now": (now or datetime.now(UTC)).strftime("%Y-%m-%d %H:%M:%S"),
        "ib_slack_days": str(IB_LANE_SLACK_DAYS),
        "coverage_threshold": str(constants.declared("coverage_alert_threshold")),
        "coverage_stale_days": str(_COVERAGE_STALE_DAYS),
    }
    return [
        _safe("launchd jobs", lambda: _launchd_section(runner=runner)),
        *[_safe(name, lambda n=name, sql=sql: run_check(n, sql, params)) for name, sql in CHECKS],
        _safe("Silver publication", lambda: _silver_publication_section(data_lake)),
        _safe("Silver symbol faults", lambda: _silver_faults_section(data_lake)),
        _safe("DuckDB catalog", lambda: _duckdb_section(run_date, database, data_lake)),
        # The internal volume is the resolved warehouse root — not
        # log_dir.parent, which points wherever an overridden log dir lives.
        _safe("Disk", lambda: _disk_section(data_lake, warehouse_dir())),
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
