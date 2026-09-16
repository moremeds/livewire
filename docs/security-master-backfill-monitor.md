# Security-master backfill: read-only monitor runbook

For the Grok bot, a scheduled Claude routine, or a person on the mini. Companion
to the operator procedure in `docs/runbook.md` § `security-master`. This file
only reads; the runbook is the one that runs things.

---

You are a READ-ONLY monitor for the one-off security-master identity backfill on
the production Mac mini (user `moremeds`). Every command below is local. Never
run `security-master sync`, `membership-sync reresolve`, or anything that writes
the lake; never kill a process; never `launchctl`. Report what you see.

## What the backfill is

`index_membership` holds `unresolved:<ticker>` placeholder events for historical
index members that were imported without a `security_id`. The backfill has two
steps, both manual, both idempotent:

1. `security-master sync` fetches each placeholder ticker's identity from
   Massive `/v3/reference/tickers` and appends intervals to
   `security_master/events.parquet`. Ledger job `security-master-sync`.
2. `membership-sync reresolve --index <one>` rewrites the placeholders onto the
   resolved `security_id`. Ledger job `membership-reresolve`, one index per run.

Hard ordering rule: both steps for an index must finish before the next weekday
01:00Z `com.livewire.membership-sync`; if a nightly add lands in between, the
reresolve guard fails closed and step 2 must wait for another step 1. That is a
schedule fact, not a defect.

## Commands, in order

```bash
date -u
cd $(readlink ~/market-warehouse/current) && source ~/market-warehouse/.venv/bin/activate && set -a && source ~/market-warehouse/.env && set +a
ps -eo pid,etime,command | grep -E 'security-master sync|membership-sync reresolve' | grep -v grep
python scripts/livewire_ops.py ledger query "select run_id, started, ended, exit_code, verdict from runs where job in ('security-master-sync','membership-reresolve') order by started desc limit 10"
python - <<'EOF'
import pyarrow.parquet as pq
print("security_master rows:", pq.read_metadata("/Users/moremeds/market-warehouse/data-lake/security_master/events.parquet").num_rows)
EOF
python scripts/livewire_ops.py ledger query "select name, scope, value from measurements where run_id = (select run_id from runs where job='security-master-sync' order by started desc limit 1) and name not like 'identity_probe_empty' order by name"
python scripts/livewire_ops.py ledger query "select scope, value, measured_at from measurements where name='membership_unresolved' and measured_at > now() - interval 2 day order by measured_at desc, scope"
python scripts/livewire_ops.py status | grep -A4 'Unresolved memberships'
ls -la ~/market-warehouse/logs/security-master-backfill/ 2>/dev/null && tail -3 ~/market-warehouse/logs/security-master-backfill/t2-progress.md 2>/dev/null
```

## How to read it

**Is it running?** A `security-master sync` process in `ps` plus a
`security-master-sync` start row with no matching close row (same `run_id`,
`ended` set) means in flight. You no longer have to infer an interrupted run
from "no close row and no process": the next start of the job closes its
predecessors with `verdict = 'ABANDONED'`, so report that verdict as
"interrupted", never as "failed". A row still open with no process is a run
interrupted since the last start — say so and move on, it is not an error to
fix, and the next run resumes by skipping covered tickers.

**Progress while it runs.** The script prints nothing until the end. Two numbers
move, once per 50-ticker chunk:

- `security_master rows` (the parquet row count) grows.
- `select count(*) from measurements where run_id='<run>' and name='identity_probe_empty'` grows.

Do not measure progress from file counts under `raw/shepherd/sha256`: the
corporate-actions lane writes the same store and drowns the signal (measured
2026-09-16: 784 files in 43 s, almost none from the sync).

**Rate.** Requests are paced by `massive_requests_per_minute/reference`
(`clients/constants.py`, overridable per run with
`LW_DECLARED_MASSIVE_REQUESTS_PER_MINUTE_REFERENCE`). Each ticker costs about
3.4 requests (2 list calls plus one `date=` probe per uncovered membership
date; the endpoint returns no `list_date`, so probes always run). At the
declared 600/min the sleep is 0.1 s against a ~0.6 s round-trip, so the run is
latency-bound and moves about 24 tickers/min single-threaded. Expect roughly
2.5 hours for a full pass over the 3,124 distinct placeholder tickers (that
is fewer than the summed per-index `membership_unresolved`, 3,455, because a
ticker in two indexes is fetched once). A rate far below that is the visible
sign of trouble: a 429 costs one 60 s backoff and one retry per ticker, then
counts in `identity_fetch_failed`, and the client does not log 429s separately.

**When a sync ends**, the measurements query gives the verdict:

| measurement                    | meaning                                               | what to say                                                                                  |
| ------------------------------ | ----------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `identity_tickers_requested`   | tickers fetched this run                              | the denominator                                                                              |
| `identity_events_appended`     | rows added to the master                              | progress                                                                                     |
| `identity_fetch_failed`        | tickers whose fetch failed (HTTP error or double 429) | any > 0 makes the run exit 1; name the tickers from the per-ticker rows below and quote them |
| `identity_no_start`            | identity found but no usable start date               | expected for old listings; stays unresolved                                                  |
| `identity_unknown_to_provider` | Massive has no record of the ticker                   | expected for pre-2003 names (Massive's earliest delisting is 2003-09-11)                     |
| `identity_conflict`            | same FIGIs, overlapping dates                         | a real finding; quote the count                                                              |
| `identity_collisions`          | append refused by the master's revision check         | should be 0; quote if not                                                                    |
| `identity_candidate`           | candidate-grade rows (r2k-proxy is always candidate)  | informational                                                                                |

Exit 1 with `identity_fetch_failed > 0` and everything else sane is a partial
success, not a page. Name the tickers rather than the count:

```bash
python scripts/livewire_ops.py ledger query "select scope, unit from measurements where name = 'identity_fetch_failed_ticker' and run_id = (select run_id from runs where job='security-master-sync' order by started desc limit 1) order by scope"
```

`scope` is `<ticker>:<http status>` and `unit` says whether that was the first
attempt or the retry after a 429 backoff. A rerun refetches exactly these;
everything already covered is skipped.

**After the reresolves**, `membership_unresolved` per index is the acceptance
signal. Compare against the pre-backfill baseline of 2026-09-16 01:00Z:
sp500 1209, ndx100 299, djia 61, r2k-proxy 1886. Events dated before 2003-01-01
are expected to stay unresolved in this phase. `membership_reresolve_conflict`

> 0 is a finding; quote it.

## Report format

1. **一句话**: backfill 状态（未开始 / 在跑 / 完成 / 中断），和是否赶得上下一个 01:00Z。
2. **进度**: master 行数、probe 行数、run 开始时间，以及按此推算的完成时刻。没有 run 就写"无"。
3. **上一次 sync 的 measurements**: 表格原样粘贴，`identity_fetch_failed` 和 `identity_conflict` 非零单独一句。
4. **Unresolved memberships**: 每个指数 before → after。reresolve 还没跑就说明还没跑。
5. **需要人决定的事**: 最多两条，附证据行。没有就"无"。

Rules: quote, do not summarise; a run with no close row and no process is
"interrupted", never "failed"; a detector with no output is UNKNOWN, not
healthy; never propose or run a fix.
