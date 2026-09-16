# An interrupted run stayed open in the ledger forever

**Rule:** A run's terminal row is written by the process that owns it, so a
process that dies cannot write one. Something else must state that the run is
over: the next start of the same job on the same host closes its predecessors
with `verdict = 'ABANDONED'`. A reader must never have to infer "no close row
and no process, therefore interrupted" by hand.

**Incident / measurement:** 2026-09-16, host **macmini**.

The ledger is append-only: a run emits an entry row (`ended` NULL) and later a
terminal row with the same `run_id`, and every reader pairs them with
`group by run_id having max(ended) is null`. Nothing ever closed an entry row
on the run's behalf.

Four `security-master-sync` runs were stopped by hand during the identity
backfill while its pacing was being corrected, plus one `daily-update` from
2026-09-15 that ended without a close row. All six entry rows were still open:

```
daily-update        19 open rows of 36
intraday-catchup    14 open rows of 27
security-master-sync 6 open rows of 7
```

Two separate causes, and only one of them was a signal at all:

- `security_master_sync.sync` caught `except Exception` around its body.
  `KeyboardInterrupt` is a `BaseException`, so Ctrl-C skipped the close entirely
  — the run had a perfectly good chance to write its terminal row and did not.
  `membership_sync` had the same three copies of the same bug.
  `sync_runner`, `sync_corporate_actions` and `run_daily_update_job` already
  used `BaseException` and were unaffected.
- A SIGKILL, a panic, or a power loss can never write the row, whatever the
  except clause says.

`status` grades an open run WARN with a `running_minutes` count and has no
threshold that escalates it, so a run stuck open reads WARN indefinitely. The
monitor runbook carried a hand-written rule telling its reader to check `ps` and
decide for itself — documentation standing in for a fact the system declined to
record.

**Cost:** no data. Every stopped run's committed chunks were kept and skipped on
the next pass. What it cost was readability: six rows that look like work in
flight, in the one table every surface reads, plus a paragraph of runbook prose
teaching humans to work around it.

**Fix:** `ledger.open_run(run_row)` replaces the bare entry-row emit at every
site that opens a run. It first closes every still-open run of the same job on
the same host with `verdict = ledger.ABANDONED`, carrying **that run's own
`started`** — a row stamped with the closer's clock would sort ahead of a real
terminal row and win the verdict. It returns the abandoned ids so the caller
logs them. `status` maps `ABANDONED` to WARN, so it surfaces and never pages.

Self-healing by construction: if the abandoned process was in fact alive, its
own terminal row lands later and carries the later `ended`, which is what every
reader's `max(ended)` / `order by ended desc` already selects. A concurrent run
is therefore mislabelled at most until it finishes.

The three `membership_sync` sites and `security_master_sync` now catch
`BaseException`, so a Ctrl-C writes the terminal row before re-raising.

→ test: `tests/test_ledger.py::test_open_run_closes_a_predecessor_that_never_wrote_its_own_terminal_row`,
`::test_the_abandoned_row_keeps_the_abandoned_runs_own_start_time`,
`::test_a_late_real_close_row_outranks_the_abandoned_one`,
`::test_another_hosts_open_run_is_left_alone`
