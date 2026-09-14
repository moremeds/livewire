# A scheduled job that never ran, and a repair wired to nothing

Rule: a command shipped with a schedule is verified with the exact argv the
plist produces, against the real argparse; and a repair the lake needs nightly
is called by a lane, not left as an operator sub-command. A detector that
grades the newest measurement must require that measurement to be from today.

Observed on `moremeds-Mini`, 2026-09-14, for PR #128 (merged 2026-09-13).

Three defects, one root: PR #128 added surfaces and never exercised them end to
end in the shape production runs them.

1. `com.livewire.membership-sync` had never succeeded. Every weekday since
   install it died at argparse:

   ```
   livewire_ingest.py membership-sync: error: unrecognized arguments: ndx100 djia
   ```

   The plist passes the panels as one space-separated list
   (`--index sp500 ndx100 djia`); the flag was declared `action="append"`,
   which takes exactly one value. Cost: zero membership events synced for the
   life of the job — the PIT panels were as static as the grok snapshots the
   job was built to replace. The installed plist had also drifted from the
   template and omits `r2k-proxy`; the template now names all four, and
   reinstalling it from the release is an operator action this fix cannot
   perform.

2. `corporate-actions convert-dividend-currency` was wired to no scheduled
   lane. Monday's lane emitted no `dividend_fx_converted`/`_skipped`/
   `dividend_currency_mismatch` measurement at all, and Silver failed ~30
   symbols on "dividend currency does not match bronze currency". The
   conversion now runs at the end of the corporate-actions lane — it repairs
   exactly what that pass reconciled — under its own `runs` row and with no
   ability to change the lane's exit code.

3. The `status` check "Foreign-currency dividends" read OK through all of it:
   it graded the newest `dividend_currency_mismatch` row, which was Sunday's
   manual run (0). A detector with no output is dead, not healthy; the check
   now reads only today's row and is UNKNOWN without one.

Tests: `tests/test_livewire_entrypoints.py::test_the_scheduled_membership_sync_argv_parses_and_covers_every_index`,
`tests/test_sync_corporate_actions.py::test_the_lane_converts_foreign_currency_dividends_and_emits_its_measurements`,
`tests/test_status.py::test_foreign_currency_dividends_without_a_measurement_today_is_unknown`.

## Follow-on: what wiring the repair into a lane moved

Calling the conversion from the nightly lane changed its scale and its
audience, and two things that were free for an operator sub-command were not
free for a lane.

- **Cost.** `_equity_currency` re-read and re-deserialized the entire
  `security_master` identity log *per symbol*. One symbol on the operator path;
  ~15k full parquet reads on the exFAT lake per nightly pass, inside a
  `LANE_BUDGET_S` the lane is SIGKILLed at. The log is read once per pass now
  (`_equity_currency_resolver`).
- **Scope.** `dividend_currency_mismatch` was always filed `scope='all'`. With
  `status` grading today's newest row, an afternoon `convert-dividend-currency
  --tickers ACR --apply` — the repair the runbook tells the operator to run —
  would file a one-symbol 0 as the whole lake's answer and erase the night's
  WARN. A `--tickers` pass now files `scope='subset'` and the check reads
  `scope='all'` only.

Checked and left alone: the conversion runs after the resume loop, so the
"finish the tail, then open a new cycle" invocation converts once, not twice;
the synthetic `<lane run id>-dividend-fx` run id is never matched by a prefix
or `LIKE` — every `runs`/`lane_results`/progress consumer keys on exact
`run_id` or on `job`, and `coverage.wait_for_upstream` waits only on
`daily-update`/`intraday-catchup`, so a dividend-fx row left open by a lane
budget kill cannot block coverage.
