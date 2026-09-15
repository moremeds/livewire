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

## Tribunal findings (2026-09-15)

Three defects in the wiring above, found on review of this branch.

- **Future ex-dates converted at today's FX.** `_fx_bar` serves the latest bar
  within `_FX_STALE_DAYS` (7), so a dividend announced with an ex-date up to a
  week ahead was converted at today's close; afterwards its currency matched the
  equity's, so it left `foreign_currency_dividends` and was never recomputed at
  the real ex-date. A row with `ex_date >= now.date()` is now skipped with
  reason `ex_date_pending` (strictly `>=`: the ex-date session's FX close is
  only final by the next morning's run).
- **`--preset` filed as `scope='all'`.** The scope test read `args.tickers`
  only, but `_resolve_tickers` also restricts on `args.preset`. A preset lane
  run therefore filed a partial count as the whole lake's answer — the same
  failure the `--tickers` fix above was written for. Scope is `all` only when
  both are falsy.
- **Resume starvation (corrects the "checked and left alone" note above).** The
  conversion ran *after* the resume loop. A resumed tail that completes opens a
  second cycle in the same invocation, so on every night where a full pass
  exceeds the lane budget the invocation was SIGKILLed inside that second cycle
  and the conversion never ran — the failure mode repeats nightly rather than
  being a one-off. The conversion now runs inside the loop, once per
  invocation, immediately after the `complete` bookkeeping and before the
  re-open; `dry_run` still skips it.
- **Regression from that relocation, caught on the second review pass.** Running
  the conversion *once*, before the second cycle, left that cycle's own
  dividends unconverted while the `dividend_currency_mismatch` it had already
  filed read 0 — `status` OK on a night Silver fails on those symbols. It now
  runs at the end of every cycle, each under its own `<lane>-dividend-fx[-n]`
  run id, since two open/close pairs sharing one `run_id` are one run to every
  `group by run_id` reader. The two-cycle test used a stub store and could not
  see it; the regression test uses the real `CorporateActionStore`.
- **The zero needs the lane to have finished.** Even converting per cycle, a
  lane SIGKILLed at its budget after cycle one leaves cycle two's dividends
  unconverted behind cycle one's `dividend_currency_mismatch` 0; the `status`
  check now reads UNKNOWN unless today's `lane_results` row for
  `corporate-actions` says `done` (no row today is a manual run, graded as
  before).
- **A swallowed conversion failure was invisible.** The lane wrapper catches
  everything (it must not fail the lane), so a cycle-two conversion that raised
  after the evidence loop left its `runs` row open and cycle one's zero grading
  OK. `convert_dividend_currency` now closes its row `exit_code=1 verdict=FAILED`
  on any exception from its whole body — not just the conversion loop — and
  `status` grades an open or non-zero `dividend-fx` run today UNKNOWN.
- **A skip is not a leftover.** `remaining = detected - converted` counted the
  `ex_date_pending` rows the first fix introduced, so every announced foreign
  dividend would have WARNed nightly until it went ex — and the runbook's
  remedy re-skips exactly those rows. Pending ex-dates are excluded from
  `dividend_currency_mismatch`; `no_fx_bar` still counts.
- **One signal, not three gates.** Gating on the `dividend-fx` run row leaked
  three ways: a failure before the row is opened, a swallowed measurement write
  that still closed the row OK, and a later `--tickers` repair becoming "today's
  newest run". The conversion now files one `dividend_fx_error` measurement on
  any failure and its own counts are written `strict=True`, so `status` reads a
  single rule — the newest whole-scope row of the day — plus the lane-done gate
  a SIGKILL cannot satisfy.
- **Mark non-USD, don't rescan (user, 2026-09-15).** "分红用的币种又不会变，mark
  non-USD 的就行了" — the nightly conversion was opening every one of the ~13.3K
  CA-store files to rediscover the few dozen foreign-currency symbols. The lane
  now marks a ticker while it reconciles it — against that equity's own
  currency, since a USD dividend on a BMD-denominated equity needs converting
  too — and carries leftovers in `repairs/dividend_fx/pending.json`;
  `tickers=[]` means an empty scope instead of falling back to the full scan,
  which is now only the manual sub-command's bootstrap/audit path. Every fetch
  returns the symbol's full dividend history, so a lost mark is repaired by the next successful fetch of that symbol,
  not a permanent miss.
