# Livewire

Local-first market data warehouse for quantitative research. The Parquet lake is
the system of record, DuckDB the query layer, ClickHouse optional for
benchmarking. Repo `livewire/`; data tree `~/market-warehouse/` (rebranded from
market-data-warehouse 2026-05-17).

This file is the rulebook and it is short on purpose. Every rule is one line and
points at the test that enforces it (`→ test:`) and/or the incident that earned
it (`→ pm:<name>` = `docs/postmortems/<name>.md`). **Incident narratives live in
`docs/postmortems/`; operator commands live in `docs/runbook.md`.** When you
learn something new here, add one post-mortem file and one line — never a
paragraph. This file doubled (497 → 1344 lines) in six weeks of `fix(...)` PRs
and the documented traps were still re-tripped; prose does not run at 03:00.

## Where things are

```
livewire/                       # git repo
├── clients/                    # client modules; clients/__init__.py is the authoritative export list
├── livewire_scripts/           # implementations behind the entrypoints (~60 modules)
├── scripts/                    # 4 entrypoints: livewire_ingest.py / livewire_quality.py / livewire_ops.py / livewire_store.py
├── presets/                    # universe definitions (sp500, ndx100-*, r2k-*, futures-*, fx-pairs, …)
├── registry/gaps.json          # the coverage denominator rows (see "The one contract")
├── livewire_node/              # nodemailer alert + digest senders (tests: npm run test:alerts)
├── launchd/                    # *.plist.example templates for the 6 scheduled jobs
├── tests/                      # pytest; 95% coverage gate (clients/ib_client.py exempt)
└── docs/
    ├── postmortems/            # one file per incident: rule + what it cost + date (55 as of 2026-09-06)
    ├── runbook.md              # every operator command, flag and env var, by task
    ├── superpowers/specs/      # designs; 2026-09-02-livewire-ledger-design.md is current
    └── audits/                 # dated read-only findings

~/market-warehouse/             # data tree (scripts/setup_market_warehouse.sh)
├── data-lake -> /Volumes/DATA_LAKE   # exFAT external volume; a cold cache is the normal morning state
│   ├── bronze/asset_class=<equity|futures|rates|volatility|fx|cmdty|corporate_action>/symbol=<S>/{1d,1m,5m,30m,1h}.parquet
│   ├── bronze-delisted/        # archived symbols; NOT authoritative for the denominator
│   ├── silver/                 # adjusted daily + factor intervals; revisions/current.json is the commit record
│   ├── raw/massive/…           # provider flat files; below the rolling GET floor they can never be refetched
│   ├── repairs/                # triage verdicts, unresolved ledger, rollback backups — protected by name
│   └── quarantine/<stamp>/     # corrupt per-symbol parquet moved aside by the publisher
├── releases/<sha>/ + current   # immutable release artifacts; scheduled jobs run `current`
├── cursors/  logs/  .env       # resume state, job logs, credentials (a release carries no .env)
├── locks/lake-io.lock          # the one lake-io flock; internal disk, never in the lake
└── .venv/                      # runtime venv for launchd jobs (dev and CI use `uv run`)
```

**Two hosts.** Production is the Mac mini (`ssh macmini`, user `moremeds`): IB
Gateway, launchd jobs, the real lake and logs live there. Development happens on
the MacBook, whose lake is a different, partial copy. A fact about production is
verified on the mini or it is not verified.

## Architecture in six lines

- Bronze = normalized provider rows at raw prices, per-ticker parquet published `temp → validate → os.replace()`, serialized per path with `fcntl.flock` (`clients/parquet_io.py`).
- Silver = fully back-adjusted daily bars + factor intervals, derived from bronze equity and the corporate-action store — both Massive-backed. Bronze is read-only to it; IB is never an input.
- DuckDB reads parquet in place; its only durable artifact is a small coverage table. It is never a second store.
- Providers: equity daily IB → Massive fallback; equity intraday Massive flat files only; futures/cmdty daily and volatility intraday IB; CBOE vol indices CBOE API; rates FRED; fx/DXY Yahoo (+ Massive intraday).
- Six launchd jobs on the mini: daily-update 05:00Z → intraday-catchup 10:00Z → watchdog 10:30Z → coverage 11:00Z (no timeout) → release-promote; universe-refresh weekly, from the repo.
- Apex is the consumer. It resolves silver by path and never reads the manifest; a corrupt symbol renders as a plausible chart while a missing one fails closed (HTTP 500). Correctness outranks coverage.

## The one contract

**"Expected" is defined once, off-disk. Everything else is a comparison against it.**

```
expected = registry/gaps.json × presets/*.json × trading calendar × timeframe × asset class
gap      = expected − actual
```

- The denominator never comes from globbing the lake; a symbol that never landed must be countable as missing. `coverage_report.py` used to glob, so BF.B and BK (both S&P 500 members, no parquet) were structurally invisible. → test: `tests/test_gap_engine.py` · pm:2026-09-01-gap-engine-disk-glob-denominator
- Every job writes its facts to the ledger (`<lake>/ledger/`) and every reader reads the ledger, never a log. → test: `tests/test_status.py` · spec `2026-09-02-livewire-ledger-design.md` §3
- A second detector is forbidden. New gap type = one registry row + one test; a row without a `test` is rejected by `load_registry`. Ten detector scripts (4,810 lines) preceded this rule. → test: `tests/test_gap_registry_contract.py`, `tests/test_gap_registry.py` · spec §1
- Gap classes emitted: `G3` nothing on disk, `G1` newest sessions missing, `G14` instrument left the tape. `G14` is exculpatory-only and fails closed three ways; a symbol absent from every session of the window is not a terminus. `G2`/`G13` are named and deliberately not emitted. → test: `tests/test_terminus.py`, `tests/test_gap_engine.py::test_interior_and_head_gaps_are_no_longer_emitted` · pm:2026-09-01-g14-terminus-fails-closed, pm:2026-09-01-g2-g13-not-emitted
- Tier follows the repair **source**, not severity: inside the Massive window a repair is unattended (A); below it only 2FA-gated IB can serve, so it is a decision (B). `heal_by_days` is `None` for sources with no rolling window. The Tier B queue depth is the measurement that decides whether an agent lane is worth building. → pm:2026-09-01-tier-follows-repair-source
- "No bar" and "no trade" are indistinguishable inside bar files. The only tie-breaker is the day's raw traded set (`_symbols.parquet`); when it is absent the answer is UNKNOWN, never "missing". (2026-08-28: the raw partition was not yet published at 11:00Z, the denominator fell back to the disk glob, and 14,795 symbols were reported missing at 1h — all false, and it triggered a whole-market republish.) → pm:2026-08-17-interior-gap-scan-measures-liquidity
- The denominator is XNYS-only (known blind spot); presets overlap and are deduplicated; a row resolving to no symbols fails the run; futures expiry is judged against the scan window, not `as_of`. → test: `tests/test_gap_registry.py`, `tests/test_gap_engine.py` · pm:2026-09-01-xnys-only-denominator, pm:2026-08-17-preset-overlap-duplicate-denominator, pm:2026-09-01-registry-row-resolving-to-no-symbols, pm:2026-09-01-futures-expiry-vs-scan-window (last one prose-only)
- The unresolved ledger (`<lake>/repairs/unresolved.json`) records permanently unsourceable `(symbol, session)` pairs with a reason so nothing is re-litigated nightly. → pm:2026-09-01-unresolved-ledger-key
- Phase 1 is read-only: the scan writes two artifacts and mutates nothing; the Tier A manifest is not yet fed to `shepherd_repair`. Nothing outside livewire cron ever writes the lake; an agent produces evidence and queue entries, never rows. `shepherd_repair` requires `source_evidence` (a `HashedRef`). → spec §9
- Silver publishes the longest silver-grade suffix of each symbol. Short and right beats long and wrong; a window that would start _later_ than the served revision is withheld. Holes are meant to ship as `INCOMPLETE_HISTORY` + `known_holes[]` (spec §7 — **designed, not yet implemented**; today silver still trims). → test: `tests/test_rebuild_silver.py` · pm:2026-07-18-window-regressions-withheld
- Every operating constant is one `DECLARED` key in `clients/constants.py`, emitted as `measurements(source='declared')` each run and compared against the 14-day p95 of `source='measured'`; a >2x drift is a `status` WARN. Override for one run with `LW_DECLARED_<KEY>`. → test: `tests/test_constants.py`, `tests/test_status.py`
- IB is a gate, never a source, for basis reconstruction: publish only what a post-last-split IB window confirms (`resolve-yahoo-basis --apply` requires `--ib-verify`). → pm:2026-07-18-unknown-price-basis-population

## Hard rules

### IB

- Connect to `127.0.0.1:4001` only. The LAN IP is TCP-open and the API silently times out after ~4 min; a "hanging" IB run is almost always this. → pm:2026-07-18-ib-gateway-lan-ip-silent-timeout
- Gateway pinned 10.45, live mode, 2FA approved by hand on every login. Livewire never restarts, retries, or manages the Gateway; a connection failure is 2FA/maintenance/session conflict, not a bug to recover. → test: `tests/test_run_ib_fetch_robust.py::test_gateway_unavailable_is_typed_and_never_retried`
- IB is not a single point of failure. Unreachable Gateway → exit 86 → lane **skipped**, run DEGRADED (not failed, no page, no retry). Eligibility is membership of the IB phase set, not the exit code. → test: `tests/test_run_daily_update_job.py::test_gateway_down_is_degraded_not_failed`, `tests/test_status.py::test_an_ib_phase_at_86_reads_degraded_not_failed` · pm:2026-07-22-ib-not-a-single-point-of-failure, pm:2026-08-08-ib-down-must-not-fail-the-run
- A preflight belongs to the phase that needs IB, never to the orchestrator: an orchestrator-level preflight lost Friday 2026-08-07 warehouse-wide (equity 0/13311, rates 0/4). → pm:2026-08-08-ib-down-must-not-fail-the-run
- Equity daily falls back to Massive on a down Gateway; futures/cmdty have no fallback and stay degraded — a manufactured success is worse than a gap. → test: `tests/test_run_daily_update_job.py::test_futures_and_cmdty_get_no_fallback`
- `IB_EARLIEST_DATE` is IB's floor, never an instrument's inception; `expected_start` has no default. → test: `tests/test_quality_detector.py::test_range_shortfall_no_head_ts_uses_expected_diff_only` · pm:2026-07-27-ib-earliest-date-false-range-shortfall
- `fetch_batch` maps a raised fetch to the exception, never to `[]` — otherwise a total outage reads as `no_trade`, `errors=0`, exit 0. → test: `tests/test_daily_update.py::TestFetchBatch::test_handles_error`, `::test_no_bars_is_still_an_empty_list`

### Providers — floors roll; re-measure before trusting a number

- Massive flat-file GET floor is a rolling 5 years (2021-07-28 on 2026-07-29); LIST advertises 2003 and lies. Probe entitlement with GET, never LIST. Never delete raw partitions. The floor is derived from the scan date, never hardcoded. → test: `tests/test_housekeeping.py::test_apply_deletes_only_the_unprotected` · pm:2026-07-29-flatfile-get-floor-list-lies, pm:2026-07-29-massive-floor-derived-from-scan-date
- Massive REST FX: 2-year floor, 5 req/min with no `Retry-After` — pace preemptively. That 5/min is FX-scoped; every rate-limit number in this repo carries a scope. DXY exists only on Yahoo; Yahoo owns `asset_class=fx`; intraday fx files are merged, never replaced. → pm:2026-07-27-fx-dxy-provider-floors
- `/v2/aggs` is entitled ~5 years, so older breaks are `inconclusive` forever; the triage verdict store (`repairs/triage/current.json`) is durable and never deleted to "force a re-triage". → pm:2026-07-17-triage-aggs-entitlement-floor
- ~90% of equity bronze is `price_basis='unknown'`; a new split against that population quarantines the symbol. Standing threat, not hypothetical. → test: `tests/test_adjustment_engine.py::test_unknown_split_affected_row_blocks_factor_construction` · pm:2026-07-18-unknown-price-basis-population
- `constants.declared("flatfile_min_publish_ratio")` (0.9) is the only thing between "raw file held 12,000 symbols, published 40" and exit 0. → test: `tests/test_ingest_flatfiles.py::TestVerifyPublishCoverage::test_lw_declared_flatfile_min_publish_ratio_overrides_the_floor` · pm:2026-07-22-flatfile-min-publish-ratio

### Scheduled jobs

- The warehouse plists point at `<warehouse>/current`, never a checkout or worktree — no `.env` there, so every credential resolves to nothing, including the alert that would report it. `universe-refresh` is the one exception (it writes `presets/`, and the release is `chmod -R a-w`). → test: `tests/test_launchd_templates.py::test_the_scheduled_jobs_run_the_release_not_a_checkout`, `::test_no_other_template_reads_the_repo` (all six templates; a new template with no entry fails) · pm:2026-07-27-launchd-pointed-at-worktree-no-env, pm:2026-09-01-universe-refresh-runs-from-repo
- `promote` exports `origin/main` but runs the checkout's own builder: `git checkout main && git pull` before promoting anything that touches the promoter. Never `rm -rf` the release `current` points at; recover with `release rollback` then `promote`. → pm:2026-07-29-promote-runs-checkout-builder, pm:2026-07-29-rm-rf-release-current-dangling
- Releases are `git archive` exports (a `git worktree` export keeps a `.git` tether to the checkout) with their own frozen venv. `promote` gates on a completed CI run for that exact SHA — `ci.yml` runs on push to main because a squash merge is a commit no PR run covered; `--allow-unverified` was for bootstrap only. Flipping `current` mid-run is safe (`os.getcwd()` is physical). The lake is deliberately **not** isolated per release: dev and prod share one `fcntl.flock` domain, and containerizing would split it.
- A release carries no `.env` and no `node_modules`; `promote` runs `npm ci --omit=dev` before `freeze`. → test: `tests/test_release.py` · pm:2026-07-29-release-missing-node-modules
- Lane budgets are per lane (`LANE_BUDGET_S`), not a total: a lane over budget is killed by process group, recorded `outcome='timeout'`, and **the next lane starts normally**. → test: `tests/test_run_daily_update_job.py::TestPerLaneBudgets` · pm:2026-07-28-daily-job-deadline-is-a-total
- Lane order is no-fallback-first (futures → cmdty → CBOE → FX → corporate-actions → equity → silver): the IB-only lanes take minutes and cannot be back-sourced, so they never queue behind a 3–8h Massive lane. → test: `::test_main_runs_the_no_fallback_lanes_before_the_expensive_ones`
- One `fcntl.flock` at `<warehouse>/locks/lake-io.lock` (internal disk, never in the lake) is held by every lane that touches the lake, in **both** runners' three lane bodies; the daily job polls at 1s and the intraday job at 60s, and a lane that waits past its own budget is recorded `outcome='blocked', blocker='lake_lock'` instead of running. Four nights of corporate-actions timeouts (10800s each) against a 39-minute lake-alone run preceded it. → test: `tests/test_run_intraday_catchup_job.py::TestBothRunnersTakeOneLock`, `tests/test_run_daily_update_job.py::TestTheLakeLock` · pm:2026-09-06-intraday-and-daily-shared-the-lake
- The lane list is declared once (`clients.constants.LANE_ORDER`, `IB_ONLY_LANES`); the job runs it and `status` generates its CHECK SQL from it, so a new lane cannot be run but ungraded. → test: `tests/test_status.py::test_adding_a_lane_makes_it_appear_in_the_lanes_terminal_check`, `tests/test_constants.py::test_declared_lane_budgets_cover_exactly_the_lane_set`
- The lane runner never runs the alert: `_page_failure` takes no runner parameter; `send_failure_alert` binds `subprocess.run` late. → test: `TestTheLaneRunnerNeverRunsTheAlert` · pm:2026-08-02-lane-runner-ran-the-alert
- Every lane pages, the timeout branch included; `_run_scheduled_lane` is the single shared lane body (no private copies). → test: `tests/test_run_daily_update_job.py::test_terminal_failure_*` · pm:2026-07-28-lane-alert-paths-missing
- The corporate-actions lane always resumes (`--resume` is unconditional on the scheduled command, Sunday included): an incompatible or complete cursor starts a fresh pass instead of raising, and a resumed tail that finishes opens one new cycle in the same invocation. Without it three nights (2026-09-03/04/05) each restarted at symbol 1 and the tail of the ~13.3K universe was never reached. → test: `tests/test_sync_corporate_actions.py::test_a_resumed_pass_finishes_its_tail_then_opens_a_new_cycle`, `tests/test_corporate_action_cursor.py::TestResumeNeverFailsTheLane` · pm:2026-09-05-corporate-actions-restarted-from-symbol-one
- corporate-actions fails on a rate (`FAILURE_RATE_TOLERANCE` 5%), never on one symbol; Silver is gated on its own two inputs only, and only on `outcome='failed'` — a timeout (124) or a down Gateway (86) is slow data, not wrong data. → guard: `livewire_scripts/sync_corporate_actions.py` · test: `tests/test_run_daily_update_job.py::TestTheSilverGateOnlyBlocksOnFailure` · pm:2026-08-02-corporate-actions-failed-on-one-symbol, pm:2026-09-06-intraday-and-daily-shared-the-lake
- `no_trade` never makes a run look failed — not in `resolve_exit_code`, not in the watchdog (`updated == 0 and errors`). → test: `tests/test_daily_outcomes.py::test_no_trade_and_partial_never_fail` · pm:2026-07-26-no-trade-paged-quiet-weekends
- A corrupt per-symbol parquet is quarantined by the publisher and counted **missing** by every reader. Both fail, in opposite directions; neither aborts. A read-only detector must not repair. Fixing only the publisher (2026-07-14) let the same file class abort `coverage` **9 times**, through 2026-09-01. → test: `tests/test_flatfile_publisher.py::test_corrupt_1m_parquet_quarantines_the_symbol_and_run_continues`, `tests/test_coverage_report.py::TestComputeCoverage::test_one_corrupt_parquet_does_not_kill_the_whole_scan` · pm:2026-07-14-corrupt-parquet-aborted-publish, pm:2026-09-01-coverage-aborted-on-corrupt-parquet
- `duckdb build`'s per-view `read_parquet(glob)` has no per-file skip (DuckDB 1.5's `ignore_errors` is not a real parameter for it); a third reader hit the same corrupt-file class and took the _whole catalog_ down for three nights before `InvalidInputException` was caught alongside `IOException`. → test: `tests/test_duckdb_catalog.py::test_build_coverage_tolerates_a_corrupt_parquet_in_the_glob` · pm:2026-09-06-duckdb-coverage-corrupt-parquet-aborted-build
- Coverage has its own job with **no timeout**: every guessed budget (600s, then 1800s) expired against a cold exFAT glob (one run ranges 1400–2860s), and a blown budget is silence, not an error — coverage was dead 2026-07-07 → 08-03 unnoticed. Never wrap a lake scan in a guessed constant. → pm:2026-08-02-coverage-budget-expired-silently
- coverage/weekly/digest run once, after Silver. The watchdog pages only on `BAD`; a run still in flight at 10:30Z is `WARN` (lane budgets sum to 9h, so completion is no longer guaranteed) and every lane check reads UNKNOWN until the run has a close row. → test: `tests/test_check_daily_update_watchdog.py` · pm:2026-07-22-coverage-weekly-digest-ordering, pm:2026-08-16-watchdog-raced-quality-marker
- `housekeeping` protects `raw/` and `repairs/` by name, dry-runs by default, previews releases too; `--appledouble` is opt-in and never nightly (34 min, 97.5% I/O wait). → test: `tests/test_housekeeping.py` · pm:2026-08-10-appledouble-sweep-cost
- `MDW_SOURCE_EVIDENCE` is committed once per run, never per response (41 min/night otherwise). → pm:2026-08-31-source-evidence-per-response-cost
- Source evidence is a **sharded** CAS (`sha256/<d[0:2]>/<d[2:4]>/`) with no per-artifact lock file and one directory fsync per commit; one flat exFAT directory (275,006 entries, 137,504 orphan locks, 25 GB) timed the corporate-actions lane out 3 nights, and every lane subprocess now runs `PYTHONUNBUFFERED=1`. → test: `tests/test_source_evidence.py::TestShardedCas`, `tests/test_sync_corporate_actions.py::TestDistinctResponseBodies` · pm:2026-09-05-source-evidence-flat-exfat-directory

### Alerts and the digest

- Never quoted-printable: every body is `key=value` telemetry and QP reads `=NN` as a byte. `textEncoding: "base64"`. → test: `tests/node/send_daily_update_failure_email.test.mjs` (`npm run test:alerts`, run by `ci.yml` since PR #98) · pm:2026-08-16-quoted-printable-corrupted-digest
- Alert values are passed single-token (`--key=value`); a value beginning with `--` used to be unsendable. → test: same file, `"a value beginning with -- survives"` · pm:2026-08-08-alert-value-starting-with-dashes
- A failed alert send is an `executions(script='send_alert', exit_code<>0)` row; `status` and the watchdog both grade it WARN. → test: `tests/test_status.py::test_any_undelivered_alert_is_a_warning`
- One missing interior day on an illiquid warrant is `info`, not `warning` (150 emails in 20 minutes, 4,408 undelivered, 2026-07-19). → test: `tests/test_quality_detector.py::test_interior_gaps_single_missing_trading_day_is_info_not_warning` · pm:2026-07-19-interior-day-warning-email-storm
- The interior-gap scan measures liquidity, not loss (96.6% flagged; SPY/AAPL/NVDA/MSFT/QQQ/TSLA absent). Not scheduled; `status` does not grade it. → pm:2026-08-17-interior-gap-scan-measures-liquidity
- `status`: `UNKNOWN` is not `OK` (`Verdict` is an `IntEnum`, OK < UNKNOWN < WARN < BAD); every check is one `(name, sql)` row in `CHECKS` over the ledger; zero rows is UNKNOWN unless the name is in `_EMPTY_IS_OK`; `launchctl` exits cap at WARN; every log line goes through `rich.markup.escape`; exit code is always 0; it never scans bar parquet. → test: `tests/test_status.py` · pm:2026-08-16-status-surface-grading
- The digest enumerates nothing itself — it renders `status.collect()`, so a check reaches both surfaces or neither. Disk headroom is reported for the lake volume _and_ the internal volume. → test: `tests/test_nightly_digest.py` · pm:2026-08-10-nightly-disk-line-wrong-volume

### Silver

- Two trims, in order: the deterministic 2021-06 seed-boundary check on raw bronze (trims to the post-seed window, never quarantines), then the blind >6.0 continuity scan on the adjusted series with durable triage verdicts exempting confirmed real moves. Everything published is silver grade _at the 6.0 definition_. → test: `tests/test_rebuild_silver.py::test_seed_corrupt_symbol_publishes_its_post_seed_window_rather_than_quarantining` · pm:2026-07-18-silver-seed-floor-blind-heuristic
- Quarantine **moves** the artifact to `<silver>/evicted/<rev>/`; Apex never reads the manifest. Factor intervals stay wider than the daily window. → test: `tests/test_rebuild_silver.py::test_a_quarantined_symbols_stale_artifact_is_moved_not_just_unmanifested`
- Two active splits on one ex-date: equal ratios collapse to one, unequal ratios fail closed. Count affected stored rows, not action records (16 symbols → 5 in history → 0 published). → test: `tests/test_adjustment_engine.py::test_one_split_restated_at_another_scale_is_collapsed_not_doubled`, `::test_conflicting_active_splits_on_one_ex_date_fail_closed` · pm:2026-08-02-two-active-splits-one-ex-date
- Cancellation inference is provider-scoped: a Massive full reconcile never cancels a yahoo-sourced action (507 repairs undone over two Sundays before this). → test: `tests/test_corporate_action_store.py::test_full_reconcile_leaves_another_provider_alone` · pm:2026-07-19-cancellation-inference-provider-scoped
- A carried-forward artifact takes its sha from the file on disk, never from the previous manifest; an interrupted publish leaves the lake ahead of the manifest and a stale sha then fails every later publish. → test: `tests/test_rebuild_silver.py::test_carried_symbol_takes_its_sha_from_disk_not_the_stale_manifest` · pm:2026-09-07-silver-carry-forward-trusted-a-stale-manifest-sha
- `--allow-window-regression` was for the rev-3 bootstrap, exactly once. → test: `tests/test_rebuild_silver.py::test_allow_window_regression_publishes_the_shorter_window`
- A publish takes its clock from the run, so a frozen PIT `as_of` cannot expire (19 tests broke with no commit in between). → PR #93

### DuckDB

- Views register on demand (`CREATE VIEW` enumerates the glob: 221s); symbol-scoped reads bypass views (0.53s vs >5 min); the coverage table is durable because the nightly 23.57 GB evicts the cache. → pm:2026-08-02-duckdb-glob-enumeration-cost
- DuckDB is imported only by the catalog modules; `status.py` never imports it; no command materialises bars out of bronze. Postgres was removed 2026-08-02. → test: `tests/test_duckdb_containment.py::test_duckdb_is_imported_only_by_the_catalog`

## How to work in this repo

Five months of corrections, each one paid for. Sources: `docs/postmortems/`, the
session archaeology of 2026-09-02.

1. **Verify on the host that owns the fact.** Production state is `ssh macmini`. "Not on this machine" is not "does not exist" — two host mix-ups on 2026-09-01 alone. Name the host in every measurement you report.
2. **Measure before you recommend.** No merge, promote, or "fixed" before the acceptance criteria have been checked on real data; recommending first and verifying after is the standing failure ("顺序反了"). Constants are measured cold on the real lake or they are not written down.
3. **Consolidate; never add a script.** Ten detectors exist. A new check is a registry row and a test. A new one-off is a deletion candidate before it is written.
4. **Never lower an acceptance criterion you cannot meet.** Report "not met" and keep the criterion.
5. **Fix the twin.** Every invariant has a sibling path — `run_daily_update_job` ↔ `sync_runner`, publisher ↔ reader, both alert queues, both orchestrators. Grep for it before calling a fix complete.
6. **A green test that mocks the failing seam proves nothing.** Fake runners swallowing `**kwargs`, bare `MagicMock` stores, mocked URLs, a Node suite CI never runs — each hid a production break. Exercise the real signature at least once.
7. **Merging is not shipping.** A fix runs in production only after `release promote`; check `readlink ~/market-warehouse/current` on the mini and the CI run for that SHA before saying "deployed". PR #95 sat merged, promoted after the nightly jobs, and never executed.
8. **Blast radius is counted in the lake, not the store.** Affected stored rows, then affected published symbols — never action records.
9. **A detector with no output is dead, not healthy.** `0/0 → 100%`, an absent log, an absent marker, a missing raw traded set: UNKNOWN, surfaced, never green.
10. **Every incident becomes one post-mortem file and one line here.** Not a paragraph here; not a new audit document nobody runs at 03:00.
11. **Say it plainly (说人话).** What is broken, what it costs, what you need decided — in that order, under three sentences before any table.
12. **Stop before pushing.** Commits, merges and promotes are explicit requests; "merge when green" for one PR does not carry to the next. Never interrupt a running backfill on the mini.

## Testing

`uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning` —
`.github/workflows/ci.yml:53` verbatim. Bare `--cov` takes its `source` from
`pyproject.toml` (`clients` + `livewire_scripts`); passing `--cov=<pkg>`
overrides that and silently measures the wrong tree. `npm run test:alerts` for the Node alert suite (CI runs it too). All new code in `clients/` and
`livewire_scripts/` gets tests; mock external I/O; temp parquet roots for
storage; `@pytest.mark.integration` for DB tests. Pre-commit secrets scanner:
`ln -sf ../../tools/pre-commit-secrets-scan.sh .git/hooks/pre-commit`.

## Commands

Everything with every flag: `docs/runbook.md`. Daily reach:

```bash
uv run python scripts/livewire_ops.py status                          # graded view; exit 0 always
python scripts/livewire_ingest.py daily [--source massive]             # equity daily catch-up
python scripts/livewire_ingest.py cboe-vol | fred-rates | fx --days 7
python scripts/livewire_ingest.py corporate-actions --full-reconcile
python scripts/livewire_store.py rebuild-silver --full --dry-run --failure-output /tmp/dry.json
python scripts/livewire_quality.py coverage --no-recover               # 1400–2860s cold; registry denominator
python scripts/livewire_ops.py release promote [--dry-run] | list | rollback
python scripts/livewire_ops.py housekeeping [--apply]
```

Operator runs on the mini use `~/market-warehouse/.venv` (`source …/bin/activate`);
dev and CI use `uv run`. Both read `~/market-warehouse/.env`.
