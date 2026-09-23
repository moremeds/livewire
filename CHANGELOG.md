# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Retired the BZ futures root from new ingestion while preserving stored BZ history. COIL is configured as a separate IPE series.
- Added verified exchange mappings for the new commodity roots and rolling futures selection: energy through the current month plus 15 months, and the first two live delivery months for GC/SI/HG and 12 agricultural roots. Newly selected contracts are full-history seeded before daily updates.
- Futures coverage now uses that same live rolling selection; when IB is unavailable it reports futures as UNKNOWN and continues equity coverage and recovery.

- EIA energy lane (`livewire_ingest.py eia`, `clients/eia_client.py`): petroleum
  (spot, retail, the weekly supply report incl. inventories, refinery, imports),
  natural gas (Henry Hub spot, storage), nuclear outages, and grid-monitor
  electricity daily + hourly, into
  `bronze/asset_class=energy/product=<p>/dataset=<d>/…`. Raw pages in source
  evidence, `runs`/`measurements` in the ledger, and a `sync_runner` phase that
  re-fetches the last 14 days of every dataset on each intraday-catchup. Hourly
  history loads from EIA's bulk `EBA.zip`, with the facets the bulk file lacks
  filled from the API. Every monthly/quarterly/annual series of EIA's bulk
  families (PET, PET_IMPORTS, NG, ELEC, COAL, TOTAL, SEDS, INTL, EMISS; STEO kept
  per release) is imported and re-imported when EIA's manifest moves; each import
  is a ledger `evidence(kind='eia_bulk')` row, the zip is kept under `raw/eia/bulk`,
  and replaced values are counted as `eia_values_revised`. `status` grades
  `EIA freshness` and `EIA bulk imports`.
  `publish_parquet` accepts a composite sort key; `get_with_retry` takes an
  opt-in `retry_statuses` (EIA opts into 429 — no other caller does).
  The EIA catalog and plan are in `docs/audits/eia/`.
- `clients/http_retry.py`: the repo's one definition of a transient HTTP
  failure. A 5xx or a transport error retries with a bounded linear backoff; a
  4xx raises on the first attempt. FRED's local copy was replaced by it and
  CBOE now uses it too (`cboe_retry_attempts`, `cboe_retry_backoff_s`).
- `ledger.open_run()` closes a job's still-open predecessors on the same host
  with `verdict = 'ABANDONED'` before opening a new run, so a run killed by
  SIGKILL or Ctrl-C no longer reads as in flight forever. `status` grades
  `ABANDONED` as WARN.
- `identity_fetch_failed_ticker` measurements name each failed security-master
  fetch as `<ticker>:<http status>`, so a rerun can target those tickers
  instead of re-walking the whole universe.
- `ledger query` prints the quoting and reserved-word rules in `--help`, and a
  bad query exits 2 with one line and a hint instead of a traceback.

- `duckdb build` now also publishes a read-only snapshot of the coverage
  catalog to `<lake>/catalog/analytics.duckdb` (temp + `os.replace`), so a
  consumer bound to the lake volume can open it while the writer keeps its
  working copy on local disk.
- `livewire_ingest.py security-master sync` backfills security identities from
  Massive `/v3/reference/tickers`, and `livewire_ingest.py membership-sync
  reresolve --index <id> --confidence {B,C,D}` rewrites each resolvable
  `unresolved:<ticker>` placeholder onto its opaque `security_id`. The mini
  held 3,455 unresolved membership ids against one verified identity row, so
  every PIT membership query answered the empty set and apex's
  `/v1/membership/*` served 503. Identity rules are the spec §4 table: a rename
  collapses to one `security_id` with two symbol rows; conflicting or
  incomplete FIGI evidence yields two ids that deliberately do not resolve; a
  record with no provable start appends nothing. Evidence is committed once per
  run before any append, a lost ticker exits 1, the reresolve pass fails closed
  on a replay its insertion would unbalance, and `known_at = now` keeps an
  `as_of` before the pass seeing the placeholder. Two scoped constants —
  `massive_requests_per_minute/reference` and `massive_backoff_s/reference` —
  pace it; the 5/min is provisional, carried over from the FX scope, and the
  first `--tickers` sample run on the mini measures the real rate to pass back
  as `LW_DECLARED_MASSIVE_REQUESTS_PER_MINUTE_REFERENCE`. The "Unresolved
  memberships" remediation hint now names these two commands instead of the
  read-only `livewire_ops.py membership` query, which only listed the
  unresolved members and fixed nothing.
  Pacing is per request (a ticker is 2-3 requests), `known_at` is each
  ticker's fetch time, evidence is committed per 50-ticker chunk so a crash
  keeps earlier chunks, a rename is joined to its earlier symbol's id when the
  halves arrive in separate fetches, a `date=` probe row must share a FIGI or
  cik with the listing it stamps, the needed dates are probed in order until
  one answers, a cross-fetch rename with overlapping dates is a conflict on
  its own id, widening one symbol across its renamed sibling is likewise a
  conflict, revisions follow the id's history, and an empty `date=` probe is
  recorded as an `identity_probe_empty` measurement so the next run does not
  repeat it (code review 2026-09-15).
- Three facts measured against the production key on 2026-09-15 shape what the
  backfill can produce (`tests/fixtures/massive_reference/README.md`, F1/F5/F6;
  the frozen bodies are the tests' only input, which never touch the network):
  the endpoint never returns `list_date`, so `effective_from` always comes from
  the `date=` probe or from nothing; a delisted record may carry no FIGI at all
  (YHOO carries `cik` only, AABA carries both FIGIs), so YHOO → AABA derives
  two security_ids rather than one renamed identity; and all 13,181 active US
  stock listings are `currency_name: usd`, so no non-USD listing exists to
  fixture. A ticker that derives only a `candidate` row is re-fetched on every
  run — only verified intervals count as covered.

### Fixed

- A transient FRED failure no longer skips the rates series after it. One
  `502 Bad Gateway` on `DGS5` (2026-09-14, intraday-catchup phase 2) left the
  loop, so `DGS10` and `DGS30` were never requested and two watchdogs paged
  over an outage that was gone by the next request.
  `clients/fred_client.py` now retries a 5xx or an `httpx.TransportError`
  `fred_retry_attempts` times with an `fred_retry_backoff_s` linear backoff
  and never retries a 4xx; `livewire_scripts/fetch_fred_rates.py` catches
  `httpx.HTTPError` per series, continues, and returns 1 if any series is
  still unfetched — a total outage reads as failed, never as `inserted 0`,
  exit 0. A malformed payload still raises. Both `LW_DECLARED_FRED_RETRY_*`
  overrides are floored (attempts ≥ 1, backoff ≥ 0) so a typo cannot skip the
  request or turn the retry into a `ValueError` the per-series catch does not absorb.
  (pm:2026-09-14-fred-502-aborted-remaining-series)
- `membership-sync --index` accepts a space-separated list, so the
  scheduled `com.livewire.membership-sync` job runs instead of dying at
  argparse on `unrecognized arguments: ndx100 djia`; the plist template now
  names all four panels.
- The corporate-actions lane converts foreign-currency dividends once per
  cycle, each under its own `runs` row, and cannot fail the lane on that
  step. `convert-dividend-currency` was previously an operator-only
  sub-command and Silver failed ~30 symbols on a currency mismatch. A
  dividend whose ex-date has not closed yet is left for the next run
  (`ex_date_pending`) instead of being priced at today's FX, `--preset` files
  `scope='subset'` like `--tickers`, and the conversion runs at the end of
  every cycle — before the resumed invocation opens a second cycle the lane
  budget may kill, and again after that cycle so its own dividends are
  converted rather than left behind a mismatch count that already read 0.
  "Foreign-currency dividends" reads UNKNOWN when today's corporate-actions
  lane did not finish, so a lane killed at its budget mid-cycle cannot leave
  that stale zero grading OK; a conversion that raises closes its own `runs`
  row `exit_code=1` and files one `dividend_fx_error` measurement, which the
  check reads as UNKNOWN until a later pass measures again. A pending ex-date no longer counts
  as a mismatch, so an announced dividend cannot WARN every night until it
  goes ex. The lane's conversion scope is the tickers whose fetch carried a
  dividend in a currency other than that equity's own, plus
  `repairs/dividend_fx/pending.json`, not a nightly scan of all ~13.3K
  corporate-action files; every fetch returns full history, so the marking
  self-heals nightly, and the manual sub-command with no `--tickers` still
  scans the store.
- The `status` check "Foreign-currency dividends" grades only today's
  `scope='all'` measurement and reads UNKNOWN without one, instead of
  inheriting the last manual run's green. A `--tickers` repair now files
  `scope='subset'` so it cannot stand in for the whole scope, and the
  conversion reads `security_master` once per pass instead of once per
  symbol.
- The raw-date staging validator no longer decodes macOS AppleDouble
  sidecars. The staging directory is created inside the raw root on the
  exFAT lake, so `glob("*.parquet")` also matched `._bucket=000.parquet`,
  and its missing Parquet footer aborted every Massive raw publication on
  2026-09-09. A stage holding only sidecars still fails as having no
  parquet files.
- A Silver rebuild that produces a byte-identical manifest no longer crashes.
  The publisher dedupes an unchanged manifest by returning the current revision
  and writing nothing, which left the transaction's reservation unused and was
  treated as a failed commit. `rebuild_silver` tried to predict that case before
  committing, but whether the assembled manifest differs is knowable only after
  assembling it — so the decision now lives in the publisher, and the caller's
  check is an optimization that can be incomplete without breaking a run. The
  2026-08-17 nightly job died this way with `reserved Silver revision was not
  committed`; the watchdog paged 4.5h later and Silver never rebuilt.

### Changed

- Removed the unused ClickHouse bootstrap (`setup_market_warehouse.sh` flags, schema, helper scripts, `clickhouse-connect`). Nothing ran it and the mini never installed it.
- Refreshed `CLAUDE.md`, `AGENTS.md` and `README.md` against the code and the mini: eight launchd jobs, digest at 15:45Z, equity daily from Massive by default, Gateway 10.50, the lake's per-subtree symlinks, rolling futures selection, and how Apex consumes the lake.

- `massive_requests_per_minute/reference` is 600/min, not the 5/min inherited
  from the free Currencies FX tier. Massive documents no cap on a paid plan and
  advises staying under 100 req/s; the backfill is latency-bound well below it.
- The watchdog no longer pages for a failure whose run already paged. A lane
  failure used to send two emails: the lane's own page, then the watchdog's an
  hour later, because the two fingerprint in unrelated namespaces.
- `cboe-vol` returns 1 when a symbol is still unfetched after retries. It
  returned `None` — exit 0 — however many symbols failed, so the phase could
  not fail. A 4xx still exits 0: a retired index is CBOE's fact, surfaced by
  the `Stale non-equity` check.
- Entrypoints that open a ledger run catch `BaseException` around the body, so
  a Ctrl-C writes the terminal row before re-raising.

- `rebuild-silver` now publishes the successfully staged symbols instead of
  aborting the whole revision when some symbols fail to stage (e.g. unresolved
  split-basis). A symbol's artifacts remain atomic, and the run exits non-zero
  only on systemic failure (all symbols failed, or the failure rate exceeds the
  daily-command threshold via `resolve_exit_code`), so a small stable set of
  unresolved symbols no longer blocks the full universe or triggers a nightly
  alert storm.

## [0.3.0] - 2026-07-04

### Added

- Full-universe equity daily lane via Massive `day_aggs` flat files (~20K
  tickers vs. the ~2.5K preset-driven `daily` command), re-enabled in the
  nightly catch-up orchestrator.
- `presets/futures-active.json` — GC (front + second month), CL, BZ Brent
  futures; contract-month codes verified against live IB.
- Backfill now tracks `oldest_date` per ticker to detect shallow history.
- Warehouse health report command.
- Daily-run observability: a machine-readable `SUMMARY_JSON` line per run with
  per-ticker outcome classification (`updated` / `no_trade` / `partial` /
  `error`) and a threshold-based exit policy, a nightly digest email replacing
  the noisy per-ticker summary storm, and coverage tracking after every
  successful daily run.

### Changed

- Replaced all legacy ticker-filtered and REST equity-intraday ingestion with a
  resumable Massive whole-market flat-file pipeline. The pipeline discovers the
  maximum entitled history, stages bucketed raw Parquet, publishes every
  provider symbol, and derives `5m`, `30m`, and `1h` from canonical `1m`.
- Routed full backfill, scheduled catch-up, coverage recovery, and explicit
  repair through `flatfile-ingest`; `intraday-backfill` is now IB-only for
  non-equity asset classes.
- Added mandatory preflight checks for Massive S3 credentials and a full-build
  storage-capacity gate.
- Parallelized flat-file ingestion, defaulted equity to Massive, and improved
  exFAT storage stability.
- Bronze writes now use zstd-3 compression; intraday storage migrated to a
  multi-file layout.
- Shifted the daily sync and intraday catch-up schedules to clear IBC's
  nightly 03:45 UTC auto-restart / 2FA window.
- Removed Cerebras alert enrichment in favor of a static, truthful incident
  report.

### Fixed

- **Futures never seeded**: `make_contract` passed `"USD"` positionally into
  ib_async's `localSymbol` slot instead of `currency`, so every futures
  contract request failed IB validation (error 200) and `asset_class=futures`
  bronze stayed empty. Also repairs the existing futures-index/-energy/-metals/
  -treasuries presets.
- **`archive-otc` would have delisted live tickers**: it differenced bronze
  against the day_aggs universe, which excludes warrants/units/rights/
  preferreds that bronze actually carries via the minute_aggs lane — a live
  dry-run flagged 286 actively-trading instruments. Rewritten to use the
  minute_aggs `_symbols.parquet` set plus a data-driven staleness guard
  (live dry-run now 286 → 0).
- Backfill depth check no longer misfires on seed cursors in
  `gap_aware_completed`.
- Added NDX→NASDAQ to `VOLATILITY_EXCHANGE_MAP`; fixed RUT intraday, ET-aware
  daily targets, and smarter failure alerts.
- Added `5m` to the IB volatility intraday lane.
- `daily_update` alert summaries now parse the structured `SUMMARY_JSON` line
  instead of regexing per-ticker prose, fixing a false "277/277 failed" alarm
  where a success message ("1 bar published from Massive") was miscounted as
  the dominant error.
- Restored the `quality_summary` completion marker (used by the watchdog)
  after removing the old summary-email spawn.

## [0.2.1] - 2026-06-03

### Fixed

- Point flatfile S3 client at `files.massive.com` instead of the stale
  `files.polygon.io` constant left over from the vendor switch. `clients/massive_client.py`
  was updated to `api.massive.com` at the time but `clients/massive_flatfile_client.py`
  was missed, so `flatfile-ingest` calls signed against the Polygon host with
  Massive credentials and failed authentication.
- Pass `--max-concurrent` to the equity intraday subprocess in
  `livewire_scripts/backfill_runner.py`. `MDW_BACKFILL_MAX_CONCURRENT` (default 10)
  was already wired into `BackfillConfig` and passed to the daily-backfill and
  volatility-intraday subprocesses, but the equity intraday lane omitted the
  flag and ran serially at the `intraday-backfill` default of 1 worker.
  Measured ~2× sustained throughput improvement.

## [0.2.0]

Initial tracked release.
