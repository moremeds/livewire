# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

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
