# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
