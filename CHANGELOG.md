# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
