# Backfill Terminal No-Data Spec

## Purpose

Prevent `python scripts/livewire.py backfill --full` from repeatedly retrying equity tickers whose current bronze snapshot already exists and whose older-history provider result is a stable terminal no-data outcome.

The full backfill runner should continue retrying transient failures, but a clean provider result showing no older history should be persisted as a completed terminal state.

## Problem

Livewire has separate cursor concepts:

- Normal historical cursor: tracks whether a ticker has been seeded into bronze.
- Backfill cursor: tracks whether older history has been inserted before the ticker's current oldest bronze `trade_date`.

The current IB backfill path only marks a ticker complete when older rows are inserted. If a ticker already has bronze data but the provider returns zero older rows, the ticker remains incomplete and is retried by future full backfills.

Observed behavior:

- A ticker can have a valid bronze file, for example `MATX` with data from `2012-07-02` through `2026-05-22`.
- The backfill phase requests older windows before `2012-07-02`.
- IB may return repeated `162 HMDS query returned no data` responses for those older windows.
- The command prints `0 backfill rows (will retry next run)` and does not mark the ticker complete.
- The full runner later revisits the same ticker even when the result is stable no older history.

This wastes IB pacing budget and extends full-build wall-clock time without improving warehouse coverage.

## Goals

- Persist terminal no-data outcomes so future full backfills skip those tickers.
- Keep transient failures retryable.
- Preserve existing cursor files and resume behavior.
- Make progress reporting count terminal no-data outcomes as complete.
- Store enough evidence to diagnose why a ticker was skipped later.
- Avoid introducing a second canonical data path; bronze Parquet remains the system of record.

## Non-Goals

- Do not change the provider selection policy for deep older-history equity backfills. `--source auto` should continue resolving to IB unless separately changed.
- Do not delete or rewrite existing bronze parquet as part of terminal no-data handling.
- Do not classify delistings or mutate preset membership in this feature.
- Do not mark no-data as terminal for interrupted, timed-out, or partially executed provider calls.
- Do not require Postgres or any analytical publish target.

## Current Code Areas

- `livewire_scripts/fetch_ib_historical.py`
  - Loads and saves historical/backfill cursors.
  - Runs `_run_backfill()` for IB-backed older-history backfills.
  - Calls `backfill_ticker()` and currently marks completion only when inserted row count is positive.
- `livewire_scripts/backfill_runner.py`
  - Runs `backfill-all` phases.
  - Uses `cursor_completed()` to decide preset progress.
- `tests/test_fetch_ib_historical.py`
  - Existing coverage for cursor formats and backfill behavior.
- Existing cursor path
  - `~/market-warehouse/logs/cursor_backfill_<preset>.json`

## Cursor Model

Backfill cursors should support explicit per-ticker, per-timeframe outcomes.

### Existing Supported Formats

Old list format:

```json
{
  "completed": ["AAPL", "MSFT"],
  "started_at": "2026-05-31T13:05:26"
}
```

Current timeframe format:

```json
{
  "completed": {
    "AAPL": ["1d"],
    "MSFT": ["1d"]
  },
  "started_at": "2026-05-31T13:05:26"
}
```

### New Status Format

```json
{
  "version": 2,
  "completed": {
    "AAPL": {
      "1d": {
        "status": "done",
        "source": "ib",
        "rows_inserted": 2381,
        "oldest_before": "1990-01-02",
        "decided_at": "2026-05-31T18:30:00Z"
      }
    },
    "MATX": {
      "1d": {
        "status": "no_older_history",
        "source": "ib",
        "rows_inserted": 0,
        "oldest_before": "2012-07-02",
        "requested_start": "1993-01-29",
        "requested_end": "2012-07-01",
        "evidence": {
          "provider": "ib",
          "error_codes": [162],
          "message_sample": "HMDS query returned no data",
          "windows_attempted": 22,
          "windows_returned_rows": 0
        },
        "decided_at": "2026-05-31T18:30:00Z"
      }
    }
  },
  "started_at": "2026-05-31T13:05:26"
}
```

The loader must accept all three formats. The saver may write the new format after this feature lands.

## Status Semantics

### `done`

The ticker/timeframe backfill inserted one or more older rows into bronze.

Completion behavior:

- Counts as complete.
- Skipped by future backfill runs unless `--reset` clears the cursor.

### `no_older_history`

The ticker/timeframe already has bronze data, the provider call completed, and no older rows were available for the requested range.

Completion behavior:

- Counts as complete.
- Skipped by future backfill runs unless `--reset` clears the cursor.
- Must include evidence fields sufficient to distinguish stable no-data from a transient failure.

### `retry`

The provider call did not produce a trustworthy terminal decision.

Examples:

- IB Gateway disconnected.
- Command timed out or was killed by stall detection.
- Contract qualification failed in a way that could be caused by session/provider instability.
- Request batch did not complete.
- Provider returned partial or ambiguous results.

Completion behavior:

- Does not count as complete.
- May be stored for diagnostics, but future runs must still retry.

### `skipped_no_bronze`

The ticker had no existing bronze snapshot during a backfill-only run.

Completion behavior:

- Does not count as complete for full historical coverage.
- The normal seed phase should create bronze first.
- Useful only as a diagnostic state if stored.

## Terminal No-Data Classification

A zero-row result may be marked `no_older_history` only when all of these are true:

- The ticker has an existing bronze `1d.parquet`.
- The oldest existing bronze date was read successfully before the request.
- The provider execution for that ticker returned normally to the script.
- The requested backfill range is known.
- No older bars were returned for that ticker.
- The provider outcome is consistent with no historical data for the requested range.

For IB, acceptable evidence includes repeated `162 HMDS query returned no data` responses for the requested older windows, provided the batch itself completed and the process was not killed or disconnected.

A zero-row result must remain retryable when any of these are true:

- The process was terminated by stall detection.
- The IB connection dropped.
- The provider call raised an exception before all requested windows were attempted.
- The ticker result is missing because its coroutine failed outside normal result handling.
- The script cannot determine the requested older range.
- The bronze file could not be read or validated.

## Runner Behavior

`backfill-all` should consider a preset complete when every ticker is in a terminal complete state for the required timeframe.

Terminal complete statuses:

- `done`
- `no_older_history`

Non-complete statuses:

- missing status
- `retry`
- `skipped_no_bronze`
- unknown status values

Progress logs should distinguish inserted-row completion from terminal no-data completion where practical, for example:

```text
Cursor: 1500/1886 terminal complete (870 inserted, 630 no older history)
```

## Operator Behavior

After this feature:

- Running `python scripts/livewire.py backfill --full` should not repeatedly retry stable no-older-history tickers.
- `--reset` should clear both `done` and `no_older_history` statuses for the selected cursor, matching current reset expectations.
- Existing cursor files should not require manual migration.
- If a cursor is loaded in an old format, it should behave exactly as it does today.
- If a cursor is saved after loading an old format, it may be rewritten in the new versioned format.

## Observability

The implementation should expose enough information for operators to answer:

- How many tickers inserted older rows?
- How many tickers had no older history?
- How many tickers remain retryable?
- Which tickers are repeatedly retrying, and why?

Minimum acceptable observability:

- Cursor entries include status and evidence.
- Backfill command output prints status-specific counts at the end of each batch or run.
- Backfill runner progress counts terminal complete statuses correctly.

Optional later improvement:

```bash
python scripts/livewire.py check --backfill-cursors
```

Expected summary shape:

```text
sp500  501 total  264 done  211 no_older_history  26 retry
ndx100 101 total   35 done   54 no_older_history  12 retry
r2k   1886 total  636 done  980 no_older_history 270 retry
```

## Data Integrity Requirements

- Never write a terminal cursor state before the corresponding provider result is known.
- Never mark `no_older_history` because a parquet write failed.
- Never let cursor status change the contents of bronze parquet.
- Preserve atomic parquet replacement semantics in `BronzeClient`.
- If rows are inserted and cursor save fails, the next run may re-evaluate the ticker; this is acceptable because parquet merge should remain idempotent.

## Migration Requirements

The cursor loader must normalize these input shapes to a common in-memory representation:

- Missing cursor file: empty cursor.
- Old list format: each symbol maps to `1d` status `done`.
- Current timeframe-list format: each listed timeframe maps to status `done`.
- New status format: preserve known status metadata.

Unknown statuses should not count as complete. They should be preserved when possible so operators can inspect them.

## Testing Requirements

Tests must cover:

- Loading old list cursor format.
- Loading current timeframe-list cursor format.
- Loading new status cursor format.
- Saving new status cursor format.
- `is_ticker_complete()` returns true for `done`.
- `is_ticker_complete()` returns true for `no_older_history`.
- `is_ticker_complete()` returns false for `retry`.
- `is_ticker_complete()` returns false for `skipped_no_bronze`.
- IB backfill with inserted rows saves `done`.
- IB backfill with clean terminal zero-row provider result saves `no_older_history`.
- IB backfill with transient provider failure does not save terminal completion.
- `backfill_runner.cursor_completed()` counts both `done` and `no_older_history`.
- `--reset` clears the new cursor format.

Focused test commands:

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/test_fetch_ib_historical.py -q
python -m pytest tests/test_backfill_runner.py -q
```

Full verification command for meaningful script changes:

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing
```

## Acceptance Criteria

- A ticker with existing bronze data and a clean provider no-older-history result is not retried by the next full backfill run.
- A ticker affected by a transient provider or process failure remains retryable.
- Existing cursor files continue to work without manual edits.
- Full backfill progress no longer undercounts terminal no-data tickers.
- Tests prove the cursor migration and status-counting behavior.
- Operator output makes the difference between inserted rows and no older history visible.

