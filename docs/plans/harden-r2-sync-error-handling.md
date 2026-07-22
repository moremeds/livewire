# Fix: R2 sync — dead-code exit logic, no per-file error handling

**Item:** M5 · Severity: medium · Status: proposed

## Problem

`livewire_scripts/sync_to_r2.py`:

1. `main()` ends with `return 0 if count >= 0 else 1` — `count` is a non-negative
   accumulator; the failure branch is unreachable dead code.
2. `upload()`/`download()` have no try/except around `s3.upload_file` /
   `s3.download_file` / (non-404) `head_object` paths: one transient network error on
   file N of thousands raises out of the loop, aborting the whole sync with a raw
   traceback and no summary of what completed. (`_remote_size` at `:51` already
   handles 404/NoSuchKey; other errors re-raise by design — that re-raise then kills
   the run.)

## Fix

Per-file resilience + honest exit code:

1. Wrap the per-file transfer body in try/except (`botocore.exceptions.ClientError`,
   `BotoCoreError`, `OSError`): log the key + error, increment a `failed` counter,
   continue to the next file.
2. `upload()`/`download()` return `(uploaded, failed)` (a 2-tuple of ints). This is
   a breaking return-contract change: ALL existing tests that assert the int return
   must be updated to unpack — in `tests/test_sync_to_r2.py`, every
   `count = upload(...)` / `count = download(...)` assertion across `TestUpload`,
   `TestDownload`, `TestMultiTimeframeSync`, `TestIncrementalUpload`,
   `TestIncrementalDownload` (~16 call sites; enumerate with
   `grep -n "= upload(\|= download(" tests/test_sync_to_r2.py`) becomes
   `uploaded, _failed = ...` asserting on `uploaded`. `TestMain` mocks
   `upload`/`download` with int `return_value`s — update those mocks to return
   `(N, 0)` tuples and assert the new `main` exit contract. Add the
   `botocore.exceptions` import (`ClientError`, `BotoCoreError`) at module top —
   the module currently only imports `boto3` lazily inside `_get_s3_client`.
3. `main()` prints a summary line (`synced=N skipped=M failed=K`) and returns
   `1 if failed else 0` — replacing the dead-code expression.
4. No retry logic — the sync is idempotent and incremental (size-compare skip
   already implemented per `TestIncrementalUpload`); the next scheduled run picks up
   stragglers. `# ponytail:` no per-file retry; rerun is the retry.

## Preconditions (verify before editing — STOP if any differ)

- `livewire_scripts/sync_to_r2.py:185` is `return 0 if count >= 0 else 1`. If it
  already inspects a failed counter, STOP (partially done).
- `upload` (:66) and `download` (:114) currently return a bare int and have no
  try/except around `s3.upload_file` / `s3.download_file`. If they already catch
  per-file, STOP.
- `_remote_size` (:54-63) re-raises non-404 errors — keep that; catch it at the
  per-file level, not inside `_remote_size`.

## Stop conditions

- No retry logic (the sync is idempotent; the next scheduled run is the retry).
- Do not change the size-compare skip logic (`TestIncrementalUpload` guards it).

## Files to change

- `livewire_scripts/sync_to_r2.py`
- `tests/test_sync_to_r2.py` (return-contract unpacking, ~16 call sites + TestMain mocks)

## Tests

`tests/test_sync_to_r2.py` (existing classes give the harness):

- New: `test_upload_continues_after_single_failure` — 3 files, stub raises
  `ClientError` on the 2nd → files 1 and 3 uploaded, return reflects 1 failure,
  `main` exits 1.
- New: `test_download_continues_after_single_failure` — same for download.
- Update `TestMain` exit-code assertions AND every int-return assertion listed in
  Fix step 2 (~16 call sites) to unpack `(uploaded, failed)`.

## Verification

- `uv run pytest tests/test_sync_to_r2.py -v` → all pass, including the two new
  "continues after single failure" tests and the updated unpacking.
- Global gate:
  `uv run pytest tests/ -v -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing`
  → exit 0, coverage ≥ 95% (the 2 time-bomb integration tests hang the full run —
  always exclude them).

STOP condition: if any gate fails for a reason other than the test updates this plan
enumerates, revert and report — do not lower thresholds or deselect additional tests.

## Risks / notes

- Exit-code contract change: previously a mid-run exception surfaced as an uncaught
  traceback (interpreter exit 1); now it's a controlled exit 1 with a summary. Any
  caller (launchd/cron wrapper, if one exists — none found in inventory) sees the
  same non-zero result.
- Keep `_remote_size`'s existing re-raise for non-404 errors, but catch it at the
  per-file level like the transfer calls.

## Acceptance criteria

- A single flaky object no longer aborts the sync; the run reports and exits
  non-zero only when at least one file actually failed.
