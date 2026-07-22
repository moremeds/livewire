# Improve: scope intraday coverage auto-recovery (or make day-scope explicit)

**Item:** M3 · Severity: medium · Status: proposed

## Problem

`auto_recover` (`livewire_scripts/coverage_report.py:214-269`):

- `1d`: targeted — `daily --source massive --force --target-date … --tickers
  <missing…>`.
- Any intraday timeframe: `flatfile-ingest repair --dates <date>` with **no symbol
  scope** — re-downloads and republishes the entire whole-market day even when 1
  symbol of 13,000 is missing. The safety cap (`DEFAULT_SAFETY_CAP = 100`,
  `coverage_report.py:48`) only gates whether the subprocess launches, not how much
  work it does.

`ingest_flatfiles.py` has no `--tickers` plumbing anywhere (parser at `:44-56`;
`download_dates`/`publish_dates` at `:89-98` take no symbol filter), so a scoped
repair requires new plumbing through the store — the flat files are whole-market by
nature, so the *download* is inherently day-scoped; only the *publish* fan-out can be
scoped.

Also untested: `tests/test_coverage_report.py` asserts the 1d command's `--tickers`
but nothing about the intraday command's shape.

## Fix

Split the honest constraint from the fixable waste:

1. **Download stays day-scoped** (whole-market SIP file — no choice).
2. **Publish becomes symbol-scopable.** Add `--tickers` to `ingest_flatfiles.py`
   repair mode, threaded into `publish_dates(..., symbols=set[str] | None)`. The
   publish fan-out seam is `livewire_scripts/flatfile_publisher.py::publish_dates`
   → `_process_bucket`'s per-ticker loop — **not** a `clients/` store method (the
   store only scans/stages); the symbol filter belongs in `flatfile_publisher.py`.
   Buckets/symbols outside the set are skipped. Default
   `None` = current full-universe behavior; only `repair` accepts the flag (reject
   it for other modes to preserve "modes operate on every symbol" semantics
   documented in CLAUDE.md — update that sentence to carve out repair).
3. `coverage_report.auto_recover` intraday branch passes `--tickers
   *missing_symbols` (it already holds the list; cap already bounds argv length to
   100).
4. Pin the command shapes in tests either way.

Fallback trigger (measurable): after implementing step 2, run
`git diff --stat livewire_scripts/flatfile_publisher.py livewire_scripts/ingest_flatfiles.py`.
If the combined changed-line count for those two files exceeds 80, ABANDON the
scoped-publish approach and take the fallback: keep day-scoped repair, add the
scope sentence to CLAUDE.md documenting it as by-design, and add ONLY the
command-shape test (`test_intraday_recovery_command_shape`). Record which path was
taken in the PR description. Default target is the scoped-publish path.

## Preconditions (verify before editing — STOP if any differ)

- `livewire_scripts/coverage_report.py:248-256` — intraday branch builds
  `flatfile-ingest repair --dates <date>` with NO `--tickers`. If it already passes
  `--tickers`, STOP (work already done).
- `livewire_scripts/flatfile_publisher.py:56-65` — `publish_dates(...)` signature as
  currently defined. If the signature differs materially from the plan's description,
  STOP and report.
- `livewire_scripts/flatfile_publisher.py:72-114` — `_process_bucket`'s per-ticker
  loop at :81 is the filter seam. If absent, STOP.
- `livewire_scripts/ingest_flatfiles.py:44-57` — argparse block has no `--tickers`.
  If present, STOP.

## Stop conditions

- Do NOT add symbol scoping to the download path or to `store` staging methods (the
  whole-market SIP file is inherently day-scoped — download bandwidth is fixed).
- Do NOT accept `--tickers` for `backfill`/`catch-up`/`discover`; reject with a
  parser error to preserve "modes operate on every symbol" semantics.

## Files to change

- `livewire_scripts/ingest_flatfiles.py` — `--tickers` (repair only), thread through
- `livewire_scripts/flatfile_publisher.py` — `publish_dates` / `_process_bucket`
  symbol filter
- `livewire_scripts/coverage_report.py` — intraday command construction
- `CLAUDE.md` — repair-mode scoping sentence

## Tests

- `tests/test_coverage_report.py::TestAutoRecover` — new
  `test_intraday_recovery_passes_missing_tickers` asserting full argv.
- `tests/test_ingest_flatfiles.py` — repair with `--tickers` publishes only those
  symbols; `--tickers` rejected for `backfill`/`catch-up`; no-flag repair unchanged.

## Risks / notes

- Raw download bandwidth is unchanged (whole-market file regardless); the win is
  publish CPU/IO — from ~13K symbol republishes to |missing|.
- Symbol names go through argv; cap of 100 keeps argv far under limits.

## Verification

- Unit checks:
  `uv run pytest tests/test_ingest_flatfiles.py tests/test_coverage_report.py -v`
  → all pass, including the new `--tickers` rejection and scoped-publish tests and
  `test_intraday_recovery_passes_missing_tickers`.
- Global gate (excludes the 2 time-bomb integration tests that hang the full run):
  `uv run pytest tests/ -v -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing`
  → exit 0 and coverage ≥ 95%.

STOP condition: if any gate fails for a reason other than tests this plan adds, revert
and report — do not lower thresholds or deselect additional tests.

## Acceptance criteria

- Coverage recovery for 3 missing intraday symbols republishes exactly 3 symbols
  (or, if fallback chosen: behavior documented + command shape pinned by test).
