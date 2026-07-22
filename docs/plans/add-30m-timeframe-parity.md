# Fix: 30m timeframe missing from weekly report and Postgres rebuild

**Item:** M4 · Severity: medium · Status: proposed

## Problem

`30m` is a first-class bronze timeframe — tracked by coverage
(`coverage_report.py:46` `TIMEFRAMES = ("1d","1m","1h","5m","30m")`, test
`test_tracks_30m_timeframe`) and synced to R2 (`sync_to_r2.py:21-27`) — but two
downstream consumers never got it:

1. `livewire_scripts/weekly_quality_summary.py:36` — `_TIMEFRAMES = ("1d", "1m",
   "1h", "5m")`; `_HEADER_RE` (`:40-47`) and `_MISSING_RE` (`:49-50`) have no `30m`
   group. The weekly markdown silently omits a whole timeframe's coverage trend and
   persistent-gap detection.
2. `livewire_scripts/rebuild_postgres_from_parquet.py:34-38` — `--timeframe`
   choices `["1d","1m","1h","5m","all"]`; `_rebuild_equity_timeframes` has no 30m
   dispatch. Postgres can never publish 30m bars.

## Fix

1. **Weekly:** add `30m` to `_TIMEFRAMES`, `_HEADER_RE`, `_MISSING_RE`. The header
   regex is positional. Verified: `format_one_liner` (`coverage_report.py:180-186`)
   iterates `TIMEFRAMES`, so the one-liner order is `1d 1m 1h 5m 30m` — **30m is
   LAST, after 5m**. The regex must tolerate **both** old lines (no 30m, for
   historical logs the weekly parser re-reads) and new lines. DEFAULT: extend the
   positional `_HEADER_RE` (`weekly_quality_summary.py:40-46`) with an OPTIONAL
   trailing 30m group appended AFTER the `5m` group — the existing optional `1m`
   group is the pattern to copy. Then in `parse_coverage_log` (:72-78) add the
   corresponding optional-group extraction for `totals["30m"]`. Do NOT rewrite to a
   findall approach (larger diff; not needed).
2. **Postgres rebuild.** Five edits — a literal implementation missing any one ships
   a runtime error:
   - `clients/postgres_schema.py` — **REQUIRED (was omitted from the original
     plan).** Add `"equities_30m"` to `POSTGRES_TABLES` (`:8-17`, after
     `"equities_5m"`) AND add a `CREATE TABLE IF NOT EXISTS {schema}.equities_30m`
     block in `iter_schema_statements` mirroring the `equities_5m` block
     byte-for-byte (`:90-100`, same 7 columns, same
     `PRIMARY KEY (bar_timestamp, symbol_id)`). Without this the loader's
     `INSERT INTO {schema}.equities_30m` (table name is `equities_{timeframe}`,
     `postgres_client.py:214`) hits a missing relation at runtime.
   - `rebuild_postgres_from_parquet.py:37` — add `"30m"` to `--timeframe` choices.
   - `rebuild_postgres_from_parquet.py:84` — add `"30m"` to the loop tuple
     `for intraday_tf in ("1m", "1h", "5m"):` (there is NO per-timeframe branch to
     mirror; the loop already handles skip-if-missing and the `all` case).
   - `clients/postgres_client.py:212` — add `"30m"` to
     `if timeframe not in ("1m", "1h", "5m"):`. Table name is `equities_{timeframe}`
     → `equities_30m` (no extra mapping needed).
   - `rebuild_postgres_from_parquet.py:99` and `:108` — add `"30m"` /
     `"30m.parquet"` to those whitelists.
3. Grep for any other timeframe whitelists missing 30m
   (`grep -rn '"5m"' --include='*.py' | grep -v 30m`) and fix in the same pass.
   `warehouse_health_report.py:32,268,1141` will hit. DEFAULT: OUT OF SCOPE — leave
   them at the 1m/5m/1h trio (the health report's interior-gap detection targets
   IB-fetched intraday; 30m equity bars are derived by Massive flatfile aggregation).
   State "health report 30m: out of scope, report-only surface" in the PR
   description. Do not edit those three lines.

## Preconditions (verify before editing — STOP if any differ)

- `clients/postgres_schema.py:8-17` lists tables through `"equities_5m"` with no
  `equities_30m`; CREATE blocks at `:90-100` define `equities_5m`. If `equities_30m`
  already exists, STOP (partially done).
- `livewire_scripts/rebuild_postgres_from_parquet.py:84` is
  `for intraday_tf in ("1m", "1h", "5m"):`. If it differs, STOP and report.
- `livewire_scripts/weekly_quality_summary.py:72-78` builds `totals` from positional
  regex groups (with an optional `1m` group). If the structure differs, STOP.

## Files to change

- `livewire_scripts/weekly_quality_summary.py`
- `livewire_scripts/rebuild_postgres_from_parquet.py` — choices (:37), loop tuple
  (:84), two whitelists (:99, :108)
- `clients/postgres_schema.py` — `equities_30m` table (list entry + DDL block)
- `clients/postgres_client.py` — 30m in the intraday whitelist (:212)
- (any stragglers the grep finds)

## Tests

- `tests/test_weekly_quality_summary.py` — new-format line with 30m parses all five
  timeframes; old-format line (no 30m) still parses (backward compat); 30m appears
  in the rendered trend table and missing-symbol section.
- `tests/test_rebuild_postgres_from_parquet.py` — `--timeframe 30m` calls the loader
  with the 30m bronze path; `all` includes 30m; missing 30m parquet with explicit
  `--timeframe 30m` raises (mirror `test_missing_explicit_1m_timeframe_raises`).
- `tests/test_postgres_client.py` — a direct (non-mocked-through-rebuild) test that
  the loader accepts `30m` and targets the right table; without it the
  `postgres_client.py:212` whitelist regression can silently return.
  IMPORTANT: live-DB tests in that file are `@pytest.mark.integration` and are
  SKIPPED by the CI gate. The guards that MUST run as non-integration unit tests:
  (a) `iter_schema_statements("md")` yields a `CREATE TABLE ... md.equities_30m`
  statement; (b) `replace_equities_intraday_from_parquet` raises `ValueError` for an
  unknown timeframe but NOT for `"30m"`. Add both as plain unit tests.

## Verification

- `uv run pytest tests/test_weekly_quality_summary.py -v` → pass (both-formats
  backward compat included).
- `uv run pytest tests/test_rebuild_postgres_from_parquet.py tests/test_postgres_client.py -v -m "not integration"`
  → pass, including the two non-integration schema/whitelist guards above.
- Global gate:
  `uv run pytest tests/ -v -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing`
  → exit 0, coverage ≥ 95% (the 2 time-bomb integration tests hang the full run —
  always exclude them).

STOP condition: if any gate fails for a reason other than tests this plan adds, revert
and report — do not lower thresholds or deselect additional tests.

## Risks / notes

- Weekly parser must not break on pre-30m historical coverage logs — the
  both-formats test is mandatory, since `weekly` re-reads the previous ISO week's
  seven daily logs.
- Postgres `all` rebuild gets one more conditional table load; rebuilds are
  replayable by design, no migration concern.

## Acceptance criteria

- Weekly report renders a 30m column for weeks whose logs contain it and doesn't
  error on weeks that don't.
- `rebuild-postgres --asset-class equity --timeframe 30m` publishes 30m bars.
