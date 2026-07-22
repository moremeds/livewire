# Improve: retention/compaction for telemetry & quality-audit JSONL

**Item:** M6 · Severity: medium (operational debt) · Status: proposed

## Problem

- `clients/telemetry.py` (`BaseTelemetry._do_write`) and `clients/quality_flags.py`
  (`append_audit`, `:68`) append to `telemetry.jsonl` / `quality_audit.jsonl`
  forever. No rotation/retention anywhere in the repo.
- Every reader — `data_quality_report._iter_jsonl`/`_load_since`
  (`livewire_scripts/data_quality_report.py:58-85`) — parses the **entire file** per
  invocation even for `--since 24h` (the default). Reports run at least daily
  (digest, watchdog paths), so cost grows linearly forever.
- Same disease, different organ: `sync_runner.run_phase` appends per-phase logs to
  static filenames (`daily_backfill_<label>.log`) with no rotation.

## Fix

Retention as a small maintenance subcommand, not inline writer complexity:

1. New `livewire_scripts/jsonl_retention.py` with `prune(path, keep_days)`.
   Timestamp field is `"ts"` (ISO-8601), verified for BOTH writers:
   `clients/telemetry.py:77` (`record.setdefault("ts", _utc_iso())`) and
   `clients/quality_flags.py:80` (`"ts": _utc_iso()`). Parse `row["ts"]` with
   `datetime.fromisoformat`; `cutoff = datetime.now(UTC) - timedelta(days=keep_days)`.
   Stream-read the file, write rows with `ts >= cutoff` to a temp file, `os.replace`
   into place (same atomic pattern as `clients/parquet_io.py`). Archive the
   pruned-out rows: gzip them to `<name>.<YYYY-MM-DD>.gz` beside the file before
   replacing — decision: **yes, archive** (cheap, preserves replayability for
   Postgres `--include-reliability` rebuilds which read these files; silent data
   deletion conflicts with the "replayable publish" design). Lines that don't parse
   as JSON or lack a parseable `ts` are KEPT in the live file (never dropped —
   possible writer-bug evidence). Empty/missing file = no-op.
2. Expose as a `scripts/livewire_quality.py` subcommand by adding
   `"prune-jsonl": "livewire_scripts.jsonl_retention"` to the `COMMANDS` dict
   (`:16-24`); `jsonl_retention.py` must expose `main(argv)` parsing `--keep-days`
   (default 90, env `MDW_JSONL_KEEP_DAYS`). DEFAULT target set: prune BOTH
   `MDW_TELEMETRY_PATH` (default `~/market-warehouse/logs/telemetry.jsonl`) and
   `MDW_QUALITY_AUDIT_PATH` (default `.../quality_audit.jsonl`) — the two
   forever-growing files. Wire into `run_daily_update_job.py` via one more non-fatal
   `_spawn_post_success_quality` call alongside the existing coverage/weekly spawns.
3. Rotate `run_phase` logs by dating the filename (`{label}_{target_date}.log`) in
   `sync_runner.py` — one-line change; note this composes with (and does not
   replace) the offset-scoped success check in
   `fix-sync-runner-success-detection.md`. Old undated logs are left in place;
   operators can delete manually.
4. Reader optimization is **not** needed once pruning bounds file size — skip the
   seek/index complexity entirely. `# ponytail:` full-file parse over a 90-day file
   is fine; revisit only if report latency is ever noticed.

## Preconditions (verify before editing — STOP if any differ)

- `clients/telemetry.py:77` and `clients/quality_flags.py:80` both key on `"ts"`.
  If either no longer writes `"ts"`, STOP (prune cutoff would silently keep/drop
  everything).
- `scripts/livewire_quality.py:16-24` `COMMANDS` dict dispatches to modules exposing
  `main(argv)`. If the dispatch shape differs, STOP.
- `livewire_scripts/sync_runner.py:119` is `log_file = log_dir / f"{label}.log"`.
  Change ONLY the filename to include `target_date`; do NOT touch `run_phase`'s
  return/success-detection logic (owned by `fix-sync-runner-success-detection.md`).

## Files to change

- New: `livewire_scripts/jsonl_retention.py`, `tests/test_jsonl_retention.py`
- `scripts/livewire_quality.py` — register `"prune-jsonl"` in `COMMANDS` (:16-24)
- `livewire_scripts/run_daily_update_job.py` — one more post-success spawn
- `livewire_scripts/sync_runner.py` — dated phase-log filenames

## Tests

- Prune: rows straddling the cutoff → old rows gone from live file, present in the
  gz archive; corrupt/ts-less lines preserved in live file (never silently dropped —
  they may be evidence of a writer bug); empty/missing file is a no-op.
- Atomicity: temp+replace (assert no partial live file on injected write failure).
- Spawn wiring: mirror `test_coverage_and_weekly_spawned_after_successful_daily`.
- sync_runner: log filename includes the target date.

## Verification

- `uv run pytest tests/test_jsonl_retention.py -v` → new prune/atomicity/archive
  tests pass.
- Spawn wiring + sync_runner:
  `uv run pytest tests/test_run_daily_update_job.py -v -W error::RuntimeWarning`
  (mirror `test_coverage_and_weekly_spawned_after_successful_daily`) and
  `uv run pytest tests/test_sync_runner.py -v` → pass.
- Global gate:
  `uv run pytest tests/ -v -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing`
  → exit 0, coverage ≥ 95% (the 2 time-bomb integration tests hang the full run —
  always exclude them).

STOP condition: if any gate fails for a reason other than tests this plan adds, revert
and report — do not lower thresholds or deselect additional tests.

## Risks / notes

- Concurrent append during prune: writer holds `open("a")` per-write, not a
  persistent handle; the `os.replace` window could drop a row written between
  read-end and replace. Run prune from the post-success chain (after ingest lanes
  finish) to make the window practically empty; accept the residual race for
  telemetry (observability data, not market data) and say so in the docstring.

## Acceptance criteria

- After prune, live JSONL contains only the retention window + unparseable lines;
  archives hold the rest; `report --since 24h` output identical before/after prune.
