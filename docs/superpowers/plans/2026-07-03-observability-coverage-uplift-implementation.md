# Livewire Observability & Coverage Uplift — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the nightly pipeline report the truth — per-ticker outcomes classified correctly (no-trade ≠ failure), one trustworthy nightly digest instead of noisy false alarms, working coverage tracking, and the datalake gaps closed (futures seeded, cmdty/fx owned, dead experiments archived).

**Architecture:** Fix outcome semantics at the source (`daily_update.py`) and emit one machine-readable `SUMMARY_JSON` line per run; every downstream consumer (wrapper retries, failure emails, digest) parses that line instead of regexing prose. Coverage moves to parquet-footer statistics scoped to the active bronze universe and gets scheduled again. Alert emails collapse to: one nightly digest on success, rare truthful failure mail on systemic failure only.

**Tech Stack:** Python 3.13 (`~/market-warehouse/.venv`), pyarrow, pytest (+`responses`), Node/Nodemailer for email (`livewire_node/`), macOS launchd.

**Operator decisions applied (2026-07-03):** daily equity universe = full active bronze (~12.9K); futures seeded with GC (COMEX gold), CL (NYMEX WTI), BZ (NYMEX Brent — verify contract qualifies in IB at execution time); Cerebras enrichment removed; raw-file retention deferred (keep all raw, surface disk headroom in digest); cmdty/fx kept and given a sync lane; option_chain_snapshot archived out of bronze.

## Global Constraints

- Coverage gate: 100% (`pyproject.toml fail_under = 100`) for included sources; every new/changed module in `clients/` and `livewire_scripts/` needs tests in `tests/test_<module>.py`.
- Mock all external I/O in tests (IB via `MagicMock`, HTTP via `responses`, parquet via tmp paths). No network at test time; real tickers with frozen real values in fixtures.
- No `Co-Authored-By`/AI trailers in commits.
- One phase = one PR; **never merge before CI is green**; branch per phase (workflow below).
- All schedule documentation in UTC/ET, never HKT.
- Run `python -m pytest tests/ -v -W error::RuntimeWarning` at least once per phase before committing when tests mock async runners.
- Test command (from worktree root): `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing`

## Worktree / PR workflow

The worktree lives at `.worktrees/uplift-obs-coverage/` (branch `uplift/obs-coverage`). Phases land as sequential PRs from this one worktree:

```bash
cd .worktrees/uplift-obs-coverage
# per phase:
git checkout -b uplift/p<N>-<slug> origin/main   # after fetching
# ... do the phase's tasks, commit ...
git push -u origin uplift/p<N>-<slug>
gh pr create --fill
# wait CI green → merge → git fetch origin → next phase branch from origin/main
```

Phase P0 uses branch `uplift/p0-land-otc-archive`, P1 `uplift/p1-outcome-semantics`, P2 `uplift/p2-coverage-truth`, P3 `uplift/p3-nightly-digest`, P4 `uplift/p4-datalake-hygiene`, P5 `uplift/p5-lane-consolidation`.

---

## Phase P0 — Land in-flight OTC-archive work

### Task P0.1: Add `.worktrees/` to `.gitignore`

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1:** Append line `.worktrees/` to `.gitignore`.
- [ ] **Step 2:** `git status` — confirm `.worktrees/` no longer shows as untracked in the primary checkout.
- [ ] **Step 3:** Commit: `git add .gitignore && git commit -m "chore: ignore .worktrees/"`

### Task P0.2: Import and land the uncommitted OTC-archive changes

The in-flight files live UNCOMMITTED in the **primary checkout** at `/Users/moremeds/projects/livewire/` (not visible in the worktree). Copy them in, review, test, commit.

**Files:**
- Create (copy from primary checkout): `livewire_scripts/archive_otc_symbols.py`, `tests/test_archive_otc_symbols.py`
- Modify (copy from primary checkout): `livewire_scripts/flatfile_publisher.py`, `clients/intraday_bronze_client.py`, `tasks/archive.md`, `tests/test_flatfile_publisher.py`

- [ ] **Step 1:** Copy the six paths from `/Users/moremeds/projects/livewire/` into the worktree, e.g.:

```bash
PRIMARY=/Users/moremeds/projects/livewire
for f in livewire_scripts/archive_otc_symbols.py tests/test_archive_otc_symbols.py \
         livewire_scripts/flatfile_publisher.py clients/intraday_bronze_client.py \
         tasks/archive.md tests/test_flatfile_publisher.py; do
  cp "$PRIMARY/$f" "$f"
done
```

- [ ] **Step 2:** Read the full diff (`git diff` + the two new files) and confirm: `archive_otc_symbols.py` moves bronze symbol dirs absent from the SIP day_aggs universe to `bronze-delisted/`, is idempotent, and has a `--dry-run` mode.
- [ ] **Step 3:** Run the full suite with coverage. Expected: PASS, coverage 100% for included sources. If `archive_otc_symbols.py` is not yet included by `pyproject.toml` coverage config, confirm its tests still exercise it fully.
- [ ] **Step 4:** Confirm `scripts/livewire_store.py` actually wires the `archive-otc` subcommand the docstring promises; if not, add the subcommand dispatch (mirror how other `livewire_store.py` subcommands dispatch into `livewire_scripts/`) with a test.
- [ ] **Step 5:** Commit (`feat(store): archive non-SIP OTC symbols to bronze-delisted`), push, open PR, wait CI, merge.
- [ ] **Step 6 (ops, after merge):** From the primary checkout on updated main: `python scripts/livewire_store.py archive-otc --dry-run`, review the list, then run without `--dry-run`. Then clean the now-landed uncommitted files out of the primary working tree (`git status` should be clean).

---

## Phase P1 — Daily update outcome semantics (kills the false alarms)

### Task P1.1: `daily_outcomes` module — summary schema, exit policy, parser

**Files:**
- Create: `livewire_scripts/daily_outcomes.py`
- Test: `tests/test_daily_outcomes.py`

**Interfaces (produced, used by P1.2/P1.3/P3):**
- `SUMMARY_PREFIX: str = "SUMMARY_JSON "`
- `build_summary_line(job, asset_class, source, target_date, updated, no_trade, partial, errors, bars_inserted, validation_issues, top_errors) -> str` — returns `SUMMARY_PREFIX + json.dumps(...)`; `top_errors` is `list[tuple[str, int]]`.
- `parse_last_summary_json(text: str) -> dict | None` — last `SUMMARY_JSON` line in a log text, parsed; `None` if absent/corrupt.
- `resolve_exit_code(updated: int, no_trade: int, partial: int, errors: int) -> int` — 1 only when `errors > max(50, 0.05 * processed)` where `processed = updated + no_trade + partial + errors`, or when `errors > 0 and updated == 0 and processed > 0`; else 0. `no_trade`/`partial` never fail a run.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for livewire_scripts.daily_outcomes."""
import json

from livewire_scripts.daily_outcomes import (
    SUMMARY_PREFIX,
    build_summary_line,
    parse_last_summary_json,
    resolve_exit_code,
)


def _line(**kw):
    base = dict(
        job="daily_update", asset_class="equity", source="massive",
        target_date="2026-07-02", updated=9091, no_trade=277, partial=95,
        errors=0, bars_inserted=9186, validation_issues=0, top_errors=[],
    )
    base.update(kw)
    return build_summary_line(**base)


def test_build_summary_line_round_trips():
    line = _line(top_errors=[("HTTP 500 from Massive", 12)])
    assert line.startswith(SUMMARY_PREFIX)
    payload = json.loads(line[len(SUMMARY_PREFIX):])
    assert payload["updated"] == 9091
    assert payload["no_trade"] == 277
    assert payload["top_errors"] == [["HTTP 500 from Massive", 12]]


def test_parse_last_summary_json_returns_last_line():
    text = "\n".join(["noise", _line(updated=1), "more", _line(updated=2)])
    assert parse_last_summary_json(text)["updated"] == 2


def test_parse_last_summary_json_none_when_absent_or_corrupt():
    assert parse_last_summary_json("no summary here") is None
    assert parse_last_summary_json(SUMMARY_PREFIX + "{not json") is None


def test_no_trade_and_partial_never_fail():
    assert resolve_exit_code(updated=0, no_trade=277, partial=95, errors=0) == 0


def test_small_error_count_tolerated():
    assert resolve_exit_code(updated=9091, no_trade=277, partial=0, errors=50) == 0


def test_error_rate_over_threshold_fails():
    # errors=600 of 10000 processed > max(50, 500) -> fail
    assert resolve_exit_code(updated=9000, no_trade=400, partial=0, errors=600) == 1


def test_zero_updates_with_errors_fails():
    assert resolve_exit_code(updated=0, no_trade=0, partial=0, errors=3) == 1


def test_all_updated_ok():
    assert resolve_exit_code(updated=10, no_trade=0, partial=0, errors=0) == 0
```

- [ ] **Step 2:** Run: `python -m pytest tests/test_daily_outcomes.py -v` — expect FAIL (module missing).
- [ ] **Step 3: Implement**

```python
"""Shared outcome schema for daily jobs.

One machine-readable SUMMARY_JSON line per run is the contract between
daily_update.py (producer) and the wrapper / digest (consumers).
"""
from __future__ import annotations

import json

SUMMARY_PREFIX = "SUMMARY_JSON "

_ERROR_ABS_TOLERANCE = 50
_ERROR_RATE_TOLERANCE = 0.05


def build_summary_line(
    *,
    job: str,
    asset_class: str,
    source: str,
    target_date: str,
    updated: int,
    no_trade: int,
    partial: int,
    errors: int,
    bars_inserted: int,
    validation_issues: int,
    top_errors: list[tuple[str, int]],
) -> str:
    payload = {
        "job": job,
        "asset_class": asset_class,
        "source": source,
        "target_date": target_date,
        "updated": updated,
        "no_trade": no_trade,
        "partial": partial,
        "errors": errors,
        "bars_inserted": bars_inserted,
        "validation_issues": validation_issues,
        "top_errors": [[msg, count] for msg, count in top_errors],
    }
    return SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":"))


def parse_last_summary_json(text: str) -> dict | None:
    result = None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith(SUMMARY_PREFIX):
            continue
        try:
            result = json.loads(stripped[len(SUMMARY_PREFIX):])
        except json.JSONDecodeError:
            continue
    return result


def resolve_exit_code(*, updated: int, no_trade: int, partial: int, errors: int) -> int:
    processed = updated + no_trade + partial + errors
    if errors == 0:
        return 0
    if updated == 0 and processed > 0:
        return 1
    if errors > max(_ERROR_ABS_TOLERANCE, _ERROR_RATE_TOLERANCE * processed):
        return 1
    return 0
```

- [ ] **Step 4:** Run the test file — expect PASS. Run full suite + coverage.
- [ ] **Step 5:** Commit: `feat(daily): add daily_outcomes summary schema and exit policy`

### Task P1.2: Reclassify outcomes in `daily_update.py` (Massive path)

**Files:**
- Modify: `livewire_scripts/daily_update.py` (counters `:710-711`, Massive loop `:726-771`, summary block `:773-787`)
- Test: `tests/test_daily_update.py` (extend)

**Interfaces:**
- Consumes: `daily_outcomes.build_summary_line`, `daily_outcomes.resolve_exit_code`.
- Produces: log lines `"{ticker}: no trade (no bars returned)"`, `"{ticker}: N bars published, older gaps remain: <dates>"`, `"{ticker}: ERROR <msg>"`; a `SUMMARY_JSON` line and outcome-class counts in the human summary table.

- [ ] **Step 1: Write failing tests** (extend `tests/test_daily_update.py`, following its existing fixture style for the massive path — it already mocks `MassiveClient`):

```python
def test_massive_no_bars_counts_no_trade_and_exits_zero(massive_daily_env):
    """A ticker with zero bars returned is no_trade, not failure; run exits 0."""
    # arrange: MassiveClient mock returns [] for ticker "AACIU", real bars for "AAPL"
    exit_code, log = run_daily_massive(massive_daily_env, tickers=["AAPL", "AACIU"])
    assert exit_code == 0
    assert "AACIU: no trade (no bars returned)" in log
    summary = parse_last_summary_json(log)
    assert summary["no_trade"] == 1 and summary["updated"] == 1 and summary["errors"] == 0


def test_massive_partial_counts_partial_and_exits_zero(massive_daily_env):
    """Target-day bar published but older gap remains -> partial, exit 0."""
    ...
    assert summary["partial"] == 1


def test_massive_exception_counts_error(massive_daily_env):
    """fetch raising -> errors count; single error among successes still exit 0."""
    ...
    assert summary["errors"] == 1


def test_massive_all_errors_exits_one(massive_daily_env):
    ...
    assert exit_code == 1
```

(Write these as four complete tests using the existing test-file helpers; if `tests/test_daily_update.py` has no massive-path fixture yet, add one `massive_daily_env` fixture that patches `MassiveClient`, `BronzeClient`, tmp bronze root, and captures console output — mirror the file's existing IB-path fixtures.)

- [ ] **Step 2:** Run them — expect FAIL (messages/summary not produced yet).
- [ ] **Step 3: Implement.** In `daily_update.py`:

Replace counters at `:710-711`:

```python
        tickers_updated = 0
        tickers_no_trade = 0
        tickers_partial = 0
        tickers_error = 0
        error_messages: Counter[str] = Counter()
```

(add `from collections import Counter` and `from livewire_scripts.daily_outcomes import build_summary_line, resolve_exit_code` at top.)

Wrap the per-ticker body (`for ticker, _duration in batch:` block, currently `:726-771`) in `try/except`:

```python
                    for ticker, _duration in batch:
                        try:
                            latest = date.fromisoformat(latest_dates[ticker])
                            bars, _sources = fetch_massive_bars(...)
                            ...  # existing body
                        except Exception as exc:  # noqa: BLE001 - per-ticker isolation
                            tickers_error += 1
                            error_messages[f"{type(exc).__name__}: {exc}"] += 1
                            console.print(f"  [red]{ticker}[/red]: ERROR {exc}")
                            continue
```

Replace the no-bars branch (`:737-740`):

```python
                        if not valid_bars:
                            console.print(f"  [dim]{ticker}[/dim]: no trade (no bars returned)")
                            tickers_no_trade += 1
                            continue
```

Replace the still-missing branch (`:759-766`):

```python
                        if remaining_dates:
                            console.print(
                                f"  [yellow]{ticker}[/yellow]: {inserted} bar"
                                f"{'s' if inserted != 1 else ''} published, older gaps remain: "
                                f"{', '.join(d.isoformat() for d in remaining_dates)}"
                            )
                            tickers_partial += 1
                            continue
```

Replace the summary block (`:773-787`):

```python
            console.print(f"\n{'═' * 60}")
            console.print("[bold]Daily Update Complete[/bold]")
            console.print(f"  Tickers updated:    {tickers_updated}")
            console.print(f"  Tickers no-trade:   {tickers_no_trade}")
            console.print(f"  Tickers partial:    {tickers_partial}")
            console.print(f"  Tickers error:      {tickers_error}")
            console.print(f"  Source massive:     {source_counts.get('massive', 0)}")
            console.print(f"  Bars inserted:      {total_inserted}")
            console.print(f"  Bars validated:     {total_validated}")
            console.print(f"  Validation issues:  {len(total_issues)}")
            console.print()
            print(build_summary_line(
                job="daily_update", asset_class=asset_class, source="massive",
                target_date=target.isoformat(), updated=tickers_updated,
                no_trade=tickers_no_trade, partial=tickers_partial,
                errors=tickers_error, bars_inserted=total_inserted,
                validation_issues=len(total_issues),
                top_errors=error_messages.most_common(3),
            ))
            return resolve_exit_code(
                updated=tickers_updated, no_trade=tickers_no_trade,
                partial=tickers_partial, errors=tickers_error,
            )
```

(Use plain `print` for the JSON line so Rich markup can't mangle it.)

- [ ] **Step 4:** Run new tests — PASS. Run full suite; fix any test asserting the old "Tickers failed" wording or old exit semantics (update those assertions to the new contract — they encode the bug we're fixing).
- [ ] **Step 5:** Commit: `fix(daily): classify no-trade/partial outcomes; threshold-based exit code`

### Task P1.3: Same classification for the IB path

**Files:**
- Modify: `livewire_scripts/daily_update.py:789-943` (IB path loop and its summary/exit)
- Test: `tests/test_daily_update.py` (extend existing IB-path tests)

- [ ] **Step 1:** Read `daily_update.py:789-943`. Build the mapping table and apply it to every `tickers_failed += 1` site in that section:
  - fetch returned zero bars and fallback not attempted/failed with no HTTP/exception error → `tickers_no_trade += 1`
  - target day filled but older gaps remain → `tickers_partial += 1`
  - exception / explicit fetch error / fallback exception → `tickers_error += 1` + `error_messages[...] += 1`
- [ ] **Step 2:** Write one failing test per mapping row against the IB path (the file's existing IB fixtures with `MagicMock` IB client), asserting counter classification and `resolve_exit_code` behavior, then implement, mirroring Task P1.2's summary block: replace the IB path's `if tickers_failed > 0: return 1` (`:941-943`) with the same `build_summary_line(...)` + `resolve_exit_code(...)` ending (`source="ib"`).
- [ ] **Step 3:** Full suite + `-W error::RuntimeWarning` (IB tests mock `ib.ib.run`). PASS.
- [ ] **Step 4:** Commit: `fix(daily): outcome classification for IB path`

### Task P1.4: Truthful failure summary in the wrapper

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py:186-249` (`extract_error_summary`)
- Test: `tests/test_run_daily_update_job.py` (extend; file exists — check name via `ls tests | grep run_daily`)

- [ ] **Step 1: Failing tests**

```python
from livewire_scripts.daily_outcomes import build_summary_line


def test_extract_error_summary_prefers_summary_json(tmp_path):
    log = tmp_path / "daily_update_2026-07-03.log"
    line = build_summary_line(
        job="daily_update", asset_class="equity", source="massive",
        target_date="2026-07-02", updated=9091, no_trade=277, partial=95,
        errors=12, bars_inserted=9186, validation_issues=0,
        top_errors=[("ConnectionError: Massive timeout", 12)],
    )
    log.write_text("  AAPL: 1 bar published from Massive\n" + line + "\n")
    summary = extract_error_summary(log)
    assert "updated=9091" in summary
    assert "no_trade=277" in summary
    assert 'dominant error (12x): "ConnectionError: Massive timeout"' in summary
    assert "1 bar published" not in summary  # success lines never surface as errors


def test_extract_error_summary_legacy_fallback_no_ticker_counting(tmp_path):
    log = tmp_path / "x.log"
    log.write_text("  AAPL: 1 bar published from Massive\nsome tail line\n")
    assert extract_error_summary(log) == "some tail line"
```

- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3: Replace `extract_error_summary` entirely:**

```python
def extract_error_summary(log_file: Path) -> str:
    try:
        text = log_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "Daily update failed, and the log file was not found."

    from livewire_scripts.daily_outcomes import parse_last_summary_json

    summary = parse_last_summary_json(text)
    if summary is not None:
        parts = [
            f"updated={summary.get('updated', 0)}",
            f"no_trade={summary.get('no_trade', 0)}",
            f"partial={summary.get('partial', 0)}",
            f"errors={summary.get('errors', 0)}",
            f"target_date={summary.get('target_date', '?')}",
            f"source={summary.get('source', '?')}",
            f"asset_class={summary.get('asset_class', '?')}",
        ]
        top = summary.get("top_errors") or []
        if top:
            msg, count = top[0]
            parts.append(f'dominant error ({count}x): "{msg}"')
        return "Daily update failed — " + ", ".join(parts)

    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if stripped and not stripped.startswith("==="):
            return stripped
    return "Daily update failed with no error summary captured in the log."
```

(The per-ticker `Counter` regex block is deleted — that regex is what reported 9,091 success lines as the "dominant error".)

- [ ] **Step 4:** Run tests — PASS; full suite; delete/update any test asserting the old ticker-line counting.
- [ ] **Step 5:** Commit: `fix(alerts): failure summary from SUMMARY_JSON; stop counting success lines as errors`

### Task P1.5: Stop `range_shortfall` noise on the Massive daily path

**Files:**
- Modify: `livewire_scripts/daily_update.py:745-752` (`_run_quality_detection` call in Massive loop)
- Test: `tests/test_daily_update.py`

Rationale: on the Massive gap-fill path, `expected_start = latest + 1 trading day` is a mechanical guess; for thin instruments a later `actual_start` just means "didn't trade", and `ib_head_timestamp` (the suppression input, `clients/quality_detector.py:49-50`) is never available. The detector cannot distinguish no-trade from data loss here, so don't feed it.

- [ ] **Step 1: Failing test:** massive-path run over an illiquid ticker whose first returned bar is 7 days after `latest` produces **no** `range_shortfall` flag (assert the quality-audit JSONL/`detect_all` mock receives `expected_start=None`).
- [ ] **Step 2:** Change the call at `:750` from `expected_start=latest + timedelta(days=1) if latest else None` to `expected_start=None`. Verify `_run_quality_detection`/`detect_all` skips range-shortfall when `expected_start is None` (Explore-verified: `detect_all` wires it at `clients/quality_detector.py:242-253`; if it doesn't already guard `None`, add the guard there with a unit test in `tests/test_quality_detector.py`).
- [ ] **Step 3:** Tests PASS; full suite; commit: `fix(quality): no range_shortfall on massive daily path (no-trade is not data loss)`

**Phase P1 verification (from primary checkout after merge):** run `python scripts/livewire_ingest.py daily --asset-class equity --source massive` on a trading day. Expected: exit 0, human table shows updated/no-trade/partial/error split, one `SUMMARY_JSON` line at the end, **no failure email**, and the next morning's log shows the wrapper did not retry.

---

## Phase P2 — Coverage truth restored

### Task P2.1: Footer-statistics freshness + active-universe denominator in `coverage_report.py`

**Files:**
- Modify: `livewire_scripts/coverage_report.py` (`_latest_date_in_parquet:104-110`, `compute_coverage:113-155`)
- Test: `tests/test_coverage_report.py` (extend existing)

**Interfaces:**
- `_latest_date_in_parquet(path, column_name) -> date | None` — same signature, now reads parquet footer statistics (no full column read); falls back to full read when stats are absent.
- `compute_coverage(target_date, bronze_root=None) -> dict[str, CoverageResult]` — denominator becomes the **active bronze universe for that timeframe**; a symbol counts as present if `latest >= target_date` **or** it is absent from the day's raw traded set (no-trade ≠ missing).

- [ ] **Step 1: Failing tests** (tmp bronze root with small real-shaped parquet fixtures — use real tickers/prices frozen, e.g. AAPL 2026-07-02 close from an existing bronze file read once at authoring time):

```python
def test_latest_date_uses_footer_stats_without_full_read(tmp_bronze):
    # write a 1d parquet with trade_date up to 2026-07-02, then corrupt-proof:
    # monkeypatch pq.read_table to raise if called with full column read;
    # _latest_date_in_parquet must still return date(2026, 7, 2) via metadata stats.


def test_compute_coverage_denominator_is_bronze_universe(tmp_bronze):
    # 3 symbols with 1d files, raw _symbols set has 5 tickers (2 not in bronze)
    # -> total == 3, not 5.


def test_no_trade_symbol_counts_present(tmp_bronze):
    # symbol WLIIU latest=2026-06-30, absent from raw _symbols for 2026-07-02
    # -> counted present (didn't trade), coverage 3/3.


def test_stale_traded_symbol_counts_missing(tmp_bronze):
    # symbol AAPL latest=2026-06-30, present in raw _symbols -> missing.
```

- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3: Implement.** New `_latest_date_in_parquet` (footer-stats API verified against pyarrow in the live venv on 2026-07-03 — `md.schema.column(i)` lacks `.name`; use `.path` and `md.num_columns`):

```python
def _latest_date_in_parquet(path: Path, column_name: str) -> date | None:
    md = pq.ParquetFile(path).metadata
    col_idx = next(
        (i for i in range(md.num_columns) if md.schema.column(i).path == column_name),
        None,
    )
    if col_idx is not None:
        max_value = None
        for rg_idx in range(md.num_row_groups):
            stats = md.row_group(rg_idx).column(col_idx).statistics
            if stats is None or not stats.has_min_max:
                max_value = None
                break
            candidate = stats.max
            if max_value is None or candidate > max_value:
                max_value = candidate
        if max_value is not None:
            if isinstance(max_value, datetime):
                return max_value.date()
            if isinstance(max_value, date):
                return max_value
            if isinstance(max_value, (str, bytes)):
                raw = max_value.decode() if isinstance(max_value, bytes) else max_value
                return date.fromisoformat(raw[:10])
    # Fallback: stats unavailable -> full column read (rare)
    table = pq.read_table(path, columns=[column_name])
    values = table.column(column_name).to_pylist()
    if not values:
        return None
    dates = [v.date() if isinstance(v, datetime) else v for v in values]
    return max(dates)
```

New `compute_coverage` core (replace `:121-153`):

```python
    traded_today = _raw_symbols_for_date(target_date, bronze_root)

    for tf in TIMEFRAMES:
        parquet_paths = sorted((bronze_root / "asset_class=equity").glob(f"symbol=*/{_filename_for(tf)}"))
        universe = {_symbol_from_parquet_path(p) for p in parquet_paths}
        latest_by_symbol = {
            _symbol_from_parquet_path(path): latest
            for path in parquet_paths
            if (latest := _latest_date_in_parquet(path, "trade_date" if tf == "1d" else "bar_timestamp")) is not None
        }
        present_symbols = {
            symbol for symbol in universe
            if (latest_by_symbol.get(symbol) or date.min) >= target_date
            or (traded_today and symbol not in traded_today)
        }
        missing = sorted(universe - present_symbols)
        results[tf] = CoverageResult(
            timeframe=tf, total=len(universe), present=len(present_symbols),
            missing_symbols=missing,
        )
```

- [ ] **Step 4:** Tests PASS; full suite; then a live smoke from the primary checkout: `time python scripts/livewire_quality.py coverage --no-recover --target-date 2026-07-02` — expected: completes in minutes (not hours), 1d ≥ 95% with `30m` still untracked (next task), and a sane missing list.
- [ ] **Step 5:** Add `"30m"` to `TIMEFRAMES` in `coverage_report.py` (12,931 symbols carry 30m files; it is currently untracked) with a matching test-fixture update.
- [ ] **Step 6:** Commit: `fix(coverage): footer-stats freshness, active-universe denominator, no-trade not missing, track 30m`

### Task P2.2: Schedule coverage + weekly report (they currently never run)

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py` (post-success hook, near the `report --email` spawn at `:300-323`)
- Test: `tests/test_run_daily_update_job.py`

- [ ] **Step 1: Failing test:** after a successful attempt, `run_with_retries` also spawns `[python, QUALITY_SCRIPT, "coverage"]` and `[python, QUALITY_SCRIPT, "weekly"]` (weekly self-skips on non-Sunday, so unconditional spawn is safe). Assert both commands appear in the recorded runner calls.
- [ ] **Step 2:** Implement: alongside the existing summary-report spawn, add the two subprocess spawns with the same non-fatal error handling pattern used for the report spawn (a coverage failure logs a warning; it must not flip the job to failure).
- [ ] **Step 3:** Tests PASS; full suite; commit: `feat(ops): run coverage + weekly quality report after successful daily job`
- [ ] **Step 4 (ops, after merge):** next morning verify `~/market-warehouse/logs/coverage_2026-07-XX.log` exists with non-zero percentages; Sunday verify `quality_weekly_*.md` renders.

### Task P2.3: Revive the day_aggs full-universe daily lane + heal the 1,091 intraday-only symbols

**Files:**
- Modify: `livewire_scripts/sync_runner.py` (phase list in `run_sync:175-329`)
- Test: `tests/test_sync_runner.py` (extend)

Rationale: raw `day_aggs_v1` is stale since 2026-06-11 (cursor mtime Jun 13) — the ~20K-universe daily lane silently stopped. Re-enabling it also publishes `1d.parquet` for the 1,091 symbols that today have intraday files but no daily file, and it keeps `bronze-delisted` archiving (P0) fed with a current SIP universe.

- [ ] **Step 1: Failing test:** `run_sync` includes a phase labeled `daily_backfill_equity_day_aggs` invoking `livewire_ingest.py flatfile-ingest-daily catch-up --days 7` positioned before the intraday flatfile phase; a phase failure is recorded but does not abort later phases (existing tolerated-failure pattern).
- [ ] **Step 2:** Implement the phase (mirror the existing `daily_backfill_intraday_equity_flatfiles` phase construction in `run_sync`; honor `MDW_DAILY_BACKFILL_INTRADAY_DAYS` analog via env `MDW_DAILY_BACKFILL_DAY_AGGS_DAYS`, default 7).
- [ ] **Step 3:** Tests PASS; commit: `feat(sync): re-enable full-universe day_aggs daily lane in nightly catch-up`
- [ ] **Step 4 (ops, after merge):** one-off heal in the primary checkout — the lane has 3 weeks of arrears: `python scripts/livewire_ingest.py flatfile-ingest-daily catch-up --days 21 --workers 4`. Then verify: count of `symbol=*/1d.parquet` ≈ count of `symbol=*/1m.parquet` (delta from 1,091 → ~0), spot-check one previously 1d-less symbol.

---

## Phase P3 — One trustworthy nightly digest; drop Cerebras

### Task P3.1: Per-phase `SUMMARY_JSON` from `sync_runner` + completion marker with counts

**Files:**
- Modify: `livewire_scripts/sync_runner.py` (end of `run_sync`), `livewire_scripts/run_intraday_catchup_job.py:198-200`
- Test: `tests/test_sync_runner.py`, `tests/test_run_intraday_catchup_job.py`

- [ ] **Step 1: Failing tests:** (a) `run_sync` ends by printing one `SUMMARY_JSON` line (via `daily_outcomes.SUMMARY_PREFIX`) with `{"job":"daily_backfill","phases":[{"label":...,"exit":0,"duration_s":...}, ...],"failed":[...]}`; (b) `run_intraday_catchup` writes `=== Done ... ===` only after that line is present in the log and its `_extract_error_summary` prefers `parse_last_summary_json` when available (mirror Task P1.4's pattern, "Intraday catchup failed — phases failed: <labels>").
- [ ] **Step 2:** Implement both. For durations, `sync_runner` already timestamps phases via its logger; record `time.monotonic()` around each `run_phase` call and carry `(label, returncode, duration_s)` into the final summary list.
- [ ] **Step 3:** Tests PASS; full suite; commit: `feat(sync): structured phase summary + truthful intraday failure summary`

### Task P3.2: Nightly digest builder

**Files:**
- Create: `livewire_scripts/nightly_digest.py`
- Test: `tests/test_nightly_digest.py`

**Interfaces:**
- `build_digest(run_date: date, log_dir: Path, data_lake: Path) -> str` — returns a plain-text digest assembled from: today's `daily_update_<date>.log` SUMMARY_JSON lines (one per asset class), `intraday_catchup_<date>.log` SUMMARY_JSON, `coverage_<date>.log` first line, and `shutil.disk_usage(data_lake)`. Sections render "(not found)" for any missing input — never raises.
- CLI `main(argv)` wired as `scripts/livewire_quality.py digest --run-date YYYY-MM-DD [--email]`; `--email` sends via the existing Nodemailer script with `--mode digest` (Task P3.3).

- [ ] **Step 1: Failing tests:** fixture log dir containing (a) a daily_update log with two SUMMARY_JSON lines (equity massive + futures ib), (b) an intraday log with a phase summary, (c) a coverage log first line, then assert `build_digest` output contains an outcomes table row `equity  updated=9091  no_trade=277  partial=95  errors=0`, the phase table, the coverage line, and a `Disk:` line; a second test with an empty dir asserts every section renders "(not found)" and the function returns a string.
- [ ] **Step 2:** Implement (pure string assembly over `parse_last_summary_json` per file — no new dependencies; collect *all* SUMMARY_JSON lines per daily log by filtering `parse_last_summary_json` over per-asset-class segments: simplest correct approach is to scan every line, `json.loads` each `SUMMARY_PREFIX` line, and keep them all in order — add `parse_all_summary_json(text) -> list[dict]` to `daily_outcomes.py` with its own test).
- [ ] **Step 3:** Wire the `digest` subcommand in `scripts/livewire_quality.py` (mirror existing subcommand dispatch), with a dispatch test if the operator-entrypoint test file covers subcommand help (it does — see `tests/` for `operator_entrypoints` tests; update expected command lists).
- [ ] **Step 4:** Tests PASS; commit: `feat(quality): nightly digest builder + digest subcommand`

### Task P3.3: Email plumbing — add digest mode, remove Cerebras, retire the noisy summary email

**Files:**
- Modify: `livewire_node/send_daily_update_failure_email.mjs` (mode dispatch `:580-585`, failure builder `:332-427`, Cerebras call site `:597-614`, fallback `:210-229`)
- Delete: `livewire_node/cerebras_client.mjs` (and its tests if any exist under `livewire_node/`)
- Modify: `livewire_scripts/run_daily_update_job.py` (replace the `report --view summary --email` spawn at `:300-315` with `digest --email`)
- Test: `tests/test_run_daily_update_job.py`; for the `.mjs`, `node --check` + a dry-run smoke

- [ ] **Step 1:** Read the mjs argument parser (`:126` area) and `buildDailySummaryMessage` (`:485-525`) to mirror its plumbing exactly, then add `--mode digest`: accepts `--body-file <path>` and sends the file content inside a `<pre>` block with subject `[Livewire] nightly digest <run-date>`.
- [ ] **Step 2:** Remove the Cerebras import, the `generateHumanReadableIncidentReport` call (`:597-611`), the fallback boilerplate (`buildFallbackIncidentReport:210-229`), and the "Probable cause"/"Proposed solution"/"Recommended next steps" cards from `buildFailureHtml` (`:386-398`). The failure email body becomes: error summary (now truthful from P1.4), run details table, log tail. Keep writing the sibling `.human.md` minus the AI sections. Delete `cerebras_client.mjs`.
- [ ] **Step 3:** `node --check livewire_node/send_daily_update_failure_email.mjs` — expect clean parse. Then a dry-run smoke: invoke with `--mode digest --body-file /tmp/digest.txt` and deliberately unset SMTP env; expected: the script fails at transport configuration (proving arg parsing + body build work) — capture and confirm the error is the SMTP-config one, not an arg/reference error.
- [ ] **Step 4:** In `run_daily_update_job.py`, replace the summary-report spawn with `[python, QUALITY_SCRIPT, "digest", "--run-date", <date>, "--email"]`; failing test first (assert new command in runner calls, old `report --view summary` command gone), then implement.
- [ ] **Step 5:** Grep for remaining `cerebras` references repo-wide (`rg -i cerebras`) — remove dead wiring (env loading mentions in `run-daily-job` docs/README/CLAUDE.md) in the same commit.
- [ ] **Step 6:** Full suite PASS; commit: `feat(alerts): nightly digest email; remove Cerebras enrichment and noisy per-warrant summary`

### Task P3.4: Watchdog covers the intraday job

**Files:**
- Modify: `livewire_scripts/check_daily_update_watchdog.py`
- Test: `tests/test_check_daily_update_watchdog.py` (extend; confirm exact filename via `ls tests | grep watchdog`)

- [ ] **Step 1: Failing tests:** watchdog run at 10:30 UTC also checks `intraday_catchup_<today UTC>.log` for the `=== Done` marker; missing file or missing marker → alert text names the intraday job; both-present → no alert.
- [ ] **Step 2:** Implement (reuse the existing marker-check helper at `:19-26`, parameterized by log-file prefix).
- [ ] **Step 3:** Tests PASS; commit: `feat(ops): watchdog covers intraday catch-up completion`

**Phase P3 verification:** next trading-day morning: exactly one "[Livewire] nightly digest" email with the outcomes table, phase table, coverage line, disk line; zero failure emails; kill the intraday job manually once (or point the watchdog at a date with no log) to confirm the watchdog alert fires and names the right job.

---

## Phase P4 — Datalake hygiene

### Task P4.1: Seed futures — GC (gold), CL (WTI), BZ (Brent)

**Files:**
- Create: `presets/futures-active.json`
- No code changes expected (existing `historical --asset-class futures` path)

- [ ] **Step 1:** Determine the current front/liquid contract expiries as of execution date (e.g. for 2026-07: GC next active month on COMEX, CL/BZ next months on NYMEX — check IB TWS/Gateway contract search; do not guess).
- [ ] **Step 2:** Write the preset (exact schema per CLAUDE.md futures preset format):

```json
{
  "name": "futures-active",
  "asset_class": "futures",
  "contracts": [
    {"root": "GC", "exchange": "COMEX", "expiry": "<verified>"},
    {"root": "CL", "exchange": "NYMEX", "expiry": "<verified>"},
    {"root": "BZ", "exchange": "NYMEX", "expiry": "<verified>"}
  ]
}
```

  (BZ = NYMEX Brent Last Day Financial — verify the contract qualifies via IB before committing; if BZ does not qualify with these params, resolve the correct exchange/symbol from IB contract search and record it in the preset comment/commit message.)
- [ ] **Step 3 (ops):** With IB Gateway up (preflight): `python scripts/livewire_ingest.py historical --preset presets/futures-active.json --asset-class futures`. Verify `data-lake/bronze/asset_class=futures/symbol=GC_<expiry>/1d.parquet` etc. exist with sane OHLC (spot-check GC close against a known quote of that day).
- [ ] **Step 4:** Confirm the nightly futures lane picks them up: next daily-run log shows futures tickers processed (it discovers from bronze, so seeding is sufficient).
- [ ] **Step 5:** Commit preset + README/CLAUDE.md preset-list note: `feat(futures): seed active GC/CL/BZ contracts`

### Task P4.2: Give cmdty + fx a daily lane

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py:23` (`ASSET_CLASSES = ["equity", "futures"]`)
- Test: `tests/test_run_daily_update_job.py`

- [ ] **Step 1:** Verify `daily_update.py` accepts `--asset-class cmdty` and `fx` end-to-end (BronzeClient schema + contract construction — CLAUDE.md says fallback is skipped for CMDTY/FX, implying support; confirm by reading the asset-class dispatch). If unsupported, stop and surface — do not force it.
- [ ] **Step 2:** Failing test: the job loop runs asset classes `["equity", "futures", "cmdty", "fx"]`; implement by extending the constant.
- [ ] **Step 3 (ops):** XAUUSD/USDEUR are 6 weeks stale — one-off catch-up from the primary checkout: `python scripts/livewire_ingest.py daily --asset-class cmdty --target-date 2026-07-02` (repeat for fx); verify max `trade_date` advances to 2026-07-02.
- [ ] **Step 4:** Commit: `feat(ops): daily lane for cmdty and fx asset classes`

### Task P4.3: Archive option_chain_snapshot out of bronze

**Files:** none (ops-only, no code)

- [ ] **Step 1 (ops):** `mkdir -p ~/market-warehouse/data-lake/bronze-archived && mv ~/market-warehouse/data-lake/bronze/asset_class=option_chain_snapshot ~/market-warehouse/data-lake/bronze-archived/` — confirm nothing globs it afterwards (`rg -l "option_chain_snapshot" livewire_scripts clients scripts` → expect no operational references; if any exist, remove them with a test).
- [ ] **Step 2:** Note the archival in `tasks/archive.md` with the date.

### Task P4.4: Disk headroom in the digest

**Files:**
- Modify: `livewire_scripts/nightly_digest.py`
- Test: `tests/test_nightly_digest.py`

- [ ] **Step 1:** Failing test: digest `Disk:` line includes free GiB and percent used, plus a `⚠ raw >` warning marker when free space < 2 × `MDW_FLATFILE_MIN_FREE_GB` (env, default 25 → warn under 50 GiB free — current state 46 GiB WILL warn, which is intended: raw retention was deferred, so the digest is the tripwire).
- [ ] **Step 2:** Implement; PASS; commit: `feat(digest): disk headroom tripwire`

---

## Phase P5 — Lane consolidation (only after P1–P3 have run clean for ≥3 trading days)

### Task P5.1: Single owner for equity 1d

**Files:**
- Modify: `livewire_scripts/sync_runner.py` (drop the `daily_backfill_equity_union` preset-union Massive REST phase — the day_aggs lane from P2.3 now owns full-universe equity 1d)
- Modify: `livewire_scripts/run_daily_update_job.py` (equity stays, but becomes recovery-only: pass `--preset` NO — instead keep as-is initially; see decision box below)
- Test: `tests/test_sync_runner.py`

**Decision box (confirm with operator before executing):** after P2.3, equity 1d is written twice nightly (day_aggs flat files at 05:00 UTC + Massive REST daily at 06:00 UTC). Recommended end state: 05:00 day_aggs = the owner; the 06:00 equity pass stays as *target-day gap recovery only* (it naturally no-ops when day_aggs already filled everything, and P1 made its residue non-alarming). If three days of digests show the 06:00 equity pass updating ~0 tickers, drop nothing and keep it as the safety net — the redundancy is then cheap and quiet. Only remove the 05:00 `daily_backfill_equity_union` phase (2,388-ticker REST loop, ~16 min nightly) since day_aggs supersedes it.

- [ ] **Step 1:** Failing test: `run_sync` phase list no longer contains `daily_backfill_equity_union`; day_aggs and intraday flatfile phases remain.
- [ ] **Step 2:** Implement; PASS; commit: `refactor(sync): day_aggs lane owns equity 1d; drop redundant preset-union REST phase`
- [ ] **Step 3 (ops):** over the following week, confirm digests show equity coverage ≥ 99% on the active universe and the 06:00 equity pass reporting near-zero work.

### Task P5.2: Documentation truth pass

**Files:**
- Modify: `CLAUDE.md`, `README.md`

- [ ] **Step 1:** Update CLAUDE.md sections that this plan invalidated: daily job outcome semantics + exit policy, digest email (replacing daily summary + failure boilerplate), Cerebras removal, coverage scheduling, day_aggs lane re-enabled, futures/cmdty/fx lanes, `.worktrees/` convention. All times in UTC/ET.
- [ ] **Step 2:** Commit: `docs: reflect observability uplift in CLAUDE.md/README`

---

## Self-review notes

- Spec coverage: P0→decision 0 (in-flight work), P1→findings 1.2/1.3 defects 1–5, P2→finding 1.5 + 1,091-symbol mismatch + stale day_aggs, P3→findings 1.4/1.2 defect 6 + Cerebras decision, P4→futures/cmdty/fx/option-snapshot decisions + disk, P5→finding 1.6. Raw retention deliberately deferred per operator (digest tripwire in P4.4 covers it).
- Known unknowns flagged in-task rather than guessed: BZ contract params (P4.1), cmdty/fx daily support (P4.2), exact test-file names for watchdog/wrapper (verify with `ls tests/`), `detect_all` None-guard (P1.5).
- Type consistency: `SUMMARY_PREFIX`/`build_summary_line`/`parse_last_summary_json`/`resolve_exit_code` names used identically across P1.2–P1.4, P3.1–P3.2; `parse_all_summary_json` introduced and consumed only in P3.2.
