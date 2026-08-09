# IB-Isolation, Sendable Alert, Coverage Job, Housekeeping — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop a down IB Gateway from taking out seven non-IB phases, make the failure alert sendable, give coverage a job it can finish, and put the warehouse's disk use under real observation.

**Architecture:** Four independent changes on one branch. Part 1 moves the IB preflight from the orchestrator down to the phases that actually use IB, and adds a Massive fallback for the equity lane. Part 2 adds `--key=value` parsing to the Node alert CLI. Part 3 moves coverage to its own launchd job and makes its footer pass incremental on file mtime. Part 4 fixes the disk check and adds a retention command.

**Tech Stack:** Python 3.13 (`uv run pytest`), Node ESM (`node --test`), launchd plists, pyarrow, DuckDB.

**Spec:** `docs/superpowers/specs/2026-08-09-ib-isolation-coverage-housekeeping-design.md`

**Branch:** `fix/ib-isolation-coverage-housekeeping` (already created, spec committed as `bab3e75`)

## Global Constraints

- Tests use REAL tickers at REAL prices, frozen as fixtures with an as-of date. No network at runtime. No placeholder symbols, no round-number prices.
- Coverage gate is 95% (`fail_under = 95` in `pyproject.toml`). Run `uv run pytest tests/ -v` before every commit.
- Two integration tests hang the suite (real Nasdaq/Stooq fallback with stale dates). Deselect them when running the full suite.
- No `Co-Authored-By` or other AI-attribution trailer in any commit message.
- Never delete: `data-lake/raw/` partitions, `repairs/*/backup/`, `repairs/triage/`, the release `current` points at.
- IB connects to `127.0.0.1:4001`. Never the LAN IP. Never restart or manage the Gateway from this repo.
- `GATEWAY_DOWN_EXIT_CODE = 86` (`clients/ib_gateway_preflight.py:28`).

## Measured facts this plan rests on

| Fact | Value | Measured |
|---|---|---|
| Coverage full run, cold, 16 threads, `--no-recover` | **2858s** (03:43:54Z → 04:31:32Z) | 2026-08-09 |
| Coverage budget it must fit in today | 1800s | PR #78 |
| equity `1d` footers, warm, 16 threads | 29.2s | 2026-08-02 |
| `intraday-catchup` exit 86 occurrences | 2026-07-27, 2026-08-08, **2026-08-09** | logs |
| Sessions missing warehouse-wide on 2026-08-07 | equity 0/13311, futures 0/14, rates 0/4; volatility 42/43 | coverage run above |
| livewire footprint on the 93%-full internal volume | ~2.5 GB (venv 824M, releases 1.2G, logs 306M, cursors 176M) | 2026-08-09 |
| `data-lake/repairs/` on the lake volume | 26 GB, of which 21 GB is 12,636 `.parquet.bak` from the 2026-07-15 cutover | 2026-08-09 |

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `scripts/livewire_ingest.py` | CLI dispatch + preflight gate | Modify `IB_COMMANDS` (`:33-41`) |
| `livewire_scripts/sync_runner.py` | 9-phase daily-backfill orchestrator | Modify `run_sync` (`:204-385`) |
| `scripts/livewire_quality.py` | quality CLI dispatch | `load_scheduled_env` for `coverage` too (`:56`) |
| `livewire_scripts/release.py` | release lifecycle | `prune` gains `dry_run` (`:203`) |
| `livewire_scripts/run_daily_update_job.py` | nightly lane runner | Modify `main` (`:786-845`), remove coverage spawn (`:570`) |
| `livewire_node/send_daily_update_failure_email.mjs` | alert CLI | Modify `parseArgs` (`:62-83`) |
| `livewire_scripts/coverage_report.py` | freshness detector | Modify `compute_coverage` (`:159-226`), add cache helpers |
| `livewire_scripts/nightly_digest.py` | digest assembly | Modify `_phases_section` (`:63`), `_coverage_section` (`:146-153`, deletes `_target_session`), `_disk_section` (`:156-163`) |
| `livewire_scripts/housekeeping.py` | **new** — retention sweeps | Create |
| `scripts/livewire_ops.py` | ops CLI dispatch | Add `housekeeping` to `COMMANDS` (`:23-27`) |
| `launchd/com.livewire.coverage.plist.example` | **new** — coverage schedule | Create |
| `CLAUDE.md` | invariants | Append |

---

### Task 1: Move the IB preflight down to the phases that use IB

**Files:**
- Modify: `scripts/livewire_ingest.py:33-41`
- Test: `tests/test_ingest_preflight_scope.py` (create)

**Interfaces:**
- Consumes: `_requires_ib_preflight(command: str, rest: Sequence[str]) -> bool` (`livewire_ingest.py:83`)
- Produces: nothing new. Behaviour change only.

**Why this is safe:** `sync_runner.py:316-332` invokes the IB phases as
`livewire_ingest.py intraday-backfill --source ib --asset-class volatility`, and
`intraday-backfill` stays in `IB_COMMANDS` with `_requires_ib_preflight`
returning `True` for it unconditionally (`livewire_ingest.py:92-93`). No check is
lost; it moves to the right granularity.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ingest_preflight_scope.py
"""The preflight belongs to the phases that use IB, not to the orchestrators.

`daily-backfill` has nine phases and two of them use IB. Gating the whole
orchestrator on the Gateway meant that on 2026-08-08 and 2026-08-09 the Massive
equity day_aggs lane, the Massive flat-file intraday lane, FRED rates and CBOE
all failed to run because of a dependency none of them have. Friday 2026-08-07
is absent from bronze warehouse-wide as a result.
"""

from scripts.livewire_ingest import _requires_ib_preflight


class TestOrchestratorsDoNotGateOnIB:
    def test_daily_backfill_does_not_require_preflight(self):
        assert _requires_ib_preflight("daily-backfill", []) is False

    def test_backfill_all_does_not_require_preflight(self):
        assert _requires_ib_preflight("backfill-all", []) is False


class TestTheIBPhasesStillGate:
    def test_intraday_backfill_still_requires_preflight(self):
        # This is what sync_runner Phase 5 actually invokes.
        assert _requires_ib_preflight(
            "intraday-backfill",
            ["--source", "ib", "--asset-class", "volatility", "--timeframe", "30m"],
        ) is True

    def test_daily_still_requires_preflight_by_default(self):
        assert _requires_ib_preflight("daily", ["--asset-class", "equity"]) is True

    def test_daily_with_massive_source_does_not(self):
        assert _requires_ib_preflight("daily", ["--asset-class", "equity", "--source", "massive"]) is False
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_ingest_preflight_scope.py -v`
Expected: `test_daily_backfill_does_not_require_preflight` and
`test_backfill_all_does_not_require_preflight` FAIL (both currently return `True`).
The other three PASS already.

- [ ] **Step 3: Make the change**

```python
# scripts/livewire_ingest.py:33-41 — replace the set
# The orchestrators are deliberately absent. `daily-backfill` and `backfill-all`
# each run nine phases of which two use IB, and `main()` runs the preflight
# before dispatching — so a down Gateway killed seven Massive/FRED/CBOE phases
# that have no IB dependency at all. Phase 5 invokes `intraday-backfill`, which
# is still listed here and does its own preflight, so nothing is unchecked.
IB_COMMANDS = {
    "daily",
    "historical",
    "robust",
    "intraday-backfill",
    "universe",
}
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_ingest_preflight_scope.py tests/test_sync_runner.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/livewire_ingest.py tests/test_ingest_preflight_scope.py
git commit -m "fix(ingest): a down Gateway killed seven phases that never touch IB

daily-backfill and backfill-all are orchestrators, not IB commands. main()
runs assert_gateway_up() before dispatching, so exit 86 landed ten seconds in
and none of the nine phases ran — including the Massive equity day_aggs lane
that owns the ~20K SIP daily universe. Friday 2026-08-07 is missing from
bronze warehouse-wide because of this.

Phase 5 invokes intraday-backfill, which stays in IB_COMMANDS and preflights
itself, so the check is not removed — it moves to the dependency it describes."
```

---

### Task 2: A phase exiting 86 is degraded, not failed

**Files:**
- Modify: `livewire_scripts/sync_runner.py:313-385`
- Modify: `livewire_scripts/nightly_digest.py:63` (`_phases_section` must render the new field)
- Test: `tests/test_sync_runner.py`, `tests/test_nightly_digest.py`

**Interfaces:**
- Consumes: `GATEWAY_DOWN_EXIT_CODE` from `clients.ib_gateway_preflight`
- Produces: `SUMMARY_JSON` for `job="daily_backfill"` gains a `"degraded"` key
  (list of phase labels). `"failed"` excludes exit-86 phases. Consumed by
  `nightly_digest._phases_section` and the watchdog.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_sync_runner.py
"""A Gateway outage must degrade the run, not fail it.

Task 1 lets the seven non-IB phases run when IB is down. This is the other
half: the two IB phases exit 86, and without this the orchestrator still
returns 1 and reports them in SUMMARY_JSON["failed"] — so the wrapper pages and
the digest shows a red run for a dependency outage the design calls degraded.
"""

import json

from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE


class TestAGatewayOutageDegradesRatherThanFails:
    def test_ib_phases_exiting_86_do_not_fail_the_run(self, tmp_path, capsys):
        config = _make_config(tmp_path)  # existing helper, tests/test_sync_runner.py:52

        def runner(command, **kwargs):
            rc = GATEWAY_DOWN_EXIT_CODE if "intraday-backfill" in command else 0
            return subprocess.CompletedProcess(command, rc)

        rc = run_sync(config, runner=runner, trading_day_fn=lambda: "2026-08-07")

        assert rc == 0, "a Gateway outage is degraded, not failed"
        summary = json.loads(
            [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SUMMARY_PREFIX)][-1]
            .removeprefix(SUMMARY_PREFIX)
        )
        assert summary["failed"] == []
        assert sorted(summary["degraded"]) == [
            "daily_backfill_intraday_30m_volatility",
            "daily_backfill_intraday_5m_volatility",
        ]

    def test_a_real_phase_failure_still_fails_the_run(self, tmp_path, capsys):
        config = _make_config(tmp_path)

        def runner(command, **kwargs):
            rc = 1 if "flatfile-ingest-daily" in command else 0
            return subprocess.CompletedProcess(command, rc)

        rc = run_sync(config, runner=runner, trading_day_fn=lambda: "2026-08-07")

        assert rc == 1
        summary = json.loads(
            [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SUMMARY_PREFIX)][-1]
            .removeprefix(SUMMARY_PREFIX)
        )
        assert "daily_backfill_equity_day_aggs" in summary["failed"]
        assert summary["degraded"] == []
```

**Verified against the real module, do not re-derive:**
- The helper is `_make_config(tmp_path)` (`tests/test_sync_runner.py:52`) — there is
  no `_sync_config`.
- `trading_day_fn` returns a **string**: `latest_complete_trading_day() -> str`
  (`sync_runner.py:83`) and every existing test passes `lambda: "2026-05-28"`.
  Passing a `date` puts a non-str into the phase command list.
- `run_sync` calls `require_flatfile_credentials()` first and returns 2 on failure,
  but `tests/test_sync_runner.py:47` has an **autouse** `_flatfile_credentials`
  fixture, so these tests need nothing extra.
- `run_phase` invokes `runner(command, stdout=…, stderr=…, text=…, check=…,
  timeout=…)` — all keyword, so `def runner(command, **kwargs)` is the right fake
  shape (matching the existing `_ok_runner`, `:91`).
- `VOL_INTRADAY_TIMEFRAMES = ("30m", "5m")` (`sync_runner.py:39`), and the phase 3b
  label is `daily_backfill_equity_day_aggs` (`:281`).

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_sync_runner.py -k Degrades -v`
Expected: FAIL — `KeyError: 'degraded'`, and `rc == 1`.

- [ ] **Step 3: Make the change**

```python
# livewire_scripts/sync_runner.py — add the import near the top
from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE
```

```python
# livewire_scripts/sync_runner.py:313-335 — Phase 5, replace the failure append
    # Phase 5: Volatility intraday via IB
    vol_tickers = load_tickers(config.vol_preset)
    ib_phase_labels: set[str] = set()
    for tf in VOL_INTRADAY_TIMEFRAMES:
        ib_phase_labels.add(f"daily_backfill_intraday_{tf}_volatility")
        rc = _phase(
            f"daily_backfill_intraday_{tf}_volatility",
            [
                py,
                ingest,
                "intraday-backfill",
                "--tickers",
                *vol_tickers,
                "--timeframe",
                tf,
                "--source",
                "ib",
                "--asset-class",
                "volatility",
                "--days",
                str(config.intraday_days),
            ],
        )
        # A down Gateway is degraded, never failed. 2FA and IBKR maintenance are
        # not something livewire recovers, and paging for them trains the reader
        # to ignore the page.
        if rc not in (0, GATEWAY_DOWN_EXIT_CODE):
            failures.append(f"vol_intraday_{tf}")
```

```python
# livewire_scripts/sync_runner.py:365-377 — replace the SUMMARY_JSON block
    # Machine-readable per-phase summary for the nightly digest / watchdog.
    # `failed` and `degraded` are disjoint by construction: the digest and the
    # watchdog both read `failed`, and a Gateway outage must not appear there.
    #
    # Membership of `ib_phase_labels` is what makes a phase eligible to degrade,
    # NOT the exit code alone. 86 is livewire's own preflight code, but nothing
    # stops a Massive/FRED/CBOE/DuckDB phase from returning it for an unrelated
    # reason, and treating that as degraded would silently swallow a real
    # failure — the same class of exit-code-versus-summary disagreement this
    # runner was already fixed for once.
    def _degraded(p: dict) -> bool:
        return p["label"] in ib_phase_labels and p["exit"] == GATEWAY_DOWN_EXIT_CODE

    print(
        SUMMARY_PREFIX
        + json.dumps(
            {
                "job": "daily_backfill",
                "target_date": str(target_date),
                "phases": phase_results,
                "failed": [p["label"] for p in phase_results if p["exit"] != 0 and not _degraded(p)],
                "degraded": [p["label"] for p in phase_results if _degraded(p)],
            },
            separators=(",", ":"),
        )
    )
```

The Phase 5 `failures.append` guard must use the same rule — `if rc not in (0,
GATEWAY_DOWN_EXIT_CODE)` is already inside the IB loop, so it is scoped
correctly by construction; no other phase's guard changes.

- [ ] **Step 3b: Make the digest render it — the field alone changes nothing**

`_phases_section` (`livewire_scripts/nightly_digest.py:63`) prints
`"ok" if p["exit"] == 0 else f"FAILED (exit {p['exit']})"`. Emitting a `degraded`
list without touching this leaves the orchestrator returning success while the
nightly email still reads `FAILED (exit 86)` for both IB phases — the operator
sees a red run and the exit code says green. The digest is the only place a
human reads this, so the classification has to reach it:

```python
# livewire_scripts/nightly_digest.py:69-72 — inside _phases_section
    degraded = set(summary.get("degraded", []))
    for p in summary.get("phases", []):
        label = p.get("label", "?")
        if p.get("exit") == 0:
            status = "ok"
        elif label in degraded:
            # A Gateway outage is not a failure. Naming it "DEGRADED" rather
            # than "FAILED (exit 86)" is the whole point of the new field —
            # a page-shaped word for a non-page trains the reader to ignore it.
            status = "DEGRADED (IB down)"
        else:
            status = f"FAILED (exit {p.get('exit')})"
        lines.append(f"  {label:<44} {status:<18} {p.get('duration_s', '?')}s")
    if summary.get("degraded"):
        lines.append(f"  degraded: {', '.join(summary['degraded'])}")
    if summary.get("failed"):
        lines.append(f"  failed: {', '.join(summary['failed'])}")
```

Test it in `tests/test_nightly_digest.py`:

```python
class TestTheDigestDistinguishesDegradedFromFailed:
    def test_an_ib_phase_at_86_reads_degraded_not_failed(self, tmp_path):
        summary = {
            "job": "daily_backfill",
            "target_date": "2026-08-07",
            "phases": [
                {"label": "daily_backfill_equity_day_aggs", "exit": 0, "duration_s": 41.0},
                {"label": "daily_backfill_intraday_30m_volatility", "exit": 86, "duration_s": 0.2},
            ],
            "failed": [],
            "degraded": ["daily_backfill_intraday_30m_volatility"],
        }
        (tmp_path / "intraday_catchup_2026-08-08.log").write_text(
            nightly_digest.SUMMARY_PREFIX + json.dumps(summary) + "\n", encoding="utf-8"
        )

        lines = nightly_digest._phases_section("2026-08-08", tmp_path)
        text = "\n".join(lines)

        assert "DEGRADED (IB down)" in text
        assert "FAILED" not in text, "a Gateway outage must not read as a failure"

    def test_a_real_failure_still_reads_failed(self, tmp_path):
        summary = {
            "job": "daily_backfill",
            "target_date": "2026-08-07",
            "phases": [{"label": "daily_backfill_equity_day_aggs", "exit": 1, "duration_s": 3.0}],
            "failed": ["daily_backfill_equity_day_aggs"],
            "degraded": [],
        }
        (tmp_path / "intraday_catchup_2026-08-08.log").write_text(
            nightly_digest.SUMMARY_PREFIX + json.dumps(summary) + "\n", encoding="utf-8"
        )
        assert "FAILED (exit 1)" in "\n".join(nightly_digest._phases_section("2026-08-08", tmp_path))
```

`SUMMARY_PREFIX` is **not** a `nightly_digest` attribute — it lives in
`livewire_scripts/daily_outcomes.py:13` (`"SUMMARY_JSON "`), which is also where
`nightly_digest` imports `parse_last_summary_json` from. Import it from there.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_sync_runner.py tests/test_nightly_digest.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/sync_runner.py tests/test_sync_runner.py
git commit -m "fix(sync): a Gateway outage degrades the run instead of failing it

Task 1 lets the seven non-IB phases run when IB is down; this stops the two
IB phases from turning the whole orchestrator red for it. SUMMARY_JSON gains
a disjoint degraded list, and failed no longer carries exit 86 — the digest
and the watchdog both read failed."
```

---

### Task 3: The equity lane falls back to Massive when IB is down

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py:803-812`
- Test: `tests/test_run_daily_update_job.py`

**Interfaces:**
- Consumes: `run_with_retries(config, daily_update_args, env=None, sleep_fn=…,
  runner=…, now_fn=…, completion_scope=None, deadline=None) -> int` (`:459` — note
  the second parameter is named `daily_update_args`, and none of these are
  keyword-only), `GATEWAY_DOWN_EXIT_CODE`
- Produces: `lane_codes["equity"]` may now be the fallback's exit code rather than 86.
  Both `degraded`/`failed` (`:822-823`) and `silver_inputs_ok` (`:831`) read
  `lane_codes`, so the retry must be inserted **before** them — i.e. right after the
  `ASSET_CLASSES` loop, which is where Step 3 puts it.

**The fallback command is already proven in production.** `sync_runner`'s Phase 1
(`:242-259`) is `daily --asset-class equity --source massive …` and runs nightly
against the same credentials from the same `~/market-warehouse/.env`. This task is
pointing an existing, exercised path at a new trigger, not introducing one. The
lane also passes no `--tickers`, so `daily` discovers the universe from bronze —
the fallback covers exactly what the IB lane would have.

**`tests/test_run_daily_update_job.py:45` has an autouse `no_real_quality_spawn`
fixture** that patches `run_post_success_quality` wholesale, so any `main([])` test
already cannot shell out to the quality CLI. Do **not** also patch
`_spawn_post_success_quality` in these tests — it is never reached.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_run_daily_update_job.py
"""Silver must not be hostage to IB.

Silver reads equity bronze and the corporate-action store, both Massive-backed.
But the equity lane runs on IB by default, so a down Gateway skipped it and
`silver_inputs_ok` then blocked the rebuild for the whole ~13K universe — the
exact cascade CLAUDE.md says must not happen, arriving by an indirect route.

Futures and cmdty get NO fallback: Massive does not carry those asset classes,
so a fallback there would be a fabricated success.

Consequence worth stating rather than discovering: if IB is down AND Massive
cannot answer either, equity's code is no longer 86, so the lane leaves
`degraded` for `failed` and the job pages. That is right — no source produced
the session's bars, which is not a degrade — but it is a real change in when
this job wakes someone up, and it fires only when both providers are gone.
"""

from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE


class TestTheEquityLaneFallsBackToMassive:
    def test_a_down_gateway_retries_equity_on_massive(self, monkeypatch, tmp_path):
        calls: list[list[str]] = []

        def fake_run_with_retries(config, daily_update_args, **kwargs):
            args = list(daily_update_args)
            calls.append(args)
            if "--source" in args and args[args.index("--source") + 1] == "massive":
                return 0
            return GATEWAY_DOWN_EXIT_CODE

        monkeypatch.setattr(daily_runner, "run_with_retries", fake_run_with_retries)
        monkeypatch.setattr(daily_runner, "run_corporate_action_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_cboe_volatility_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_fx_sync", lambda *a, **k: 0)
        silver_ran: list[bool] = []
        monkeypatch.setattr(
            daily_runner, "run_silver_rebuild", lambda *a, **k: (silver_ran.append(True), 0)[1]
        )

        daily_runner.main([])

        equity_calls = [c for c in calls if "equity" in c]
        assert len(equity_calls) == 2, "equity should be retried exactly once"
        assert equity_calls[1][equity_calls[1].index("--source") + 1] == "massive"
        assert silver_ran == [True], "Silver must rebuild once the fallback succeeds"

    def test_futures_and_cmdty_get_no_fallback(self, monkeypatch, tmp_path):
        calls: list[list[str]] = []

        def fake_run_with_retries(config, daily_update_args, **kwargs):
            calls.append(list(daily_update_args))
            return GATEWAY_DOWN_EXIT_CODE

        monkeypatch.setattr(daily_runner, "run_with_retries", fake_run_with_retries)
        monkeypatch.setattr(daily_runner, "run_corporate_action_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_cboe_volatility_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_fx_sync", lambda *a, **k: 0)

        daily_runner.main([])

        for asset_class in ("futures", "cmdty"):
            lane_calls = [c for c in calls if asset_class in c]
            assert len(lane_calls) == 1, f"{asset_class} must not be retried — Massive has no such data"
            assert "--source" not in lane_calls[0]
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_run_daily_update_job.py -k FallsBackToMassive -v`
Expected: FAIL — `len(equity_calls) == 1`, and `silver_ran == []`.

- [ ] **Step 3: Make the change**

```python
# livewire_scripts/run_daily_update_job.py — insert after the ASSET_CLASSES loop (:812)
    # Massive owns equity daily whenever IB cannot answer. Silver reads equity
    # bronze and the corporate-action store and nothing else, both Massive-backed
    # — so without this a Gateway outage silently gated the adjusted rebuild for
    # the whole ~13K universe, the same cascade the lane split was meant to end.
    #
    # `_requires_ib_preflight` exempts `daily --source massive`, so the retry
    # cannot hit the preflight again. Futures and cmdty deliberately get no
    # fallback: Massive does not carry those asset classes.
    if lane_codes.get("equity") == GATEWAY_DOWN_EXIT_CODE:
        lane_codes["equity"] = run_with_retries(
            config,
            args + ["--asset-class", "equity", "--source", "massive"],
            env=env,
            completion_scope="equity",
            deadline=deadline,
        )
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_run_daily_update_job.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/run_daily_update_job.py tests/test_run_daily_update_job.py
git commit -m "fix(ops): the equity lane falls back to Massive when IB is down

Silver depends on equity bronze and the corporate-action store, both
Massive-backed — but the equity lane runs on IB, so a down Gateway skipped it
and silver_inputs_ok blocked the rebuild for the whole universe. Retry the
lane once on --source massive, which the preflight explicitly exempts.

Futures and cmdty get no fallback: Massive does not carry them, and a
fallback there would manufacture a success out of missing data."
```

---

### Task 4: The alert CLI must accept any value

**Files:**
- Modify: `livewire_node/send_daily_update_failure_email.mjs:62-83`
- Modify: `livewire_scripts/run_daily_update_job.py:157`, `livewire_scripts/run_intraday_catchup_job.py:85`, `livewire_scripts/coverage_report.py:439`, `livewire_scripts/health_check.py:340`, `livewire_scripts/universe_screener.py:262`
- Test: `tests/node/send_daily_update_failure_email.test.mjs`, `tests/test_run_daily_update_job.py`

**Interfaces:**
- Consumes: `parseArgs(argv) -> options`
- Produces: `parseArgs` additionally accepts `--key=value`. The five Python call
  sites emit `--error-summary=<text>` as one argv token instead of two.

- [ ] **Step 1: Write the failing Node test**

```javascript
// append to tests/node/send_daily_update_failure_email.test.mjs
// The 2026-08-08 and 2026-08-09 intraday-catchup pages were never sent:
// "Missing value for --error-summary". The summary is log-derived text and
// began with "--- Runbook: ...", and parseArgs treats any value starting with
// "--" as the next flag. The watchdog caught it 5.5h later.
test("a value beginning with -- survives when passed as --key=value", () => {
  const summary = "--- Runbook: /Users/moremeds/runbooks/trading-stack/ib-gateway-ibc.md ---";
  const options = parseArgs([
    "--run-date", "2026-08-08",
    `--error-summary=${summary}`,
    "--job-name", "intraday_catchup",
  ]);
  assert.equal(options.errorSummary, summary);
  assert.equal(options.jobName, "intraday_catchup");
});

test("an = inside the value is preserved", () => {
  const options = parseArgs(["--error-summary=exit_code=86 lanes=equity,futures"]);
  assert.equal(options.errorSummary, "exit_code=86 lanes=equity,futures");
});

test("the two-token form still works for ordinary values", () => {
  const options = parseArgs(["--run-date", "2026-08-08", "--exit-code", "86"]);
  assert.equal(options.runDate, "2026-08-08");
  assert.equal(options.exitCode, 86);
});
```

- [ ] **Step 2: Run it and watch it fail**

Run: `npm run test:alerts`
Expected: FAIL — `Missing value for --error-summary=--- Runbook: ...` (the whole
token is read as a flag name).

- [ ] **Step 3: Make the Node change**

```javascript
// livewire_node/send_daily_update_failure_email.mjs:68-84 — replace the loop head
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }

    if (!token.startsWith("--")) {
      throw new Error(`Unexpected argument: ${token}`);
    }

    // `--key=value` first. The two-token form cannot carry a value that starts
    // with "--", and the error summary is log-derived text: on 2026-08-08 it
    // began with "--- Runbook: ..." and the page was never sent. Splitting on
    // the FIRST "=" only, so an "=" inside the value survives.
    let key;
    let value;
    const equals = token.indexOf("=");
    if (equals > 2) {
      key = token.slice(2, equals);
      value = token.slice(equals + 1);
    } else {
      key = token.slice(2);
      value = argv[index + 1];
      if (value == null || value.startsWith("--")) {
        throw new Error(`Missing value for --${key}`);
      }
      index += 1;
    }
```

Leave the `switch (key)` block below unchanged. Also update the usage text at
`send_daily_update_failure_email.mjs:21` from `--error-summary TEXT` to
`--error-summary=TEXT` — the help is what the next operator copies, and the
two-token form it currently advertises is the form that lost the page.

- [ ] **Step 4: Run the Node test**

Run: `npm run test:alerts`
Expected: PASS.

- [ ] **Step 5: Write the failing Python test**

```python
# append to tests/test_run_daily_update_job.py
"""The Python side must emit the single-token form.

Fixing the parser alone leaves the callers still passing two tokens, which
still breaks the moment the summary begins with "--".
"""


class TestTheAlertCommandCarriesTheSummaryAsOneToken:
    def test_error_summary_is_a_single_equals_token(self, tmp_path):
        summary = "--- Runbook: /Users/moremeds/runbooks/trading-stack/ib-gateway-ibc.md ---"
        config = _config(tmp_path)  # existing helper, tests/test_run_daily_update_job.py:58
        request = daily_runner.AlertRequest(
            run_date="2026-08-08",
            log_file=tmp_path / "daily_update_2026-08-08.log",
            attempts=1,
            exit_code=86,
            error_summary=summary,
            repo_root=tmp_path / "repo",
        )

        command = daily_runner.build_alert_command(config, request)

        assert f"--error-summary={summary}" in command
        assert "--error-summary" not in command, "the bare two-token form must be gone"
```

**Verified, do not re-derive:** `AlertRequest` (`run_daily_update_job.py:59`) has
six required fields — `run_date`, `log_file`, `attempts`, `exit_code`,
`error_summary`, `repo_root` — and none have defaults. The command is assembled by
`build_alert_command(config, request)`, which `send_failure_alert` (`:271`) then
hands to the runner; testing the builder directly needs no subprocess fake at all.
The config helper is `_config(tmp_path, *, node_bin=…)` (`:58`), not
`_runner_config`.

- [ ] **Step 6: Run it and watch it fail**

Run: `uv run pytest tests/test_run_daily_update_job.py -k OneToken -v`
Expected: FAIL — the command still contains the bare `--error-summary`.

- [ ] **Step 7: Change all five call sites**

In each file, replace the two-element form:

```python
        "--error-summary",
        summary,
```

with the single token:

```python
        # One token. The two-token form breaks whenever the summary begins with
        # "--", which is how the 2026-08-08 page was lost.
        f"--error-summary={summary}",
```

The local variable holding the summary differs per file — read each call site
and use its own name:
- `livewire_scripts/run_daily_update_job.py:157`
- `livewire_scripts/run_intraday_catchup_job.py:85`
- `livewire_scripts/coverage_report.py:439`
- `livewire_scripts/health_check.py:340`
- `livewire_scripts/universe_screener.py:262`

⚠️ **Five existing assertions pin the two-token form and will fail.** They are
not incidental — each is a real test of the alert command, so update rather than
delete them:

| Test | Currently asserts |
|---|---|
| `tests/test_coverage_report.py:482` | `idx = cmd.index("--error-summary")` then reads `cmd[idx+1]` |
| `tests/test_coverage_report.py:492` | same |
| `tests/test_health_check.py:321` | `"--error-summary" in cmd` |
| `tests/test_health_check.py:333` | `summary_idx = cmd.index("--error-summary") + 1` |
| `tests/test_universe_screener.py:296` | `"--error-summary" in cmd` |
| `tests/test_run_daily_update_job.py:458` | `"--error-summary" in command` |

Rewrite each as a prefix match on the single token, which tests the same thing
without depending on adjacency:

```python
summary = next(a for a in cmd if a.startswith("--error-summary="))
assert summary.removeprefix("--error-summary=") == expected
```

**The Node tests at `tests/node/…:46,221,264,340,387` stay as they are.** They
use the two-token form, and the parser must keep accepting it — that is exactly
what the new `test("the two-token form still works…")` case asserts. Only the
Python emitters change form; the parser gains a form.

- [ ] **Step 8: Run everything**

Run: `npm run test:alerts && uv run pytest tests/ -v -m "not integration"`
Expected: all PASS.

The two network-touching tests are `@pytest.mark.integration`-marked in
`tests/test_storage_client_compat.py` (`:61`
`test_fetch_helpers_write_parquet_for_compat_storage` and `:86`
`test_main_calls_write_ticker_parquet_for_compat_storage`) — they are the only two
so marked in the suite, so `-m "not integration"` is the whole deselection and no
node ids need guessing.

- [ ] **Step 9: Commit**

```bash
git add livewire_node/ tests/node/ livewire_scripts/ tests/test_run_daily_update_job.py
git commit -m "fix(alerts): a summary starting with -- silently killed every page

parseArgs had no --key=value form, so any flag value beginning with -- threw
'Missing value'. The error summary is log-derived text; on 2026-08-08 and
2026-08-09 it began with '--- Runbook: ...' and the intraday-catchup page was
never sent. The watchdog caught the first one 5.5 hours later.

Parser accepts --key=value, splitting on the first = only. All five Python
call sites now emit the single-token form, so no value can be read as a flag."
```

---

### Task 5: Coverage reads a footer only when the file changed

**Files:**
- Modify: `livewire_scripts/coverage_report.py:159-226`
- Test: `tests/test_coverage_report.py`

**Interfaces:**
- Consumes: `_latest_date_in_parquet(path: Path, column_name: str) -> date | None` (`:120`)
- Produces:
  - `_latest_date_with_cache(path: Path, column_name: str, cache: dict) -> tuple[date | None, bool, tuple[float, int] | None]` — returns `(latest, was_cache_hit, (mtime, size))`
  - `compute_coverage(target_date, bronze_root=None, cache_path: Path | None = None)` — new third parameter; `None` means no caching (existing callers unaffected until Task 6 wires it)

**Cache format** (`<log_dir>/coverage_footer_cache.json`):

```json
{"/abs/path/symbol=NVDA/1d.parquet": {"mtime": 1754700000.0, "size": 41233, "latest": "2026-08-06"}}
```

**`size` is in the key on purpose.** mtime alone is not sufficient here: bronze
publishes by `os.replace()`, and **exFAT stores mtime at 2-second granularity**,
so a republish landing inside that bucket leaves the timestamp unchanged and a
mtime-only cache would keep serving the pre-publish max date. Any real
republish that adds or removes a row also changes the file size, so the pair is
far stronger than either half — and both come from the one `stat()` the code
already makes.

`"latest": null` records a file that yielded no date, so an unreadable file is
not re-opened every run either.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_coverage_report.py
"""A parquet whose mtime has not moved cannot have a new max date.

Re-reading its footer is pure cost, and on the external exFAT volume that cost
IS the runtime: a full cold pass measured 2858s on 2026-08-09 against an 1800s
budget, while the same 1d pass warm takes 29.2s. Threads do not fix a cold
metadata walk; not doing the walk does.
"""

import json


def _count_opens(monkeypatch) -> list[Path]:
    """Record every footer read, delegating to the real one."""
    opens: list[Path] = []
    real = coverage_report._latest_date_in_parquet
    monkeypatch.setattr(
        coverage_report,
        "_latest_date_in_parquet",
        lambda path, column_name: (opens.append(path), real(path, column_name))[1],
    )
    return opens


class TestFooterReadsAreIncremental:
    def test_an_unchanged_file_is_not_reopened(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])  # existing helper, :59
        cache_path = tmp_path / "cache.json"
        opens = _count_opens(monkeypatch)

        first = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )
        assert len(opens) >= 1
        opens.clear()

        second = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=root, cache_path=cache_path
        )
        assert opens == [], "an unchanged parquet must not be reopened"
        assert second["1d"].present == first["1d"].present
        assert second["1d"].missing_symbols == first["1d"].missing_symbols

    def test_a_touched_file_is_reread(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        cache_path = tmp_path / "cache.json"
        coverage_report.compute_coverage(date(2026, 8, 6), bronze_root=root, cache_path=cache_path)

        parquet = root / "asset_class=equity" / "symbol=NVDA" / "1d.parquet"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)])
        # Bump mtime explicitly. Two writes inside one test can land on the same
        # stat timestamp, and a test that depends on filesystem clock resolution
        # is a flake waiting for a slower machine.
        stamp = parquet.stat().st_mtime + 10
        os.utime(parquet, (stamp, stamp))

        opens = _count_opens(monkeypatch)
        results = coverage_report.compute_coverage(
            date(2026, 8, 7), bronze_root=root, cache_path=cache_path
        )
        assert opens == [parquet], "a rewritten parquet must be reread"
        assert results["1d"].missing_symbols == []

    def test_no_cache_path_means_no_caching(self, tmp_path, monkeypatch):
        root = tmp_path / "bronze"
        _write_daily(root, "NVDA", [date(2026, 8, 5), date(2026, 8, 6)])
        opens = _count_opens(monkeypatch)
        for _ in range(2):
            coverage_report.compute_coverage(date(2026, 8, 6), bronze_root=root)
        assert len(opens) == 2, "without a cache path every run reads every footer"
```

**Verified, do not re-derive:** the helper is
`_write_daily(bronze_root, symbol, dates)` (`tests/test_coverage_report.py:59`) —
there is no `_write_equity_1d`. Reusing it also settles the frozen-price question:
no new fixture data is authored. `CoverageResult` (`coverage_report.py:76`) exposes
`timeframe/total/present/missing_symbols`. With no `raw/.../_symbols.parquet`
present the traded set is empty, so nothing counts missing — which is what these
assertions rely on. Add `import os` to the test module.

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_coverage_report.py -k Incremental -v`
Expected: FAIL — `TypeError: compute_coverage() got an unexpected keyword argument 'cache_path'`.

- [ ] **Step 3: Add the cache helpers**

```python
# livewire_scripts/coverage_report.py — add after _latest_date_in_parquet (:157)

def _load_footer_cache(cache_path: Path | None) -> dict:
    """Return the persisted footer cache, or an empty one.

    A corrupt or unreadable cache is not an error: the worst case is one slow
    run, and failing the freshness detector because its optimisation file is
    malformed would be trading a real signal for a cosmetic one.
    """
    if cache_path is None:
        return {}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _save_footer_cache(cache_path: Path | None, cache: dict) -> None:
    if cache_path is None:
        return
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(cache, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, cache_path)
    except OSError as exc:  # pragma: no cover - logged but tolerated
        log.warning("could not persist the footer cache: %s", exc)


def _latest_date_with_cache(
    path: Path, column_name: str, cache: dict
) -> tuple[date | None, bool, tuple[float, int] | None]:
    """Return (latest_date, cache_hit, (mtime, size)). Reads `cache`; never writes it.

    A parquet whose mtime has not moved since the last run cannot have gained a
    later max date, so opening its footer is pure cost. On the external exFAT
    volume that cost is the entire runtime — 2858s cold on 2026-08-09 against an
    1800s budget, versus 29.2s warm for the same 1d pass.

    Read-only on purpose. This runs on 16 threads, and having each worker write
    into a shared dict would rest the whole cache's correctness on an argument
    about `dict.__setitem__` atomicity under the GIL — an argument that stops
    holding the day anyone runs this on a free-threaded build. `pool.map`
    preserves input order, so the caller reassembles the new cache
    single-threaded from the returned tuples and the question never arises.
    """
    key = str(path)
    try:
        stat = path.stat()
    except OSError:
        return None, False, None
    stamp = (stat.st_mtime, stat.st_size)
    entry = cache.get(key)
    if entry is not None and (entry.get("mtime"), entry.get("size")) == stamp:
        stored = entry.get("latest")
        return (date.fromisoformat(stored) if stored else None), True, stamp
    return _latest_date_in_parquet(path, column_name), False, stamp
```

Add `import json` and `import os` to the module imports if absent.

- [ ] **Step 4: Wire it into compute_coverage**

```python
# livewire_scripts/coverage_report.py:159-162 — new signature
def compute_coverage(
    target_date: date,
    bronze_root: Path | None = None,
    cache_path: Path | None = None,
) -> dict[str, CoverageResult]:
```

```python
# livewire_scripts/coverage_report.py — after `results: dict[str, CoverageResult] = {}` (:171)
    cache = _load_footer_cache(cache_path)
    fresh: dict = {}
```

```python
# livewire_scripts/coverage_report.py:199-211 — replace the threaded block
        column_name = "trade_date" if tf == "1d" else "bar_timestamp"
        # Threaded: the pass is one small footer read per file, so it is bound by
        # I/O rather than the GIL — pyarrow releases it for the read and the parse.
        started = time.monotonic()
        worker = partial(_latest_date_with_cache, column_name=column_name, cache=cache)
        with ThreadPoolExecutor(max_workers=FOOTER_READ_WORKERS) as pool:
            rows = list(pool.map(worker, parquet_paths))
        hits = sum(1 for _, cached, _ in rows if cached)
        # Rebuilt, not mutated: `fresh` ends up holding exactly the files that
        # exist right now, so a symbol archived to bronze-delisted/ drops out
        # instead of accumulating in the cache forever.
        for path, (latest, _, stamp) in zip(parquet_paths, rows, strict=True):
            if stamp is not None:
                fresh[str(path)] = {
                    "mtime": stamp[0],
                    "size": stamp[1],
                    "latest": latest.isoformat() if latest else None,
                }
        latest_by_symbol = {
            _symbol_from_parquet_path(path): latest
            for path, (latest, _, _) in zip(parquet_paths, rows, strict=True)
            if latest is not None
        }
        # Logged so the next time this outgrows its budget it is measurable rather
        # than a bare timeout. It outgrew the old one silently for four weeks, and
        # then outgrew the replacement too.
        log.info(
            "%s: %d files, %d cached, %d read, %.1fs",
            tf,
            len(parquet_paths),
            hits,
            len(parquet_paths) - hits,
            time.monotonic() - started,
        )
```

```python
# livewire_scripts/coverage_report.py — immediately before `return results` (:226)
    _save_footer_cache(cache_path, fresh)
```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/test_coverage_report.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/coverage_report.py tests/test_coverage_report.py
git commit -m "perf(quality): coverage rereads a footer only when the file changed

A full cold pass measured 2858s on 2026-08-09 against the 1800s budget PR #78
set — the budget was raised on a warm-cache measurement and the lake lives on
an external exFAT volume where cold is the normal state. Threads do not fix a
cold metadata walk.

An unchanged mtime cannot mean a later max date, so cache (mtime, latest) per
file and reread only what moved. The per-timeframe log line now reports cached
versus read, so the next regression is measurable instead of a bare timeout."
```

---

### Task 6: Coverage gets its own job with no budget

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py:565-570`
- Modify: `livewire_scripts/coverage_report.py` (`main`, `:475`) — pass the cache path
- Modify: `scripts/livewire_quality.py:56` — load the scheduled env for `coverage`
- Modify: `tests/test_coverage_report.py:550,565,594,626` — widen the stub lambdas
- Create: `launchd/com.livewire.coverage.plist.example`
- Test: `tests/test_run_daily_update_job.py`

**Interfaces:**
- Consumes: `compute_coverage(target_date, bronze_root, cache_path)` from Task 5
- Produces: nothing importable. The daily job no longer spawns `coverage`.

**Schedule — 11:00 UTC, and the reasoning matters.** The daily job starts 06:00
UTC. Its *healthy* peak is 3.27h (≈09:15 UTC), but `MDW_DAILY_JOB_DEADLINE_SECONDS`
is **4h**, so a legitimate slow run may still be publishing at **10:00 UTC**.
Scheduling coverage at 09:30 would let it walk bronze while equity or Silver is
mid-publish — a mixed-time snapshot, false "missing" counts, a spurious recovery
subprocess, and cache entries recorded against a half-written tree. Pick a time
after the *deadline*, not after the average: **11:00 UTC**, which is also after
the 10:30 UTC watchdog. On this Mac (`Asia/Hong_Kong`, UTC+8) that is
`Hour: 19, Minute: 0`. Task 7 removes the digest's dependency on coverage having
already run, so nothing constrains this from the other side.

⚠️ **The entrypoint must load the scheduled environment.**
`scripts/livewire_quality.py:56-59` calls `load_scheduled_env(REPO_ROOT)` **only**
for `watchdog`, because until now every other quality command was spawned by a
job that had already loaded it. A standalone launchd job is cold: without this,
coverage resolves `MASSIVE_API_KEY` and the SMTP credentials to nothing, so its
auto-recovery cannot fetch and its alert cannot send — it would measure the gap
and then be unable to do or say anything about it. Add `coverage` to that branch:

```python
# scripts/livewire_quality.py:56 — both of these are now launched cold by launchd
    if args.command in {"watchdog", "coverage"}:
        load_scheduled_env(REPO_ROOT)
```

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_run_daily_update_job.py
"""Coverage does not belong on the nightly job's critical path.

It was given a 600s budget, then 1800s; both were guesses against a warm cache
and both expired. An arbitrary timeout around a job whose runtime is dominated
by cold external-volume I/O is the bug, not the number.
"""


class TestTheDailyJobNoLongerRunsCoverage:
    def test_no_coverage_subcommand_is_spawned(self, tmp_path):
        commands: list[list[str]] = []

        def fake_runner(command, **kwargs):
            commands.append(list(command))
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        config = _config(tmp_path)
        log_file = tmp_path / "daily_update_2026-08-08.log"

        run_post_success_quality(config, log_file, runner=fake_runner)

        subcommands = [c[2:] for c in commands]  # drop [python, livewire_quality.py]
        assert not any(sub[:1] == ["coverage"] for sub in subcommands), (
            "coverage has its own launchd job now"
        )
        assert ["weekly"] in subcommands, "weekly still runs here"
        assert any(sub[:1] == ["digest"] for sub in subcommands), "the digest still runs here"
```

**Why this calls `run_post_success_quality` directly rather than `main([])`:**
`tests/test_run_daily_update_job.py:45` has an **autouse** `no_real_quality_spawn`
fixture patching `run_post_success_quality` wholesale, so a `main([])` test can
never observe what it spawns. `run_post_success_quality(config, log_file,
runner=…)` is already how the existing test at `:335` does it.

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_run_daily_update_job.py -k NoLongerRunsCoverage -v`
Expected: FAIL — a `["coverage"]` subcommand is present.

- [ ] **Step 3: Remove the spawn**

Inside `run_post_success_quality` (`livewire_scripts/run_daily_update_job.py:551`),
delete the `["coverage"]` spawn and the comment block above it — the block ending
`_spawn_post_success_quality(runner, log_file, ["coverage"], "coverage report",
timeout=1800)` — replacing it with:

```python
    # Coverage runs as com.livewire.coverage, not here. It was given 600s, then
    # 1800s; a cold full pass measured 2858s on 2026-08-09. The bug was putting
    # a guessed budget around work whose runtime is dominated by cold I/O on an
    # external volume — so it now has its own job and no budget at all.
    # weekly self-skips on non-Sunday.
    _spawn_post_success_quality(runner, log_file, ["weekly"], "weekly quality report")
```

- [ ] **Step 4: Pass the cache path from coverage's own main**

In `livewire_scripts/coverage_report.py` `main()` (`:475`), pass
`cache_path=_resolved_log_dir() / "coverage_footer_cache.json"` into the **first**
`compute_coverage(...)` call. Read the surrounding lines first — `main` also
calls `compute_non_equity_coverage`, which is a 61-file pass and needs no cache.

⚠️ **Four existing tests stub `compute_coverage` with a two-parameter lambda** —
`tests/test_coverage_report.py:550, 565, 594, 626` all use
`lambda d, bronze_root=None: compute_coverage(d, bronze_root=…)`. Once `main()`
passes `cache_path=`, every one raises `TypeError: unexpected keyword argument`.
Widen them to `lambda d, bronze_root=None, cache_path=None: …` — they are
stubbing the call, not asserting its arity.

⚠️ **The post-recovery re-check must pass `cache_path=None`.** Auto-recovery
republishes parquet and then re-measures within the same run, and **exFAT stores
mtime at 2-second granularity** — a rewrite finishing inside that window leaves
the stat unchanged, so a cached re-check would report the gap recovery just
closed. Across the 24 hours between scheduled runs the granularity is irrelevant;
within one run it is exactly the failure mode. The re-check reads only the small
set of symbols recovery touched, so skipping the cache there costs nothing.

- [ ] **Step 5: Create the plist**

```xml
<!-- launchd/com.livewire.coverage.plist.example -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!--
  Coverage freshness pass. Runs 11:00 UTC — after the daily job's 4h DEADLINE
  (06:00 + 4h = 10:00 UTC), not merely after its 3.27h healthy peak, and after
  the 10:30 UTC watchdog. A slow-but-legal daily run must never still be
  publishing while this walks bronze.

  launchd has no TimeZone key, so Hour/Minute are Mac-local. On this Mac
  (Asia/Hong_Kong, UTC+8) 19:00 local = 11:00 UTC. Other Mac timezones:
    America/New_York (EDT, UTC-4)  -> Hour 7,  Minute 0
    Europe/London    (BST, UTC+1)  -> Hour 12, Minute 0
    UTC                            -> Hour 11, Minute 0

  Deliberately no timeout: this job's runtime is dominated by cold metadata
  reads on the external lake volume, and every budget guessed for it so far
  has expired (600s, then 1800s; a cold pass measured 2858s).
-->
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.livewire.coverage</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>-c</string>
        <string>cd /path/to/warehouse/current &amp;&amp; .venv/bin/python scripts/livewire_quality.py coverage</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>19</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>/tmp/com.livewire.coverage.stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/tmp/com.livewire.coverage.stderr.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
```

**Matched to `launchd/com.livewire.daily-update.plist.example` (read 2026-08-09),
not invented.** Three details are load-bearing and were wrong in an earlier draft
of this plan:

- **`/bin/bash -c "cd … && .venv/bin/python …"`**, not a direct `ProgramArguments`
  array. The `cd` into `current` is what makes `os.getcwd()` physical, which is
  the mechanism that lets a flip of the symlink mid-run be safe.
- **`EnvironmentVariables.PATH` must include `/opt/homebrew/bin`.** launchd gives
  a job a minimal PATH. Coverage shells out for recovery and sends its alert
  through the Nodemailer CLI — without homebrew on PATH, `node` is not found and
  the alert dies exactly where this branch is trying to stop alerts from dying.
- **No `RunAtLoad`, no `WorkingDirectory`** — neither appears in the three
  existing job plists, and `RunAtLoad` would fire a full coverage pass every time
  anyone reloads the agent.

- [ ] **Step 6: Run the tests**

Run: `uv run pytest tests/test_run_daily_update_job.py tests/test_coverage_report.py -v`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/run_daily_update_job.py livewire_scripts/coverage_report.py launchd/com.livewire.coverage.plist.example tests/test_run_daily_update_job.py
git commit -m "fix(quality): coverage gets its own job instead of a guessed budget

600s expired every night from 2026-07-07; 1800s expired 5 of 6 nights after
PR #78. Both numbers came from warm-cache measurements of work whose real cost
is cold metadata I/O on the external lake volume — a cold pass measured 2858s.

Coverage now runs as com.livewire.coverage at 11:00 UTC with no timeout, and
uses the incremental footer cache. The daily job keeps weekly and the digest."
```

---

### Task 7: The digest reads the newest coverage log

**Files:**
- Modify: `livewire_scripts/nightly_digest.py:146-153`
- Test: `tests/test_nightly_digest.py`

**Interfaces:**
- Consumes: nothing new
- Produces: `_coverage_section(run_date: str, log_dir: Path) -> list[str]` — same
  signature, now resolves the newest `coverage_*.log` rather than an exact date.
- **Deletes `_target_session` (`:130`).** Verified: `_coverage_section` is its only
  caller anywhere in `livewire_scripts/` or `tests/`. Leaving it would be dead code
  that still has to clear the 95% coverage gate. Delete its direct tests with it.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_nightly_digest.py
"""Coverage now runs on its own schedule, so its log will not match the run date.

Requiring an exact filename match would make the digest report "(not found)"
forever — the same silence that hid the dead detector for four weeks. Read the
newest log and print the date it actually measured.
"""


class TestTheDigestFindsCoverageOnAnySchedule:
    def test_a_coverage_log_from_another_date_is_found(self, tmp_path):
        (tmp_path / "coverage_2026-08-06.log").write_text(
            "2026-08-06 coverage: 1d=13265/13270 (99.96%)\n", encoding="utf-8"
        )
        (tmp_path / "coverage_2026-08-07.log").write_text(
            "2026-08-07 coverage: 1d=13300/13311 (99.92%)\n", encoding="utf-8"
        )

        lines = nightly_digest._coverage_section("2026-08-09", tmp_path)

        assert any("2026-08-07" in line for line in lines), "the newest log wins"
        assert not any("2026-08-06" in line for line in lines)
        assert "(not found)" not in "".join(lines)

    def test_no_coverage_logs_at_all_still_says_not_found(self, tmp_path):
        lines = nightly_digest._coverage_section("2026-08-09", tmp_path)
        assert "  (not found)" in lines

    def test_an_empty_coverage_log_is_not_found(self, tmp_path):
        (tmp_path / "coverage_2026-08-07.log").write_text("", encoding="utf-8")
        lines = nightly_digest._coverage_section("2026-08-09", tmp_path)
        assert "  (not found)" in lines

    def test_a_recent_log_does_not_warn(self, tmp_path):
        (tmp_path / "coverage_2026-08-08.log").write_text(
            "2026-08-08 coverage: 1d=13300/13311 (99.92%)\n", encoding="utf-8"
        )
        assert not any("⚠" in line for line in nightly_digest._coverage_section("2026-08-09", tmp_path))

    def test_a_stale_log_warns_that_the_job_may_be_dead(self, tmp_path):
        # Decoupling the schedules must not buy a silent detector back. If the
        # coverage job stops firing, the newest log stops advancing and the
        # digest would otherwise print a reassuring line indefinitely.
        (tmp_path / "coverage_2026-06-17.log").write_text(
            "2026-06-17 coverage: 1d=13100/13141 (99.69%)\n", encoding="utf-8"
        )
        lines = nightly_digest._coverage_section("2026-08-09", tmp_path)
        assert any("⚠" in line and "coverage job" in line for line in lines)
        assert any("2026-06-17" in line for line in lines), "still shows what it measured"
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_nightly_digest.py -k OnAnySchedule -v`
Expected: FAIL — the first test gets `(not found)`.

- [ ] **Step 3: Make the change**

```python
# livewire_scripts/nightly_digest.py:146-153 — replace _coverage_section
def _coverage_section(run_date: str, log_dir: Path) -> list[str]:
    """Report the newest coverage measurement, whatever day it measured.

    Coverage runs as its own launchd job now, so its log will rarely carry the
    run date. Matching on an exact filename would report "(not found)" every
    night — indistinguishable from the detector being dead, which is exactly
    how the real outage hid for four weeks. The measured date is in the line
    itself, so a lag is visible rather than silent.
    """
    lines = ["Coverage:"]
    logs = sorted(log_dir.glob("coverage_*.log"))
    for path in reversed(logs):
        text = _read_text(path)
        if not text or not text.strip():
            continue
        lines.append("  " + text.splitlines()[0].strip())
        # Decoupling the schedules removes the ordering bug but opens a new
        # silence: if com.livewire.coverage stops firing, the newest log simply
        # stops advancing and the digest keeps printing a green line forever —
        # the same dead-detector shape, one level up. Age is the only thing that
        # distinguishes "measured yesterday" from "has not run since July".
        measured = path.stem.removeprefix("coverage_")
        try:
            age = (date.fromisoformat(run_date) - date.fromisoformat(measured)).days
        except ValueError:
            return lines
        if age > _COVERAGE_STALE_DAYS:
            lines.append(f"  ⚠ newest coverage log is {age} days old — has the coverage job run?")
        return lines
    lines.append("  (not found)")
    return lines
```

Add `_COVERAGE_STALE_DAYS = 3` beside the other module constants — coverage is
daily, the digest reads yesterday's by design, and 3 absorbs one missed run
without absorbing a dead job.

Sorting `coverage_YYYY-MM-DD.log` lexically is date order; taking the newest
non-empty one skips a stub left by a crashed run.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_nightly_digest.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/nightly_digest.py tests/test_nightly_digest.py
git commit -m "fix(digest): read the newest coverage log, not one exact filename

Coverage runs on its own schedule now, so an exact-date lookup would report
(not found) every night — the same silence that hid the dead detector for four
weeks. The measured date is printed, so a lag shows instead of vanishing.

This removes the ordering coupling between the two jobs rather than moving it
onto the watchdog's schedule."
```

---

### Task 8: The disk check watches both volumes

**Files:**
- Modify: `livewire_scripts/nightly_digest.py:156-163`
- Test: `tests/test_nightly_digest.py`

**Interfaces:**
- Consumes: nothing new
- Produces: `_disk_section(data_lake: Path, warehouse: Path | None = None) -> list[str]`.
  `build_digest` (`:166`) passes `warehouse=log_dir.parent`.

**The defect:** `data-lake` is a symlink to `/Volumes/DATA_LAKE/livewire/data-lake`.
`shutil.disk_usage(data_lake)` therefore measures the 13 TiB external volume
(6.6 TiB free) and prints a reassuring number every night, while the internal
volume holding `releases/`, `logs/`, `cursors/` and the venv sits at 93% with
14.7 GiB free — under the 25 GiB `MDW_FLATFILE_MIN_FREE_GB` reserve and
unreported. Note honestly: livewire's own footprint there is only ~2.5 GB, so
this is a monitoring gap, not livewire filling the disk.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_nightly_digest.py
"""One symlink was enough to silently swap the monitored volume.

data-lake points at an external 13 TiB volume, so the nightly line read
"6752.4 GiB free (48% used)" while the volume that actually holds releases,
logs and the venv was under its own reserve with nothing reporting it.
"""

import collections

_Usage = collections.namedtuple("_Usage", "total used free")
_GIB = 1024**3


class TestTheDiskCheckWatchesBothVolumes:
    def test_a_full_warehouse_volume_warns_even_when_the_lake_is_empty(self, tmp_path, monkeypatch):
        lake = tmp_path / "lake"
        warehouse = tmp_path / "warehouse"
        lake.mkdir()
        warehouse.mkdir()

        def fake_usage(path):
            if Path(path) == lake:
                return _Usage(13_000 * _GIB, 6_400 * _GIB, 6_600 * _GIB)
            return _Usage(228 * _GIB, 213 * _GIB, 14 * _GIB)

        monkeypatch.setattr(nightly_digest.shutil, "disk_usage", fake_usage)

        lines = nightly_digest._disk_section(lake, warehouse)
        text = "\n".join(lines)

        assert "6600" in text.replace(",", "") or "6600.0" in text
        assert "14.0 GiB" in text
        assert "⚠" in text, "the warehouse volume is under reserve and must warn"

    def test_both_healthy_does_not_warn(self, tmp_path, monkeypatch):
        lake = tmp_path / "lake"
        warehouse = tmp_path / "warehouse"
        lake.mkdir()
        warehouse.mkdir()
        monkeypatch.setattr(
            nightly_digest.shutil,
            "disk_usage",
            lambda path: _Usage(13_000 * _GIB, 6_400 * _GIB, 6_600 * _GIB),
        )
        lines = nightly_digest._disk_section(lake, warehouse)
        assert "⚠" not in "\n".join(lines)

    def test_one_volume_reports_once_when_both_paths_share_it(self, tmp_path, monkeypatch):
        shared = tmp_path / "everything"
        shared.mkdir()
        monkeypatch.setattr(
            nightly_digest.shutil,
            "disk_usage",
            lambda path: _Usage(228 * _GIB, 100 * _GIB, 128 * _GIB),
        )
        lines = nightly_digest._disk_section(shared, shared)
        assert len([ln for ln in lines if ln.startswith("Disk")]) == 1
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_nightly_digest.py -k BothVolumes -v`
Expected: FAIL — `_disk_section()` takes 1 positional argument.

- [ ] **Step 3: Make the change**

```python
# livewire_scripts/nightly_digest.py:156-163 — replace _disk_section
def _disk_section(data_lake: Path, warehouse: Path | None = None) -> list[str]:
    """Report every distinct volume the warehouse depends on.

    `data-lake` is a symlink to an external volume, so measuring it alone read
    "6752.4 GiB free" every night while the internal volume holding releases,
    logs, cursors and the venv sat below its own reserve, unreported. One
    symlink silently swapped the monitored object.

    Deduplicated on the usage triple, not on st_dev: when both paths live on one
    filesystem — any deployment without the external drive — disk_usage returns
    the identical numbers and this prints a single line.

    # ponytail: two genuinely distinct volumes with byte-identical total/used/free
    # would collapse to one line. Cosmetic, astronomically unlikely, and it keeps
    # the dedup to data this function already has.
    """
    paths = [("lake", data_lake)] if warehouse is None else [("lake", data_lake), ("warehouse", warehouse)]
    lines: list[str] = []
    seen: set[tuple[int, int, int]] = set()
    for label, path in paths:
        try:
            usage = shutil.disk_usage(path)
        except OSError:
            continue
        if tuple(usage) in seen:
            continue
        seen.add(tuple(usage))
        free_gib = usage.free / _GIB
        pct_used = 100.0 * (usage.used / usage.total)
        suffix = "" if len(paths) == 1 else f" [{label}]"
        line = f"Disk{suffix}: {free_gib:.1f} GiB free ({pct_used:.0f}% used)"
        if free_gib < 2 * _MIN_FREE_GB:
            line += f"  ⚠ raw retention deferred — free space under {2 * _MIN_FREE_GB:.0f} GiB"
        lines.append(line)
    return lines
```

**Two things this pins down, both found by trying to write the test:**

1. **Dedup cannot key on `st_dev`.** The test creates `lake` and `warehouse` as two
   directories under one `tmp_path` — necessarily the same device — so an `st_dev`
   dedup collapses them and the two-volume case becomes untestable without faking
   `Path.stat` as well. Keying on the usage triple makes the fake the only thing the
   test has to control, and gives the same answer in production.
2. **The warning string must keep the substring `raw retention deferred`.** The
   existing `test_disk_tripwire_warns_under_reserve`
   (`tests/test_nightly_digest.py:112`) asserts exactly that, and
   `test_build_digest_missing_inputs_render_not_found` (`:104`) passes
   `log_dir=tmp_path/"empty_logs"` and `data_lake=tmp_path`, so `log_dir.parent`
   and the lake share a filesystem and the dedup must keep it at one `Disk:` line.

```python
# livewire_scripts/nightly_digest.py:176 — in build_digest, pass the second volume
        _disk_section(data_lake, log_dir.parent),
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_nightly_digest.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/nightly_digest.py tests/test_nightly_digest.py
git commit -m "fix(digest): the disk check measured the wrong volume every night

data-lake is a symlink to an external 13 TiB drive, so shutil.disk_usage saw
6.6 TiB free and printed a green line, while the internal volume holding
releases, logs, cursors and the venv sat at 93% / 14.7 GiB — below the 25 GiB
reserve, with nothing reporting it. Each release promote takes another 422 MB.

Both volumes now report, deduplicated on the (total, used, free) triple so a
single-filesystem deployment still prints one line."
```

---

### Task 9: A housekeeping command with a retention policy

**Files:**
- Create: `livewire_scripts/housekeeping.py`
- Modify: `scripts/livewire_ops.py:23-27`
- Test: `tests/test_housekeeping.py` (create)

**Interfaces:**
- Consumes: `livewire_scripts.release.prune(keep: int) -> list[str]` (`release.py:203`),
  `livewire_scripts.paths.log_dir()`, `livewire_scripts.paths.data_lake_dir()`
- **Modifies `release.prune` to take `dry_run: bool = False`** — three lines: skip
  the `_discard(path)` call and still return the names. Without it the dry run
  cannot show the release deletions at all, and releases are the largest single
  category in the policy (422 MB each). `promote(..., dry_run=…)` already
  establishes this parameter's spelling in the same module.
- Produces:
  - `PROTECTED_LAKE_DIRS: frozenset[str]`
  - `plan_appledouble(data_lake: Path) -> list[tuple[str, Path]]` — the opt-in
    whole-lake `._*` walk, deliberately outside the nightly sweep
  - `plan_housekeeping(log_dir_path: Path, data_lake: Path, *, log_retention_days: int, keep_evicted: int, now: date | None) -> list[tuple[str, Path]]` — `(reason, path)` pairs, never mutates. Releases are **not** its business: `release.prune()` owns them because it is the only thing that knows to spare what `current` points at.
  - `main(argv=None) -> int` — `--dry-run` (default true), `--apply`

**Scope, deliberately narrow.** Automated retention covers logs, releases and
evicted silver revisions — three small, known paths. AppleDouble sidecars need a
recursive walk of the whole lake, so they are `--appledouble`, opt-in, and never
part of the nightly sweep. Retention does **not** touch
`data-lake/repairs/`, which is 26 GB — 21 GB of it 12,636 `.parquet.bak` files
from the 2026-07-15 cutover. Those are verbatim rollback material and deleting
them is an operator decision with a sign-off, not a retention rule. They also sit
on the volume with 6.6 TiB free, so there is no pressure forcing the call.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_housekeeping.py
"""Retention must never be able to eat something unrecoverable.

Four categories are protected by name, and the test asserts each survives a run
with retention set aggressively enough to delete everything else:

  raw/            older than the rolling 5-year GET floor cannot be refetched
  repairs/triage/ a verdict obtainable today may be unobtainable next year
  repairs/*/backup/ the only basis for rollback-legacy-basis
  the release `current` points at — deleting it leaves current dangling and
                  promote then refuses to rebuild
"""

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from livewire_scripts.housekeeping import plan_appledouble, plan_housekeeping


_NOW = datetime(2026, 8, 9, 12, 0, 0)  # the fixed "today" every test below passes as `now`


def _touch(path: Path, *, days_old: int = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    if days_old:
        # A POSIX timestamp, not ordinal*86400 — ordinals count from year 1, so
        # that arithmetic lands the mtime somewhere around the year 4000 and the
        # age comparison silently inverts.
        stamp = (_NOW - timedelta(days=days_old)).timestamp()
        os.utime(path, (stamp, stamp))
    return path


# Checked against BOTH planners. `plan_housekeeping` no longer walks the lake at
# all, so on its own these four would pass vacuously — and a protection test that
# cannot fail is worse than none, because it reads as coverage. `plan_appledouble`
# is the function that does the recursive walk, so it is the one that has to be
# proven not to enter these trees.
@pytest.mark.parametrize(
    "relative",
    [
        # older than the rolling 5-year GET floor; cannot be refetched, ever
        "raw/massive/us_stocks_sip/day_aggs_v1/date=2021-07-28/part.parquet",
        # a verdict obtainable today may be unobtainable next year
        "repairs/triage/current.json",
        # the only basis rollback-legacy-basis has
        "repairs/yahoo-relabel-batch1/backup/NVDA.parquet",
        # 12,636 verbatim .bak files from the 2026-07-15 cutover: operator call
        "repairs/adjusted-silver-cutover-20260715-production/A.abc.parquet.bak",
        # the protection must hold for the sidecars inside those trees too
        "raw/massive/us_stocks_sip/day_aggs_v1/date=2021-07-28/._part.parquet",
    ],
)
class TestProtectedPathsSurvive:
    def test_the_nightly_sweep_never_plans_it(self, tmp_path, relative):
        lake = tmp_path / "data-lake"
        protected = _touch(lake / relative)
        planned = plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=0, keep_evicted=0, now=date(2026, 8, 9),
        )
        assert protected not in [p for _, p in planned]

    def test_the_opt_in_lake_walk_never_plans_it(self, tmp_path, relative):
        lake = tmp_path / "data-lake"
        protected = _touch(lake / relative)
        assert protected not in [p for _, p in plan_appledouble(lake)]


class TestRetentionDoesItsJob:
    def test_old_logs_are_planned_and_recent_ones_are_not(self, tmp_path):
        logs = tmp_path / "logs"
        old = _touch(logs / "daily_update_2026-06-01.log", days_old=90)
        recent = _touch(logs / "daily_update_2026-08-08.log", days_old=1)
        planned = [p for _, p in plan_housekeeping(
            logs, tmp_path / "data-lake",
            log_retention_days=60, keep_evicted=2, now=date(2026, 8, 9),
        )]
        assert old in planned
        assert recent not in planned

    def test_only_the_oldest_evicted_revisions_are_planned(self, tmp_path):
        lake = tmp_path / "data-lake"
        for rev in ("10", "12", "14", "19", "21"):
            _touch(lake / f"silver/evicted/{rev}/NVDA.parquet")
        planned = [str(p) for _, p in plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=60, keep_evicted=2, now=date(2026, 8, 9),
        )]
        assert any("evicted/10" in p for p in planned)
        assert any("evicted/14" in p for p in planned)
        assert not any("evicted/19" in p for p in planned), "the 2 newest are kept"
        assert not any("evicted/21" in p for p in planned)

    def test_appledouble_is_opt_in_and_never_in_the_nightly_sweep(self, tmp_path):
        lake = tmp_path / "data-lake"
        sidecar = _touch(lake / "bronze/asset_class=equity/symbol=NVDA/._1d.parquet")
        real = _touch(lake / "bronze/asset_class=equity/symbol=NVDA/1d.parquet")

        nightly = [p for _, p in plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=60, keep_evicted=2, now=date(2026, 8, 9),
        )]
        assert sidecar not in nightly, "an rglob over the whole lake is not a nightly job"

        opt_in = [p for _, p in plan_appledouble(lake)]
        assert sidecar in opt_in
        assert real not in opt_in

    def test_appledouble_still_respects_the_protected_trees(self, tmp_path):
        lake = tmp_path / "data-lake"
        protected = _touch(lake / "raw/massive/us_stocks_sip/day_aggs_v1/date=2021-07-28/._part.parquet")
        assert protected not in [p for _, p in plan_appledouble(lake)]


class TestDryRunIsTheDefault:
    def test_plan_never_mutates(self, tmp_path):
        logs = tmp_path / "logs"
        old = _touch(logs / "daily_update_2026-06-01.log", days_old=90)
        plan_housekeeping(
            logs, tmp_path / "data-lake",
            log_retention_days=60, keep_evicted=2, now=date(2026, 8, 9),
        )
        assert old.exists(), "planning is read-only"
```

Add `import os` at the top of the test module.

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_housekeeping.py -v`
Expected: FAIL — `ModuleNotFoundError: livewire_scripts.housekeeping`.

- [ ] **Step 3: Write the module**

```python
#!/usr/bin/env python3
"""Retention sweeps for warehouse artifacts that nothing else prunes.

Deliberately narrow. Four categories are unrecoverable and are protected by
name, never by a size or age rule:

  data-lake/raw/       anything older than the rolling 5-year provider GET floor
                       can never be refetched. LIST advertises 2003; GET 403s.
  repairs/triage/      a triage verdict obtainable today may be unobtainable next
                       year, because the entitlement floor rolls forward.
  repairs/*/backup/    the only basis rollback-legacy-basis has.
  the release `current` points at — promote short-circuits on the symlink, not
                       the directory, so deleting the target leaves current
                       dangling and promote then refuses to rebuild it.

data-lake/repairs/ as a whole is out of scope. It is 26 GB, 21 GB of which is
12,636 verbatim .parquet.bak files from the 2026-07-15 cutover — rollback
material whose disposal is an operator decision, not a retention rule. It also
lives on the volume with 6.6 TiB free, so nothing forces the call.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import date, datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from livewire_scripts.paths import data_lake_dir, log_dir

log = logging.getLogger(__name__)

#: Lake subtrees no retention rule may ever enter. Matched on the path's parts,
#: so `repairs/triage/anything/deeper` is protected too.
PROTECTED_LAKE_DIRS = frozenset({"raw", "repairs"})

LOG_RETENTION_DAYS = 60
KEEP_RELEASES = 3
KEEP_EVICTED = 2


def _is_protected(path: Path, lake: Path) -> bool:
    try:
        relative = path.relative_to(lake)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] in PROTECTED_LAKE_DIRS


def plan_housekeeping(
    log_dir_path: Path,
    data_lake: Path,
    *,
    log_retention_days: int = LOG_RETENTION_DAYS,
    keep_evicted: int = KEEP_EVICTED,
    now: date | None = None,
) -> list[tuple[str, Path]]:
    # No `keep_releases` here on purpose: releases are pruned by
    # `release.prune()` in main(), which alone knows not to collect what
    # `current` points at. A parameter this function never reads would be a
    # promise it does not keep.
    """Return (reason, path) pairs this run would delete. Never mutates."""
    now = now or datetime.now().date()
    planned: list[tuple[str, Path]] = []

    # Logs. 395 files back to 2026-06 with no rotation at the time of writing.
    cutoff = now.toordinal() - log_retention_days
    for path in sorted(log_dir_path.glob("*.log")):
        try:
            modified = date.fromtimestamp(path.stat().st_mtime)
        except OSError:
            continue
        if modified.toordinal() < cutoff:
            planned.append((f"log older than {log_retention_days}d", path))

    # Evicted silver revisions, keeping the newest `keep_evicted` by revision
    # number. Sorted numerically: lexical order puts "10" before "9".
    evicted = data_lake / "silver" / "evicted"
    if evicted.is_dir():
        revisions = sorted(
            (d for d in evicted.iterdir() if d.is_dir() and d.name.isdigit()),
            key=lambda d: int(d.name),
        )
        for directory in revisions[: max(0, len(revisions) - keep_evicted)]:
            planned.append(("superseded evicted revision", directory))

    # AppleDouble sidecars are NOT swept here. `data_lake.rglob("._*")` is a full
    # recursive walk of a 13 TiB exFAT volume — the exact operation this branch is
    # fixing everywhere else (a single-timeframe glob measured 281s cold; `du -sh`
    # over bronze never returned). Putting it inside a nightly 600s budget would
    # reintroduce the bug one task over — and worse than "the sidecars survive":
    # planning completes before anything is deleted, so a traversal that blows
    # the budget deletes NOTHING, logs and evicted revisions included. The whole
    # sweep would be permanently ineffective while reporting only a warning.
    # They are a one-off artifact of the exFAT move, not recurring garbage, so
    # `--appledouble` runs it deliberately instead.
    return planned


def plan_appledouble(data_lake: Path) -> list[tuple[str, Path]]:
    """Opt-in sweep. Walks the whole lake — minutes, not seconds. Never nightly."""
    return [
        ("AppleDouble sidecar", path)
        for path in sorted(data_lake.rglob("._*"))
        if not _is_protected(path, data_lake)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Warehouse retention sweeps")
    parser.add_argument("--apply", action="store_true", help="Actually delete (default: dry run)")
    parser.add_argument(
        "--appledouble",
        action="store_true",
        help="Also sweep exFAT ._* sidecars. Walks the whole lake — minutes. Not for the nightly job.",
    )
    parser.add_argument("--log-retention-days", type=int, default=LOG_RETENTION_DAYS)
    parser.add_argument("--keep-releases", type=int, default=KEEP_RELEASES)
    parser.add_argument("--keep-evicted", type=int, default=KEEP_EVICTED)
    parser.add_argument("--log-dir", type=Path, default=None)
    parser.add_argument("--data-lake", type=Path, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    resolved_logs = args.log_dir or log_dir()
    resolved_lake = args.data_lake or data_lake_dir()

    planned = plan_housekeeping(
        resolved_logs,
        resolved_lake,
        log_retention_days=args.log_retention_days,
        keep_evicted=args.keep_evicted,
    )
    if args.appledouble:
        planned += plan_appledouble(resolved_lake)
    for reason, path in planned:
        log.info("%s %s (%s)", "DELETE" if args.apply else "would delete", path, reason)

    # release.prune never collects the release `current` points at. Previewed in
    # dry run too: the operator review this command exists for is worthless if
    # the one category that deletes 422 MB at a time is invisible until --apply.
    from livewire_scripts.release import prune

    for name in prune(args.keep_releases, dry_run=not args.apply):
        log.info("%s release %s", "pruned" if args.apply else "would prune", name)

    deleted = 0
    failed = 0
    if args.apply:
        for _, path in planned:
            try:
                if path.is_dir():
                    shutil.rmtree(path)  # not ignore_errors: see below
                else:
                    path.unlink(missing_ok=True)
                deleted += 1
            except OSError as exc:
                # `ignore_errors=True` would let this report a clean sweep while
                # the artifacts are still there — the exact "green while wrong"
                # shape the rest of this branch exists to remove.
                failed += 1
                log.warning("could not delete %s: %s", path, exc)
        log.info("%d item(s) deleted, %d failed", deleted, failed)
        return 1 if failed else 0

    log.info("%d item(s) would be deleted", len(planned))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_housekeeping.py -v`
Expected: all PASS.

- [ ] **Step 5: Register the command**

```python
# scripts/livewire_ops.py:23-27
COMMANDS = {
    "run-daily-job": "livewire_scripts.run_daily_update_job",
    "run-intraday-catchup-job": "livewire_scripts.run_intraday_catchup_job",
    "release": "livewire_scripts.release",
    "housekeeping": "livewire_scripts.housekeeping",
}
```

- [ ] **Step 6: Verify the CLI wiring by hand**

Run: `uv run python scripts/livewire_ops.py housekeeping --help`
Expected: the argparse help for the housekeeping command.

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/housekeeping.py scripts/livewire_ops.py tests/test_housekeeping.py
git commit -m "feat(ops): a retention sweep for artifacts nothing else prunes

Logs (395 files back to 2026-06, no rotation), releases (via the existing
release.prune, which never collects what current points at), superseded
evicted silver revisions, and exFAT AppleDouble sidecars that also pollute
symbol discovery. Dry run by default.

raw/ and repairs/ are protected by name, not by an age or size rule: raw is
unrefetchable below the rolling GET floor, and repairs holds the triage verdict
store and every rollback backup. The 26 GB of cutover .parquet.bak files stay
an operator decision — they sit on the volume with 6.6 TiB free, so nothing
forces the call."
```

---

### Task 10: Wire housekeeping into the nightly job, and document the invariants

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py` (after the digest)
- Modify: `CLAUDE.md`
- Test: `tests/test_run_daily_update_job.py`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_run_daily_update_job.py
class TestHousekeepingRunsAfterTheDigest:
    def test_the_nightly_job_runs_a_housekeeping_sweep(self, tmp_path):
        commands: list[list[str]] = []

        def fake_runner(command, **kwargs):
            commands.append(list(command))
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        run_post_success_quality(_config(tmp_path), tmp_path / "daily_update_2026-08-08.log",
                                 runner=fake_runner)

        sweeps = [c for c in commands if "housekeeping" in c]
        assert len(sweeps) == 1
        assert sweeps[0][1].endswith("livewire_ops.py"), "housekeeping is an ops command"
        assert "--apply" in sweeps[0]
        # It runs last: the digest must already have been sent.
        assert commands.index(sweeps[0]) == len(commands) - 1
```

Same reason as Task 6: the autouse `no_real_quality_spawn` fixture makes
`main([])` blind to what `run_post_success_quality` spawns, so the test calls it
directly.

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_run_daily_update_job.py -k Housekeeping -v`
Expected: FAIL — no housekeeping command is spawned.

- [ ] **Step 3: Add the spawn**

`_spawn_post_success_quality` already is this function — try/except, `check=False`,
`capture_output`, and a `WARNING: <label> failed:` line in exactly the shape
`_quality_jobs_section` counts. The only thing it hardcodes is `QUALITY_SCRIPT`, so
give it a parameter rather than writing a second copy that will drift from the
first:

```python
# livewire_scripts/run_daily_update_job.py:344 — one new keyword argument
def _spawn_post_success_quality(runner, log_file, args, label, timeout=120, script=None):
    """Run a post-success subcommand; a failure logs a warning only.

    These jobs must never flip a successful daily run to failure.
    """
    try:
        result = runner(
            [sys.executable, str(script or QUALITY_SCRIPT), *args],
            timeout=timeout,
            check=False,
            capture_output=True,
        )
```

The rest of the body is unchanged. Define `OPS_SCRIPT` beside `QUALITY_SCRIPT`,
pointing at `scripts/livewire_ops.py`, and add this as the **last** call in
`run_post_success_quality`, after the digest:

```python
    # Retention sweep, last. It can only warn: a sweep that deleted nothing is
    # never worth failing a successful ingest run for, and the warning is
    # already counted — `_quality_jobs_section` matches this exact shape, which
    # is the only reason the four-week coverage outage was eventually visible.
    _spawn_post_success_quality(
        runner, log_file, ["housekeeping", "--apply"], "housekeeping",
        timeout=600, script=OPS_SCRIPT,
    )
```

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/ -v -m "not integration"`
Expected: all PASS, coverage gate ≥95%.

- [ ] **Step 5: Document the invariants**

Append to `CLAUDE.md` under "Scheduled-job invariants worth not re-breaking":

```markdown
- ⚠️ **A preflight belongs to the phase that needs it, not to the orchestrator.**
  `daily-backfill` and `backfill-all` sat in `IB_COMMANDS`, and `main()`
  preflights before dispatching — so a down Gateway exited 86 ten seconds in and
  none of the nine phases ran, including the Massive `equity_day_aggs` lane that
  owns the ~20K SIP daily universe. Measured 2026-08-08 and 2026-08-09; Friday
  2026-08-07 is absent from bronze warehouse-wide (equity 0/13311, futures 0/14,
  rates 0/4 — only CBOE and FX, the two non-IB lanes of the *other* job, have it).
  This is the same invariant as "IB is not a single point of failure", which was
  implemented in `run_daily_update_job` and never checked against `sync_runner`.
  **It is not a weekend pattern** — 2 of 16 logged days, and the previous weekend
  ran fine.
- **The equity lane falls back to Massive on a down Gateway.** Silver reads
  equity bronze and the corporate-action store, both Massive-backed, but the
  equity lane runs on IB — so `silver_inputs_ok` gated the rebuild for the whole
  universe on a dependency Silver does not have. Futures and cmdty get no
  fallback: Massive does not carry them.
- ⚠️ **Any alert value beginning with `--` was unsendable.** `parseArgs` had no
  `--key=value` form and rejected a value starting with `--`. The error summary
  is log-derived text; on 2026-08-08 it began with `--- Runbook: ...` and the
  page was never sent — the watchdog caught it 5.5h later. All five Python call
  sites now emit the single-token form.
- ⚠️ **Coverage has its own job because every budget guessed for it expired.**
  600s (from 2026-07-07), then 1800s (5 of 6 nights after PR #78). Both numbers
  came from warm-cache measurements; a cold full pass measured **2858s** on
  2026-08-09. The lake is on an external exFAT volume and the nightly 23.57 GB
  of intraday writes evict the cache, so **cold is the normal state** and thread
  count does not help. `com.livewire.coverage` runs at 11:00 UTC with no timeout,
  and the footer pass now caches `(mtime, latest)` per file — an unchanged mtime
  cannot mean a later max date.
- ⚠️ **The nightly disk line measured the wrong volume.** `data-lake` is a
  symlink to `/Volumes/DATA_LAKE`, so `shutil.disk_usage` reported 6.6 TiB free
  every night while the internal volume holding `releases/`, `logs/`, `cursors/`
  and the venv sat at 93% / 14.7 GiB — below the 25 GiB reserve, unreported.
  livewire's own footprint there is only ~2.5 GB, so this is a monitoring gap
  rather than livewire filling the disk; each `release promote` still takes
  another 422 MB. `_disk_section` now reports both, deduplicated on the usage triple so a
  single-filesystem deployment still prints one line.
- **`housekeeping` prunes logs (60d), releases (keep 3) and superseded evicted
  silver revisions (keep 2).** `raw/` and `repairs/` are protected **by name**,
  never by an age rule: raw below the rolling GET floor cannot be refetched, and
  repairs holds the triage verdict store plus every rollback backup. The 26 GB of
  2026-07-15 cutover `.parquet.bak` files are out of scope by design.
- ⚠️ **The AppleDouble sweep is `--appledouble`, opt-in, and must never go in the
  nightly job.** Finding `._*` means `rglob` over the whole 13 TiB exFAT volume —
  the operation measured at 281s cold for a *single* timeframe glob. Under a
  nightly budget the failure is worse than surviving sidecars: planning finishes
  before anything is deleted, so a traversal that blows the budget deletes
  **nothing**, logs and evicted revisions included, while reporting one warning.
```

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/run_daily_update_job.py CLAUDE.md tests/test_run_daily_update_job.py
git commit -m "feat(ops): run the housekeeping sweep nightly, and record the invariants

Sweep runs after the digest and can only warn, never fail the run — the same
shape _quality_jobs_section already counts.

CLAUDE.md gains the four invariants this branch establishes, each with the
measurement behind it: preflight granularity, the Massive equity fallback, the
alert argument form, coverage's cold-I/O cost, and the two-volume disk check."
```

---

### Task 11: One-off dry run, then the PR

**Files:** none — verification only.

**Outcome (2026-08-10).** All six steps done. PR #79 merged as `e08004d`,
release `e08004da…` promoted and serving, `com.livewire.coverage` loaded.
Measured: coverage **1534.04s → 1398.77s** (footer cache = 8.8%),
`--appledouble` **2047s / 324,121 sidecars**. Step 2's protected-path check
passed on the real lake — no `raw/`, no `repairs/`, and not the release
`current` points at.

- [x] **Step 1: Full suite**

Run: `uv run pytest tests/ -v --cov=clients --cov=scripts --cov=livewire_scripts --cov-report=term-missing`
Expected: PASS, coverage ≥95%.

Run: `npm run test:alerts`
Expected: PASS.

- [x] **Step 2: Housekeeping dry run against the real warehouse**

Run: `uv run python scripts/livewire_ops.py housekeeping`
Expected: `would delete` lines for old logs and superseded evicted revisions,
plus `would prune release <sha>` lines. **Read it before going further.**
Confirm nothing under `data-lake/raw/`, `data-lake/repairs/`, or the release
`current` points at appears anywhere in the output — cross-check the latter with
`readlink ~/market-warehouse/current`.

Then, separately and once:

Run: `uv run python scripts/livewire_ops.py housekeeping --appledouble`
Expected: minutes, not seconds — this is the whole-lake walk. Review the `._*`
list before ever adding `--apply` to it. This command is **not** wired into any
schedule and must not be.

- [x] **Step 3: Coverage timing check with the cache cold, then warm**

Run: `time uv run python scripts/livewire_quality.py coverage --no-recover --force`
twice. Expected: the first run logs mostly `read`, the second mostly `cached`,
and the second is dramatically faster. Record both numbers — they replace the
2858s figure in the spec.

- [x] **Step 4: Open the PR**

```bash
git push -u origin fix/ib-isolation-coverage-housekeeping
gh pr create --title "fix: a down Gateway must not take out seven non-IB phases" --body "$(cat <<'BODY'
Reviewing the week after #76/#77/#78 — which held; seven nights ran clean and
Silver rebuilt 20 → 24 — surfaced four defects, three of them the same failure
mode: a check that still runs, still reports, and is still green, while
measuring the wrong thing.

**1. A down Gateway killed seven phases that never touch IB.** `daily-backfill`
sat in `IB_COMMANDS`, so `assert_gateway_up()` exited 86 ten seconds in and none
of its nine phases ran — including the Massive `equity_day_aggs` lane. Friday
2026-08-07 is missing from bronze warehouse-wide as a result (equity 0/13311,
futures 0/14, rates 0/4). Fired 2026-08-08 and again 2026-08-09. Preflight moves
down to the two phases that use IB; a phase exiting 86 is degraded, not failed;
and the equity lane falls back to `--source massive` so Silver stops being
hostage to a dependency it does not have.

**2. The one alert that should have fired could not be sent.** `parseArgs` had
no `--key=value` form and threw on any value starting with `--`. The summary
began with `--- Runbook: ...`. The watchdog caught it 5.5h later.

**3. Coverage still exceeded its budget 5 of 6 nights.** The 1800s from #78 was
a guess against a warm cache; a cold pass measures **2858s**. Coverage gets its
own job with no timeout, and its footer pass now caches `(mtime, latest)` — an
unchanged mtime cannot mean a later max date.

**4. The nightly disk line measured the wrong volume.** `data-lake` is a symlink
to the external drive, so it read 6.6 TiB free while the internal volume holding
releases, logs and the venv sat below its own reserve, unreported.

Plus a `housekeeping` retention command. `raw/` and `repairs/` are protected by
name — unrefetchable data and rollback material do not get an age rule.

Spec: `docs/superpowers/specs/2026-08-09-ib-isolation-coverage-housekeeping-design.md`
Plan: `docs/superpowers/plans/2026-08-09-ib-isolation-coverage-housekeeping.md`
BODY
)"
```

- [x] **Step 5: Wait for CI, then merge and promote**

Never merge before CI is green. After merging, wait for the push-to-main run to
complete, then:

```bash
git checkout main && git pull
python scripts/livewire_ops.py release promote
```

`promote` runs the **checkout's** builder while exporting `origin/main`, so the
`git checkout main && git pull` is required, not optional.

- [x] **Step 6: Install the new plist**

```bash
WAREHOUSE=~/market-warehouse
sed "s|/path/to/warehouse|$WAREHOUSE|g" launchd/com.livewire.coverage.plist.example \
  > ~/Library/LaunchAgents/com.livewire.coverage.plist
launchctl load ~/Library/LaunchAgents/com.livewire.coverage.plist
```

---

## Self-review

**Spec coverage.** Every spec section maps to a task: Part 1.1 → Task 1, 1.2 →
Task 2, 1.3 → Task 3, Part 2 → Task 4, Part 3.1 → Task 6, 3.2 → Task 5, 3.3 →
Task 7, Part 4.1 → Task 8, 4.2 → Task 9, 4.3 → Tasks 10 and 11. The spec's
testing section is distributed across the tasks' Step 1s.

**Two spec corrections this plan carries** (fold into the spec before opening the
PR):
1. The cold coverage figure is **2858s**, not ">1140s and still running".
2. The internal volume being 93% full is **not livewire's doing** — its footprint
   there is ~2.5 GB of 173 GB used. The monitoring gap is real; the framing of
   housekeeping as urgent space reclamation was wrong. The reclaimable bulk
   (26 GB in `repairs/`) is on the volume with 6.6 TiB free and is deliberately
   out of the automated policy.

**Type consistency.** `plan_housekeeping` returns `list[tuple[str, Path]]` and
every test unpacks `(reason, path)`. `_latest_date_with_cache` returns
`tuple[date | None, bool]` and `compute_coverage` unpacks `(latest, cached)`.
`_disk_section(data_lake, warehouse=None)` matches its `build_digest` call site.
`GATEWAY_DOWN_EXIT_CODE` is imported from `clients.ib_gateway_preflight` in both
`sync_runner` and `run_daily_update_job`.

**Resolved by reading the source (2026-08-09), no longer open questions:**

| Was uncertain | Verified |
|---|---|
| `AlertRequest` fields | six, all required: `run_date`, `log_file`, `attempts`, `exit_code`, `error_summary`, `repo_root` (`run_daily_update_job.py:59`). The command comes from `build_alert_command(config, request)`, so Task 4 tests the builder and needs no subprocess fake. |
| sync_runner test helper | `_make_config(tmp_path)` (`:52`), **not** `_sync_config`. `_flatfile_credentials` is autouse (`:47`). |
| `trading_day_fn`'s type | returns **`str`** (`sync_runner.py:83`); every existing test passes `lambda: "2026-05-28"`. |
| coverage test helper | `_write_daily(bronze_root, symbol, dates)` (`test_coverage_report.py:59`), **not** `_write_equity_1d`. Reusing it means no new fixture prices are authored. |
| the two network tests | the only two `@pytest.mark.integration` in the suite, both in `tests/test_storage_client_compat.py` (`:61`, `:86`) — so `-m "not integration"` is the whole deselection. |
| existing `main()` tests | `tests/test_run_daily_update_job.py:45` has an **autouse** fixture patching `run_post_success_quality`, so Tasks 6 and 10 must call it directly (as `:335` already does) rather than assert through `main([])`. |

**Three defects Pass 1 found in this plan's own test code, now fixed above:**
1. `st_dev` dedup made Task 8's two-volume test unwritable — two dirs under one
   `tmp_path` always share a device. Deduping on the usage triple is both testable
   and correct in production.
2. `_touch`'s `toordinal() * 86400` is not a POSIX timestamp; it put the mtime near
   the year 4000 and inverted every age comparison in Task 9.
3. `plan_housekeeping` took a `keep_releases` it never read — releases are
   `release.prune()`'s business, because only it spares what `current` points at.

| the coverage plist's shape | matched to `launchd/com.livewire.daily-update.plist.example`: `/bin/bash -c "cd … && .venv/bin/python …"`, `EnvironmentVariables.PATH` including `/opt/homebrew/bin`, no `RunAtLoad`, no `WorkingDirectory`. |
| `SUMMARY_PREFIX`'s home | `livewire_scripts/daily_outcomes.py:13`, not `nightly_digest`. |

**Six defects the codex tribunal found that Pass 1 and Pass 3 both missed:**
1. `scripts/livewire_quality.py:56` loads the scheduled env **only for
   `watchdog`** — a cold launchd coverage job would have no Massive key and no
   SMTP credentials, so it could measure the gap and then neither fix nor report
   it.
2. 09:30 UTC is inside the daily job's legal window: the deadline is 4h from
   06:00, i.e. **10:00 UTC**, not the 3.27h healthy peak. Moved to 11:00 UTC.
3. Adding a `degraded` field changes nothing a human sees —
   `_phases_section` still prints `FAILED (exit 86)`. The rendering is the fix.
4. Six existing assertions pin the two-token `--error-summary` form
   (`test_coverage_report.py:482,492`, `test_health_check.py:321,333`,
   `test_universe_screener.py:296`, `test_run_daily_update_job.py:458`).
5. Four existing stubs type `compute_coverage` as `(d, bronze_root=None)`
   (`test_coverage_report.py:550,565,594,626`) and would `TypeError` on
   `cache_path`.
6. `mtime` alone is not a safe cache key on a 2-second-granularity filesystem
   whose publish path is `os.replace()`; `st_size` joins it.

**Since measured (2026-08-10, after the merge and promote):** the wall-clock
effect of the footer cache is **8.8%** — 1534.04s with no cache file, 1398.77s
with all 71,763 entries hitting. The cache is worth 135s of a ~1500s run because
16 threads had already compressed the footer reads to about that; `user+sys` was
26.7s of 1534s, i.e. the job is 98% I/O-blocked. Nothing in this plan rested on
the cache being fast — the no-timeout job is the fix, and this number is why.
