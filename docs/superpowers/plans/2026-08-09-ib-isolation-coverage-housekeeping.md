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
| `livewire_scripts/run_daily_update_job.py` | nightly lane runner | Modify `main` (`:786-845`), remove coverage spawn (`:570`) |
| `livewire_node/send_daily_update_failure_email.mjs` | alert CLI | Modify `parseArgs` (`:62-83`) |
| `livewire_scripts/coverage_report.py` | freshness detector | Modify `compute_coverage` (`:159-226`), add cache helpers |
| `livewire_scripts/nightly_digest.py` | digest assembly | Modify `_coverage_section` (`:146-153`), `_disk_section` (`:156-163`) |
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
- Test: `tests/test_sync_runner.py`

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
    def test_ib_phases_exiting_86_do_not_fail_the_run(self, tmp_path, capsys, monkeypatch):
        config = _sync_config(tmp_path)  # existing helper in this module

        def runner(command, *args, **kwargs):
            rc = GATEWAY_DOWN_EXIT_CODE if "intraday-backfill" in command else 0
            return subprocess.CompletedProcess(command, rc)

        rc = run_sync(config, runner=runner, trading_day_fn=lambda: date(2026, 8, 7))

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
        config = _sync_config(tmp_path)

        def runner(command, *args, **kwargs):
            rc = 1 if "flatfile-ingest-daily" in command else 0
            return subprocess.CompletedProcess(command, rc)

        rc = run_sync(config, runner=runner, trading_day_fn=lambda: date(2026, 8, 7))

        assert rc == 1
        summary = json.loads(
            [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SUMMARY_PREFIX)][-1]
            .removeprefix(SUMMARY_PREFIX)
        )
        assert "daily_backfill_equity_day_aggs" in summary["failed"]
        assert summary["degraded"] == []
```

If `_sync_config` does not already exist in `tests/test_sync_runner.py`, write it
as a module-level helper returning a `SyncConfig` with `log_dir=tmp_path` and the
script paths pointed at the repo root, matching how the existing tests in that
file build their config.

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
    for tf in VOL_INTRADAY_TIMEFRAMES:
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
    print(
        SUMMARY_PREFIX
        + json.dumps(
            {
                "job": "daily_backfill",
                "target_date": str(target_date),
                "phases": phase_results,
                "failed": [
                    p["label"] for p in phase_results if p["exit"] not in (0, GATEWAY_DOWN_EXIT_CODE)
                ],
                "degraded": [p["label"] for p in phase_results if p["exit"] == GATEWAY_DOWN_EXIT_CODE],
            },
            separators=(",", ":"),
        )
    )
```

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
- Consumes: `run_with_retries(config, args, *, env, completion_scope, deadline) -> int` (`:459`), `GATEWAY_DOWN_EXIT_CODE`
- Produces: `lane_codes["equity"]` may now be the fallback's exit code rather than 86, which `silver_inputs_ok` (`:831`) reads.

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
"""

from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE


class TestTheEquityLaneFallsBackToMassive:
    def test_a_down_gateway_retries_equity_on_massive(self, monkeypatch, tmp_path):
        calls: list[list[str]] = []

        def fake_run_with_retries(config, args, *, env, completion_scope, deadline):
            calls.append(list(args))
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
        monkeypatch.setattr(daily_runner, "_spawn_post_success_quality", lambda *a, **k: None)

        daily_runner.main([])

        equity_calls = [c for c in calls if "equity" in c]
        assert len(equity_calls) == 2, "equity should be retried exactly once"
        assert equity_calls[1][equity_calls[1].index("--source") + 1] == "massive"
        assert silver_ran == [True], "Silver must rebuild once the fallback succeeds"

    def test_futures_and_cmdty_get_no_fallback(self, monkeypatch, tmp_path):
        calls: list[list[str]] = []

        def fake_run_with_retries(config, args, *, env, completion_scope, deadline):
            calls.append(list(args))
            return GATEWAY_DOWN_EXIT_CODE

        monkeypatch.setattr(daily_runner, "run_with_retries", fake_run_with_retries)
        monkeypatch.setattr(daily_runner, "run_corporate_action_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_cboe_volatility_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_fx_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "_spawn_post_success_quality", lambda *a, **k: None)

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

Leave the `switch (key)` block below unchanged.

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
    def test_error_summary_is_a_single_equals_token(self):
        summary = "--- Runbook: /Users/moremeds/runbooks/trading-stack/ib-gateway-ibc.md ---"
        captured: list[list[str]] = []

        def fake_run(command, **kwargs):
            captured.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="sent")

        daily_runner.send_failure_alert(
            _runner_config(),  # existing helper in this module
            daily_runner.AlertRequest(exit_code=86, attempts=1, error_summary=summary),
            Path("/tmp/daily_update_2026-08-08.log"),
            runner=fake_run,
        )

        command = captured[0]
        assert f"--error-summary={summary}" in command
        assert "--error-summary" not in command, "the bare two-token form must be gone"
```

Adjust `AlertRequest(...)` construction to match the real dataclass in
`run_daily_update_job.py`; read it before writing this test rather than
guessing the field names.

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

- [ ] **Step 8: Run everything**

Run: `npm run test:alerts && uv run pytest tests/ -v --deselect tests/test_daily_bar_fallback.py::test_nasdaq_fallback_live --deselect tests/test_daily_bar_fallback.py::test_stooq_fallback_live`
Expected: all PASS. (Confirm the two deselect targets against the real test ids
before running; the constraint is that the two network-touching compat tests are
excluded.)

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
  - `_latest_date_with_cache(path: Path, column_name: str, cache: dict) -> tuple[date | None, bool]` — returns `(latest, was_cache_hit)`
  - `compute_coverage(target_date, bronze_root=None, cache_path: Path | None = None)` — new third parameter; `None` means no caching (existing callers unaffected until Task 6 wires it)

**Cache format** (`<log_dir>/coverage_footer_cache.json`):

```json
{"/abs/path/symbol=NVDA/1d.parquet": {"mtime": 1754700000.0, "latest": "2026-08-06"}}
```

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


class TestFooterReadsAreIncremental:
    def test_an_unchanged_file_is_not_reopened(self, tmp_path, monkeypatch):
        parquet = _write_equity_1d(tmp_path, "NVDA", last_date=date(2026, 8, 6))  # existing helper
        cache_path = tmp_path / "cache.json"

        opens: list[Path] = []
        real = coverage_report._latest_date_in_parquet

        def counting(path, column_name):
            opens.append(path)
            return real(path, column_name)

        monkeypatch.setattr(coverage_report, "_latest_date_in_parquet", counting)

        first = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=tmp_path / "bronze", cache_path=cache_path
        )
        assert len(opens) >= 1
        opens.clear()

        second = coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=tmp_path / "bronze", cache_path=cache_path
        )
        assert opens == [], "an unchanged parquet must not be reopened"
        assert second["1d"].present == first["1d"].present
        assert second["1d"].missing_symbols == first["1d"].missing_symbols

    def test_a_touched_file_is_reread(self, tmp_path, monkeypatch):
        parquet = _write_equity_1d(tmp_path, "NVDA", last_date=date(2026, 8, 6))
        cache_path = tmp_path / "cache.json"
        coverage_report.compute_coverage(
            date(2026, 8, 6), bronze_root=tmp_path / "bronze", cache_path=cache_path
        )

        _write_equity_1d(tmp_path, "NVDA", last_date=date(2026, 8, 7))  # rewrites, new mtime

        opens: list[Path] = []
        real = coverage_report._latest_date_in_parquet
        monkeypatch.setattr(
            coverage_report,
            "_latest_date_in_parquet",
            lambda p, column_name: (opens.append(p), real(p, column_name))[1],
        )

        results = coverage_report.compute_coverage(
            date(2026, 8, 7), bronze_root=tmp_path / "bronze", cache_path=cache_path
        )
        assert opens, "a rewritten parquet must be reread"
        assert results["1d"].missing_symbols == []

    def test_no_cache_path_means_no_caching(self, tmp_path, monkeypatch):
        _write_equity_1d(tmp_path, "NVDA", last_date=date(2026, 8, 6))
        opens: list[Path] = []
        real = coverage_report._latest_date_in_parquet
        monkeypatch.setattr(
            coverage_report,
            "_latest_date_in_parquet",
            lambda p, column_name: (opens.append(p), real(p, column_name))[1],
        )
        for _ in range(2):
            coverage_report.compute_coverage(date(2026, 8, 6), bronze_root=tmp_path / "bronze")
        assert len(opens) >= 2, "without a cache path every run reads every footer"
```

`_write_equity_1d` must write a real parquet at
`<tmp>/bronze/asset_class=equity/symbol=<TICKER>/1d.parquet` with a `trade_date`
column. If `tests/test_coverage_report.py` already has such a helper, reuse it;
otherwise write one using **NVDA's real closes** frozen at authoring time with
the as-of date in a comment, per the no-synthetic-data rule.

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


def _latest_date_with_cache(path: Path, column_name: str, cache: dict) -> tuple[date | None, bool]:
    """Return (latest_date, cache_hit).

    A parquet whose mtime has not moved since the last run cannot have gained a
    later max date, so opening its footer is pure cost. On the external exFAT
    volume that cost is the entire runtime — 2858s cold on 2026-08-09 against an
    1800s budget, versus 29.2s warm for the same 1d pass.

    `cache` is mutated from the thread pool. Each worker assigns one distinct
    key and no worker reads another's, so the GIL makes this safe without a lock.
    """
    key = str(path)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None, False
    entry = cache.get(key)
    if entry is not None and entry.get("mtime") == mtime:
        stored = entry.get("latest")
        return (date.fromisoformat(stored) if stored else None), True
    latest = _latest_date_in_parquet(path, column_name)
    cache[key] = {"mtime": mtime, "latest": latest.isoformat() if latest else None}
    return latest, False
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
```

```python
# livewire_scripts/coverage_report.py:199-211 — replace the threaded block
        column_name = "trade_date" if tf == "1d" else "bar_timestamp"
        # Threaded: the pass is one small footer read per file, so it is bound by
        # I/O rather than the GIL — pyarrow releases it for the read and the parse.
        started = time.monotonic()
        worker = partial(_latest_date_with_cache, column_name=column_name, cache=cache)
        with ThreadPoolExecutor(max_workers=FOOTER_READ_WORKERS) as pool:
            pairs = list(pool.map(worker, parquet_paths))
        hits = sum(1 for _, cached in pairs if cached)
        latest_by_symbol = {
            _symbol_from_parquet_path(path): latest
            for path, (latest, _) in zip(parquet_paths, pairs, strict=True)
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
    _save_footer_cache(cache_path, cache)
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
- Create: `launchd/com.livewire.coverage.plist.example`
- Test: `tests/test_run_daily_update_job.py`

**Interfaces:**
- Consumes: `compute_coverage(target_date, bronze_root, cache_path)` from Task 5
- Produces: nothing importable. The daily job no longer spawns `coverage`.

**Schedule.** The daily job starts 06:00 UTC and peaks at 3.27h, so it is done by
~09:15 UTC. The watchdog runs 10:30 UTC. Put coverage at **09:30 UTC**, which on
this Mac (`Asia/Hong_Kong`, UTC+8) is `Hour: 17, Minute: 30`. Task 7 removes the
digest's dependency on coverage having already run, so this time only needs to be
after the daily job, not before the digest.

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_run_daily_update_job.py
"""Coverage does not belong on the nightly job's critical path.

It was given a 600s budget, then 1800s; both were guesses against a warm cache
and both expired. An arbitrary timeout around a job whose runtime is dominated
by cold external-volume I/O is the bug, not the number.
"""


class TestTheDailyJobNoLongerRunsCoverage:
    def test_no_coverage_subcommand_is_spawned(self, monkeypatch):
        spawned: list[list[str]] = []
        monkeypatch.setattr(
            daily_runner,
            "_spawn_post_success_quality",
            lambda runner, log_file, args, label, timeout=120: spawned.append(list(args)),
        )
        monkeypatch.setattr(daily_runner, "run_with_retries", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_corporate_action_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_cboe_volatility_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_fx_sync", lambda *a, **k: 0)
        monkeypatch.setattr(daily_runner, "run_silver_rebuild", lambda *a, **k: 0)

        daily_runner.main([])

        assert ["coverage"] not in spawned, "coverage has its own launchd job now"
        assert ["weekly"] in spawned, "weekly still runs here"
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_run_daily_update_job.py -k NoLongerRunsCoverage -v`
Expected: FAIL — `["coverage"]` is in `spawned`.

- [ ] **Step 3: Remove the spawn**

Delete the coverage spawn and its comment block at
`livewire_scripts/run_daily_update_job.py:563-570`, replacing it with:

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
`cache_path=_resolved_log_dir() / "coverage_footer_cache.json"` into the
`compute_coverage(...)` call. Read the surrounding lines first — `main` also
calls `compute_non_equity_coverage`, which is a 61-file pass and needs no cache.

- [ ] **Step 5: Create the plist**

```xml
<!-- launchd/com.livewire.coverage.plist.example -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!--
  Coverage freshness pass. Runs 09:30 UTC, after the daily job (06:00 UTC,
  peaks at 3.27h) and independent of the watchdog at 10:30 UTC.

  launchd has no TimeZone key, so Hour/Minute are Mac-local. On this Mac
  (Asia/Hong_Kong, UTC+8) 17:30 local = 09:30 UTC. Other Mac timezones:
    America/New_York (EDT, UTC-4)  -> Hour 5, Minute 30
    Europe/London    (BST, UTC+1)  -> Hour 10, Minute 30
    UTC                            -> Hour 9, Minute 30

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
        <string>/path/to/warehouse/current/.venv/bin/python</string>
        <string>/path/to/warehouse/current/scripts/livewire_quality.py</string>
        <string>coverage</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/warehouse/current</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>17</integer>
        <key>Minute</key>
        <integer>30</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/path/to/warehouse/logs/coverage_job.log</string>
    <key>StandardErrorPath</key>
    <string>/path/to/warehouse/logs/coverage_job.log</string>
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
```

Compare against `launchd/com.livewire.daily-update.plist.example` and match its
structure — if it sets `EnvironmentVariables` or a different venv path shape,
mirror that rather than the above.

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

Coverage now runs as com.livewire.coverage at 09:30 UTC with no timeout, and
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
  `_target_session` (`:130`) becomes unused by this function; leave it in place,
  it is still the honest way to name the session elsewhere.

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
        if text and text.strip():
            lines.append("  " + text.splitlines()[0].strip())
            return lines
    lines.append("  (not found)")
    return lines
```

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

    Deduplicated by device id: when both paths live on one filesystem — any
    deployment without the external drive — this prints a single line.
    """
    paths = [("lake", data_lake)] if warehouse is None else [("lake", data_lake), ("warehouse", warehouse)]
    lines: list[str] = []
    seen: set[int] = set()
    for label, path in paths:
        try:
            device = path.stat().st_dev
        except OSError:
            continue
        if device in seen:
            continue
        seen.add(device)
        usage = shutil.disk_usage(path)
        free_gib = usage.free / _GIB
        pct_used = 100.0 * (usage.used / usage.total)
        suffix = "" if len(paths) == 1 else f" [{label}]"
        line = f"Disk{suffix}: {free_gib:.1f} GiB free ({pct_used:.0f}% used)"
        if free_gib < 2 * _MIN_FREE_GB:
            line += f"  ⚠ under {2 * _MIN_FREE_GB:.0f} GiB — raw retention deferred / promotes at risk"
        lines.append(line)
    return lines
```

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

Both volumes now report, deduplicated by device id so a single-filesystem
deployment still prints one line."
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
- Produces:
  - `PROTECTED_LAKE_DIRS: frozenset[str]`
  - `plan_housekeeping(log_dir: Path, data_lake: Path, *, log_retention_days: int, keep_releases: int, keep_evicted: int, now: date) -> list[tuple[str, Path]]` — `(reason, path)` pairs, never mutates
  - `main(argv=None) -> int` — `--dry-run` (default true), `--apply`

**Scope, deliberately narrow.** Automated retention covers logs, releases,
evicted silver revisions and AppleDouble sidecars. It does **not** touch
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

from datetime import date
from pathlib import Path

from livewire_scripts.housekeeping import plan_housekeeping


def _touch(path: Path, *, days_old: int = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    if days_old:
        stamp = (date(2026, 8, 9).toordinal() - days_old) * 86400
        os.utime(path, (stamp, stamp))
    return path


class TestProtectedPathsSurvive:
    def test_raw_partitions_are_never_planned_for_deletion(self, tmp_path):
        lake = tmp_path / "data-lake"
        raw = _touch(lake / "raw/massive/us_stocks_sip/day_aggs_v1/date=2021-07-28/part.parquet")
        planned = plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=0, keep_releases=0, keep_evicted=0, now=date(2026, 8, 9),
        )
        assert raw not in [p for _, p in planned]

    def test_the_triage_verdict_store_is_never_planned(self, tmp_path):
        lake = tmp_path / "data-lake"
        verdicts = _touch(lake / "repairs/triage/current.json")
        planned = plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=0, keep_releases=0, keep_evicted=0, now=date(2026, 8, 9),
        )
        assert verdicts not in [p for _, p in planned]

    def test_repair_backups_are_never_planned(self, tmp_path):
        lake = tmp_path / "data-lake"
        backup = _touch(lake / "repairs/yahoo-relabel-batch1/backup/NVDA.parquet")
        planned = plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=0, keep_releases=0, keep_evicted=0, now=date(2026, 8, 9),
        )
        assert backup not in [p for _, p in planned]

    def test_the_whole_repairs_tree_is_out_of_scope(self, tmp_path):
        lake = tmp_path / "data-lake"
        cutover = _touch(lake / "repairs/adjusted-silver-cutover-20260715-production/A.abc.parquet.bak")
        planned = plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=0, keep_releases=0, keep_evicted=0, now=date(2026, 8, 9),
        )
        assert cutover not in [p for _, p in planned]


class TestRetentionDoesItsJob:
    def test_old_logs_are_planned_and_recent_ones_are_not(self, tmp_path):
        logs = tmp_path / "logs"
        old = _touch(logs / "daily_update_2026-06-01.log", days_old=90)
        recent = _touch(logs / "daily_update_2026-08-08.log", days_old=1)
        planned = [p for _, p in plan_housekeeping(
            logs, tmp_path / "data-lake",
            log_retention_days=60, keep_releases=3, keep_evicted=2, now=date(2026, 8, 9),
        )]
        assert old in planned
        assert recent not in planned

    def test_only_the_oldest_evicted_revisions_are_planned(self, tmp_path):
        lake = tmp_path / "data-lake"
        for rev in ("10", "12", "14", "19", "21"):
            _touch(lake / f"silver/evicted/{rev}/NVDA.parquet")
        planned = [str(p) for _, p in plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=60, keep_releases=3, keep_evicted=2, now=date(2026, 8, 9),
        )]
        assert any("evicted/10" in p for p in planned)
        assert any("evicted/14" in p for p in planned)
        assert not any("evicted/19" in p for p in planned), "the 2 newest are kept"
        assert not any("evicted/21" in p for p in planned)

    def test_appledouble_sidecars_are_planned(self, tmp_path):
        lake = tmp_path / "data-lake"
        sidecar = _touch(lake / "bronze/asset_class=equity/symbol=NVDA/._1d.parquet")
        real = _touch(lake / "bronze/asset_class=equity/symbol=NVDA/1d.parquet")
        planned = [p for _, p in plan_housekeeping(
            tmp_path / "logs", lake,
            log_retention_days=60, keep_releases=3, keep_evicted=2, now=date(2026, 8, 9),
        )]
        assert sidecar in planned
        assert real not in planned


class TestDryRunIsTheDefault:
    def test_plan_never_mutates(self, tmp_path):
        logs = tmp_path / "logs"
        old = _touch(logs / "daily_update_2026-06-01.log", days_old=90)
        plan_housekeeping(
            logs, tmp_path / "data-lake",
            log_retention_days=60, keep_releases=3, keep_evicted=2, now=date(2026, 8, 9),
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
    keep_releases: int = KEEP_RELEASES,
    keep_evicted: int = KEEP_EVICTED,
    now: date | None = None,
) -> list[tuple[str, Path]]:
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

    # AppleDouble sidecars. exFAT artifacts; they also pollute symbol discovery.
    for path in sorted(data_lake.rglob("._*")):
        if not _is_protected(path, data_lake):
            planned.append(("AppleDouble sidecar", path))

    return planned


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Warehouse retention sweeps")
    parser.add_argument("--apply", action="store_true", help="Actually delete (default: dry run)")
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
        keep_releases=args.keep_releases,
        keep_evicted=args.keep_evicted,
    )
    for reason, path in planned:
        log.info("%s %s (%s)", "DELETE" if args.apply else "would delete", path, reason)

    if args.apply:
        for _, path in planned:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
        # release.prune never collects the release `current` points at.
        from livewire_scripts.release import prune

        for name in prune(args.keep_releases):
            log.info("pruned release %s", name)

    log.info("%d item(s) %s", len(planned), "deleted" if args.apply else "would be deleted")
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
    def test_the_nightly_job_runs_a_housekeeping_sweep(self, monkeypatch):
        spawned: list[list[str]] = []
        monkeypatch.setattr(
            daily_runner,
            "_spawn_post_success_quality",
            lambda runner, log_file, args, label, timeout=120: spawned.append(list(args)),
        )
        for name in (
            "run_with_retries", "run_corporate_action_sync",
            "run_cboe_volatility_sync", "run_fx_sync", "run_silver_rebuild",
        ):
            monkeypatch.setattr(daily_runner, name, lambda *a, **k: 0)
        swept: list[list[str]] = []
        monkeypatch.setattr(daily_runner, "_spawn_housekeeping", lambda runner, log_file: swept.append(["ran"]))

        daily_runner.main([])

        assert swept == [["ran"]]
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_run_daily_update_job.py -k Housekeeping -v`
Expected: FAIL — `AttributeError: module has no attribute '_spawn_housekeeping'`.

- [ ] **Step 3: Add the spawn**

```python
# livewire_scripts/run_daily_update_job.py — beside _spawn_post_success_quality
def _spawn_housekeeping(runner, log_file) -> None:
    """Run the retention sweep. A failure logs a warning and nothing more.

    Housekeeping deleting nothing is never worth failing a successful ingest
    run for — but the warning must be counted, which `_quality_jobs_section`
    already does by matching the same "WARNING: <label> failed:" shape.
    """
    try:
        result = runner(
            [sys.executable, str(OPS_SCRIPT), "housekeeping", "--apply"],
            timeout=600,
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            append_log(log_file, f"WARNING: housekeeping failed: exit_code={result.returncode}")
    except Exception as exc:  # pragma: no cover - logged but tolerated
        append_log(log_file, f"WARNING: housekeeping failed: {exc}")
```

Define `OPS_SCRIPT` beside the existing `QUALITY_SCRIPT` constant, pointing at
`scripts/livewire_ops.py`. Call `_spawn_housekeeping(runner, log_file)`
immediately after the nightly digest is sent.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/ -v` (with the two network compat tests deselected)
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
  count does not help. `com.livewire.coverage` runs at 09:30 UTC with no timeout,
  and the footer pass now caches `(mtime, latest)` per file — an unchanged mtime
  cannot mean a later max date.
- ⚠️ **The nightly disk line measured the wrong volume.** `data-lake` is a
  symlink to `/Volumes/DATA_LAKE`, so `shutil.disk_usage` reported 6.6 TiB free
  every night while the internal volume holding `releases/`, `logs/`, `cursors/`
  and the venv sat at 93% / 14.7 GiB — below the 25 GiB reserve, unreported.
  livewire's own footprint there is only ~2.5 GB, so this is a monitoring gap
  rather than livewire filling the disk; each `release promote` still takes
  another 422 MB. `_disk_section` now reports both, deduplicated by device id.
- **`housekeeping` prunes logs (60d), releases (keep 3), superseded evicted
  silver revisions (keep 2) and AppleDouble sidecars.** `raw/` and `repairs/`
  are protected **by name**: raw below the rolling GET floor cannot be refetched,
  and repairs holds the triage verdict store plus every rollback backup. The
  26 GB of 2026-07-15 cutover `.parquet.bak` files are out of scope by design.
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

- [ ] **Step 1: Full suite**

Run: `uv run pytest tests/ -v --cov=clients --cov=scripts --cov=livewire_scripts --cov-report=term-missing`
Expected: PASS, coverage ≥95%.

Run: `npm run test:alerts`
Expected: PASS.

- [ ] **Step 2: Housekeeping dry run against the real warehouse**

Run: `uv run python scripts/livewire_ops.py housekeeping`
Expected: a list of `would delete` lines. **Read it before going further.**
Confirm nothing under `data-lake/raw/`, `data-lake/repairs/`, or the release
`current` points at appears anywhere in the output.

- [ ] **Step 3: Coverage timing check with the cache cold, then warm**

Run: `time uv run python scripts/livewire_quality.py coverage --no-recover --force`
twice. Expected: the first run logs mostly `read`, the second mostly `cached`,
and the second is dramatically faster. Record both numbers — they replace the
2858s figure in the spec.

- [ ] **Step 4: Open the PR**

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

- [ ] **Step 5: Wait for CI, then merge and promote**

Never merge before CI is green. After merging, wait for the push-to-main run to
complete, then:

```bash
git checkout main && git pull
python scripts/livewire_ops.py release promote
```

`promote` runs the **checkout's** builder while exporting `origin/main`, so the
`git checkout main && git pull` is required, not optional.

- [ ] **Step 6: Install the new plist**

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

**Known soft spots the implementer must resolve by reading, not guessing:**
- `AlertRequest`'s real field names (Task 4, Step 5).
- Whether `tests/test_sync_runner.py` already has a `_sync_config` helper (Task 2).
- Whether `tests/test_coverage_report.py` already has a parquet-writing helper
  (Task 5) — and if not, the frozen-real-price rule applies to the new one.
- The exact ids of the two network-touching compat tests to deselect.
- `launchd/com.livewire.daily-update.plist.example`'s actual structure (Task 6).
