# Livewire ledger L2 — constants become measurements — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every operating constant in this repo lives in one dict with a
recorded scope, the orchestrator emits it to the ledger as
`measurements(source='declared')` at run start, and `status` flags when 14
days of real `source='measured'` rows drift more than 2× from the declared
value — closing bucket C (`docs/postmortems/` taxonomy: "operating constant
assumed, not measured", 11 post-mortems, 7 prose-only) the same way L1 closed
buckets B/E/F/G.

**Architecture:** `clients/constants.py` declares one **flat** dict,
`DECLARED: dict[str, tuple[float, str]]` (`key -> (value, unit)`), where the
key carries its scope after a `/` — `"lane_budget_s/equity"`,
`"massive_requests_per_minute/fx"`, and unscoped keys such as
`"failure_rate_tolerance"` with no `/` at all. One function,
`declared(key) -> float`, looks the key up and applies an `LW_DECLARED_<KEY>`
env override. Splitting a key into `(name, scope)` happens **only at emit
time** (`key.split("/", 1)`, scope `""` when the key has no `/`), so the
ledger keeps its two columns while the source of truth stays one flat
mapping. `run_daily_update_job.main()` emits the whole `DECLARED` dict as
`measurements(source='declared')` rows once per run, right after it emits the
`runs` row. Call sites that used to read a bespoke env var or a bare
module-level constant call `constants.declared(...)` instead. `status.py`
gets one new `CHECKS` row comparing the latest declared value against the
14-day p95 of same-`(name, scope)` measured rows.

**Tech Stack:** Python 3.13, pyarrow (ledger schema, unchanged from L1),
DuckDB SQL (status checks), pytest + `monkeypatch.setenv`/`setitem`.

**Spec:** `docs/superpowers/specs/2026-09-02-livewire-ledger-design.md` §4
("Constants become measurements"). §2's per-lane budgets and the `measurements`
table itself are L1, already merged (`origin/main` sha `7e1124455d7df0aa42626dd622ed99f6923b3b6d`,
promoted to the mini) — this plan only adds the `declared`/`measured` pairing
and the drift check on top of what L1 shipped.

**Scope:** Only the constants listed in Task 1's `DECLARED` dict are migrated.
The ~30 other tuning env vars (`MDW_BACKFILL_*`, `MDW_ORCHESTRATOR_*`,
`MDW_FLATFILE_*WORKERS`/`*BUCKETS`/`*LOOKBACK`, `MDW_ALERT_*`,
`MDW_SYNC_PHASE_TIMEOUT_SECONDS`) stay as they are; nine of them are pinned by
name in tests, and rewriting those tests buys nothing today. Migrate one when
it first causes an incident.

## Global Constraints

- `clients/constants.py` is **the only place a number lives** for the values
  in `DECLARED` — every other module imports from it, none re-declares.
- `DECLARED` is **flat**: `key -> (value, unit)`. The scope is part of the
  key, after a `/`. There is no nested `{scope: (value, unit)}` form anywhere
  in this plan; a name that has several scopes gets several keys.
- A number whose meaning is scope-bound (spec §4: "every rate-limit and floor
  number in this repo is scope-bound") **must** carry the scope in its key.
  `massive_requests_per_minute/fx` is right; a bare
  `massive_requests_per_minute` is a bug in this file.
- Env override collapses to exactly one form: `LW_DECLARED_` + the key
  upper-cased with `/` and `-` both replaced by `_`. So
  `lane_budget_s/corporate-actions` → `LW_DECLARED_LANE_BUDGET_S_CORPORATE_ACTIONS`
  and `failure_rate_tolerance` → `LW_DECLARED_FAILURE_RATE_TOLERANCE`. There is
  no double-underscore variant and no per-module alias.
- The per-module env vars this replaces (`MDW_FLATFILE_MIN_PUBLISH_RATIO`,
  `MDW_COVERAGE_ALERT_THRESHOLD`, `MDW_FLATFILE_MIN_FREE_GB`) are **deleted**,
  not kept as aliases.
- `ledger.emit()` is the only write path (from L1); this plan adds no new
  ledger table and no new column to `measurements`.
- Never touch the currently-running mini job while implementing this (per
  `CLAUDE.md` rule 12) — this is dev-branch work, tested with `uv run pytest`,
  not run against production until promoted.

---

### Task 1: `clients/constants.py` — the DECLARED dict and lookup function

**Files:**

- Create: `clients/constants.py`
- Test: `tests/test_constants.py`

**Interfaces:**

- Produces: `DECLARED: dict[str, tuple[float, str]]` (key → (value, unit), scope
  carried in the key after `/`); `declared(key: str) -> float` — later tasks
  call this instead of `os.getenv` or a bare constant.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_constants.py
import pytest

from clients import constants


def test_declared_returns_the_dict_value():
    assert constants.declared("failure_rate_tolerance") == 0.05


def test_declared_rejects_an_unknown_key():
    with pytest.raises(KeyError):
        constants.declared("no_such_constant")


def test_a_scoped_name_without_its_scope_is_not_a_key():
    # the scope is part of the key; the bare name is not declared
    with pytest.raises(KeyError):
        constants.declared("lane_budget_s")


def test_env_override_wins_and_is_typed_as_float(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_FAILURE_RATE_TOLERANCE", "0.10")
    assert constants.declared("failure_rate_tolerance") == 0.10


def test_lane_budget_is_declared_once_per_lane():
    assert constants.declared("lane_budget_s/corporate-actions") == 3 * 60 * 60
    assert constants.declared("lane_budget_s/cmdty") == 30 * 60


def test_lane_budget_env_override_is_per_lane(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_LANE_BUDGET_S_CORPORATE_ACTIONS", "7200")
    assert constants.declared("lane_budget_s/corporate-actions") == 7200
    # a different lane is untouched
    assert constants.declared("lane_budget_s/cmdty") == 30 * 60


def test_every_declared_entry_is_a_value_and_a_nonempty_unit():
    for key, (value, unit) in constants.DECLARED.items():
        assert isinstance(value, (int, float))
        assert isinstance(unit, str) and unit
        assert key == key.strip() and "//" not in key


def test_env_keys_are_unique_across_declared():
    # '/' and '-' both flatten to '_', so two different keys could in principle
    # collide on one env var name. Assert they do not.
    env_keys = [constants._env_key(k) for k in constants.DECLARED]
    assert len(set(env_keys)) == len(env_keys)


def test_every_lane_in_lane_order_has_a_declared_budget():
    from livewire_scripts.run_daily_update_job import LANE_ORDER

    for lane in LANE_ORDER:
        assert f"lane_budget_s/{lane}" in constants.DECLARED
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_constants.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'clients.constants'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/constants.py
"""The one place every operating constant in this repo lives.

Each key is scope-bound (spec docs/superpowers/specs/2026-09-02-livewire-ledger-design.md
section 4): the 5 req/min FX limit is FX-only, the lane budgets are one value
per lane. The scope is the part of the key after '/'; a key with no '/' is
genuinely global. A scope-bound number declared without its scope is a bug in
this file, not an exception to the rule.

`run_daily_update_job.main()` emits this whole dict as
`measurements(source='declared')` once per run, splitting each key into
(name, scope) at emit time. `status.py` compares those rows against the
14-day p95 of `source='measured'` rows with the same (name, scope) and WARNs
on a >2x drift.
"""

from __future__ import annotations

import os

# key -> (value, unit). Scope is the segment after '/', absent when global.
DECLARED: dict[str, tuple[float, str]] = {
    # Per-lane wall-clock budgets. One per lane in run_daily_update_job.LANE_ORDER,
    # plus the fallback used when a scope is not a known lane.
    "lane_budget_s/futures": (30 * 60, "s"),
    "lane_budget_s/cmdty": (30 * 60, "s"),
    "lane_budget_s/cboe": (30 * 60, "s"),
    "lane_budget_s/fx": (30 * 60, "s"),
    "lane_budget_s/corporate-actions": (3 * 60 * 60, "s"),
    "lane_budget_s/equity": (2 * 60 * 60, "s"),
    "lane_budget_s/silver": (2 * 60 * 60, "s"),
    "lane_budget_s/default": (30 * 60, "s"),
    # Share of attempted symbols that may fail before a run counts as systemic.
    "failure_rate_tolerance": (0.05, "ratio"),
    # Massive flat-file GET floor, rolling. Derived from the scan date, never
    # hardcoded as a date (pm:2026-07-29-massive-floor-derived-from-scan-date).
    "massive_window_days": (1827, "days"),
    # Massive REST FX plan: 5 succeed, the 6th 429s, no Retry-After. FX-scoped.
    "massive_requests_per_minute/fx": (5, "per_min"),
    # Minimum share of a raw flat file's ticker set a publish must cover.
    "flatfile_min_publish_ratio": (0.9, "ratio"),
    # Coverage ratio below which the surface and the digest complain.
    "coverage_alert_threshold": (0.95, "ratio"),
    # Free space a flat-file plan requires before it starts.
    "flatfile_min_free_gb": (25, "GB"),
}


def _env_key(key: str) -> str:
    """`lane_budget_s/corporate-actions` -> `LW_DECLARED_LANE_BUDGET_S_CORPORATE_ACTIONS`."""
    return "LW_DECLARED_" + key.upper().replace("/", "_").replace("-", "_")


def split_scope(key: str) -> tuple[str, str]:
    """`key` -> `(name, scope)`; scope is "" when the key carries none.

    Used only by the emitter, which writes the ledger's two columns.
    """
    name, _, scope = key.partition("/")
    return name, scope


def declared(key: str) -> float:
    """Return the declared value for `key`, env override applied.

    Raises KeyError if the key is not declared — a call site with a typo fails
    loudly at call time, not silently with a made-up default.
    """
    value, _unit = DECLARED[key]
    override = os.environ.get(_env_key(key))
    return float(override) if override is not None else float(value)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_constants.py -v`
Expected: PASS (9 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/constants.py tests/test_constants.py
git commit -m "feat(ledger): add clients/constants.py, the one place a number lives"
```

---

### Task 2: emit `declared` at run start, and `measured` lane elapsed at lane end

**Files:**

- Modify: `livewire_scripts/run_daily_update_job.py` — the `ledger.emit("runs", [run_row], run_id=run_id())` call in `main()` (grep it, it's the first one), and `_emit_lane()` at `:143-177`
- Modify: `livewire_scripts/sync_runner.py` — `run_phase()` at `:136`, its `lane_results` emit at `:207-231`
- Test: `tests/test_run_daily_update_job.py`, `tests/test_sync_runner.py`

**Interfaces:**

- Consumes: `clients.constants.DECLARED`, `clients.constants.split_scope` (Task 1).
- Produces: `measurements(name='lane_budget_s', scope=<lane>, source='measured')`
  rows — the only thing that makes Task 3's drift check gradeable.

- [ ] **Step 1: Write the failing test for the declared emit**

```python
# tests/test_run_daily_update_job.py — add to the existing file
def test_main_emits_every_declared_constant_as_a_measurement(monkeypatch, tmp_path, ...):
    # Use the existing main()-invoking fixture in this file (it already stubs
    # subprocess calls and points ledger.ledger_root() at tmp_path); after the
    # run, assert on the ledger contents:
    from clients import constants, ledger

    rows = ledger.query(
        "select name, scope, value, unit, source from measurements "
        "where source = 'declared' order by name, scope"
    )
    expected = {
        constants.split_scope(key): (float(value), unit)
        for key, (value, unit) in constants.DECLARED.items()
    }
    seen = {(r["name"], r["scope"]): (r["value"], r["unit"]) for r in rows}
    assert seen == expected
```

Wire this into whatever fixture the file already uses to invoke `main()` end
to end (the file has one — the L1 lane-budget tests exercise `main()` the same
way; follow that pattern exactly rather than inventing a second harness).

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_run_daily_update_job.py -k emits_every_declared -v`
Expected: FAIL — `seen` is empty (no `source='declared'` rows exist yet)

- [ ] **Step 3: Emit the declared rows**

Find the block in `main()` that does:

```python
    ledger.emit("runs", [run_row], run_id=run_id())
```

and add immediately after it:

```python
    ledger.emit(
        "measurements",
        [
            {
                "name": name,
                "scope": scope,
                "measured_at": _utc_now(),
                "value": float(value),
                "unit": unit,
                "source": "declared",
                "run_id": run_id(),
            }
            for key, (value, unit) in constants.DECLARED.items()
            for name, scope in [constants.split_scope(key)]
        ],
        run_id=run_id(),
    )
```

Add `from clients import constants` to the imports at the top of
`livewire_scripts/run_daily_update_job.py` alongside the existing
`from clients import ledger`. `_utc_now()` already exists in this module (it
backs `_emit_last_session`) — reuse it, don't redefine it.

- [ ] **Step 4: Write the failing test for the measured lane elapsed row**

**This is the gap that makes or breaks the plan.** Today nothing in the repo
writes `measurements(source='measured')` for lane elapsed time — grep confirms
the only `source='measured'` producers are `last_session`
(`run_daily_update_job.py:194`), `silver_failed` /
`silver_window_regressions` (`:1031`, `:1040`) and the coverage rows
(`coverage_report.py:511-535`). None of them is `lane_budget_s`. Without this
step Task 3's check reads UNKNOWN forever and the plan ships a dead detector
(`CLAUDE.md` rule 9).

```python
# tests/test_run_daily_update_job.py
def test_emit_lane_also_writes_a_measured_lane_budget_row(tmp_path, monkeypatch):
    # same tmp-ledger fixture pattern as tests/test_status.py:29-32
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    from clients import ledger
    from livewire_scripts import run_daily_update_job as job

    job._emit_lane("equity", started=..., ended=..., exit_code=0,
                   elapsed_s=1234.0, outcome="done")

    lanes = ledger.query("select lane, elapsed_s from lane_results")
    measured = ledger.query(
        "select name, scope, value, unit, source, run_id from measurements "
        "where source = 'measured'"
    )
    assert lanes[0]["lane"] == "equity"
    assert measured == [
        {"name": "lane_budget_s", "scope": "equity", "value": 1234.0,
         "unit": "s", "source": "measured", "run_id": lanes[0].get("run_id", measured[0]["run_id"])}
    ]
```

```python
# tests/test_sync_runner.py — the twin (CLAUDE.md rule 5)
def test_run_phase_also_writes_a_measured_lane_budget_row(tmp_path, monkeypatch):
    # run_phase() with a stub runner returning exit 0; assert the same pairing:
    # one lane_results row and one measurements(name='lane_budget_s',
    # scope=<label>, source='measured') row with the same run_id.
```

- [ ] **Step 5: Run both tests to verify they fail**

Run: `uv run pytest tests/test_run_daily_update_job.py tests/test_sync_runner.py -k measured_lane_budget -v`
Expected: FAIL — `measured` is empty in both

- [ ] **Step 6: Emit the measured row from both lane emitters**

In `livewire_scripts/run_daily_update_job.py::_emit_lane` (`:143-177`), inside
the same `try:` as the existing `lane_results` emit and immediately after it:

```python
        ledger.emit(
            "measurements",
            [
                {
                    "name": "lane_budget_s",
                    "scope": scope,
                    "measured_at": _utc_now(),
                    "value": float(elapsed_s),
                    "unit": "s",
                    "source": "measured",
                    "run_id": run_id(),
                }
            ],
            run_id=run_id(),
        )
```

The existing `except Exception` already covers it — a ledger failure must not
kill the lane, and the same rule applies to the measurement.

In `livewire_scripts/sync_runner.py::run_phase` (`:136`), after the
`_emit_ledger("lane_results", ...)` call at `:207-231`, add the twin:

```python
        _emit_ledger(
            "measurements",
            [
                {
                    "name": "lane_budget_s",
                    "scope": label,
                    "measured_at": datetime.now(UTC),
                    "value": float(time.monotonic() - clock),
                    "unit": "s",
                    "source": "measured",
                    "run_id": run,
                }
            ],
            run,
        )
```

Note both sites compute `elapsed_s` the same way they already do for
`lane_results` — reuse the value, do not re-read the clock, or the two rows
disagree by a few milliseconds and the drift check compares a different
number than the lane budget check does.

- [ ] **Step 7: The (name, scope) pairs the drift check compares**

Before writing Task 3, fix on paper what the check's input actually is. A
declared-vs-measured comparison only means something for a constant that has a
real-world counterpart to measure: an elapsed time does, a threshold or a ratio
does not — nothing "measures" a 0.9 publish floor or a 5 req/min plan limit.
So the check is restricted to `name = 'lane_budget_s'`, scope `default`
excluded, and every row below is a pair `status` compares.

| name            | scope               | measured producer                      |
| --------------- | ------------------- | -------------------------------------- |
| `lane_budget_s` | `futures`           | `_emit_lane` / `run_phase` (this task) |
| `lane_budget_s` | `cmdty`             | `_emit_lane` / `run_phase` (this task) |
| `lane_budget_s` | `cboe`              | `_emit_lane` / `run_phase` (this task) |
| `lane_budget_s` | `fx`                | `_emit_lane` / `run_phase` (this task) |
| `lane_budget_s` | `corporate-actions` | `_emit_lane` / `run_phase` (this task) |
| `lane_budget_s` | `equity`            | `_emit_lane` / `run_phase` (this task) |
| `lane_budget_s` | `silver`            | `_emit_lane` / `run_phase` (this task) |

`lane_budget_s/default` is a code fallback rather than any lane's operating
constant — no lane is named `default`, so it can never have a producer and is
excluded by name in Task 3's SQL. Every one of the seven pairs above has a
producer as of Task 2, so UNKNOWN on this check carries one specific meaning:
**a lane budget is declared but that lane produced no `elapsed_s` in 14 days**
— a lane that stopped running. That is real signal, not a gap in the plan.

The other declared keys (`failure_rate_tolerance`, `massive_window_days`,
`massive_requests_per_minute/fx`, `flatfile_min_publish_ratio`,
`coverage_alert_threshold`, `flatfile_min_free_gb`) are recorded as
`source='declared'` for audit only and are not drift-checked until one of them
gets a measured producer.

Note: `coverage_report.py` already emits `coverage_elapsed_s` (scope `all`) as
`measured`, but no key declares it, and the check reads only `lane_budget_s`
rows anyway. That is intended — a measured row with no declared
counterpart is not drift.

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/test_run_daily_update_job.py tests/test_sync_runner.py -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add livewire_scripts/run_daily_update_job.py livewire_scripts/sync_runner.py \
        tests/test_run_daily_update_job.py tests/test_sync_runner.py
git commit -m "feat(ledger): emit declared constants at run start and measured lane elapsed at lane end"
```

---

### Task 3: `status.py` — declared-vs-measured drift check

**Files:**

- Modify: `livewire_scripts/status.py` (append to the `CHECKS` list, after the
  existing `"IB-only lanes behind"` tuple — it's the last entry before the
  closing `]`)
- Modify: `livewire_scripts/status.py:229` (the `"Lanes within budget"` fix-hint text)
- Test: `tests/test_status.py`

**Interfaces:**

- Consumes: `measurements` rows with `source in ('declared', 'measured')`
  written by Task 2 (both sides).
- Produces: one new row in the `status.collect()` output, `"Declared constants match reality"`.

- [ ] **Step 1: Write the failing test**

`tests/test_status.py` imports no check SQL by name today — it imports
`status` and a few named helpers (`:12-14`) and drives everything through
`status.collect()`. Keep that: **the new check's SQL is inlined in the
`CHECKS` tuple, not extracted to a module constant**, and the test exercises
it through `status.collect()` on a tmp ledger seeded with declared + measured
rows. The tmp-ledger fixture already exists at `tests/test_status.py:29-32`
(`monkeypatch.setenv("LW_LEDGER_ROOT", ...)`, autouse) — use it.

Three cases, one per verdict:

```python
def _seed(name, scope, declared_value, measured_values):
    rows = [
        {"name": name, "scope": scope, "measured_at": NOW, "value": declared_value,
         "unit": "s", "source": "declared", "run_id": RUN},
    ] + [
        {"name": name, "scope": scope, "measured_at": NOW, "value": v,
         "unit": "s", "source": "measured", "run_id": f"{RUN}-{i}"}
        for i, v in enumerate(measured_values)
    ]
    ledger.emit("measurements", rows, run_id=RUN)


def _verdict(rows, label="Declared constants match reality"):
    return next(r for r in rows if r.name == label).verdict


def test_declared_vs_measured_warns_on_a_2x_drift():
    _seed("lane_budget_s", "corporate-actions", 10800.0, [25000.0] * 5)
    assert _verdict(status.collect()) is status.Verdict.WARN


def test_declared_vs_measured_is_ok_within_2x():
    _seed("lane_budget_s", "corporate-actions", 10800.0, [9000.0] * 5)
    assert _verdict(status.collect()) is status.Verdict.OK


def test_declared_vs_measured_is_unknown_when_a_lane_stopped_running():
    # a declared lane budget with no elapsed_s in the window = that lane did not run
    _seed("lane_budget_s", "cmdty", 1800.0, [])
    assert _verdict(status.collect()) is status.Verdict.UNKNOWN
```

All three cases seed `lane_budget_s` rows, because `lane_budget_s` is the only
name the check reads (Task 2 Step 7). A threshold key such as
`failure_rate_tolerance` seeded into the same ledger must not change any of the
three verdicts — worth one extra assertion if the harness makes it cheap.

(`NOW` and `RUN` already exist at the top of the file; `Verdict` is the
`IntEnum` from `status.py` — `UNKNOWN` is not `OK`, per
pm:2026-08-16-status-surface-grading.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_status.py -k declared_vs_measured -v`
Expected: FAIL — `StopIteration`, no check by that name exists

- [ ] **Step 3: Write minimal implementation**

Append to `CHECKS` in `livewire_scripts/status.py`, right after the
`"IB-only lanes behind"` tuple:

```python
    (
        "Declared constants match reality",
        "select case when _n = 0 then 'UNKNOWN' "
        "when declared_value > 2 * measured_p95 or measured_p95 > 2 * declared_value "
        "then 'WARN' else 'OK' end as verdict, "
        "name, scope, declared_value, measured_p95 from ("
        "  select name, scope, "
        "    max(value) filter (where source = 'declared') as declared_value, "
        "    quantile_cont(value, 0.95) filter (where source = 'measured') as measured_p95, "
        "    count(*) filter (where source = 'measured') as _n "
        "  from measurements "
        "  where measured_at >= today() - interval 14 day "
        "    and name = 'lane_budget_s' and scope <> 'default' "
        "  group by name, scope"
        ") "
        "where declared_value is not null "
        "order by case when _n = 0 then 1 "
        "  when declared_value > 2 * measured_p95 or measured_p95 > 2 * declared_value "
        "  then 0 else 2 end, name, scope limit 1",
    ),
```

One row, worst-drift-first (WARN before UNKNOWN before OK) — same pattern the
existing `DuckDB catalog` check uses to report the single worst offender
rather than flooding the surface with one line per constant. The 14-day window
is `measured_at >= today() - interval 14 day`; `quantile_cont(value, 0.95)` is
DuckDB's exact continuous quantile, which is what "14-day p95" in the spec
means.

- [ ] **Step 4: Point the "Lanes within budget" fix-hint at the new home**

`status.py:229` currently reads:

```python
        "raise the lane's LANE_BUDGET_S only after measuring it cold; see run_daily_update_job.LANE_BUDGET_S"
```

After Task 4 that dict is derived, not authored. Change the pointer to
`clients/constants.py`:

```python
        "raise the lane's budget only after measuring it cold; see clients/constants.py (lane_budget_s/<lane>)"
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_status.py -v`
Expected: PASS (the three new cases plus every existing status test)

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/status.py tests/test_status.py
git commit -m "feat(status): flag when a declared constant drifts 2x from measured reality"
```

---

### Task 4: migrate the scattered constants to `constants.declared()`

**Files:**

- Modify: `livewire_scripts/sync_corporate_actions.py:27` (`FAILURE_RATE_TOLERANCE`; usage at `:370` unchanged)
- Modify: `livewire_scripts/ingest_flatfiles.py:159` (`MDW_FLATFILE_MIN_PUBLISH_RATIO`)
- Modify: `livewire_scripts/run_daily_update_job.py:48-57` (the `LANE_BUDGET_S` dict and `DEFAULT_LANE_BUDGET_S`)
- Modify: `livewire_scripts/fetch_fx.py:57-58` (`MASSIVE_REQUESTS_PER_MINUTE`)
- Modify: `clients/gap_engine.py:21` (`MASSIVE_WINDOW_DAYS`)
- Modify: `livewire_scripts/coverage_report.py:71` (`MDW_COVERAGE_ALERT_THRESHOLD`)
- Modify: `livewire_scripts/flatfile_planner.py:67` (`MDW_FLATFILE_MIN_FREE_GB`)
- Modify: `livewire_scripts/status.py:27,32` (`_MIN_FREE_GB`, `_COVERAGE_THRESHOLD`)
- Test: existing tests for each file, plus one new test pinning the override

**Interfaces:**

- Consumes: `clients.constants.declared` (Task 1).
- Produces: nothing new — every one of these becomes a thin read, callers
  outside this task are unaffected because the values are unchanged.

- [ ] **Step 1: Update `sync_corporate_actions.py`**

Replace:

```python
FAILURE_RATE_TOLERANCE = 0.05
```

with:

```python
from clients import constants

FAILURE_RATE_TOLERANCE = constants.declared("failure_rate_tolerance")
```

Line `370`'s usage (`failed > FAILURE_RATE_TOLERANCE * attempted`) is
unchanged — it reads the module-level name either way, so no other line in
this file moves.

- [ ] **Step 2: Update `ingest_flatfiles.py:159`**

Replace:

```python
    ratio_floor = min_ratio if min_ratio is not None else float(os.getenv("MDW_FLATFILE_MIN_PUBLISH_RATIO", "0.9"))
```

with:

```python
from clients import constants

    ratio_floor = min_ratio if min_ratio is not None else constants.declared("flatfile_min_publish_ratio")
```

(the import goes at the top of the file, not inline — shown together only to
keep the diff readable.)

No test pins this env var today — `grep -rn MDW_FLATFILE_MIN_PUBLISH_RATIO
tests/` is empty, which is exactly why
pm:2026-07-22-flatfile-min-publish-ratio says "guard only, **no test**". Add
one now, so the override has a test the moment it has a new name:

```python
# tests/test_ingest_flatfiles.py
def test_lw_declared_flatfile_min_publish_ratio_overrides_the_floor(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_FLATFILE_MIN_PUBLISH_RATIO", "0.5")
    # a publish covering 60% of the raw ticker set passes at 0.5 and fails at 0.9
```

- [ ] **Step 3: Update `run_daily_update_job.py`'s `LANE_BUDGET_S`**

Replace the hard-coded dict:

```python
LANE_BUDGET_S: dict[str, float] = {
    "futures": 30 * 60,
    ...
}
DEFAULT_LANE_BUDGET_S = 30 * 60
```

with:

```python
LANE_BUDGET_S: dict[str, float] = {
    lane: constants.declared(f"lane_budget_s/{lane}") for lane in LANE_ORDER
}
DEFAULT_LANE_BUDGET_S = constants.declared("lane_budget_s/default")
```

`LANE_ORDER` is declared one line above this block already — no reordering
needed. Every existing `LANE_BUDGET_S.get(scope, DEFAULT_LANE_BUDGET_S)` call
site (`:165`, `:551`, `:779`) is unchanged; they read the dict, not the
literal. Task 1's `test_every_lane_in_lane_order_has_a_declared_budget` is
what stops a new lane from silently falling through to the default.

- [ ] **Step 4: Update `fetch_fx.py` and `gap_engine.py`**

`fetch_fx.py:57-58`:

```python
MASSIVE_REQUESTS_PER_MINUTE = constants.declared("massive_requests_per_minute/fx")
MASSIVE_MIN_INTERVAL_SECONDS = 60.0 / MASSIVE_REQUESTS_PER_MINUTE
```

(add `from clients import constants` to the imports; keep the comment block
above — it is the measurement history for the value, not dead prose.)

`gap_engine.py:21`:

```python
MASSIVE_WINDOW_DAYS = int(constants.declared("massive_window_days"))
```

Leave the comment above it (`# 2021-07-27 -> 403, ...`) in place, same reason.

- [ ] **Step 5: Update the two remaining `MDW_` reads and `status.py`'s import-time pair**

`coverage_report.py:71`:

```python
DEFAULT_THRESHOLD = constants.declared("coverage_alert_threshold")
```

`flatfile_planner.py:67`:

```python
    minimum_free_bytes = int(constants.declared("flatfile_min_free_gb") * 1024**3)
```

`status.py:27,32` are **import-time** `os.getenv` reads:

```python
_MIN_FREE_GB = float(os.getenv("MDW_FLATFILE_MIN_FREE_GB", "25"))
_COVERAGE_THRESHOLD = float(os.getenv("MDW_COVERAGE_ALERT_THRESHOLD", "0.95"))
```

Migrating them must make them **call-time** reads — move each into the
function that uses it (`constants.declared("flatfile_min_free_gb")` /
`constants.declared("coverage_alert_threshold")`) and delete the module-level
name. An import-time `declared()` is read once per process, so
`monkeypatch.setenv` in a test that has already imported `status` has no
effect, and an operator's one-run override is ignored — the exact silent-noop
class this plan exists to remove. `tests/test_nightly_digest.py:45` currently
sets `MDW_FLATFILE_MIN_FREE_GB`; switch it to
`LW_DECLARED_FLATFILE_MIN_FREE_GB` and confirm it still bites (it only can if
the read is call-time).

- [ ] **Step 6: Run the full affected-file test suites**

Run: `uv run pytest tests/test_sync_corporate_actions.py tests/test_ingest_flatfiles.py tests/test_run_daily_update_job.py tests/test_fetch_fx.py tests/test_gap_engine.py tests/test_coverage_report.py tests/test_flatfile_planner.py tests/test_status.py tests/test_nightly_digest.py -v`
Expected: PASS — fix any test still asserting an old `MDW_` name by switching
it to `LW_DECLARED_<KEY>` per Task 1's `_env_key`.

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/sync_corporate_actions.py livewire_scripts/ingest_flatfiles.py \
        livewire_scripts/run_daily_update_job.py livewire_scripts/fetch_fx.py clients/gap_engine.py \
        livewire_scripts/coverage_report.py livewire_scripts/flatfile_planner.py livewire_scripts/status.py \
        tests/
git commit -m "refactor(constants): scattered constants now read clients.constants.declared"
```

---

### Task 5: delete the duplicated preset tuple, the dead env-var mentions, and the runbook rows

**Files:**

- Modify: `livewire_scripts/sync_runner.py:36` (delete `EQUITY_PRESETS`, import from `backfill_runner.py:34` instead)
- Modify: `docs/runbook.md` (delete the per-var rows, add one `LW_DECLARED_` row)
- Modify: `CLAUDE.md` (one line under "The one contract")

**Interfaces:**

- Consumes: `backfill_runner.EQUITY_PRESETS` (already public, identical tuple).
- Produces: nothing — this is cleanup with no new surface.

- [ ] **Step 1: Delete the duplicated preset tuple**

In `livewire_scripts/sync_runner.py:36`, replace:

```python
EQUITY_PRESETS = ("presets/sp500.json", "presets/ndx100.json", "presets/r2k.json")
```

with an import from the canonical copy at `backfill_runner.py:34`:

```python
from livewire_scripts.backfill_runner import EQUITY_PRESETS
```

`sync_runner.py:73` keeps working (same tuple, same name). Note that
`livewire_scripts/ingest_daily_flatfiles.py:27` imports `EQUITY_PRESETS`
**from `sync_runner`**, so the re-export must survive — an `import` statement
keeps the name bound at module level, a `del` or a rename would not. No test
references `EQUITY_PRESETS` (`grep -rn EQUITY_PRESETS tests/` is empty), so
there is nothing to update on the test side; the only guard is that the two
importing modules still resolve.

- [ ] **Step 2: Run the affected suites**

Run: `uv run pytest tests/test_sync_runner.py tests/test_ingest_daily_flatfiles.py -v`
Expected: PASS unchanged — the tuple's _value_ did not change, only where it's defined

- [ ] **Step 3: Confirm `MDW_DAILY_JOB_DEADLINE_SECONDS` is already dead in Python**

L1 deleted every Python reader of `MDW_DAILY_JOB_DEADLINE_SECONDS`; only docs
still mention it. Verify, then clean the docs:

```bash
grep -rn MDW_DAILY_JOB_DEADLINE_SECONDS --include='*.py' --exclude-dir=.worktrees .
```

Expected: empty. (Hits under `.worktrees/` are a stale worktree, not the main
tree — that is why `--exclude-dir=.worktrees` is on the command.) If it is
empty, delete any remaining mention in `docs/runbook.md`; leave the
post-mortems alone, they are the historical record.

Do **not** invent a `constants.declared()` entry for it. In particular
`clients/coverage_denominator.py` does **not** read it — that file has a
literal `DELIVERY_ALLOWANCE_SECONDS = 9 * 60 * 60` at `:22` with no
`os.getenv` anywhere near it. Any earlier claim that
`coverage_denominator.py:39` reads this env var is false and must not be
re-introduced into the plan.

- [ ] **Step 4: Update `docs/runbook.md`**

Delete the per-var rows for the constants this plan migrated:

```
| `MDW_FLATFILE_MIN_PUBLISH_RATIO`  | `0.9`        | Minimum share of the raw file's ticker set a publish must cover before the run fails. Skipped on a resumed run |
| `MDW_COVERAGE_ALERT_THRESHOLD`    | `0.95`       | ... |
| `MDW_FLATFILE_MIN_FREE_GB`        | `25`         | ... |
```

and add, in their place, one row:

```
| `LW_DECLARED_<KEY>` | see `clients/constants.py` | Overrides any declared constant for one run. `<KEY>` is the `DECLARED` key upper-cased with `/` and `-` as `_` — e.g. `LW_DECLARED_FAILURE_RATE_TOLERANCE=0.10`, `LW_DECLARED_LANE_BUDGET_S_CORPORATE_ACTIONS=7200` |
```

- [ ] **Step 5: Add the one CLAUDE.md line**

One line under "The one contract", not a paragraph (`CLAUDE.md` rule 10):

```
- Every operating constant is one `DECLARED` key in `clients/constants.py`, emitted as `measurements(source='declared')` each run and compared against the 14-day p95 of `source='measured'`; a >2x drift is a `status` WARN. Override for one run with `LW_DECLARED_<KEY>`. → test: `tests/test_constants.py`, `tests/test_status.py`
```

- [ ] **Step 6: Full suite**

Run: `uv run pytest tests/ -q --cov=clients --cov=scripts --cov-fail-under=95`
Expected: PASS, coverage gate holds

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/sync_runner.py docs/runbook.md CLAUDE.md
git commit -m "chore(constants): drop the duplicated preset tuple and the superseded env-var doc rows"
```

---

## Acceptance criteria

All four must hold before this is called done. Do not lower one that cannot be
met — report "not met" and keep it (`CLAUDE.md` rule 4).

- [ ] **(a) Tests green with the gate.** `uv run pytest tests/ -q --cov=clients --cov=scripts --cov-fail-under=95` passes. `npm run test:alerts` is untouched by this plan but must still pass.
- [ ] **(b) No superseded env var left in Python.**

```bash
grep -rn 'MDW_FLATFILE_MIN_PUBLISH_RATIO\|MDW_DAILY_JOB_DEADLINE_SECONDS\|MDW_COVERAGE_ALERT_THRESHOLD\|MDW_FLATFILE_MIN_FREE_GB' \
  --include='*.py' --exclude-dir=.worktrees .
```

returns nothing outside `clients/constants.py` (which mentions none of them by
name either — so in practice: nothing at all).

- [ ] **(c) The new check renders in all three verdicts.** `uv run python scripts/livewire_ops.py status` against a ledger seeded with three `lane_budget_s` pairs — (1) a >2x drift lane, (2) an in-band lane, (3) a declared lane with no measured rows in the window — shows `Declared constants match reality` as WARN, OK and UNKNOWN respectively. Seed via `LW_LEDGER_ROOT` pointing at a tmp dir — never against the mini's ledger.
- [ ] **(d) Docs carry the change.** `CLAUDE.md` gains exactly **one** line under "The one contract" naming the test file(s); `docs/runbook.md` gains the `LW_DECLARED_<KEY>` override row and has lost the old per-var rows.

---

## Self-Review

**Spec coverage** (against §4):

- `DECLARED` dict, one place a number lives — Task 1 ✓
- orchestrator emits the whole dict as `measurements(source='declared')` at run start — Task 2 ✓
- `source='measured'` lane elapsed rows — **added by Task 2** ✗→✓. L1 emits `lane_results.elapsed_s` but no `measurements` row for it, so without Task 2 Steps 4-6 the drift check would have read UNKNOWN forever. This was the plan's one real gap.
- "one check: declared vs 14-day p95 of measured, >2x → WARN, printing both numbers" — Task 3 ✓
- "env override collapses to one prefix `LW_DECLARED_<name>`" — Task 1's `_env_key` ✓, exactly one form, no `__<SCOPE>` variant: the scope is already inside the key, so `lane_budget_s/corporate-actions` flattens to `LW_DECLARED_LANE_BUDGET_S_CORPORATE_ACTIONS` under the same single rule that gives `failure_rate_tolerance` → `LW_DECLARED_FAILURE_RATE_TOLERANCE`. A test asserts no two keys collide on one env name.
- "deleted: the scattered constants and their per-module os.getenv reads... and the corresponding env-var paragraphs in docs/runbook.md" — Tasks 4 & 5 ✓
- `sync_runner.py`'s duplicated preset constants, called out in the L1 plan's footnote as "L2 scope" — Task 5 ✓

**Shape:** `DECLARED` is flat, `key -> (value, unit)`, everywhere in this plan.
`declared(key) -> float` takes one positional argument and no `scope=` keyword.
The `(name, scope)` split exists only in `split_scope()` and only at emit time.
There is no nested `{scope: (value, unit)}` form left in this document.

**Verified against the working tree (2026-09-04), not assumed:**

- `MDW_DAILY_JOB_DEADLINE_SECONDS` has **no** Python reader in the main tree; the only hits are under `.worktrees/` and in `docs/`. It is not "one live reader in `coverage_denominator.py`" — that file's `DELIVERY_ALLOWANCE_SECONDS` at `:22` is a literal with no `os.getenv`.
- `MASSIVE_REQUESTS_PER_MINUTE` is at `fetch_fx.py:57`, not `:53`.
- No test sets `MDW_FLATFILE_MIN_PUBLISH_RATIO`; no test references `EQUITY_PRESETS`. Task 4 Step 2 adds the first test for the ratio override.
- `MDW_FLATFILE_MIN_FREE_GB` has two readers (`status.py:27`, `flatfile_planner.py:67`) and one test (`tests/test_nightly_digest.py:45`); `MDW_COVERAGE_ALERT_THRESHOLD` has two (`status.py:32`, `coverage_report.py:71`). All four are in Task 4's scope, which criterion (b) requires.

**Check input:** the drift check reads `lane_budget_s` only (scope `default`
excluded) — all seven of those pairs have a producer as of Task 2, so UNKNOWN
means "this lane produced no elapsed_s in 14 days", a real signal. The other
declared keys are thresholds and ratios with no measurable counterpart; they
are recorded as `source='declared'` for audit only and are not drift-checked
until one of them gets a measured producer.

**Deliberately out of scope (flag, don't silently fix):** the ~30 other tuning
env vars listed under **Scope** at the top; `coverage_denominator.py`'s
`DELIVERY_ALLOWANCE_SECONDS` literal, which is denominator timing rather than
an operating constant and gets a `DECLARED` key from whoever next has a reason
to touch that file.
