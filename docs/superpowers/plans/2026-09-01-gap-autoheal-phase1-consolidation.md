# Gap Auto-Heal Phase 1 Consolidation — Implementation Plan

> **For agentic workers:** execute this plan with the repository owner's
> `/execute-plan` skill, task by task, in order. Steps use checkbox (`- [ ]`)
> syntax for tracking. Do **not** use `superpowers:subagent-driven-development`
> or any parallel-dispatch pattern — the repository owner's global `CLAUDE.md`
> forbids it.

**Goal:** Make "a bar we should have is not on disk" answerable by exactly one
detector, whose denominator is registry-backed rather than purely disk-derived,
and which can tell a no-trade day apart from an instrument that left the tape.

**Architecture:** `clients/coverage_denominator.py` already builds
`presets × trading_calendar × timeframe`. This plan gives it an ingestion-deadline
rule, adds a pure terminus test over the raw traded sets coverage already reads,
teaches `clients/gap_engine.py` the terminus class, then makes
`livewire_scripts/coverage_report.py` **run the windowed classifier itself** —
denominator → on-disk diff → `classify` → Tier A manifest + Tier B decision
queue — and only then deletes `livewire_scripts/gap_scan.py`, its `launchd`
template and its subcommand.

**Absorption comes before deletion, and that ordering is the correction this
revision makes.** Coverage as it stands is a *one-session freshness* job: its
presence test is `latest >= target_date` and it never enumerates *which*
sessions are absent. `gap_scan` is a *30-day windowed classifier* producing
`Finding` objects. They are not the same detector. Deleting the second without
building its replacement would leave `classify`, `Finding`, `heal_by_days`, the
tier and the decision queue — spec §10 deliverables 5 and 8 — with **no
production caller at all**, and §10 deliverable 8's queue depth is the stated
measurement for whether Phase 2 is worth building. Net change to the script and
job count is still negative. No new scheduled job: `com.livewire.coverage` at
11:00 UTC already runs after the ingestion deadline.

**Tech Stack:** Python 3.13, `uv` exclusively (`uv run pytest`), `pyarrow.parquet`
for footer and column reads, stdlib `datetime`/`json`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md`
(revised in `7aac492`, `902ac8f`, `76d1bc2`, and again alongside this plan
revision).

**Revision note (2026-09-01, post cross-model review):** this plan was rewritten
after a Codex + Cursor/Grok tribunal found six CRITICAL defects in the first
draft. The three structural ones: deleting `gap_scan` left `classify` with no
production caller (now Task 7); the terminus carve-out was a no-op because the
no-trade exemption still counted those symbols present (now Task 6 step 3); and
`as_of=session_due_at(target_date)` made the deadline rule tautological on the
one path production runs (now Task 1's warning and Task 6's `as_of` parameter).
Two claims in the first draft were asserted without verification and are
corrected in place: the test fixtures do **not** hold real frozen prices, and
**nothing** parses the `non-equity 1d:` log line.
**Measurement this plan is calibrated against:**
`docs/audits/2026-09-01-terminus-vs-no-trade.md`.

## Global Constraints

Copied verbatim from the repository's `CLAUDE.md` and the spec. Every task's
requirements implicitly include this section.

- **`uv` exclusively** — never bare `python`, `pip`, or an activated venv. Tests
  are `uv run pytest tests/ -v`, which is what CI runs.
- **Coverage gate is 95%** (`fail_under = 95` in `pyproject.toml`,
  `--cov-fail-under=95` in CI). All new code in `clients/` and `scripts/` must
  have tests.
- **CI runs `ruff check` AND `ruff format --check` as separate jobs.** Passing
  one does not imply the other. Run `uv run ruff format .` before every commit;
  two PRs in this repo have already failed on exactly this.
- **No synthetic market data.** Real tickers only; no invented prices presented
  as observed. ⚠️ The existing `tests/test_coverage_report.py::_write_daily`
  (`:74`) writes structural placeholders — `open 1.0, high 2.0, low 0.5,
  close 1.5, volume 1000` — **not** frozen real prices. An earlier revision of
  this plan asserted the opposite; that claim was wrong and is corrected here.
  Reuse the helper as-is (this plan asserts on *dates and membership*, never on
  a price, so its values are inert), and do not add a new fixture that asserts
  on a price. A `_symbols.parquet` fixture is a list of real tickers and is fine.
- **Nothing writes the data lake except through `shepherd_repair`.** Every
  function in this plan is read-only against the lake except the manifest/queue
  writers, which write under `<data-lake>/repairs/`, never under `bronze/`.
- **Never `--no-verify` on a commit.** A secrets pre-commit hook runs.
- **Do not push or open a PR unless explicitly asked.** Commit locally.
- **Never add a `Co-Authored-By` or any AI attribution trailer.**
- Work happens in the existing worktree
  `.worktrees/gap-autoheal-phase1` on branch `feat/gap-autoheal-phase1`. Do not
  create a new branch or worktree.
- **The deadline this plan depends on is not enforced for 3 of 7 lanes**
  (issue #94). That is a separate change; do not fix it here, and do not let a
  task in this plan depend on it being fixed.

## File Structure

| File | Responsibility | Action |
| --- | --- | --- |
| `clients/coverage_denominator.py` | expected series; now knows when a session is *due*, not merely closed | modify |
| `clients/terminus.py` | pure suffix test over per-session traded sets; the raw-partition reader | **create** |
| `clients/gap_engine.py` | classify a diff into G1/G3/G14; assign tier honestly | modify |
| `clients/gap_registry.py` | valid gap ids | modify |
| `registry/gaps.json` | six rows; G14 on the equity row only | modify |
| `livewire_scripts/coverage_report.py` | **the one detector**; registry denominator, terminus carve-out, the windowed classifier, Tier A manifest, Tier B queue | modify |
| `livewire_scripts/gap_scan.py` | — | **delete** (Task 8, after Task 7 absorbs it) |
| `launchd/com.livewire.gap-scan.plist.example` | — | **delete** |
| `launchd/com.livewire.universe-refresh.plist.example` | a **producer** (`universe_sync`), spec §10 deliverable 4 | **keep — not touched** |
| `scripts/livewire_quality.py` | subcommand table | modify (remove one line) |
| `tests/test_coverage_denominator.py` | due rule | modify |
| `tests/test_terminus.py` | suffix test | **create** |
| `tests/test_gap_engine.py` | G14, tier honesty, no G2/G13 | modify |
| `tests/test_coverage_report.py` | registry universe, terminus exclusion, log surface | modify |
| `tests/test_coverage_orchestration.py` | end-to-end: denominator → classify → both artifacts | **create** |
| `tests/test_gap_scan.py`, `tests/test_gap_scan_integration.py` | — | **delete**, replaced by the above |

---

### Task 1: A session is expected when it is *due on disk*, not when it closes

`build_denominator` filters `d < as_of` with `as_of` a `date`. A run at 04:21 UTC
on 2026-09-01 therefore expected session 2026-08-31 and produced **497 phantom
tail gaps out of 501 findings** — one per sp500 member — because the job that
fills that session starts at 06:00 UTC on 2026-09-01. Spec §5.

**Files:**
- Modify: `clients/coverage_denominator.py`
- Test: `tests/test_coverage_denominator.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `session_due_at(session: date, lag_days: int = 1) -> datetime` (tz-aware UTC).
    `lag_days` exists for one measured reason: spec §8.1's rates row is **T+2**,
    because FRED publishes a day behind. A uniform T+1 would manufacture one
    phantom rates gap every single day.
  - `build_denominator(preset_paths: list[Path], asset_class: str, timeframe: str, start: date, end: date, as_of: datetime, lag_days: int = 1) -> list[ExpectedSeries]` — **`as_of` changes from `date` to `datetime`**; every caller must be updated (Tasks 5–7 and the tests here).

⚠️ **The due rule must be tested through its real caller, not only through the
helper.** Passing `as_of=session_due_at(target_date)` into `build_denominator`
makes the filter `session_due_at(d) <= as_of` **tautologically true** for
`start == end == target_date` — the mechanism is inert on exactly the path
production runs. So `compute_coverage` (Tasks 5–7) takes an `as_of: datetime |
None = None` that defaults to `datetime.now(UTC)` **in `main()`**, and the
regression test in Task 9 exercises `compute_coverage` at 04:21 UTC and at
11:00 UTC — not `build_denominator` in isolation.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_coverage_denominator.py`:

```python
from datetime import UTC, date, datetime

from clients.coverage_denominator import build_denominator, session_due_at


def test_session_is_due_at_the_daily_job_deadline_the_following_day():
    # run-daily-job starts 06:00 UTC on S+1 and MDW_DAILY_JOB_DEADLINE_SECONDS
    # (4h) puts its deadline at 10:00 UTC.
    assert session_due_at(date(2026, 8, 31)) == datetime(2026, 9, 1, 10, 0, tzinfo=UTC)


def test_session_due_at_honours_the_existing_deadline_env_var(monkeypatch):
    monkeypatch.setenv("MDW_DAILY_JOB_DEADLINE_SECONDS", "7200")
    assert session_due_at(date(2026, 8, 31)) == datetime(2026, 9, 1, 8, 0, tzinfo=UTC)


def test_a_closed_but_not_yet_due_session_is_not_expected(tmp_path):
    # The 2026-09-01 04:21 UTC production run: session 2026-08-31 had closed and
    # its job had not started. 497 of 501 findings were this.
    preset = tmp_path / "p.json"
    preset.write_text('{"name": "p", "tickers": ["AAPL"]}')
    series = build_denominator(
        [preset], "equity", "1d",
        date(2026, 8, 27), date(2026, 8, 31),
        as_of=datetime(2026, 9, 1, 4, 21, tzinfo=UTC),
    )
    assert date(2026, 8, 31) not in series[0].sessions
    assert date(2026, 8, 28) in series[0].sessions


def test_the_same_session_is_expected_once_the_deadline_passes(tmp_path):
    preset = tmp_path / "p.json"
    preset.write_text('{"name": "p", "tickers": ["AAPL"]}')
    series = build_denominator(
        [preset], "equity", "1d",
        date(2026, 8, 27), date(2026, 8, 31),
        as_of=datetime(2026, 9, 1, 10, 0, tzinfo=UTC),
    )
    assert date(2026, 8, 31) in series[0].sessions


def test_a_naive_as_of_is_rejected_rather_than_assumed_utc(tmp_path):
    preset = tmp_path / "p.json"
    preset.write_text('{"name": "p", "tickers": ["AAPL"]}')
    with pytest.raises(ValueError, match="tz-aware"):
        build_denominator(
            [preset], "equity", "1d",
            date(2026, 8, 27), date(2026, 8, 31),
            as_of=datetime(2026, 9, 1, 10, 0),
        )
```

Add `import pytest` at the top of the file if it is not already imported.

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_coverage_denominator.py -v`
Expected: FAIL with `ImportError: cannot import name 'session_due_at'`.

- [ ] **Step 3: Implement**

In `clients/coverage_denominator.py`, replace the imports and the `as_of`
filter:

```python
import os
from datetime import UTC, date, datetime, time, timedelta

# run-daily-job's StartCalendarInterval, in UTC. The lane that fills session S
# starts here on S+1.
JOB_START_UTC = time(6, 0)
# The default of MDW_DAILY_JOB_DEADLINE_SECONDS (4h). Read at call time, not at
# import, so a test and a scheduled run can disagree.
DEFAULT_JOB_DEADLINE_SECONDS = 14400


def session_due_at(session: date, lag_days: int = 1) -> datetime:
    """The instant session *session* is due on disk.

    ponytail: reuses MDW_DAILY_JOB_DEADLINE_SECONDS rather than introducing a
    second constant to keep in step. "Closed" is not "delivered" -- the job that
    fills S starts 06:00 UTC on S+1, and a denominator that expects S the moment
    it closes manufactures one tail gap per symbol in the universe.

    lag_days is the number of days after the session that the filling job
    starts. It is 1 for every lane run by run-daily-job, and 2 for rates: FRED
    publishes a session behind, which spec section 8.1 already records as
    T+2. A uniform T+1 there manufactures one phantom gap per series per day.
    """
    seconds = int(os.environ.get("MDW_DAILY_JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS))
    start = datetime.combine(session + timedelta(days=lag_days), JOB_START_UTC, tzinfo=UTC)
    return start + timedelta(seconds=seconds)


# Spec section 8.1. Anything not listed is T+1.
DUE_LAG_DAYS = {"rates": 2}
```

Change the signature `as_of: date` to `as_of: datetime`, add `lag_days: int = 1`,
and replace the filter:

```python
    if as_of.tzinfo is None:
        raise ValueError("as_of must be tz-aware; a naive local datetime silently shifts the due rule")
    # A session is expected only once the job that fills it was due to finish.
    # Guarding on `d < as_of.date()` instead would reintroduce the 497 phantoms:
    # closing is not delivery.
    sessions = tuple(
        d for d in trading_dates_in_range(start, end) if session_due_at(d, lag_days) <= as_of
    )
```

Add one more test to Step 1, because the rates lag is the case a uniform rule
gets wrong every day:

```python
def test_rates_is_due_a_day_later_than_equity():
    # Spec section 8.1: FRED publishes a session behind, so the rates row is T+2.
    # A uniform T+1 expects DGS10 for session S at 10:00 UTC on S+1, when the
    # series legitimately does not exist yet -- one phantom gap per series, daily.
    session = date(2026, 8, 28)
    assert session_due_at(session, lag_days=2) - session_due_at(session) == timedelta(days=1)
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_coverage_denominator.py -v`
Expected: PASS. Pre-existing tests in that file that pass a `date` as `as_of`
will now fail — update each to a tz-aware `datetime` at `10:00` UTC on the day
after the last session they expect, which preserves their intent.

- [ ] **Step 5: Format, lint, commit**

```bash
uv run ruff format . && uv run ruff check .
git add clients/coverage_denominator.py tests/test_coverage_denominator.py
git commit -m "fix(gap): a session is expected when it is due on disk, not when it closes"
```

---

### Task 2: Terminus detection — a trailing run of absences is not a no-trade day

`coverage_report.py:322` exempts a symbol absent from the day's raw traded set.
The rule is load-bearing (without it the interior scan flags 96.6% of the
universe) and it is also what hides EA, AVB and EQR — three S&P 500 members that
stopped printing on 2026-08-04, 2026-08-14 and 2026-08-17 and never returned.
The separation is a suffix test, not a threshold. Spec §3 (G14), §4.4.

**Files:**
- Create: `clients/terminus.py`
- Test: `tests/test_terminus.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `MIN_TERMINUS_SESSIONS: int` (default `5`)
  - `terminus_of(traded_by_session: dict[date, set[str]], symbol: str, min_sessions: int = MIN_TERMINUS_SESSIONS) -> date | None`
  - `traded_by_session(raw_root: Path, sessions: Sequence[date]) -> dict[date, set[str]]`
  - `RAW_MINUTE_AGGS: str` — the raw sub-path, so callers do not re-spell it

- [ ] **Step 1: Write the failing test**

Create `tests/test_terminus.py`:

```python
from __future__ import annotations

from datetime import date

from clients.terminus import terminus_of

# Five real August 2026 NYSE sessions. Ticker sets are membership lists, not
# prices -- no market values are asserted here.
S = [date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 31)]


def _tape(**presence: str) -> dict[date, set[str]]:
    """presence maps a symbol to a 5-char mask of 'x' (traded) / '.' (absent)."""
    return {d: {sym for sym, mask in presence.items() if mask[i] == "x"} for i, d in enumerate(S)}


def test_a_symbol_trading_on_the_latest_session_has_no_terminus():
    assert terminus_of(_tape(AAPL="xxxxx"), "AAPL", min_sessions=1) is None


def test_a_single_absent_day_between_two_present_days_is_not_a_terminus():
    # The no-trade case the exemption exists for: an illiquid name that did not
    # print on one session. It printed again afterwards.
    assert terminus_of(_tape(SLND="xx.xx"), "SLND", min_sessions=1) is None


def test_a_trailing_run_of_absences_is_a_terminus_at_its_first_session():
    # EQR stopped printing after 2026-08-17 and never returned; the shape here is
    # that run, compressed into the five-session fixture.
    assert terminus_of(_tape(EQR="xxx.."), "EQR", min_sessions=2) == S[3]


def test_a_symbol_absent_from_every_session_terminates_at_the_window_start():
    # BK: an sp500 member with no 1d.parquet and no row on the tape at all.
    assert terminus_of(_tape(AAPL="xxxxx"), "BK", min_sessions=1) == S[0]


def test_a_trailing_run_shorter_than_the_minimum_is_not_yet_a_terminus():
    # One absent session at the end is indistinguishable from a no-trade day.
    # Calling it a terminus is how a detector starts paging on illiquid names.
    assert terminus_of(_tape(EQR="xxxx."), "EQR", min_sessions=2) is None


def test_an_empty_tape_yields_no_terminus():
    # No raw partitions on disk means the test cannot answer, and "cannot answer"
    # must never render as "delisted".
    assert terminus_of({}, "AAPL") is None
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `uv run pytest tests/test_terminus.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'clients.terminus'`.

- [ ] **Step 3: Implement**

Create `clients/terminus.py`:

```python
"""Tell an instrument that left the tape apart from one that did not trade.

`coverage` exempts a symbol absent from the day's raw traded set -- no-trade is
not missing. That rule is load-bearing: without it the interior gap scan flags
96.6% of the universe. It is also what hid three S&P 500 members that stopped
printing and never returned (docs/audits/2026-09-01-terminus-vs-no-trade.md).

The separation is a suffix test over the traded sets coverage already opens, not
a threshold on a bar file. See section 4.4 of
docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

RAW_MINUTE_AGGS = "raw/massive/us_stocks_sip/minute_aggs_v1"

# Spec 4.4 says the terminus test has "no threshold to tune". That was written
# about the SHAPE of the test -- a suffix, not a severity cutoff -- and it stays
# true: this is not a "how many missing bars is too many" dial like the 5m scan's,
# which is the circular question 4.4 rejects. But it IS a calibrated constant,
# and calling it thresholdless would be dishonest. Spec 4.4 is amended to say so.
# ponytail: one trading week. A listed instrument that does not print for five
# consecutive sessions is not having a quiet day. Measured 2026-09-01, the four
# real termini had runs of 21/19/11/10 sessions and the other 511 members of the
# sp500+ndx100 universe had none, so anything from 2 to 10 separates them here.
# Raise it if a calibration run produces a false positive; never lower it below
# 2, where a single no-trade day becomes a delisting.
MIN_TERMINUS_SESSIONS = 5


def terminus_of(
    traded_by_session: dict[date, set[str]],
    symbol: str,
    min_sessions: int = MIN_TERMINUS_SESSIONS,
) -> date | None:
    """First session of *symbol*'s trailing run of absences, or None.

    None means "still on the tape, or not absent for long enough to tell". An
    empty tape returns None: failing to measure must never render as delisted.
    """
    sessions = sorted(traded_by_session)
    if not sessions:
        return None
    absent_from = len(sessions)
    while absent_from > 0 and symbol not in traded_by_session[sessions[absent_from - 1]]:
        absent_from -= 1
    if len(sessions) - absent_from < min_sessions:
        return None
    return sessions[absent_from]


def traded_by_session(raw_root: Path, sessions: Sequence[date]) -> dict[date, set[str]]:
    """Per-session traded sets read from the raw flat-file partitions.

    A session with no partition is omitted rather than recorded as an empty set:
    an absent file is "we did not fetch that day", and an empty set would read as
    "nothing traded" -- which would terminate the entire universe at once.
    """
    out: dict[date, set[str]] = {}
    for session in sessions:
        path = raw_root / f"date={session.isoformat()}" / "_symbols.parquet"
        if not path.exists():
            continue
        out[session] = set(pq.read_table(path, columns=["ticker"]).column("ticker").to_pylist())
    return out
```

- [ ] **Step 4: Run the test and verify it passes**

Run: `uv run pytest tests/test_terminus.py -v`
Expected: PASS, 6 tests.

- [ ] **Step 5: Add the reader's test**

Append to `tests/test_terminus.py`:

```python
import pyarrow as pa
import pyarrow.parquet as pq

from clients.terminus import traded_by_session


def _write_tape(root, session, tickers):
    d = root / f"date={session.isoformat()}"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"ticker": tickers}), d / "_symbols.parquet")


def test_traded_by_session_omits_a_session_with_no_partition(tmp_path):
    _write_tape(tmp_path, S[0], ["AAPL", "BK"])
    _write_tape(tmp_path, S[2], ["AAPL"])

    got = traded_by_session(tmp_path, S)

    # S[1], S[3] and S[4] have no partition and must be absent from the result,
    # not present as empty sets -- an empty set would terminate every symbol.
    assert set(got) == {S[0], S[2]}
    assert got[S[0]] == {"AAPL", "BK"}
```

- [ ] **Step 6: Run, format, lint, commit**

```bash
uv run pytest tests/test_terminus.py -v
uv run ruff format . && uv run ruff check .
git add clients/terminus.py tests/test_terminus.py
git commit -m "feat(gap): separate an instrument that left the tape from one that did not trade"
```

---

### Task 3: G14 in the engine; drop G2 and G13; a tier must ask the store

Three changes to `clients/gap_engine.py`, all from the same measurement:

1. **G14.** All four true findings were symbols that left the tape.
2. **G2 and G13 go.** They produced zero true findings out of 501. Spec §10
   deliverable 2: an unexercised branch in a detector is the thing this design
   exists to stop shipping.
3. **Tier honesty.** All four were emitted Tier A `source: massive` when
   Massive's own tape is what lacks them — a repair that fetches nothing,
   forever. Spec §9.3 rule 4.

⚠️ **What G14 does NOT do, and why the spec changes rather than the code.**
Spec §3 as first written defined G14 as absence "with no corporate action
explaining it", and §11 criterion 8 demanded the engine read the store's
freshness before emitting one. Neither is implementable against the store this
repo has: `clients/corporate_action_store.py:421` writes exactly two
`action_type` values, `"split"` and `"cash_dividend"`, and **neither removes a
ticker from the SIP tape**. There is no delisting event and no rename event to
consult, so a store lookup could never explain a terminus. G14 is therefore
emitted on **tape evidence alone**, and the spec is amended (§3, §11 criterion
8) to say so and to record the missing-event-type as the reason. Adding a
delisting feed is a separate change with its own measurement.

**Files:**
- Modify: `clients/gap_engine.py`, `clients/gap_registry.py`
- Test: `tests/test_gap_engine.py`

**Interfaces:**
- Consumes: `ExpectedSeries` (Task 1), `terminus_of` (Task 2).
- Produces:
  - `Finding.gap` is now one of `"G1" | "G3" | "G14"`.
  - `classify(series: ExpectedSeries, present: set[date], massive_floor: date, terminus: date | None = None) -> list[Finding]` — **new fourth parameter**.
  - `VALID_GAPS` in `clients/gap_registry.py` gains `"G14"`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_gap_engine.py`:

```python
from clients.gap_engine import Finding, classify


def _series(symbol="EQR", asset_class="equity", timeframe="1d", sessions=()):
    from clients.coverage_denominator import ExpectedSeries

    return ExpectedSeries(symbol, asset_class, timeframe, tuple(sessions))


FLOOR = date(2021, 7, 12)  # measured Massive entitlement floor, 2026-07-17


def test_a_terminus_is_g14_and_never_tier_a():
    # EQR left the tape on 2026-08-18. No source can supply bars for an
    # instrument that is not printing, so Tier A would queue a repair that
    # fetches nothing, forever.
    sessions = (date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20))
    findings = classify(
        _series(sessions=sessions), present={date(2026, 8, 17)},
        massive_floor=FLOOR, terminus=date(2026, 8, 18),
    )
    assert [f.gap for f in findings] == ["G14"]
    assert findings[0].tier == "B"
    assert findings[0].heal_by_days is None


def test_a_terminus_swallows_the_tail_rather_than_emitting_both():
    # Without this the same sessions are reported twice: once as a repairable G1
    # and once as an unrepairable G14, and the Tier A queue gets a job it cannot
    # do.
    sessions = (date(2026, 8, 18), date(2026, 8, 19))
    findings = classify(
        _series(sessions=sessions), present={date(2026, 8, 17)},
        massive_floor=FLOOR, terminus=date(2026, 8, 18),
    )
    assert len(findings) == 1


def test_a_missing_file_with_a_terminus_is_g14_not_g3():
    # BK is in sp500.json, has no 1d.parquet, and has never been on the tape.
    findings = classify(
        _series(symbol="BK", sessions=(date(2026, 8, 18),)), present=set(),
        massive_floor=FLOOR, terminus=date(2026, 8, 3),
    )
    assert [f.gap for f in findings] == ["G14"]
    assert findings[0].tier == "B"


def test_a_missing_file_with_no_terminus_is_still_g3_tier_a():
    # The acceptance-criterion-2 case: a symbol that never landed but IS on the
    # tape is a real, repairable gap. This must not regress.
    findings = classify(
        _series(sessions=(date(2026, 8, 18),)), present=set(),
        massive_floor=FLOOR, terminus=None,
    )
    assert [f.gap for f in findings] == ["G3"]
    assert findings[0].tier == "A"


def test_interior_and_head_gaps_are_no_longer_emitted():
    # G2 and G13 produced zero true findings out of 501 on the first production
    # run. Only the tail is reported.
    sessions = (date(2026, 8, 3), date(2026, 8, 5), date(2026, 8, 7))
    findings = classify(
        _series(sessions=sessions), present={date(2026, 8, 4), date(2026, 8, 6)},
        massive_floor=FLOOR, terminus=None,
    )
    assert [f.gap for f in findings] == ["G1"]
    assert findings[0].sessions == (date(2026, 8, 7),)
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_gap_engine.py -v`
Expected: FAIL — `classify()` takes no `terminus` keyword.

- [ ] **Step 3: Implement**

In `clients/gap_engine.py`, change the `Finding.gap` comment and rewrite
`classify`:

```python
    gap: str  # "G1" tail | "G3" nothing on disk | "G14" left the tape
```

```python
def classify(
    series: ExpectedSeries,
    present: set[date],
    massive_floor: date,
    terminus: date | None = None,
) -> list[Finding]:
    expected = set(series.sessions)
    missing = tuple(sorted(expected - present))
    if not missing:
        return []
    if terminus is not None:
        # An instrument that left the tape cannot be repaired from any source, so
        # it is one Tier B finding rather than a repairable G1/G3. Emitting both
        # would put a job in the Tier A queue that fetches nothing, forever.
        terminal = tuple(d for d in missing if d >= terminus)
        if terminal:
            return [_terminus_finding(series, terminal)]
    if not present:
        return [_finding(series, "G3", missing, massive_floor)]

    # ponytail: tail only. G2 (interior) and G13 (head) produced zero true
    # findings out of 501 on the first production run, and interior absence
    # within bar files alone is the circular question that made the 5m scan flag
    # 96.6% of the universe. Reinstate either only with a measurement asking for
    # it -- the taxonomy still names them (spec section 3).
    newest_present = max(present)
    tail = tuple(d for d in missing if d > newest_present)
    return [_finding(series, "G1", tail, massive_floor)] if tail else []
```

Add above `classify`:

```python
def _terminus_finding(series: ExpectedSeries, sessions: tuple[date, ...]) -> Finding:
    """Always Tier B, in every cell, with no heal-by.

    Spec section 9.3 rule 4: a tier is a claim about a store. No store carries
    bars for an instrument that is not printing, so the rolling-window arithmetic
    that produces `heal_by_days` has nothing to measure and would sort a job that
    can never run to the front of the repair queue.
    """
    return Finding(
        symbol=series.symbol,
        asset_class=series.asset_class,
        timeframe=series.timeframe,
        gap="G14",
        sessions=sessions,
        heal_by_days=None,
        tier="B",
        source=repair_source(series.asset_class),
    )
```

In `clients/gap_registry.py`, widen the gap-id set and correct its comment:

```python
# G1..G12 are the spec taxonomy. G13 (head gap) and G14 (terminus: the symbol
# left the tape) were added by this engine. G2 and G13 are named but not emitted
# -- see clients/gap_engine.classify.
VALID_GAPS = {f"G{n}" for n in range(1, 15)}
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_gap_engine.py -v`
Expected: PASS. Existing tests asserting a `G2` or `G13` result must be
**deleted, not weakened** — the branches are gone. Existing tests asserting `G1`
and `G3` must still pass unchanged; if one does not, the change is wrong.

- [ ] **Step 5: Format, lint, commit**

```bash
uv run ruff format . && uv run ruff check .
git add clients/gap_engine.py clients/gap_registry.py tests/test_gap_engine.py
git commit -m "feat(gap): G14 terminus, tier B by construction; retire the unexercised G2/G13 branches"
```

---

### Task 4: The registry rows say what the engine actually emits

Six rows declare `"gap": ["G1","G2","G3"]`. After Task 3 the engine emits
G1/G3 everywhere and G14 **only where a terminus can actually be computed**.

⚠️ **G14 goes on the equity row only.** `terminus_of` reads the SIP raw traded
sets (`raw/massive/us_stocks_sip/minute_aggs_v1`). There is no equivalent tape
for rates, fx, volatility, futures or cmdty, and Task 7 computes no terminus for
them — so a G14 on those five rows would be the registry claiming a check that
never runs, which is the exact failure `VALID_CHECKS` exists to prevent
(`clients/gap_registry.py:20-24`).

**Files:**
- Modify: `registry/gaps.json`
- Test: `tests/test_gap_registry_contract.py`

**Interfaces:**
- Consumes: `VALID_GAPS` (Task 3).
- Produces: rows whose `gap` tuple is exactly `("G1", "G3", "G14")` and whose
  `test` field points at a test that exists.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_gap_registry_contract.py`:

```python
def test_no_row_declares_a_gap_the_engine_no_longer_emits():
    # classify() emits G1, G3 and G14 and nothing else. A row naming G2 promises
    # a check that no longer performs.
    for row in load_registry(Path("registry/gaps.json")):
        assert set(row.gap) <= {"G1", "G3", "G14"}, row.id


def test_g14_is_declared_only_where_a_tape_exists_to_compute_it():
    # terminus_of reads the SIP raw traded sets. There is no such tape for rates,
    # fx, volatility, futures or cmdty, so a G14 there would be a claim with no
    # check behind it -- the registry-side version of the disk-glob failure.
    for row in load_registry(Path("registry/gaps.json")):
        assert ("G14" in row.gap) == (row.asset_class == "equity"), row.id
```

- [ ] **Step 2: Run and verify it fails**

Run: `uv run pytest tests/test_gap_registry_contract.py -v`
Expected: FAIL — rows still declare `G2`.

- [ ] **Step 3: Edit the six rows**

In `registry/gaps.json`, drop `"G2"` from all six `gap` arrays, and add `"G14"`
to the **equity row only**:

```json
    "gap": ["G1", "G3", "G14"],     // equity row
    "gap": ["G1", "G3"],            // the other five
```

**Leave every `id` unchanged.** An earlier revision renamed them
`g1-g2-g3-*` → `g1-g3-g14-*`; nothing parses an id, the rename touches no
behaviour, and it would break any queue entry or manifest already keyed on the
old value. Leave `asset_class`, `timeframe`, `universe`, `check`, `params` and
`since` untouched too. Set every row's `test` to
`tests/test_gap_engine.py::test_a_missing_file_with_no_terminus_is_still_g3_tier_a`.

- [ ] **Step 4: Run and verify it passes**

Run: `uv run pytest tests/test_gap_registry_contract.py tests/test_gap_registry.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
uv run ruff format . && uv run ruff check .
git add registry/gaps.json tests/test_gap_registry_contract.py
git commit -m "chore(gap): registry rows declare the gaps the engine emits"
```

---

### Task 5: The non-equity denominator comes from the registry, and gains fx + cmdty

`compute_non_equity_coverage` globs disk and hardcodes
`NON_EQUITY_ASSET_CLASSES = ("volatility", "futures", "rates")` — omitting `fx`
and `cmdty` entirely (spec §4). These universes are small (~14 volatility
indices, a handful of futures, 4 Treasury series), have no no-trade exemption and
no footer cache, so this is where the registry denominator lands with the least
risk. Do this before the equity path.

**Files:**
- Modify: `livewire_scripts/coverage_report.py:363-397`
- Test: `tests/test_coverage_report.py`

**Interfaces:**
- Consumes: `build_denominator`, `session_due_at`, `DUE_LAG_DAYS` (Task 1); `load_registry` (existing).
- Produces: `compute_non_equity_coverage(target_date: date, bronze_root: Path | None = None, registry_path: Path | None = None, presets_dir: Path | None = None, as_of: datetime | None = None) -> dict[str, CoverageResult]`, keyed by asset class, now including `"fx"` and `"cmdty"`.
- `format_non_equity_line` renders the same `<date> non-equity 1d: <ac>=<p>/<t> …`
  shape; only the number of terms changes.

⚠️ **`presets_dir` is a parameter, not a hardcoded `Path("presets")`.** A
cwd-relative preset directory is a trap this repo has already been bitten by:
`CLAUDE.md` records that `repair-legacy-basis`'s `--presets-dir` default made
`--priority-only` "silently repair zero symbols and exit 0" when run from
anywhere but the repo root. Here the same default would silently produce an
**empty denominator**, i.e. `14/14 (100.00%)` with a universe of zero.

⚠️ **These asset classes do not trade an XNYS calendar.**
`clients/gap_registry.py:25-30` records verbatim that fx trades ~24/5, CME
futures keep their own sessions and FRED publishes on its own schedule, and
calls this "a KNOWN, DEFERRED limitation … recorded here so a new asset class
cannot inherit the blind spot silently". This task adds two classes to a
detector built on that blind spot, so it must carry the limitation forward
explicitly rather than describing them as "least risk" — which the previous
revision did, wrongly.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_coverage_report.py`:

```python
def test_non_equity_denominator_includes_fx_and_cmdty(tmp_path):
    # Both were absent from the hardcoded tuple, so a stale DXY or a stale gold
    # contract was invisible to coverage at every timeframe.
    bronze = tmp_path / "bronze"
    (bronze / "asset_class=rates" / "symbol=DGS10").mkdir(parents=True)
    results = compute_non_equity_coverage(date(2026, 8, 28), bronze_root=bronze)
    assert "fx" in results
    assert "cmdty" in results


def test_a_non_equity_symbol_that_never_landed_is_counted_missing(tmp_path):
    # The whole point of the registry denominator: DGS30 is in the rates preset
    # and has no directory at all, so a disk glob cannot see it.
    bronze = tmp_path / "bronze"
    (bronze / "asset_class=rates" / "symbol=DGS10").mkdir(parents=True)
    results = compute_non_equity_coverage(date(2026, 8, 28), bronze_root=bronze)
    assert "DGS30" in results["rates"].missing_symbols


def test_rates_is_not_expected_until_t_plus_2(tmp_path):
    # FRED publishes a session behind (spec 8.1). At 11:00 UTC on 2026-08-29 the
    # 2026-08-28 session is due for equity but NOT for rates, so an empty rates
    # tree must produce a zero-length denominator rather than 4 phantom gaps.
    bronze = tmp_path / "bronze"
    (bronze / "asset_class=rates").mkdir(parents=True)
    results = compute_non_equity_coverage(
        date(2026, 8, 28), bronze_root=bronze,
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert results["rates"].total == 0


def test_every_non_equity_row_is_declared_xnys_or_rejected():
    # gap_registry.XNYS_CALENDAR_ASSET_CLASSES is the recorded blind spot. This
    # test does not fix the calendar; it makes adding a sixth asset class an
    # explicit act rather than a silent inheritance.
    from clients.gap_registry import XNYS_CALENDAR_ASSET_CLASSES, load_registry

    for row in load_registry(Path("registry/gaps.json")):
        assert row.asset_class in XNYS_CALENDAR_ASSET_CLASSES, row.id
```

- [ ] **Step 2: Run and verify it fails**

Run: `uv run pytest tests/test_coverage_report.py -k non_equity -v`
Expected: FAIL — `KeyError: 'fx'`.

- [ ] **Step 3: Implement**

In `livewire_scripts/coverage_report.py`, replace the `NON_EQUITY_ASSET_CLASSES`
constant and the body of `compute_non_equity_coverage`:

```python
# ponytail: derived from the registry, not written here. The hardcoded tuple
# omitted fx and cmdty, so a stale DXY was invisible -- and the omission was
# invisible too, because nothing compared the tuple to the asset classes the
# warehouse actually carries.
def _non_equity_rows(registry_path: Path | None):
    rows = load_registry(registry_path or Path("registry/gaps.json"))
    return [r for r in rows if r.asset_class != "equity" and r.timeframe == "1d"]


def compute_non_equity_coverage(
    target_date: date,
    bronze_root: Path | None = None,
    registry_path: Path | None = None,
    presets_dir: Path | None = None,
    as_of: datetime | None = None,
) -> dict[str, CoverageResult]:
    """Return per-asset-class 1d freshness for the non-equity universes.

    The denominator is the registry universe, never the files on disk: a symbol
    that never landed has to stay countable. No no-trade exemption -- these are
    small universes and a stale one is a real gap.

    ponytail: the calendar is XNYS for every class here, which is WRONG for fx
    (~24/5), CME futures and FRED -- see gap_registry.XNYS_CALENDAR_ASSET_CLASSES.
    A bar expected on an XNYS holiday is not expected at all, so its absence is
    invisible. Known, deferred, and carried forward deliberately; the fix is a
    per-class calendar, not a tweak here.
    """
    bronze_root = bronze_root or _resolved_data_lake() / "bronze"
    presets_dir = presets_dir or Path("presets")
    # Real wall clock, not the session's own due time: passing session_due_at
    # (target_date) here would make the due filter tautologically true and the
    # whole deadline rule inert.
    as_of = as_of or datetime.now(UTC)
    results: dict[str, CoverageResult] = {}
    for row in _non_equity_rows(registry_path):
        expected = build_denominator(
            [presets_dir / f"{name}.json" for name in row.universe],
            row.asset_class,
            "1d",
            target_date,
            target_date,
            as_of=as_of,
            lag_days=DUE_LAG_DAYS.get(row.asset_class, 1),
        )
        # An empty denominator means the session is not due yet for this class
        # (rates at T+2). Zero of zero, not N phantom gaps.
        universe = {series.symbol for series in expected if series.sessions}
        present = set()
        for symbol in universe:
            path = bronze_root / f"asset_class={row.asset_class}" / f"symbol={symbol}" / "1d.parquet"
            if not path.exists():
                continue
            latest = _latest_date_in_parquet(path, "trade_date")
            if latest is not None and latest >= target_date:
                present.add(symbol)
        results[row.asset_class] = CoverageResult(
            timeframe=row.asset_class,
            total=len(universe),
            present=len(present),
            missing_symbols=sorted(universe - present),
        )
    return results
```

**Delete `NON_EQUITY_ASSET_CLASSES` entirely** rather than widening it. Once the
universe comes from the registry and `format_non_equity_line` iterates the
results, nothing reads the constant — keeping a wider copy of a list that is now
derived is precisely the duplicate-source-of-truth this task removes. Update its
import in `tests/test_coverage_report.py:21` and its three uses at `:732,738,739`
to read the registry (or the returned dict's keys) instead.

Change `format_non_equity_line` to iterate the results rather than the constant,
so a registry row added later renders without a second edit:

```python
def format_non_equity_line(target_date: date, results: dict[str, CoverageResult]) -> str:
    parts = [f"{ac}={results[ac].present}/{results[ac].total}" for ac in sorted(results)]
    return f"{target_date} non-equity 1d: " + " ".join(parts)
```

Add to the imports at the top of the file:

```python
from datetime import UTC, datetime

from clients.coverage_denominator import DUE_LAG_DAYS, build_denominator, session_due_at
from clients.gap_registry import load_registry
```

- [ ] **Step 4: Run and verify it passes**

Run: `uv run pytest tests/test_coverage_report.py -v`
Expected: PASS.

⚠️ **Correction to the previous revision of this plan, which claimed
"`status.py` and the digest read the `non-equity 1d:` line by prefix and parse
`ac=p/t` terms". That is false.** Verified 2026-09-01:
`status.py:308` selects lines with `re.compile(r"\bcoverage:\s")`, which the
`non-equity 1d:` line does not match; `weekly_quality_summary.py:44-50` requires
a `coverage:` header followed by `1d=`; and a repo-wide grep finds **no parser
for that line at all**. The two extra terms are still safe — because nothing
reads them, not because a parser tolerates them. That is also a gap worth naming:
the non-equity line is written and graded by nobody. Wiring it into `status`
is out of scope here (see Out of scope). Still run
`uv run pytest tests/test_status.py tests/test_nightly_digest.py -v` to confirm
nothing regressed.

- [ ] **Step 5: Format, lint, commit**

```bash
uv run ruff format . && uv run ruff check .
git add livewire_scripts/coverage_report.py tests/test_coverage_report.py
git commit -m "fix(coverage): non-equity denominator comes from the registry, and fx/cmdty stop being invisible"
```

---

### Task 6: Equity 1d — a registry-backed denominator and a terminus carve-out

The equity `1d` universe is `on_disk`, so BK — an `sp500.json` member with no
`1d.parquet` — is undetectable by construction. Union in the registry
denominator, and route terminus symbols **out of the denominator entirely** so
the count stays honest in both directions.

**Do not touch the intraday branch.** `universe = set(traded_today)` for
intraday is already provider-derived and correct; the footer cache, the thread
pool and the recovery path stay exactly as they are.

⚠️ **This is a union, not a replacement, and the plan says so.** The registry's
equity row names `["sp500", "ndx100"]` = 515 symbols; `on_disk` is ~13,270. So
**~96% of the 1d denominator remains disk-derived after this task**, and the
"never derived from disk" language in spec §4 describes a destination, not this
change. What the union buys is precisely one thing, and it is the thing that
matters: a registry member with **no file at all** becomes expressible. Spec §8
lists five equity presets (`sp500, ndx100, r2k, etfs, adrs`); widening the row
to all five is a separate change gated on a calibration run at that scale — see
the terminus-scope warning below for why.

⚠️ **Terminus is computed over the REGISTRY universe only, never over
`on_disk`.** The 2026-09-01 measurement that produced zero false positives ran
over 515 liquid sp500+ndx100 members. `on_disk` is ~13,270 symbols dominated by
the illiquid tail that `CLAUDE.md` records as legitimately not printing for days
at a time — the same population that made the 5m interior scan flag 96.6% of the
universe. Applying a threshold calibrated on the liquid 515 to all 13,270 is a
category error, and it is the one that would turn this feature into another
standing WARN nobody reads. Widen the scope only with a measured run at the
wider scale, recorded in the audit note.

**Files:**
- Modify: `livewire_scripts/coverage_report.py:240-334`, and `auto_recover`'s recheck at `:519`
- Test: `tests/test_coverage_report.py`

**Interfaces:**
- Consumes: `build_denominator`, `session_due_at` (Task 1); `terminus_of`,
  `traded_by_session`, `RAW_MINUTE_AGGS` (Task 2).
- Produces:
  - `CoverageResult` gains `terminus_symbols: tuple[tuple[str, date], ...] = ()` — `(symbol, terminus date)` pairs, so the log line can name the date without a second lookup.
  - `compute_coverage(target_date, bronze_root=None, cache_path=None, registry_path=None, presets_dir=None, as_of=None)`.
  - `format_terminus_block(results) -> list[str]` returning at most one
    `  1d terminus: <sym>@<date>, …` line.
  - **Invariant:** `total == present + len(missing_symbols)`, and a terminus
    symbol appears in none of the three.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_coverage_report.py`. Use the file's existing
`_write_daily(bronze_root, symbol, dates)` (`:74`) and
`_write_raw_symbols(bronze_root, target, symbols)` (`:121`) — do **not** add new
bronze/tape fixtures. Their OHLC values are structural placeholders, which is
fine here because nothing below asserts on a price.

```python
def test_a_preset_member_with_no_parquet_is_counted_missing(tmp_path):
    # BK, measured 2026-09-01: an sp500 member with no 1d.parquet at all. The
    # disk-glob denominator cannot express this symbol, which is why it read as
    # 100% healthy for weeks.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_raw_symbols(bronze, date(2026, 8, 28), ["AAPL", "BK"])
    results = compute_coverage(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "BK"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert "BK" in results["1d"].missing_symbols


def test_a_terminus_symbol_is_in_neither_present_nor_missing(tmp_path):
    # EQR left the tape 2026-08-18. Counting it missing queues an impossible
    # repair; counting it present is what hid it. It is in NEITHER, and the
    # ratio must not be able to read green because of it.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_daily(bronze, "EQR", [date(2026, 8, 17)])
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    results = compute_coverage(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    result = results["1d"]
    assert "EQR" in dict(result.terminus_symbols)
    assert "EQR" not in result.missing_symbols
    # The regression this test exists for: the no-trade exemption also counts an
    # absent symbol PRESENT, so subtracting terminus from `missing` alone is a
    # no-op and the ratio still reads 100%.
    assert result.total == result.present + len(result.missing_symbols)
    assert result.total == 1


def test_a_one_day_absence_is_still_exempted_as_no_trade(tmp_path):
    # The exemption stays load-bearing. Without it the interior scan flags 96.6%
    # of the universe, and this test is the guard on that.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_daily(bronze, "SLND", [date(2026, 8, 27)])
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 27)):
        _write_raw_symbols(bronze, session, ["AAPL", "SLND"])
    _write_raw_symbols(bronze, date(2026, 8, 28), ["AAPL"])
    results = compute_coverage(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "SLND"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert "SLND" not in results["1d"].missing_symbols
    assert results["1d"].terminus_symbols == ()


def test_before_the_deadline_the_session_is_not_expected_at_all(tmp_path):
    # Spec section 11 criterion 11, exercised through compute_coverage rather
    # than through build_denominator. Passing as_of=session_due_at(target_date)
    # would make the due filter tautologically true, so the ONLY test that can
    # catch a regression here is one that goes through the real caller with a
    # real clock. 04:21 UTC on 2026-08-29 is before the 10:00 UTC deadline for
    # session 2026-08-28, so nothing is due and nothing is missing.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 27)])
    _write_raw_symbols(bronze, date(2026, 8, 28), ["AAPL"])
    early = compute_coverage(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 4, 21, tzinfo=UTC),
    )
    assert early["1d"].total == 0
    late = compute_coverage(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert late["1d"].missing_symbols == ["AAPL"]


def test_terminus_is_not_computed_for_symbols_outside_the_registry(tmp_path):
    # SLND is on disk but not in any preset. The terminus threshold is calibrated
    # on 515 liquid names; the illiquid on-disk tail genuinely does not print for
    # days, so applying it there manufactures the 96.6% disease.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", [date(2026, 8, 28)])
    _write_daily(bronze, "SLND", [date(2026, 8, 5)])
    for session in _sessions(date(2026, 8, 6), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    results = compute_coverage(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert results["1d"].terminus_symbols == ()
```

Add exactly one new helper, because nothing writes a registry today:

```python
def _registry_for(tmp_path: Path, tickers: list[str]) -> Path:
    """A one-row registry plus the preset it names, both under tmp_path.

    Pair it with presets_dir=tmp_path / "presets" at the call site; production
    resolves preset names against a directory it is GIVEN, never against the CWD.
    """
    presets = tmp_path / "presets"
    presets.mkdir(exist_ok=True)
    (presets / "t.json").write_text(json.dumps({"name": "t", "tickers": tickers}))
    registry = tmp_path / "gaps.json"
    registry.write_text(json.dumps([{
        "id": "g1-g2-g3-equity-daily",
        "gap": ["G1", "G3", "G14"],
        "asset_class": "equity",
        "timeframe": "1d",
        "universe": ["t"],
        "check": "denominator_diff",
        "params": {},
        "tier": "A",
        "since": "2026-08-31",
        "test": "tests/test_gap_engine.py::test_a_missing_file_with_no_terminus_is_still_g3_tier_a",
    }]))
    return registry


def _sessions(start: date, end: date) -> list[date]:
    from clients.trading_calendar import trading_dates_in_range

    return trading_dates_in_range(start, end)
```

- [ ] **Step 2: Run and verify they fail**

Run: `uv run pytest tests/test_coverage_report.py -k "preset_member or terminus or no_trade or deadline" -v`
Expected: FAIL — `compute_coverage() got an unexpected keyword argument 'registry_path'`.

- [ ] **Step 3: Implement**

Add the field to `CoverageResult`:

```python
    # (symbol, terminus date) pairs. In NEITHER `present` nor `missing_symbols`,
    # and NOT in `total`: no source can supply bars for an instrument that
    # stopped printing, so counting these missing queues an impossible repair and
    # counting them present is what hid BK, EA, AVB and EQR.
    terminus_symbols: tuple[tuple[str, date], ...] = ()
```

Add the parameters to `compute_coverage` and, immediately after `traded_today`
is computed, build the terminus window once for the whole run:

```python
def compute_coverage(
    target_date: date,
    bronze_root: Path | None = None,
    cache_path: Path | None = None,
    registry_path: Path | None = None,
    presets_dir: Path | None = None,
    as_of: datetime | None = None,
) -> dict[str, CoverageResult]:
    ...
    # The real clock. NOT session_due_at(target_date): that makes the due filter
    # `session_due_at(d) <= as_of` tautologically true for a single-session
    # window, so the entire deadline rule would be inert on the one code path
    # production runs.
    as_of = as_of or datetime.now(UTC)
    presets_dir = presets_dir or Path("presets")
```

```python
    # One window, read once, shared by every timeframe. 40 calendar days is
    # comfortably more than 20 sessions including holidays; the slice, not the
    # span, defines the window. previous_trading_day(d) takes a single argument
    # and cannot step back N sessions, which is why this is a slice.
    window = trading_dates_in_range(
        target_date - timedelta(days=40), target_date
    )[-TERMINUS_WINDOW_SESSIONS:]
    tape = traded_by_session(bronze_root.parent / RAW_MINUTE_AGGS, window)
```

Replace the `1d` universe line and the `present_symbols` / `missing` block
together — they are one change, and splitting them is exactly how the previous
revision produced a no-op:

```python
        registry_universe: set[str] = set()
        if tf == "1d":
            expected = build_denominator(
                _equity_preset_paths(registry_path, presets_dir),
                "equity", "1d", target_date, target_date, as_of=as_of,
            )
            # An empty session tuple means the ingestion deadline has not passed
            # for this session. Zero of zero -- not one phantom tail gap per
            # symbol, which is what 497 of the first run's 501 findings were.
            if not any(series.sessions for series in expected):
                results[tf] = CoverageResult(tf, 0, 0, [], ())
                continue
            # The registry, not the disk. A symbol that never landed has no file
            # to glob, which is why BK -- an sp500 member -- read as 100% healthy.
            # ponytail: a UNION, so ~96% of this denominator is still disk-derived.
            # Widening the registry row is a separate, measured change.
            registry_universe = {series.symbol for series in expected}
            universe = on_disk | registry_universe
        else:
            universe = on_disk if not traded_today else set(traded_today)

        # Scoped to the registry universe deliberately: the threshold is
        # calibrated on 515 liquid names and the on-disk tail legitimately does
        # not print for days.
        terminus: dict[str, date] = {}
        if tf == "1d":
            terminus = {
                symbol: when
                for symbol in registry_universe
                if (when := terminus_of(tape, symbol)) is not None
            }

        # A terminus leaves the denominator ENTIRELY. Subtracting it from
        # `missing` alone is a no-op: the no-trade exemption below already counts
        # an absent symbol as present, so the symbol simply moves from one
        # counted bucket to the other and the ratio still reads green.
        countable = universe - set(terminus)
        present_symbols = {
            symbol
            for symbol in countable
            if (latest_by_symbol.get(symbol) or date.min) >= target_date
            or (tf == "1d" and traded_today and symbol not in traded_today)
        }
        missing = sorted(countable - present_symbols)
        # ponytail: a plain assert; nothing in this repo runs python -O. It is
        # here because the two buckets drifting apart silently is the exact
        # failure mode this task is correcting.
        assert len(countable) == len(present_symbols) + len(missing)
        results[tf] = CoverageResult(
            timeframe=tf,
            total=len(countable),
            present=len(present_symbols),
            missing_symbols=missing,
            terminus_symbols=tuple(sorted(terminus.items())),
        )
```

Add the helper and the log block:

```python
def _equity_preset_paths(registry_path: Path | None, presets_dir: Path) -> list[Path]:
    rows = load_registry(registry_path or Path("registry/gaps.json"))
    names: list[str] = []
    for row in rows:
        if row.asset_class == "equity" and row.timeframe == "1d":
            names.extend(row.universe)
    return [presets_dir / f"{name}.json" for name in dict.fromkeys(names)]


def format_terminus_block(results: dict[str, CoverageResult]) -> list[str]:
    """One `  1d terminus:` line, or none.

    Deliberately NOT of the form `<tf>=<p>/<t>`: `status._coverage_section`
    selects the last line matching `coverage:` and parses timeframe terms, so a
    detail line that looked like a measurement would be read as one.
    """
    pairs = results["1d"].terminus_symbols
    if not pairs:
        return []
    listed = ", ".join(f"{symbol}@{when.isoformat()}" for symbol, when in pairs[:10])
    suffix = f", ... ({len(pairs)} total)" if len(pairs) > 10 else ""
    return [f"  1d terminus: {listed}{suffix}"]
```

Import `trading_dates_in_range` from `clients.trading_calendar`, `timedelta`
from `datetime`, and `RAW_MINUTE_AGGS`, `terminus_of`, `traded_by_session` from
`clients.terminus`. Add the window constant beside them:

```python
# The window the 2026-09-01 measurement used. MIN_TERMINUS_SESSIONS decides
# inside it; this only bounds how far back a terminus can be dated.
TERMINUS_WINDOW_SESSIONS = 20
```

Call `format_terminus_block(results)` wherever `format_missing_blocks(results)`
is already appended to the log, immediately after it.

- [ ] **Step 4: Fix the auto-recovery recheck**

`auto_recover` re-runs coverage at `livewire_scripts/coverage_report.py:519`:

```python
    rechecked = compute_coverage(effective_target, bronze_root=bronze_root)[timeframe]
```

It drops every new argument, so the recheck runs with the **disk-glob**
denominator: a registry-only symbol like BK vanishes from `universe` and reads
as recovered. Thread the arguments through `auto_recover` and pass them:

```python
    rechecked = compute_coverage(
        effective_target, bronze_root=bronze_root,
        registry_path=registry_path, presets_dir=presets_dir, as_of=as_of,
    )[timeframe]
```

Add a test asserting a registry-only symbol is still reported after a recovery
attempt that could not have fetched it.

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest tests/ -v`
Expected: PASS **except** `tests/test_gap_scan*.py`, which still exercise
`gap_scan.py:98`'s `--as-of` as a `date` and now raise `TypeError` against the
`datetime` filter from Task 1. That is expected and is not fixed here: Task 7
replaces those tests and Task 8 deletes the module. Run
`uv run pytest tests/ -v --ignore=tests/test_gap_scan.py --ignore=tests/test_gap_scan_integration.py`
for a clean signal, and record in the commit message that the two files are
knowingly red between Tasks 6 and 8.

The `coverage:` and `non-equity 1d:` line shapes are unchanged; only the
`1d=<p>/<t>` totals move, which is the intended one-time step.

- [ ] **Step 6: Format, lint, commit**

```bash
uv run ruff format . && uv run ruff check .
git add livewire_scripts/coverage_report.py tests/test_coverage_report.py
git commit -m "fix(coverage): registry-backed 1d denominator; a terminus leaves the denominator entirely"
```

---

### Task 7: `coverage_report` runs the classifier — absorb before deleting

**This task is the one the previous revision was missing, and without it the
whole plan is a net deletion of function.** `classify()` has exactly one
production caller: `gap_scan.py:62`. Task 6 builds `CoverageResult` objects and
never a `Finding`. If `gap_scan` were deleted now, `Finding`, `heal_by_days`,
the tier logic, the G14 class added in Task 3 and the decision queue would all
have zero scheduled callers — and spec §10 deliverable 8 names that queue's
depth as the measurement that decides whether Phase 2 gets built.

Coverage today answers *"is this symbol current as of one session?"*
(`latest >= target_date`). The classifier answers *"which sessions in a 30-day
window are absent, and what class and tier is each run of them?"* This task
makes `coverage_report` answer the second question too — once, over the same
registry rows and the same tape read Task 6 already performs.

**Files:**
- Modify: `livewire_scripts/coverage_report.py`
- Test: `tests/test_coverage_orchestration.py` (**create**)

**Interfaces:**
- Consumes: `build_denominator`, `session_due_at`, `DUE_LAG_DAYS` (Task 1);
  `terminus_of`, `traded_by_session` (Task 2); `actual_sessions`, `classify`,
  `massive_floor_for`, `load_unresolved`, `suppress_unresolved` (existing in
  `clients/gap_engine.py`).
- Produces:
  - `scan_findings(target_date, *, bronze_root, registry_path=None, presets_dir=None, as_of=None, window_days=30) -> list[Finding]`
  - `write_tier_a_manifest(findings, path)` and `write_decision_requests(findings, path)`, moved from `gap_scan.py` with the same JSON shape, so an existing consumer does not change.

- [ ] **Step 1: Move the writers, with their whole dependency**

Move `write_tier_a_manifest`, `write_decision_requests`, `_urgency` **and
`_entry` (`livewire_scripts/gap_scan.py:66-75`)** into
`livewire_scripts/coverage_report.py`. `_entry` is what both writers call to
build a row; the previous revision's copy list omitted it, so the step could not
have run. Copy their tests from `tests/test_gap_scan.py` into
`tests/test_coverage_orchestration.py`, changing only the import line.

One behavioural change while moving `write_decision_requests`
(`gap_scan.py:84-88`), which hardcodes `verdict="inconclusive"`:

```python
        # Spec section 10.8 added `terminus` as a fifth verdict precisely because
        # "we could not tell" and "the instrument stopped printing" are different
        # answers, and an operator triaging the queue acts differently on each.
        "verdict": "terminus" if finding.gap == "G14" else "inconclusive",
```

- [ ] **Step 2: Write the failing end-to-end test**

Create `tests/test_coverage_orchestration.py`. This replaces
`tests/test_gap_scan_integration.py`, which Task 8 deletes — the coverage the
suite loses there has to be regained here, not merely dropped.

```python
def test_a_terminus_reaches_the_decision_queue_and_not_the_tier_a_manifest(tmp_path):
    # The whole chain, end to end: registry -> denominator -> on-disk diff ->
    # classify -> artifacts. EQR left the tape, so it must land in the Tier B
    # queue with verdict "terminus" and must NOT land in the Tier A manifest,
    # where the repair executor would fetch nothing forever.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", _sessions(date(2026, 8, 3), date(2026, 8, 28)))
    _write_daily(bronze, "EQR", _sessions(date(2026, 8, 3), date(2026, 8, 17)))
    for session in _sessions(date(2026, 8, 3), date(2026, 8, 17)):
        _write_raw_symbols(bronze, session, ["AAPL", "EQR"])
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])

    findings = scan_findings(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert [(f.symbol, f.gap, f.tier) for f in findings] == [("EQR", "G14", "B")]

    manifest, queue = tmp_path / "a.json", tmp_path / "b.json"
    write_tier_a_manifest(findings, manifest)
    write_decision_requests(findings, queue)
    assert json.loads(manifest.read_text()) == []
    assert json.loads(queue.read_text())[0]["verdict"] == "terminus"


def test_a_registry_member_with_no_file_is_a_g3_in_the_tier_a_manifest(tmp_path):
    # BK: an sp500 member with no 1d.parquet and a live tape presence. Nothing
    # about it is a terminus, so it is a plain repairable G3 -- and Tier A,
    # because equity inside Massive's rolling window is repairable unattended.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", _sessions(date(2026, 8, 3), date(2026, 8, 28)))
    for session in _sessions(date(2026, 8, 3), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL", "BK"])

    findings = scan_findings(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL", "BK"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 11, 0, tzinfo=UTC),
    )
    assert [(f.symbol, f.gap, f.tier) for f in findings] == [("BK", "G3", "A")]


def test_a_session_before_its_deadline_produces_no_findings(tmp_path):
    # The 497 phantoms, at the artifact layer. A run at 04:21 UTC must write an
    # empty manifest, not one tail gap per symbol in the universe.
    bronze = tmp_path / "bronze"
    _write_daily(bronze, "AAPL", _sessions(date(2026, 8, 3), date(2026, 8, 27)))
    for session in _sessions(date(2026, 8, 3), date(2026, 8, 28)):
        _write_raw_symbols(bronze, session, ["AAPL"])
    findings = scan_findings(
        date(2026, 8, 28), bronze_root=bronze,
        registry_path=_registry_for(tmp_path, ["AAPL"]),
        presets_dir=tmp_path / "presets",
        as_of=datetime(2026, 8, 29, 4, 21, tzinfo=UTC),
    )
    assert findings == []
```

- [ ] **Step 3: Run and verify they fail**

Run: `uv run pytest tests/test_coverage_orchestration.py -v`
Expected: FAIL — `ImportError: cannot import name 'scan_findings'`.

- [ ] **Step 4: Implement `scan_findings`**

In `livewire_scripts/coverage_report.py`:

```python
# The classifier window. gap_scan used 30 days; keeping it means the artifacts
# this produces are comparable to the ones it produced.
SCAN_WINDOW_DAYS = 30


def scan_findings(
    target_date: date,
    *,
    bronze_root: Path,
    registry_path: Path | None = None,
    presets_dir: Path | None = None,
    as_of: datetime | None = None,
    window_days: int = SCAN_WINDOW_DAYS,
) -> list[Finding]:
    """Every registry row, diffed over a trailing window and classified.

    This is the windowed classifier gap_scan.py used to be. It is here, and not
    in a fourth script, because it needs exactly what coverage already reads:
    the registry universe, the trading calendar, the on-disk bronze tree and the
    raw traded sets. Two detectors answering one question with two denominators
    is what spec section 11 criterion 7 forbids.
    """
    as_of = as_of or datetime.now(UTC)
    presets_dir = presets_dir or Path("presets")
    start = target_date - timedelta(days=window_days)
    floor = massive_floor_for(target_date)

    window = trading_dates_in_range(
        target_date - timedelta(days=40), target_date
    )[-TERMINUS_WINDOW_SESSIONS:]
    tape = traded_by_session(bronze_root.parent / RAW_MINUTE_AGGS, window)

    findings: list[Finding] = []
    for row in load_registry(registry_path or Path("registry/gaps.json")):
        expected = build_denominator(
            [presets_dir / f"{name}.json" for name in row.universe],
            row.asset_class, row.timeframe, start, target_date,
            as_of=as_of, lag_days=DUE_LAG_DAYS.get(row.asset_class, 1),
        )
        for series in expected:
            if not series.sessions:
                continue
            # Terminus is an equity-tape fact. No other asset class has a tape to
            # ask, which is why only the equity registry row declares G14.
            terminus = terminus_of(tape, series.symbol) if row.asset_class == "equity" else None
            findings.extend(
                classify(series, actual_sessions(bronze_root, series), floor, terminus=terminus)
            )
    unresolved = load_unresolved(bronze_root.parent / "repairs" / "unresolved.json")
    return suppress_unresolved(findings, unresolved)
```

- [ ] **Step 5: Wire it into `main()`**

In `coverage_report.main()`, after the coverage line is written and **before**
`auto_recover` runs, call `scan_findings` once and write both artifacts under
`<data-lake>/repairs/`:

```python
    findings = scan_findings(effective_target, bronze_root=bronze_root, as_of=as_of)
    repairs = _resolved_data_lake() / "repairs"
    write_tier_a_manifest(findings, repairs / f"tier_a_{effective_target}.json")
    write_decision_requests(findings, repairs / f"decisions_{effective_target}.json")
    log_lines.append(
        f"  scan: {len(findings)} findings "
        f"(tier A {sum(1 for f in findings if f.tier == 'A')}, "
        f"tier B {sum(1 for f in findings if f.tier == 'B')})"
    )
```

`main()` is where `as_of = datetime.now(UTC)` is established and passed to both
`compute_coverage` and `scan_findings`, so one run cannot grade two different
clocks.

⚠️ **Budget.** `com.livewire.coverage` has **no timeout** by design
(`CLAUDE.md`: a cold pass measured 2858s and every guessed budget expired), so
there is nothing to blow here. But `scan_findings` calls `actual_sessions`,
which does a **column read**, not a footer read, per series. Over 515 registry
symbols that is bounded; it is the reason `scan_findings` iterates the registry
universe and not the ~13,270-file glob. Record the measured wall clock in Task 9
step 5 and stop if it exceeds ~300s.

- [ ] **Step 6: Run and commit**

```bash
uv run pytest tests/test_coverage_orchestration.py tests/test_coverage_report.py -v
uv run ruff format . && uv run ruff check .
git add livewire_scripts/coverage_report.py tests/test_coverage_orchestration.py
git commit -m "feat(coverage): coverage_report runs the windowed classifier and writes both repair artifacts"
```

---

### Task 8: Retire the fourth detector

Spec §11 criterion 7 is measured by deletion. This runs **after** Task 7, never
before: the writers and the classifier now live in `coverage_report.py` and have
a real caller, so `gap_scan.py` is genuinely redundant rather than merely gone.

**Files:**
- Modify: `scripts/livewire_quality.py:24`
- Delete: `livewire_scripts/gap_scan.py`, `launchd/com.livewire.gap-scan.plist.example`, `tests/test_gap_scan.py`, `tests/test_gap_scan_integration.py`
- **Keep** `launchd/com.livewire.universe-refresh.plist.example` — see below
- Test: `tests/test_coverage_orchestration.py`

- [ ] **Step 1: Delete**

```bash
git rm livewire_scripts/gap_scan.py \
       launchd/com.livewire.gap-scan.plist.example \
       tests/test_gap_scan.py tests/test_gap_scan_integration.py
```

**`launchd/com.livewire.universe-refresh.plist.example` stays.** Convergence is
about *detectors*, and that template schedules `universe_sync` — a **producer**,
and the one spec §10 deliverable 4 asks for. It duplicates nothing. BK is the
argument for keeping it: BK is in `bronze-delisted/` **and** in
`presets/sp500.json`, so nothing is removing archived symbols from the universe
the denominator is built from, which is exactly what that job would fix. It is
un-installed, and installing it is out of scope here (it needs `MASSIVE_API_KEY`
and §4.3's producer run).

In `scripts/livewire_quality.py`, delete the line
`"gap-scan": "livewire_scripts.gap_scan",`.

- [ ] **Step 2: Write the convergence guard**

Append to `tests/test_coverage_orchestration.py`:

```python
def test_no_second_gap_detector_exists():
    """Spec section 11 criterion 7, measured by deletion rather than by intent.

    A fourth detector re-implements every "why is this bar legitimately absent"
    rule from scratch, and gets them wrong -- gap_scan carried neither the
    no-trade exemption nor the ingestion deadline.
    """
    repo = Path(__file__).resolve().parent.parent
    assert not (repo / "livewire_scripts/gap_scan.py").exists()
    assert not (repo / "launchd/com.livewire.gap-scan.plist.example").exists()
    assert "gap_scan" not in (repo / "scripts/livewire_quality.py").read_text()
    # The universe-refresh template is a PRODUCER, not a detector, and spec
    # section 10 deliverable 4 still wants it. Convergence does not delete it.
    assert (repo / "launchd/com.livewire.universe-refresh.plist.example").exists()


def test_classify_still_has_a_production_caller():
    """The other half of criterion 7, and the one the first revision failed.

    Deleting the only caller of classify() would retire the classifier along with
    the script, leaving Finding, the tier and the decision queue dead. Convergence
    means one caller, not zero.
    """
    import inspect

    from livewire_scripts import coverage_report

    assert "classify(" in inspect.getsource(coverage_report.scan_findings)
```

- [ ] **Step 3: Run the full suite and the coverage gate**

```bash
uv run pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: PASS with the 95% gate satisfied, and **no ignores** — the two
knowingly-red `gap_scan` test files are gone as of Step 1, so this is the first
point since Task 6 at which the whole suite is green. If it is not, stop: the
classifier absorption in Task 7 is incomplete.

- [ ] **Step 4: Format, lint, commit**

```bash
uv run ruff format . && uv run ruff check .
git add -A
git commit -m "refactor(gap): retire gap_scan — one detector, and its classifier now lives in coverage_report"
```

---

### Task 9: Calibrate and verify against the production lake

The acceptance criteria that matter here cannot be met by a test suite. Spec
§11 criteria 9–11 name their regression cases; this task runs them on the real
lake. **Read-only. Nothing in this task writes the data lake.**

⚠️ **Every command below runs against a CHECKOUT of this branch, never against
`~/market-warehouse/current`.** `CLAUDE.md`: the scheduled jobs `cd` into
`current`, an immutable `git archive` export that does not contain this code
until `release promote` runs. Verifying against `current` would measure the
**old** detector and prove nothing.

**Files:**
- Modify: `docs/audits/2026-09-01-terminus-vs-no-trade.md` — append a
  "Verification" section (the file already exists from the spike)

- [ ] **Step 1: Criterion 9 — terminus separation, zero false positives**

Over the `sp500 + ndx100` registry universe (515 members) and the trailing 20
sessions, assert the positive set is exactly `{BK, EA, AVB, EQR}` and the other
511 produce nothing:

```bash
scp -r clients presets macmini:/tmp/gap-check/
ssh macmini 'cd /tmp/gap-check && ~/market-warehouse/.venv/bin/python - ' <<'PY'
import json, os
from datetime import date
from pathlib import Path
from clients.terminus import terminus_of, traded_by_session
from clients.trading_calendar import trading_dates_in_range

lake = Path(os.path.expanduser("~/market-warehouse/data-lake"))
raw = lake / "raw/massive/us_stocks_sip/minute_aggs_v1"
members = set()
for name in ("sp500", "ndx100"):
    members |= set(json.loads(Path(f"presets/{name}.json").read_text())["tickers"])
window = trading_dates_in_range(date(2026, 8, 4), date(2026, 8, 31))
tape = traded_by_session(raw, window)
for n in (2, 3, 5, 8):
    hits = {s: t for s in members if (t := terminus_of(tape, s, min_sessions=n))}
    print(f"min_sessions={n}: {len(hits)} findings -> {sorted(hits)}")
PY
```

Expected at every value tested: exactly `['AVB', 'BK', 'EA', 'EQR']`. If a value
produces a fifth symbol, that value is too low — record which, and raise
`MIN_TERMINUS_SESSIONS` to the lowest value that still yields exactly four.

- [ ] **Step 2: Criterion 11 — no phantom tail gaps before the deadline, through the real caller**

Not `build_denominator` in isolation — that is the tautology Task 1 warns about.
Run `compute_coverage` twice against the same session with two different clocks:

```bash
uv run python - <<'PY'
from datetime import UTC, date, datetime
from pathlib import Path
from livewire_scripts.coverage_report import compute_coverage

lake = Path.home() / "market-warehouse/data-lake"
for hour in (4, 11):
    r = compute_coverage(date(2026, 8, 31), bronze_root=lake / "bronze",
                         as_of=datetime(2026, 9, 1, hour, 0, tzinfo=UTC))["1d"]
    print(hour, r.total, r.present, len(r.missing_symbols), len(r.terminus_symbols))
PY
```
Expected: `4 0 0 0 0` then a non-zero total at `11`. A non-zero `total` at hour 4
means the deadline rule is inert — stop and fix Task 6, do not adjust the
expectation.

- [ ] **Step 3: Criterion 2 — BK is still detected, and as what**

Run `scan_findings` for a recent session against the production lake and confirm
`BK` appears with `gap="G14"`, `tier="B"`, in the decision queue and **not** in
the Tier A manifest. Record EA/AVB/EQR's classes alongside it.

- [ ] **Step 4: Criterion 8 — producer liveness on the mini**

The criterion is verified here, not created. Four claims, all checked on disk on
the production host, none from code:

```bash
ssh macmini 'launchctl list | grep -c com.livewire.coverage; \
  ls -la ~/market-warehouse/logs/coverage_*.log | tail -3; \
  ls -d ~/market-warehouse/data-lake/raw/massive/us_stocks_sip/minute_aggs_v1/date=* | wc -l; \
  ls -d ~/market-warehouse/data-lake/raw/massive/us_stocks_sip/minute_aggs_v1/date=* | tail -1'
```

Expected: the job is loaded (count 1); the newest coverage log is under three
days old; the raw partition count covers the terminus window; **and the newest
partition is recent** — a stale tape makes every symbol look like a terminus at
once, which is the failure mode that would page the whole universe. If the
coverage job is **not** loaded, stop — every conclusion in this plan about
"coverage already runs this at 11:00 UTC" is false and the plan needs a
scheduling task.

The producers the due rule depends on are the ones issue #94 leaves unbudgeted;
this step measures their **output**, which is the only thing this plan can
assert without fixing #94.

- [ ] **Step 5: Measure the added cost**

`scan_findings` does a column read per registry series, on top of coverage's
footer pass. Time it:

```bash
uv run python -c "
import time; from datetime import UTC, date, datetime; from pathlib import Path
from livewire_scripts.coverage_report import scan_findings
t=time.time(); f=scan_findings(date(2026,8,31), bronze_root=Path.home()/'market-warehouse/data-lake/bronze', as_of=datetime.now(UTC))
print(len(f), round(time.time()-t,1), 's')"
```

Record the number. If it exceeds ~300s, stop and narrow the window rather than
guessing a timeout — `CLAUDE.md` records four separate budgets that were guessed
and expired.

- [ ] **Step 6: Record the results**

Append a `## Verification (post-implementation)` section to
`docs/audits/2026-09-01-terminus-vs-no-trade.md` with every command's actual
output, the chosen `MIN_TERMINUS_SESSIONS`, and the measured `scan_findings`
wall clock. If any expectation failed, stop and report — do not adjust the
assertion to match the output.

- [ ] **Step 7: Commit**

```bash
git add docs/audits/2026-09-01-terminus-vs-no-trade.md clients/terminus.py
git commit -m "docs(gap): record the production calibration for MIN_TERMINUS_SESSIONS and the scan cost"
```

---

## Out of scope — do not do these here

- **Issue #94** (three lane wrappers drop the job deadline; the Done marker
  reports the start time). Separate cause, separate file, separate PR. Spec §5
  records that this plan's due rule rests on a guarantee the code does not yet
  provide, and that is deliberate.
- **Issue #89** (deprecated Massive reference endpoints). Unrelated.
- **Tier A execution wiring to `shepherd_repair`** (spec §11 criterion 1). The
  manifest is written by Task 7 and nothing consumes it. That is the next
  change, and its input is the queue this plan produces.
- **Giving the `non-equity 1d:` log line a reader.** Verified 2026-09-01: no
  parser in the repo reads it — not `status.py`, not
  `weekly_quality_summary.py`, not the digest. Task 5 widens a line nobody
  grades. Wiring it into `status.collect()` is a real gap and a separate change;
  it is named here so it is not lost.
- **Widening the equity registry row to spec §8's five presets**
  (`sp500, ndx100, r2k, etfs, adrs`). The row stays at `sp500 + ndx100` because
  that is the universe the terminus threshold was measured on. Widening it needs
  a calibration run at the wider scale first — Task 9 step 1, re-run over the
  wider member set — or it reproduces the 96.6% false-positive shape on the
  illiquid tail.
- **A per-asset-class trading calendar.** `clients/gap_registry.py:25-30` records
  the XNYS blind spot for fx, CME futures and FRED. Task 5 adds two classes that
  inherit it, deliberately and with a test naming it.
- **Installing any `launchd` job.** `com.livewire.coverage` at 11:00 UTC already
  runs after the ingestion deadline and already runs this code path. Spec §11
  criterion 8 is *verified* in Task 9 step 4, not created — and if that check
  finds the job unloaded, stop and re-plan rather than installing it here.
- **Scheduling `universe_sync` / `shepherd_universe`** (spec §10 deliverable 4).
  Its template is kept, not installed; it needs `MASSIVE_API_KEY` and §4.3's
  producer run, and it is the first follow-on after this plan.
- **G2 and G13.** Named in the taxonomy, not emitted. Reinstating either needs a
  measurement asking for it.

  ⚠️ **This one is genuinely contested and is recorded, not settled.** The
  cross-model review argued that G2 should stay for `1d`: a missing *daily* bar
  bounded by present bars is not the circular 5m question (SIP emits a daily bar
  whenever there was a trade, and the raw traded set answers "was there a trade"
  independently), and the spike's "zero G2 findings" came from a 4-symbol
  residue after 497 phantoms were removed — a sample too small to retire a
  branch on. The counter-argument is spec §10 deliverable 2: an unexercised
  branch should not ship. **Resolution: retire G2 in this plan as written, and
  re-measure it as the first follow-on** — run the Task 9 step 1 harness with
  the interior branch restored over the 515-member universe and a 90-day window,
  and reinstate G2 if it produces a non-empty, non-noise result. `classify`'s
  signature does not change either way, so this costs nothing to defer.
