# Gap Auto-Heal Phase 1 Consolidation — Implementation Plan

> **For agentic workers:** execute this plan with the repository owner's
> `/execute-plan` skill, task by task, in order. Steps use checkbox (`- [ ]`)
> syntax for tracking. Do **not** use `superpowers:subagent-driven-development`
> or any parallel-dispatch pattern — the repository owner's global `CLAUDE.md`
> forbids it.

**Goal:** Make "a bar we should have is not on disk" answerable by exactly one
detector, whose denominator comes from the registry rather than from disk, and
which can tell a no-trade day apart from an instrument that left the tape.

**Architecture:** `clients/coverage_denominator.py` already builds
`presets × trading_calendar × timeframe`. This plan gives it an ingestion-deadline
rule, adds a pure terminus test over the raw traded sets coverage already reads,
teaches `clients/gap_engine.py` the terminus class, and rewires
`livewire_scripts/coverage_report.py` onto both — then deletes
`livewire_scripts/gap_scan.py`, its two `launchd` templates and its subcommand.
Net change to the script and job count is negative. No new scheduled job:
`com.livewire.coverage` at 11:00 UTC already runs after the ingestion deadline.

**Tech Stack:** Python 3.13, `uv` exclusively (`uv run pytest`), `pyarrow.parquet`
for footer and column reads, stdlib `datetime`/`json`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md`
(revised in `7aac492` and `902ac8f`).
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
- **No synthetic market data.** Test fixtures use real tickers at real frozen
  values with an as-of date. A `_symbols.parquet` fixture is a list of real
  tickers and is fine; invented prices are not.
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
| `registry/gaps.json` | six rows, now G1/G3/G14 | modify |
| `livewire_scripts/coverage_report.py` | **the one reporting surface**; registry denominator, terminus exclusion, Tier A manifest, Tier B queue | modify |
| `livewire_scripts/gap_scan.py` | — | **delete** |
| `launchd/com.livewire.gap-scan.plist.example` | — | **delete** |
| `launchd/com.livewire.universe-refresh.plist.example` | — | **delete** |
| `scripts/livewire_quality.py` | subcommand table | modify (remove one line) |
| `tests/test_coverage_denominator.py` | due rule | modify |
| `tests/test_terminus.py` | suffix test | **create** |
| `tests/test_gap_engine.py` | G14, tier honesty, no G2/G13 | modify |
| `tests/test_coverage_report.py` | registry universe, terminus exclusion, log surface | modify |
| `tests/test_gap_scan.py`, `tests/test_gap_scan_integration.py` | — | **delete** |

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
  - `session_due_at(session: date) -> datetime` (tz-aware UTC)
  - `build_denominator(preset_paths: list[Path], asset_class: str, timeframe: str, start: date, end: date, as_of: datetime) -> list[ExpectedSeries]` — **`as_of` changes from `date` to `datetime`**; every caller must be updated (Task 6 and the tests here).

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


def session_due_at(session: date) -> datetime:
    """The instant session *session* is due on disk.

    ponytail: reuses MDW_DAILY_JOB_DEADLINE_SECONDS rather than introducing a
    second constant to keep in step. "Closed" is not "delivered" -- the job that
    fills S starts 06:00 UTC on S+1, and a denominator that expects S the moment
    it closes manufactures one tail gap per symbol in the universe.
    """
    seconds = int(os.environ.get("MDW_DAILY_JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS))
    start = datetime.combine(session + timedelta(days=1), JOB_START_UTC, tzinfo=UTC)
    return start + timedelta(seconds=seconds)
```

Change the signature `as_of: date` to `as_of: datetime` and replace the filter:

```python
    if as_of.tzinfo is None:
        raise ValueError("as_of must be tz-aware; a naive local datetime silently shifts the due rule")
    # A session is expected only once the job that fills it was due to finish.
    # Guarding on `d < as_of.date()` instead would reintroduce the 497 phantoms:
    # closing is not delivery.
    sessions = tuple(d for d in trading_dates_in_range(start, end) if session_due_at(d) <= as_of)
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
G1/G3/G14. A row that claims a check it does not run is the registry-side version
of the disk-glob failure this engine replaces.

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
def test_every_row_declares_exactly_the_gaps_denominator_diff_emits():
    # denominator_diff emits G1, G3 and G14 and nothing else. A row naming G2
    # would promise a check that classify() no longer performs.
    for row in load_registry(Path("registry/gaps.json")):
        assert set(row.gap) == {"G1", "G3", "G14"}, row.id
```

- [ ] **Step 2: Run and verify it fails**

Run: `uv run pytest tests/test_gap_registry_contract.py -v`
Expected: FAIL — rows still declare `G2`.

- [ ] **Step 3: Edit the six rows**

In `registry/gaps.json`, for **each of the six rows**, replace the `gap` array
and the `id` prefix:

```json
    "id": "g1-g3-g14-equity-daily",
    "gap": [
      "G1",
      "G3",
      "G14"
    ],
```

Row ids in order: `g1-g3-g14-equity-daily`, `g1-g3-g14-rates-daily`,
`g1-g3-g14-fx-daily`, `g1-g3-g14-cmdty-daily`, `g1-g3-g14-volatility-daily`,
`g1-g3-g14-futures-daily`. Leave `asset_class`, `timeframe`, `universe`,
`check`, `params` and `since` untouched. Set every row's `test` to
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
- Consumes: `build_denominator` (Task 1), `load_registry` (existing).
- Produces: `compute_non_equity_coverage(target_date: date, bronze_root: Path | None = None, registry_path: Path | None = None) -> dict[str, CoverageResult]`, keyed by asset class, now including `"fx"` and `"cmdty"`.
- `format_non_equity_line` renders the same `<date> non-equity 1d: <ac>=<p>/<t> …`
  shape; only the number of terms changes.

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
) -> dict[str, CoverageResult]:
    """Return per-asset-class 1d freshness for the non-equity universes.

    The denominator is the registry universe, never the files on disk: a symbol
    that never landed has to stay countable. No no-trade exemption -- these are
    small, continuously-quoted universes and a stale one is a real gap.
    """
    bronze_root = bronze_root or _resolved_data_lake() / "bronze"
    presets_dir = Path("presets")
    results: dict[str, CoverageResult] = {}
    for row in _non_equity_rows(registry_path):
        expected = build_denominator(
            [presets_dir / f"{name}.json" for name in row.universe],
            row.asset_class,
            "1d",
            target_date,
            target_date,
            as_of=session_due_at(target_date),
        )
        universe = {series.symbol for series in expected}
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


NON_EQUITY_ASSET_CLASSES: tuple[str, ...] = ("volatility", "futures", "rates", "fx", "cmdty")
```

Change `format_non_equity_line` to iterate the results rather than the constant,
so a registry row added later renders without a second edit:

```python
def format_non_equity_line(target_date: date, results: dict[str, CoverageResult]) -> str:
    parts = [f"{ac}={results[ac].present}/{results[ac].total}" for ac in sorted(results)]
    return f"{target_date} non-equity 1d: " + " ".join(parts)
```

Add to the imports at the top of the file:

```python
from clients.coverage_denominator import build_denominator, session_due_at
from clients.gap_registry import load_registry
```

- [ ] **Step 4: Run and verify it passes**

Run: `uv run pytest tests/test_coverage_report.py -v`
Expected: PASS. `status.py` and the digest read the `non-equity 1d:` line by
prefix and parse `ac=p/t` terms, so two extra terms are additive — confirm with
`uv run pytest tests/test_status.py tests/test_nightly_digest.py -v`.

- [ ] **Step 5: Format, lint, commit**

```bash
uv run ruff format . && uv run ruff check .
git add livewire_scripts/coverage_report.py tests/test_coverage_report.py
git commit -m "fix(coverage): non-equity denominator comes from the registry, and fx/cmdty stop being invisible"
```

---

### Task 6: Equity 1d — registry denominator, terminus exclusion, one reporting surface

The equity `1d` universe is `on_disk`, so BK — an `sp500.json` member with no
`1d.parquet` — is undetectable by construction. Union in the registry
denominator, and route terminus symbols out of `missing` into their own reported
class so the count stays honest in both directions.

**Do not touch the intraday branch.** `universe = set(traded_today)` for
intraday is already provider-derived and correct; the footer cache, the thread
pool and the recovery path stay exactly as they are.

**Files:**
- Modify: `livewire_scripts/coverage_report.py:240-334`
- Test: `tests/test_coverage_report.py`

**Interfaces:**
- Consumes: `build_denominator`, `session_due_at` (Task 1); `terminus_of`,
  `traded_by_session`, `RAW_MINUTE_AGGS` (Task 2).
- Produces:
  - `CoverageResult` gains `terminus_symbols: tuple[tuple[str, date], ...] = ()` — `(symbol, terminus date)` pairs, so the log line can name the date without a second lookup.
  - `compute_coverage(target_date, bronze_root=None, cache_path=None, registry_path=None)`.
  - `format_terminus_block(results) -> list[str]` returning at most one
    `  1d terminus: <sym>@<date>, …` line.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_coverage_report.py`:

```python
def test_a_preset_member_with_no_parquet_is_counted_missing(tmp_path):
    # BK, measured 2026-09-01: an sp500 member with no 1d.parquet. The disk-glob
    # denominator cannot express this symbol at all.
    bronze = _bronze_with(tmp_path, {"AAPL": date(2026, 8, 28)})
    _write_tape(bronze.parent / "raw/massive/us_stocks_sip/minute_aggs_v1",
                date(2026, 8, 28), ["AAPL", "BK"])
    results = compute_coverage(date(2026, 8, 28), bronze_root=bronze,
                               registry_path=_registry_for(tmp_path, ["AAPL", "BK"]))
    assert "BK" in results["1d"].missing_symbols


def test_a_terminus_symbol_is_reported_separately_and_not_as_missing(tmp_path):
    # EQR left the tape. Counting it missing puts an unrepairable job in the
    # recovery path; counting it present is what hid it for weeks. It is neither.
    bronze = _bronze_with(tmp_path, {"AAPL": date(2026, 8, 28), "EQR": date(2026, 8, 17)})
    raw = bronze.parent / "raw/massive/us_stocks_sip/minute_aggs_v1"
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 28)):
        _write_tape(raw, session, ["AAPL"])
    results = compute_coverage(date(2026, 8, 28), bronze_root=bronze,
                               registry_path=_registry_for(tmp_path, ["AAPL", "EQR"]))
    assert "EQR" not in results["1d"].missing_symbols
    assert "EQR" in dict(results["1d"].terminus_symbols)


def test_a_one_day_absence_is_still_exempted_as_no_trade(tmp_path):
    # The exemption stays load-bearing. Without it the interior scan flags 96.6%
    # of the universe, and this test is the guard on that.
    bronze = _bronze_with(tmp_path, {"AAPL": date(2026, 8, 28), "SLND": date(2026, 8, 27)})
    raw = bronze.parent / "raw/massive/us_stocks_sip/minute_aggs_v1"
    for session in _sessions(date(2026, 8, 18), date(2026, 8, 27)):
        _write_tape(raw, session, ["AAPL", "SLND"])
    _write_tape(raw, date(2026, 8, 28), ["AAPL"])
    results = compute_coverage(date(2026, 8, 28), bronze_root=bronze,
                               registry_path=_registry_for(tmp_path, ["AAPL", "SLND"]))
    assert "SLND" not in results["1d"].missing_symbols
    assert results["1d"].terminus_symbols == ()
```

`tests/test_coverage_report.py` **already has** `_write_daily(bronze_root,
symbol, dates)` (`:74`) and `_write_raw_symbols(bronze_root, target, symbols)`
(`:121`). Use those — do not add `_bronze_with` or `_write_tape`. The tests above
are written against them; adapt the calls rather than duplicating the fixtures,
whose frozen closes are real prices at a recorded as-of date.

Add exactly one new helper, because nothing writes a registry today:

```python
def _registry_for(tmp_path: Path, tickers: list[str]) -> Path:
    """A one-row registry plus the preset it names, both under tmp_path.

    The preset lives in a `presets/` subdirectory because the production code
    resolves preset paths relative to the repo root; the test passes an absolute
    registry path and monkeypatches nothing else.
    """
    presets = tmp_path / "presets"
    presets.mkdir(exist_ok=True)
    (presets / "t.json").write_text(json.dumps({"name": "t", "tickers": tickers}))
    registry = tmp_path / "gaps.json"
    registry.write_text(json.dumps([{
        "id": "g1-g3-g14-equity-daily",
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
```

`_equity_preset_paths` resolves `presets/<name>.json` relative to the process
CWD, so for the test to find `tmp_path/presets/t.json` the helper must take the
presets directory too. Change its signature to
`_equity_preset_paths(registry_path: Path | None, presets_dir: Path | None = None)`
and thread `presets_dir` through `compute_coverage`, defaulting to
`Path("presets")`. Do the same in `compute_non_equity_coverage` from Task 5 —
its `presets_dir = Path("presets")` is currently a hardcoded local, which makes
that function untestable outside the repo root, the exact trap
`repair-legacy-basis` already hit (`CLAUDE.md`: *"--presets-dir defaults to a
cwd-relative Path("presets"), so --priority-only elsewhere used to silently
repair zero symbols and exit 0"*).

- [ ] **Step 2: Run and verify they fail**

Run: `uv run pytest tests/test_coverage_report.py -k "preset_member or terminus or no_trade" -v`
Expected: FAIL — `compute_coverage() got an unexpected keyword argument 'registry_path'`.

- [ ] **Step 3: Implement**

Add the field to `CoverageResult`:

```python
    # (symbol, terminus date) pairs. Neither missing nor present: no source can
    # supply bars for an instrument that stopped printing, so counting these
    # missing queues an impossible repair and counting them present is what hid
    # BK, EA, AVB and EQR.
    terminus_symbols: tuple[tuple[str, date], ...] = ()
```

In `compute_coverage`, add the parameter and, immediately after `traded_today`
is computed, build the terminus map once for the whole run:

```python
    # One window, read once, shared by every timeframe. The 20-session window is
    # what the 2026-09-01 measurement used; MIN_TERMINUS_SESSIONS decides inside
    # it.
    # 40 calendar days is comfortably more than 20 sessions including holidays;
    # the slice, not the span, defines the window.
    window = trading_dates_in_range(target_date - timedelta(days=40), target_date)[-TERMINUS_WINDOW_SESSIONS:]
    tape = traded_by_session(bronze_root.parent / RAW_MINUTE_AGGS, window)
```

Replace the `1d` universe line:

```python
        # The registry, not the disk. A symbol that never landed has no file to
        # glob, which is why BK -- an sp500 member -- read as 100% healthy.
        if tf == "1d":
            expected = build_denominator(
                _equity_preset_paths(registry_path),
                "equity", "1d", target_date, target_date,
                as_of=session_due_at(target_date),
            )
            universe = on_disk | {series.symbol for series in expected}
        else:
            universe = on_disk if not traded_today else set(traded_today)
```

Replace the `present_symbols` / `missing` block:

```python
        terminus = {}
        if tf == "1d":
            terminus = {s: t for s in universe if (t := terminus_of(tape, s)) is not None}
        present_symbols = {
            symbol
            for symbol in universe
            if (latest_by_symbol.get(symbol) or date.min) >= target_date
            or (tf == "1d" and traded_today and symbol not in traded_today)
        }
        # A terminus is neither present nor missing: it is a decision request.
        # Leaving it in `missing` would trip the safety cap and hand the recovery
        # subprocess a symbol no provider carries.
        missing = sorted(universe - present_symbols - set(terminus))
        results[tf] = CoverageResult(
            timeframe=tf,
            total=len(universe),
            present=len(present_symbols),
            missing_symbols=missing,
            terminus_symbols=tuple(sorted(terminus.items())),
        )
```

Add the helper and the log block:

```python
def _equity_preset_paths(registry_path: Path | None) -> list[Path]:
    rows = load_registry(registry_path or Path("registry/gaps.json"))
    names: list[str] = []
    for row in rows:
        if row.asset_class == "equity" and row.timeframe == "1d":
            names.extend(row.universe)
    return [Path("presets") / f"{name}.json" for name in dict.fromkeys(names)]


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

`previous_trading_day(d)` takes a single argument — it cannot step back N
sessions, which is why the window is sliced rather than computed from it.

Call `format_terminus_block(results)` wherever `format_missing_blocks(results)`
is already appended to the log, immediately after it.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest tests/ -v`
Expected: PASS. Pay attention to `tests/test_status.py`,
`tests/test_weekly_quality_summary.py` and `tests/test_nightly_digest.py` — all
three parse this log surface. The `coverage:` and `non-equity 1d:` lines are
unchanged in shape; only the `1d=<p>/<t>` totals move, which is the intended
one-time step.

- [ ] **Step 5: Format, lint, commit**

```bash
uv run ruff format . && uv run ruff check .
git add livewire_scripts/coverage_report.py tests/test_coverage_report.py
git commit -m "fix(coverage): registry denominator for equity 1d, and a terminus is neither present nor missing"
```

---

### Task 7: Retire the fourth detector

Spec §11 criterion 7 is measured by deletion. `gap_scan.py` answers the same
question as `coverage_report.py` with a different denominator and carries neither
the no-trade exemption nor the ingestion deadline; on the full 14,811-symbol
universe it would reproduce the 96.6% interior-scan disease.

Move the two output writers first, delete second — in that order, so the Tier A
manifest and the Tier B decision queue (spec §10 deliverables 5 and 8) survive
the deletion.

**Files:**
- Modify: `livewire_scripts/coverage_report.py`, `scripts/livewire_quality.py:24`
- Delete: `livewire_scripts/gap_scan.py`, `launchd/com.livewire.gap-scan.plist.example`, `tests/test_gap_scan.py`, `tests/test_gap_scan_integration.py`
- **Keep** `launchd/com.livewire.universe-refresh.plist.example` — see the note below
- Test: `tests/test_coverage_report.py`

**Interfaces:**
- Consumes: `Finding` (Task 3), `CoverageResult` (Task 6).
- Produces: `write_tier_a_manifest(findings, path)` and
  `write_decision_requests(findings, path)` on `coverage_report`, with the same
  JSON shape `gap_scan` wrote, so an existing consumer of those files does not
  have to change.

- [ ] **Step 1: Copy the two writers into `coverage_report.py`**

Copy `write_tier_a_manifest` and `write_decision_requests` verbatim from
`livewire_scripts/gap_scan.py` into `livewire_scripts/coverage_report.py`,
together with the `_urgency` sort key. Copy their tests from
`tests/test_gap_scan.py` into `tests/test_coverage_report.py` unchanged except
for the import line.

- [ ] **Step 2: Run the copied tests**

Run: `uv run pytest tests/test_coverage_report.py -v`
Expected: PASS.

- [ ] **Step 3: Delete**

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

- [ ] **Step 4: Write the convergence guard**

Append to `tests/test_coverage_report.py`:

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
```

- [ ] **Step 5: Run the full suite and the coverage gate**

```bash
uv run pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: PASS with the 95% gate satisfied. `clients/gap_engine.py` lost two
branches, so its covered fraction should rise, not fall.

- [ ] **Step 6: Format, lint, commit**

```bash
uv run ruff format . && uv run ruff check .
git add -A
git commit -m "refactor(gap): retire gap_scan and its two launchd templates — one engine, not a fourth detector"
```

---

### Task 8: Calibrate and verify against the production lake

The only acceptance criteria that matter here cannot be met by a test suite. Spec
§11 criteria 9–11 name their regression cases; this task runs them on the real
lake. **Read-only. Nothing in this task writes the data lake.**

**Files:**
- Create: `docs/audits/2026-09-01-terminus-vs-no-trade.md` — append a
  "Verification" section (the file already exists from the spike)

**Interfaces:**
- Consumes: everything above.
- Produces: measured values for `MIN_TERMINUS_SESSIONS` and the criterion-9
  false-positive count, recorded in the audit note.

- [ ] **Step 1: Criterion 9 — terminus separation, zero false positives**

On the production host, over the `sp500 + ndx100` universe (515 members) and the
trailing 20 sessions, assert the positive set is exactly `{BK, EA, AVB, EQR}` and
the negative set (the other 511) produces nothing:

```bash
ssh macmini 'cd ~/market-warehouse && ./.venv/bin/python - ' <<'PY'
import json, os
from pathlib import Path
from clients.terminus import terminus_of, traded_by_session
from clients.trading_calendar import trading_dates_in_range
from datetime import date

lake = Path(os.path.expanduser("~/market-warehouse/data-lake"))
raw = lake / "raw/massive/us_stocks_sip/minute_aggs_v1"
repo = Path(os.path.expanduser("~/market-warehouse/current"))
members = set()
for name in ("sp500", "ndx100"):
    members |= set(json.loads((repo / "presets" / f"{name}.json").read_text())["tickers"])
window = trading_dates_in_range(date(2026, 8, 4), date(2026, 8, 31))
tape = traded_by_session(raw, window)
for n in (2, 3, 5, 8):
    hits = {s: terminus_of(tape, s, min_sessions=n) for s in members}
    hits = {s: t for s, t in hits.items() if t}
    print(f"min_sessions={n}: {len(hits)} findings -> {sorted(hits)}")
PY
```

Expected at every value tested: exactly `['AVB', 'BK', 'EA', 'EQR']`. If a value
produces a fifth symbol, that value is too low — record which, and raise
`MIN_TERMINUS_SESSIONS` to the lowest value that still yields exactly four.
**PYTHONPATH must point at the checkout that has `clients.terminus`** — the
served release does not carry this code yet, so prefix
`PYTHONPATH=/path/to/checkout`.

- [ ] **Step 2: Criterion 11 — no phantom tail gaps before the deadline**

Run the equity `1d` denominator twice against the same session, once before and
once after that session's due time, and assert the session appears only in the
second:

```bash
uv run python - <<'PY'
from datetime import UTC, date, datetime
from pathlib import Path
from clients.coverage_denominator import build_denominator

for hour in (4, 11):
    s = build_denominator([Path("presets/sp500.json")], "equity", "1d",
                          date(2026, 8, 27), date(2026, 8, 31),
                          as_of=datetime(2026, 9, 1, hour, 0, tzinfo=UTC))
    print(hour, date(2026, 8, 31) in s[0].sessions)
PY
```
Expected: `4 False` then `11 True`.

- [ ] **Step 3: Criterion 2 — BK is still detected**

The one result that justifies replacing the denominator. Run
`compute_coverage` for a recent session against the production lake with
`--no-recover` and confirm `BK` appears in the `1d terminus:` line (it is a
terminus, not a plain missing symbol, which is the corrected classification).

- [ ] **Step 4: Criterion 8 — producer liveness on the mini**

The criterion is verified here, not created. Three claims, each checked on disk
on the production host and none from code:

```bash
ssh macmini 'launchctl list | grep -c com.livewire.coverage; \
  ls -la ~/market-warehouse/logs/coverage_*.log | tail -3; \
  ls -d ~/market-warehouse/data-lake/raw/massive/us_stocks_sip/minute_aggs_v1/date=* | wc -l'
```

Expected: the job is loaded (count 1); the newest coverage log is under three
days old; the raw partition count covers the terminus window. If the coverage job
is **not** loaded, stop — every conclusion in this plan about "coverage already
runs this at 11:00 UTC" is false and the plan needs a scheduling task.

Note the timestamp trap from issue #94 does not apply to `com.livewire.coverage`
— it is a separate job, not a lane inside `run-daily-job`.

- [ ] **Step 5: Record the results**

Append a `## Verification (post-implementation)` section to
`docs/audits/2026-09-01-terminus-vs-no-trade.md` with the three commands' actual
output and the chosen `MIN_TERMINUS_SESSIONS`. If any expectation failed, stop
and report — do not adjust the assertion to match the output.

- [ ] **Step 6: Commit**

```bash
git add docs/audits/2026-09-01-terminus-vs-no-trade.md clients/terminus.py
git commit -m "docs(gap): record the production calibration for MIN_TERMINUS_SESSIONS"
```

---

## Out of scope — do not do these here

- **Issue #94** (three lane wrappers drop the job deadline; the Done marker
  reports the start time). Separate cause, separate file, separate PR. Spec §5
  records that this plan's due rule rests on a guarantee the code does not yet
  provide, and that is deliberate.
- **Issue #89** (deprecated Massive reference endpoints). Unrelated.
- **Tier A execution wiring to `shepherd_repair`** (spec §11 criterion 1). The
  manifest is written; nothing consumes it. That is the next change, and its
  input is the queue this plan produces.
- **Installing any `launchd` job.** `com.livewire.coverage` at 11:00 UTC already
  runs after the ingestion deadline and already runs this code path. Spec §11
  criterion 8 is *verified* in Task 8 step 4, not created — and if that check
  finds the job unloaded, stop and re-plan rather than installing it here.
- **Scheduling `universe_sync` / `shepherd_universe`** (spec §10 deliverable 4).
  Its template is kept, not installed; it needs `MASSIVE_API_KEY` and §4.3's
  producer run, and it is the first follow-on after this plan.
- **G2 and G13.** Named in the taxonomy, not emitted. Reinstating either needs a
  measurement asking for it.
