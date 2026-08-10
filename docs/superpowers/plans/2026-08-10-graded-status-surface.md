# Graded Status Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every status item a verdict and a paste-able fix command, so a catastrophe never renders like a routine line again.

**Architecture:** One assessment layer (`livewire_scripts/status.py`) producing `list[Section]`, and two renderers over it — a new `livewire_ops.py status` terminal table and the existing nightly digest email. The six section parsers currently living in `nightly_digest.py` move into `status.py` unchanged; three new checks (launchd, undelivered alerts, DuckDB catalog) are added for sources the digest cannot reach today.

**Tech Stack:** Python 3.13, `rich` (already a dependency), `pytest`, `pyright`, `ruff`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-10-graded-status-surface-design.md`

## Global Constraints

- **No new dependencies.** `rich` is already in `pyproject.toml`; use it. Nothing else gets added.
- **`livewire_scripts/status.py` must NEVER `import duckdb`.** `tests/test_duckdb_containment.py` allows exactly two modules (`clients/duckdb_catalog.py`, `livewire_scripts/duckdb_catalog_cli.py`). Do not add an entry to `ALLOWED_DUCKDB_IMPORTERS`. Reach DuckDB only through a function on `clients/duckdb_catalog.py`.
- **Coverage gate is 95%** (`fail_under = 95` in `pyproject.toml`). Every new branch needs a test.
- **All external I/O is mocked in tests** — no `launchctl`, no real filesystem outside `tmp_path`, no network.
- **`collect()` never raises.** Every check catches its own exceptions and degrades to `Verdict.UNKNOWN`.
- **`UNKNOWN` is never `OK`.** A missing input is a failure to measure. This is the defect being fixed; do not let a "(not found)" render green.
- **`status` always exits 0.** Do not add a nonzero-on-BAD contract.
- **No `--json` flag.** Out of scope.
- **Never scan parquet.** Every check must be milliseconds. If a check needs footer statistics, it reads the DuckDB coverage table instead.
- Commit messages: no `Co-Authored-By` trailer, no AI attribution. Never `git add -A`; stage explicit paths. Never `--no-verify`.
- Run before every commit: `uv run ruff check . && uv run ruff format --check . && uv run pytest tests/ -m "not integration" -q`

---

## File Structure

| File | Responsibility |
|---|---|
| `livewire_scripts/status.py` (create) | `Verdict`, `Section`, the nine check functions, `collect()`, the terminal renderer, `main()` |
| `livewire_scripts/nightly_digest.py` (modify) | Shrinks to `build_digest` + `_send_email` + `main`; imports sections from `status` |
| `clients/duckdb_catalog.py` (modify) | Gains `coverage_headline()` — the only DuckDB read `status` is allowed |
| `scripts/livewire_ops.py` (modify) | Registers the `status` subcommand |
| `tests/test_status.py` (create) | Tests for every verdict rule |
| `tests/test_nightly_digest.py` (modify) | Updated for the verdict prefix |
| `CLAUDE.md` (modify) | Documents the invariants |

---

### Task 1: Move the six section parsers into `status.py`, unchanged

A pure move. Behaviour must be byte-identical so the existing digest tests pass untouched — that is the gate on this task.

**Files:**
- Create: `livewire_scripts/status.py`
- Modify: `livewire_scripts/nightly_digest.py`
- Test: `tests/test_nightly_digest.py` (must pass with NO edits)

**Interfaces:**
- Consumes: nothing.
- Produces: `status._read_text(path) -> str | None`, `status._outcomes_section(run_date: str, log_dir: Path) -> list[str]`, `status._phases_section(run_date: str, log_dir: Path) -> list[str]`, `status._silver_section(run_date: str, log_dir: Path) -> list[str]`, `status._quality_jobs_section(run_date: str, log_dir: Path) -> list[str]`, `status._coverage_section(run_date: str, log_dir: Path) -> list[str]`, `status._disk_section(data_lake: Path, warehouse: Path | None = None) -> list[str]`. Also the module constants `_MIN_FREE_GB`, `_GIB`, `_COVERAGE_STALE_DAYS`, `_QUALITY_WARNING_RE`.

- [ ] **Step 1: Run the existing digest tests to capture the green baseline**

Run: `uv run pytest tests/test_nightly_digest.py -q`
Expected: PASS. Note the count — the same count must pass at the end of this task.

- [ ] **Step 2: Create `livewire_scripts/status.py` with the moved code**

Cut lines 41–238 of `livewire_scripts/nightly_digest.py` (`_read_text` through `_disk_section`) and paste them into the new module, along with the constants `_MIN_FREE_GB`, `_GIB`, `_COVERAGE_STALE_DAYS` and `_QUALITY_WARNING_RE`. Do not change a single line of their bodies.

```python
#!/usr/bin/env python3
"""Assess Livewire's operational state and grade every item.

The reports this replaces stated facts and never judged them: warehouse-wide
zero coverage rendered in the same font as a routine trim, and "(not found)"
— meaning the run's log could not be located at all — read exactly like a
healthy line. Every check here carries a verdict and, when it is not OK, the
command that addresses it.

Nothing here scans parquet. `coverage` costs 1400-2860s and `warehouse` reads
footers; this module reads only what the nightly jobs already produced, and
grades how old that is as its own signal.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from datetime import date
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from livewire_scripts.daily_outcomes import parse_all_summary_json, parse_last_summary_json

_MIN_FREE_GB = float(os.getenv("MDW_FLATFILE_MIN_FREE_GB", "25"))
_GIB = 1024**3
#: Coverage is daily and the digest reads yesterday's by design, so 3 absorbs
#: one missed run without absorbing a job that has stopped firing entirely.
_COVERAGE_STALE_DAYS = 3

# ... the six moved functions and _QUALITY_WARNING_RE, verbatim ...
```

- [ ] **Step 3: Re-point `nightly_digest.py` at the moved functions**

Delete the moved bodies and their now-unused imports (`re`, `shutil` stays — `main()` uses `shutil.which`), then import them:

```python
from livewire_scripts.status import (
    _coverage_section,
    _disk_section,
    _outcomes_section,
    _phases_section,
    _quality_jobs_section,
    _silver_section,
)
```

`build_digest` is unchanged.

- [ ] **Step 4: Run the digest tests — the same count must still pass**

Run: `uv run pytest tests/test_nightly_digest.py -q`
Expected: PASS, identical count to Step 1. Any failure means the move was not faithful.

Two things that look like they should break and do not — **do not "fix" them in this task**:
- Eleven tests call `nightly_digest._disk_section(...)` / `nightly_digest._coverage_section(...)` directly. The imported names are still module attributes, and they still return `list[str]`, so the calls resolve. Task 2 is where they change.
- `monkeypatch.setattr(nightly_digest.shutil, "disk_usage", ...)` still works: `nightly_digest.shutil` and `status.shutil` are the same cached module object, and the patch lands on that object.

- [ ] **Step 5: Run the full gate**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pytest tests/ -m "not integration" -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/status.py livewire_scripts/nightly_digest.py
git commit -m "refactor: move the digest's section parsers into a status module

A pure move ahead of grading them. nightly_digest is about sending an email;
deciding whether the warehouse is healthy is a separate job, and a second
consumer (a terminal command) is about to need the same parsers."
```

---

### Task 2: Give the six sections verdicts

**Files:**
- Modify: `livewire_scripts/status.py`
- Modify: `livewire_scripts/nightly_digest.py:build_digest`
- Test: `tests/test_status.py` (create), `tests/test_nightly_digest.py`

**Interfaces:**
- Consumes: the six `_*_section` functions from Task 1.
- Produces: `status.Verdict` (enum: `OK`, `UNKNOWN`, `WARN`, `BAD`, ordered worst-last, with `.glyph` and `.style`), `status.Section` (frozen dataclass: `name: str`, `verdict: Verdict`, `lines: list[str]`, `fix: str | None = None`), `status._safe(name: str, builder) -> Section`, `status.collect(run_date: date, log_dir: Path, data_lake: Path, *, runner=subprocess.run, database: Path | None = None) -> list[Section]`. All six section functions now return `Section` instead of `list[str]`, and `nightly_digest.build_digest` consumes `collect()`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_status.py`:

```python
"""Tests for the graded status surface."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from livewire_scripts.status import (
    Section,
    Verdict,
    _coverage_section,
    _disk_section,
    _outcomes_section,
    _phases_section,
    _quality_jobs_section,
    _silver_section,
)

_SILVER = (
    'SUMMARY_JSON {"revision":25,"rebuilt":2,"unchanged":13076,"trimmed":254,'
    '"failed":233,"window_regressions":39}'
)
_EQUITY_OK = (
    'SUMMARY_JSON {"job":"daily_update","asset_class":"equity","source":"ib",'
    '"target_date":"2026-08-07","updated":13000,"no_trade":277,"partial":0,'
    '"errors":0,"bars_inserted":13000,"validation_issues":0,"top_errors":[]}'
)


def test_a_missing_run_log_is_bad_not_ok(tmp_path: Path) -> None:
    """The defect being fixed: '(not found)' used to render like a healthy line.
    A log that does not exist means the run appears never to have happened."""
    section = _outcomes_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.BAD
    assert section.fix is not None


def test_a_log_with_no_summary_is_unknown(tmp_path: Path) -> None:
    """Distinct from the above: the job ran and wrote something, but produced
    no machine-readable outcome. Cannot measure is not the same as did not run."""
    (tmp_path / "daily_update_2026-08-10.log").write_text("starting...\n", encoding="utf-8")
    section = _outcomes_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.UNKNOWN
    assert section.verdict is not Verdict.OK


def test_outcomes_with_no_errors_is_ok(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(_EQUITY_OK, encoding="utf-8")
    assert _outcomes_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_a_total_wipeout_is_bad_and_one_flaky_symbol_is_only_warn(tmp_path: Path) -> None:
    """updated=0 with 13,311 errors must not render at the same severity as one
    bad warrant. resolve_exit_code already encodes the measured rule."""
    wipeout = (
        'SUMMARY_JSON {"job":"daily_update","asset_class":"equity","source":"ib",'
        '"target_date":"2026-08-07","updated":0,"no_trade":0,"partial":0,'
        '"errors":13311,"bars_inserted":0,"validation_issues":0,"top_errors":[]}'
    )
    (tmp_path / "daily_update_2026-08-10.log").write_text(wipeout, encoding="utf-8")
    assert _outcomes_section("2026-08-10", tmp_path).verdict is Verdict.BAD

    one_bad = (
        'SUMMARY_JSON {"job":"daily_update","asset_class":"equity","source":"ib",'
        '"target_date":"2026-08-07","updated":13000,"no_trade":277,"partial":0,'
        '"errors":1,"bars_inserted":13000,"validation_issues":0,"top_errors":[]}'
    )
    (tmp_path / "daily_update_2026-08-10.log").write_text(one_bad, encoding="utf-8")
    assert _outcomes_section("2026-08-10", tmp_path).verdict is Verdict.WARN


def test_every_non_ok_section_carries_a_runnable_fix(tmp_path: Path) -> None:
    """Pain point 3. A fix with an unsubstituted <placeholder> is not a fix."""
    for section in collect(date(2026, 8, 10), tmp_path, tmp_path, runner=_fake_launchctl):
        if section.verdict is Verdict.OK:
            continue
        assert section.fix, f"{section.name} is {section.verdict} with no fix"
        assert "<" not in section.fix, f"{section.name} fix has an unsubstituted placeholder"


def test_a_failed_phase_is_bad_and_a_degraded_phase_is_warn(tmp_path: Path) -> None:
    failed = (
        'SUMMARY_JSON {"job":"daily_backfill","phases":[{"label":"equity","exit":1,'
        '"duration_s":9}],"failed":["equity"],"degraded":[]}'
    )
    (tmp_path / "intraday_catchup_2026-08-10.log").write_text(failed, encoding="utf-8")
    assert _phases_section("2026-08-10", tmp_path).verdict is Verdict.BAD

    degraded = (
        'SUMMARY_JSON {"job":"daily_backfill","phases":[{"label":"futures","exit":86,'
        '"duration_s":9}],"failed":[],"degraded":["futures"]}'
    )
    (tmp_path / "intraday_catchup_2026-08-10.log").write_text(degraded, encoding="utf-8")
    assert _phases_section("2026-08-10", tmp_path).verdict is Verdict.WARN


def test_withheld_windows_are_a_warning(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER, encoding="utf-8")
    section = _silver_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.WARN
    assert any("39" in line for line in section.lines)


def test_coverage_below_the_threshold_is_bad(tmp_path: Path) -> None:
    """The real 2026-08-07 line: warehouse-wide zero, previously rendered plain."""
    (tmp_path / "coverage_2026-08-10.log").write_text(
        "2026-08-10 coverage: 1d=0/13311 (0.00%) 1m=0/14613 (0.00%) "
        "1h=0/14613 (0.00%) 5m=0/14613 (0.00%) 30m=0/14613 (0.00%)\n",
        encoding="utf-8",
    )
    section = _coverage_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.BAD
    assert section.fix is not None


def test_coverage_above_the_threshold_is_ok(tmp_path: Path) -> None:
    (tmp_path / "coverage_2026-08-10.log").write_text(
        "2026-08-10 coverage: 1d=13100/13141 (99.69%) 1m=14000/14100 (99.29%) "
        "1h=14000/14100 (99.29%) 5m=14000/14100 (99.29%) 30m=14000/14100 (99.29%)\n",
        encoding="utf-8",
    )
    assert _coverage_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_a_stale_coverage_log_is_bad_even_when_the_numbers_are_green(tmp_path: Path) -> None:
    """A dead detector is worse than a red one: it reads green forever."""
    (tmp_path / "coverage_2026-07-01.log").write_text(
        "2026-07-01 coverage: 1d=13100/13141 (99.69%) 1m=14000/14100 (99.29%) "
        "1h=14000/14100 (99.29%) 5m=14000/14100 (99.29%) 30m=14000/14100 (99.29%)\n",
        encoding="utf-8",
    )
    assert _coverage_section("2026-08-10", tmp_path).verdict is Verdict.BAD


def test_disk_below_the_reserve_is_bad(tmp_path: Path, monkeypatch) -> None:
    class _Usage:
        total = 100 * 1024**3
        used = 96 * 1024**3
        free = 4 * 1024**3

    monkeypatch.setattr("livewire_scripts.status.shutil.disk_usage", lambda _p: _Usage())
    assert _disk_section(tmp_path).verdict is Verdict.BAD


def test_quality_jobs_all_green_is_ok(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text("nothing wrong\n", encoding="utf-8")
    assert _quality_jobs_section("2026-08-10", tmp_path).verdict is Verdict.OK


def test_a_failed_quality_job_is_a_warning(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(
        "WARNING: coverage failed: timed out after 1800 seconds\n", encoding="utf-8"
    )
    assert _quality_jobs_section("2026-08-10", tmp_path).verdict is Verdict.WARN


def test_section_is_frozen() -> None:
    section = Section(name="x", verdict=Verdict.OK, lines=["x"])
    assert section.fix is None


def test_unknown_outranks_ok_so_a_run_verdict_can_never_be_green_on_a_gap() -> None:
    """The ordering is the mechanism, not the documentation. max() over a run
    of sections must not report OK when one of them could not measure."""
    assert max(Verdict.OK, Verdict.UNKNOWN) is Verdict.UNKNOWN
    assert max(Verdict.OK, Verdict.UNKNOWN, Verdict.WARN, Verdict.BAD) is Verdict.BAD


def test_collect_returns_a_section_per_check(tmp_path: Path) -> None:
    sections = collect(date(2026, 8, 10), tmp_path, tmp_path)
    assert len(sections) == 6
    assert all(isinstance(s, Section) for s in sections)


def test_collect_never_raises_when_a_check_explodes(tmp_path: Path, monkeypatch) -> None:
    def _boom(*_args, **_kwargs):
        raise RuntimeError("footer read exploded")

    monkeypatch.setattr("livewire_scripts.status._disk_section", _boom)
    sections = collect(date(2026, 8, 10), tmp_path, tmp_path)
    disk = [s for s in sections if s.name == "Disk"]
    assert len(disk) == 1
    assert disk[0].verdict is Verdict.UNKNOWN
    assert "footer read exploded" in "\n".join(disk[0].lines)
```

Import `inspect`, `collect`, `_safe` and `date` at the top of the test file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_status.py -q`
Expected: FAIL — `ImportError: cannot import name 'Section'`.

- [ ] **Step 3: Add the types and the verdicts**

In `livewire_scripts/status.py`:

```python
import enum
from dataclasses import dataclass, field


class Verdict(enum.IntEnum):
    """Ordered worst-last so `max()` over a run of checks is the run's verdict.

    UNKNOWN outranks OK deliberately. A check that could not measure has not
    passed — rendering it green is exactly how coverage stayed dead for four
    weeks while the digest printed a line every night.

    IntEnum, not Enum: plain Enum members are unorderable and `max()` over them
    raises TypeError, so the ordering above would have been documentation with
    no mechanism behind it. Identity comparison (`is Verdict.OK`) still works.
    """

    OK = 0
    UNKNOWN = 1
    WARN = 2
    BAD = 3

    @property
    def glyph(self) -> str:
        return {Verdict.OK: "OK ", Verdict.UNKNOWN: "?? ", Verdict.WARN: "WARN", Verdict.BAD: "BAD "}[self]

    @property
    def style(self) -> str:
        return {Verdict.OK: "green", Verdict.UNKNOWN: "magenta", Verdict.WARN: "yellow", Verdict.BAD: "red"}[self]


@dataclass(frozen=True)
class Section:
    """One graded check: the prose the digest already printed, plus a judgement."""

    name: str
    verdict: Verdict
    lines: list[str] = field(default_factory=list)
    fix: str | None = None
```

Then change each of the six functions to build and return a `Section`. Keep every existing `lines.append(...)` exactly as it is; only the `return` changes.

`Section.name` must be **exactly** these strings — `collect()` and the tests match on them:

| function | `Section.name` |
|---|---|
| `_outcomes_section` | `"Daily update outcomes"` |
| `_phases_section` | `"Intraday catch-up phases"` |
| `_silver_section` | `"Silver rebuild"` |
| `_quality_jobs_section` | `"Quality jobs"` |
| `_coverage_section` | `"Coverage"` |
| `_disk_section` | `"Disk"` |

Verdict rules:

**Every non-OK verdict must carry a fix with no unsubstituted placeholder.** A
fix reading `grep '^WARNING:' <log>` is pain point 3 surviving the change —
interpolate the real path.

- `_outcomes_section` — **log file absent → `BAD`** (the nightly run appears never to have happened), fix `launchctl start com.livewire.daily-update`. Log present but no summaries → `UNKNOWN`, fix `tail -50 {path}`. Otherwise grade with the repo's own systemic-failure rule (below). Any remaining `errors` > 0 → `WARN`, fix `grep -c ERROR {path}`. Else `OK`.
- `_phases_section` — same missing-vs-malformed split as above, naming `com.livewire.intraday-catchup`. `summary["failed"]` non-empty → `BAD`, fix `python scripts/livewire_ingest.py daily-backfill`. Else `summary["degraded"]` non-empty → `WARN`, fix `nc -z 127.0.0.1 4001  # IB Gateway: 2FA/maintenance, do not restart from this repo`. Else `OK`.
- `_silver_section` — no summary → `UNKNOWN`, fix `_SILVER_FIX` (below). `window_regressions` > 0 → `WARN`, fix `_SILVER_FIX` — **not** `validate_silver_canary --tickers <symbol>`: the Silver summary carries aggregate counts only and names no symbol, so that command cannot be run as printed. `--failure-output` is what produces the symbol list. Else `OK`. (The `failed` delta arrives in Task 7.)
- `_quality_jobs_section` — any warning → `WARN`, fix `grep '^WARNING:' {log_dir}/daily_update_{run_date}.log` with both values interpolated. Else `OK`.
- `_coverage_section` — no log → `UNKNOWN`, fix `launchctl start com.livewire.coverage`. Parse the measurement with the regex below; any timeframe whose ratio is below `_THRESHOLD` → `BAD`, fix `python scripts/livewire_quality.py coverage --target-date {measured}` with the measured date interpolated. Age > `_COVERAGE_STALE_DAYS` → `BAD`, fix `launchctl start com.livewire.coverage`. An unparseable line → `UNKNOWN`, fix `head -1 {path}   # coverage line did not parse`. Else `OK`.
- `_disk_section` — no volume readable → `UNKNOWN`, fix `df -h`. Any volume under `_MIN_FREE_GB` → `BAD`; under `2 * _MIN_FREE_GB` → `WARN`; else `OK`. Fix `python scripts/livewire_ops.py housekeeping` (dry run is the default). Note this reuses the existing **number** but is a new verdict *interpretation*: today the digest only prints a ⚠ below 2× and says nothing below 1×.

Module level:

```python
_SILVER_FIX = "python scripts/livewire_store.py rebuild-silver --full --dry-run --failure-output /tmp/silver-dry.json"
```

**`_outcomes_section` must not flatten scale into one severity.** The repo
already contains the measured systemic-failure rule, in
`daily_outcomes.resolve_exit_code`: zero updates with any error, or errors over
`max(50, 5% of processed)`. Grading every nonzero `errors` as `WARN` would put
`updated=0, errors=13311` at the same severity as one flaky warrant — which is
the exact disease this whole change exists to cure, reproduced inside the cure.
Reuse the rule rather than inventing a second one:

```python
from livewire_scripts.daily_outcomes import parse_all_summary_json, parse_last_summary_json, resolve_exit_code

    systemic = any(
        resolve_exit_code(
            updated=int(s.get("updated", 0)),
            no_trade=int(s.get("no_trade", 0)),
            partial=int(s.get("partial", 0)),
            errors=int(s.get("errors", 0)),
        )
        != 0
        for s in summaries
    )
```

`systemic` → `BAD`, fix `python scripts/livewire_ingest.py daily --target-date {target}` using the summary's own `target_date`.

Add the coverage parser:

```python
#: Matches one timeframe in `format_one_liner`'s output, e.g. "1d=0/13311 (0.00%)".
_COVERAGE_TF_RE = re.compile(r"(?P<tf>\w+)=(?P<present>\d+)/(?P<total>\d+)")
#: The same knob coverage_report.py already uses; adopting it adds no new judgement.
_THRESHOLD = float(os.getenv("MDW_COVERAGE_ALERT_THRESHOLD", "0.95"))


def _coverage_ratios(measurement: str) -> dict[str, float]:
    """Per-timeframe ratio from a coverage one-liner.

    total == 0 is 1.0, matching CoverageResult.ratio — an asset class with no
    files is not a gap, and dividing by it would raise out of a section that
    promises never to.
    """
    return {
        m["tf"]: (1.0 if int(m["total"]) == 0 else int(m["present"]) / int(m["total"]))
        for m in _COVERAGE_TF_RE.finditer(measurement)
    }
```

- [ ] **Step 4: Add `_safe` and `collect()`, and make `build_digest` consume them**

⚠️ **`build_digest` must call `collect()`, never its own list of sections.** An
earlier draft of this plan had `build_digest` enumerate the six sections
directly and introduced `collect()` a task later; Tasks 4–6 then added the
launchd, undelivered-alert and DuckDB checks to `collect()` only. The result
would have been that **the nightly email — the surface the operator actually
reported as broken — never receives the 4,408-file backlog**, and that the
email renderer alone runs outside `_safe`, so a crashing check silently kills
the whole digest. One assessment layer means one, for both renderers.

In `livewire_scripts/status.py`:

```python
def _safe(name: str, builder) -> Section:
    """Run one check, degrading a crash to UNKNOWN.

    Both renderers go through this: nightly_digest's contract is that a missing
    input cannot suppress the whole report, and a check that *crashes* must be
    visible rather than silently absent.
    """
    try:
        return builder()
    except Exception as exc:  # a broken check must not kill the report
        return Section(
            name=name,
            verdict=Verdict.UNKNOWN,
            lines=[f"{name}: check failed — {exc}"],
            fix="python scripts/livewire_ops.py status   # reproduce, then read the traceback",
        )


def collect(
    run_date: date,
    log_dir: Path,
    data_lake: Path,
    *,
    runner=subprocess.run,
    database: Path | None = None,
) -> list[Section]:
    """Assess every cheap signal. Never raises. Never scans parquet.

    `runner` and `database` exist so tests reach no real machine state. Without
    them the launchd check (Task 4) shells out to the operator's real
    `launchctl` and the catalog check (Task 6) opens the operator's real
    analytics.duckdb — both from a unit test, which the repo's testing rules
    forbid. They are declared here rather than in Task 4/6 so the signature
    never changes underneath a written test.
    """
    run = run_date.isoformat()
    return [
        _safe("Daily update outcomes", lambda: _outcomes_section(run, log_dir)),
        _safe("Intraday catch-up phases", lambda: _phases_section(run, log_dir)),
        _safe("Silver rebuild", lambda: _silver_section(run, log_dir)),
        _safe("Quality jobs", lambda: _quality_jobs_section(run, log_dir)),
        _safe("Coverage", lambda: _coverage_section(run, log_dir)),
        _safe("Disk", lambda: _disk_section(data_lake, log_dir.parent)),
    ]
```

Add `import subprocess` to `status.py` (Task 4 is its first real use).

In `livewire_scripts/nightly_digest.py`, replace the whole section list with one `collect()` call, and replace the six-name import with `from livewire_scripts.status import Verdict, collect`:

```python
def build_digest(run_date: date, log_dir: Path, data_lake: Path) -> str:
    """Assemble the nightly digest text. Never raises on missing inputs.

    Renders exactly what `livewire_ops.py status` renders — same checks, same
    verdicts, same fixes. Anything added to collect() reaches both surfaces or
    neither; there is no list here to forget to update.
    """
    blocks = [f"Livewire nightly digest — {run_date.isoformat()}"]
    for section in collect(run_date, log_dir, data_lake):
        headline = section.lines[0] if section.lines else f"{section.name}: (no detail)"
        lines = [f"[{section.verdict.glyph}] {headline}", *section.lines[1:]]
        # Same rule as render(): a fix line on a green section is noise, and
        # noise on the green path is what trains a reader to skim the email.
        if section.fix and section.verdict is not Verdict.OK:
            lines.append(f"  fix: {section.fix}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks) + "\n"
```

Add to `tests/test_status.py`:

```python
def test_the_digest_and_the_terminal_see_the_same_checks(tmp_path: Path) -> None:
    """The defect this replaces: build_digest had its own hard-coded list, so
    every check added later reached the terminal and never the email."""
    from livewire_scripts import nightly_digest

    source = inspect.getsource(nightly_digest.build_digest)
    assert "collect(" in source
    for name in ("_outcomes_section", "_coverage_section", "_disk_section"):
        assert name not in source, "build_digest must not enumerate sections itself"
```

- [ ] **Step 5: Migrate the eleven direct callers in `tests/test_nightly_digest.py`**

Task 1 kept these green because the moved functions still returned `list[str]`. Task 2 changes that return type, so every direct call breaks. There are exactly eleven, all in the two test classes at the end of the file:

- `nightly_digest._disk_section(...)` — 5 call sites (lines ~496, 509, 519, 531, 565, 579)
- `nightly_digest._coverage_section(...)` — 6 call sites (lines ~420, 435, 455, 461, 551, 560)

**Move both test classes to `tests/test_status.py`** and change each call to the direct import plus `.lines`, e.g. `"\n".join(_disk_section(lake, warehouse).lines)`. Do not leave them reaching through `nightly_digest` into another module's internals — that is what rots.

`monkeypatch.setattr(nightly_digest.shutil, "disk_usage", ...)` becomes `monkeypatch.setattr(status.shutil, ...)`. Note that either form actually works, because both names bind the same cached `shutil` module object and the patch lands on that object's attribute — but write the honest one.

Everything else in `tests/test_nightly_digest.py` keeps passing: `_LOG_DIR` and `datetime` stay in that module, and the whole-digest assertions are substring checks (`"Disk:" in out`) that survive a verdict prefix. Do not weaken any assertion that checks content.

- [ ] **Step 6: Run both test files**

Run: `uv run pytest tests/test_status.py tests/test_nightly_digest.py -q`
Expected: PASS. Fix only assertions that anchor on line starts.

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/status.py livewire_scripts/nightly_digest.py tests/test_status.py tests/test_nightly_digest.py
git commit -m "feat: every status section now carries a verdict and a fix command

Warehouse-wide zero coverage and a routine trim rendered in the same font, and
'(not found)' — the run's log missing entirely — read like a healthy line.
UNKNOWN deliberately outranks OK: a check that could not measure has not passed."
```

---

### Task 3: `livewire_ops.py status` — the terminal renderer

**Files:**
- Modify: `livewire_scripts/status.py`
- Modify: `scripts/livewire_ops.py:23-28`
- Test: `tests/test_status.py`

**Interfaces:**
- Consumes: `Section`, `Verdict`, `collect()` and `_safe()` from Task 2.
- Produces: `status.render(sections: list[Section]) -> str`, `status.main(argv: list[str] | None = None) -> int`, and the `status` subcommand on `scripts/livewire_ops.py`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_status.py` (the `collect()` tests belong to Task 2, where `collect` is introduced — add these there instead if you are working ahead):

```python
def test_render_shows_the_fix_for_anything_not_ok() -> None:
    out = render([Section(name="Coverage", verdict=Verdict.BAD, lines=["Coverage:"], fix="run me")])
    assert "run me" in out
    assert "BAD" in out


def test_render_omits_the_fix_when_ok() -> None:
    out = render([Section(name="Disk", verdict=Verdict.OK, lines=["Disk: fine"], fix="run me")])
    assert "run me" not in out


def test_render_survives_markup_in_log_derived_text(capsys) -> None:
    """Measured: a log line containing "[/]" raises MarkupError and takes the
    whole command down; "[bold red]" is silently eaten as a style."""
    from rich.console import Console

    hostile = Section(
        name="Quality jobs",
        verdict=Verdict.WARN,
        lines=["Quality jobs: 1 FAILED", "  coverage failed: timed out [/] after [bold red]1800[/bold red]s"],
    )
    Console().print(render([hostile]))
    out = capsys.readouterr().out
    assert "[/]" in out
    assert "1800" in out


def test_main_exits_zero_even_when_everything_is_broken(tmp_path: Path, capsys) -> None:
    """A nonzero exit invites someone to schedule this, and every stale
    launchctl red would then page."""
    rc = main(["--run-date", "2026-08-10", "--log-dir", str(tmp_path), "--data-lake", str(tmp_path)])
    assert rc == 0
    assert "Livewire status" in capsys.readouterr().out
```

Extend the import at the top of the file with `collect`, `main`, `render`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_status.py -q`
Expected: FAIL — `ImportError: cannot import name 'collect'`.

- [ ] **Step 3: Implement `collect`, `render` and `main`**

```python
import argparse
from datetime import UTC, datetime

from rich.console import Console
from rich.markup import escape

from livewire_scripts.paths import data_lake_dir, log_dir as default_log_dir


def render(sections: list[Section]) -> str:
    """Render for a terminal. Returns rich markup; Console() applies it.

    EVERY line here is log-derived text and MUST go through `escape()`.
    Measured 2026-08-10 against rich: a line containing "[/]" raises
    MarkupError and takes the whole command down, and a line containing
    "[bold red]" is silently consumed as a style — the text vanishes from the
    report. Both shapes occur in real log output (error payloads, path
    fragments, `top_errors` reprs).

    Note that a bare "[BAD ]" is NOT a hazard — rich leaves unrecognised tags
    literal. The verdict keeps its brackets so the terminal and the email read
    identically; colour is added on top, not instead.
    """
    lines = ["Livewire status"]
    for section in sections:
        # `lines` defaults to [] on the dataclass and render() is the one path
        # with no try/except above it — an empty-lines Section must not be the
        # thing that kills the report it was added to.
        headline = section.lines[0] if section.lines else f"{section.name}: (no detail)"
        lines.append(f"[{section.verdict.style}][{section.verdict.glyph}][/] {escape(headline)}")
        lines.extend(f"  {escape(line.lstrip())}" for line in section.lines[1:])
        if section.fix and section.verdict is not Verdict.OK:
            lines.append(f"  [dim]fix:[/] {escape(section.fix)}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Assess Livewire's operational state")
    parser.add_argument("--run-date", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--log-dir", type=Path, default=None)
    parser.add_argument("--data-lake", type=Path, default=None)
    args = parser.parse_args(argv)
    sections = collect(
        args.run_date,
        args.log_dir or default_log_dir(),
        args.data_lake or data_lake_dir(),
    )
    # Exit 0 always: see the module docstring. rich strips markup when not a TTY.
    Console().print(render(sections))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 4: Register the subcommand**

In `scripts/livewire_ops.py`, add to `COMMANDS`:

```python
    "status": "livewire_scripts.status",
```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/test_status.py tests/test_livewire_entrypoints.py -q`
Expected: PASS. If `test_livewire_entrypoints.py` asserts an exact subcommand list, add `status` to it.

- [ ] **Step 6: Run it against the real warehouse and eyeball the output**

Run: `uv run python scripts/livewire_ops.py status`
Expected: six graded lines. Coverage should read BAD (the newest log measures 2026-08-07 at 0.00%) and Disk should read BAD or WARN.

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/status.py scripts/livewire_ops.py tests/test_status.py tests/test_livewire_entrypoints.py
git commit -m "feat: livewire_ops.py status — one graded terminal view"
```

---

### Task 4: The launchd check

**Files:**
- Modify: `livewire_scripts/status.py`
- Test: `tests/test_status.py`

**Interfaces:**
- Consumes: `Section`, `Verdict`.
- Produces: `status._launchd_section(runner=subprocess.run) -> Section`, and `collect()` returns 7 sections.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_job_that_is_not_loaded_is_bad() -> None:
    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout="-\t0\tcom.livewire.daily-update\n", returncode=0)

    section = _launchd_section(runner=_runner)
    assert section.verdict is Verdict.BAD
    assert "com.livewire.coverage" in "\n".join(section.lines)


def test_a_nonzero_exit_is_capped_at_warn() -> None:
    """launchctl reports the LAST exit with no timestamp. Today's watchdog=1 is
    residue from a run that predates the fix already in production. Calling that
    BAD is the fastest way to make the whole surface ignorable."""
    stdout = "".join(f"-\t0\t{label}\n" for label in _LAUNCHD_JOBS)
    stdout = stdout.replace("-\t0\tcom.livewire.intraday-catchup", "-\t86\tcom.livewire.intraday-catchup")

    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout=stdout, returncode=0)

    section = _launchd_section(runner=_runner)
    assert section.verdict is Verdict.WARN
    assert "no timestamp" in "\n".join(section.lines)


def test_all_jobs_green_is_ok() -> None:
    stdout = "".join(f"-\t0\t{label}\n" for label in _LAUNCHD_JOBS)

    def _runner(_cmd, **_kw):
        return SimpleNamespace(stdout=stdout, returncode=0)

    assert _launchd_section(runner=_runner).verdict is Verdict.OK


def test_launchctl_missing_is_unknown() -> None:
    def _runner(_cmd, **_kw):
        raise FileNotFoundError("launchctl")

    assert _launchd_section(runner=_runner).verdict is Verdict.UNKNOWN
```

Add `from types import SimpleNamespace` and `_LAUNCHD_JOBS`, `_launchd_section` to the imports.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_status.py -k launchd -q`
Expected: FAIL — `cannot import name '_launchd_section'`.

- [ ] **Step 3: Implement**

```python
# `import subprocess` already landed in Task 3 as collect()'s runner default.

#: Every plist under launchd/. A job that is absent cannot run and cannot
#: recover on its own, which is the only BAD this check ever reports.
_LAUNCHD_JOBS: tuple[str, ...] = (
    "com.livewire.daily-update",
    "com.livewire.daily-update-watchdog",
    "com.livewire.intraday-catchup",
    "com.livewire.coverage",
    "com.livewire.release-promote",
)


def _launchd_section(runner=subprocess.run) -> Section:
    """Grade the scheduled jobs from `launchctl list`.

    `launchctl list` prints "PID\\tStatus\\tLabel" and the status is the LAST
    exit code with no indication of when it happened. This check therefore
    caps a nonzero exit at WARN: right now the watchdog shows 1 and
    intraday-catchup shows 86, both residue from runs predating the fix now in
    production. Overstating a stale red trains the reader to ignore the surface.
    """
    try:
        result = runner(["launchctl", "list"], capture_output=True, text=True, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return Section("launchd jobs", Verdict.UNKNOWN, [f"launchd jobs: launchctl unavailable — {exc}"])

    loaded: dict[str, str] = {}
    for line in (result.stdout or "").splitlines():
        parts = line.split("\t")
        if len(parts) >= 3 and parts[2] in _LAUNCHD_JOBS:
            loaded[parts[2]] = parts[1]

    missing = [label for label in _LAUNCHD_JOBS if label not in loaded]
    nonzero = {label: code for label, code in loaded.items() if code not in {"0", "-"}}

    lines = ["launchd jobs:"]
    for label in _LAUNCHD_JOBS:
        lines.append(f"  {label:<38} {loaded.get(label, 'NOT LOADED')}")
    if missing:
        # Name EVERY missing job, not just the first — an operator who runs the
        # printed command and sees the section still red learns to distrust it.
        # And check the plist actually exists: the repo ships `.plist.example`
        # templates that must be rendered first, so `launchctl load` on an
        # uninstalled label fails with a message that explains nothing.
        agents = Path.home() / "Library/LaunchAgents"
        installed = [label for label in missing if (agents / f"{label}.plist").exists()]
        uninstalled = [label for label in missing if label not in installed]
        lines.append(f"  missing: {', '.join(missing)}")
        if uninstalled:
            fix = (
                f"render the plist first — no {agents}/{uninstalled[0]}.plist exists; "
                f"see launchd/{uninstalled[0]}.plist.example and the CLAUDE.md scheduling block"
            )
        else:
            fix = " && ".join(f"launchctl load {agents}/{label}.plist" for label in installed)
        return Section("launchd jobs", Verdict.BAD, lines, fix=fix)
    if nonzero:
        lines.append("  note: exit code carries no timestamp — check the matching log before acting")
        return Section("launchd jobs", Verdict.WARN, lines, fix="ls -lt ~/market-warehouse/logs/ | head")
    return Section("launchd jobs", Verdict.OK, lines)
```

Add to `collect()`, first in the list:

```python
        _safe("launchd jobs", lambda: _launchd_section(runner=runner)),
```

Update `test_collect_returns_a_section_per_check` to expect 7, and give every `collect(...)` call in the tests a fake runner so no test shells out to the real `launchctl`:

```python
def _fake_launchctl(_cmd, **_kw):
    return SimpleNamespace(stdout="".join(f"-\t0\t{label}\n" for label in _LAUNCHD_JOBS), returncode=0)
```

`main()` builds its own `collect()` arguments, so `test_main_exits_zero_even_when_everything_is_broken` cannot pass `runner=`. Add to it:

```python
    monkeypatch.setattr("livewire_scripts.status.subprocess.run", _fake_launchctl)
```

(and take `monkeypatch` as a parameter). Without this the test shells out to the operator's real `launchctl`.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_status.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/status.py tests/test_status.py
git commit -m "feat(status): report the launchd jobs, capping a stale red at WARN"
```

---

### Task 5: The undelivered-alert backlog

**Files:**
- Modify: `livewire_scripts/status.py`
- Test: `tests/test_status.py`

**Interfaces:**
- Consumes: `Section`, `Verdict`.
- Produces: `status._undelivered_section(log_dir: Path) -> Section`, and `collect()` returns 8 sections.

- [ ] **Step 1: Write the failing tests**

```python
def test_an_empty_or_absent_queue_is_ok(tmp_path: Path) -> None:
    assert _undelivered_section(tmp_path).verdict is Verdict.OK
    (tmp_path / "quality_alerts_undelivered").mkdir()
    assert _undelivered_section(tmp_path).verdict is Verdict.OK


def test_any_undelivered_alert_is_a_warning(tmp_path: Path) -> None:
    """4,408 of these were on disk, newest 2026-08-02, and appeared in no
    report anywhere — the channel that would report it was the broken one."""
    queue = tmp_path / "quality_alerts_undelivered"
    queue.mkdir()
    (queue / "2026-08-02T05-54-08Z_ib_CL_202612.html").write_text("<p>x</p>", encoding="utf-8")
    section = _undelivered_section(tmp_path)
    assert section.verdict is Verdict.WARN
    assert "1" in "\n".join(section.lines)
    assert section.fix is not None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_status.py -k undelivered -q`
Expected: FAIL — `cannot import name '_undelivered_section'`.

- [ ] **Step 3: Implement**

```python
def _undelivered_queues(log_dir: Path) -> list[Path]:
    """BOTH queues. The repo keeps two, deliberately and separately.

    `MDW_UNDELIVERED_DIR` (default `quality_alerts_undelivered`) is per-flag
    quality alerts — the 4,408 files. `run_daily_update_job.undelivered_dir`
    writes scheduled-job alerts to `<log_dir>/alerts_undelivered` and its
    docstring says the split is intentional. A section called "Undelivered
    alerts" that counts only one of them is misnamed, and the one it would omit
    is the *job failure* page.
    """
    return [
        Path(os.environ.get("MDW_UNDELIVERED_DIR", log_dir / "quality_alerts_undelivered")),
        log_dir / "alerts_undelivered",
    ]


def _undelivered_section(log_dir: Path) -> Section:
    """Count alerts that could not be sent.

    No severity ladder by age. 4,408 files whose newest is a week old is a
    historic pile-up; one file from an hour ago is an active failure; a rule
    that tries to tell them apart would be guessing. Print both numbers and let
    the reader judge.
    """
    lines, total, newest_ts, unreadable = ["Undelivered alerts:"], 0, 0.0, []
    for directory in _undelivered_queues(log_dir):
        try:
            entries = [p for p in directory.iterdir() if p.is_file()]
        except FileNotFoundError:
            lines.append(f"  {directory.name:<28} none")
            continue
        except OSError as exc:
            unreadable.append(f"{directory.name}: {exc}")
            lines.append(f"  {directory.name:<28} unreadable — {exc}")
            continue
        if not entries:
            lines.append(f"  {directory.name:<28} none")
            continue
        stamp = max(p.stat().st_mtime for p in entries)
        newest_ts = max(newest_ts, stamp)
        total += len(entries)
        lines.append(
            f"  {directory.name:<28} {len(entries):>6,} file(s), newest {date.fromtimestamp(stamp).isoformat()}"
        )

    if unreadable:
        return Section("Undelivered alerts", Verdict.UNKNOWN, lines, fix=f"ls -ld {log_dir}")
    if not total:
        return Section("Undelivered alerts", Verdict.OK, ["Undelivered alerts: none"])
    return Section(
        "Undelivered alerts",
        Verdict.WARN,
        [f"Undelivered alerts: {total:,} across 2 queues, newest {date.fromtimestamp(newest_ts).isoformat()}"]
        + lines[1:],
        # An honest instruction: read one to learn WHY delivery failed, then
        # delete the batch. `ls | head` alone neither diagnoses nor clears, and
        # a fix that overpromises is a fix nobody trusts twice.
        fix=f"cat $(ls -t {log_dir}/quality_alerts_undelivered/* | head -1)   # then rm the batch once understood",
    )
```

Add to `collect()` after the launchd check:

```python
        _safe("Undelivered alerts", lambda: _undelivered_section(log_dir)),
```

and update the count test to 8.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_status.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/status.py tests/test_status.py
git commit -m "feat(status): surface the undelivered-alert backlog

4,408 files sat in quality_alerts_undelivered and appeared in no report — the
channel that would have reported it was the one that broke."
```

---

### Task 6: The DuckDB catalog check

**Files:**
- Modify: `clients/duckdb_catalog.py`
- Modify: `livewire_scripts/status.py`
- Test: `tests/test_status.py`, `tests/test_duckdb_catalog.py`

**Interfaces:**
- Consumes: `Section`, `Verdict`.
- Produces: `clients.duckdb_catalog.coverage_headline(database: Path | str | None = None) -> dict[str, tuple[int, date | None]]`; `status._duckdb_section(target: date, database: Path | None = None) -> Section`; `collect()` returns 9 sections.

- [ ] **Step 1: Write the failing test for `coverage_headline`**

Append to `tests/test_duckdb_catalog.py`:

This file already has `lake` and `silver` fixtures and builds real catalogs with `build_coverage(dest, lake_root=lake, silver_root=silver)` — reuse that exact pattern (see `test_build_coverage_publishes_expected_rows`). Do not invent a second seeding helper.

```python
def test_coverage_headline_reports_symbols_and_newest_date(tmp_path: Path, lake: Path, silver: Path) -> None:
    from clients.duckdb_catalog import coverage_headline

    database = tmp_path / "analytics.duckdb"
    build_coverage(database, lake_root=lake, silver_root=silver)
    headline = coverage_headline(database)

    assert "bronze_equity_1d" in headline
    symbols, last = headline["bronze_equity_1d"]
    assert symbols > 0
    assert last is not None


def test_coverage_headline_raises_when_the_catalog_was_never_built(tmp_path: Path) -> None:
    from clients.duckdb_catalog import coverage_headline

    with pytest.raises(FileNotFoundError):
        coverage_headline(tmp_path / "absent.duckdb")


def test_coverage_headline_raises_when_the_file_holds_no_coverage_table(tmp_path: Path) -> None:
    """What an interrupted `duckdb build` leaves behind. The caller cannot
    catch DuckDB's own exception — importing duckdb is what the containment
    test forbids it — so the translation has to happen here."""
    from clients.duckdb_catalog import connect, coverage_headline

    database = tmp_path / "empty.duckdb"
    connect(database, read_only=False).close()

    with pytest.raises(FileNotFoundError):
        coverage_headline(database)
```

Add `coverage_headline` to the file's existing `from clients.duckdb_catalog import (...)` block, and `import pytest` if it is not already there.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_duckdb_catalog.py -k headline -q`
Expected: FAIL — `cannot import name 'coverage_headline'`.

- [ ] **Step 3: Implement `coverage_headline`**

In `clients/duckdb_catalog.py`, below `build_coverage`:

```python
def coverage_headline(database: Path | str | None = None) -> dict[str, tuple[int, date | None]]:
    """Per-view symbol count and newest ``last_date`` from the coverage table.

    The one read `status` is allowed: `tests/test_duckdb_containment.py` permits
    only this module and the catalog CLI to import duckdb, and that test is what
    keeps DuckDB a query layer rather than a second warehouse.

    Raises FileNotFoundError when the catalog has never been built — including
    the case where the FILE exists but holds no `coverage` table, which is what
    an interrupted `duckdb build` leaves behind. The caller cannot distinguish
    them itself: catching DuckDB's CatalogException would mean importing duckdb,
    which is exactly what the containment test forbids it from doing. Translate
    here, where duckdb is allowed.
    """
    path = Path(database) if database is not None else default_database()
    if not path.exists():
        raise FileNotFoundError(path)
    with open_catalog(path) as con:
        try:
            rows = con.execute(
                "SELECT view_name, count(*), max(last_date) FROM coverage GROUP BY view_name ORDER BY view_name"
            ).fetchall()
        except duckdb.CatalogException as exc:
            raise FileNotFoundError(f"{path}: no coverage table") from exc
    return {str(name): (int(count), last) for name, count, last in rows}
```

Add `from datetime import date` to the module imports if absent.

- [ ] **Step 4: Write the failing tests for the status check**

```python
def test_duckdb_check_is_unknown_when_duckdb_is_not_installed(monkeypatch) -> None:
    """~/market-warehouse/.venv genuinely has no duckdb. A status command that
    cannot start in one of the three real environments is worthless.

    Patches the `_coverage_headline` seam rather than `builtins.__import__` —
    replacing __import__ affects every import for the duration of the test,
    including pytest's own, and a status check is not worth that blast radius.
    """

    def _no_duckdb(_db):
        raise ImportError("No module named 'duckdb'")

    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _no_duckdb)
    section = _duckdb_section(date(2026, 8, 10))
    assert section.verdict is Verdict.UNKNOWN
    assert "duckdb" in "\n".join(section.lines).lower()


def test_duckdb_check_is_bad_when_the_table_lags_by_more_than_three_sessions(monkeypatch) -> None:
    monkeypatch.setattr(
        "livewire_scripts.status._coverage_headline",
        lambda _db: {"bronze_equity_1d": (13311, date(2026, 7, 1))},
    )
    assert _duckdb_section(date(2026, 8, 10)).verdict is Verdict.BAD


def test_duckdb_check_is_ok_when_current(monkeypatch) -> None:
    monkeypatch.setattr(
        "livewire_scripts.status._coverage_headline",
        lambda _db: {"bronze_equity_1d": (13311, date(2026, 8, 10))},
    )
    assert _duckdb_section(date(2026, 8, 10)).verdict is Verdict.OK


def test_duckdb_check_is_unknown_when_never_built(monkeypatch) -> None:
    def _absent(_db):
        raise FileNotFoundError("analytics.duckdb")

    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _absent)
    section = _duckdb_section(date(2026, 8, 10))
    assert section.verdict is Verdict.UNKNOWN
    assert section.fix is not None
```

- [ ] **Step 5: Run to verify failure**

Run: `uv run pytest tests/test_status.py -k duckdb -q`
Expected: FAIL — `cannot import name '_duckdb_section'`.

- [ ] **Step 6: Implement the status check**

```python
#: Indirection so tests can replace the catalog read without importing duckdb,
#: and so the ImportError guard has exactly one place to live.
def _coverage_headline(database: Path | None):
    from clients.duckdb_catalog import coverage_headline

    return coverage_headline(database)


#: Deliberately the same NUMBER as _COVERAGE_STALE_DAYS and deliberately a
#: separate constant: that one counts calendar days, this one counts trading
#: sessions. Sharing the name would make a future edit to one silently change
#: the other's meaning.
_CATALOG_STALE_SESSIONS = 3


def _sessions_behind(newest: date, target: date, limit: int = 10) -> int:
    """Trading sessions between *newest* and *target*, saturating at *limit*.

    Sessions, not calendar days: newest=Friday against target=Monday is one
    session behind but three days, and a calendar-day rule would flag every
    Monday morning as stale.
    """
    from clients.trading_calendar import previous_trading_day

    cursor, count = target, 0
    while cursor > newest and count < limit:
        cursor = previous_trading_day(cursor)
        count += 1
    return count


def _duckdb_section(target: date, database: Path | None = None) -> Section:
    """Grade the DuckDB coverage table's own staleness.

    The table is refreshed by the last phase of `daily-backfill`. When that
    orchestrator stopped running, the table quietly froze — on 2026-08-10 it
    still read 2026-08-07 — and nothing anywhere said so. Catalog staleness is
    a symptom of an upstream lane, which is exactly why it belongs here.
    """
    try:
        headline = _coverage_headline(database)
    except ImportError as exc:
        return Section(
            "DuckDB catalog",
            Verdict.UNKNOWN,
            [f"DuckDB catalog: duckdb unavailable in this environment — {exc}"],
            fix="use the release venv: ~/market-warehouse/current/.venv/bin/python",
        )
    except FileNotFoundError:
        return Section(
            "DuckDB catalog",
            Verdict.UNKNOWN,
            ["DuckDB catalog: never built"],
            fix="python scripts/livewire_store.py duckdb build",
        )
    # No broad `except Exception` here: collect() wraps every check in _safe(),
    # which already degrades an unexpected crash to UNKNOWN. The two caught
    # above are caught because each has a SPECIFIC, actionable message.

    dates = [last for _count, last in headline.values() if last is not None]
    if not dates:
        return Section(
            "DuckDB catalog",
            Verdict.UNKNOWN,
            ["DuckDB catalog: table holds no dated rows"],
            fix="python scripts/livewire_store.py duckdb build",
        )

    # The WORST view, not the freshest. `max(dates)` would let one current view
    # green the whole check while bronze_equity_1d sat frozen — the detail lines
    # would print the stale view under an OK headline that carries no fix, which
    # is the same "a fact nobody grades" shape this module exists to kill.
    laggard, oldest = min(
        ((name, last) for name, (_count, last) in headline.items() if last is not None),
        key=lambda item: item[1],
    )
    behind = _sessions_behind(oldest, target)
    lines = [
        "DuckDB catalog:",
        f"  oldest view {laggard} last_date={oldest.isoformat()}  ({behind} session(s) behind {target})",
    ]
    for view_name, (count, last) in sorted(headline.items()):
        lines.append(f"  {view_name:<24} {count:>7,} symbols  last={last}")
    if behind > _CATALOG_STALE_SESSIONS:
        return Section("DuckDB catalog", Verdict.BAD, lines, fix="python scripts/livewire_store.py duckdb build")
    if behind > 1:
        return Section("DuckDB catalog", Verdict.WARN, lines, fix="python scripts/livewire_store.py duckdb build")
    return Section("DuckDB catalog", Verdict.OK, lines)
```

Add to `collect()`, after the coverage check:

```python
        _safe("DuckDB catalog", lambda: _duckdb_section(run_date, database)),
```

Update the count test to 9, and pass `database=tmp_path / "absent.duckdb"` in every `collect(...)` test call — otherwise `default_database()` opens the operator's real `~/market-warehouse/analytics.duckdb` from a unit test. An absent path yields `UNKNOWN`, which is the correct verdict for a test warehouse.

`main()` cannot pass `database=`, so patch the same `_coverage_headline` seam every other DuckDB test uses. Add to `test_main_exits_zero_even_when_everything_is_broken`:

```python
    def _absent(_db):
        raise FileNotFoundError("analytics.duckdb")

    monkeypatch.setattr("livewire_scripts.status._coverage_headline", _absent)
```

One seam, patched the same way everywhere. Do not reach for `default_database` — importing it at module level in `status.py` would pull in `clients.duckdb_catalog`, and therefore `duckdb`, at import time, which is exactly what the ImportError guard exists to avoid.

- [ ] **Step 7: Run the containment test and the full suite**

Run: `uv run pytest tests/test_duckdb_containment.py tests/test_status.py tests/test_duckdb_catalog.py -q`
Expected: PASS, with **no new entry** in `ALLOWED_DUCKDB_IMPORTERS`. If the containment test fails, `status.py` imports duckdb somewhere — fix `status.py`, never the allowlist.

- [ ] **Step 8: Commit**

```bash
git add clients/duckdb_catalog.py livewire_scripts/status.py tests/test_status.py tests/test_duckdb_catalog.py
git commit -m "feat(status): grade the DuckDB catalog's own staleness

The table froze on 2026-08-07 when daily-backfill stopped running and nothing
said so. Read through clients/duckdb_catalog so the containment test — which is
what keeps DuckDB a query layer — needs no new exception."
```

---

### Task 7: Silver reports the change in `failed`, not an absolute

**Files:**
- Modify: `livewire_scripts/status.py:_silver_section`
- Test: `tests/test_status.py`

**Interfaces:**
- Consumes: `Section`, `Verdict`, `parse_all_summary_json`.
- Produces: `status._previous_silver_summary(run_date: str, log_dir: Path) -> dict | None`. `_silver_section` keeps its signature.

- [ ] **Step 1: Write the failing tests**

```python
_SILVER_CLEAN = (
    'SUMMARY_JSON {"revision":25,"rebuilt":2,"unchanged":13076,"trimmed":254,'
    '"failed":233,"window_regressions":0}'
)
_SILVER_WORSE = (
    'SUMMARY_JSON {"revision":26,"rebuilt":2,"unchanged":13076,"trimmed":254,'
    '"failed":301,"window_regressions":0}'
)


def test_silver_failed_rising_is_a_warning(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-09.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER_WORSE, encoding="utf-8")
    section = _silver_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.WARN
    assert "+68" in "\n".join(section.lines)


def test_silver_failed_flat_is_ok_and_still_prints_the_absolute(tmp_path: Path) -> None:
    """failed=233 is not graded on an absolute line — there is no measured
    basis for one, and inventing a threshold would be fabrication."""
    (tmp_path / "daily_update_2026-08-09.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    section = _silver_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.OK
    assert "failed=233" in "\n".join(section.lines)


def test_silver_without_a_baseline_is_unknown(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(_SILVER_CLEAN, encoding="utf-8")
    assert _silver_section("2026-08-10", tmp_path).verdict is Verdict.UNKNOWN
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_status.py -k silver -q`
Expected: FAIL — the current section returns OK with no delta line.

- [ ] **Step 3: Implement the baseline lookup and the delta rule**

```python
def _previous_silver_summary(run_date: str, log_dir: Path) -> dict | None:
    """The most recent Silver SUMMARY_JSON from a log older than *run_date*.

    Reverse-sorted filenames mean this normally reads exactly one file. There is
    no absolute threshold for `failed` anywhere in this module: 233 may be
    normal or catastrophic and nothing measured tells us which, so the baseline
    is the previous run and the signal is the change.

    The date is PARSED rather than string-compared. `daily_update_*.log` also
    matches `daily_update_watchdog_<date>.log`, which is a different job's log
    — those sort first under `reverse=True` and only fall out of a `>=` string
    comparison because "w" happens to exceed "2". Right answer, wrong reason.
    """
    try:
        cutoff = date.fromisoformat(run_date)
    except ValueError:
        return None
    for path in sorted(log_dir.glob("daily_update_*.log"), reverse=True):
        try:
            stamp = date.fromisoformat(path.stem.removeprefix("daily_update_"))
        except ValueError:
            continue  # daily_update_watchdog_*.log, or anything else undated
        if stamp >= cutoff:
            continue
        summaries = [s for s in parse_all_summary_json(_read_text(path) or "") if "window_regressions" in s]
        if summaries:
            return summaries[-1]
    return None
```

In `_silver_section`, after the existing `lines.append(...)` calls, replace the Task 2 verdict logic with:

```python
    regressions = s.get("window_regressions", 0)
    if regressions:
        # (existing ⚠ line stays exactly as written)
        ...
    previous = _previous_silver_summary(run_date, log_dir)
    if previous is None:
        lines.append("  no previous run to compare against")
        return Section("Silver rebuild", Verdict.UNKNOWN, lines, fix=_SILVER_FIX)
    delta = int(s.get("failed", 0)) - int(previous.get("failed", 0))
    lines.append(f"  failed {delta:+d} vs revision {previous.get('revision', '?')}")
    if regressions or delta > 0:
        return Section("Silver rebuild", Verdict.WARN, lines, fix=_SILVER_FIX)
    return Section("Silver rebuild", Verdict.OK, lines)
```

with `_SILVER_FIX = "python scripts/livewire_store.py rebuild-silver --full --dry-run --failure-output /tmp/silver-dry.json"` at module level.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_status.py -q`
Expected: PASS. Task 2's `test_withheld_windows_are_a_warning` still passes — `window_regressions=39` keeps WARN with or without a baseline.

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/status.py tests/test_status.py
git commit -m "feat(status): grade Silver on the change in failed, not an absolute

There is no measured basis for an absolute line on failed=233, so the baseline
is the previous run and the signal is the delta."
```

---

### Task 8: Documentation and the full gate

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/superpowers/plans/2026-08-10-graded-status-surface.md` (tick the boxes)

**Interfaces:**
- Consumes: everything above.
- Produces: nothing new.

- [ ] **Step 1: Document the command**

In `CLAUDE.md`, under "Reliability tooling", add:

````markdown
### Status — one graded view

`scripts/livewire_ops.py status` grades nine cheap signals and prints a fix
command for anything not OK. It reads only what the nightly jobs already
produced — **it never scans parquet**, because `coverage` costs 1400–2860s and
`warehouse` reads footers.

```bash
python scripts/livewire_ops.py status
```

- **`UNKNOWN` is not `OK`.** A missing input is a failure to measure, which is
  how coverage stayed dead for four weeks while the digest printed a green line
  every night. `Verdict` is ordered so `UNKNOWN` outranks `OK`.
- **`launchctl` exit codes carry no timestamp**, so a nonzero exit is capped at
  WARN and annotated. Overstating a stale red is the fastest way to make the
  whole surface ignorable. A job that is *not loaded* is BAD — it cannot recover
  on its own.
- **Silver is graded on the change in `failed`, never an absolute.** Nothing
  measured says whether `failed=233` is normal, so the baseline is the previous
  run.
- **`status.py` must never `import duckdb`** — it reads the catalog through
  `clients.duckdb_catalog.coverage_headline`, and guards the import so it still
  runs in `~/market-warehouse/.venv`, which has no duckdb installed.
- **Exit code is always 0.** A nonzero-on-BAD contract would invite scheduling
  it, and every stale `launchctl` red would then page.
- The nightly digest renders the same `Section` objects, so a threshold is
  defined once and both surfaces get it.
````

- [ ] **Step 2: Run the complete CI gate**

```bash
uv run ruff check . && \
uv run ruff format --check . && \
uv run pyright && \
uv run pytest tests/ -m "not integration" --cov --cov-fail-under=95 -W error::RuntimeWarning
```
Expected: all pass, coverage ≥ 95%.

- [ ] **Step 3: Run the command against the real warehouse one final time**

Run: `uv run python scripts/livewire_ops.py status`
Expected: nine graded lines. Record the actual output in the PR body — it is the evidence the surface works on real data.

- [ ] **Step 4: Tick every checkbox in this plan, then commit**

```bash
git add CLAUDE.md docs/superpowers/plans/2026-08-10-graded-status-surface.md
git commit -m "docs: the status surface and why UNKNOWN outranks OK"
```

- [ ] **Step 5: Push and open the PR**

```bash
git push -u origin status-graded-surface
gh pr create --title "feat: status reports state facts but never judge them" --body-file <path>
```

Wait for CI to go green before merging. Never merge on a red or pending check.

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| `Section`/`Verdict` types | 2 |
| Six sections move to `status.py` | 1 |
| `collect(run_date)` preserves digest file selection | 2 |
| **`build_digest` consumes `collect()`, never its own list** | 2 (+ a test asserting it) |
| Outcomes reuse `resolve_exit_code`, not a flat `errors > 0` | 2 |
| Missing log → BAD, malformed log → UNKNOWN | 2 |
| Every non-OK verdict carries a placeholder-free fix | 2 (+ a test asserting it) |
| DuckDB grades the oldest view, not the newest | 6 |
| Both undelivered queues counted | 5 |
| `UNKNOWN` is not `OK` | 2 (type ordering + test), 8 (documented) |
| Check 1 launchd, WARN-capped | 4 |
| Checks 2/3 nightly + intraday | 2 |
| Check 4 coverage, any timeframe under threshold | 2 |
| Check 5 Silver deltas | 2 (regressions) + 7 (delta) |
| Check 6 DuckDB without importing duckdb | 6 |
| Check 7 disk, reuses `MDW_FLATFILE_MIN_FREE_GB` | 2 |
| Check 8 undelivered backlog | 5 |
| Check 9 quality jobs | 2 |
| `collect()` never raises | 3 (`_safe` + test) |
| Terminal renderer, exit 0 always | 3 |
| No `--json`, no dead-man switch | not implemented, by design |
| Testing requirements | every task |

**Type consistency:** `Section(name, verdict, lines, fix)` and `Verdict.{OK,UNKNOWN,WARN,BAD}` are used identically in Tasks 2–7. `_coverage_headline` (status-side indirection) is distinct from `coverage_headline` (catalog-side) by design — Task 6 defines both.

**Known follow-up, deliberately out of scope:** a run crossing UTC midnight splits its log across two files, so a date-scoped check can read an incomplete run. Not production-reachable — the daily job starts at 06:00 UTC with a 3.27h healthy peak — and fixing it means changing how every job names its log.
