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
- Produces: `status.Verdict` (enum: `OK`, `WARN`, `BAD`, `UNKNOWN`, with a `.glyph` property), `status.Section` (frozen dataclass: `name: str`, `verdict: Verdict`, `lines: list[str]`, `fix: str | None = None`). All six section functions now return `Section` instead of `list[str]`.

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


def test_a_missing_log_is_unknown_not_ok(tmp_path: Path) -> None:
    """The defect being fixed: '(not found)' used to render like a healthy line."""
    section = _outcomes_section("2026-08-10", tmp_path)
    assert section.verdict is Verdict.UNKNOWN
    assert section.verdict is not Verdict.OK


def test_outcomes_with_no_errors_is_ok(tmp_path: Path) -> None:
    (tmp_path / "daily_update_2026-08-10.log").write_text(_EQUITY_OK, encoding="utf-8")
    assert _outcomes_section("2026-08-10", tmp_path).verdict is Verdict.OK


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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_status.py -q`
Expected: FAIL — `ImportError: cannot import name 'Section'`.

- [ ] **Step 3: Add the types and the verdicts**

In `livewire_scripts/status.py`:

```python
import enum
from dataclasses import dataclass, field


class Verdict(enum.Enum):
    """Ordered worst-last so `max()` over a run of checks is the run's verdict.

    UNKNOWN outranks OK deliberately. A check that could not measure has not
    passed — rendering it green is exactly how coverage stayed dead for four
    weeks while the digest printed a line every night.
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

Then change each of the six functions to build and return a `Section`. Keep every existing `lines.append(...)` exactly as it is; only the `return` changes. Verdict rules:

- `_outcomes_section` — no summaries → `UNKNOWN`, fix `check that com.livewire.daily-update ran: launchctl list | grep livewire`. Any summary with `errors` > 0 → `WARN`. Otherwise `OK`.
- `_phases_section` — no summary → `UNKNOWN`, same fix wording but naming `com.livewire.intraday-catchup`. `summary["failed"]` non-empty → `BAD`, fix `python scripts/livewire_ingest.py daily-backfill`. Else `summary["degraded"]` non-empty → `WARN`, fix `nc -z 127.0.0.1 4001  # IB Gateway: 2FA/maintenance, do not restart from this repo`. Else `OK`.
- `_silver_section` — no summary → `UNKNOWN`, fix `python scripts/livewire_store.py rebuild-silver --full --dry-run`. `window_regressions` > 0 → `WARN`, fix `python livewire_scripts/validate_silver_canary.py --tickers <symbol>`. Else `OK`. (The `failed` delta arrives in Task 7.)
- `_quality_jobs_section` — any warning → `WARN`, fix `grep '^WARNING:' <log>`. Else `OK`.
- `_coverage_section` — no log → `UNKNOWN`. Parse the measurement with the regex below; any timeframe whose ratio is below `_THRESHOLD` → `BAD`, fix `python scripts/livewire_quality.py coverage --target-date <measured>`. Age > `_COVERAGE_STALE_DAYS` → `BAD`, fix `launchctl start com.livewire.coverage`. An unparseable line → `UNKNOWN`. Else `OK`.
- `_disk_section` — no volume readable → `UNKNOWN`. Any volume under `_MIN_FREE_GB` → `BAD`; under `2 * _MIN_FREE_GB` → `WARN`; else `OK`. Fix `python scripts/livewire_ops.py housekeeping` (dry run is the default).

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

- [ ] **Step 4: Update `build_digest` to render verdicts and fixes**

```python
def build_digest(run_date: date, log_dir: Path, data_lake: Path) -> str:
    """Assemble the nightly digest text. Never raises on missing inputs."""
    run = run_date.isoformat()
    blocks = [f"Livewire nightly digest — {run}"]
    for section in [
        _outcomes_section(run, log_dir),
        _phases_section(run, log_dir),
        _silver_section(run, log_dir),
        _quality_jobs_section(run, log_dir),
        _coverage_section(run, log_dir),
        _disk_section(data_lake, log_dir.parent),
    ]:
        lines = [f"[{section.verdict.glyph}] {section.lines[0]}", *section.lines[1:]]
        if section.fix:
            lines.append(f"  fix: {section.fix}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks) + "\n"
```

- [ ] **Step 5: Run both test files**

Run: `uv run pytest tests/test_status.py tests/test_nightly_digest.py -q`
Expected: `test_status.py` passes. `test_nightly_digest.py` may need small edits where an assertion anchors to the start of a line; substring assertions such as `"Disk:" in out` still pass. Fix only assertions that anchor on line starts — do not weaken any assertion that checks content.

- [ ] **Step 6: Commit**

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
- Consumes: `Section`, `Verdict` and the six section functions from Task 2.
- Produces: `status.collect(run_date: date, log_dir: Path, data_lake: Path) -> list[Section]`, `status.render(sections: list[Section]) -> str`, `status.main(argv: list[str] | None = None) -> int`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_status.py`:

```python
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


def test_render_shows_the_fix_for_anything_not_ok() -> None:
    out = render([Section(name="Coverage", verdict=Verdict.BAD, lines=["Coverage:"], fix="run me")])
    assert "run me" in out
    assert "BAD" in out


def test_render_omits_the_fix_when_ok() -> None:
    out = render([Section(name="Disk", verdict=Verdict.OK, lines=["Disk: fine"], fix="run me")])
    assert "run me" not in out


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

from livewire_scripts.paths import data_lake_dir, log_dir as default_log_dir


def _safe(name: str, builder) -> Section:
    """Run one check, degrading a crash to UNKNOWN.

    collect() must never raise: nightly_digest's contract is that a missing
    input cannot suppress the whole report. A check that *crashes* is now
    visible rather than silently absent.
    """
    try:
        return builder()
    except Exception as exc:  # noqa: BLE001 - a broken check must not kill the report
        return Section(name=name, verdict=Verdict.UNKNOWN, lines=[f"{name}: check failed — {exc}"])


def collect(run_date: date, log_dir: Path, data_lake: Path) -> list[Section]:
    """Assess every cheap signal. Never raises. Never scans parquet."""
    run = run_date.isoformat()
    return [
        _safe("Daily update outcomes", lambda: _outcomes_section(run, log_dir)),
        _safe("Intraday catch-up phases", lambda: _phases_section(run, log_dir)),
        _safe("Silver rebuild", lambda: _silver_section(run, log_dir)),
        _safe("Quality jobs", lambda: _quality_jobs_section(run, log_dir)),
        _safe("Coverage", lambda: _coverage_section(run, log_dir)),
        _safe("Disk", lambda: _disk_section(data_lake, log_dir.parent)),
    ]


def render(sections: list[Section]) -> str:
    lines = ["Livewire status"]
    for section in sections:
        lines.append(f"[{section.verdict.style}][{section.verdict.glyph}][/] {section.lines[0]}")
        lines.extend(f"  {line.lstrip()}" for line in section.lines[1:])
        if section.fix and section.verdict is not Verdict.OK:
            lines.append(f"  [dim]fix:[/] {section.fix}")
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
import subprocess

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
        return Section(
            "launchd jobs",
            Verdict.BAD,
            lines,
            fix=f"launchctl load ~/Library/LaunchAgents/{missing[0]}.plist",
        )
    if nonzero:
        lines.append("  note: exit code carries no timestamp — check the matching log before acting")
        return Section("launchd jobs", Verdict.WARN, lines, fix="ls -lt ~/market-warehouse/logs/ | head")
    return Section("launchd jobs", Verdict.OK, lines)
```

Add to `collect()`, first in the list:

```python
        _safe("launchd jobs", _launchd_section),
```

and update `test_collect_returns_a_section_per_check` to expect 7.

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
def _undelivered_section(log_dir: Path) -> Section:
    """Count alerts that could not be sent.

    No severity ladder by age. 4,408 files whose newest is a week old is a
    historic pile-up; one file from an hour ago is an active failure; a rule
    that tries to tell them apart would be guessing. Print both numbers and let
    the reader judge.
    """
    directory = Path(os.environ.get("MDW_UNDELIVERED_DIR", log_dir / "quality_alerts_undelivered"))
    try:
        entries = [p for p in directory.iterdir() if p.is_file()]
    except FileNotFoundError:
        return Section("Undelivered alerts", Verdict.OK, ["Undelivered alerts: none"])
    except OSError as exc:
        return Section("Undelivered alerts", Verdict.UNKNOWN, [f"Undelivered alerts: {exc}"])

    if not entries:
        return Section("Undelivered alerts", Verdict.OK, ["Undelivered alerts: none"])

    newest = date.fromtimestamp(max(p.stat().st_mtime for p in entries))
    return Section(
        "Undelivered alerts",
        Verdict.WARN,
        [f"Undelivered alerts: {len(entries):,} file(s) in {directory}, newest {newest.isoformat()}"],
        fix=f"review then clear: ls -lt {directory} | head",
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

```python
def test_coverage_headline_reports_symbols_and_newest_date(tmp_path: Path) -> None:
    from clients.duckdb_catalog import coverage_headline

    database = tmp_path / "analytics.duckdb"
    _seed_coverage_table(database)  # existing helper in this file; reuse it
    headline = coverage_headline(database)

    assert "bronze_equity_1d" in headline
    symbols, last = headline["bronze_equity_1d"]
    assert symbols > 0
    assert last is not None


def test_coverage_headline_raises_when_the_catalog_was_never_built(tmp_path: Path) -> None:
    from clients.duckdb_catalog import coverage_headline

    with pytest.raises(FileNotFoundError):
        coverage_headline(tmp_path / "absent.duckdb")
```

If `tests/test_duckdb_catalog.py` has no `_seed_coverage_table` helper, build the fixture the way the existing coverage-table tests in that file already do — reuse their setup rather than inventing a second one.

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

    Raises FileNotFoundError when the catalog has never been built.
    """
    path = Path(database) if database is not None else default_database()
    if not path.exists():
        raise FileNotFoundError(path)
    with open_catalog(path) as con:
        rows = con.execute(
            "SELECT view_name, count(*), max(last_date) FROM coverage GROUP BY view_name ORDER BY view_name"
        ).fetchall()
    return {str(name): (int(count), last) for name, count, last in rows}
```

Add `from datetime import date` to the module imports if absent.

- [ ] **Step 4: Write the failing tests for the status check**

```python
def test_duckdb_check_is_unknown_when_duckdb_is_not_installed(monkeypatch) -> None:
    """~/market-warehouse/.venv genuinely has no duckdb. A status command that
    cannot start in one of the three real environments is worthless."""
    import builtins

    real_import = builtins.__import__

    def _no_duckdb(name, *args, **kwargs):
        if name == "clients.duckdb_catalog":
            raise ImportError("No module named 'duckdb'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_duckdb)
    assert _duckdb_section(date(2026, 8, 10)).verdict is Verdict.UNKNOWN


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


def _sessions_behind(newest: date, target: date, limit: int = 10) -> int:
    """Trading sessions between *newest* and *target*, saturating at *limit*."""
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
    except Exception as exc:  # noqa: BLE001 - a broken catalog must not kill the report
        return Section("DuckDB catalog", Verdict.UNKNOWN, [f"DuckDB catalog: {exc}"])

    dates = [last for _count, last in headline.values() if last is not None]
    if not dates:
        return Section(
            "DuckDB catalog",
            Verdict.UNKNOWN,
            ["DuckDB catalog: table holds no dated rows"],
            fix="python scripts/livewire_store.py duckdb build",
        )

    newest = max(dates)
    behind = _sessions_behind(newest, target)
    lines = ["DuckDB catalog:", f"  newest last_date={newest.isoformat()}  ({behind} session(s) behind {target})"]
    for view_name, (count, last) in sorted(headline.items()):
        lines.append(f"  {view_name:<24} {count:>7,} symbols  last={last}")
    if behind > _COVERAGE_STALE_DAYS:
        return Section("DuckDB catalog", Verdict.BAD, lines, fix="python scripts/livewire_store.py duckdb build")
    if behind > 1:
        return Section("DuckDB catalog", Verdict.WARN, lines, fix="python scripts/livewire_store.py duckdb build")
    return Section("DuckDB catalog", Verdict.OK, lines)
```

Add to `collect()`, after the coverage check:

```python
        _safe("DuckDB catalog", lambda: _duckdb_section(run_date)),
```

and update the count test to 9.

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
    """
    current = f"daily_update_{run_date}.log"
    for path in sorted(log_dir.glob("daily_update_*.log"), reverse=True):
        if path.name >= current:
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
uv run pytest tests/ -m "not integration" --cov=clients --cov=scripts --cov=livewire_scripts --cov-fail-under=95 -W error::RuntimeWarning
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
| `collect(run_date)` preserves digest file selection | 3 |
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
