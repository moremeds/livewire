# Livewire Ledger L1 Implementation Plan

> **For agentic workers:** Execute with the user's `/execute-plan` skill (worktree in `.worktrees/ledger-l1/`, straight-through implementation, milestone commits, evidence-based verification). Do NOT use subagent-driven-development or parallel dispatch. Steps use `- [ ]` checkboxes.

**Goal:** Every scheduled job writes its facts to an append-only parquet ledger, and every reader (`status`, watchdog, digest) reads the ledger instead of re-parsing log prose. The 4h total job deadline becomes per-lane budgets, and the lane order becomes no-fallback-first, so neither a slow lane nor a down IB Gateway can starve the cheap IB-only lanes.

**Architecture:** `clients/ledger.py` declares six pyarrow schemas and two functions — `emit()` publishes one parquet file per (table, date, run) through the existing `clients/parquet_io.py` temp→validate→`os.replace` + `fcntl.flock` path; `query()` delegates to a new `ledger_query()` in `clients/duckdb_catalog.py` (the only module allowed to import duckdb) which registers an in-memory view per table over `read_parquet(..., union_by_name=true)`. `run_daily_update_job.main()` mints `LW_RUN_ID`, emits `runs`, and `_run_scheduled_lane` emits `lane_results` on entry and on terminal. `status.collect()` becomes a list of `(name, sql)` `CHECKS` executed against those views. The watchdog becomes a caller of `collect()`.

**Tech Stack:** Python 3.13, uv, pyarrow, DuckDB (read-only), pytest ≥95% coverage gate.
**Spec:** docs/superpowers/specs/2026-09-02-livewire-ledger-design.md (§1–§3, §7, §8)

## Global Constraints

- `uv run` only; never bare python/pip.
- No Co-Authored-By or AI trailers in commit messages.
- Coverage gate 95% (`--cov=clients --cov=scripts --cov-fail-under=95`).
- Every deleted log-parser has its test deleted in the same task; every new check has one test.
- Ledger root: env `LW_LEDGER_ROOT`, default `<lake>/ledger`; run id env `LW_RUN_ID`.
- Nothing in this PR writes bars or touches `analytics.duckdb`.
- Consolidate: each task's "Files" block lists what it deletes.

## Decisions taken while writing this plan (each stated once, with its reason)

1. **Append strategy (Task 1): numbered files `<run_id>-<seq>.parquet`.** Read-concat-rewrite would violate the spec's own "a file is never rewritten" (§1) and needs a read under the lock; `<seq>` needs only a directory listing. Simpler, so it wins.
2. **`emit` adds one column, `seq int64`, to every schema** — the row's index in its file. `parquet_io.publish_parquet` validates the sort column ascending and duplicate-free (`clients/parquet_io.py:98-104`), and no spec column is unique per row (`started` repeats). `seq` gives free row-count + atomicity validation with zero duplicated publish code. Callers never pass it; passing it is an "extra column" error.
3. **`query()` lives in `clients/duckdb_catalog.py`, not `clients/ledger.py`.** `tests/test_duckdb_containment.py:28-37` walks the **whole AST** (`ast.walk`) of every file under `clients/`, `livewire_scripts/`, `scripts/`, so a function-local `import duckdb` counts exactly like a module-level one; the allow-list is `{clients/duckdb_catalog.py, livewire_scripts/duckdb_catalog_cli.py}`. `clients/ledger.py` imports `duckdb_catalog`, never `duckdb`, and leaves the containment test untouched.
4. **`coverage_footer_cache.json` is KEPT, against spec §3's delete list.** It is not a parsed-state file — it is a per-parquet `(mtime, size) → max date` cache (`livewire_scripts/coverage_report.py:225-256`, `:486`) whose only job is to keep the cold exFAT footer walk from being re-paid every night. Deleting it re-opens the cost behind pm:2026-08-02-coverage-budget-expired-silently. Nothing parses it, so it is not part of the three-parsers defect this spec closes. Reported as a spec correction, not silently dropped.
5. **`sync_runner` is alive and is NOT deleted** — see Task 0(b).
6. **`measurements.value` is `float64`, so `last_session` is stored as epoch days** (`(session - date(1970,1,1)).days`), not an ISO string. The column has nowhere to put text and adding one would churn every table.
7. **No new IB socket check is written.** The in-lane preflight already exists and already exits 86 — see Task 4(b).
8. **`run_with_retries` is instrumented too, not only `_run_scheduled_lane`.** futures, cmdty and equity do NOT go through `_run_scheduled_lane` — `main()` runs them through `run_with_retries` (`livewire_scripts/run_daily_update_job.py:482`, which calls `run_daily_update_attempt` at `:515`). Instrumenting only the lane runner would mean: no `lane_results` and no `last_session` row for futures/cmdty (so "IB-only lanes behind" would read UNKNOWN forever in production — the exact check this PR exists to add), no equity row for `silver_is_blocked()` (an equity failure would stop blocking Silver — a regression against today's behaviour), and no per-lane budget on the three lanes that most need one. So `run_with_retries` emits the entry row before the attempt loop, the terminal row after it, the `last_session` measurement, and passes `timeout=LANE_BUDGET_S.get(done_scope, DEFAULT_LANE_BUDGET_S)` down to `run_daily_update_attempt`. Task 4.2.
9. **The watchdog no longer assumes the run has finished, because nothing guarantees it any more.** The old `MDW_DAILY_JOB_DEADLINE_SECONDS` (4h from 06:00Z) is what made "run finished before the 10:30Z watchdog" true; per-lane budgets sum to 9h, so at 10:30Z a healthy run may legitimately still be in a lane. Grading its lanes then would reintroduce pm:2026-08-16-watchdog-raced-quality-marker in a new costume. Two mechanisms, both in Task 5: a `Daily update finished` check that WARNs (never BADs) with the elapsed minutes while the run is still open, and `_last_run_id` returning the run id **only for a closed run**, so every per-run lane check reads UNKNOWN — not BAD — while the run is open.
10. **`runs.verdict` is translated to a `Verdict` name inside the SQL, not in Python.** The ledger's run vocabulary is `OK`/`DEGRADED`/`FAILED`/`UNKNOWN` (spec §8) and `status.Verdict` has `OK`/`UNKNOWN`/`WARN`/`BAD` (`livewire_scripts/status.py:51-77`); `run_check` does `Verdict[row["verdict"]]`, so an unmapped `'DEGRADED'` is a `KeyError` on every degraded night. The mapping (`FAILED`→`BAD`, `DEGRADED`→`WARN`, `OK`→`OK`, everything else `UNKNOWN`) lives in the check's `case` expression, so the two vocabularies stay independent and `run_check` keeps one rule.
11. **"IB-only lanes behind" uses 4 calendar days of slack, not 2.** The measurement is calendar days between `last_session` and today, and Friday's session is 3 calendar days old every Monday — a threshold of 2 pages every Monday morning forever. `IB_LANE_SLACK_DAYS = 4` absorbs a weekend plus one holiday. The cost is stated rather than hidden: one night of blindness after a long weekend. L2 replaces the arithmetic with the XNYS calendar and drops the slack back to sessions.

---

## Task 0 — Preflight (ops + verification, no code)

**Files:** none changed.

- [ ] (a) On the mini, stop the weekly universe-refresh job for the duration of the cutover (it is the one job that runs from the repo checkout, pm:2026-09-01-universe-refresh-runs-from-repo, so it would run half-old code against a half-new ledger):

  ```bash
  ssh macmini 'launchctl unload ~/Library/LaunchAgents/com.livewire.universe-refresh.plist && (launchctl list | grep -c universe-refresh || true)'
  ```

  Expected printed output: `0`. (`grep -c` exits 1 on a zero count, which is the success case here — hence the `|| true`.)
  **Reversal, recorded here so it is not re-derived:** `ssh macmini 'launchctl load ~/Library/LaunchAgents/com.livewire.universe-refresh.plist'`. Reload it in the same session that promotes L1.

- [ ] (b) **sync_runner caller finding — the spec's unverified precondition, now verified.** `grep -rn "sync_runner\|daily-backfill"` across `scripts/`, `livewire_scripts/`, `launchd/`, `docs/`:

  | Caller                                                                  | Site                                                                                                                                                                                                                |
  | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `livewire_ingest.py daily-backfill` (CLI)                               | `scripts/livewire_ingest.py:75`, `:107`, `:123`                                                                                                                                                                     |
  | `livewire.py sync --full` (CLI)                                         | `scripts/livewire.py:116`                                                                                                                                                                                           |
  | **the intraday-catchup launchd job**                                    | `livewire_scripts/run_intraday_catchup_job.py:73` builds `[python, livewire_ingest.py, "daily-backfill"]`; `launchd/com.livewire.intraday-catchup.plist.example:14` runs `livewire_ops.py run-intraday-catchup-job` |
  | `run_daily_update_job` imports `TIMEOUT_EXIT_CODE` from it              | `livewire_scripts/run_daily_update_job.py:21`                                                                                                                                                                       |
  | `ingest_daily_flatfiles` imports `EQUITY_PRESETS, ticker_union` from it | `livewire_scripts/ingest_daily_flatfiles.py:27`                                                                                                                                                                     |
  | `status` prints it as a fix command                                     | `livewire_scripts/status.py:193`                                                                                                                                                                                    |

  **DECISION: `sync_runner` is live production code reached by a scheduled launchd job (intraday-catchup, 05:00Z). It is NOT deleted.** L1 does the "fix the twin" half only: `sync_runner.run_phase` gains a `lane_results` emit with `job='intraday-catchup'`, reusing `clients/ledger.emit`, so the intraday phases are as visible as the daily lanes. Its `run_phase:126` / `_phase:223` / `phase_timeout_seconds:114` **stay** — routing them through `_run_scheduled_lane` would import the daily orchestrator into the intraday job and couple two schedules; that consolidation is outside L1's one-PR scope and no §7 acceptance criterion needs it.

- [ ] (c) Create the worktree:

  ```bash
  git checkout main && git pull \
    && git worktree add .worktrees/ledger-l1 -b feat/ledger-l1 main \
    && grep -q '^\.worktrees/' .gitignore && echo WORKTREE-OK
  ```

  Expected output ends with `WORKTREE-OK`. All later paths are relative to `.worktrees/ledger-l1/`.

---

## Task 1 — `clients/ledger.py`: schemas + `emit`

**Files:** create `clients/ledger.py`, `tests/test_ledger.py`. Deletes: nothing yet.
**Interfaces:**

```python
LEDGER_TABLES: dict[str, pa.Schema]     # runs, lane_results, measurements, findings, evidence, executions
def ledger_root() -> Path               # LW_LEDGER_ROOT, else data_lake_dir()/"ledger"
def new_run_id(job: str) -> str         # f"{job}-{utcnow:%Y%m%dT%H%M%SZ}-{os.getpid()}"
def emit(table: str, rows: list[dict], *, run_id: str) -> Path
```

- [ ] **1.1 Failing test.** Create `tests/test_ledger.py`:

  ```python
  """Tests for clients/ledger.py — the append-only run ledger."""
  # imports: the module under test, pytest, pathlib/datetime, and `clients.ledger`.
  @pytest.fixture
  def root(tmp_path: Path, monkeypatch) -> Path:
      monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
      return tmp_path / "ledger"
  def _run_row() -> dict:
      now = datetime(2026, 9, 2, 6, 0, tzinfo=UTC)
      return {
          "run_id": "daily-update-20260902T060000Z-42", "job": "daily-update",
          "host": "macmini", "release_sha": "abc123", "presets_sha": "p1",
          "registry_sha": "r1", "started": now, "ended": None,
          "exit_code": None, "verdict": None,
      }
  def test_a_run_row_round_trips(root: Path) -> None:
      path = ledger.emit("runs", [_run_row()], run_id="daily-update-20260902T060000Z-42")
      assert path.parent.parent == root / "runs"
      back = pq.read_table(path).to_pylist()
      assert back[0]["job"] == "daily-update"
      assert back[0]["verdict"] is None
      assert back[0]["seq"] == 0
  @pytest.mark.parametrize("table", sorted(ledger.LEDGER_TABLES))
  def test_every_table_round_trips_a_row(root: Path, table: str) -> None:
      path = ledger.emit(table, [ledger.example_row(table)], run_id="t-20260902T060000Z-1")
      assert pq.read_table(path).num_rows == 1
  def test_an_extra_column_raises(root: Path) -> None:
      with pytest.raises(ValueError, match="unexpected column"):
          ledger.emit("runs", [_run_row() | {"nonsense": 1}], run_id="r1")
  def test_a_missing_column_raises(root: Path) -> None:
      row = _run_row()
      del row["host"]
      with pytest.raises(ValueError, match="missing column"):
          ledger.emit("runs", [row], run_id="r1")
  def test_a_caller_may_not_pass_seq(root: Path) -> None:
      with pytest.raises(ValueError, match="unexpected column"):
          ledger.emit("runs", [_run_row() | {"seq": 3}], run_id="r1")
  def test_zero_rows_is_refused(root: Path) -> None:
      with pytest.raises(ValueError, match="zero rows"):
          ledger.emit("runs", [], run_id="r1")
  def test_a_second_emit_from_one_run_never_rewrites_the_first(root: Path) -> None:
      first = ledger.emit("runs", [_run_row()], run_id="r1")
      second = ledger.emit("runs", [_run_row()], run_id="r1")
      assert first.name == "r1.parquet"
      assert second.name == "r1-1.parquet"
      assert first.exists() and second.exists()
  def test_the_root_comes_from_the_environment(tmp_path: Path, monkeypatch) -> None:
      monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "elsewhere"))
      assert ledger.ledger_root() == tmp_path / "elsewhere"
  def test_the_default_root_is_under_the_lake(tmp_path: Path, monkeypatch) -> None:
      monkeypatch.delenv("LW_LEDGER_ROOT", raising=False)
      monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))
      assert ledger.ledger_root() == tmp_path / "lake" / "ledger"
  def test_a_run_id_carries_job_and_pid() -> None:
      rid = ledger.new_run_id("daily-update")
      assert rid.startswith("daily-update-") and rid.endswith(f"-{os.getpid()}")
  ```

  Run: `uv run pytest tests/test_ledger.py -v` → expect a collection error, `ModuleNotFoundError: No module named 'clients.ledger'`, 0 passed.

- [ ] **1.2 Implement.** Create `clients/ledger.py`:

  ```python
  """The append-only run ledger: what happened, as rows, not as log prose.
  Three readers used to reconstruct "what happened last night" from one prose
  artifact — status.py, the watchdog, the digest — which is why a fix in one
  left the other two broken (spec 2026-09-02-ledger §0). They now read here.
  Append-only: a file is NEVER rewritten. A correction is a new row; a second
  emit from the same run is a new numbered file. Readers dedupe by taking the
  latest row per key.

  `seq` orders rows within ONE file only. Every emit writes its own file and
  restarts `seq` at 0, so ordering by `seq` across files is meaningless — an
  entry row (seq 0) and its terminal row (also seq 0) are indistinguishable
  by it. Order across files by `ended nulls first`, never by `seq`.
  """
  from __future__ import annotations
  import os
  from datetime import UTC, datetime
  from pathlib import Path
  import pyarrow as pa
  from clients.parquet_io import publish_parquet, symbol_lock
  from livewire_scripts.paths import data_lake_dir
  _TS = pa.timestamp("us", tz="UTC")
  #: Appended by `emit`, never by a caller: the row's index inside its file.
  #: `publish_parquet` validates its sort column ascending and duplicate-free
  #: (clients/parquet_io.py:98-104) and no spec column is unique per row.
  SEQ_COLUMN = "seq"
  LEDGER_TABLES: dict[str, pa.Schema] = {
      "runs": pa.schema([
          ("run_id", pa.string()), ("job", pa.string()), ("host", pa.string()),
          ("release_sha", pa.string()), ("presets_sha", pa.string()),
          ("registry_sha", pa.string()), ("started", _TS), ("ended", _TS),
          ("exit_code", pa.int64()), ("verdict", pa.string()),
          (SEQ_COLUMN, pa.int64()),
      ]),
      "lane_results": pa.schema([
          ("run_id", pa.string()), ("lane", pa.string()), ("started", _TS),
          ("ended", _TS), ("exit_code", pa.int64()), ("budget_s", pa.float64()),
          ("elapsed_s", pa.float64()), ("outcome", pa.string()),
          ("blocker", pa.string()), (SEQ_COLUMN, pa.int64()),
      ]),
      "measurements": pa.schema([
          ("name", pa.string()), ("scope", pa.string()), ("measured_at", _TS),
          ("value", pa.float64()), ("unit", pa.string()), ("source", pa.string()),
          ("run_id", pa.string()), (SEQ_COLUMN, pa.int64()),
      ]),
      "findings": pa.schema([
          ("finding_hash", pa.string()), ("gap_class", pa.string()),
          ("symbol", pa.string()), ("asset_class", pa.string()),
          ("timeframe", pa.string()), ("sessions", pa.list_(pa.string())),
          ("tier", pa.string()), ("source", pa.string()), ("run_id", pa.string()),
          (SEQ_COLUMN, pa.int64()),
      ]),
      "evidence": pa.schema([
          ("evidence_hash", pa.string()), ("kind", pa.string()),
          ("subject", pa.string()), ("payload_json", pa.string()),
          ("source_url", pa.string()), ("fetched_at", _TS),
          ("proposer", pa.string()), ("run_id", pa.string()),
          (SEQ_COLUMN, pa.int64()),
      ]),
      "executions": pa.schema([
          ("evidence_hash", pa.string()), ("script", pa.string()),
          ("attempt", pa.int64()), ("args_json", pa.string()),
          ("release_sha", pa.string()), ("started", _TS), ("ended", _TS),
          ("exit_code", pa.int64()), ("receipt_json", pa.string()),
          ("run_id", pa.string()), (SEQ_COLUMN, pa.int64()),
      ]),
  }
  def ledger_root() -> Path:
      override = os.environ.get("LW_LEDGER_ROOT")
      return Path(override).expanduser() if override else data_lake_dir() / "ledger"
  def new_run_id(job: str) -> str:
      """`<job>-<utc-ts>-<pid>` (spec §8). Children never mint one; they read LW_RUN_ID."""
      return f"{job}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{os.getpid()}"
  def example_row(table: str) -> dict:
      """A schema-complete row of defaults, for tests and for `ledger emit` docs."""
      now = datetime.now(UTC)
      defaults = {pa.string(): None, pa.int64(): None, pa.float64(): None,
                  _TS: now, pa.list_(pa.string()): []}
      return {f.name: defaults[f.type] for f in LEDGER_TABLES[table] if f.name != SEQ_COLUMN}
  def _validate(table: str, rows: list[dict]) -> None:
      expected = {f.name for f in LEDGER_TABLES[table]} - {SEQ_COLUMN}
      for index, row in enumerate(rows):
          keys = set(row)
          if extra := sorted(keys - expected):
              raise ValueError(f"{table} row {index}: unexpected column(s) {extra}")
          if missing := sorted(expected - keys):
              raise ValueError(f"{table} row {index}: missing column(s) {missing}")
  def _next_path(directory: Path, run_id: str) -> Path:
      """`<run_id>.parquet`, then `<run_id>-1.parquet`, … Never rewrite a file."""
      candidate = directory / f"{run_id}.parquet"
      suffix = 0
      while candidate.exists():
          suffix += 1
          candidate = directory / f"{run_id}-{suffix}.parquet"
      return candidate
  def emit(table: str, rows: list[dict], *, run_id: str) -> Path:
      """Publish `rows` as one parquet file. Raises on an unknown table or bad columns."""
      if table not in LEDGER_TABLES:
          raise ValueError(f"unknown ledger table {table!r}")
      if not rows:
          raise ValueError(f"{table}: refusing to emit zero rows")
      _validate(table, rows)
      numbered = [row | {SEQ_COLUMN: index} for index, row in enumerate(rows)]
      arrow = pa.Table.from_pylist(numbered, schema=LEDGER_TABLES[table])
      directory = ledger_root() / table / f"date={datetime.now(UTC):%Y-%m-%d}"
      directory.mkdir(parents=True, exist_ok=True)
      # One lock per (table, date): two processes must not pick the same
      # `<run_id>-<seq>` name between the exists() check and the rename.
      with symbol_lock(directory / f"{table}.dir"):
          return publish_parquet(_next_path(directory, run_id), arrow, sort_column=SEQ_COLUMN)
  ```

- [ ] **1.3 Run:** `uv run pytest tests/test_ledger.py -v` → expect `15 passed` (9 named tests + 6 parametrised table round-trips), 0 failed.

- [ ] **1.4 Commit:** `git add clients/ledger.py tests/test_ledger.py && git commit -m "feat(ledger): six append-only parquet tables and emit()"`

---

## Task 2 — `clients/ledger.py`: `query(sql)` + housekeeping protection

**Files:** edit `clients/duckdb_catalog.py` (add `ledger_query`), `clients/ledger.py` (add `query`), `livewire_scripts/housekeeping.py:46`; edit `tests/test_ledger.py`, `tests/test_housekeeping.py`. Deletes: nothing.
**Interfaces:** `duckdb_catalog.ledger_query(sql, *, root, tables) -> list[dict]`; `ledger.query(sql) -> list[dict]`.

- [ ] **2.1 Failing tests.** Append to `tests/test_ledger.py`:

  ```python
  def test_query_reads_back_emitted_rows(root: Path) -> None:
      ledger.emit("runs", [_run_row()], run_id="r1")
      assert ledger.query("select job, verdict from runs") == [{"job": "daily-update", "verdict": None}]
  def test_a_table_with_no_files_is_an_empty_view_not_a_sql_error(root: Path) -> None:
      """A check against a table nothing wrote must read UNKNOWN, not explode."""
      ledger.emit("runs", [_run_row()], run_id="r1")
      assert ledger.query("select * from lane_results") == []
  def test_every_table_is_queryable_on_a_completely_empty_root(root: Path) -> None:
      for table in ledger.LEDGER_TABLES:
          assert ledger.query(f"select * from {table}") == []
  def test_files_from_two_dates_are_one_view(root: Path) -> None:
      import shutil
      ledger.emit("runs", [_run_row()], run_id="r1")
      source = next((root / "runs").glob("date=*/*.parquet"))
      later = root / "runs" / "date=2099-01-01"
      later.mkdir(parents=True)
      shutil.copy(source, later / "r2.parquet")
      assert len(ledger.query("select run_id from runs")) == 2
  ```

  In `tests/test_housekeeping.py`, add `"ledger/runs/date=2026-09-02/r1.parquet"` to the `relative` parametrize list feeding `TestProtectedPathsSurvive` (`:59`, `:72`), plus:

  ```python
  def test_the_ledger_is_protected_by_name(self) -> None:
      from livewire_scripts.housekeeping import PROTECTED_LAKE_DIRS
      assert "ledger" in PROTECTED_LAKE_DIRS
  ```

  Run: `uv run pytest tests/test_ledger.py -k query -v` → expect `AttributeError: module 'clients.ledger' has no attribute 'query'`.
  Run: `uv run pytest tests/test_housekeeping.py -k ledger -v` → expect `AssertionError: assert 'ledger' in frozenset({'raw', 'repairs'})`.

- [ ] **2.2 Implement.** In `clients/duckdb_catalog.py`, after `default_database()` (`:147`):

  ```python
  _LEDGER_VIEW_SQL = (
      "CREATE OR REPLACE TEMP VIEW {name} AS "
      "SELECT * FROM read_parquet({glob!r}, union_by_name=true)"
  )
  def ledger_query(sql: str, *, root: Path, tables: Iterable[str]) -> list[dict]:
      """Run `sql` over the ledger, in memory, read-only.
      A table with no parquet under it still gets a view — an empty one — so a
      check against a table nothing wrote reads UNKNOWN (zero rows) instead of
      dying on `Table with name X does not exist`. That distinction is the
      whole grading rule: a detector with no output is dead, not healthy.
      """
      con = duckdb.connect(":memory:")
      try:
          for name in tables:
              directory = Path(root) / name
              if any(directory.glob("*/*.parquet")):
                  con.execute(_LEDGER_VIEW_SQL.format(name=name, glob=f"{directory}/*/*.parquet"))
              else:
                  con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM (SELECT 1) WHERE false")
          cursor = con.execute(sql)
          columns = [d[0] for d in cursor.description]
          return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
      finally:
          con.close()
  ```

  In `clients/ledger.py`:

  ```python
  def query(sql: str) -> list[dict]:
      """Read the ledger. Every table is a view; an unwritten table is empty, not missing.
      `duckdb` is imported by `clients/duckdb_catalog.py` and nowhere else —
      `tests/test_duckdb_containment.py` walks the whole AST of every runtime
      module, so even a function-local import here would break that contract.
      """
      from clients.duckdb_catalog import ledger_query
      return ledger_query(sql, root=ledger_root(), tables=tuple(LEDGER_TABLES))
  ```

  In `livewire_scripts/housekeeping.py:46`:

  ```python
  #: Lake subtrees no retention rule may ever enter. Matched on the path's parts,
  #: so `repairs/triage/anything/deeper` is protected too. `ledger` joins them
  #: because it is the run record every reader grades against: a pruned ledger
  #: file is a night that silently never happened.
  PROTECTED_LAKE_DIRS = frozenset({"raw", "repairs", "ledger"})
  ```

- [ ] **2.3 Run:** `uv run pytest tests/test_ledger.py tests/test_housekeeping.py tests/test_duckdb_containment.py -v` → expect all passed, including `test_duckdb_is_imported_only_by_the_catalog PASSED`.
- [ ] **2.4 Commit:** `git commit -am "feat(ledger): query() over in-memory duckdb views; protect <lake>/ledger"`

---

## Task 3 — `livewire_ops.py ledger emit|query`

**Files:** edit `scripts/livewire_ops.py:23` (`COMMANDS`), create `livewire_scripts/ledger_cli.py`, `tests/test_ledger_cli.py`. Deletes: nothing.
**Interfaces:** `livewire_scripts/ledger_cli.main(argv) -> int`.

- [ ] **3.1 Failing test.** Create `tests/test_ledger_cli.py`:

  ```python
  """Tests for `livewire_ops.py ledger emit|query` — the agent/human inbox surface."""
  # imports: the module under test, pytest, pathlib/datetime, and `clients.ledger`.
  @pytest.fixture(autouse=True)
  def root(tmp_path: Path, monkeypatch) -> Path:
      monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
      return tmp_path / "ledger"
  def _evidence(**over) -> dict:
      return {"evidence_hash": "h1", "kind": "delisting", "subject": "ZZZZ",
              "payload_json": "{}", "source_url": "https://example.invalid/x",
              "fetched_at": "2026-09-02T06:00:00+00:00", "proposer": "human",
              "run_id": "manual-1"} | over
  def test_emit_writes_one_row() -> None:
      assert ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())]) == 0
      assert ledger.query("select subject from evidence") == [{"subject": "ZZZZ"}]
  def test_emit_accepts_a_list_of_rows() -> None:
      payload = json.dumps([_evidence(evidence_hash="h1"), _evidence(evidence_hash="h2")])
      assert ledger_cli.main(["emit", "--table", "evidence", "--json", payload]) == 0
      assert len(ledger.query("select evidence_hash from evidence")) == 2
  def test_emit_uses_lw_run_id_when_set(monkeypatch) -> None:
      monkeypatch.setenv("LW_RUN_ID", "daily-update-20260902T060000Z-7")
      ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())])
      assert [p.name for p in ledger_root().glob("evidence/*/*.parquet")] == [
          "daily-update-20260902T060000Z-7.parquet"
      ]
  def test_emit_without_lw_run_id_mints_a_manual_one(monkeypatch) -> None:
      monkeypatch.delenv("LW_RUN_ID", raising=False)
      ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())])
      assert next(ledger_root().glob("evidence/*/*.parquet")).name.startswith("manual-")
  def test_a_bad_column_exits_nonzero_and_says_which(capsys) -> None:
      assert ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence(nonsense=1))]) == 1
      assert "unexpected column" in capsys.readouterr().err
  def test_query_prints_one_json_object_per_line(capsys) -> None:
      ledger_cli.main(["emit", "--table", "evidence", "--json", json.dumps(_evidence())])
      assert ledger_cli.main(["query", "select kind, subject from evidence"]) == 0
      lines = capsys.readouterr().out.strip().splitlines()
      assert [json.loads(line) for line in lines] == [{"kind": "delisting", "subject": "ZZZZ"}]
  def test_the_ops_entrypoint_dispatches_ledger() -> None:
      """The real signature, not a mock: `livewire_ops.py ledger …` must reach main()."""
      import scripts.livewire_ops as ops
      assert ops.COMMANDS["ledger"] == "livewire_scripts.ledger_cli"
      assert ops.main(["ledger", "query", "select 1 as one"]) == 0
  ```

  Run: `uv run pytest tests/test_ledger_cli.py -v` → expect `ModuleNotFoundError: No module named 'livewire_scripts.ledger_cli'`.

- [ ] **3.2 Implement.** Create `livewire_scripts/ledger_cli.py`:

  ```python
  #!/usr/bin/env python3
  """`livewire_ops.py ledger emit|query` — the one way anything writes the ledger.
  Agents, humans and cron all use this command; what differs per caller is
  LW_LEDGER_ROOT and nothing else (spec §5). No second format, no second validator.
  """
  from __future__ import annotations
  import argparse
  import json
  import os
  import sys
  from collections.abc import Sequence
  from datetime import UTC, datetime
  import pyarrow as pa
  from clients import ledger
  def _coerce(table: str, row: dict) -> dict:
      """Turn JSON scalars into the schema's types. Only timestamps need it."""
      out = dict(row)
      for field in ledger.LEDGER_TABLES[table]:
          value = out.get(field.name)
          if isinstance(value, str) and pa.types.is_timestamp(field.type):
              out[field.name] = datetime.fromisoformat(value)
      return out
  def main(argv: Sequence[str] | None = None) -> int:
      parser = argparse.ArgumentParser(prog="livewire_ops.py ledger", description="Read and write the run ledger")
      sub = parser.add_subparsers(dest="action", required=True)
      emit_p = sub.add_parser("emit", help="Append rows to one ledger table")
      emit_p.add_argument("--table", required=True, choices=sorted(ledger.LEDGER_TABLES))
      emit_p.add_argument("--json", required=True, help="One row object, or a JSON array of rows")
      query_p = sub.add_parser("query", help="Run SQL over the ledger; prints one JSON object per line")
      query_p.add_argument("sql")
      args = parser.parse_args(list(argv) if argv is not None else None)
      if args.action == "emit":
          payload = json.loads(args.json)
          rows = payload if isinstance(payload, list) else [payload]
          run_id = os.environ.get("LW_RUN_ID") or f"manual-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{os.getpid()}"
          try:
              path = ledger.emit(args.table, [_coerce(args.table, r) for r in rows], run_id=run_id)
          except ValueError as exc:
              print(str(exc), file=sys.stderr)
              return 1
          print(str(path))
          return 0
      for row in ledger.query(args.sql):
          print(json.dumps(row, default=str))
      return 0
  if __name__ == "__main__":  # pragma: no cover
      raise SystemExit(main())
  ```

  In `scripts/livewire_ops.py:23` add to `COMMANDS`: `"ledger": "livewire_scripts.ledger_cli",`.

- [ ] **3.3 Run:** `uv run pytest tests/test_ledger_cli.py -v` → expect `7 passed`.
- [ ] **3.4 Commit:** `git commit -am "feat(ledger): livewire_ops.py ledger emit|query"`

---

## Task 4 — Orchestrator: lane order, per-lane budgets, `runs` / `lane_results` / `last_session`

**Files:** edit `livewire_scripts/run_daily_update_job.py`, `livewire_scripts/sync_runner.py`, `tests/test_run_daily_update_job.py`; rewrite `livewire_scripts/check_daily_update_watchdog.py` and `tests/test_check_daily_update_watchdog.py` (step 4.3 — they import the symbols this task deletes, so they cannot wait for Task 6).
**Deletes:** `JobDeadline` (`:196-233`), every `deadline=` parameter and argument, `JOB_COMPLETE_MARKER` (`:113`), `job_tail_complete` (`:116`), `completed_scopes` (`:388`), `skipped_scopes` (`:403`), `log_has_completion_marker` (`:421`), `_LEGACY_DONE_TIMESTAMP_RE` (`:386`), `undelivered_dir` (`:639`), `record_undelivered_alert` (`:649`); tests `TestJobDeadline` (`tests/test_run_daily_update_job.py:1005-1028`), `test_a_lane_started_past_the_deadline_never_runs` (`:1043`), `test_a_healthy_attempt_spends_the_remaining_deadline` (`:1056`), `test_completed_scopes_parses_per_asset_markers` (`:235`), `test_legacy_done_line_counts_as_wildcard` (`:251`), and the completion-marker half of `test_extract_error_summary_and_completion_marker` (`:213`, keep the error-summary half).

**Interfaces:**

```python
LANE_ORDER: tuple[str, ...]
LANE_BUDGET_S: dict[str, float]
def _run_scheduled_lane(config, command, label, done_scope, *, env, runner, now_fn) -> int
def run_daily_update_attempt(command, log_file, env=None, runner=..., timeout=None) -> CompletedProcess
def silver_is_blocked() -> str | None
```

### 4(a) Lane order: no-fallback-first

Today `main()` (`:846-870`) runs corporate-actions → equity → futures → cmdty → CBOE → FX → silver. Corporate-actions alone ran 8h39m (2026-08-29) and 3h57m (2026-09-01), so the lanes behind it were killed both nights — and the two lanes with **no fallback at all** (futures, cmdty: IB-only, Massive does not carry them, `test_futures_and_cmdty_get_no_fallback`) are the ones that take minutes. They must run while the Gateway is known-up, before anything expensive.

**New order:** `futures → cmdty → cboe → fx → corporate-actions → equity (+ Massive fallback) → silver`.

**Data-dependency check performed before choosing this order** (grep, then read):

- The only inter-lane dependency in the module is the corporate-action store, read at `livewire_scripts/daily_update.py:974` (`_action_store_for_bronze(bronze_dir).latest_active(ticker)`). Read at `:955-975`, that call sits in the **`else` branch — equity only**; futures take `bars_to_futures_rows`, cmdty and fx take `bars_to_midpoint_rows`, and neither touches the store.
- `grep -c corporate livewire_scripts/fetch_cboe_volatility.py livewire_scripts/fetch_fx.py` → `0` and `0`.
- Silver's inputs are unchanged (equity bronze + the corporate-action store), so its gate stays "corporate-actions ok AND equity ok"; both still run before it.

### 4(b) IB fails fast inside the lane — no new mechanism

**Verified: the in-lane preflight already exists and already exits 86.** `scripts/livewire_ingest.py:116-119` calls `assert_gateway_up()` **inside the child process** — the lane, not the orchestrator, exactly as CLAUDE.md requires — gated by `_requires_ib_preflight` (`:88-99`, true for `daily` unless `--source massive`, and for every `IB_COMMANDS` member). `clients/ib_gateway_preflight.assert_gateway_up` (`:39-66`) is a `socket.create_connection` with `timeout=1.0` to `127.0.0.1:4001` and `sys.exit(GATEWAY_DOWN_EXIT_CODE)` on failure. A Gateway that accepts TCP but never answers the API is caught one layer down: `livewire_ingest.py:134` maps `IBConnectionError` to 86, and `clients/ib_client.py:161` connects with `timeout: int = 10`. Worst case ≈11s, well inside the ≤60s cap. **No new socket check is written.**

What L1 changes is only the _recording_: exit 86 becomes `outcome='blocked', blocker='ib_unreachable'` instead of `'skipped'`, so a down Gateway is a named backlog rather than an unexplained absence. `test_gateway_down_is_degraded_not_failed` keeps its semantics unchanged — the run is DEGRADED, the lane does not page, nothing is retried.

### 4(c) Every lane records the session it reached

Each lane emits `measurements(name='last_session', scope=<lane>, source='measured', value=<epoch days>)` after it runs, read from the lane's SUMMARY_JSON `target_date` (already printed by `daily_update`). Task 5 turns it into the "IB-only lanes behind" check, which is what tells the operator "IB is down and here is the backlog" instead of a silent nine-session gap.

- [ ] **4.1 Failing test.** In `tests/test_run_daily_update_job.py`, delete the tests listed above and add:

  ```python
  class TestPerLaneBudgets:
      """One slow lane must not starve the next. pm:2026-07-28-daily-job-deadline-is-a-total."""
      @pytest.fixture(autouse=True)
      def ledger_root(self, tmp_path, monkeypatch):
          monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
          monkeypatch.setenv("LW_RUN_ID", "daily-update-20260902T060000Z-1")
          return tmp_path / "ledger"
      def _lane(self, config, lane, command):
          return run_daily_update_job._run_scheduled_lane(
              config, command, lane, lane, env=None,
              runner=run_daily_update_job._run_in_own_process_group,
              now_fn=run_daily_update_job._utc_now,
          )
      def test_a_lane_over_budget_is_killed_and_the_next_lane_still_runs(self, tmp_path, monkeypatch):
          from clients import ledger
          config = _config(tmp_path)
          monkeypatch.setitem(run_daily_update_job.LANE_BUDGET_S, "cmdty", 1.0)
          # The new order: the two no-fallback IB lanes first, the slow one in
          # the middle, and a lane after it that must still get its turn.
          self._lane(config, "futures", ["true"])
          self._lane(config, "cmdty", [sys.executable, "-c", "import time; time.sleep(30)"])
          self._lane(config, "cboe", ["true"])
          rows = ledger.query(
              "select lane, outcome, exit_code from lane_results "
              "where outcome is not null order by lane"
          )
          assert rows == [
              {"lane": "cboe", "outcome": "done", "exit_code": 0},
              {"lane": "cmdty", "outcome": "timeout", "exit_code": 124},
              {"lane": "futures", "outcome": "done", "exit_code": 0},
          ]
      def test_a_lane_that_exits_86_blocks_in_under_a_second_and_the_next_lane_starts(self, tmp_path):
          """IB down must cost the run a second, not a lane budget."""
          from clients import ledger
          config = _config(tmp_path)
          started = time.monotonic()
          self._lane(config, "futures", [sys.executable, "-c", "raise SystemExit(86)"])
          self._lane(config, "cmdty", ["true"])
          assert time.monotonic() - started < 5.0
          rows = ledger.query(
              "select lane, outcome, blocker from lane_results where outcome is not null order by lane"
          )
          assert rows == [
              {"lane": "cmdty", "outcome": "done", "blocker": None},
              {"lane": "futures", "outcome": "blocked", "blocker": "ib_unreachable"},
          ]
      def test_each_lane_writes_an_entry_row_before_it_runs(self, tmp_path):
          from clients import ledger
          self._lane(_config(tmp_path), "cboe", ["true"])
          # NOT ordered by seq: each emit is its own file and seq restarts at 0,
          # so the entry row and the terminal row are both seq 0. The entry row
          # is the one with `ended` NULL.
          assert [r["outcome"] for r in ledger.query(
              "select outcome from lane_results order by ended nulls first"
          )] == [None, "done"]
      def test_run_with_retries_records_a_lane_row_and_uses_the_lane_budget(self, tmp_path, monkeypatch):
          """futures/cmdty/equity never touch _run_scheduled_lane — decision 8."""
          from clients import ledger
          config = _config(tmp_path)
          monkeypatch.setitem(run_daily_update_job.LANE_BUDGET_S, "futures", 1234.0)
          seen = {}
          def _runner(command, stdout=None, env=None, timeout=None, **_):
              seen["timeout"] = timeout
              return SimpleNamespace(returncode=0, stdout="")
          rc = run_daily_update_job.run_with_retries(
              config, ["--asset-class", "futures"], runner=_runner,
              sleep_fn=lambda _: None, completion_scope="futures",
          )
          assert rc == 0
          assert seen["timeout"] == 1234.0, "the lane budget must reach the child"
          rows = ledger.query(
              "select lane, outcome, budget_s from lane_results order by ended nulls first"
          )
          assert rows == [
              {"lane": "futures", "outcome": None, "budget_s": 1234.0},
              {"lane": "futures", "outcome": "done", "budget_s": 1234.0},
          ]
      def test_run_with_retries_records_a_blocked_lane_on_86(self, tmp_path):
          from clients import ledger
          def _runner(command, stdout=None, env=None, timeout=None, **_):
              return SimpleNamespace(returncode=run_daily_update_job.GATEWAY_DOWN_EXIT_CODE, stdout="")
          run_daily_update_job.run_with_retries(
              _config(tmp_path), ["--asset-class", "cmdty"], runner=_runner,
              sleep_fn=lambda _: None, completion_scope="cmdty",
          )
          assert ledger.query(
              "select outcome, blocker from lane_results where outcome is not null"
          ) == [{"outcome": "blocked", "blocker": "ib_unreachable"}]
      def test_main_runs_the_no_fallback_lanes_before_the_expensive_ones(self, tmp_path, monkeypatch,
                                                                        no_real_quality_spawn):
          order = []
          monkeypatch.setattr(run_daily_update_job, "build_config", lambda: _config(tmp_path))
          monkeypatch.setattr(run_daily_update_job, "_run_scheduled_lane",
                              lambda c, cmd, label, scope, **k: order.append(scope) or 0)
          monkeypatch.setattr(run_daily_update_job, "run_with_retries",
                              lambda c, args, **k: order.append(k["completion_scope"]) or 0)
          run_daily_update_job.main([])
          assert order.index("futures") < order.index("corporate-actions")
          assert order.index("cmdty") < order.index("corporate-actions")
          assert order.index("corporate-actions") < order.index("equity") < order.index("silver")
      def test_main_opens_and_closes_one_runs_row(self, tmp_path, monkeypatch, no_real_quality_spawn):
          from clients import ledger
          monkeypatch.setattr(run_daily_update_job, "build_config", lambda: _config(tmp_path))
          monkeypatch.setattr(run_daily_update_job, "run_with_retries", lambda *a, **k: 0)
          monkeypatch.setattr(run_daily_update_job, "_run_scheduled_lane", lambda *a, **k: 0)
          assert run_daily_update_job.main([]) == 0
          runs = ledger.query("select job, host, verdict from runs order by ended nulls first")
          assert [r["job"] for r in runs] == ["daily-update", "daily-update"]
          assert runs[0]["verdict"] is None and runs[1]["verdict"] == "OK"
          assert runs[0]["host"] == socket.gethostname()
      def test_the_silver_gate_reads_the_equity_lane_row_not_an_in_process_dict(self, tmp_path):
          """Spec §2: the gate reads this run's equity lane_results row."""
          from clients import ledger
          ledger.emit("lane_results", [{
              "run_id": "daily-update-20260902T060000Z-1", "lane": "equity",
              "started": datetime.now(UTC), "ended": datetime.now(UTC), "exit_code": 1,
              "budget_s": 7200.0, "elapsed_s": 1.0, "outcome": "failed", "blocker": None,
          }], run_id="daily-update-20260902T060000Z-1")
          assert run_daily_update_job.silver_is_blocked() == "equity"
      def test_a_lane_records_the_session_it_reached(self, tmp_path):
          from clients import ledger
          run_daily_update_job._emit_last_session("futures", date(2026, 9, 1))
          assert ledger.query("select name, scope, value, unit, source from measurements") == [
              {"name": "last_session", "scope": "futures", "value": 20697.0,
               "unit": "epoch_days", "source": "measured"}
          ]
  ```

  Adapt in place, same file: `test_gateway_down_is_degraded_not_failed` (`:986`) keeps its exit-code assertion and gains
  `assert ledger.query("select verdict from runs where verdict is not null") == [{"verdict": "DEGRADED"}]`;
  `test_terminal_failure_alert_non_zero` (`:626`) swaps its `undelivered_dir` assertion for
  `assert ledger.query("select script, exit_code from executions where script = 'send_alert'") == [{"script": "send_alert", "exit_code": 3}]`.
  `test_futures_and_cmdty_get_no_fallback` (`:1266`), `TestTheLaneRunnerNeverRunsTheAlert` (`:1137`) and `test_terminal_failure_sends_alert` / `_without_alert_result` (`:548`, `:598`) keep their command/runner assertions unchanged.

  Run: `uv run pytest tests/test_run_daily_update_job.py -k PerLaneBudgets -v` → expect `AttributeError: module 'livewire_scripts.run_daily_update_job' has no attribute 'LANE_BUDGET_S'` (9 errors).

- [ ] **4.2 Implement.** In `livewire_scripts/run_daily_update_job.py`:

  ```python
  #: Cheapest-first, no-fallback-first. futures and cmdty are IB-only — Massive
  #: does not carry them, so an unrun lane is a permanent gap — and they take
  #: minutes. Behind corporate-actions (3-8h observed) and equity (hours) they
  #: were killed on 2026-09-01 and 2026-09-02. Verified before reordering: the
  #: only inter-lane read is the corporate-action store at daily_update.py:974,
  #: which is inside the equity-only `else` branch; CBOE and FX import nothing
  #: from it. Silver's gate (corporate-actions ok AND equity ok) is unchanged
  #: and both still precede it.
  LANE_ORDER = ("futures", "cmdty", "cboe", "fx", "corporate-actions", "equity", "silver")
  #: Per-lane wall-clock budgets, in seconds. These replace the single
  #: MDW_DAILY_JOB_DEADLINE_SECONDS total: seven sequential lanes under one 4h
  #: budget meant corporate-actions at 8h39m (2026-08-29) killed equity and
  #: skipped Silver. Over budget now kills that lane's process group and the
  #: NEXT LANE STARTS NORMALLY. pm:2026-07-28-daily-job-deadline-is-a-total.
  #:
  #: Every value here is `source='declared'` — copied from today's observed
  #: behaviour, not measured. L2 (spec §4) moves them to clients/constants.py
  #: (L2; does not exist yet) and adds the declared-vs-measured drift check
  #: that will correct them.
  #:
  #: THESE SUM TO 9h (30m x 4 + 3h + 2h + 2h). The old 4h total was also what
  #: guaranteed the run had finished before the 10:30Z watchdog; that guarantee
  #: is GONE. The watchdog must therefore never grade a lane of a run that has
  #: no close row — see decision 9 and the `Daily update finished` check.
  LANE_BUDGET_S: dict[str, float] = {
      "futures": 30 * 60,
      "cmdty": 30 * 60,
      "cboe": 30 * 60,
      "fx": 30 * 60,
      "corporate-actions": 3 * 60 * 60,
      "equity": 2 * 60 * 60,
      "silver": 2 * 60 * 60,
  }
  DEFAULT_LANE_BUDGET_S = 30 * 60
  #: 86 is blocked, not skipped: a named blocker is what turns "IB is down" into
  #: a backlog the status surface can show, instead of an unexplained absence.
  _OUTCOME_BY_EXIT = {0: "done", TIMEOUT_EXIT_CODE: "timeout", GATEWAY_DOWN_EXIT_CODE: "blocked"}
  _EPOCH = date(1970, 1, 1)
  def run_id() -> str:
      """This run's id. Minted by main(), inherited by children via LW_RUN_ID.

      It NEVER mints one lazily. A fresh id per call is worse than a crash:
      `silver_is_blocked()` and `record_failed_send` would each query a run id
      no row was ever written under, and both would silently read "nothing
      wrong". Every entrypoint sets LW_RUN_ID before it does anything else
      (`main()` here, `new_run_id("watchdog")` / `("coverage")` in the two
      separate processes), and every test that touches the ledger sets it too.
      """
      run = os.environ.get("LW_RUN_ID")
      if not run:
          raise RuntimeError("LW_RUN_ID is not set; main() mints it")
      return run
  def _emit_lane(scope, *, started, ended, exit_code, elapsed_s, outcome, blocker=None,
                 log_file: Path | None = None) -> None:
      """Never let a ledger write kill a lane: the record is not the work.

      `log_file` is the lane's own log, passed by the caller that already has
      it. Calling `build_config()` here to rediscover a log path would make the
      failure handler depend on the config machinery that may itself be what
      failed; with no log file the warning goes to stderr.
      """
      try:
          ledger.emit("lane_results", [{
              "run_id": run_id(), "lane": scope, "started": started, "ended": ended,
              "exit_code": exit_code, "budget_s": LANE_BUDGET_S.get(scope, DEFAULT_LANE_BUDGET_S),
              "elapsed_s": elapsed_s, "outcome": outcome, "blocker": blocker,
          }], run_id=run_id())
      except Exception as exc:  # pragma: no cover - logged but tolerated
          message = f"WARNING: could not write lane_results for {scope}: {exc}"
          if log_file is not None:
              append_log(log_file, message)
          else:
              print(message, file=sys.stderr)
  def _emit_last_session(scope: str, session: date | None) -> None:
      """The session this lane actually reached, as epoch days.
      `measurements.value` is float64, so a date has to be a number; epoch days
      is the one encoding that stays comparable across scopes. Without this the
      only evidence of a nine-session futures backlog is its absence.
      """
      if session is None:
          return
      ledger.emit("measurements", [{
          "name": "last_session", "scope": scope, "measured_at": _utc_now(),
          "value": float((session - _EPOCH).days), "unit": "epoch_days",
          "source": "measured", "run_id": run_id(),
      }], run_id=run_id())
  ```

  `_run_scheduled_lane` (`:713`) becomes:

  ```python
  def _run_scheduled_lane(
      config: RunnerConfig,
      command: list[str],
      label: str,
      done_scope: str,
      *,
      env: dict[str, str] | None,
      runner: callable,
      now_fn: callable,
  ) -> int:
      started_at = now_fn()
      log_file = build_log_file(config.log_dir, started_at)
      append_log(log_file, f"=== {label} {started_at:%Y-%m-%dT%H:%M:%SZ} ===")
      append_log(log_file, f"Command: {' '.join(command)}")
      # Entry row: outcome NULL. A lane that dies without writing its terminal
      # row therefore reads UNKNOWN, never green (rule 9).
      _emit_lane(done_scope, started=started_at, ended=None, exit_code=None,
                 elapsed_s=None, outcome=None, log_file=log_file)
      budget = LANE_BUDGET_S.get(done_scope, DEFAULT_LANE_BUDGET_S)
      clock = time.monotonic()
      result = run_daily_update_attempt(command, log_file, env=env, runner=runner, timeout=budget)
      ended_at = now_fn()
      # The IB preflight lives in the CHILD (livewire_ingest.py:116-119, a 1s TCP
      # probe to 127.0.0.1:4001), so an unreachable Gateway returns 86 in about a
      # second and this lane's budget is never spent on a hang.
      _emit_lane(
          done_scope, started=started_at, ended=ended_at, exit_code=result.returncode,
          elapsed_s=time.monotonic() - clock,
          outcome=_OUTCOME_BY_EXIT.get(result.returncode, "failed"),
          blocker="ib_unreachable" if result.returncode == GATEWAY_DOWN_EXIT_CODE else None,
          log_file=log_file,
      )
      _emit_last_session(done_scope, _last_session_from_log(log_file))
      if result.returncode == 0:
          append_log(log_file, f"=== Done {done_scope} {ended_at:%Y-%m-%dT%H:%M:%SZ} ===")
          return result.returncode
      append_log(log_file, f"=== {label} Failed {ended_at:%Y-%m-%dT%H:%M:%SZ} (exit_code={result.returncode}) ===")
      if result.returncode != GATEWAY_DOWN_EXIT_CODE:
          _page_failure(config, log_file, result.returncode, attempts=None, env=env)
      return result.returncode
  ```

  where `_last_session_from_log` reads the lane's last `SUMMARY_JSON` via the existing `parse_all_summary_json` and returns `date.fromisoformat(summary["target_date"])` or `None`.

  `run_daily_update_attempt` drops its `deadline` parameter and the non-positive-budget branch entirely (there is no shared budget left to exhaust); its `except subprocess.TimeoutExpired` branch is unchanged.

  **`run_with_retries` (`:482`) is instrumented the same way** — decision 8: futures, cmdty and equity reach `run_daily_update_attempt` (`:515`) through here, never through `_run_scheduled_lane`, so without this there are no lane rows for the two IB-only lanes, no equity row for `silver_is_blocked()`, and no budget on the three longest lanes. Concrete edited code (only the changed lines are shown in full; the retry loop body between them is untouched):

  ```python
  def run_with_retries(
      config: RunnerConfig,
      daily_update_args: Sequence[str],
      env: dict[str, str] | None = None,
      sleep_fn: callable = time.sleep,
      runner: callable = _run_in_own_process_group,
      now_fn: callable = _utc_now,
      completion_scope: str | None = None,
  ) -> int:                                   # `deadline: JobDeadline | None` is deleted
      started_at = now_fn()
      log_file = build_log_file(config.log_dir, started_at)
      command = tuple(build_daily_update_command(config, daily_update_args))
      done_scope = completion_scope or _completion_scope_from_args(daily_update_args)
      budget = LANE_BUDGET_S.get(done_scope, DEFAULT_LANE_BUDGET_S)

      append_log(log_file, f"=== Daily Update {started_at:%Y-%m-%dT%H:%M:%SZ} ===\n")
      append_log(log_file, f"Runner command: {' '.join(command)}")
      append_log(
          log_file,
          (
              "Runner config: "
              f"attempts={config.max_attempts} "
              f"retry_delay_seconds={config.retry_delay_seconds} "
              f"budget_s={budget} "
              f"hostname={socket.gethostname()}"
          ),
      )
      # Entry row before the attempt loop, exactly as _run_scheduled_lane does:
      # a lane that dies mid-run leaves outcome NULL and reads UNKNOWN (rule 9).
      _emit_lane(done_scope, started=started_at, ended=None, exit_code=None,
                 elapsed_s=None, outcome=None, log_file=log_file)
      clock = time.monotonic()

      final_exit_code = 1
      for attempt in range(1, config.max_attempts + 1):
          ...                                  # loop body unchanged, except:
          result = run_daily_update_attempt(command, log_file, env=env, runner=runner, timeout=budget)
          ...                                  # the 86 branch below `return`s, so both
                                               # exits go through _finish_lane()
      ...
      _page_failure(config, log_file, final_exit_code, attempts=config.max_attempts, env=env)
      return _finish_lane(done_scope, log_file, started_at, clock, final_exit_code, now_fn)
  ```

  The two early `return`s inside the loop (exit 86, and exit 0) become `return _finish_lane(done_scope, log_file, started_at, clock, GATEWAY_DOWN_EXIT_CODE, now_fn)` and `return _finish_lane(done_scope, log_file, started_at, clock, 0, now_fn)`, so there is exactly one terminal-row writer and no path can leave the entry row dangling:

  ```python
  def _finish_lane(done_scope, log_file, started_at, clock, exit_code, now_fn) -> int:
      """Write this lane's terminal row and its session, then return the code.
      One writer, all exits — rule 5. The outcome is derived from the FINAL
      exit code (after retries), which is the only one the run's verdict uses.
      """
      _emit_lane(
          done_scope, started=started_at, ended=now_fn(), exit_code=exit_code,
          elapsed_s=time.monotonic() - clock,
          outcome=_OUTCOME_BY_EXIT.get(exit_code, "failed"),
          blocker="ib_unreachable" if exit_code == GATEWAY_DOWN_EXIT_CODE else None,
          log_file=log_file,
      )
      _emit_last_session(done_scope, _last_session_from_log(log_file))
      return exit_code
  ```

  `_page_failure`'s `record_undelivered_alert(...)` call (`:479`) becomes:

  ```python
          ledger.emit("executions", [{
              "evidence_hash": None, "script": "send_alert", "attempt": 1,
              "args_json": json.dumps({"run_date": alert_request.run_date}),
              "release_sha": _release_sha(), "started": _utc_now(), "ended": _utc_now(),
              "exit_code": alert_result.returncode, "receipt_json": json.dumps({"output": alert_output}),
              "run_id": run_id(),
          }], run_id=run_id())
  ```

  New helpers near `build_config`:

  ```python
  def _release_sha() -> str | None:
      """The artifact that actually ran — bucket F, closed by recording it.
      `<warehouse>/current` is a symlink to `releases/<sha>`; a dev checkout has
      no such link and records None rather than guessing.
      """
      try:
          return Path(os.readlink(resolve_warehouse_dir() / "current")).name
      except OSError:
          return None
  def _file_sha(paths) -> str:
      digest = hashlib.sha256()
      for path in sorted(paths):
          digest.update(path.read_bytes())
      return digest.hexdigest()
  def silver_is_blocked() -> str | None:
      """Name the lane that blocks Silver this run, or None. Reads the ledger, not a dict."""
      rows = ledger.query(
          "select lane, exit_code from lane_results "
          f"where run_id = '{run_id()}' and outcome is not null "
          "and lane in ('corporate-actions', 'equity')"
      )
      by_lane = {r["lane"]: r["exit_code"] for r in rows}
      for lane in ("corporate-actions", "equity"):
          if by_lane.get(lane, 0) != 0:
              return lane
      return None
  ```

  `main()` (`:846`): delete `deadline = JobDeadline.start()` and every `deadline=deadline` argument; reorder the body to `LANE_ORDER` — `run_futures/cmdty` (the two `run_with_retries` asset-class calls for `futures` and `cmdty`) first, then `run_cboe_volatility_sync`, `run_fx_sync`, `run_corporate_action_sync`, then equity `run_with_retries` with the existing Massive-fallback block (`:884-899`) untouched, then Silver. At the top:

  ```python
      os.environ.setdefault("LW_RUN_ID", ledger.new_run_id("daily-update"))
      env = os.environ.copy()
      started_at = _utc_now()
      run_row = {
          "run_id": run_id(), "job": "daily-update", "host": socket.gethostname(),
          "release_sha": _release_sha(),
          "presets_sha": _file_sha((REPO_ROOT / "presets").glob("*.json")),
          "registry_sha": _file_sha([REPO_ROOT / "registry" / "gaps.json"]),
          "started": started_at, "ended": None, "exit_code": None, "verdict": None,
      }
      ledger.emit("runs", [run_row], run_id=run_id())
  ```

  and at every return point:

  ```python
      verdict = "FAILED" if final_code else ("DEGRADED" if degraded else "OK")
      ledger.emit("runs", [run_row | {"ended": _utc_now(), "exit_code": final_code, "verdict": verdict}],
                  run_id=run_id())
  ```

  The Silver gate becomes `blocker = silver_is_blocked()`; the `else` branch adds `_emit_lane("silver", started=_utc_now(), ended=_utc_now(), exit_code=None, elapsed_s=0.0, outcome="blocked", blocker=blocker, log_file=log_file)` beside the existing `=== Skipped silver` log line.

  In `run_post_success_quality` (`:574`), replace the `JOB_COMPLETE_MARKER` append (`:636`) with a lane row for the digest tail, since the watchdog no longer reads a marker:

  ```python
      _emit_lane("digest", started=tail_started, ended=_utc_now(), exit_code=0,
                 elapsed_s=time.monotonic() - tail_clock, outcome="done", log_file=log_file)
  ```

  Silver's measured number, needed by the Task 5 check — after `run_silver_rebuild` returns, take the lane's SUMMARY_JSON via `parse_all_summary_json` and emit:

  ```python
      ledger.emit("measurements", [{
          "name": "silver_failed", "scope": "silver", "measured_at": _utc_now(),
          "value": float(summary.get("failed", 0)), "unit": "symbols",
          "source": "measured", "run_id": run_id(),
      }], run_id=run_id())
  ```

  In `livewire_scripts/sync_runner.py`, `run_phase` (`:126`) gains the same two `ledger.emit("lane_results", …)` calls, so the intraday job's phases are visible to the same checks (Task 0(b): it is live). It is a separate process with no inherited `LW_RUN_ID`, so it mints one **once, at the top of its `main()`**, the same way the watchdog and coverage do — never per phase, or the two phase rows would land under two different run ids:

  ```python
      os.environ.setdefault("LW_RUN_ID", ledger.new_run_id("intraday-catchup"))
  ```

- [ ] **4.3 The watchdog is rewritten IN THIS TASK, not in Task 6.** It has to be: `livewire_scripts/check_daily_update_watchdog.py:20-31` imports `JOB_COMPLETE_MARKER`, `completed_scopes`, `job_tail_complete`, `log_has_completion_marker`, `skipped_scopes` and `undelivered_dir` from `run_daily_update_job`, and `tests/test_check_daily_update_watchdog.py:24` imports them too. Step 4.2 deletes all six, so leaving the watchdog for Task 6 leaves the tree with an `ImportError` at every commit boundary in between. Task 6 keeps only the digest and coverage.

  **Files:** rewrite `livewire_scripts/check_daily_update_watchdog.py`, rewrite `tests/test_check_daily_update_watchdog.py`.
  **Deletes:** `stale_equity_summary` (`:47`), `undelivered_alert_count` (`:85`), `determine_watchdog_error` (`:113`), `determine_intraday_watchdog_error` (`:130`), `tail_pending` / `quality_complete` / `intraday_complete` (`:172-174`), the `quality_summary_<date>.marker` read (`:159`); tests `TestQualitySummaryMarker` (`tests/test_check_daily_update_watchdog.py:328-396`), `test_a_tail_still_in_flight_does_not_page_for_the_missing_marker` (`:140`), `test_a_finished_tail_with_no_marker_still_pages` (`:157`), `test_the_completion_marker_is_not_mistaken_for_a_scope` (`:167`), `test_alerts_when_subset_of_asset_classes_done` (`:299`).
  **Kept:** `record_alert_marker` (`:143`) and `build_watchdog_marker_file` (`:109`) — idempotence state, not a fact about the run (spec §8). `TestHelpers::test_parse_args_and_path_builders` (`:67`) and `TestMain` (`:398`) are kept as tests.

  Failing test — rewrite the body of `tests/test_check_daily_update_watchdog.py` (`_section(verdict, name="X")` is a two-line helper returning a `status.Section`):

  ```python
  class TestTheWatchdogIsAStatusCaller:
      @pytest.fixture(autouse=True)
      def root(self, tmp_path, monkeypatch):
          monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
          monkeypatch.setenv("LW_RUN_ID", "watchdog-20260902T103000Z-1")
      def test_an_all_green_status_pages_nobody(self, tmp_path, monkeypatch):
          monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.OK)])
          sent = []
          monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
          assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == 0
          assert sent == []
      def test_one_bad_section_pages_once(self, tmp_path, monkeypatch):
          monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD, "Lanes terminal")])
          sent = []
          monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
          assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == 0
          assert len(sent) == 1
      def test_a_second_run_the_same_day_does_not_page_again(self, tmp_path, monkeypatch):
          monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD, "Lanes terminal")])
          sent = []
          monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
          watchdog.run_watchdog(_config(tmp_path), "2026-09-02")
          watchdog.run_watchdog(_config(tmp_path), "2026-09-02")
          assert len(sent) == 1
      def test_unknown_alone_does_not_page_but_status_still_shows_it(self, tmp_path, monkeypatch):
          """UNKNOWN is not OK, but it is not a 03:00 page either — the digest carries it."""
          monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.UNKNOWN)])
          sent = []
          monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
          assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == 0
          assert sent == []
      def test_a_warn_section_does_not_page(self, tmp_path, monkeypatch):
          """A run still in flight at 10:30Z WARNs (decision 9). WARN never pages."""
          monkeypatch.setattr(watchdog, "collect",
                              lambda *a, **k: [_section(Verdict.WARN, "Daily update finished")])
          sent = []
          monkeypatch.setattr(watchdog, "send_failure_alert", lambda *a, **k: sent.append(a) or _ok())
          assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == 0
          assert sent == []
      def test_a_failed_send_is_recorded_as_an_execution_row(self, tmp_path, monkeypatch):
          from clients import ledger
          monkeypatch.setattr(watchdog, "collect", lambda *a, **k: [_section(Verdict.BAD)])
          monkeypatch.setattr(watchdog, "send_failure_alert",
                              lambda *a, **k: subprocess.CompletedProcess([], 7, stdout="smtp down"))
          assert watchdog.run_watchdog(_config(tmp_path), "2026-09-02") == watchdog.ALERT_FAILED_EXIT_CODE
          assert ledger.query("select exit_code from executions where script = 'send_alert'") == [{"exit_code": 7}]
  ```

  Implement — `livewire_scripts/check_daily_update_watchdog.py` collapses to:

  ```python
  """Page when the graded status surface says BAD. It parses nothing.
  Three parsers over one prose log is the defect the ledger closes; this file
  was the second of them. It now asks `status.collect()` the same question the
  terminal and the digest ask, so a check reaches all three surfaces or none.
  """
  from clients import ledger
  from livewire_scripts.status import Verdict, collect
  ALERT_FAILED_EXIT_CODE = 3
  def main(argv=None) -> int:
      # A separate process: it inherits no LW_RUN_ID, and `run_id()` refuses to
      # mint one lazily (decision on run_id in Task 4.2), so the entrypoint mints
      # it once, before anything can want to write a row.
      os.environ.setdefault("LW_RUN_ID", ledger.new_run_id("watchdog"))
      args = parse_args(argv)
      return run_watchdog(build_config(), args.run_date)
  def run_watchdog(config: RunnerConfig, run_date: str, runner=None) -> int:
      marker_file = build_watchdog_marker_file(config.warehouse_dir, run_date)
      sections = collect(date.fromisoformat(run_date), config.log_dir, data_lake_dir())
      # BAD only. WARN is where "the run is still going at 10:30Z" lands, and
      # paging on that is pm:2026-08-16-watchdog-raced-quality-marker again.
      bad = [s for s in sections if s.verdict is Verdict.BAD]
      if not bad:
          return 0
      if marker_file.exists():
          # Idempotence state, not a fact about the run: one page per day.
          return 0
      reason = "; ".join(s.lines[0] if s.lines else s.name for s in bad)
      log_file = build_daily_log_file(config.log_dir, run_date)
      result = send_failure_alert(
          config,
          AlertRequest(run_date=run_date, log_file=log_file, attempts=None,
                       exit_code=1, error_summary=reason, repo_root=REPO_ROOT),
          log_file,
      )
      if result is None or result.returncode != 0:
          record_failed_send(run_date, result)
          return ALERT_FAILED_EXIT_CODE
      record_alert_marker(marker_file, reason)
      return 0
  ```

  `record_failed_send` is imported from `run_daily_update_job` (the `executions(script='send_alert')` writer factored out of `_page_failure` in step 4.2) so there is exactly one writer of that row — rule 5, fix the twin.

- [ ] **4.4 Run:**
      `uv run pytest tests/test_run_daily_update_job.py tests/test_check_daily_update_watchdog.py -v` → expect all passed.
      `grep -c deadline livewire_scripts/run_daily_update_job.py` → expect `0`.
      `uv run python -c "import livewire_scripts.check_daily_update_watchdog"` → expect no `ImportError` (the deleted symbols really are gone from both sides).
- [ ] **4.5 Commit:** `git commit -am "feat(ledger): no-fallback-first lane order, per-lane budgets, runs/lane_results rows; watchdog calls status.collect()"`

---

## Task 5 — `status.py` → `CHECKS`

**Files:** rewrite `livewire_scripts/status.py`, rewrite `tests/test_status.py`.
**Deletes:** `_outcomes_section` (`:94`), `_phases_section` (`:155`), `_previous_silver_summary` (`:207`), `_silver_section` (`:237`), `_quality_jobs_section` (`:282`), `_coverage_ratios` (`:313`), `_coverage_section` (`:326`), `_undelivered_queues` (`:578`), `_undelivered_section` (`:594`), `_read_text` (`:87`), `_QUALITY_WARNING_RE` (`:279`), `_COVERAGE_TF_RE` (`:304`), `_COVERAGE_LINE_RE` (`:308`), and every test in `tests/test_status.py` that builds a fake log (`:44`–`:241`, `TestTheDigestFindsCoverageOnAnySchedule`, `TestTheSectionsSurviveDegenerateInputs`, `TestTheDigestDistinguishesDegradedFromFailed`).
**Kept:** `Verdict` (`:51`), `Section` (`:78`), `_disk_section` (`:437`), `_launchd_section` (`:513`), `_duckdb_section` (`:703` — it reads `analytics.duckdb`, not logs; spec §9 leaves it in place), `_safe` (`:768`), `collect` (`:786`), `render` (`:816`), `main` (`:843`).

**Interfaces:**

```python
CHECKS: list[tuple[str, str]]
def run_check(name: str, sql: str, params: dict[str, str]) -> Section
def collect(run_date, log_dir, data_lake, *, runner=..., database=None, main_sha=None) -> list[Section]
```

- [ ] **5.1 Failing test.** Rewrite `tests/test_status.py` around a ledger fixture (keeping `_fake_launchctl` / `_no_catalog` at `:657-663`, the render tests, `test_section_is_frozen`, `test_unknown_outranks_ok_*`, the disk class and the launchd tests verbatim):

  ```python
  """Tests for livewire_scripts/status.py — one reader, over the ledger."""
  # imports: the module under test, pytest, pathlib/datetime, and `clients.ledger`.
  RUN = "daily-update-20260902T060000Z-1"
  NOW = datetime.now(UTC)
  EPOCH = date(1970, 1, 1)
  @pytest.fixture(autouse=True)
  def root(tmp_path, monkeypatch):
      monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
      return tmp_path / "ledger"
  def _run(**over):
      ledger.emit("runs", [{
          "run_id": RUN, "job": "daily-update", "host": "macmini",
          "release_sha": "deadbeef", "presets_sha": "p", "registry_sha": "r",
          "started": NOW, "ended": NOW, "exit_code": 0, "verdict": "OK",
      } | over], run_id=RUN)
  def _lane(lane, **over):
      ledger.emit("lane_results", [{
          "run_id": RUN, "lane": lane, "started": NOW, "ended": NOW,
          "exit_code": 0, "budget_s": 1800.0, "elapsed_s": 12.0,
          "outcome": "done", "blocker": None,
      } | over], run_id=RUN)
  def _last_session(scope, session: date):
      ledger.emit("measurements", [{
          "name": "last_session", "scope": scope, "measured_at": NOW,
          "value": float((session - EPOCH).days), "unit": "epoch_days",
          "source": "measured", "run_id": RUN,
      }], run_id=RUN)
  def _section(name, **kw):
      sections = status.collect(date.today(), Path("/nonexistent"), Path("/nonexistent"),
                                runner=_fake_launchctl, database=None, **kw)
      return next(s for s in sections if s.name == name)
  def test_no_run_row_at_all_is_unknown_not_ok():
      assert _section("Daily update ran").verdict is Verdict.UNKNOWN
  def test_a_run_today_is_ok():
      _run(); assert _section("Daily update ran").verdict is Verdict.OK
  def test_a_failed_run_is_bad():
      """runs.verdict speaks OK/DEGRADED/FAILED/UNKNOWN; Verdict does not. Decision 10."""
      _run(verdict="FAILED", exit_code=1)
      assert _section("Daily update ran").verdict is Verdict.BAD
  def test_a_degraded_run_is_warn():
      _run(verdict="DEGRADED")
      assert _section("Daily update ran").verdict is Verdict.WARN
  def test_a_closed_run_reads_finished():
      _run(); assert _section("Daily update finished").verdict is Verdict.OK
  def test_a_run_still_open_at_watchdog_time_warns_and_grades_no_lane_bad():
      """9h of lane budgets vs a 10:30Z watchdog: still-running is not broken.

      Decision 9 / pm:2026-08-16-watchdog-raced-quality-marker.
      """
      _run(ended=None, exit_code=None, verdict=None)
      _lane("equity", outcome=None, ended=None, exit_code=None)
      finished = _section("Daily update finished")
      assert finished.verdict is Verdict.WARN
      assert "running_minutes" in "\n".join(finished.lines)
      # Nothing that grades a lane may read BAD while the run is open.
      for name in ("Lanes terminal", "Silver advanced", "Lanes within budget"):
          assert _section(name).verdict is not Verdict.BAD
  def test_a_lane_with_no_terminal_row_is_bad():
      _run(); _lane("equity", outcome=None, ended=None, exit_code=None)
      assert _section("Lanes terminal").verdict is Verdict.BAD
  def test_every_lane_terminal_is_ok():
      _run(); _lane("equity"); _lane("silver")
      assert _section("Lanes terminal").verdict is Verdict.OK
  def test_silver_that_did_not_run_is_unknown():
      _run(); _lane("equity"); assert _section("Silver advanced").verdict is Verdict.UNKNOWN
  def test_silver_blocked_is_bad():
      _run(); _lane("silver", outcome="blocked", blocker="equity", exit_code=None)
      assert _section("Silver advanced").verdict is Verdict.BAD
  def test_any_undelivered_alert_is_a_warning():
      """pm:2026-08-08 — a WARNING in a log nobody reads is not an alert."""
      _run()
      ledger.emit("executions", [{
          "evidence_hash": None, "script": "send_alert", "attempt": 1,
          "args_json": "{}", "release_sha": "deadbeef", "started": NOW,
          "ended": NOW, "exit_code": 3, "receipt_json": "{}", "run_id": RUN,
      }], run_id=RUN)
      section = _section("Undelivered alerts")
      assert section.verdict is Verdict.WARN
      assert "send_alert" in "\n".join(section.lines)
  def test_a_delivered_alert_is_ok():
      _run()
      assert _section("Undelivered alerts").verdict is Verdict.OK
  def test_an_ib_phase_at_86_reads_degraded_not_failed():
      """pm:2026-07-22-ib-not-a-single-point-of-failure, on ledger rows."""
      _run(verdict="DEGRADED")
      _lane("futures", exit_code=86, outcome="blocked", blocker="ib_unreachable")
      assert _section("Lanes terminal").verdict is Verdict.OK
      assert _section("Release matches main", main_sha="deadbeef").verdict is Verdict.OK
  def test_a_release_behind_main_is_bad():
      _run()
      assert _section("Release matches main", main_sha="0ther").verdict is Verdict.BAD
  def test_no_main_sha_supplied_is_unknown_never_green():
      _run()
      assert _section("Release matches main").verdict is Verdict.UNKNOWN
  def test_a_lane_over_its_budget_warns():
      _run(); _lane("corporate-actions", budget_s=10800.0, elapsed_s=31140.0,
                    outcome="timeout", exit_code=124)
      section = _section("Lanes within budget")
      assert section.verdict is Verdict.WARN
      assert "corporate-actions" in "\n".join(section.lines)
  def test_a_lane_inside_its_budget_is_ok():
      _run(); _lane("cboe")
      assert _section("Lanes within budget").verdict is Verdict.OK
  def test_an_ib_only_lane_days_behind_warns_and_names_its_blocker():
      """IB down for a week must read as a backlog, not as silence."""
      _run()
      _lane("futures", exit_code=86, outcome="blocked", blocker="ib_unreachable")
      _last_session("futures", date.today() - timedelta(days=9))
      section = _section("IB-only lanes behind")
      assert section.verdict is Verdict.WARN
      body = "\n".join(section.lines)
      assert "futures" in body and "ib_unreachable" in body
  def test_an_ib_only_lane_current_is_ok():
      _run(); _lane("futures")
      _last_session("futures", date.today() - timedelta(days=1))
      assert _section("IB-only lanes behind").verdict is Verdict.OK
  def test_a_weekend_gap_is_not_a_backlog():
      """Friday's session is 3 calendar days old every Monday. Decision 11."""
      _run(); _lane("futures")
      _last_session("futures", date.today() - timedelta(days=3))
      assert _section("IB-only lanes behind").verdict is Verdict.OK
  def test_an_ib_only_lane_that_never_reported_a_session_is_unknown():
      _run(); _lane("futures")
      assert _section("IB-only lanes behind").verdict is Verdict.UNKNOWN
  def _silver_failed(value: float, at: datetime):
      ledger.emit("measurements", [{
          "name": "silver_failed", "scope": "silver", "measured_at": at,
          "value": value, "unit": "symbols", "source": "measured", "run_id": RUN,
      }], run_id=RUN)
  def test_one_silver_measurement_is_not_a_change():
      _run(); _silver_failed(4.0, NOW)
      assert _section("Silver failures did not grow").verdict is Verdict.UNKNOWN
  def test_growing_silver_failures_warn():
      _run()
      _silver_failed(4.0, NOW - timedelta(days=1))
      _silver_failed(9.0, NOW)
      section = _section("Silver failures did not grow")
      assert section.verdict is Verdict.WARN
      assert "9" in "\n".join(section.lines)
  def test_shrinking_silver_failures_are_ok():
      _run()
      _silver_failed(9.0, NOW - timedelta(days=1))
      _silver_failed(4.0, NOW)
      assert _section("Silver failures did not grow").verdict is Verdict.OK
  def test_a_broken_check_never_takes_the_report_down(monkeypatch):
      monkeypatch.setattr(status.ledger, "query",
                          lambda sql: (_ for _ in ()).throw(RuntimeError("boom")))
      sections = status.collect(date.today(), Path("/nonexistent"), Path("/nonexistent"),
                                runner=_fake_launchctl)
      assert any(s.verdict is Verdict.UNKNOWN for s in sections)
  def test_every_check_is_a_name_and_a_select():
      assert status.CHECKS
      assert all(sql.strip().lower().startswith("select") for _, sql in status.CHECKS)
  ```

  Run: `uv run pytest tests/test_status.py -v` → expect `AttributeError: module 'livewire_scripts.status' has no attribute 'CHECKS'`.

- [ ] **5.2 Implement.** In `livewire_scripts/status.py`, replace the deleted section functions with:

  ```python
  #: Every operational check is one SQL statement over the ledger plus one test.
  #: A new check is a row here — never another `_foo_section` function, because
  #: three hand-written parsers over one prose artifact is the defect this
  #: module exists to end (spec 2026-09-02-ledger §0, §3).
  #:
  #: Contract for the SQL: zero rows => UNKNOWN (a check that could not measure
  #: has not passed), except for the names in `_EMPTY_IS_OK`. A row carrying a
  #: `verdict` column names its own verdict; any other row is OK. `$run`,
  #: `$today` and `$main_sha` are substituted by `collect()`.
  CHECKS: list[tuple[str, str]] = [
      (
          # The ledger's run vocabulary (OK/DEGRADED/FAILED/UNKNOWN, spec §8) is
          # NOT status.Verdict's (OK/UNKNOWN/WARN/BAD). run_check does
          # `Verdict[row["verdict"]]`, so an untranslated 'DEGRADED' is a
          # KeyError on exactly the nights IB is down. Translate in SQL, here,
          # once — decision 10.
          "Daily update ran",
          "select case verdict when 'FAILED' then 'BAD' when 'DEGRADED' then 'WARN' "
          "when 'OK' then 'OK' else 'UNKNOWN' end as verdict, run_id, started "
          "from runs where job = 'daily-update' and date(started) = date '$today' "
          "and ended is not null order by started desc limit 1",
      ),
      (
          # The 4h total deadline is what used to guarantee the run had finished
          # before this 10:30Z check; the per-lane budgets sum to 9h and
          # guarantee nothing. A run still in flight is WARN with its elapsed
          # time — never BAD, which is what pages. Decision 9.
          # Two rows share one run_id (entry, terminal), so aggregate: the run is
          # open iff no row for it carries an `ended`.
          "Daily update finished",
          "select 'WARN' as verdict, run_id, "
          "date_diff('minute', min(started), now()) as running_minutes "
          "from runs where run_id = '$open_run' "
          "group by run_id having max(ended) is null",
      ),
      (
          "Lanes terminal",
          "select case when count(*) = 0 then 'OK' else 'BAD' end as verdict, "
          "count(*) as unterminated, string_agg(lane, ', ') as lanes from ("
          "  select lane from lane_results where run_id = '$run' "
          "  group by lane having max(coalesce(outcome, '')) = ''"
          ")",
      ),
      (
          "Silver advanced",
          "select case when outcome = 'done' then 'OK' else 'BAD' end as verdict, "
          "outcome, blocker from lane_results "
          "where run_id = '$run' and lane = 'silver' and outcome is not null "
          "order by ended desc limit 1",
      ),
      (
          "Undelivered alerts",
          "select 'WARN' as verdict, script, count(*) as failed_sends from executions "
          "where script = 'send_alert' and exit_code <> 0 and date(started) = date '$today' "
          "group by script",
      ),
      (
          "Release matches main",
          "select case when release_sha = '$main_sha' then 'OK' else 'BAD' end as verdict, "
          "release_sha, '$main_sha' as main_sha from runs where run_id = '$run'",
      ),
      (
          "Lanes within budget",
          "select 'WARN' as verdict, lane, elapsed_s, budget_s from lane_results "
          "where run_id = '$run' and elapsed_s is not null and elapsed_s > budget_s "
          "order by elapsed_s desc",
      ),
      (
          # Spec §3: Silver is graded on the CHANGE in `failed`, which is why
          # run_daily_update_job emits the silver_failed measurement. Growth is
          # WARN; fewer than two measurements is UNKNOWN, because one number is
          # not a change. Without this the emit has no reader and should not
          # exist at all.
          "Silver failures did not grow",
          "select case when count(*) < 2 then 'UNKNOWN' "
          "when max(case when rn = 1 then value end) > max(case when rn = 2 then value end) "
          "then 'WARN' else 'OK' end as verdict, "
          "max(case when rn = 1 then value end) as failed_now, "
          "max(case when rn = 2 then value end) as failed_before from ("
          "  select value, row_number() over (order by measured_at desc) as rn "
          "  from measurements where name = 'silver_failed'"
          ") where rn <= 2",
      ),
      (
          # futures and cmdty are IB-only: no Massive fallback exists, so a down
          # Gateway is a growing backlog and nothing else reports it.
          "IB-only lanes behind",
          "select case when max(behind) > $ib_slack_days then 'WARN' else 'OK' end as verdict, "
          "string_agg(lane || '@' || last_session || case when blocker is null then '' "
          "else ' (' || blocker || ')' end, ', ') as lanes, max(behind) as sessions_behind from ("
          "  select m.scope as lane, date '1970-01-01' + cast(m.value as int) as last_session, "
          "         date_diff('day', date '1970-01-01' + cast(m.value as int), date '$today') as behind, "
          "         (select l.blocker from lane_results l where l.lane = m.scope "
          "          and l.outcome is not null order by l.ended desc limit 1) as blocker "
          "  from measurements m where m.name = 'last_session' and m.scope in ('futures', 'cmdty') "
          "  qualify row_number() over (partition by m.scope order by m.measured_at desc) = 1"
          ")",
      ),
  ]
  #: A check whose SQL found nothing is OK only where emptiness IS the healthy
  #: state. Everywhere else zero rows means "not measured", which is UNKNOWN.
  #: "Daily update finished" is here because zero rows means the run is closed;
  #: a run that never started is already UNKNOWN under "Daily update ran".
  _EMPTY_IS_OK = {"Undelivered alerts", "Lanes within budget", "Daily update finished"}
  #: Calendar days, not sessions: 4 absorbs a weekend + one holiday. Friday's
  #: session is 3 calendar days old every Monday, so a threshold of 2 WARNs
  #: every Monday forever. The cost is stated, not hidden: one night of
  #: blindness after a long weekend. L2 replaces this with the XNYS calendar.
  IB_LANE_SLACK_DAYS = 4
  _FIXES = {
      "Daily update ran": "launchctl list | grep livewire.daily-update   # then read <log_dir>/daily_update_$today.log",
      "Daily update finished": "python scripts/livewire_ops.py ledger query \"select lane, outcome, elapsed_s from lane_results where run_id = '$open_run'\"   # which lane is still open",
      "Silver failures did not grow": _SILVER_FIX,
      "Lanes terminal": "python scripts/livewire_ops.py ledger query \"select lane, outcome from lane_results where run_id = '$run'\"",
      "Silver advanced": _SILVER_FIX,
      "Undelivered alerts": "python scripts/livewire_ops.py ledger query \"select receipt_json from executions where script = 'send_alert' and exit_code <> 0\"",
      "Release matches main": "python scripts/livewire_ops.py release promote",
      "Lanes within budget": "raise the lane's LANE_BUDGET_S only after measuring it cold; see run_daily_update_job.LANE_BUDGET_S",
      "IB-only lanes behind": (
          "nc -z 127.0.0.1 4001 && echo up || echo down   # then 2FA by hand; rerun: "
          "python scripts/livewire_ingest.py daily --asset-class futures / --asset-class cmdty"
      ),
  }
  def _substitute(sql: str, params: dict[str, str]) -> str:
      for key, value in params.items():
          sql = sql.replace(f"${key}", value)
      return sql
  def run_check(name: str, sql: str, params: dict[str, str]) -> Section:
      """Execute one check. Zero rows is UNKNOWN unless emptiness is health."""
      rows = [r for r in ledger.query(_substitute(sql, params)) if any(v is not None for v in r.values())]
      fix = _substitute(_FIXES.get(name, ""), params) or None
      if not rows:
          if name in _EMPTY_IS_OK:
              return Section(name, Verdict.OK, [f"{name}: none"])
          return Section(name, Verdict.UNKNOWN, [f"{name}: no rows — nothing measured"], fix=fix)
      verdict = max(Verdict[str(row["verdict"])] if row.get("verdict") else Verdict.OK for row in rows)
      lines = [f"{name}:"] + [
          "  " + "  ".join(f"{k}={v}" for k, v in row.items() if k != "verdict") for row in rows
      ]
      return Section(name, verdict, lines, fix=fix if verdict is not Verdict.OK else None)
  def _last_run_id(today: str, *, closed: bool) -> str:
      """Today's daily-update run id, or "" — which makes every `$run` check
      match nothing and therefore read UNKNOWN.

      `closed=True` (what every lane-grading check gets) requires a row with
      `ended is not null`. The per-lane budgets sum to 9h while the watchdog
      fires at 10:30Z, so a healthy run is routinely still in flight when this
      is asked; grading its lanes then would call a running lane BAD. Decision
      9, and pm:2026-08-16-watchdog-raced-quality-marker, which is the same
      mistake with a marker file instead of a row.
      """
      clause = "and ended is not null " if closed else ""
      rows = ledger.query(
          "select run_id from runs where job = 'daily-update' "
          f"and date(started) = date '{today}' {clause}order by started desc limit 1"
      )
      return str(rows[0]["run_id"]) if rows else ""
  ```

  `collect` becomes:

  ```python
  def collect(
      run_date: date,
      log_dir: Path,
      data_lake: Path,
      *,
      runner=subprocess.run,
      database: Path | None = None,
      main_sha: str | None = None,
  ) -> list[Section]:
      """Assess every cheap signal. Never raises. Never scans bar parquet; the
      ledger is the only parquet it reads.

      `main_sha` is supplied by the caller: `status` never shells out to git
      (spec §3 check 5). Without it the release check reads UNKNOWN, which is
      the correct grade for "could not measure" and is never green.

      `$run` is a CLOSED run only, `$open_run` is the latest run whether closed
      or not. Every lane-grading check uses `$run`, so while the nightly run is
      still in flight they match no rows and read UNKNOWN instead of BAD.
      """
      today = run_date.isoformat()
      params = {
          "today": today,
          "run": _last_run_id(today, closed=True),
          "open_run": _last_run_id(today, closed=False),
          "main_sha": main_sha or "\x00unknown",
          "ib_slack_days": str(IB_LANE_SLACK_DAYS),
      }
      return [
          _safe("launchd jobs", lambda: _launchd_section(runner=runner)),
          *[_safe(name, lambda n=name, s=sql: run_check(n, s, params)) for name, sql in CHECKS],
          _safe("DuckDB catalog", lambda: _duckdb_section(run_date, database)),
          _safe("Disk", lambda: _disk_section(data_lake, log_dir.parent)),
      ]
  ```

  `main()` gains `--main-sha` (default `None`) and passes it through. `log_dir` and `data_lake` stay in the signature: the disk check still needs both volumes (pm:2026-08-10-nightly-disk-line-wrong-volume).

- [ ] **5.3 Run:**
      `uv run pytest tests/test_status.py -v` → expect all passed.
      `grep -c '^def _.*_section' livewire_scripts/status.py` → expect `3` (spec §7 item 4).
- [ ] **5.4 Commit:** `git commit -am "feat(status): CHECKS over the ledger; delete the log parsers"`

---

## Task 6 — Digest and coverage

(The watchdog was rewritten in Task 4.3 — it imports symbols step 4.2 deletes, so it could not wait for this task.)

**Files:** edit `livewire_scripts/nightly_digest.py:85-92`, edit `livewire_scripts/coverage_report.py`; edit `tests/test_nightly_digest.py`, edit `tests/test_coverage_report.py`.
**Deletes:** the `quality_summary_<date>.marker` write in `nightly_digest.py:86-92`; test `test_failed_send_does_not_write_marker` (`tests/test_nightly_digest.py:210`).

- [ ] **6.1 Failing test.** In `tests/test_nightly_digest.py`, replace `test_failed_send_does_not_write_marker` (`:210`) with:

  ```python
  def test_the_digest_lane_is_recorded_in_the_ledger(tmp_path, monkeypatch):
      from clients import ledger
      monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
      monkeypatch.setenv("LW_RUN_ID", "daily-update-20260902T060000Z-1")
      run_daily_update_job.run_post_success_quality(
          _config(tmp_path), tmp_path / "daily_update_2026-09-02.log",
          runner=lambda *a, **k: subprocess.CompletedProcess([], 0),
      )
      assert ledger.query("select lane, outcome from lane_results where lane = 'digest'") == [
          {"lane": "digest", "outcome": "done"}
      ]
  def test_no_quality_marker_is_written_anywhere(tmp_path, monkeypatch):
      """The marker and its 10:30Z race are gone: an absent lane row is UNKNOWN by construction."""
      monkeypatch.setenv("MDW_NODE_BIN", "/bin/true")
      nightly_digest.main(["--run-date", "2026-09-02", "--log-dir", str(tmp_path), "--email"],
                          runner=lambda *a, **k: subprocess.CompletedProcess([], 0))
      assert list(tmp_path.glob("*.marker")) == []
  ```

  In `tests/test_coverage_report.py` add:

  ```python
  def test_coverage_emits_its_percentage_and_elapsed_seconds(tmp_path, monkeypatch):
      from clients import ledger
      monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
      coverage_report.emit_coverage_measurements(
          date(2026, 9, 2), {"1d": _result(present=100, total=100)}, elapsed_s=1432.0
      )
      assert ledger.query("select name, scope, value, source from measurements order by name") == [
          {"name": "coverage_elapsed_s", "scope": "all", "value": 1432.0, "source": "measured"},
          {"name": "coverage_pct", "scope": "1d", "value": 1.0, "source": "measured"},
      ]
  ```

  Run: `uv run pytest tests/test_nightly_digest.py tests/test_coverage_report.py -v` → expect failures on the missing `digest` lane row and on `coverage_report.emit_coverage_measurements`.

- [ ] **6.2 Implement.**

  `livewire_scripts/nightly_digest.py`: delete `:86-92` (the marker write); `main` returns `rc` directly.

  `livewire_scripts/coverage_report.py`: add

  ```python
  def emit_coverage_measurements(target: date, results: dict[str, CoverageResult], *, elapsed_s: float) -> None:
      """Publish coverage as rows. The `coverage:` log line stays, for humans only.
      pm:2026-08-14-coverage-log-first-line-oldest: `_coverage_section` picked
      the wrong `coverage:` line out of an appended log. A row has no ordering
      to get wrong. `coverage_elapsed_s` is the number that makes the next
      budget argument measurable instead of guessed (pm:2026-08-02).
      """
      now = datetime.now(UTC)
      run = os.environ["LW_RUN_ID"]   # minted once in main(), see below
      rows = [{"name": "coverage_pct", "scope": tf, "measured_at": now, "value": float(r.ratio),
               "unit": "ratio", "source": "measured", "run_id": run} for tf, r in sorted(results.items())]
      rows.append({"name": "coverage_elapsed_s", "scope": "all", "measured_at": now,
                   "value": float(elapsed_s), "unit": "s", "source": "measured", "run_id": run})
      ledger.emit("measurements", rows, run_id=run)
  ```

  Coverage is its own launchd job, so it inherits no `LW_RUN_ID` and `ledger.run_id()` refuses to mint one lazily. Its `main()` mints one, once, first:

  ```python
      os.environ.setdefault("LW_RUN_ID", ledger.new_run_id("coverage"))
  ```

  and the test above must set `LW_RUN_ID` alongside `LW_LEDGER_ROOT` (`monkeypatch.setenv("LW_RUN_ID", "coverage-20260902T110000Z-1")`) — as must every test in `tests/test_nightly_digest.py` and `tests/test_check_daily_update_watchdog.py` that reaches a ledger write.

  Then call `emit_coverage_measurements` in `main` right after `format_one_liner` (`:1058`), timing the `compute_coverage` call with `time.monotonic()`. **`cache_path=_resolved_log_dir() / "coverage_footer_cache.json"` (`:1054`) is left exactly as it is** — decision 4 above.

- [ ] **6.3 Run:** `uv run pytest tests/test_nightly_digest.py tests/test_coverage_report.py tests/test_check_daily_update_watchdog.py -v` → expect all passed (the watchdog suite is re-run here because Task 5 changed `collect`, which it now calls).
- [ ] **6.4 Commit:** `git commit -am "feat(ledger): drop the quality marker; coverage emits measurements"`

---

## Task 7 — Docs

**Files:** edit `CLAUDE.md`, `docs/runbook.md`, `launchd/*.plist.example` (only if the grep below hits).
**Deletes:** the runbook paragraphs for `MDW_DAILY_JOB_DEADLINE_SECONDS`, the quality-summary marker, and `alerts_undelivered`.

- [ ] **7.1** In `CLAUDE.md`, "The one contract", add one line (one line, per the file's own rule):

  ```
  - Every job writes its facts to the ledger (`<lake>/ledger/`) and every reader reads the ledger, never a log. → test: `tests/test_status.py` · spec `2026-09-02-livewire-ledger-design.md` §3
  ```

  In "Scheduled jobs", replace the `MDW_DAILY_JOB_DEADLINE_SECONDS` bullet with two lines:

  ```
  - Lane budgets are per lane (`LANE_BUDGET_S`), not a total: a lane over budget is killed by process group, recorded `outcome='timeout'`, and **the next lane starts normally**. → test: `tests/test_run_daily_update_job.py::TestPerLaneBudgets` · pm:2026-07-28-daily-job-deadline-is-a-total
  - Lane order is no-fallback-first (futures → cmdty → CBOE → FX → corporate-actions → equity → silver): the IB-only lanes take minutes and cannot be back-sourced, so they never queue behind a 3–8h Massive lane. → test: `::test_main_runs_the_no_fallback_lanes_before_the_expensive_ones`
  ```

  Replace the coverage/weekly/digest ordering bullet's second sentence (the watchdog's marker rule is gone): the line becomes

  ```
  - coverage/weekly/digest run once, after Silver. The watchdog pages only on `BAD`; a run still in flight at 10:30Z is `WARN` (lane budgets sum to 9h, so completion is no longer guaranteed) and every lane check reads UNKNOWN until the run has a close row. → test: `tests/test_check_daily_update_watchdog.py` · pm:2026-07-22-coverage-weekly-digest-ordering, pm:2026-08-16-watchdog-raced-quality-marker
  ```

  In "Alerts and the digest", replace the undelivered-alerts bullet (`CLAUDE.md:122`) with:

  ```
  - A failed alert send is an `executions(script='send_alert', exit_code<>0)` row; `status` and the watchdog both grade it WARN. → test: `tests/test_status.py::test_any_undelivered_alert_is_a_warning`
  ```

  and replace the `status` bullet in the same section with:

  ```
  - `status`: `UNKNOWN` is not `OK` (`Verdict` is an `IntEnum`, OK < UNKNOWN < WARN < BAD); every check is one `(name, sql)` row in `CHECKS` over the ledger; zero rows is UNKNOWN unless the name is in `_EMPTY_IS_OK`; `launchctl` exits cap at WARN; every log line goes through `rich.markup.escape`; exit code is always 0; it never scans bar parquet. → test: `tests/test_status.py` · pm:2026-08-16-status-surface-grading
  ```

  (`rich.markup.escape` stays because `render` is kept unchanged — verify with `grep -n escape livewire_scripts/status.py` before writing the line. `pm:2026-08-14-coverage-log-first-line-oldest` drops off this bullet: there is no `coverage:` line to pick out any more.)

- [ ] **7.2** In `docs/runbook.md`: delete the `MDW_DAILY_JOB_DEADLINE_SECONDS`, `quality_summary_<date>.marker` and `alerts_undelivered` paragraphs; add under Commands:

  ```bash
  uv run python scripts/livewire_ops.py ledger query "select lane, outcome, elapsed_s from lane_results order by started desc limit 20"
  uv run python scripts/livewire_ops.py ledger query "select scope, date '1970-01-01' + cast(value as int) as last_session from measurements where name = 'last_session'"
  uv run python scripts/livewire_ops.py ledger emit --table evidence --json '{"evidence_hash":"…","kind":"request","subject":"silver:TSLA","payload_json":"{}","source_url":null,"fetched_at":"2026-09-02T06:00:00+00:00","proposer":"human","run_id":"manual-1"}'
  # LW_LEDGER_ROOT overrides the root (default <lake>/ledger); LW_RUN_ID names the run.
  ```

- [ ] **7.3 Verify no template or doc still names the deleted mechanisms:**

  ```bash
  grep -rn 'MDW_DAILY_JOB_DEADLINE_SECONDS\|quality_summary_.*\.marker\|alerts_undelivered\|MDW_UNDELIVERED_DIR' \
    launchd/ scripts/ livewire_scripts/ clients/ docs/runbook.md CLAUDE.md
  ```

  Expected output: no matches. Fix any hit before committing.

  The pattern is deliberately narrow. A bare `quality_summary` matches `livewire_quality.py`, `data_quality_report.py`, `quality_flags.py`, `coverage_denominator.py`, the coverage plist and a dozen unrelated documents — an unsatisfiable grep is a check that gets skipped. **`docs/postmortems/` and `docs/superpowers/` are historical records — never edit them to satisfy this grep.**

- [ ] **7.4 Commit:** `git commit -am "docs(ledger): one contract line, lane order and budgets, ledger commands"`

---

## Task 8 — Full verification, then PR

- [ ] **8.1** `uv run pytest tests/ -v --cov=clients --cov=scripts --cov-fail-under=95` → expect `Required test coverage of 95% reached` and 0 failed. If a deleted parser leaves an uncovered branch, delete the branch — never lower the gate (rule 4).
- [ ] **8.2** `npm run test:alerts` → expect all Node alert tests pass (the alert body format is untouched here; this is the regression guard for pm:2026-08-16-quoted-printable-corrupted-digest).
- [ ] **8.3** `uv run pytest tests/ -W error::RuntimeWarning -q` → expect 0 failed (the async-runner mocking rule from CLAUDE.md "Testing").
- [ ] **8.4 Spec §7 acceptance greps, on the branch:**

  ```bash
  grep -c '^def _.*_section' livewire_scripts/status.py                 # expect: 3   (§7 item 4)
  grep -n 're\.compile' livewire_scripts/status.py livewire_scripts/check_daily_update_watchdog.py   # expect: no output (§7 item 5)
  grep -rn 'MDW_DAILY_JOB_DEADLINE_SECONDS' --include='*.py' .          # expect: no output (§7 item 9, deadline half)
  ```

  §7 item 9's `MDW_FLATFILE_MIN_PUBLISH_RATIO` half belongs to L2 (§4, `clients/constants.py` — L2; does not exist yet) and is **not** checked here.

- [ ] **8.5 End-to-end dry run against a throwaway warehouse** (no real lake, no IB, no real child process):

  ```bash
  export TMPWH=$(mktemp -d) LW_LEDGER_ROOT=$TMPWH/ledger MDW_WAREHOUSE_DIR=$TMPWH MDW_DATA_LAKE=$TMPWH/data-lake
  uv run python - <<'PY'
  import subprocess
  from livewire_scripts import run_daily_update_job as job

  spawned = []

  # `run_with_retries`'s `runner` default is bound at DEFINITION time, so
  # rebinding `job._run_in_own_process_group` does not reach it — the three
  # lanes that matter (futures, cmdty, equity) would spawn real
  # daily_update.py children against a real IB Gateway. Patch the seam both
  # paths actually call instead.
  def fake_attempt(command, log_file, env=None, runner=None, timeout=None):
      spawned.append(list(command))
      return subprocess.CompletedProcess(list(command), 0)

  job.run_daily_update_attempt = fake_attempt

  # The post-success tail shells out too (`_spawn_post_success_quality`, whose
  # `runner` defaults to `subprocess.run`): give it an explicit fake runner.
  real_tail = job.run_post_success_quality
  def tail(config, log_file, runner=None):
      def _runner(cmd, *a, **k):
          spawned.append(list(cmd))
          return subprocess.CompletedProcess(list(cmd), 0, stdout="")
      return real_tail(config, log_file, runner=_runner)
  job.run_post_success_quality = tail

  rc = job.main([])
  assert not any("daily_update.py" in " ".join(map(str, c)) for c in spawned), spawned
  assert not any("livewire_ingest.py" in " ".join(map(str, c)) for c in spawned), spawned
  print(f"dry run rc={rc}, nothing real was spawned ({len(spawned)} stubbed commands)")
  raise SystemExit(rc)
  PY
  uv run python scripts/livewire_ops.py ledger query "select lane, outcome from lane_results order by lane, ended nulls first"
  ```

  Expected stdout of the last command, one JSON object per line — all seven lanes, entry row then terminal row, plus the digest tail:

  ```
  {"lane": "cboe", "outcome": null}
  {"lane": "cboe", "outcome": "done"}
  {"lane": "cmdty", "outcome": null}
  {"lane": "cmdty", "outcome": "done"}
  {"lane": "corporate-actions", "outcome": null}
  {"lane": "corporate-actions", "outcome": "done"}
  {"lane": "digest", "outcome": "done"}
  {"lane": "equity", "outcome": null}
  {"lane": "equity", "outcome": "done"}
  {"lane": "futures", "outcome": null}
  {"lane": "futures", "outcome": "done"}
  {"lane": "fx", "outcome": null}
  {"lane": "fx", "outcome": "done"}
  {"lane": "silver", "outcome": null}
  {"lane": "silver", "outcome": "done"}
  ```

  If `futures`, `cmdty` or `equity` is missing here, `run_with_retries` was not instrumented (decision 8) and "IB-only lanes behind" will read UNKNOWN forever in production — stop and fix it, do not defer it.

  Then: `uv run python scripts/livewire_ops.py ledger query "select job, verdict from runs order by ended nulls first"` → expect two rows, `verdict` `null` then `"OK"`. Clean up: `rm -rf $TMPWH`.

- [ ] **8.6 Push the branch and open the PR, then STOP.**

  ```
  git push -u origin feat/ledger-l1
  gh pr create --base main \
    --title 'feat(ledger): append-only run ledger; per-lane budgets; status reads the ledger' \
    --body-file docs/superpowers/plans/.pr-body-ledger-l1.md
  ```

  PR body (write it to that file first, then delete the file after the PR is open):

  > Implements spec `docs/superpowers/specs/2026-09-02-livewire-ledger-design.md` §1–§3 (L1).
  >
  > - `clients/ledger.py`: six append-only parquet tables, `emit()` through the existing publish path, `query()` over in-memory DuckDB views.
  > - `run_daily_update_job`: no-fallback-first lane order (futures → cmdty → CBOE → FX → corporate-actions → equity → silver) and `LANE_BUDGET_S` per lane, replacing the 4h total; `runs` + `lane_results` + `last_session` rows; `JobDeadline`, the scope parsers, the completion marker and `alerts_undelivered` are gone.
  > - `status.py`: `CHECKS` (name, sql) over the ledger — including "IB-only lanes behind" — and the 10 section functions plus every log regex deleted. The watchdog is now a caller of `collect()`.
  > - `sync_runner` stays (verified: reached by the intraday-catchup launchd job via `run_intraday_catchup_job.py:73`); it gains the same lane rows.
  >
  > Kept against the spec's delete list: `coverage_footer_cache.json` — it is a parquet-footer perf cache for the cold exFAT scan, not parsed state.

  **STOP here.** Merge and `release promote` are separate explicit requests (CLAUDE.md rule 12). The `universe-refresh` job stays unloaded until the promote; reload it then with the command recorded in Task 0(a).
