# Corporate-Actions Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the corporate-actions lane record, in the ledger, how many provider attempts it made, how many were throttled, how many never got a response, how long it spent asleep in pacing/backoff, and how slow the responses were — so "why did corporate-actions take 2.5 hours" is answered by a query instead of a guess — and make that query survive the AppleDouble shadow files the exFAT lake volume leaves beside every parquet.

> **Revision 2026-09-03 (post `/tribunal-review`).** Six findings applied: the emit path is now injectable and therefore testable (it was not); the metric set now covers the time the old set silently excluded (throttle sleep, backoff sleep, and attempts that never returned a response); the verification command now matches CI; the one-PR justification now rests on a measurement instead of an assumption; Task 1's own commands now run the tests they name; the L2 citation is dropped.

**Architecture:** Two halves of one capability, in one PR because neither is worth shipping alone: the write side records the facts, and the read side is currently the thing that crashes when you go to read them. `ledger.query()` → `duckdb_catalog.ledger_query()` hands DuckDB a raw `*/*.parquet` glob, which on the exFAT lake matches the `._*` AppleDouble files macOS writes beside every real parquet; DuckDB then dies with `Invalid Input Error: No magic bytes found at end of file`. Emitting new rows nobody can read back would be a half-fix, so the glob is hardened first (Task 1) and the new measurements land on top of it.

On the write side, `MassiveClient` already calls `telemetry.record_request(endpoint, status, dt_ms)` on every **response**, but `sync_corporate_actions.py` constructs its client without a telemetry object, so every call is a no-op. That seam alone is also not enough to explain elapsed time, and this is the correction that shaped the plan: in `_get` (`clients/massive_client.py:373-416`) `started` is taken _after_ `_throttle()`, `_sleep_backoff` runs _after_ `_record_request`, and a connection/read timeout `continue`s without recording anything at all. A symbol can therefore burn minutes in pacing, backoff and timeouts and contribute zero samples. So this plan also records the two sleeps and the response-less attempts.

Concretely: (1) `MassiveTelemetry` gets thread-safe run totals that work whether or not a JSONL sink is configured, plus a `record_wait`; (2) `MassiveClient` records failed attempts and the time it sleeps; (3) `sync_corporate_actions.run()` takes a telemetry object, passes one shared instance into every worker's client, and emits its totals as `measurements(source='measured')` rows. No new script, no new sink, no change to request behaviour — the lane runs exactly as fast as it does today and now says why.

**Tech Stack:** Python 3.13, `uv`, pytest, pyarrow/DuckDB ledger (`clients/ledger.py`).

**Spec:** `docs/superpowers/specs/2026-09-02-livewire-ledger-design.md` — §3 (the ledger is the fact channel) and §4 (a constant nobody measured is the recurring failure). The governing one-line contract is in `CLAUDE.md`: _"Every job writes its facts to the ledger (`<lake>/ledger/`) and every reader reads the ledger, never a log."_

## Global Constraints

- **Never a log as the fact channel.** The run already prints a JSON summary to stdout; that stays, but the new facts go to the ledger. A reader must never have to parse `daily_update_<date>.log`.
- **Telemetry must never abort the lane.** Wrap the emit in `try/except Exception` and log, exactly as `coverage_report.py:539-542` does. A corporate-actions run that succeeded must not fail because the ledger write did.
- **Thread safety is mandatory.** `_fetch_parallel` builds one `MassiveClient` per worker (default 4) from `client_factory()` and runs them in a `ThreadPoolExecutor`. One shared telemetry object is written by all of them concurrently.
- **`ledger.emit` refuses zero rows** (`clients/ledger.py:168-169`). A run with no recorded requests (injected-client tests, `--dry-run` with no work) must skip the emit, not crash.
- **Measurement naming follows the existing rows:** `name` is a bare snake_case metric, `scope` is the lane (`"corporate-actions"`), `source` is `"measured"`. Model: `coverage_report.py:528-537` (`coverage_elapsed_s` / `scope="all"`). The metric names carry **no lane prefix** — `scope` already holds the lane, and the same names must be reusable when fx or equity get the same treatment.
- **One PR, both halves.** The repo rule is one change / one PR. The honest justification, after checking: `find ~/market-warehouse/data-lake -name '._*'` on **the mini, 2026-09-03, returns 0 files** — so the claim that the read path is _today_ broken on the real lake is **not** established, and the earlier draft asserted it without measuring. What remains true is (a) the failure is reproducible on demand (verified locally on duckdb 1.5.5), (b) the sweep that removes sidecars is `housekeeping --appledouble`, which is **opt-in and deliberately never nightly** (34 min, 97.5% I/O wait), so nothing prevents them recurring, and (c) the read path is the only way to verify the rows Task 3 writes. That is enough to keep them together and not enough to claim an outage. Confound to keep in mind: a Monitor task in the authoring session sweeps AppleDouble files, so the zero may be its doing.
- **`uv` only.** Never bare `python`/`pip`.
- **The verification command is CI's, verbatim:** `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning` (`.github/workflows/ci.yml:53`). Do **not** pass `--cov=clients --cov=scripts`: that overrides `pyproject.toml:34-35` (`source = ["clients", "livewire_scripts"]`) and measures the four thin `scripts/` entrypoints instead of `livewire_scripts/`, where most of this change lives. `CLAUDE.md`'s Testing section still names the wrong command; Task 4 fixes it.

## Non-Goals (deliberate, with reasons)

- **Not tuning `--workers` or adding preemptive pacing.** `sync_corporate_actions.py:254` builds `MassiveClient(...)` with no `min_interval_seconds`, so unlike FX (5 rpm, preemptive) this lane has no declared rate ceiling. Picking one now would be a guessed constant — the exact failure mode this repo's rule 2 ("Measure before you recommend"; "Constants are measured cold on the real lake") exists to stop. This plan produces the measurement that a later change would be entitled to act on.
- **Not adding a `status.py` check for the new rows.** A check needs a threshold, and there is no measured baseline to set one from — that is what this plan produces. (Drift-vs-declared belongs to a later ledger stage; no such plan is on this branch, so it is not cited here.)
- **Not wiring a JSONL sink.** `jsonl_path=None` is the intended production configuration; see the trap in Task 2.

---

### Task 1: `ledger_query` ignores AppleDouble shadow files

The lake lives on an exFAT volume (`/Volumes/DATA_LAKE`), where macOS writes an `._<name>` AppleDouble sidecar next to files. `ledger_query` globs `*/*.parquet`, which matches those sidecars, and hands the raw glob string to DuckDB.

Reproduced on this machine, duckdb 1.5.5 — a directory holding `r.parquet` plus a junk `._r.parquet`:

```
GLOB FAILS: InvalidInputException Invalid Input Error:
  No magic bytes found at end of file '.../date=2026-09-03/._r.parquet'
```

Note this glob pattern is not an implementation slip — it is specified verbatim in the design spec §1 and in the L1 plan's `_LEDGER_VIEW_SQL`. This task hardens a designed contract against a filesystem the design did not account for; it does not redesign it.

Two things were verified before choosing the fix, because the obvious one does not work:

- **A bound list parameter cannot be used here.** `read_parquet(?, union_by_name=true)` works in a plain `SELECT`, but inside the `CREATE OR REPLACE TEMP VIEW` this function actually uses it fails with `Binder Error: Unexpected prepared parameter. This type of statement can't be prepared!` So the file list has to be interpolated, exactly as the current `{glob!r}` already interpolates the glob string — passing a `list[str]` through the same `!r` produces valid DuckDB list syntax.
- **The hive partition column survives.** The current glob makes DuckDB infer a `date` column from the `date=YYYY-MM-DD` directory. An explicit file list infers it identically (verified: rows come back as `(1, datetime.date(2026, 9, 3))` either way), so no reader's column set changes. Note this is a _property of the fix_, not something the suite guards: no test asserts the hive `date` column, and `status.py`'s `CHECKS` use SQL `date(started)`, not the partition column. Do not describe the `status` suite as the regression check for it.

**Twin check (CLAUDE.md rule 5), done:** `grep -n read_parquet clients/duckdb_catalog.py` finds one other template, `_VIEW_SQL` (line 59), which globs bronze on the same exFAT volume. It is **immune**: its `ViewSpec.glob` (line 81) is `<dir>/*/<literal filename>`, so the wildcard sits in the directory position and can never match a `._x.parquet`. No second fix is needed; lines 308 and 397 already pass explicit `{files!r}` lists, which is the local precedent this fix follows.

**Files:**

- Modify: `clients/duckdb_catalog.py:153-175` (`_LEDGER_VIEW_SQL` and `ledger_query`)
- Test: `tests/test_duckdb_catalog.py`

**Interfaces:**

- Unchanged public signature: `ledger_query(sql: str, *, root: Path, tables: Mapping[str, pa.Schema]) -> list[dict]`. Only which files it feeds DuckDB changes.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_duckdb_catalog.py`. `pa`, `pq` and `Path` are already imported there (lines 12-16), but `ledger_query` is **not** in that file's import block (lines 19-32) — add it, alphabetically between `ensure_view` and `read_symbols`:

```python
from clients.duckdb_catalog import (
    ...
    ensure_view,
    ledger_query,
    read_symbols,
    ...
)
```

Worth knowing before writing these: `grep -rln ledger_query tests/` returns **nothing**. The function every ledger reader goes through has no direct test at all, which is how a glob that matches `._*` reached production. These two tests are its first.

```python
def test_ledger_query_ignores_appledouble_shadow_files(tmp_path):
    """The lake is exFAT: macOS drops a `._x.parquet` beside every `x.parquet`,
    and DuckDB dies on it with "No magic bytes found at end of file"."""
    directory = tmp_path / "runs" / "date=2026-09-03"
    directory.mkdir(parents=True)
    pq.write_table(pa.table({"run_id": ["daily-update-1"]}), directory / "runs.parquet")
    (directory / "._runs.parquet").write_bytes(b"Mac OS X AppleDouble stub")

    rows = ledger_query(
        "select run_id from runs",
        root=tmp_path,
        tables={"runs": pa.schema([("run_id", pa.string())])},
    )

    assert [row["run_id"] for row in rows] == ["daily-update-1"]


def test_ledger_query_treats_a_shadow_only_directory_as_empty(tmp_path):
    """A directory holding nothing but sidecars has no rows — it is not an error,
    and it must not build a view over an empty file list."""
    directory = tmp_path / "runs" / "date=2026-09-03"
    directory.mkdir(parents=True)
    (directory / "._runs.parquet").write_bytes(b"Mac OS X AppleDouble stub")

    rows = ledger_query(
        "select run_id from runs",
        root=tmp_path,
        tables={"runs": pa.schema([("run_id", pa.string())])},
    )

    assert rows == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_duckdb_catalog.py -k "appledouble or shadow_only" -v`
(`-k appledouble` alone silently collects only the first of the two — check the collected count is 2.)
Expected: the first FAILs with `duckdb.InvalidInputException: ... No magic bytes found at end of file '.../._runs.parquet'`; the second FAILs the same way (today the sidecar makes `any(...)` true, so a view is built over it).

- [ ] **Step 3: Write minimal implementation**

In `clients/duckdb_catalog.py`, rename the placeholder in the SQL template to say what it now holds, and filter the listing once:

```python
_LEDGER_VIEW_SQL = "CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM read_parquet({files!r}, union_by_name=true)"


def _ledger_files(directory: Path) -> list[str]:
    """Return the real parquet under `directory`, minus macOS AppleDouble sidecars.

    The lake is exFAT, so every `x.parquet` may have a `._x.parquet` beside it.
    DuckDB reads whatever the glob matches and fails the whole query on the
    first sidecar, so the glob is resolved here rather than handed to DuckDB.
    """
    return sorted(str(path) for path in directory.glob("*/*.parquet") if not path.name.startswith("._"))


def ledger_query(
    sql: str,
    *,
    root: Path,
    tables: Mapping[str, pa.Schema],
) -> list[dict]:
    """Run SQL over append-only ledger parquet in a read-only memory database."""
    con = duckdb.connect(":memory:")
    try:
        for name in tables:
            files = _ledger_files(Path(root) / name)
            if files:
                con.execute(_LEDGER_VIEW_SQL.format(name=name, files=files))
            else:
                con.register(name, pa.Table.from_batches([], schema=tables[name]))
        cursor = con.execute(sql)
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    finally:
        con.close()
```

The single glob now serves both the emptiness check and the view, so a shadow-only directory can no longer pass the check and then build a view over nothing.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_duckdb_catalog.py tests/test_status.py tests/test_ledger.py -v`
Expected: PASS. `status.py` and the watchdog are the heaviest `ledger_query` readers, so their suites are the regression check that the view still resolves the columns those readers actually select. They do **not** cover the hive `date` column (nothing does); if you want that guarded, assert it in the first new test rather than claiming these suites do.

- [ ] **Step 5: Commit**

```bash
git add clients/duckdb_catalog.py tests/test_duckdb_catalog.py
git commit -m "fix(ledger): skip AppleDouble sidecars when building ledger views"
```

---

### Task 2: `MassiveTelemetry` records run totals

`MassiveTelemetry` is currently a stub with no production caller (`grep -rn MassiveTelemetry` finds only its definition and its tests). Two traps decide the implementation:

1. **`BaseTelemetry` disables itself when `jsonl_path is None`** (`clients/telemetry.py:53-56`): `start()` sets `_disabled = True` and `_emit()` returns early. Production wants no JSONL file, so counting must happen _before_ and _independently of_ `self._emit(...)`.
2. **`start()` is never called on this code path.** `MassiveClient.start()`/`stop()` run from `__enter__`/`__exit__`, but `_fetch_parallel` (`sync_corporate_actions.py:164-181`) calls `client_factory()` and later `client.close()` directly — it never uses `with client`. So `_started` stays `False` and `_emit()` returns early even if a path were configured. Counting outside the `_emit` gate is the only thing that works here.

**Files:**

- Modify: `clients/telemetry.py:216-244` (the `MassiveTelemetry` class)
- Test: `tests/test_telemetry.py`

**Interfaces:**

- Produces: `MassiveTelemetry.summary() -> dict[str, float]` with keys `requests` (every HTTP **attempt**, response or not), `throttled` (attempts answered 429), `errors` (attempts that never produced a response — connection/read timeout, recorded as `status=0`), `wait_s` (seconds slept in pacing + backoff), `latency_p95_ms` (p95 of per-attempt socket time; `0.0` when nothing was measured). Consumed by Task 3.
- Produces: `record_wait(seconds: float) -> None`. Consumed by `MassiveClient` (Task 2b).
- Unchanged: `record_request(endpoint: str, status: int, dt_ms: int) -> None`, `record_rate_limit(remaining: int, reset_at: int) -> None`.

**Why `errors` and `wait_s` exist, and why `latency_p95_ms` alone was rejected:** the earlier draft claimed p95 could separate "slow endpoint" from "long backoff tail". It cannot — read `_get` (`clients/massive_client.py:373-416`): `started` is taken after `_throttle()`, `_sleep_backoff` runs after `_record_request`, and a timeout `continue`s without recording. p95 therefore never contained the backoff tail in the first place. `wait_s` is the sleep, `errors` is the invisible attempt, and p95 is honestly just socket time. Together they sum to something you can compare against `lane_results.elapsed_s`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_telemetry.py`:

```python
def test_massive_totals_are_recorded_without_a_jsonl_sink():
    """Production passes jsonl_path=None and never calls start(); the counters
    still have to work. BaseTelemetry disables _emit in both of those cases."""
    t = MassiveTelemetry(jsonl_path=None)

    for dt_ms in range(1, 101):
        t.record_request(endpoint="/v3/reference/splits", status=200, dt_ms=dt_ms)
    t.record_request(endpoint="/v3/reference/splits", status=429, dt_ms=4000)
    t.record_request(endpoint="/v3/reference/splits", status=0, dt_ms=30000)
    t.record_wait(12.5)
    t.record_wait(0.5)

    summary = t.summary()
    assert summary["requests"] == 102
    assert summary["throttled"] == 1
    assert summary["errors"] == 1
    assert summary["wait_s"] == 13.0
    # 102 samples, the 429 and the timeout being the slowest: p95 sits in the high tail.
    assert summary["latency_p95_ms"] >= 97.0


def test_massive_summary_is_zeroed_before_any_request():
    """ledger.emit refuses zero rows, so the caller needs a truthful zero."""
    assert MassiveTelemetry(jsonl_path=None).summary() == {
        "requests": 0,
        "throttled": 0,
        "errors": 0,
        "wait_s": 0.0,
        "latency_p95_ms": 0.0,
    }


def test_massive_totals_survive_concurrent_workers():
    """_fetch_parallel shares one telemetry across 4 client threads."""
    t = MassiveTelemetry(jsonl_path=None)

    def hammer() -> None:
        for _ in range(500):
            t.record_request(endpoint="/v3/reference/splits", status=200, dt_ms=10)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for future in [executor.submit(hammer) for _ in range(4)]:
            future.result()

    assert t.summary()["requests"] == 2000
```

Add the import at the top of `tests/test_telemetry.py`:

```python
from concurrent.futures import ThreadPoolExecutor
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_telemetry.py -k massive_ -v`
Expected: FAIL with `AttributeError: 'MassiveTelemetry' object has no attribute 'summary'`

- [ ] **Step 3: Write minimal implementation**

In `clients/telemetry.py`, add `from statistics import quantiles` and `from threading import Lock` to the imports, then replace the body of `MassiveTelemetry`:

```python
class MassiveTelemetry(BaseTelemetry):
    """Massive.io telemetry: per-request JSONL (optional) plus run totals.

    The totals are kept outside `_emit` on purpose. Production constructs this
    with `jsonl_path=None`, and `_fetch_parallel` never enters the client as a
    context manager, so `_started` stays False — both of which make `_emit` a
    no-op. Counting there would silently record nothing.
    """

    def __init__(self, *, jsonl_path: Path | None, source: str = "massive"):
        super().__init__(source=source, jsonl_path=jsonl_path)
        self._totals_lock = Lock()
        self._requests = 0
        self._throttled = 0
        self._errors = 0
        self._wait_s = 0.0
        self._latencies_ms: list[int] = []

    def start(self) -> None:
        super().start()
        if not self._disabled:
            _logger.info("MassiveTelemetry started (stub; Sub-C activates record_request)")

    def record_request(self, endpoint: str, status: int, dt_ms: int) -> None:
        with self._totals_lock:
            self._requests += 1
            if status == 429:
                self._throttled += 1
            elif status == 0:  # never got a response: connection or read timeout
                self._errors += 1
            self._latencies_ms.append(int(dt_ms))
        self._emit(
            {
                "event": "massive_request",
                "endpoint": endpoint,
                "status": int(status),
                "dt_ms": int(dt_ms),
            }
        )

    def record_rate_limit(self, remaining: int, reset_at: int) -> None:
        self._emit(
            {
                "event": "massive_rate_limit",
                "remaining": int(remaining),
                "reset_at": int(reset_at),
            }
        )

    def record_wait(self, seconds: float) -> None:
        """Accumulate time slept in pacing or retry backoff.

        This is the half of the lane's elapsed time that `record_request` cannot
        see: `_throttle()` runs before the clock starts and `_sleep_backoff`
        runs after the request is recorded.
        """
        with self._totals_lock:
            self._wait_s += float(seconds)

    def summary(self) -> dict[str, float]:
        """Return run totals. `latency_p95_ms` is 0.0 when nothing was measured."""
        with self._totals_lock:
            requests = self._requests
            throttled = self._throttled
            errors = self._errors
            wait_s = self._wait_s
            samples = sorted(self._latencies_ms)
        if len(samples) < 2:
            p95 = float(samples[0]) if samples else 0.0
        else:
            p95 = float(quantiles(samples, n=20, method="inclusive")[18])
        return {
            "requests": requests,
            "throttled": throttled,
            "errors": errors,
            "wait_s": round(wait_s, 3),
            "latency_p95_ms": p95,
        }
```

`quantiles(..., n=20)[18]` is the 95th percentile; `method="inclusive"` avoids extrapolating past the observed maximum on small samples. Holding ~15K ints for a full universe run is roughly 120 KB — not worth a streaming estimator.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_telemetry.py -v`
Expected: PASS, including the two pre-existing `MassiveTelemetry` tests at lines 237 and 255.

- [ ] **Step 5: Commit**

```bash
git add clients/telemetry.py tests/test_telemetry.py
git commit -m "feat(telemetry): give MassiveTelemetry run totals that work with no sink"
```

---

### Task 2b: `MassiveClient` records the attempts and the sleeps it currently hides

Without this task the new metrics are a response counter with a blind spot the size of the problem. Three edits in `_get` and its helpers, all inside the `self._telemetry is None` early-return that already exists.

**Files:**

- Modify: `clients/massive_client.py` — `_get` (378-383), `_throttle` (476-484), `_sleep_backoff` (486-494)
- Test: `tests/test_massive_client.py`

**Interfaces:**

- Consumes: `MassiveTelemetry.record_wait(seconds)` and the existing `record_request` from Task 2.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_connection_timeout_is_recorded_as_an_attempt_not_silence():
    """_get used to `continue` on ReqTimeout without recording anything, so a
    symbol could burn two minutes in timeouts and show up as zero requests."""
    telemetry = MassiveTelemetry(jsonl_path=None)
    client = MassiveClient(api_key="fixture-token", max_retries=1, backoff_factor=0, telemetry=telemetry)
    client._session = _AlwaysTimesOut()

    with pytest.raises(MassiveAPIError):
        client._get("/v3/reference/splits")

    summary = telemetry.summary()
    assert summary["requests"] == 2
    assert summary["errors"] == 2


def test_backoff_sleep_is_recorded_as_wait_not_latency():
    """The backoff sleep happens after _record_request, so it can never appear
    in latency_p95_ms — it has to be counted separately or it is invisible."""
    telemetry = MassiveTelemetry(jsonl_path=None)
    client = MassiveClient(api_key="fixture-token", max_retries=1, backoff_factor=0, telemetry=telemetry)

    client._sleep_backoff(attempt=3)

    assert telemetry.summary()["wait_s"] == 0.0  # backoff_factor=0 → slept 0s, but it was recorded
    assert telemetry.summary()["requests"] == 0
```

Note the second test asserts the _plumbing_, not a duration: sleeping for real in a unit test is banned, so `backoff_factor=0` gives a truthful zero through the real code path. `_AlwaysTimesOut` is a two-line double whose `get` raises `requests.exceptions.ReadTimeout`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_massive_client.py -k "connection_timeout_is_recorded or backoff_sleep_is_recorded" -v`
Expected: the first FAILs with `requests == 0`; the second FAILs with `KeyError: 'wait_s'` until Task 2 is in place, then passes trivially — it is the plumbing guard, not the interesting one.

- [ ] **Step 3: Write minimal implementation**

In `_get`, record the attempt that never got a response:

```python
            except (ReqConnectionError, ReqTimeout) as exc:
                last_exc = exc
                self._record_request(endpoint, 0, started)  # status 0: no response
                if attempt < self._max_retries:
                    self._sleep_backoff(attempt)
                    continue
                raise MassiveAPIError(f"Connection failed after {attempt + 1} attempts: {exc}") from exc
```

And record both sleeps. `_throttle`:

```python
            if wait > 0:
                time.sleep(wait)
                self._record_wait(wait)
```

`_sleep_backoff` — restructured so the one exit point records what it slept:

```python
    def _sleep_backoff(self, attempt: int, resp: requests.Response | None = None) -> None:
        retry_after = None if resp is None else resp.headers.get("Retry-After")
        delay = self._backoff_factor * (2**attempt)
        if retry_after is not None:
            try:
                delay = float(retry_after)
            except (TypeError, ValueError):
                pass
        time.sleep(delay)
        self._record_wait(delay)

    def _record_wait(self, seconds: float) -> None:
        if self._telemetry is None:
            return
        self._telemetry.record_wait(seconds)
```

`_sleep_backoff`'s behaviour is unchanged apart from the recording: a malformed `Retry-After` still falls through to the exponential delay, and a well-formed one still wins.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_massive_client.py -v`
Expected: PASS, the pre-existing retry/backoff tests included — they assert on `time.sleep` calls and behaviour, both unchanged.

- [ ] **Step 5: Commit**

```bash
git add clients/massive_client.py tests/test_massive_client.py
git commit -m "feat(massive): record response-less attempts and time slept in backoff"
```

---

### Task 3: The corporate-actions lane emits its provider totals to the ledger

**The seam this task exists to close, and the trap the first draft fell into.** `run()` resolves its client as `client_factory or default_client_factory` (`livewire_scripts/sync_corporate_actions.py:280` and `:292`). `default_client_factory` is the only place a telemetry object would be attached — so any test that injects a `client_factory`, which is the only way to test this lane, **bypasses it**. The first draft's test therefore could not have passed, and worse, it would have shipped a production-only code path with a green suite: CLAUDE.md rule 6 exactly. The fix is to make the telemetry an argument of `run()` rather than a private closure variable, and to cover the wiring inside `default_client_factory` with its own test.

**Files:**

- Modify: `livewire_scripts/sync_corporate_actions.py` — the imports, the `run()` signature, `default_client_factory` (lines 253-254), and the summary block (lines 341-350)
- Test: `tests/test_sync_corporate_actions.py` (including `_Client`, lines 42-55)

**Interfaces:**

- Consumes: `MassiveTelemetry.summary()` from Task 2, populated by Task 2b.
- Produces: `run(..., telemetry: MassiveTelemetry | None = None)`. `None` means "construct one", which is what production does.
- Produces: five `measurements` rows per run, `scope="corporate-actions"`, `source="measured"`:
  | `name`                    | `unit`  | what it is                                              |
  | ------------------------- | ------- | ------------------------------------------------------- |
  | `provider_requests`       | `count` | HTTP attempts, response or not                            |
  | `provider_throttled`      | `count` | attempts answered 429                                     |
  | `provider_errors`         | `count` | attempts that never got a response                        |
  | `provider_wait_s`         | `s`     | seconds slept in pacing + backoff                         |
  | `provider_latency_p95_ms` | `ms`    | p95 socket time per attempt (**not** including the sleeps) |

Names carry no `massive_ca_` prefix: `scope` already says which lane and `source` says how it was obtained, so prefixing duplicates two fields and blocks fx/equity from reusing the same names later.

`run_id` resolution follows `clients/quality_flags.py:185`: `os.environ.get("LW_RUN_ID") or ledger.new_run_id("corporate-actions")`. Under the nightly job `LW_RUN_ID` is always inherited — `run_daily_update_job.py:910` calls `os.environ.setdefault("LW_RUN_ID", ...)` and line 913 does `env = os.environ.copy()` afterwards — so these rows join the lane's `lane_results` row on `run_id`. A hand-run gets its own id rather than failing.

- [ ] **Step 1: Give `_Client` a `close()`**

`_Client` (`tests/test_sync_corporate_actions.py:42-55`) has `get_splits` and `get_dividends` and nothing else. `run()`'s cleanup calls `owned_client.close()`, so the moment a test drives the owned-client path it dies with `AttributeError` before any assertion runs. Add:

```python
    def close(self):
        self.closed = True
```

and set `self.closed = False` in `__init__`.

- [ ] **Step 2: Write the failing tests**

Add to `tests/test_sync_corporate_actions.py`:

```python
def test_provider_totals_reach_the_ledger_not_just_the_log(tmp_path, monkeypatch):
    """2026-09-03: the lane ran 2h15m and nothing on disk said whether it was
    rate-limited, timing out, or just slow — telemetry was never passed."""
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260903T060005Z-49009")
    telemetry = MassiveTelemetry(jsonl_path=None)
    telemetry.record_request(endpoint="/v3/reference/splits", status=200, dt_ms=120)
    telemetry.record_request(endpoint="/v3/reference/splits", status=429, dt_ms=90)
    telemetry.record_wait(4.25)

    rc = sync_corporate_actions.run(
        ["--tickers", "AAPL", "--workers", "1"],
        client=_Client(),
        store=_Store(),
        data_lake_root=tmp_path,
        telemetry=telemetry,
    )

    assert rc == 0
    rows = ledger.query(
        "select name, scope, source, unit, value from measurements "
        "where run_id = 'daily-update-20260903T060005Z-49009' order by name"
    )
    assert [row["name"] for row in rows] == [
        "provider_errors",
        "provider_latency_p95_ms",
        "provider_requests",
        "provider_throttled",
        "provider_wait_s",
    ]
    assert {row["scope"] for row in rows} == {"corporate-actions"}
    assert {row["source"] for row in rows} == {"measured"}
    by_name = {row["name"]: row["value"] for row in rows}
    assert by_name["provider_requests"] == 2.0
    assert by_name["provider_throttled"] == 1.0
    assert by_name["provider_wait_s"] == 4.25


def test_the_default_client_factory_actually_attaches_the_telemetry(tmp_path, monkeypatch):
    """The seam the injected-factory tests skip: production goes through
    default_client_factory, and that is the only place the wiring exists."""
    monkeypatch.setenv("MASSIVE_API_KEY", "fixture-token")
    built: list[dict] = []

    class _Recorder(_Client):
        def __init__(self, **kwargs):
            super().__init__()
            built.append(kwargs)

    monkeypatch.setattr(sync_corporate_actions, "MassiveClient", _Recorder)
    telemetry = MassiveTelemetry(jsonl_path=None)

    sync_corporate_actions.run(
        ["--tickers", "AAPL", "--workers", "1"],
        store=_Store(),
        data_lake_root=tmp_path,
        telemetry=telemetry,
    )

    assert built and built[0]["telemetry"] is telemetry


def test_a_run_that_measured_nothing_emits_nothing(tmp_path, monkeypatch):
    """ledger.emit refuses zero rows; a run that made no measured request
    must skip the emit rather than abort a lane that otherwise succeeded."""
    monkeypatch.setenv("LW_RUN_ID", "manual-20260903T000000Z-1")

    rc = sync_corporate_actions.run(
        ["--tickers", "AAPL"], client=_Client(), store=_Store(), data_lake_root=tmp_path
    )

    assert rc == 0
    assert ledger.query("select count(*) as n from measurements")[0]["n"] == 0
```

Add to that file's imports:

```python
from clients import ledger
from clients.telemetry import MassiveTelemetry
```

No ledger-root setup is needed: `tests/conftest.py:21-28` is an `autouse` fixture that already points `LW_LEDGER_ROOT` at `tmp_path / "ledger"` for every test, and `clients/ledger.py:121-123` honours that override ahead of `data_lake_dir()`. The tests can never touch the real lake.

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_corporate_actions.py -k "provider_totals or default_client_factory_actually or measured_nothing" -v`
Expected: 3 collected. The first two FAIL with `TypeError: run() got an unexpected keyword argument 'telemetry'`; the third passes already (nothing emits today) and is the guard that the emit stays conditional.

- [ ] **Step 4: Write minimal implementation**

Add to the imports in `livewire_scripts/sync_corporate_actions.py`:

```python
from clients import ledger
from clients.telemetry import MassiveTelemetry
```

Add the parameter to `run()` (after `data_lake_root`):

```python
    telemetry: MassiveTelemetry | None = None,
```

and near the top of the body, beside the `evidence` line:

```python
    # One telemetry for all workers: _fetch_parallel builds one client per
    # worker and the totals only mean anything summed across the lane. It is a
    # parameter rather than a local because default_client_factory is bypassed
    # whenever a test injects a client or a factory.
    telemetry = telemetry or MassiveTelemetry(jsonl_path=None)
```

Then `default_client_factory`:

```python
    def default_client_factory() -> MassiveClient:
        return MassiveClient(
            response_evidence_recorder=None if evidence is None else evidence.recorder(),
            telemetry=telemetry,
        )
```

Immediately after `print(json.dumps(summary, sort_keys=True))` (line 350):

```python
    _emit_provider_measurements(telemetry)
```

And the function above `def main(...)`:

```python
_PROVIDER_MEASUREMENTS = (
    ("provider_requests", "requests", "count"),
    ("provider_throttled", "throttled", "count"),
    ("provider_errors", "errors", "count"),
    ("provider_wait_s", "wait_s", "s"),
    ("provider_latency_p95_ms", "latency_p95_ms", "ms"),
)


def _emit_provider_measurements(telemetry: MassiveTelemetry) -> None:
    """Publish what the provider cost this lane. Never aborts the run.

    2026-09-03: corporate-actions ran 2h15m of its 3h budget and nothing
    durable recorded whether it was throttled, timing out, or simply slow,
    because the client was built with telemetry=None.
    """
    totals = telemetry.summary()
    if not totals["requests"]:
        return
    now = datetime.now(UTC)
    run = os.environ.get("LW_RUN_ID") or ledger.new_run_id("corporate-actions")
    rows = [
        {
            "name": name,
            "scope": "corporate-actions",
            "measured_at": now,
            "value": float(totals[key]),
            "unit": unit,
            "source": "measured",
            "run_id": run,
        }
        for name, key, unit in _PROVIDER_MEASUREMENTS
    ]
    try:
        ledger.emit("measurements", rows, run_id=run)
    except Exception as exc:  # pragma: no cover - telemetry must not fail a good run
        print(f"WARNING: could not write provider measurements: {exc}", file=sys.stderr)
```

`datetime`, `UTC`, `os` and `sys` are already imported in this module (lines 8, 9, 13).

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_sync_corporate_actions.py -v`
Expected: PASS, all pre-existing tests included — in particular `test_one_flaky_symbol_in_a_large_run_does_not_fail_the_run` and `test_a_systemic_failure_rate_still_fails_the_run`, which assert on the stdout summary that must stay byte-identical.

- [ ] **Step 6: Run the suite exactly as CI does**

Run: `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning`
Expected: PASS. This is `.github/workflows/ci.yml:53` verbatim; `--cov` with no argument takes `source` from `pyproject.toml:34-35`, which is what puts `livewire_scripts/` on the report. `tests/test_duckdb_containment.py` matters here — `sync_corporate_actions.py` now imports `clients.ledger`, which imports `duckdb_catalog` lazily inside `ledger.query()` (`clients/ledger.py:181`), so the containment rule still holds. If that test fails, the import must stay lazy; do not "fix" it by relaxing the test.

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/sync_corporate_actions.py tests/test_sync_corporate_actions.py
git commit -m "feat(corporate-actions): record provider request totals in the ledger"
```

---

### Task 4: Document the new rows, and fix the stale testing command

**Files:**

- Modify: `docs/runbook.md`
- Modify: `CLAUDE.md` (Testing section)

- [ ] **Step 1: Add the query an operator will actually run**

Under the corporate-actions section of `docs/runbook.md`, add:

```markdown
Why was corporate-actions slow last night?

    uv run python scripts/livewire_ops.py ledger query "select name, value, unit from measurements where scope = 'corporate-actions' and run_id = '<run_id>' order by name"

Read them together; no single one of them answers the question.

- `provider_wait_s` large → the lane is asleep, not working. Throttled (see
  below) or retrying. More `--workers` will not help.
- `provider_throttled` large → the provider is pushing back. More workers make
  it worse; the lane needs preemptive pacing (`min_interval_seconds`) like fx
  has. Note the 5 req/min figure in this repo is **fx-scoped**; this lane's
  ceiling has never been measured.
- `provider_errors` large → attempts are timing out with no response. Each one
  costs a full request timeout plus a backoff, and it is invisible in the
  response counts.
- `provider_latency_p95_ms` high with the three above near zero → the endpoint
  itself is slow. This is the only case where more `--workers` is the lever.

`provider_latency_p95_ms` is socket time per attempt only. It does not include
`provider_wait_s`, by construction — the sleeps happen outside the measured
window.
```

- [ ] **Step 2: Fix the testing command in `CLAUDE.md`**

The Testing section says CI runs `--cov=clients --cov=scripts --cov-fail-under=95`. It does not, and following it measures the wrong package (see Global Constraints). Replace that sentence with the real command from `.github/workflows/ci.yml:53`:

```markdown
`uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning` (what
CI runs; `--cov` takes its `source` from `pyproject.toml`, which is
`clients` + `livewire_scripts` — passing `--cov=<pkg>` overrides that and
silently measures the wrong tree).
```

This is one line inside the same PR, not a follow-up: the plan's own verification step is wrong today because this line is wrong.

- [ ] **Step 3: Commit**

```bash
git add docs/runbook.md CLAUDE.md
git commit -m "docs: how to read corporate-actions provider telemetry, and the real CI test command"
```

---

## Self-Review

**1. Spec coverage.** The governing contract is one line ("every job writes its facts to the ledger... never a log") plus §4's premise that unmeasured operating constants are the recurring failure. Task 2 satisfies both: the facts land in `measurements`, and the number that would justify a future `--workers` or pacing constant now exists as data. The interpretive question this was created to answer — throttled vs. timing out vs. slow — is answered by `provider_throttled` / `provider_errors` / `provider_wait_s` read against `provider_latency_p95_ms`, and Task 4 writes down how to read them.

**2. Placeholder scan.** No TBDs; every code step carries the actual code, and every path, line number and env var in it was read from the current `main` rather than recalled.

**3. Type consistency.** `summary()` returns `dict[str, float]` with keys `requests`/`throttled`/`errors`/`wait_s`/`latency_p95_ms` in Task 2; `_PROVIDER_MEASUREMENTS` in Task 3 reads exactly those five keys and no others. `record_request`'s signature is unchanged, so `MassiveClient._record_request` (`clients/massive_client.py:496-500`), which calls it with keyword arguments `endpoint=`, `status=`, `dt_ms=`, keeps working.

**4. Ablation check.** The first draft's rationale for p95 was **wrong and is withdrawn**: it claimed p95 separates a uniformly slow endpoint from a long backoff tail. It cannot — the backoff sleep happens after `_record_request`, so it was never in the samples. That is why Task 2b exists, and the ablation is now: remove `provider_wait_s` and a rate-limited lane looks identical to a healthy one that made the same number of requests; remove `provider_errors` and a lane drowning in timeouts reports fewer requests than it made, which reads as *less* work rather than more. Both fail, so both stay. p95 survives on a narrower claim — it distinguishes a slow endpoint from a fast one once the sleeps are accounted for separately. Removing the shared-telemetry/lock design in favour of per-worker objects was rejected: the totals would have to be summed by a caller across clients `_fetch_parallel` does not return. The JSONL sink, by contrast, *was* ablated — nothing reads it, `start()` is never called on this path, and the ledger is the required channel — so it stays `None` and unwired. The `massive_ca_` name prefix was also ablated out: `scope` already carries the lane.

**5. Known bound in Task 1.** File paths are interpolated with `!r`, so a lake path containing a single quote would produce a double-quoted Python repr that DuckDB reads as an identifier rather than a string. Ledger paths are `<root>/<table>/date=YYYY-MM-DD/<run_id>[-n].parquet` and run ids are `<job>-<utc-ts>-<pid>`, so no quote can appear; a bound parameter is not available as an alternative (verified above). Worth a line in the fix, not a sanitiser.

**6. Known gap, deliberately left.** These rows describe the provider, not the lane's wall-clock. `lane_results.elapsed_s` already carries the latter, written by the orchestrator, so nothing new is needed to join them. `provider_wait_s + (requests x latency)` will not reconcile exactly to it — parquet writes, store commits and cursor I/O are outside every counter here, and no attempt is made to close that gap.\n\n**7. What the tribunal changed.** Six findings, all verified against the code before being applied: the untestable emit seam (Task 3's new signature and `default_client_factory` test), the metric set that excluded the sleeps (Task 2b), the coverage command that measured the wrong tree (Global Constraints + Task 4 Step 2), the unmeasured one-PR premise (Global Constraints, now stating the mini's 0-file result), the `-k` filter that collected one of two tests plus the false claim that `status`'s suite guards the hive column (Task 1), and a citation to a plan not on this branch (Non-Goals). Panel: Claude / Codex / Cursor-Grok; Gemini unlicensed on this machine.

## Execution Handoff

Plan saved. Note that per the user's standing session rule, approved plans are executed with `/execute-plan` (worktree → straight-through implementation → milestone commits), not via subagent fan-out. The worktree already exists at `.worktrees/fix-corporate-actions-telemetry` on branch `fix/corporate-actions-observability`.
