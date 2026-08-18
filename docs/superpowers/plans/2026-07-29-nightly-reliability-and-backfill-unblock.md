# Nightly Reliability + Backfill Unblock Implementation Plan

> **STATUS 2026-08-18 — LANDED, CHECKBOXES NEVER TICKED.** The work in this plan
> shipped directly through PRs #72-#87, not through the plan runner, so the boxes
> below read 1/48 and mean nothing. Do not re-run it. Spot-checked on main at
> commit `15e525d`: `npm ci` in `release.py`, `MDW_DAILY_JOB_DEADLINE_SECONDS` in
> `run_daily_update_job.py`, the `_page_failure` call in `_run_scheduled_lane`,
> `launchd/com.livewire.coverage.plist.example`, and `_quality_jobs_section` in
> `status.py` are all present. Kept as the record of why those changes exist.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the nightly job's ability to die loudly instead of wedging silently, then remove the false capacity reading that blocks any deep flat-file backfill.

**Architecture:** Two PRs of code changes plus three read-only probes. The
nightly job cannot report a failure today, in four separate ways: no timeout at
all (a wedge blocks launchd forever), no `nodemailer` in the release (a detected
failure sends nothing), `_run_scheduled_lane` has no alert path (so the lane
that actually wedged could never page), and post-success quality jobs fail into
an uncounted WARNING (`coverage` has timed out every night since 2026-07-07 and
nothing said so). Investigations with no confirmed root cause are scoped as
probes with persisted output, never as speculative fixes.

**What the ordering actually requires — stated honestly.** Only **Phase 0 is a
hard prerequisite**: nothing should read or write the lake while a wedged
process holds it. Everything after that is a *deployment preference*, not a
dependency:

- Phase 1 → Phase 3 **is** required: a multi-day backfill on a volume with an
  undiagnosed wedge should not start before failures are detectable.
- Phase 1 → Phase 2 is **not** required. The three probes are read-only and
  consume neither alerts nor timeouts; they need only Phase 0 and no competing
  writer. Run them in parallel with Phase 1 if that is convenient.

An earlier draft claimed the whole chain was forced. It is not, and presenting a
preference as a constraint hides a scheduling option the operator should have.

**Tech Stack:** Python 3.13, `uv`, pytest (95% coverage gate), `subprocess`,
Node 25 / npm 11 (`nodemailer` 8.0.2), macOS launchd, Massive S3 flat files.

## Global Constraints

- Never add `Co-Authored-By:` or any AI-attribution trailer to a commit message.
- Push a branch and open a PR via `gh pr create`; never push to `main` directly.
- One change, one PR. Amend the existing branch rather than opening a follow-up.
- Never merge before CI is green.
- Worktrees live only under `.worktrees/<branch-slug>/`.
- All new code in `clients/` and `scripts/` must have tests; `fail_under = 95`.
- `uv run pytest` is the command that matches CI.
- Tests mock all external I/O. No test opens a socket or hits the network.
- Never present invented market values as real. Probe results are recorded with
  their as-of date and the exact command that produced them.
- IB Gateway is on `127.0.0.1:4001`. Never restart it from this repo, never
  auto-retry an IB connection failure.
- Research/probe output is persisted to durable storage, never left in stdout.

## Measured Baseline (2026-07-29, do not re-derive)

| Fact | Value | Source |
|---|---|---|
| Heaviest single `daily-update` lane, 12-day sample | **1.87 h** (07-25); next 1.41 h, 0.61 h, 0.59 h, 0.58 h, 0.23 h | `daily_update_2026-07-*.log` marker pairs |
| **Whole-job** wall clock, 28-day sample — healthy | **3.27 h** max (07-25); 3.22 h (07-26), 2.94 h (07-27) | first→last timestamp in each `daily_update_2026-07-*.log` |
| **Whole-job** wall clock — anomalous | 4.96 h (07-22), 8.10 h (07-19), 10.32 h (07-23), 19.44 h (07-28) | same |
| Watchdog ceiling | **4.5 h** — job starts 06:00 UTC, watchdog checks 10:30 UTC | plist `StartCalendarInterval` |
| Lanes run per job | **7**, sequentially: corporate-actions, equity, futures, cmdty, CBOE, FX, Silver | `run_daily_update_job.py:669-701` |
| `run_daily_update_attempt` timeout | **none** | `livewire_scripts/run_daily_update_job.py:175-190` |
| `_run_scheduled_lane` alert calls | **zero** — corporate-actions / CBOE / FX / Silver cannot page | `run_daily_update_job.py:562-584` |
| `send_failure_alert` reachability | line 409, **only** by falling out of the retry loop; 371 and ~380 return past it | `run_daily_update_job.py` |
| Release Node deps | **absent** — `node_modules/` is gitignored, `git archive` omits it | watchdog log `Cannot find package 'nodemailer'` |
| Capacity guard measures | `/dev/disk3s5`, **24 GiB** avail | `livewire_scripts/flatfile_planner.py:40` |
| Data actually lands on | `/dev/disk7s2` → `/Volumes/DATA_LAKE`, **6.6 TiB** avail | `df -h ~/market-warehouse/data-lake` |
| `MDW_FLATFILE_MIN_FREE_GB` default | 25 GiB — **above** the 24 GiB it misreads | `flatfile_planner.py:45` |
| Both flat-file stores' write root | `warehouse_dir / "data-lake" / "raw" / "massive" / "us_stocks_sip" / {minute,day}_aggs_v1` — **hardcoded, does not consult `MDW_DATA_LAKE`** | `clients/massive_flatfile_store.py:38`, `clients/massive_daily_flatfile_store.py:39` |
| `coverage` outcome every night since ≥ 2026-07-07 | `timed out after 600 seconds`, logged as a WARNING that never affects the exit code | `daily_update_2026-07-{07..27}.log` |
| Raw flat files held | 1285 days, 2021-06-11 → 2026-07-27, both `day_aggs` and `minute_aggs` | `cursors/massive_*_flatfile_state.json` |
| Provider LIST claims | 5755 days back to 2003-09-10 | same state files, `discovery.earliest` |
| **Provider GET floor — MEASURED 2026-07-29** | **2021-07-28**. 2021-07-27 and every earlier probe → `403 Forbidden`. Identical for `day_aggs` and `minute_aggs`. 1827 days = **5.00 years** — a rolling window, same as the `/v2/aggs` REST floor | `logs/probes/2026-07-29-flatfile-get-floor.json` |

---

## Phase 0: Stop the bleeding (operational, no code, no PR)

The wedged instances block launchd from ever firing again. Nothing in this plan
can be verified against a live run until they are gone. **Progress lost is
zero** — the stuck child has burned 0.2 s of CPU in 20 hours.

- [ ] **Step 1: Record the wedge state before touching it**

The exFAT hang has no confirmed root cause. Capture evidence before it is
destroyed, so Phase 4 has something to work from.

```bash
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=~/market-warehouse/logs/wedge-forensics/$STAMP
mkdir -p "$OUT"
for p in $(pgrep -f livewire_ops.py) $(pgrep -f livewire_ingest.py); do
  ps -o pid=,ppid=,etime=,time=,stat=,command= -p "$p" >> "$OUT/ps.txt" 2>/dev/null
  # `sample -file <path>` was tested on this host and exits non-zero; redirect
  # stdout instead, which works regardless of the flag spelling.
  sample "$p" 3 > "$OUT/sample-$p.txt" 2>&1 || true
  lsof -p "$p" > "$OUT/lsof-$p.txt" 2>/dev/null || true
done
df -h ~/market-warehouse ~/market-warehouse/data-lake > "$OUT/df.txt"
```

- [ ] **Step 2: Kill the daily-update chain by process group, not by PID**

**Verified precondition:** neither the plist templates nor the installed
`~/Library/LaunchAgents/com.livewire.daily-update.plist` sets `KeepAlive`
(`grep -c KeepAlive` → 0). They use `StartCalendarInterval`, so killing does
**not** trigger an immediate relaunch into the same wedge — the job waits for
its next scheduled time. If a future plist adds `KeepAlive`, `launchctl unload`
it before killing or this step loops forever.

Killing the wrapper PID alone reparents its descendants to `init` and they keep
running — the wedged grandchild is the one that matters. Kill the group:

**PIDs are reused by the OS.** Never build a kill command from a PID observed
in an earlier session — prove the identity first, then kill only what you
proved:

```bash
# 1. Get the CURRENT wrapper PID from launchd, never from notes.
WRAPPER=$(launchctl list | awk '$3=="com.livewire.daily-update" {print $1}')
[ "$WRAPPER" = "-" ] && { echo "job not running; Phase 0 is a no-op"; exit 0; }

# 2. Prove it is what you think it is BEFORE signalling anything.
ps -p "$WRAPPER" -o pid=,ppid=,etime=,time=,command=
#    Expect: a livewire command under <warehouse>/current or livewire_ops.py.
#    If the command line is anything else, STOP — the PID was reused.

# 3. Only now derive the group, and look at every member.
PGID=$(ps -o pgid= -p "$WRAPPER" | tr -d ' ')
pgrep -g "$PGID" | xargs -r ps -o pid=,etime=,time=,command= -p

# 4. Kill the group.
kill -TERM -"$PGID" 2>/dev/null; sleep 5
pgrep -g "$PGID" || echo "group gone"
```

If anything survives `TERM`, escalate:

```bash
kill -KILL -"$PGID" 2>/dev/null; sleep 5
pgrep -g "$PGID" || echo "group gone"
```

**If a process survives `SIGKILL`**, it is blocked in an uninterruptible
syscall. That is a hard confirmation of the exFAT hypothesis and the process
cannot be reaped without unmounting `/Volumes/DATA_LAKE`. **Stop here and
report** — do not unmount a volume holding 6.1 TiB of the system of record
without the operator's explicit go-ahead.

> PIDs are from the 2026-07-29 10:43 HKT observation. Re-resolve them with
> `launchctl list | grep livewire` before running — do not paste stale PIDs.
> If the wedge has cleared on its own by then (it did once already, between
> 2026-07-28 evening and 07-29 morning), Phase 0 is a no-op: skip to Phase 1.

- [ ] **Step 3: Confirm launchd can fire again**

```bash
launchctl list | grep livewire
```

Expected: `com.livewire.daily-update` shows `-` in the PID column (not a live
PID). A live PID means launchd still considers the job running and will keep
skipping its schedule.

- [ ] **Step 4: Do NOT kill `com.livewire.intraday-catchup`**

At the time of writing it is genuinely working (PID 81121, 54% CPU, publishing
flat files). Killing it would discard real in-flight progress. Verify before
deciding:

```bash
ps -o pid=,etime=,time= -p $(pgrep -f flatfile-ingest | head -1)
```

A CPU time that advances over a 15 s interval means working; a frozen CPU time
means wedged. Only a frozen one is a kill candidate.

---

## Phase 1 (PR A): A wedged scheduled job must die and page

**Branch:** `fix/nightly-job-fails-loudly`
**PR title:** `fix(ops): a wedged nightly job must die and page, not stall silently`

Three mechanisms, one outcome: today the nightly job can fail without anyone
finding out. Task 1 restores the alert's ability to send; Task 2 guarantees
there is a failure to alert *about* instead of an eternal stall; Task 2b stops
a quality job from failing every night into a WARNING nobody counts. They ship
together because any one alone still leaves the job silently broken in the
other two ways. Task 3 is the end-to-end proof, not a separate change.

### Task 1: The release ships the Node alert runtime

**Files:**
- Modify: `livewire_scripts/release.py` — add `build_node_modules` after `build_venv` (ends line 127), and call it between `build_venv(staging)` (line 223) and `freeze(staging)` (line 224)
- Test: `tests/test_release.py`

**Interfaces:**
- Consumes: `_run(cmd, cwd=..., check=...)`, already in `release.py`
- Produces: `build_node_modules(dest: Path) -> None` — called between `build_venv` and `freeze`

> The call site's local variable is `staging`, not `dest`. Tests that assert on
> source text must match `freeze(staging)`.

- [ ] **Step 1: Write the failing tests**

```python
def test_release_installs_node_modules_when_package_json_exists(tmp_path):
    (tmp_path / "package.json").write_text('{"dependencies":{"nodemailer":"8.0.2"}}')
    calls = []
    with patch("livewire_scripts.release._run", side_effect=lambda cmd, **kw: calls.append((cmd, kw))):
        build_node_modules(tmp_path)
    assert ["npm", "ci", "--omit=dev"] in [c for c, _ in calls]


def test_release_without_package_json_installs_nothing(tmp_path):
    calls = []
    with patch("livewire_scripts.release._run", side_effect=lambda cmd, **kw: calls.append(cmd)):
        build_node_modules(tmp_path)
    assert calls == []


def test_node_modules_are_installed_before_the_tree_is_frozen(tmp_path):
    """`freeze` chmods the tree a-w; npm cannot write after that."""
    import inspect
    from livewire_scripts import release
    source = inspect.getsource(release)
    # The promote path's local is `staging`, not `dest` — match the call site.
    assert source.index("build_node_modules(staging)") < source.index("freeze(staging)")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_release.py -k node -v`
Expected: FAIL with `ImportError: cannot import name 'build_node_modules'`

- [ ] **Step 3: Implement**

```python
def build_node_modules(dest: Path) -> None:
    """Install the Node alert dependencies into the release.

    `git archive` exports only tracked files and `node_modules/` is gitignored,
    so every release built since the artifact cutover shipped without
    nodemailer. The failure alert is the one message a broken nightly run
    depends on, and it could not send:

        Cannot find package 'nodemailer' imported from
          <release>/livewire_node/send_daily_update_failure_email.mjs

    Must run before `freeze`, which makes the tree read-only.
    """
    if not (dest / "package.json").exists():
        return
    _run(["npm", "ci", "--omit=dev"], cwd=dest)
    # A release whose alert path cannot import is never promoted — the same
    # rule `build_venv` already applies to the Python tree.
    _run(["node", "--input-type=module", "-e", "import('nodemailer')"], cwd=dest)
```

**Decision — this fails the promote, it does not warn.** `npm ci` needs the
network, so a registry outage at 12:00 UTC now blocks promotion where it
previously succeeded. That is the correct trade: a release that cannot alert is
precisely the failure this task exists to prevent, and `promote` already keeps
serving the previous release when it refuses to build. Do **not** soften this to
`check=False`.

**Verified available on this host:** `npm` 11.12.1 and `node` v25.9.0 at
`/opt/homebrew/bin/`; `package.json` pins `nodemailer` 8.0.2 and declares
`engines.node >= 22`; `package-lock.json` is tracked, so `git archive` exports
it and `npm ci` has the lockfile it requires.

- [ ] **Step 4: Wire it into the build, before `freeze`**

`livewire_scripts/release.py:223-224`:

```python
        build_venv(staging)
        build_node_modules(staging)   # <- new; must precede freeze
        freeze(staging)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_release.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/release.py tests/test_release.py
git commit -m "fix(release): ship the Node alert runtime, not just the Python tree"
```

### Task 2: The nightly job has a total deadline, and every lane pages when it dies

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py` — `run_daily_update_attempt` (175-190), `_run_scheduled_lane` (562-584), `run_with_retries` (327-410), `main` (659+)
- Test: `tests/test_run_daily_update_job.py`

**Interfaces:**
- Consumes: `GATEWAY_DOWN_EXIT_CODE` (86), already imported at line 18
- Consumes: `TIMEOUT_EXIT_CODE` (124) — **defined only in `livewire_scripts/sync_runner.py:37` and NOT currently imported here.** Add:
  ```python
  from livewire_scripts.sync_runner import TIMEOUT_EXIT_CODE
  ```
  Verified safe: `sync_runner` does not import `run_daily_update_job`, so no cycle.
- Produces: `JobDeadline` (a monotonic total budget), `run_daily_update_attempt(..., deadline=...)`, and an alerting `_run_scheduled_lane`

#### Three defects, all confirmed against the code — the first draft of this task fixed none of them correctly

**(a) A per-attempt timeout is not a bound on the nightly run.** `main()` runs
lanes *sequentially*: corporate-actions (669), then equity/futures/cmdty
(674), then CBOE, FX, and Silver (683-701). Seven lanes. A 3 h per-attempt
budget therefore permits a **21 h** job — it would not have prevented the
2026-07-28 stall from blowing past the 10:30 UTC watchdog. The job starts at
06:00 UTC and the watchdog checks at 10:30 UTC, so the real requirement is a
**4 h total deadline**, not a per-lane one.

**Measured basis — whole-job wall clock, not per-lane.** An earlier draft justified
4 h as "2× headroom over a ~2 h job", extrapolating from the heaviest single
lane (1.87 h). That was wrong. Measuring first-to-last timestamp across all 28
`daily_update_2026-07-*.log` files gives:

| | |
|---|---|
| Healthy runs | 0.08 – **3.27 h** (max 07-25; 07-26 3.22 h, 07-27 2.94 h) |
| Anomalous runs | 4.96 h (07-22), 8.10 h (07-19), 10.32 h (07-23), 19.44 h (07-28) |
| Watchdog ceiling | **4.5 h** — job starts 06:00 UTC, watchdog checks 10:30 UTC |

So the deadline must sit in the narrow band **(3.27 h, 4.5 h)**. 4 h gives
roughly **22% headroom over the worst healthy run**, not 2×. That is tight, and
it is the honest number: a legitimately slow night near 3.3 h has little slack.

**Known consequence, accepted deliberately:** a 4 h deadline would have killed
the 07-22 run at 4.96 h. Whether that run was healthy-but-slow or an early
instance of the same wedge is **unknown** — it is not classified anywhere. If
Task 5b or Phase 4 later shows that ~5 h healthy runs are normal, raise
`MDW_DAILY_JOB_DEADLINE_SECONDS` and move the watchdog with it; do not silently
keep killing real work.

One total deadline is also **strictly simpler** than a per-lane budget plus a
total: it is one number, one mechanism, and it cannot be satisfied lane-by-lane
while the job as a whole overruns.

**(b) The timeout path must not skip the alert.** `run_with_retries` sends the
only failure alert at line 409, reachable **only by falling out of the retry
loop**. Both existing early returns (`GATEWAY_DOWN_EXIT_CODE` at 371, success at
~380) bypass it deliberately. A `return TIMEOUT_EXIT_CODE` — which the first
draft of this plan proposed — would have made the timeout the one failure mode
that never pages, in a PR whose entire purpose is to make failures page. Use
`break`, not `return`.

**(c) `_run_scheduled_lane` never alerts at all.** Verified: zero occurrences of
`send_failure_alert` in lines 562-584. It logs `=== <label> Failed ... ===` and
returns the code. **Corporate-actions — the lane that actually wedged on
2026-07-28 — runs through this function**, as do CBOE, FX, and Silver. Only the
three asset-class lanes (via `run_with_retries`) can page today. That is why the
sole alert attempt last night came from the *watchdog*, a different job
entirely.

> **Alternative considered and rejected for this PR.** Centralizing all paging
> in `main()` is the cleaner end state — one boundary owns final status. It also
> changes when the three asset-class lanes alert, which is behavior this PR is
> not trying to alter. Making `_run_scheduled_lane` page the same way
> `run_with_retries` already does is symmetric, ~6 lines, and leaves the
> existing lanes untouched. Note the centralization idea in the PR body as
> follow-up.

**Design decision — the timeout kills the process *group*.**
`subprocess.run(timeout=...)` calls `process.kill()`, which signals only the
direct child. Verified: neither runner uses `start_new_session`, `setsid`, or
`killpg`. `corporate-actions --workers 4` fans out to a worker pool, so killing
the direct child **orphans every worker** — still running, still holding the
per-parquet `fcntl.flock`, still wedged. launchd would then start the next
instance into lock contention with processes it believes it killed.

> `livewire_scripts/sync_runner.py:133-155` has the identical latent defect.
> Out of scope here, but if the implementer shares one helper between both
> modules that is a single coherent change and beats duplicating it. Decide
> once; state the decision in the PR body.

- [ ] **Step 1: Fix the test module's imports first**

`tests/test_run_daily_update_job.py` opens a `from
livewire_scripts.run_daily_update_job import (` list at **line 16**. None of
`TIMEOUT_EXIT_CODE`, `JobDeadline`, `_run_scheduled_lane`, or
`_run_in_own_process_group` is in it, so the tests below would fail at
**import** with `ImportError`, not with the assertion they are meant to
demonstrate.

Two options; pick one and be consistent:

- Add the names to the explicit list at line 16, or
- Reach them through the module alias the file already binds at line 15
  (`from livewire_scripts import run_daily_update_job as daily_runner`), e.g.
  `daily_runner.JobDeadline`. This needs no import edit at all and is the
  smaller diff.

Do this **before** writing any test in this task, or every "Expected: FAIL
with …" line below is wrong.

- [ ] **Step 2: Write the failing tests**

```python
def test_the_total_deadline_bounds_the_whole_job_not_one_lane(monkeypatch):
    """7 lanes x a 3h per-lane budget was a 21h job. The watchdog checks at
    +4.5h, so the bound has to be on the total."""
    monkeypatch.delenv("MDW_DAILY_JOB_DEADLINE_SECONDS", raising=False)
    deadline = JobDeadline.start()
    # Must clear the worst HEALTHY whole-job run (3.27h, 2026-07-25) and stay
    # under the watchdog window (06:00 -> 10:30 UTC = 4.5h).
    assert 3.27 * 3600 < deadline.total_seconds < 4.5 * 3600


def test_remaining_shrinks_as_the_job_runs():
    clock = iter([1000.0, 1000.0, 4600.0])
    deadline = JobDeadline.start(total_seconds=7200, clock=lambda: next(clock))
    assert deadline.remaining() == 7200 - 3600


def test_a_lane_started_past_the_deadline_is_not_run_at_all(tmp_path):
    """Handing subprocess a zero or negative timeout is a crash, not a skip."""
    deadline = JobDeadline.start(total_seconds=0, clock=lambda: 0.0)
    result = run_daily_update_attempt(["x"], tmp_path / "j.log", deadline=deadline)
    assert result.returncode == TIMEOUT_EXIT_CODE


def test_a_hung_attempt_is_killed_and_reported_as_timeout(tmp_path):
    log_file = tmp_path / "job.log"

    def hang(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="daily", timeout=kwargs["timeout"])

    result = run_daily_update_attempt(["x"], log_file, runner=hang, timeout=10)
    assert result.returncode == TIMEOUT_EXIT_CODE
    assert "process group killed" in log_file.read_text()


def test_a_timeout_pages_instead_of_returning_early(tmp_path):
    """send_failure_alert sits at the END of run_with_retries; an early return
    would make the timeout the one failure that never alerts."""
    sent = []

    def hang(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="daily", timeout=kwargs["timeout"])

    config = _config(tmp_path, max_attempts=3)
    with patch(
        "livewire_scripts.run_daily_update_job.send_failure_alert",
        side_effect=lambda *a, **k: sent.append(1) or subprocess.CompletedProcess(["alert"], 0),
    ):
        code = run_with_retries(config, ["--asset-class", "equity"], runner=hang, sleep_fn=lambda _: None)
    assert code == TIMEOUT_EXIT_CODE
    assert sent == [1]          # paged exactly once


def test_a_timeout_is_never_retried(tmp_path):
    """A wedge is not transient, and 3 attempts would blow the total deadline."""
    attempts = []

    def hang(*args, **kwargs):
        attempts.append(1)
        raise subprocess.TimeoutExpired(cmd="daily", timeout=kwargs["timeout"])

    config = _config(tmp_path, max_attempts=3)
    with patch("livewire_scripts.run_daily_update_job.send_failure_alert", return_value=None):
        run_with_retries(config, ["--asset-class", "equity"], runner=hang, sleep_fn=lambda _: None)
    assert len(attempts) == 1


def test_a_scheduled_lane_failure_pages(tmp_path):
    """corporate-actions/CBOE/FX/Silver run through _run_scheduled_lane, which
    had no alert path at all — which is why the 2026-07-28 corporate-actions
    wedge produced no alert from this job."""
    sent = []
    config = _config(tmp_path)
    runner = MagicMock(return_value=subprocess.CompletedProcess(["x"], 1))
    with patch(
        "livewire_scripts.run_daily_update_job.send_failure_alert",
        side_effect=lambda *a, **k: sent.append(1) or subprocess.CompletedProcess(["alert"], 0),
    ):
        code = _run_scheduled_lane(
            config, ["x"], "Corporate Action Sync", "corporate-actions",
            env=None, runner=runner, now_fn=_utc_now,
        )
    assert code == 1
    assert sent == [1]


def test_a_scheduled_lane_that_succeeds_does_not_page(tmp_path):
    sent = []
    config = _config(tmp_path)
    runner = MagicMock(return_value=subprocess.CompletedProcess(["x"], 0))
    with patch(
        "livewire_scripts.run_daily_update_job.send_failure_alert",
        side_effect=lambda *a, **k: sent.append(1),
    ):
        _run_scheduled_lane(
            config, ["x"], "FX Sync", "fx",
            env=None, runner=runner, now_fn=_utc_now,
        )
    assert sent == []


def test_a_gateway_down_lane_does_not_page(tmp_path):
    """Degraded is not failed — an unreachable Gateway must stay silent."""
    sent = []
    config = _config(tmp_path)
    runner = MagicMock(return_value=subprocess.CompletedProcess(["x"], GATEWAY_DOWN_EXIT_CODE))
    with patch(
        "livewire_scripts.run_daily_update_job.send_failure_alert",
        side_effect=lambda *a, **k: sent.append(1),
    ):
        _run_scheduled_lane(
            config, ["x"], "Corporate Action Sync", "corporate-actions",
            env=None, runner=runner, now_fn=_utc_now,
        )
    assert sent == []
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/test_run_daily_update_job.py -k "deadline or timeout or scheduled_lane" -v`
Expected: FAIL on assertions (not on `ImportError` — Step 1 already fixed that).

- [ ] **Step 4: Implement the total deadline**

```python
@dataclass(frozen=True)
class JobDeadline:
    """A monotonic wall-clock budget for the WHOLE scheduled job.

    `main()` runs seven lanes sequentially. A per-lane budget of N hours
    permits a 7N-hour job, which is how the 2026-07-28 run blew past the
    10:30 UTC watchdog while every individual lane looked bounded. The job
    starts at 06:00 UTC and the watchdog checks at 10:30 UTC; 4h leaves 2x
    headroom over the ~2h a healthy whole run takes (heaviest single lane
    measured 1.87h on 2026-07-25) and still lands inside that window.
    """

    total_seconds: float
    started_at: float
    clock: callable = time.monotonic

    @classmethod
    def start(cls, total_seconds: float | None = None, clock: callable = time.monotonic) -> JobDeadline:
        budget = float(os.getenv("MDW_DAILY_JOB_DEADLINE_SECONDS", str(4 * 60 * 60))) \
            if total_seconds is None else float(total_seconds)
        return cls(total_seconds=budget, started_at=clock(), clock=clock)

    def remaining(self) -> float:
        return self.total_seconds - (self.clock() - self.started_at)
```

`time` and `os` are already imported (lines 12 and 6); `dataclass` at line 14.

- [ ] **Step 5: Spend the deadline in every attempt, and kill the group**

```python
def _run_in_own_process_group(command, *, stdout, env, timeout):
    """Run `command` in its own session; kill the whole group on timeout.

    `subprocess.run(timeout=...)` signals only the direct child, so a lane that
    fans out (`corporate-actions --workers 4`) would leave every worker
    orphaned, still holding the per-parquet flock and still wedged.
    """
    with subprocess.Popen(
        list(command), stdout=stdout, stderr=subprocess.STDOUT,
        text=True, env=env, start_new_session=True,
    ) as proc:
        try:
            proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.communicate()
            raise
        return subprocess.CompletedProcess(list(command), proc.returncode)


def run_daily_update_attempt(
    command: Sequence[str],
    log_file: Path,
    env: dict[str, str] | None = None,
    runner: callable = _run_in_own_process_group,
    timeout: float | None = None,
    deadline: JobDeadline | None = None,
) -> subprocess.CompletedProcess:
    budget = timeout if timeout is not None else (deadline.remaining() if deadline else None)
    if budget is not None and budget <= 0:
        # A non-positive timeout is a crash, not a skip. The job is already
        # over its total budget; do not start another lane.
        append_log(log_file, "=== Deadline exhausted before this lane started ===")
        return subprocess.CompletedProcess(list(command), TIMEOUT_EXIT_CODE)
    with log_file.open("a", encoding="utf-8") as handle:
        try:
            return runner(list(command), stdout=handle, env=env, timeout=budget)
        except subprocess.TimeoutExpired:
            append_log(log_file, f"=== Timed out after {budget:.0f}s (process group killed) ===")
            return subprocess.CompletedProcess(list(command), TIMEOUT_EXIT_CODE)
```

Add `import signal` to the imports.

> **Breaking change for existing tests.** The default `runner` changes from
> `subprocess.run` to `_run_in_own_process_group`, whose signature drops
> `check=` / `text=` / `stderr=`. `tests/test_run_daily_update_job.py` injects
> `runner=` in many places (see the `attempt 1/3` and `attempt 2/2` assertions
> near lines 501 and 541). Every injected fake must accept
> `(command, *, stdout, env, timeout)`. Update them in this task, not later.

**SIGKILL safety.** Bronze publication is `temp -> validate -> os.replace()`, so
a killed writer leaves a temp file, never a torn canonical parquet. `fcntl.flock`
is released by the kernel when the fd closes on process death, so killing the
group releases every lock it held. That is why killing the group is safe *and*
why leaving orphans alive is not.

- [ ] **Step 6: Make the timeout break to the alert, never return past it**

In `run_with_retries`, immediately after the existing `GATEWAY_DOWN_EXIT_CODE`
branch (line 362-371):

```python
        if result.returncode == TIMEOUT_EXIT_CODE:
            # `break`, NOT `return`: send_failure_alert is at the end of this
            # function (line 409) and is reachable only by leaving the loop.
            # A wedge is also not transient — retrying spends the remaining
            # total deadline for nothing.
            append_log(
                log_file,
                f"=== Timed out {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} (no retry) ===",
            )
            final_exit_code = TIMEOUT_EXIT_CODE
            break
```

- [ ] **Step 7: Give `_run_scheduled_lane` the alert it never had**

```python
def _run_scheduled_lane(config, command, label, done_scope, *, env, runner, now_fn, deadline=None) -> int:
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    append_log(log_file, f"=== {label} {started_at:%Y-%m-%dT%H:%M:%SZ} ===")
    append_log(log_file, f"Command: {' '.join(command)}")
    result = run_daily_update_attempt(command, log_file, env=env, runner=runner, deadline=deadline)
    if result.returncode == 0:
        append_log(log_file, f"=== Done {done_scope} {now_fn():%Y-%m-%dT%H:%M:%SZ} ===")
        return result.returncode

    append_log(
        log_file,
        f"=== {label} Failed {now_fn():%Y-%m-%dT%H:%M:%SZ} (exit_code={result.returncode}) ===",
    )
    # This function had no alert path at all, so a corporate-actions, CBOE, FX
    # or Silver failure was visible only in a log nobody reads. A down Gateway
    # stays silent: degraded is not failed.
    if result.returncode != GATEWAY_DOWN_EXIT_CODE:
        _page_lane_failure(config, label, log_file, result.returncode, env=env)
    return result.returncode
```

`_page_lane_failure` builds an `AlertRequest` the same way `run_with_retries`
does at lines 400-407 (`run_date` from the log stem, `error_summary` from
`extract_error_summary(log_file)`, `repo_root=REPO_ROOT`) and calls
`send_failure_alert`, recording an undelivered alert on non-zero exactly as
lines 418-427 already do. Factor those ~10 lines out of `run_with_retries` and
call the same helper from both, rather than writing them twice.

- [ ] **Step 8: Thread one deadline through `main()`**

```python
def main(argv=None) -> int:
    config = build_config()
    args = list(argv or sys.argv[1:])
    env = os.environ.copy()
    deadline = JobDeadline.start()   # one budget for the whole job
    ...
```

Pass `deadline=deadline` into `run_with_retries` and every
`run_*_sync` / `run_silver_rebuild` call so each lane spends from the same
budget. The single-lane `--asset-class` path (line 666) gets its own fresh
`JobDeadline.start()`.

- [ ] **Step 9: Run the tests, then the full suite**

Run: `uv run pytest tests/test_run_daily_update_job.py -v`
then `uv run pytest tests/ -v -W error::RuntimeWarning --cov=clients --cov=scripts --cov-fail-under=95`
Expected: PASS, coverage >= 95%

> The two time-bomb integration tests hang the suite (real Nasdaq/Stooq
> fallback against stale dates). Deselect them if the run stalls.

- [ ] **Step 10: Document the env var in CLAUDE.md**

Next to `MDW_SYNC_PHASE_TIMEOUT_SECONDS` in the reliability list:

```markdown
- `MDW_DAILY_JOB_DEADLINE_SECONDS` (default `14400`, 4h): total wall-clock
  budget for one `run-daily-job` run, shared across every lane. There was no
  timeout on this path at all, so a wedged child blocked launchd indefinitely.
  It is deliberately a *total*, not per-lane: `main()` runs seven lanes
  sequentially, so a per-lane budget of N hours permits a 7N-hour job. A lane
  that exhausts the budget is killed by process group, is never retried, and
  pages.
```

- [ ] **Step 11: Commit**

```bash
git add livewire_scripts/run_daily_update_job.py tests/test_run_daily_update_job.py CLAUDE.md
git commit -m "fix(ops): bound the whole nightly job and page when a lane dies"
```

### Task 2b: A post-success quality job that fails every night must be visible

**Files:**
- Modify: `livewire_scripts/run_daily_update_job.py:259-274` (`_spawn_post_success_quality`)
- Test: `tests/test_run_daily_update_job.py`

**The defect, measured — not the one this plan originally claimed.** The plan's
first draft said coverage stopped because it "only fires after a successful
daily job". That is **false**: `run_post_success_quality` is called
unconditionally at the end of `main()` (`run_daily_update_job.py:~700`). The
real cause is in the logs:

```
WARNING: coverage report failed: Command '[... 'livewire_quality.py', 'coverage']'
timed out after 600 seconds
```

This appears on 2026-07-07, -08, -09, -10, -13, -14, -16, -17, -20, -21, -22,
-23, -27 — **every night the job ran**, for three weeks. `nightly digest failed:
exit_code=1` appears alongside it on -16 through -21.

`_spawn_post_success_quality` catches everything and writes a WARNING, by
design: *"These jobs must never flip a successful daily run to failure."* That
design is right. What is missing is that nothing counts the WARNINGs, so a
permanently broken coverage job is indistinguishable from a healthy one.

**Do not "fix" this by raising the 600 s budget.** The budget is not known to be
wrong — Task 5b measures the real runtime first. Raising a limit against an
unmeasured workload just moves the wall.

**Simplification (Pass 3b): no new marker.** The first draft of this task added
a `QUALITY_DEGRADED` log marker for the digest to grep. That is redundant —
`_spawn_post_success_quality` **already** writes
`WARNING: {label} failed: {reason}`, which carries both the job name and the
cause, and that format is what three weeks of real logs are already in. Adding a
parallel marker means two formats to keep in sync for zero new information.
**`run_daily_update_job.py` is not modified by this task at all**; the entire
change lives in the digest.

**Files:**
- Modify: `livewire_scripts/nightly_digest.py`
- Test: `tests/test_nightly_digest.py`

- [ ] **Step 1: Write the failing test**

**Verified shape:** `build_digest` (`nightly_digest.py:124`) declares
`sections: list[list[str]]` at line 127 and joins with
`"\n\n".join("\n".join(section) for section in sections)` at line 135. A section
is therefore a **`list[str]`, not a `str`** — return the wrong type and the join
silently explodes the string into one character per line. `_silver_section`
(line 77) is the model to follow. `tests/test_nightly_digest.py` already exists.

```python
def test_the_digest_reports_a_failed_quality_job(tmp_path):
    """coverage timed out at 600s every night from 2026-07-07 and the digest
    never mentioned it, so nobody found out for three weeks."""
    log_file = tmp_path / "daily_update_2026-07-27.log"
    log_file.write_text(
        "=== Done equity 2026-07-27T02:00:00Z ===\n"
        "WARNING: coverage report failed: Command '[...]' timed out after 600 seconds\n"
    )
    section = quality_jobs_section(log_file)
    assert isinstance(section, list)          # sections are list[str], see line 127
    body = "\n".join(section)
    assert "coverage report" in body
    assert "timed out after 600 seconds" in body


def test_the_digest_says_so_when_every_quality_job_passed(tmp_path):
    log_file = tmp_path / "daily_update_2026-07-27.log"
    log_file.write_text("=== Done equity 2026-07-27T02:00:00Z ===\n")
    assert "all green" in "\n".join(quality_jobs_section(log_file))


def test_one_job_failing_on_every_retry_is_reported_once(tmp_path):
    log_file = tmp_path / "daily_update_2026-07-27.log"
    log_file.write_text("WARNING: coverage report failed: boom\n" * 3)
    body = "\n".join(quality_jobs_section(log_file))
    assert body.count("coverage report") == 1


def test_a_missing_log_file_does_not_raise(tmp_path):
    """test_nightly_digest.py:87 already covers missing-input expectations;
    the new section must not change them."""
    assert quality_jobs_section(tmp_path / "nope.log") == ["Quality jobs: all green"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_nightly_digest.py -k quality_job -v`
Expected: FAIL — `quality_jobs_section` is not defined

- [ ] **Step 3: Add the section**

```python
_QUALITY_WARNING_RE = re.compile(r"^WARNING: (?P<label>.+?) failed: (?P<reason>.+)$", re.MULTILINE)


def quality_jobs_section(log_file: Path) -> list[str]:
    """Report post-success quality jobs that failed.

    Returns list[str], matching `sections: list[list[str]]` at line 127.

    `_spawn_post_success_quality` swallows these into a WARNING on purpose —
    they must never flip a successful run to failure. But nothing counted them,
    so `coverage` timed out at its 600s budget every night from 2026-07-07 to
    at least 07-27 with no visible consequence.
    """
    text = log_file.read_text(encoding="utf-8", errors="ignore") if log_file.exists() else ""
    # Dedup by label: one job failing on all three retry passes is one problem.
    seen = {m["label"]: m["reason"].strip() for m in _QUALITY_WARNING_RE.finditer(text)}
    if not seen:
        return ["Quality jobs: all green"]
    return [f"Quality jobs: {len(seen)} FAILED"] + [
        f"  {label}: {reason}" for label, reason in sorted(seen.items())
    ]
```

- [ ] **Step 4: Wire the section into the digest body**

`nightly_digest.py:127` builds `sections: list[list[str]]`. Append
`quality_jobs_section(log_file)` to that list, following `_silver_section`
(line 77) as the model. Do not invent a new assembly mechanism, and do not
return a bare string — line 135 joins each section's elements with `"\n"`, so a
`str` would be split into one character per line.

- [ ] **Step 5: Run the tests to verify they pass, then the full suite**

Run: `uv run pytest tests/ -v -W error::RuntimeWarning --cov=clients --cov=scripts --cov-fail-under=95`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/nightly_digest.py tests/test_nightly_digest.py
git commit -m "fix(ops): report post-success quality failures in the nightly digest"
```

### Task 3: Prove the alert actually sends

A test that mocks `subprocess` proves the code path; it does not prove
`nodemailer` resolves inside a frozen release. This is the check the whole PR
exists for.

- [ ] **Step 1: Promote a release from the merged SHA**

```bash
python scripts/livewire_ops.py release promote
```

- [ ] **Step 2: Prove the alert module imports inside the release**

```bash
cd "$(readlink -f ~/market-warehouse/current)" && \
  node --input-type=module -e "import('nodemailer').then(() => console.log('ok'))"
```

Expected: `ok`. A `Cannot find package` here means Task 1 did not take effect.

- [ ] **Step 3: Send one real test alert and confirm receipt**

`send-alert` has **no `--dry-run`**. Its real interface, read from
`build_alert_command` (`run_daily_update_job.py:146-166`), is:
`--run-date --log-file --error-summary --repo-root --job-name`
(plus optional `--attempts` / `--exit-code`). Sending is the only way to test it.

```bash
REL="$(readlink -f ~/market-warehouse/current)"
printf 'plan verification, not a real incident\n' > /tmp/plan-verify.log
"$REL/.venv/bin/python" "$REL/scripts/livewire_ops.py" send-alert \
  --run-date "$(date -u +%Y-%m-%d)" \
  --log-file /tmp/plan-verify.log \
  --error-summary "review-cycle verification of the restored alert path" \
  --repo-root "$REL" \
  --job-name plan_verification \
  --exit-code 0
```

This sends a real email. Expected: exit 0, and the message arrives.

- [ ] **Step 4: Confirm nothing was quarantined**

```bash
ls -la ~/market-warehouse/logs/alerts_undelivered/ 2>/dev/null | tail -5
```

An alert that fails to send is persisted there. No new file after Step 3 is the
pass condition; a new file means the send failed and Task 1 did not work.

---

## Phase 2: Probe the two unknowns (read-only, no PR, output persisted)

Neither probe changes code. Both answer a question that determines whether
later work is worth doing at all. **Persist both results** — a probe that only
prints to stdout is lost the moment the shell exits, and the Massive
entitlement floor rolls forward daily, so today's answer is unobtainable later.

### Task 4: Is pre-2021 flat-file history actually gettable? — **DONE 2026-07-29. No.**

> **RESULT.** One GET per calendar year against both prefixes, then a binary
> search to pin the boundary:
>
> ```
> day_aggs / minute_aggs   2003…2021  →  403 Forbidden   (every year)
>                          2022…2026  →  GET ok
> binary search            2021-07-27 →  403
>                          2021-07-28 →  OK
> ```
>
> **The floor is 2021-07-28 — exactly 1827 days (5.00 years) before the probe
> date.** It is a *rolling* 5-year entitlement, identical to the documented
> `/v2/aggs` REST floor, and it applies to both prefixes.
>
> This is the exact trap CLAUDE.md already records for FX: *"Massive's S3
> `global_forex/` prefix lists back to 2010 but GETs 403. Probe permission
> boundaries with GET, never with LIST."* The same was true here and had never
> been tested for `us_stocks_sip`.
>
> **Consequences, both directions:**
>
> - **There is no deep history to fetch.** The "1285 of 5755 days = 22%"
>   framing was wrong: the real denominator is the entitled window, and the
>   warehouse already holds **all of it**. `raw_completed` starts 2021-06-11 —
>   *earlier* than today's floor, because those files were downloaded when the
>   window reached further back. History is accumulated, not re-fetchable.
> - **Never delete raw partitions to reclaim space.** Anything older than the
>   current floor cannot be re-downloaded, ever. This now has the same status as
>   the triage verdict store.
> - **Task 7 is cancelled** (see Phase 3). Task 6 remains correct — the guard
>   should never have measured the wrong volume — but it unblocks nothing.
>
> Re-measure before trusting any of this: the floor rolls forward one day per day.
> Raw result: `~/market-warehouse/logs/probes/2026-07-29-flatfile-get-floor.json`.

<details><summary>Original probe procedure (kept for the next re-measure)</summary>

`discovery.earliest` says `2003-09-10`, but that comes from **LIST**. CLAUDE.md
already records the matching trap for FX: *"Massive's S3 `global_forex/` prefix
lists back to 2010 but GETs 403. Probe permission boundaries with GET, never
with LIST."* The same claim for `day_aggs`/`minute_aggs` has never been tested.

- [ ] **Step 1: GET one object per candidate year, both prefixes**

API verified against `clients/massive_flatfile_client.py`: the GET is
`download_date_to_path(d: date, destination: Path) -> Path`; `list_objects()`
returns dicts with a `"Key"`; the day_aggs prefix constant is
`S3_PREFIX_DAILY = "us_stocks_sip/day_aggs_v1"` (line 27). `date_from_key` is
already exported by `livewire_scripts/flatfile_planner.py`.

```bash
STAMP=$(date -u +%Y-%m-%d)
OUT=~/market-warehouse/logs/probes/$STAMP-flatfile-get-floor.json
mkdir -p "$(dirname "$OUT")"
cd "$(readlink -f ~/market-warehouse/current)"
./.venv/bin/python - "$OUT" <<'PY'
import json, sys, tempfile
from datetime import date
from pathlib import Path

from clients.massive_flatfile_client import S3_PREFIX_DAILY, MassiveFlatfileClient
from livewire_scripts.flatfile_planner import date_from_key

results = {}
with tempfile.TemporaryDirectory() as tmp:
    for label, kwargs in (("minute_aggs", {}), ("day_aggs", {"prefix": S3_PREFIX_DAILY})):
        with MassiveFlatfileClient(**kwargs) as client:
            # One candidate date per calendar year: the earliest object that year.
            first_of_year: dict[int, date] = {}
            for obj in client.list_objects():
                if not obj["Key"].endswith(".csv.gz"):
                    continue
                d = date_from_key(obj["Key"])
                if d.year not in first_of_year or d < first_of_year[d.year]:
                    first_of_year[d.year] = d
            for year, d in sorted(first_of_year.items()):
                dest = Path(tmp) / f"{label}-{d}.csv.gz"
                try:
                    client.download_date_to_path(d, dest)
                    results[f"{label}/{year}"] = f"GET ok ({d}, {dest.stat().st_size} bytes)"
                except Exception as exc:  # record the real status, never guess it
                    results[f"{label}/{year}"] = f"{type(exc).__name__}: {exc}"[:300]
                dest.unlink(missing_ok=True)

payload = {
    "as_of": str(date.today()),
    "probe": "Massive flat-file GET entitlement, one date per year, both prefixes",
    "results": results,
}
Path(sys.argv[1]).write_text(json.dumps(payload, indent=2))
print(json.dumps(payload, indent=2))
PY
```

> Downloads one file per year per prefix. A 2003 minute file is small relative
> to the 78.94 GiB whole-corpus figure, but check `du -sh` on the temp dir if
> the run seems slow — the loop deletes each file immediately after measuring.

- [ ] **Step 2: Record the verdict in CLAUDE.md**

CLAUDE.md currently states the day_aggs universe reaches "back to 2003". If the
probe shows a GET floor, that sentence is false and must be corrected to name
the measured floor and its as-of date, in the same style as the FX section.

- [x] **Step 3: Decide whether Phase 3 is worth doing**

- ~~GET reaches 2003 → Phase 3 unblocks ~18 years of history. Do it.~~
- **GET floors at 2021-07-28 → the capacity guard is still a real bug worth
  fixing, but there is no deeper history to fetch and the backfill program ends
  here.** Said plainly rather than building for history that cannot be had.

</details>

### Task 5: Why do most symbols sit at 2026-07-24 while SPY has 07-27?

The `day_aggs` catch-up that finished 2026-07-29 10:18 reported
`published_tickers=10536 rows=10009`, yet at 10:50 AAPL/NVDA/INTC/AGL `1d` all
ended 2026-07-24 while SPY ended 2026-07-27. One publish, inconsistent coverage.

**Update, measured 2026-07-29 ~11:20:** AAPL now ends **2026-07-28** with 11,496
rows (was 11,494 / 07-24 half an hour earlier). So the gap was at least partly a
**publish still in flight**, not a permanent hole — the sample was taken while
`flatfile-ingest` was mid-run. This weakens the "inconsistent publish" reading
considerably.

The probe is still worth running, but its question has changed: **not** "why is
coverage inconsistent" but "**is there any residual hole once the run settles**".
Run it after the current `intraday-catchup` finishes, not during. Root cause of
any remaining spread is unknown — **do not fix anything until it is known.**

- [ ] **Step 1: Establish the true scope**

```bash
~/market-warehouse/current/.venv/bin/python - <<'PY'
import collections, glob, os
import pyarrow.parquet as pq
root = os.path.expanduser("~/market-warehouse/data-lake/bronze/asset_class=equity")
tail = collections.Counter()
for i, p in enumerate(sorted(glob.glob(f"{root}/symbol=*/1d.parquet"))):
    try:
        # `pq.read_table` on a path under `symbol=<ticker>/` can trigger Hive
        # partition inference and raise a schema conflict that is an artifact
        # of the reader, not of the file. ParquetFile reads the file alone.
        col = pq.ParquetFile(p).read(columns=["trade_date"]).column("trade_date")
        tail[str(col[-1])] += 1
    except Exception as exc:
        # A false ERROR here would corrupt the distribution that chooses the
        # next investigation, so record the type rather than swallowing it.
        tail[f"ERROR {type(exc).__name__}"] += 1
for d, n in sorted(tail.items())[-12:]:
    print(f"{d}  {n:>6,}")
PY
```

This is a full scan of ~13K files on the exFAT volume; expect minutes, not
seconds. Run it after Phase 0, never against a live publish. **Wrap it in a
hard timeout** — it reads the volume whose wedge is still undiagnosed, and an
unbounded scan is exactly the shape that hangs:

```bash
# macOS has no coreutils `timeout` by default; use perl's alarm, always present.
perl -e 'alarm 1800; exec @ARGV' -- ~/market-warehouse/current/.venv/bin/python /tmp/tail_scan.py
```

Write the snippet above to `/tmp/tail_scan.py` first rather than piping a
heredoc through `perl -e`, which would swallow the script on stdin.

- [ ] **Step 2: Persist the distribution and only then diagnose**

Write the counter to `~/market-warehouse/logs/probes/<date>-1d-tail-distribution.json`.
A bimodal 07-24/07-27 split points at the publish path; a long tail of many
distinct dates points at per-symbol gaps instead. The shape chooses the next
step — do not pre-commit to a fix.

---

### Task 5b: How long does `coverage` actually take?

Task 2b makes the failure visible; it does not say whether 600 s is the wrong
budget or whether coverage is broken. Measure before choosing.

- [ ] **Step 1: Time one real run, uncapped, and persist the result**

```bash
STAMP=$(date -u +%Y-%m-%d)
REL="$(readlink -f ~/market-warehouse/current)"
/usr/bin/time -l "$REL/.venv/bin/python" "$REL/scripts/livewire_quality.py" coverage --no-recover \
  > ~/market-warehouse/logs/probes/$STAMP-coverage-runtime.txt 2>&1
tail -20 ~/market-warehouse/logs/probes/$STAMP-coverage-runtime.txt
```

`--no-recover` keeps this read-only: the recovery subprocess is the expensive,
mutating part and is not what we are timing. Run it **after Phase 0**, so it is
not competing with a wedged process for the same volume.

- [ ] **Step 2: Choose the outcome from the measurement, not in advance**

- Completes in well under 600 s → the nightly failures have another cause
  (contention with the concurrent lanes, or the exFAT wedge). Do **not** touch
  the budget; record the finding and revisit after Phase 4.
- Completes but takes longer than 600 s → raise the budget to ~1.5× the
  measured time, in the same evidence-based style as Task 2's 3 h default.
- Does not complete → coverage itself is the defect. Scope it separately; it is
  out of this plan.

---

## Phase 3 (PR B): The capacity guard measures the volume the bytes land on

**Branch:** `fix/flatfile-capacity-measures-data-lake`
**PR title:** `fix(flatfile): measure free space on the volume the data lands on`

**Gated on Task 4.** If GET floors at 2021 this PR is still correct — the guard
should never have measured the wrong volume — but it unblocks nothing, and the
PR body must say so instead of implying a backfill will follow.

### Task 6: Point `discover_plan` at the data-lake volume

**Files:**
- Modify: `livewire_scripts/flatfile_planner.py:39-53`
- Modify: `livewire_scripts/ingest_flatfiles.py:66`
- Modify: `livewire_scripts/ingest_daily_flatfiles.py:95`
- Test: `tests/test_flatfile_planner.py`

**Interfaces:**
- Consumes: `MassiveFlatfileStore.raw_root` and `MassiveDailyFlatfileStore.raw_root` — both already constructed at each call site, one line above `discover_plan`
- Produces: `discover_plan(client, storage_dir: Path)` — parameter renamed from `warehouse_dir`; both call sites pass `store.raw_root`

**Why `store.raw_root` and not `data_lake_dir()`.** Both stores hardcode
`warehouse_dir / "data-lake" / "raw" / ...` (`massive_flatfile_store.py:38`,
`massive_daily_flatfile_store.py:39`) and **never consult `MDW_DATA_LAKE`**. So
with `MDW_DATA_LAKE` pointed anywhere else, `data_lake_dir()` and the actual
write location diverge and the guard measures the wrong volume again — the same
bug in a new place. `store.raw_root` is the write path *by construction* and
cannot drift from it.

> **The deeper inconsistency is real and is deliberately NOT fixed here.**
> `livewire_scripts/paths.py:14` supports `MDW_DATA_LAKE`, and both stores
> ignore it. On a host that sets that variable without a matching
> `warehouse/data-lake` symlink, ingestion writes somewhere other than the
> configured lake — a genuine latent bug, wider than capacity planning.
> Removing it means changing both store constructors to take the resolved
> data-lake directory and updating every caller. That is a **write-redirection
> risk** on the system of record and belongs in its own PR with its own
> compatibility tests, not bolted onto a one-line capacity fix. `store.raw_root`
> is correct under every current configuration and stays correct after that
> future refactor, because it always names wherever the store decided to write.
> **Open the follow-up issue as part of this PR so it is not lost.**

- [ ] **Step 1: Write the failing tests**

```python
def test_capacity_is_measured_on_the_directory_that_receives_the_bytes(tmp_path):
    """The warehouse root and the raw root are different volumes here:
    ~/market-warehouse is APFS internal (24 GiB free) while
    ~/market-warehouse/data-lake is /Volumes/DATA_LAKE (6.6 TiB free).
    Measuring the root refused a backfill the real volume had room for.
    """
    client = MagicMock()
    client.list_objects.return_value = [
        {"Key": "us_stocks_sip/minute_aggs_v1/2026/06/2026-06-05.csv.gz", "Size": 10},
    ]
    raw_root = tmp_path / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1"
    raw_root.mkdir(parents=True)
    seen = []
    with patch(
        "livewire_scripts.flatfile_planner.shutil.disk_usage",
        side_effect=lambda p: seen.append(Path(p)) or MagicMock(free=1000),
    ):
        discover_plan(client, raw_root)
    assert seen == [raw_root]


def test_a_raw_root_that_does_not_exist_yet_measures_its_nearest_real_ancestor(tmp_path):
    """On a fresh install neither the raw root nor its parents exist. The old
    single-level `.parent` fallback would still hand disk_usage a missing path.
    """
    client = MagicMock()
    client.list_objects.return_value = [
        {"Key": "us_stocks_sip/minute_aggs_v1/2026/06/2026-06-05.csv.gz", "Size": 10},
    ]
    missing = tmp_path / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1"
    seen = []
    with patch(
        "livewire_scripts.flatfile_planner.shutil.disk_usage",
        side_effect=lambda p: seen.append(Path(p)) or MagicMock(free=1000),
    ):
        discover_plan(client, missing)
    assert seen == [tmp_path]


def test_both_ingest_entrypoints_measure_their_own_store_root():
    """A caller passing the warehouse root reintroduces the false reading."""
    import inspect
    from livewire_scripts import ingest_daily_flatfiles, ingest_flatfiles
    for module in (ingest_flatfiles, ingest_daily_flatfiles):
        assert "discover_plan(client, store.raw_root)" in inspect.getsource(module)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_flatfile_planner.py -v`
Expected: FAIL — the second test fails on `discover_plan(client, warehouse)`

- [ ] **Step 3: Rename the parameter and walk to a real ancestor**

```python
def _existing_ancestor(path: Path) -> Path:
    """The nearest existing directory at or above `path`.

    `shutil.disk_usage` raises on a missing path, and on a fresh install
    neither the raw root nor several of its parents exist yet. The previous
    single-level `.parent` fallback did not go far enough.
    """
    for candidate in (path, *path.parents):
        if candidate.exists():
            return candidate
    return Path(path.anchor or "/")


def discover_plan(client: MassiveFlatfileClient, storage_dir: Path) -> FlatfilePlan:
    """Plan a flat-file range against the volume that will actually hold it.

    This measured the warehouse root, which on this host is APFS internal with
    ~24 GiB free, while every downloaded byte lands under `data-lake/` on a
    13 TiB exFAT volume with 6.6 TiB free. With MDW_FLATFILE_MIN_FREE_GB=25
    the guard refused every full backfill on a reading from the wrong disk.

    Callers pass their store's `raw_root` rather than a path derived from
    `MDW_DATA_LAKE`: the stores hardcode `warehouse_dir / "data-lake" / ...`
    and never read that variable, so anything else can drift from where the
    bytes truly land.
    """
    objects = client.list_objects()
    dated = sorted((date_from_key(obj["Key"]), int(obj["Size"])) for obj in objects if obj["Key"].endswith(".csv.gz"))
    if not dated:
        raise RuntimeError("Massive minute flat-file listing returned no objects")
    usage = shutil.disk_usage(_existing_ancestor(storage_dir))
    ...
```

- [ ] **Step 4: Update both call sites**

Both already construct their store one line above the `discover_plan` call, so
no new import is needed.

`livewire_scripts/ingest_flatfiles.py:66` (store built at line 63) and
`livewire_scripts/ingest_daily_flatfiles.py:95` (store built at line 89):

```python
plan = discover_plan(client, store.raw_root)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_flatfile_planner.py tests/ -v --cov=clients --cov=scripts --cov-fail-under=95`
Expected: PASS

- [ ] **Step 6: Verify against the real host, read-only**

```bash
~/market-warehouse/current/.venv/bin/python scripts/livewire_ingest.py flatfile-ingest discover
```

Expected: the logged `free` figure is now the TiB-scale number from
`df -h ~/market-warehouse/data-lake`, not ~24 GiB.

- [ ] **Step 7: Commit, PR, green CI, merge**

```bash
git add livewire_scripts/flatfile_planner.py livewire_scripts/ingest_flatfiles.py \
        livewire_scripts/ingest_daily_flatfiles.py tests/test_flatfile_planner.py
git commit -m "fix(flatfile): measure free space on the volume the data lands on"
git push -u origin fix/flatfile-capacity-measures-data-lake
gh pr create --title "fix(flatfile): measure free space on the volume the data lands on" --body "..."
```

### Task 7: Run the deep backfill — **CANCELLED 2026-07-29**

Task 4 measured the provider GET floor at **2021-07-28**, a rolling 5-year
window. Everything older is `403 Forbidden`, for both `day_aggs` and
`minute_aggs`. **There is no deep history to run a backfill against**, so this
task has nothing to do. The warehouse already holds the entire entitled window
(`raw_completed` starts 2021-06-11, *earlier* than today's floor, because those
files were fetched when the window reached further back).

Two things this changes permanently:

- **Never delete raw partitions to reclaim space.** Anything older than the
  current floor cannot be re-downloaded, ever. Same standing as the triage
  verdict store: a file obtained today may be unobtainable next year.
- **`flatfile-ingest backfill` is not a deep-history tool.** It re-fetches
  inside the rolling window only. If a future entitlement upgrade moves the
  floor, re-run Task 4's probe first and revive this task from git history.

---

## Phase 4: The undiagnosed wedge (investigation, no fix yet)

Two processes hung for 5–10 h at ~0.2 s CPU, in two different syscalls
(`os_scandir`, `open`), in two different lanes, both on `/Volumes/DATA_LAKE`,
while fresh processes read the same volume in 0.04 s. **Confidence that exFAT
is the cause: MEDIUM-HIGH. Root cause: unknown.** Phase 1 makes the symptom
survivable; it does not cure it.

- [ ] **Step 1: Read the Phase 0 forensics before anything else**

`~/market-warehouse/logs/wedge-forensics/<stamp>/` holds the only samples of
the live wedge that will exist.

- [ ] **Step 2: Check the volume for damage — read-only, never `-y`**

Verified 2026-07-29 — `diskutil info /Volumes/DATA_LAKE` reports Device Node
`/dev/disk7s2`, File System Personality `ExFAT`, Media Read-Only `No`, Volume
Read-Only `No`. `fsck` takes the **raw** node, `rdisk7s2`. Re-confirm the
identifier before running: device numbering changes across reboots and
re-plugs.

```bash
NODE=$(diskutil info /Volumes/DATA_LAKE | awk -F': *' '/Device Node/{print $2}')
echo "will check: ${NODE/\/dev\//\/dev\/r}"
sudo fsck_exfat -n "${NODE/\/dev\//\/dev\/r}"
```

`-n` answers "no" to every repair prompt. **Never run a repairing `fsck` on the
system of record without an explicit operator decision and a backup plan.**

- [ ] **Step 3: Record the finding, then stop**

Write to `~/market-warehouse/logs/probes/<date>-exfat-wedge.md`. If `fsck`
reports a clean volume, the hypothesis is weakened and the next suspect is the
`fskit` driver — which is an OS-level issue this repo does not own. Escalating
to "replace the filesystem" is an operator decision, not a code change.

---

## Phase 5: The pre-existing backlog (unchanged by this plan)

Listed so nothing is lost, **not scheduled here**. Each needs its own plan.

| Item | Blocker | Note |
|---|---|---|
| Merge PR #71 (egress preflight) | none — CI green, `MERGEABLE` | Fixes the 07-24 outage shape only; it would **not** have prevented the 07-28 wedge, since all five provider hosts were reachable |
| Sunday `--full-reconcile` cancels Yahoo-added splits | none | **Blocks the Yahoo lane below.** Cancellation inference is not provider-scoped |
| Lane A: Yahoo 2021-06-18 interior backfill | the row above | 3,963 symbols; 595 need `raw = yahoo_close × Π split` reconstruction |
| Silver rev-17 (7 ETFs' deep history) | not published | |

---

## Self-Review

**Spec coverage.** Every problem named in the 2026-07-29 diagnosis maps to a
task: nodemailer → Task 1; no timeout and no lane alert → Task 2; uncounted
quality failures → Task 2b; alert unverified → Task 3; LIST-vs-GET entitlement →
Task 4; 07-24/07-27 inconsistency → Task 5; coverage runtime → Task 5b;
capacity guard → Task 6; deep backfill → Task 7; exFAT wedge → Phase 4; PR #71
and the Yahoo lane → Phase 5.

**Findings from review that changed the design, not just the wording:**

| # | Finding | Effect |
|---|---|---|
| 1 | `send_failure_alert` is reachable only by falling out of `run_with_retries`' loop (line 409). The first draft's `return TIMEOUT_EXIT_CODE` would have made the timeout the **one failure that never pages** | Task 2 Step 6: `break`, not `return`, with a regression test asserting exactly one page |
| 2 | `_run_scheduled_lane` (562-584) contains **zero** calls to `send_failure_alert`. Corporate-actions — the lane that actually wedged — plus CBOE, FX and Silver could never page | Task 2 Step 7: give it the alert path, silent on `GATEWAY_DOWN_EXIT_CODE` |
| 3 | `main()` runs **seven lanes sequentially**, so a 3 h *per-lane* budget permits a 21 h job and still overruns the 10:30 UTC watchdog | Task 2 redesigned around one **total** `JobDeadline` (4 h), which is also one mechanism instead of two |
| 4 | `subprocess.run(timeout=)` kills only the direct child; `--workers 4` lanes would leave orphans holding `flock` | Task 2 Step 5: `_run_in_own_process_group` + `killpg` |
| 5 | Both stores hardcode `warehouse_dir/"data-lake"` and ignore `MDW_DATA_LAKE`, so measuring `data_lake_dir()` would reintroduce the bug elsewhere | Task 6 measures `store.raw_root`; the deeper mismatch is scoped to its own PR |
| 6 | `coverage` has timed out at 600 s **every night since 2026-07-07**; the plan had wrongly called this a consequence of the wedge | Task 2b (report it) + Task 5b (measure before touching the budget) |
| 7 | The test module imports a fixed name list; the proposed tests would fail at `ImportError`, not the intended assertion | Task 2 Step 1 fixes imports before any test is written |
| 8 | `pq.read_table` under `symbol=<ticker>/` can trigger Hive partition inference and record false `ERROR`s | Task 5 uses `pq.ParquetFile(p).read(...)` |
| 9 | Phase 1 → Phase 2 was presented as forced; the probes are read-only and need only Phase 0 | Architecture section now separates hard prerequisite from preference |

**Simplifications applied.** Task 2b originally added a `QUALITY_DEGRADED` log
marker; the existing `WARNING: <label> failed: <reason>` already carries both
fields and three weeks of real logs are in that format, so the marker was cut
and `run_daily_update_job.py` is not touched by that task at all. Task 2's
per-lane budget collapsed into a single total deadline. **Kept despite being
cuttable:** `_existing_ancestor` in Task 6 — a `mkdir(parents=True)` at the call
site would be one line shorter, but it makes the read-only `discover`
subcommand create directories, and a pure helper is the safer trade.

**Placeholders.** The `gh pr create --body "..."` calls are the only ellipses
and are deliberate — the body is written from the finished diff. Task 2b Step 4
names `nightly_digest.py` without quoting its section helper, because that
module's shape must be read at implementation time; the step says so instead of
inventing an API.

**Every API the plan names was verified against the repo on 2026-07-29:**
`build_venv`/`freeze` called on `staging` at `release.py:223-224`;
`TIMEOUT_EXIT_CODE` only at `sync_runner.py:37` and **not** imported by
`run_daily_update_job` (the import is now an explicit step); `send-alert` has
no `--dry-run` (real flags taken from `build_alert_command:146-166`);
`download_date_to_path(d, destination)` is the client's GET;
`S3_PREFIX_DAILY` at `massive_flatfile_client.py:27`; both stores expose
`raw_root`; `sample -file` fails on this host.

**Type consistency.** `JobDeadline.start()/remaining()`,
`_run_in_own_process_group(command, *, stdout, env, timeout)`,
`_page_lane_failure(config, label, log_file, exit_code, *, env)`,
`build_node_modules(dest)`, `quality_jobs_section(log_file)`,
`_existing_ancestor(path)`, and `discover_plan(client, storage_dir)` are each
defined once and referenced under the same name throughout. The only
`attempt_timeout_seconds` / `QUALITY_DEGRADED` mentions left are explicit
"the first draft proposed X" notes explaining why the design changed.

**Known gaps, deliberate.** Task 5, Task 5b, and Phase 4 end in a recorded
finding rather than a fix, because none has a confirmed root cause. Writing a
fix for any of them now would be guessing.
