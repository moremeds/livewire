# Improve: small config/tooling knobs bundle

**Item:** I6 (+ pyright scripts gap) · Severity: low · Status: proposed

Four independent one-file tweaks, bundled as one PR only because each is ~5 lines;
if any grows, split it out.

## 1. `MDW_COVERAGE_SAFETY_CAP` env override

`DEFAULT_SAFETY_CAP = 100` (`livewire_scripts/coverage_report.py:48`) is neither
env- nor CLI-tunable (`main()` at `:316` has no flag). Add
`int(os.getenv("MDW_COVERAGE_SAFETY_CAP", "100"))` resolved at **call time** — note
this deliberately differs from the existing `MDW_COVERAGE_ALERT_THRESHOLD`, which is
resolved at module-import time (`DEFAULT_THRESHOLD = float(os.getenv(...))`);
call-time is friendlier to tests and post-import env loading. Document
in CLAUDE.md's env-var list. Test: monkeypatched env changes the abort threshold.

## 2. Unify `node_bin` resolution

`scripts/livewire_ops.py:41-44` (`_dispatch_send_alert`) resolves node as
`os.getenv("MDW_NODE_BIN", "node")` — no absolute-path fallback — while
`run_daily_update_job.py` / `run_intraday_catchup_job.py` / `nightly_digest.py` all
use `os.getenv("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"`.
Under launchd's restricted PATH, the weak variant breaks if `MDW_NODE_BIN` is unset.
Fix: extract `resolve_node_bin()` into `livewire_scripts/scheduled_env.py` (already
the shared ops-env module), use it in all four places. Test: with `MDW_NODE_BIN`
unset and `which` failing, fallback path returned.

## 3. launchd install instructions also substitute the python path

All three `launchd/*.plist.example` files hardcode
`/Users/chenxi/market-warehouse/.venv/bin/python`; CLAUDE.md's install `sed` only
replaces `/path/to/repo`. Change the examples to a `/path/to/venv-python`
placeholder and extend the documented one-liner:
`sed -e "s|/path/to/repo|$(pwd)|g" -e "s|/path/to/venv-python|$HOME/market-warehouse/.venv/bin/python|g" …`.
No test; verify by running the sed and `plutil -lint` on the output.

## 4. Pyright coverage for `scripts/`

`pyproject.toml [tool.pyright] exclude = ["tests", "scripts"]` leaves all five CLI
entry points un-type-checked. Remove `"scripts"` from the exclude (add it to
`include`), run `uv run pyright scripts/` and count the errors. BOUND: if pyright
reports more than 15 errors, OR any error requires a behavioral/logic change to fix
(not just a type annotation, `assert`, `cast`, or `if x is None` guard), STOP and
report the full pyright output — do not attempt the fix in this bundle; split item 4
into its own plan. Only annotation/guard/`cast`-level edits are in scope here. Do not
add per-file `# pyright: ignore` or `reportX = false` suppressions.

## Preconditions (STOP if any differ)

- `livewire_scripts/coverage_report.py:48` reads `DEFAULT_SAFETY_CAP = 100`.
- `scripts/livewire_ops.py:42` reads `node_bin = os.getenv("MDW_NODE_BIN", "node")`.
- `livewire_scripts/scheduled_env.py` exists (target for `resolve_node_bin()`); if
  not, STOP — do not create a new module without confirming the shared-env home.
- `pyproject.toml:77` reads `exclude = ["tests", "scripts"]`.
- All three `launchd/*.plist.example:13` contain
  `/Users/chenxi/market-warehouse/.venv/bin/python`.

If any differ, STOP and re-locate.

## Files to change

- `livewire_scripts/coverage_report.py`, `livewire_scripts/scheduled_env.py`,
  `scripts/livewire_ops.py`, three `livewire_scripts/*_job.py`/`nightly_digest.py`
  call sites, `launchd/*.plist.example` ×3, `CLAUDE.md`, `pyproject.toml`

## Sequencing

After `dedupe-cli-dispatch` (item 4 touches the same files) and independent of
everything else.

## Verification (run all)

1. Env knob: `uv run pytest tests/test_coverage_report.py -v` (add the new
   monkeypatched-`MDW_COVERAGE_SAFETY_CAP` test) → green. Confirm resolution is
   call-time:
   `grep -n 'getenv("MDW_COVERAGE_SAFETY_CAP"' livewire_scripts/coverage_report.py`
   returns a hit INSIDE a function body, not at module top level next to :47-48.
2. node_bin unified:
   `grep -rn "MDW_NODE_BIN" scripts/livewire_ops.py livewire_scripts/*_job.py livewire_scripts/nightly_digest.py livewire_scripts/scheduled_env.py`
   → every call site routes through `resolve_node_bin()`; no bare
   `os.getenv("MDW_NODE_BIN", "node")` remains. New test in the scheduled_env test
   module (with `MDW_NODE_BIN` unset and `shutil.which` patched to return None)
   asserts the `/opt/homebrew/bin/node` fallback.
3. plist sed round-trips clean:
   `for f in launchd/*.plist.example; do sed -e "s|/path/to/repo|$(pwd)|g" -e "s|/path/to/venv-python|$HOME/market-warehouse/.venv/bin/python|g" "$f" | plutil -lint -; done`
   → every output "OK". Then `grep -rn "/Users/chenxi" launchd/` → 0 hits.
4. Pyright: `uv run pyright scripts/` → 0 errors; `grep -n '"scripts"' pyproject.toml`
   shows it under `include`, absent from `exclude`.
5. Global gate (excludes the 2 time-bomb integration tests):
   `uv run pytest tests/ -v -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing`
   → green, ≥ 95%.

STOP condition: if any gate fails for a reason other than tests this plan adds, revert
and report — do not lower thresholds or deselect additional tests.

## Acceptance criteria

- Env knob honored; node resolution identical across all four spawn sites; freshly
  sed-generated plists lint clean with no `/Users/chenxi` remnants; pyright green
  over `scripts/`.
