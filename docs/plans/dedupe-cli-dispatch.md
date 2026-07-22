# Refactor: extract the five duplicated `_dispatch_module` copies

**Item:** I3 · Severity: low (refactor) · Status: proposed

## Problem

`_dispatch_module` (importlib load, argv swap around `module.main()`,
signature-arity check) is copy-pasted across all five entry points — and the copies
have **diverged behaviorally**:

- `scripts/livewire.py:74-88` and `scripts/livewire_ingest.py:41-55` catch
  `SystemExit` and convert code 0/None to `return 0` (re-raising non-zero).
- `scripts/livewire_ops.py:29-38`, `scripts/livewire_quality.py:27-36`,
  `scripts/livewire_store.py:25-34` have **no** SystemExit handling — a dispatched
  module calling `sys.exit(0)` (argparse `--help` does) propagates the exception out
  of these three entry points instead of returning 0.

## Fix

1. New `livewire_scripts/cli_dispatch.py` with the **superset** implementation (the
   livewire.py variant: SystemExit(0/None) → 0, non-zero re-raised). Placing it in
   `livewire_scripts/` (not `scripts/`) puts it inside the coverage and pyright
   gates.
2. Replace all five copies with `from livewire_scripts.cli_dispatch import
   dispatch_module`. Net: five ~15-line blocks → one tested function + five imports.
3. This *changes* behavior for ops/quality/store: today a dispatched module calling
   `sys.exit(0)` propagates `SystemExit` out of those three entry points; after the
   change it returns 0. This is a DELIBERATE behavior change on the superset, not
   incidental. Requirements:
   - `tests/test_cli_dispatch.py` MUST assert the new behavior: SystemExit(0) and
     SystemExit(None) → return 0; SystemExit(2) re-raised.
   - Before editing, run `grep -rn "SystemExit" tests/` and confirm NO existing test
     asserts that ops/quality/store *propagate* SystemExit(0). If one does, STOP —
     the "bug fix" framing is wrong and needs owner sign-off.
   - `tests/test_livewire_entrypoints.py:246`
     (`test_ingest_preserves_nonzero_system_exit`) pins the non-zero re-raise path;
     the superset must keep that test green.

## Files to change

- New: `livewire_scripts/cli_dispatch.py`, `tests/test_cli_dispatch.py`
- `scripts/livewire.py`, `livewire_ingest.py`, `livewire_ops.py`,
  `livewire_quality.py`, `livewire_store.py`

## Tests

- `tests/test_cli_dispatch.py`: argv-aware `main(argv)` gets the list; no-arg
  `main()` called bare; argv restored after dispatch (including on exception);
  SystemExit(0)/SystemExit(None) → 0; SystemExit(2) re-raised; return value None →
  0; int passthrough.
- Existing `tests/test_livewire_cli.py` + `tests/test_livewire_entrypoints.py` are
  the regression net for all five entry points. Only `tests/test_livewire_cli.py`
  references `_dispatch_module` by name (imports from `scripts.livewire` at :12-15,
  monkeypatches `scripts.livewire._dispatch_module` at :135);
  `tests/test_livewire_entrypoints.py` drives each module's `.main()` and needs no
  change. DEFAULT: update `test_livewire_cli.py` to import/patch
  `livewire_scripts.cli_dispatch.dispatch_module`; do NOT keep aliases. If updating
  the test proves to touch more than those two sites, STOP and report before adding
  any alias.

## Risks / notes

- Zero user-facing CLI changes except the SystemExit(0) fix for three entry points.
- Do this refactor **after** `fix-watchdog-env-loading` lands to avoid churning the
  same file in two open PRs (per one-change-one-PR discipline).

## Acceptance criteria

- One implementation; `--help` through every entry point exits 0 without a
  traceback; full suite green.

## Verification (run all)

- `--help` exits 0 with no traceback on all five entry points:
  `for m in livewire livewire_ingest livewire_ops livewire_quality livewire_store; do uv run python scripts/$m.py --help >/dev/null 2>&1; echo "$m -> $?"; done`
  → every line ends `-> 0`.
- One implementation, no lingering copies:
  `grep -rn "def _dispatch_module" scripts/` → 0 hits; the only definition lives in
  `livewire_scripts/cli_dispatch.py`.
- Full suite excluding the 2 time-bomb integration tests, with coverage gate:
  `uv run pytest tests/ -v -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing`
  → green, ≥ 95%. `livewire_scripts/cli_dispatch.py` is inside the gate, so
  `tests/test_cli_dispatch.py` must cover it.

STOP condition: if any gate fails for a reason other than the test updates this plan
enumerates, revert and report — do not lower thresholds or deselect additional tests.
