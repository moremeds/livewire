# Fix: IBClient.connect() transient-retry is never exercised

**Item:** L1 · Severity: low-medium · Status: proposed

## Problem

`IBClient.connect(..., max_retries: int = 1)` (`clients/ib_client.py:155-246`) has an
inner retry loop for transient failures (`while attempt < max_retries`, sleep
`min(attempt, 5)`), but with the default of 1 it executes exactly once. All three
production call sites pass only `host`/`port`:

- `livewire_scripts/daily_update.py:825`
- `livewire_scripts/fetch_ib_historical.py:796`
- `livewire_scripts/backfill_intraday.py:467`

So a single TCP hiccup/timeout at connect aborts the entire phase with
`IBConnectionError` (the clientId-escalation loop only handles error 326), wasting a
whole scheduled cycle. The module docstring advertises "retry logic for transient
connection errors" that never fires.

## Fix

**Constraint (review finding — this drives the design):** the retry loop catches
bare `except Exception` (`ib_client.py:217`), so raising the default would retry
*every* failure — including session-level states (session conflict, 2FA-pending,
market-data permission) that pass the TCP preflight but fail inside `connect()`.
CLAUDE.md explicitly forbids auto-retry for exactly those states. A blanket
`max_retries=3` default therefore contradicts a standing project rule.

Chosen approach: **leave the default at 1; pass `max_retries=3` explicitly at the
three call sites, and narrow what the inner loop retries.** Retry only transient
exception types (`asyncio.TimeoutError`, `ConnectionError`/`OSError`); any other
exception — including IB error-code paths that indicate session state — raises
immediately as today. Backoff already exists (`sleep min(attempt, 5)`); total added
worst-case delay ~3s. Update the module docstring to describe the real behavior.

This needs an owner sign-off before implementation since it touches the
no-auto-retry rule; the plan's position is that a typed transient-only retry is
compatible with the rule's intent (2FA/session states are not retried), but the
call is the owner's.

## Decision

EXECUTOR: this item changes retry behavior that touches the CLAUDE.md no-auto-retry
rule. If there is no explicit owner approval recorded below this line (e.g.
"APPROVED: typed transient-only retry, <date>"), STOP — do not implement. Do not
infer approval from the Problem/Fix sections; the recommendation is not the decision.

APPROVED: typed transient-only retry (owner approval, 2026-07-05). Default stays 1;
`max_retries=3` at the three call sites; retry only
`(asyncio.TimeoutError, ConnectionError, OSError)`. 2FA/session-state failures still
raise immediately — the CLAUDE.md no-auto-retry intent is preserved.

## Preconditions (STOP if any differ)

- `clients/ib_client.py:162` reads `max_retries: int = 1,` in `connect()`'s signature.
- `clients/ib_client.py:217` reads `except Exception as exc:`.
- `livewire_scripts/daily_update.py:825` reads `ib.connect(host=args.host, port=args.port)`.
- `livewire_scripts/fetch_ib_historical.py:796` reads `ib.connect(host=args.host, port=args.port)`.
- `livewire_scripts/backfill_intraday.py:467` reads `provider.connect(host=args.host, port=args.port)`
  (note: the variable is `provider`, bound to `IBClient()` at :465 — grep for
  `provider.connect`, not `ib.connect`, in this file).

If any line content or number differs, STOP and re-locate before editing.

## Files to change

- `clients/ib_client.py` — typed transient-exception retry in the inner loop +
  docstring
- `livewire_scripts/daily_update.py:825`, `fetch_ib_historical.py:796`,
  `backfill_intraday.py:467` — pass `max_retries=3`

## Tests

`tests/test_ib_client.py` (module is outside the coverage gate but has focused
tests — extend them):

- New: `test_transient_failure_retries_then_succeeds` — connect stub raises a
  timeout once then succeeds (with `max_retries=3`) → connected, 2 attempts, same
  clientId.
- New: `test_transient_failure_exhausts_retries_and_raises` — always-timeout stub →
  `IBConnectionError` after exactly 3 attempts.
- New: `test_non_transient_failure_does_not_retry` — a non-transient exception with
  `max_retries=3` → raises after exactly 1 attempt (this is the CLAUDE.md-rule
  regression test).
- `test_non_326_error_does_not_retry_client_ids` — unchanged (default stays 1);
  still must not escalate clientId on non-326.

## Risks / notes

- Interaction with 326 handling — the ordering is fixed, do not redesign it. The
  outer `for` loop over clientId candidates (ib_client.py:191) owns clientId
  escalation; the inner `while attempt < max_retries` loop (ib_client.py:197) owns
  transient retry for the *current* clientId; after the inner loop exhausts, the
  existing client-id-in-use check (ib_client.py:230) is what breaks to the next
  clientId. Your change is ONLY: narrow the `except Exception as exc:` at
  ib_client.py:217 to catch and retry solely
  `(asyncio.TimeoutError, ConnectionError, OSError)`, re-raising every other exception
  immediately (before the sleep). Do not touch the outer loop or the 326 branch. If
  the loop structure at :191/:197/:230 differs from this description, STOP and report.
- Sleep in tests: patch `time.sleep` to avoid 3s test latency.

## Acceptance criteria

- A connect stub that fails once with a timeout no longer fails the run.
- 326 escalation behavior byte-for-byte unchanged in its tests.

## Verification (run all; all must pass)

- Targeted, catches leaked coroutines from the connect stubs:
  `uv run pytest tests/test_ib_client.py -v -W error::RuntimeWarning`
  → all tests pass, including the 3 new ones and the unchanged
  `test_non_326_error_does_not_retry_client_ids`.
- Full suite (excludes the 2 time-bomb integration tests that hang the run):
  `uv run pytest tests/ -v -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing`
  → green, coverage gate ≥ 95%. Note: `clients/ib_client.py` is omitted from the
  coverage gate (pyproject `omit`), so new lines there won't move the number — its
  tests must still pass on their own.
- Call sites carry the new arg:
  `grep -n "max_retries=3" livewire_scripts/daily_update.py livewire_scripts/fetch_ib_historical.py livewire_scripts/backfill_intraday.py`
  → exactly 3 lines (backfill_intraday.py's is on `provider.connect(...)`).

STOP condition: if any gate fails for a reason other than the tests this plan adds,
revert and report — do not lower thresholds or deselect additional tests.
