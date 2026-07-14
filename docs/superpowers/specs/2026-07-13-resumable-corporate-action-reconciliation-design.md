# Resumable Corporate-Action Reconciliation Design

## Goal

Make full-equity Massive corporate-action reconciliation fast and safely
resumable so it can populate the action inventory required before a production
Silver cutover. The first production run uses four workers and must resume after
provider, process, or host interruption without repeating successful symbols.

## Constraints

- Corporate-action Parquet remains the only canonical action write path.
- A symbol is checkpointed only after its canonical reconciliation succeeds.
- Provider fetches may run concurrently; canonical reconciliation, counters,
  and cursor publication remain serialized in the main thread.
- Targeted, preset, dry-run, and full-reconcile semantics remain unchanged.
- No order or IB Gateway behavior is introduced.
- API credentials never enter logs, counters, or cursor artifacts.

## CLI

Extend `corporate-actions` with:

- `--workers N`, effective CLI default `4`, constrained to `1..16`;
- `--resume`, which reuses a compatible cursor;
- `--cursor PATH`, defaulting to
  `<data-lake-root>/cursors/corporate_actions/<scope-id>.json`.

The scope ID is derived from the canonical case-preserving ticker-set hash plus the
`full_reconcile` and `dry_run` modes. This prevents a targeted, dry-run, or
scheduled command from overwriting an interrupted full-universe cursor.
Without `--resume`, the matching scope cursor is replaced with a fresh run.
With `--resume`, a missing cursor starts a fresh resumable run, an incomplete
compatible cursor continues, and an incompatible or already-completed cursor
fails closed rather than silently mixing scopes or suppressing a fresh
reconciliation.

## Cursor Identity

The atomic JSON cursor contains:

- schema version;
- resolved data-lake root;
- New York start date and UTC start timestamp as metadata;
- SHA-256 of the sorted canonical ticker list, preserving provider-significant
  mixed case such as `BCPC` versus `BCpC`;
- ticker count;
- `full_reconcile` and `dry_run` flags;
- sorted successfully completed symbols;
- completion state and UTC completion timestamp.

An incomplete compatible cursor may resume across midnight or after a host
interruption. A completed cursor cannot be resumed: the operator must start a
fresh run without `--resume`, which prevents an old completed set from
suppressing newly announced corrections. Scope, root, or mode changes require
a distinct or explicitly supplied compatible cursor.

Cursor writes use `temporary file -> fsync -> os.replace`. Only the main thread
writes the cursor.

## Execution Model

Use a `ThreadPoolExecutor` with one long-lived fetch loop per worker and a
bounded result queue. Each worker owns one `MassiveClient` session for its
lifetime and fetches splits plus dividends for assigned symbols. Results return
to the main thread, which calls `CorporateActionStore.reconcile`, updates
aggregate counters, marks the symbol complete, and atomically advances the
cursor. All worker-owned clients close when their worker exits.

The bounded queue prevents 13,000 pending futures and permits prompt shutdown.
Output counters add `resumed`; existing inserted, revised, cancelled,
unchanged, and failed meanings remain stable.

Injected test clients continue to work without changing existing callers. When
`run(..., client=...)` is used and `--workers` was not explicitly supplied, the
effective worker count is `1`; explicitly combining a supplied client with
`--workers > 1` is rejected. Ordinary CLI runs default to four workers.
Parallel tests and production use an injectable client factory so sessions are
never shared across threads.

## Retry Policy

Do not add a second retry loop around symbol acquisition. `MassiveClient`
already centrally retries HTTP 429 and 5xx responses plus transient connection
and timeout failures, using its configured exponential backoff and
`Retry-After` behavior. Validation errors, terminal provider responses, and
canonical store errors remain terminal for that symbol after the client policy
is exhausted. Failed symbols are counted and left incomplete for the next
resumed run.

The command returns nonzero when any symbol remains failed, even if other
symbols completed successfully.

Authentication and authorization failures are run-fatal rather than repeated
for the remaining universe. They signal all workers to stop accepting new
symbols, leave the cursor incomplete, and return nonzero so an operator can fix
credentials and resume. A run is marked complete only when every requested
symbol is in the completed set and the run has no failures.

## Safety and Observability

- Cursor identity is printed without credentials.
- Per-symbol errors remain on stderr.
- The terminal JSON includes requested, attempted, pending, resumed, completed,
  and the existing reconciliation counters.
- `requested = resumed + attempted + pending`; `pending` is normally zero and
  records symbols deliberately not started after a run-fatal error. `completed`
  is the total durable completed set after the invocation, including resumed
  symbols, while action counters describe only reconciliations performed by the
  current invocation.
- `dry_run` may use concurrency and resume, but its cursor identity cannot be
  reused by a mutating run.
- A targeted or scheduled run cannot overwrite another scope's default cursor.
- Cancellation inference remains available only with explicit
  `--full-reconcile`.

## Tests

Add tests proving:

- worker-count validation;
- four-worker execution uses distinct client instances;
- worker-owned clients are closed after success and failure;
- only successful reconciliations enter the cursor;
- resume skips completed symbols and retries failures;
- incomplete cursors resume across dates while completed cursors reject resume;
- root, ticker-set, and mode mismatches reject an explicitly supplied cursor;
- default cursor paths isolate targeted, dry-run, and full-reconcile scopes;
- provider retry behavior is not duplicated above `MassiveClient`;
- authentication failure stops new work and leaves a resumable incomplete
  cursor;
- cursor writes are atomic and contain no API key;
- targeted sequential injected-client compatibility remains intact;
- aggregate exit status and counters remain deterministic.

## Production Gate

Run the full universe with `--workers 4 --resume --full-reconcile`. Require zero
failed symbols and cursor-confirmed successful reconciliation for every one of
the 13,099 Bronze equities before the split-basis audit. For symbols with no
actions, cursor completion is the durable proof of a successful empty fetch;
the store does not create an empty Parquet file. This design does not authorize
the production Silver pointer cutover.
