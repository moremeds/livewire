# Silver atomic publication: proposed contract change

Status: design for approval; no runtime implementation or production migration authorized by this document.

## Problem and acceptance boundary

The current publisher overwrites served daily and factor files before replacing
`revisions/current.json`. A killed process can leave old manifest hashes pointing
at new bytes. Recovering hashes permits another publish but does not prevent
consumers from reading uncommitted bytes.

Required outcome: a reader pins one committed manifest and reads only the files
named by it. An interrupted publication exposes either the previous committed
manifest or the complete new one; never an intermediate file set. Daily and
factor files selected for a symbol belong to the same publication decision.
This does not promise a frozen Bronze snapshot for concurrent intraday reads.

This requires approval for a Silver storage/path contract change and coordinated
Apex consumer changes. A Livewire-only writer patch cannot satisfy the boundary
while consumers keep constructing mutable paths.

## Smallest coherent design

1. Keep `silver/revisions/current.json` as the **only commit record**. Keep the
   existing manifest fields and relative `artifacts[].path` references; change
   their targets to immutable artifact locations. Do not add a second current
   pointer or a database.
2. Write changed symbol pairs beneath
   `silver/generations/<unique-attempt-id>/asset_class=equity/symbol=.../1d.parquet`
   and the corresponding `adjustments/.../factors.parquet`. Allocate a unique
   attempt directory, including on retries, so a killed attempt is never
   overwritten. These files are staging until a committed manifest names them;
   no second rename of the generation directory is necessary.
3. Reuse SilverClient's schema validation and publisher locking. Validate both
   files, compute their hashes, and flush artifacts and directory entries before
   publishing the manifest. Published artifact paths are never rewritten.
   Audit the exact fsync support on the production filesystem during validation.
4. Assemble a complete manifest. Unchanged symbols retain the **committed path
   and hash**, without copying bytes or using hard links (exFAT has no hard-link
   requirement in this design). Changed symbols replace both references;
   quarantined symbols are omitted. A carried reference is not legitimized by
   replacing its expected hash with whatever bytes happen to be on disk.
5. Write and flush the immutable revision manifest, then atomically replace and
   flush `current.json` and its parent directory. This swap is the commit point.
   No consumer is allowed to discover generations by glob or newest timestamp.
6. On restart, `current.json` remains authoritative. An immutable manifest ahead
   of current is uncommitted: quarantine its metadata without deleting its
   artifact directory, then rebuild using a new attempt id. Do not adopt an
   orphan solely because its schema/revision fields match. This deliberately
   replaces automatic orphan adoption with one unambiguous commit boundary.
7. Keep old generation files and uncommitted attempts. Garbage collection is
   deferred; it needs retention and active-reader rules and must not be added to
   this repair. Track added disk use and reject publication before unsafe space
   exhaustion. Initial migration needs one additional Silver-sized copy; later
   runs retain changed files, not full copies of every generation.

The existing semantic comparison can decide which symbols changed, but it must
read paths from the pinned current manifest. In the new layout, an unreferenced
file is an abandoned attempt, not evidence to auto-remanifest. Quarantine means
omission from the new manifest; do not move files referenced by older revisions.

## Exact implementation surfaces

Livewire source reviewed in worktree based on `62c9f14`:

- `clients/silver_client.py`: currently constructs fixed daily/factor paths and
  writes them; accept an attempt output root for writing and explicit committed
  paths for comparisons. Prevent overwriting an existing immutable artifact.
- `clients/silver_revision.py`: owns transaction reservation, artifact checking,
  manifest publication and orphan reconciliation. Keep one implementation here;
  change orphan handling and persistence ordering rather than adding a second
  publisher.
- `livewire_scripts/rebuild_silver.py`: the sole runtime caller of
  `SilverClient.publish_daily` / `publish_factors`; replace mutable publication,
  carry-forward, orphan-remanifesting and physical eviction with the rules above.
- `clients/duckdb_catalog.py`: `view_specs`, `symbol_files` and Silver reads
  currently derive fixed paths/globs. Resolve the manifest once per logical
  query/connection snapshot and construct explicit file lists from it. A Silver
  join must not independently select a later manifest for its factor view.
- `livewire_scripts/validate_adjusted_history.py`: currently constructs fixed
  Silver daily paths; bind validation inputs and run identity to a pinned
  committed manifest.
- `clients/pit_silver_revision.py`: already uses manifest artifact references;
  verify compatibility with the generation prefix and historical revision
  retention. Its own current record remains a lineage record, not a competing
  Silver publication pointer.
- `livewire_scripts/shepherd_silver.py`: verify its existing manifest-based
  handoff remains valid; change only if integration tests require it.
- `tests/test_silver_client.py`, `tests/test_silver_revision.py`,
  `tests/test_rebuild_silver.py`, `tests/test_duckdb_catalog.py`, adjusted-history
  and PIT tests: extend existing tests around the contract, not test counts.
- Update `README.md`, `CLAUDE.md` and `.codex/project-memory.md` after agreement
  so served-path and manifest semantics describe the same implementation.

Apex source was read on the mini at checkout
`54b26761dd95e358fcbe1b47d66166f1ebd1cc27`; its running deployment was not established:

- `src/infrastructure/adapters/livewire/ohlc_provider.py:139-160` constructs
  fixed daily/factor paths. Resolve them from one committed manifest, and fail
  closed when the requested artifact is absent. No fallback to a legacy file
  that the manifest omitted.
- `src/infrastructure/adapters/livewire/revisions.py`: its reader already
  validates manifest artifact hashes but returns revision metadata without an
  artifact path map. Retain a validated mapping for the pinned snapshot and
  reuse it in bar reads rather than independently deriving paths.
- `src/application/subscriptions/revision_watcher.py` and the subscription
  manager refresh path: pass the accepted snapshot through a reseed, so reads
  for a revision do not accidentally resolve a newer one midway through.
- `src/infrastructure/adapters/livewire/paths.py`, `src/api/server.py` and
  `scripts/check_silver_canary.py`: audit remaining path construction, lifecycle
  and mounting assumptions. Keep API responses unchanged where possible.
- `tests/unit/infrastructure/livewire/test_ohlc_provider.py` and
  `tests/integration/test_silver_revision_e2e.py`: exercise committed reads and
  interrupted publication with the actual consumer implementations.

Locate all remaining consumers before implementation. The above is the reviewed
minimum, not permission to silently leave another fixed-path consumer behind.

## Rejected shortcuts

- Write-ahead backup/rollback: readers can observe overwritten bytes before a
  kill and until recovery; rollback alone does not meet acceptance.
- A persistent “publishing” marker: works only if every reader checks it and
  coordinates reads against writes; therefore still needs cross-repo work and
  causes unavailable data during long publications.
- Per-symbol pointer swaps: cannot establish one whole-revision snapshot and
  need an additional rule to keep separate daily/factor reads together.
- Swapping the Silver root symlink: existing readers may resolve/cache a root
  or mount it once, and multiple file opens can cross the swap. It is not a
  substitute for pinning one manifest.
- Mirroring immutable files back to old served paths: recreates the original
  mutable serving path and cannot be called atomic publication.

## Migration, deployment and rollback

1. Capture exact Livewire and Apex deployed revisions, configured lake roots and
   consumer mounts. Inventory scheduled and manually invoked Silver consumers.
   Check free space for initial migration and retained generations. Preserve all
   old files and manifests.
2. Build the candidate generation on a disposable fixture first, then prepare
   the production migration under an approved maintenance/cutover procedure.
   Do not treat an already inconsistent legacy manifest as a valid baseline by
   refreshing its hashes. Re-derive and validate the initial immutable snapshot
   from canonical inputs; explicitly report symbols omitted by existing quality
   rules.
3. Deploy manifest-based consumers before allowing the new writer to publish.
   They may understand existing manifest relative paths for migration, but must
   not fall back to fixed-path discovery. If the legacy manifest cannot pass
   validation, keep the consumer unavailable during cutover rather than serving
   an invented good state. Suspend legacy Silver writers until cutover completes.
4. Enable the immutable writer, commit one validated snapshot, then verify both
   direct Apex reads and watcher/subscription reseeds. Resume scheduled work and
   verify a normal nightly run separately from the migration run.
5. Roll back code only to a version that understands the committed storage
   contract. Returning to the old mutable writer requires a separate maintenance
   procedure and data-layout restoration; it is not a safe release-symlink flip.
   To restore prior data, publish a **new monotonic manifest revision** referencing
   retained verified artifacts from the chosen older generation, since Apex's
   watcher rejects decreasing revision numbers. Preserve all evidence and files.

## Runnable acceptance and Claude Code release verification

Use a small disposable lake containing at least two symbols, one changed pair,
one unchanged pair, and distinct old/new prices. Run the actual rebuild in a
child process with controlled stop points, send SIGKILL, then read using the real
Livewire and Apex readers. Python exception cleanup is not a SIGKILL test.

Required stop points: after the new daily file, after its factor file, after all
artifact validation, after immutable manifest persistence but before current
swap, and after the current swap. Before the swap every read must remain old;
after it every read must follow the complete new manifest. An in-flight reader
that pinned old must finish with old. Retry must commit successfully without
manual deletion or overwriting an abandoned attempt.

Also verify: a truncated staged artifact cannot advance current; absent symbols
fail closed even if legacy files exist; unchanged references remain unchanged;
quarantine leaves old pinned readers usable; no-op rebuild preserves current;
concurrent writers serialize; historical PIT verification still works; and a
new monotonic rollback revision serves the selected older artifacts.

After implementation, run relevant existing tests and required repository
coverage checks. Claude Code's independent release check must establish:

- Merged SHA, CI result, promoted release target, actual running Livewire/Apex
  revisions and configured consumer roots agree with the reviewed implementation.
- Disposable crash tests use released code and the production filesystem class;
  no destructive crash experiment runs against the real lake.
- One real manifest and its referenced hashes pass validation, representative
  daily and factor-backed reads match it, and watcher health identifies the same
  accepted revision. Neither a successful command exit nor a new manifest alone
  proves consumer correctness.
- One subsequent normally scheduled run succeeds, records its lane outcome and
  commit, and does not expose uncommitted generations or revive omitted symbols.
- Remaining uncertainty and retained-orphan storage growth are explicitly
  reported. Do not declare runtime closure from local tests or deployment alone.

## Dependency order

- S1 approve storage and consumer boundary; `depends_on: []`.
- S2 implement immutable writer and manifest resolution; `depends_on: [S1]`.
- S3 implement Apex snapshot reads/reseed integration; `depends_on: [S1]`.
- S4 cross-repo crash, migration and rollback tests; `depends_on: [S2, S3]`.
- S5 reviewed releases and approved cutover; `depends_on: [S4]`.
- S6 independent released-code and normal-run verification; `depends_on: [S5]`.

Graph: `S1 -> {S2, S3} -> S4 -> S5 -> S6`.
