# Silver atomic publication and consumer data contract

Status: implementation authorized and in progress; production migration remains a separate cutover approval. This document defines the producer data contract, not Apex's internal API.

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

The approved storage/path change requires manifest-based consumers. The resumed
scope keeps Apex changes inside its data adapter plus necessary caller wiring;
subscription, cache, indicator, signal and event redesign is deferred to the Apex
rewrite. Those internals must not define Livewire's publication contract.

## Stable producer contract

- Manifest v1 preserves `schema_version`, monotonic positive `revision`,
  `generation_id`, UTC `published_at` and `corporate_actions_as_of`,
  `affected[] {symbol, earliest_date, timeframes}`, and
  `artifacts[] {path, sha256}`. Unknown incompatible versions fail explicitly.
  `current.json` must equal the immutable revision manifest byte for byte.
- `affected` is complete represented membership, including carried symbols,
  rather than a change delta. Every member has one daily/factor pair.
  `earliest_date` describes daily coverage; `timeframes` describes adjustment
  applicability, not the existence of Bronze files for every timeframe.
- Artifact paths are relative to Silver, stay within that root, and retain
  encoded symbol partitions. Prefixes are opaque to consumers; `generation_id`
  does not require every carried artifact to live in that generation directory.
- Daily prices are adjusted and `adj_close == close`; volume changes only for
  splits. Raw intraday timestamps are UTC; factor joins use their
  America/New_York trading date and exactly one inclusive factor interval per
  bar. A committed pair may contain carried artifact revisions older than the
  manifest revision. Factor coverage can precede the trimmed daily window.
- Missing references/files, invalid schema, hash mismatch or uncovered factors
  fail explicitly for adjusted reads. Never substitute raw or fixed-path data.
  Absence means no published artifact; disappearance establishes withdrawal,
  but v1 does not encode its reason. Current implementation refuses an empty
  publication: an all-failed run retains the previous pointer, so complete
  withdrawal is not yet representable.
- A nonzero attempt may still commit a healthy subset. Attempt failure and
  committed publication are separate facts. Publication is not consumer
  acknowledgement, recomputation completion or signal delivery.
- Retain committed generations and abandoned attempts. Future deletion needs
  an explicit retention and reader-lifetime agreement. Old pinned reads remain
  usable after later publication or withdrawal. Bronze intraday is not frozen
  by a Silver snapshot.

The Apex adapter owns pinning, artifact verification, date/factor interpretation
and explicit availability errors. Its Python classes and HTTP/WS behavior may
change independently; this contract promises no atomic signal replacement,
polling latency, tick replay or internal cache behavior.

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

Livewire implementation worktree is based on `23e1de2`:

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

Apex implementation worktree is based on `905cab6b`; deployment must be checked
again at cutover rather than inferred from the source checkout:

- `src/infrastructure/adapters/livewire/ohlc_provider.py` resolves daily/factor
  paths from one pinned manifest and fails closed when the requested artifact
  is absent. It does not fall back to a legacy file omitted by the manifest.
- `src/infrastructure/adapters/livewire/revisions.py` validates the complete
  manifest structure and retains its artifact mapping. Normal reads verify
  hashes for the requested artifacts; the full verification mode checks all.
- Minimal callers pass an accepted snapshot to adapter reads where required.
  Transactional reseeding, cache replacement and queued-signal invalidation
  remain Apex internal work, outside this release's atomicity claim.
- Chart and instrument request wiring and `scripts/check_silver_canary.py`
  report/use the revision they pinned. `paths.py` and `server.py` retain their
  existing configuration and lifecycle responsibilities.
- `tests/unit/infrastructure/livewire/test_snapshots.py` and adapter/request
  tests exercise committed reads, missing/corrupt data and manifest changes.
  Livewire's `tests/contract/verify_apex_interop.py` runs the actual producer and
  consumer using their separate Python environments.

This lists the implemented compatibility boundary. It does not establish
transactional behavior for Apex consumers above that boundary.

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
   direct Apex adapter reads. Assess deferred consumer lifecycle risks explicitly
   before enabling it. Resume scheduled work and
   verify a normal nightly run separately from the migration run.
5. Roll back code only to a version that understands the committed storage
   contract. Returning to the old mutable writer requires a separate maintenance
   procedure and data-layout restoration; it is not a safe release-symlink flip.
   To restore prior data, publish a **new monotonic manifest revision** referencing
   retained verified artifacts from the chosen older generation, preserving the
   producer's monotonic revision contract. Preserve all evidence and files.

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
  daily and factor-backed adapter reads match its accepted revision.
  Neither a successful command exit nor a new manifest alone
  proves consumer correctness.
- One subsequent normally scheduled run succeeds, records its lane outcome and
  commit, and does not expose uncommitted generations or revive omitted symbols.
- Remaining uncertainty and retained-orphan storage growth are explicitly
  reported. Do not declare runtime closure from local tests or deployment alone.

## Dependency order

- S1 approve storage and consumer boundary; `depends_on: []`.
- S2 implement immutable writer and manifest resolution; `depends_on: [S1]`.
- S3 implement Apex snapshot adapter reads and minimal wiring; `depends_on: [S1]`.
- S4 cross-repo crash, migration and rollback tests; `depends_on: [S2, S3]`.
- S5 reviewed releases and approved cutover; `depends_on: [S4]`.
- S6 independent released-code and normal-run verification; `depends_on: [S5]`.

Graph: `S1 -> {S2, S3} -> S4 -> S5 -> S6`.
