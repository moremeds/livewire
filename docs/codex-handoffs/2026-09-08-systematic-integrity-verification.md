# Systematic integrity: independent verification and cutover gate

This is the successor to the PR #124 verification note. The candidate is on
Livewire `fix/systematic-integrity` ([PR #125](https://github.com/moremeds/livewire/pull/125))
and Apex `fix/livewire-snapshots` ([PR #161](https://github.com/moremeds/apex/pull/161)).
Do not infer a release from these branch names, a passing local test, or a PR.
Record exact reviewed commits from the PRs before beginning; record merged
commits and deployed artifacts separately after an approved cutover.

## Goal and boundary

Prove that a failed writer cannot expose an incomplete Silver generation,
destroy a valid catalog, leave descendants holding write locks, or prevent
independent healthy symbols/buckets/providers from progressing. Warnings must
identify affected work, evidence, retained data and the retry/clear condition.

The stable interface is the
[Livewire published-data contract](../plans/2026-09-08-silver-atomic-publication.md).
Apex changes are request-level adapter compatibility. They do **not** provide
transactional subscription reseeding, indicator/signal replacement or event
epoch rejection. Those belong to the planned Apex rewrite and must not be
reported as completed by this candidate.

Production is accessed through `ssh macmini`. Do not inspect a MacBook lake as
production evidence. Do not restart IB Gateway, submit orders, repair canonical
data, delete generations, or trigger a production rebuild during verification.

## 1. Establish the exact state

- Record PR/base/head/merge SHAs and each actual CI job result. Livewire's
  early `39e32b8` CI result proves the small gate/first-alert fix only.
- On the Mini, resolve `~/market-warehouse/current`, compare its tracked source
  files with the reviewed Git tree, and inspect the installed launchd arguments.
  A directory named after a SHA alone is not proof of unchanged source bytes.
- Check active writer/repair processes and their source/release paths. An empty
  point-in-time process listing is not a maintenance fence.
- Record the Apex running image ID, OCI revision, container mounts and actual
  readable Silver root. The host checkout SHA is not the container revision.
- Record current manifest bytes/revision, immutable manifest identity, free
  internal/external space, and retained generation bytes. Hash actual selected
  artifacts rather than treating existence as verification.

Candidate evidence on 2026-09-08: Livewire implementation
`c11caa8d96b8416c1a5256f71179639618461692` passed local and Linux CI at
2,660 tests and 95.08% coverage (runs `34183052268`, `34183127817`). Ruff and
format checks passed; Pyright reported zero errors and 25 warnings. Apex
`ae57f93cfb6f772277c6a309f5ae428dd1ce3298` passed CI integration, but CI lint
and type gates failed outside the adapter diff and unit CI was consequently
skipped. This is not a fully green compatible release pair.

## 2. Reproduce the code checks in isolated checkouts

Livewire's required coverage command uses the configured `clients` and
`livewire_scripts` source set. Do not substitute the thin `scripts` wrappers.

```sh
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning
npm ci
npm run test:alerts
```

Report the raw coverage value and process exit. A genuine 94.99% result must
fail. Record baseline warnings separately from branch regressions. The unchanged
Nodemailer dependency currently has an audit advisory; this task does not make
a major dependency upgrade.

For Apex, run its exact CI lint/type/unit/integration commands. The integration
job explicitly excludes the live Futu and multi-broker test files. Do not run
those as an unbounded substitute for a hermetic gate. At candidate preparation,
two Yahoo-backed unit tests and five unchanged Black files failed locally;
those are open baseline checks, not a full-suite pass. CI installed different
tool versions: Black 26.5.1 passed, isort 9.0.1 rejected unchanged
`src/backtest/execution/parallel.py` and `order_matching.py`; CI mypy rejected
the unchanged `src/backtest/optimization/bayesian.py:73` direction argument.
Record environment differences rather than conflating local and CI outcomes.

Run the manual producer/consumer interoperability check from Livewire:

```sh
python tests/contract/verify_apex_interop.py \
  --livewire-root /absolute/path/to/reviewed/livewire \
  --apex-root /absolute/path/to/reviewed/apex
```

It must use each checkout's existing Python environment, publish with the real
Livewire writer, then read through the real Apex adapter. Both adjusted daily
and intraday values, trading dates and pinned revision must match. This does not
exercise Apex's live signal lifecycle.

## 3. Test crashes on disposable Mini storage

Create a **new unique** disposable root on the external filesystem; keep scratch
on the internal disk. Bind every `MDW_*`/`LW_*` output to the disposable root.
Never reuse canonical paths or a pre-existing test directory that pytest could
clear. First verify actual file/directory fsync and cross-process flock there.

Using reviewed code, run the five cases in
`tests/test_silver_atomic_publication.py`: kill the actual child after daily,
factors, validation, immutable manifest persistence, and current replacement.
Before current swaps, a newly pinned reader must still read old. After it swaps,
the complete new pair must be readable. An already pinned old reader must remain
usable in both cases. Retry must succeed without deleting abandoned attempts.

Also verify the existing tests for corrupt staged bytes, unavailable scratch,
unreadable symbol isolation, missing/withdrawn artifacts, raw-date recovery,
catalog staging contention, manual repair stale-input rejection, migration
contention, and process-group cleanup. Exception injection alone is not SIGKILL
evidence. Filesystem fsync success is not proof against every power-loss mode.

## 4. Measure the real workload shape

Use copied daily Bronze, corporate-action events and optional triage inputs in
the disposable root. Record source counts/bytes and copy duration separately;
the copy is not a coherent production snapshot. Freeze the same as-of date for
all scenarios so midnight does not invalidate the no-op comparison.

Measure full first publication, no-op, one-symbol incremental update, failed
attempt and retry. Record elapsed time, maximum RSS, input-copy lock duration,
output/retained bytes, exit code, failed symbols and committed revision.
Classify the earlier 269 failures/53 window regressions from current evidence;
do not simply repeat those old counts. A healthy subset commit is not complete
data health, and one full observation is not a p95 or a business SLA.

The rejected in-memory design projected 16.005 GiB of row objects alone on the
16 GiB Mini. The candidate instead copies inputs to temporary disk and retains
per-symbol staged row files. Verify both memory and scratch/output capacity.
The observed schedule is daily 13:00, intraday 18:00, coverage 19:00 HKT;
the trigger spacing is not an agreed data-delivery deadline.

## 5. Production approval is a separate final gate

Before requesting approval, provide exact compatible commits, full CI results,
disposable test receipts, space/time measurements, allowed interruption and a
specific rollback/stop procedure. Keep automatic release promotion and all old
writers from crossing the migration while maintaining a reversible record of
their installed configuration. Merely merging Livewire could allow its next
automatic promotion to activate the writer prematurely.

Preserve all existing data and releases. A legacy manifest with mismatching
hashes is not repaired by blessing its current bytes. Build and validate the
initial immutable snapshot from protected canonical inputs. Deploy compatible
manifest-based readers before enabling the new writer. Return to an older data
snapshot by publishing a new monotonic manifest referencing retained verified
artifacts; reverting to a mutable writer is not a safe symlink rollback.
The runnable regression `test_rebuild_preserves_bytes_pinned_by_prior_revision`
also exercises this rollback through `SilverRevisionPublisher.publish`: revision
3 references revision 1's retained artifacts while the pinned revision 2 remains
readable. A production invocation must first select and validate the intended
historical manifest and confirm the current revision under the publisher lock.

Legacy `rollback-legacy-basis` intentionally restores its selected original
backup and has no applied-target hash in old sidecars. Its new lock and checksum
checks do not authorize overwriting later completed writes. Any real rollback
still needs the user's concrete data-change approval.

## 6. Report closure only after a normal run

After the separately approved cutover, verify exact deployed files/container,
current manifest and selected hashes, real adapter reads, catalog freshness and
actionable warning/recovery delivery. Then observe one normally scheduled run,
with named lane outcomes and current-data evidence. A manual migration is not
that scheduled run.

Report four verdicts separately: code/CI; disposable filesystem/crash behavior;
deployment/cutover; normal production outcome. Keep Apex live-signal atomicity,
complete-withdrawal representation in manifest v1, hardware redundancy and
unproven backup recovery explicit. Do not convert them into a global “all fixed.”
