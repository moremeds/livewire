# Claude Code: independently verify the datalake integrity release

Read this document, inspect the current source and release state, and verify the
checks below. Do not inherit a previous "green" conclusion from this handoff.

## Scope and release identity

This change rejects an unreadable catalog source before replacing the database,
grades missing production views and remaining Silver failures explicitly, and
runs the catalog lane after Silver in daily-update. A failed catalog is a failed
run even if IB-only lanes are also degraded. It does not repair the existing
Bronze files or resolve the 269 Silver failures / 53 window regressions observed
on 2026-09-07. Those counts are a baseline, not a quality acceptance threshold.

The separate [Silver atomic publication design](../plans/2026-09-08-silver-atomic-publication.md)
changes the storage/consumer contract and is not implemented by this patch.
Do not claim SIGKILL-safe Silver snapshots from this release.

Set `VERIFY_SHA` to the **full merged commit supplied in the release handoff**.
Never substitute whatever `origin/main` happens to point at when you verify.
Development base was `62c9f14db6629465d9d6ccae5f9986d606d7523e`; the production
baseline was `dfd77103a0a458258798ec88321b8c0eb08929d7`.

Production is `ssh macmini`, user `moremeds`. The MacBook lake is not a production
substitute. The mini's data root is `~/market-warehouse/data-lake`, with bulk
subdirectories pointing onto `/Volumes/DATA_LAKE/livewire/data-lake` and mutable
metadata on internal storage. Inspect the resolved paths again before any test.

## 1. Match code, CI and release before testing

On the mini, set the supplied SHA, then run:

```sh
VERIFY_SHA='<full merged SHA from the release handoff>'
VERIFY_RELEASE="$HOME/market-warehouse/releases/$VERIFY_SHA"
test "$(basename "$(readlink "$HOME/market-warehouse/current")")" = "$VERIFY_SHA"
test -d "$VERIFY_RELEASE"
test ! -e "$VERIFY_RELEASE/.git"
test -x "$VERIFY_RELEASE/.venv/bin/python"
cd "$VERIFY_RELEASE"
.venv/bin/python -c 'import clients, livewire_scripts; print(clients.__file__, livewire_scripts.__file__)'
gh run list --repo moremeds/livewire --commit "$VERIFY_SHA" --workflow ci.yml \
  --json headSha,status,conclusion,url
```

Require successful completed CI for this exact merged SHA and imports from that
release. Check the latest deployment and running job independently: a process
started before the pointer changed can still be running the old release. Do not
call `release promote` merely to make this identity check pass.

## 2. Re-run isolated code regressions at the exact commit

Use a separate checkout of `VERIFY_SHA` and the frozen development dependencies;
do not run `uv sync` inside the read-only release or modify the working checkout.
Run the committed tests, not a reconstructed approximation:

```sh
uv sync --dev --frozen
uv run pytest tests/test_duckdb_catalog.py tests/test_duckdb_catalog_cli.py \
  tests/test_status.py tests/test_run_daily_update_job.py tests/test_constants.py \
  -q -W error::RuntimeWarning
```

Confirm the regressions actually cover:

- Valid catalog -> physically truncated source -> build raises and the previous
  database is byte-identical. The first-build case creates no destination.
- Restore the fixture -> retry succeeds without manually removing the staging
  database. Permission/other I/O errors are not classified as empty sources.
- One missing production view is BAD even when all other views are current;
  zero/undated rows cannot pass. Silver failures remain WARN when unchanged or
  shrinking; no measurements remain UNKNOWN.
- Daily runs the real `duckdb build` argv after Silver and before the digest;
  a catalog failure propagates to a nonzero FAILED run, including an IB outage.
  Dry-run does not build a catalog. The status/digest `Catalog build` check sees
  the current terminal build even before the enclosing run closes; an older
  successful catalog cannot hide this failure.

All fixtures must use temporary roots; no fixture may truncate real lake files.
For complete CI equivalence also run `uv run ruff check .`,
`uv run ruff format --check .`, `uv run pyright`,
`uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning`, and
`npm ci && npm run test:alerts`. Bare `--cov` uses the configured real source
tree; `--cov=scripts` would measure the entrypoints instead of implementations.

Baseline tooling limits: pyright reports warnings in untouched code; the locked
Node dependencies include a Nodemailer security advisory (GHSA-p6gq-j5cr-w38f).
The dependency upgrade is outside this patch. External tribunal tools produced
no review: Claude rejected the isolation flag, Gemini returned license 403, and
Cursor repeatedly disconnected. Native independent review is recorded separately;
do not describe it as a successful cross-model tribunal.

## 3. Exercise released code against the real mini lake, without replacing it

When no scheduled writer owns the lake, build to a **new disposable database**
using the released interpreter and source. Acquire the existing lake lock for
the read, so this check does not race or compete with a writer. Do not guess that
an idle process list guarantees the next scheduled job cannot start.

```sh
cd "$VERIFY_RELEASE"
VERIFY_OUT="$(mktemp -d "$HOME/market-warehouse/catalog-verification.XXXXXX")"
export VERIFY_OUT
.venv/bin/python - <<'PY'
import fcntl
import os
from pathlib import Path
from clients.duckdb_catalog import build_coverage
from livewire_scripts.paths import lake_lock_path

destination = Path(os.environ['VERIFY_OUT']) / 'candidate.duckdb'
with lake_lock_path().open('a') as handle:
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    print(build_coverage(destination))
PY
```

If the lock is busy, defer. If RJF or another source is still unreadable, expect
nonzero exit and no `candidate.duckdb`: **the code safeguard passes, production
data acceptance fails**. Record the exact path/error and stop that branch.
Do not truncate, delete, quarantine, or replace a source as part of verification.
Any data recovery must preserve the full history and follow a separately
authorized, evidence-bound ingestion/repair procedure.

If the build succeeds, inspect the candidate, not the old production database:

```sh
.venv/bin/python scripts/livewire_store.py duckdb \
  --database "$VERIFY_OUT/candidate.duckdb" sql \
  'SELECT view_name,count(*) AS symbols,min(last_date) AS oldest,max(last_date) AS newest FROM coverage GROUP BY view_name ORDER BY view_name'
```

Require all seven production views (`bronze_equity_1d`, `bronze_futures_1d`,
`bronze_cmdty_1d`, `bronze_rates_1d`, `bronze_fx_1d`,
`bronze_volatility_1d`, `silver_equity_1d`) with dated, nonzero rows. Compare
actual expected symbols against the repo's presets/registry and trading calendar;
neither nonzero counts nor `max(last_date)` alone proves complete coverage.

## 4. Verify per-symbol outcomes and remaining quality failures

Resolve `VERIFY_TARGET` using the repo's calendar for the intended closed
session. In particular, 2026-09-07 was not an equity trading session; the previous
closed equity session was 2026-09-04. Respect asset-class calendar limitations.

```sh
VERIFY_TARGET="$(.venv/bin/python -c 'from datetime import UTC, datetime; from clients.pit_silver_revision import daily_bar_cutoff; print(daily_bar_cutoff(datetime.now(UTC)))')"
.venv/bin/python scripts/livewire_store.py duckdb \
  --database "$VERIFY_OUT/candidate.duckdb" freshness --target-date "$VERIFY_TARGET"
.venv/bin/python scripts/livewire_store.py duckdb \
  --database "$VERIFY_OUT/candidate.duckdb" lag --target-date "$VERIFY_TARGET" --json
.venv/bin/python scripts/livewire_ops.py ledger query \
  "select name,value,measured_at,run_id from measurements where name in ('silver_failed','silver_window_regressions') order by measured_at desc limit 6"
.venv/bin/python scripts/livewire_ops.py status
```

Name missing/stale symbols and distinguish delisted/no-trade/unknown from actual
missing data using existing evidence. Positive unresolved Silver failures must
not turn green because the count fell or stayed constant. A Silver lane marked
completed is not proof that a revision advanced or its files form a snapshot.
Record the current manifest revision and timestamp separately. Do not silently
accept the baseline counts as "fixed".

## 5. Require one new normally scheduled run on the release

Do not replace this check with a manually started daily job. After the scheduler
has run the new release, inspect:

```sh
.venv/bin/python scripts/livewire_ops.py ledger query \
  "select run_id,release_sha,started,ended,exit_code,verdict from runs where job='daily-update' and ended is not null order by started desc limit 5"
```

Choose the exact new run ID and query its terminal lane rows, including `catalog`:

```sh
.venv/bin/python scripts/livewire_ops.py ledger query \
  "select lane,started,ended,outcome,exit_code,blocker from lane_results where run_id='REPLACE_WITH_EXACT_RUN_ID' and ended is not null order by started"
```

Require its `release_sha = VERIFY_SHA`; catalog after Silver and before the
digest; no hidden catalog failure. A lock-blocked lane is incomplete, not
successful. If the known bad source remains, the correct result is a FAILED
catalog/run with an explicit source error and preserved previous database.
Successful full acceptance additionally requires a current complete published
catalog and explained per-symbol data quality outcomes.

## 6. Report separate verdicts and stop conditions

Report these independently: **code regression**, **exact deployment**,
**normal scheduled run**, **catalog completeness**, **Silver data quality**,
and **Silver atomic publication**. Each needs evidence or NOT VERIFIED/BLOCKED.
The last item remains unimplemented under this patch's scope.

Stop on SHA drift, a busy writer lock, unexpected source mutation, catalogue
omission, failed regression, or unexplained new quality failures. Do not restart
IB Gateway, prune releases, mutate production data, or lower acceptance criteria.
If code rollback is needed, preserve the failing run evidence and use the
existing release rollback mechanism only within the user's authorization.

Return run IDs, exact SHAs, UTC observation times, commands/results, named
symbols, and the remaining blockers. A zero process exit is not the final verdict.
