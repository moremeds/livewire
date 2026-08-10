# One graded status surface — design

**Date:** 2026-08-10
**Status:** proposed

## The problem

Livewire produces plenty of status. It produces no *judgement*.

This morning's real digest:

```
Coverage:
  2026-08-07 coverage: 1d=0/13311 (0.00%) 1m=0/14613 (0.00%) ...

Disk [warehouse]: 9.2 GiB free (96% used)  ⚠ raw retention deferred
Silver rebuild:
  revision=25  rebuilt=2  unchanged=13076  trimmed=254  failed=233
Daily update outcomes:
  (not found)
```

`1d=0/13311` is warehouse-wide zero coverage. `trimmed=254` is routine. They are
the same font, the same indentation, the same section weight. `(not found)`
— which here means *the run's log could not be located at all* — is
typographically indistinguishable from a healthy line.

Meanwhile `logs/quality_alerts_undelivered/` holds **4,408** files, newest
2026-08-02. That backlog appears in no report anywhere. The channel that would
report it is the channel that broke.

Three named pain points, from the operator:

1. **分不出轻重** — catastrophe and routine render identically.
2. **该发的时候没发** — the alert path fails silently (4,408 undelivered; the
   2026-08-08 page was never sent because its summary began with `--`).
3. **没告诉我该干什么** — a known-bad line names no next command.

Explicitly *not* a pain point: email as a channel. Email stays.

## What already exists

| Surface | Source | Answers |
|---|---|---|
| `livewire_quality.py digest` | parses the night's logs | lane outcomes, Silver, coverage, disk |
| `livewire_quality.py warehouse` | scans parquet footers | per-snapshot health → HTML |
| `livewire_quality.py coverage` | scans parquet footers | coverage for a target trading day |
| `livewire_store.py duckdb freshness/lag/stale` | the coverage table | staleness, silver lag |
| `launchctl list` | launchd | last exit code per job |

DuckDB is already wrapped twice — `clients/duckdb_catalog.py` (library) and
`livewire_scripts/duckdb_catalog_cli.py` (CLI, 7 subcommands). No third wrapper
is needed. The one real friction was that `~/market-warehouse/.venv` has no
`duckdb` installed while the release venv and the repo venv both do; that is an
install, not an abstraction, and is out of scope here.

The genuinely missing thing is a **grading layer** over what already exists.

## Rejected approaches

**Apex serves it.** Apex is FastAPI and already reads the lake, but it is
livewire's *downstream consumer* (`APEX_LIVEWIRE_ROOT`, read-only parquet).
Livewire's operational state lives in logs, cursors, launchd and the DuckDB
catalog. Serving it from apex inverts the dependency direction.

**An MCP server.** MCP is a transport, not the missing piece. Both chosen
consumers — a human at a terminal and an email — read text. Claude can already
run the CLI. Build the assessment; add a transport if a programmatic consumer
ever appears.

**Add thresholds to the existing digest and stop.** Smallest diff, but the
digest's only input is the night's logs. It cannot see `launchctl` exit codes,
cannot see that the DuckDB coverage table is stuck on 2026-08-07, and cannot see
the 4,408 undelivered alerts — the three things that actually bit this week. To
see them it must grow new data sources, at which point it *is* the design below,
without a name.

## Design

One assessment layer, two renderers.

```
livewire_scripts/status.py        collect() -> list[Section]
        │
        ├── livewire_ops.py status        → terminal table
        └── nightly_digest.build_digest   → email body (unchanged shape, plus verdicts)
```

`Section` is a small dataclass:

```python
@dataclass(frozen=True)
class Section:
    name: str            # "Coverage"
    verdict: Verdict     # OK | WARN | BAD | UNKNOWN
    lines: list[str]     # the prose the digest already prints today
    fix: str | None      # a paste-able command, when there is one
```

`collect(run_date)` always takes a date, so the digest's behaviour is preserved
exactly: date-scoped sections keep reading `*_<run_date>.log`, and the coverage
section keeps reading the *newest* `coverage_*.log` as it already does. `status`
simply defaults `run_date` to today (UTC). No section changes which file it
reads.

The six section functions currently in `nightly_digest.py` **move** to
`status.py` and change their return type from `list[str]` to `Section`. Their
parsing bodies are unchanged — three weeks of real logs are already in that
shape, and the prose in them is good. `nightly_digest.py` keeps `build_digest`,
`_send_email` and `main`, dropping from ~300 lines to ~90; `build_digest`
becomes a join over `collect()` that prefixes each section with its verdict and
appends its `fix` line.

Three new sections are added for the sources the digest cannot currently reach.

### `UNKNOWN` is not `OK`

The single most important rule. Today `(not found)` renders like a healthy line.
`UNKNOWN` must render as visibly not-green — a missing input is a failure to
measure, and failures to measure are how every defect this quarter hid.

### The nine checks

All nine are milliseconds. **None scans parquet.** `coverage` itself costs
1400–2860s and `warehouse` scans footers; `status` reads only what the nightly
jobs already produced, and grades *how old that is* as its own signal.

| # | Check | Reads | Verdict rule | Basis |
|---|---|---|---|---|
| 1 | launchd jobs | `launchctl list` | job absent → BAD; nonzero exit → **WARN, never BAD** | see below |
| 2 | Nightly run | `daily_update_<run_date>.log` `SUMMARY_JSON` | `failed` non-empty → BAD; `degraded` non-empty → WARN; no log → BAD; log without SUMMARY_JSON → UNKNOWN | existing `failed`/`degraded` fields |
| 3 | Intraday phases | `intraday_catchup_<run_date>.log` | same rule as #2 | same |
| 4 | Coverage | newest `coverage_*.log` | **any** tracked timeframe below `MDW_COVERAGE_ALERT_THRESHOLD` (0.95) → BAD; log age > 3 days → BAD | **reuses existing constants** |
| 5 | Silver | Silver `SUMMARY_JSON` + the previous run's | `window_regressions` > 0 → WARN; `failed` **increased** → WARN | see below |
| 6 | DuckDB catalog | table mtime + `max(last_date)` | lag > 1 session → WARN; > 3 sessions → BAD | 3 = `_COVERAGE_STALE_DAYS` |
| 7 | Disk | both volumes | free < 2×`MDW_FLATFILE_MIN_FREE_GB` → WARN; < 1× → BAD | **reuses existing constant** |
| 8 | Undelivered alerts | `quality_alerts_undelivered/` | count > 0 → WARN | judgement, see below |
| 9 | Quality jobs | `WARNING: … failed:` lines | any → WARN | existing parser |

Checks 4, 7 and 9 reuse thresholds that already exist in the codebase; adopting
them introduces no new judgement. Checks 1, 5, 6 and 8 need new rules, and each
is stated below with what it rests on.

### Check 1 — `launchctl` exit codes carry no timestamp

`launchctl list` reports the *last* exit status with no indication of when it
happened. Right now `com.livewire.daily-update-watchdog` shows `1` and
`com.livewire.intraday-catchup` shows `86`, but both are residue from runs that
predate the fix now in production; they will roll over tonight.

`status` cannot distinguish a stale red from a fresh one. So a nonzero exit is
capped at **WARN**, annotated *"exit code carries no timestamp — check the
log"*, and never rendered as BAD. A job that is not loaded at all is BAD, since
a job that cannot run cannot recover on its own.

Overstating this check would be the fastest way to make the whole surface
ignorable, which is the disease being treated.

### Check 5 — Silver reports deltas, not absolutes

Is `failed=233` bad? There is no measured basis for any absolute line, and
inventing one would be fabrication. The baseline is instead the previous run:
glob the log directory for the two most recent logs carrying a Silver
`SUMMARY_JSON`, and report the change.

`failed` increasing → WARN. `failed` flat or falling → OK, with the absolute
count still printed. `window_regressions` > 0 → WARN regardless of the delta —
that field already has a documented meaning (published history was withheld) and
does not need a baseline.

When no previous run is available the verdict is UNKNOWN, not OK.

### Check 6 — DuckDB without importing DuckDB

`tests/test_duckdb_containment.py` permits exactly two modules to
`import duckdb`. `status.py` is not one of them and must not become one — that
test is what keeps DuckDB a query layer instead of a second warehouse.

So `clients/duckdb_catalog.py` gains one small read-only function returning per
view its symbol count and `max(last_date)`; `status.py` calls it. The database
file's mtime is a plain `Path.stat()` and needs nothing from DuckDB.

`status.py` must wrap that import in `try/except ImportError` and report the
check as UNKNOWN when DuckDB is unavailable. This is not defensive
programming for its own sake: `~/market-warehouse/.venv` genuinely lacks
`duckdb` today, and a status command that cannot start in one of the three real
environments is worthless.

### Check 8 — the undelivered backlog

Any file present → WARN, printing the count and the newest timestamp. No
severity ladder by age: 4,408 files with a newest date of 2026-08-02 is a
historic pile-up, one file from an hour ago is an active failure, and a rule
that tries to tell them apart would be guessing. The line names both numbers and
lets the reader judge.

There is deliberately **no dead-man's switch** in this design. The operator's
decision: surfacing the backlog in `status` is enough for now. An external
heartbeat would add a network dependency to catch a failure mode a daily glance
already catches.

## Interface

```bash
livewire_ops.py status              # terminal table, always exit 0
```

Rendering: one line per check — verdict glyph, name, headline, and an indented
`fix:` line for anything not OK. Full detail lines follow the shape the digest
already prints.

Exit code is always 0. **Skipped:** a nonzero exit on BAD — neither chosen
consumer reads exit codes, and adding the contract invites someone to wire it
into a job, at which point every stale `launchctl` red becomes a page. Add it
when something actually consumes it.

**Skipped:** `--json`. Both consumers read text. Add it when a programmatic
consumer exists.

## Error handling

`collect()` never raises. Every check catches its own failures and degrades to
`UNKNOWN` with the exception text as its detail line. This preserves the
existing contract in `nightly_digest`'s docstring — a missing input can never
suppress the whole report — and extends it: a check that *crashes* is now
visible rather than silently absent.

## Testing

Per repo rules: `tests/test_status.py`, all I/O mocked, 95% coverage gate.

- One test per verdict rule, driving each check with a fixture log.
- `UNKNOWN` is not `OK` — asserted directly, since that is the defect being fixed.
- A check that raises degrades to `UNKNOWN` and does not break `collect()`.
- `build_digest` output still contains every line the current digest prints,
  asserted against the existing digest tests, which must keep passing unchanged
  apart from the added verdict/fix lines.
- The DuckDB check reports `UNKNOWN` when the import fails.
- `tests/test_duckdb_containment.py` must stay green with no new entry in
  `ALLOWED_DUCKDB_IMPORTERS`.

## What this does not do

- Does not re-measure anything. Coverage and warehouse health keep their own
  jobs and their own costs.
- Does not add a server, an API, or an MCP.
- Does not change how alerts are sent — only that the backlog becomes visible.
- Does not set absolute thresholds for Silver `failed`/`trimmed`.
