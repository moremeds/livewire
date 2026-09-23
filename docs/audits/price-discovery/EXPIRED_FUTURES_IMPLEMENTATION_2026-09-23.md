# Expired futures backfill — Task 2 implementation evidence (2026-09-23)

Worker: `implementer` (Devin, kind devin). Task: add an explicit, default-off
`includeExpired` path to the existing futures historical IB pipeline so the
lead can later run only `CL_202610` and `OJ_202609` through `robust`. No
production fetch or write was performed; no commits were made.

## Session context (loaded rules, model, boundaries)

- cwd: `/Users/chenxi/projects/livewire/.worktrees/expired-futures-backfill`
  (branch `feat/expired-futures-backfill`, base `65a0d96`).
- Rules/memory loaded at session start:
  - `/Users/chenxi/.claude/CLAUDE.md` (global rules, injected)
  - `/Users/chenxi/.config/devin/AGENTS.md` (Devin global rules incl. private
    overlay pointer)
  - `/Users/chenxi/.codeium/windsurf/memories/global_rules.md` (empty)
  - `AGENTS.md`, `CLAUDE.md` in the worktree root (repo rules)
  - `/Users/chenxi/projects/c-memory/INDEX.md` and
    `wiki/projects/livewire.md` (private overlay memory index + project page)
  - `/Users/chenxi/.agents/skills/herd/SKILL.md` (herd worker contract/report
    protocol)
- Observed model: SWE-2 Max (self-reported by runtime banner; requested model
  was Devin SWE-2 Max per the execution contract).
- Boundaries honored: edits limited to `livewire_scripts/fetch_ib_historical.py`,
  `livewire_scripts/run_ib_fetch_robust.py`, `tests/test_fetch_ib_historical.py`,
  `tests/test_run_ib_fetch_robust.py`, and this evidence file. `tasks/todo.md`
  was already modified by the lead before this session and was not touched.
  No writes to presets, production paths, lake, cursors, logs, or `.env`.
  No network calls; no commit/push/PR.

## Ground truth confirmed before editing

- Dependency: repo imports `ib_async` (the ib_insync successor), not
  `ib_insync` itself. Installed `ib_async==2.1.0`; `Contract` is a dataclass
  with field `includeExpired: bool = False` — verified via
  `dataclasses.fields(Contract)` and by setting it on a `Future` instance:
  `Future('CL','202610','NYMEX',currency='USD').includeExpired` mutates to
  `True`. (`docs/ib_tws_api.md:80` documents the same attribute.)
- Call chain: `scripts/livewire_ingest.py` dispatches `historical` →
  `livewire_scripts.fetch_ib_historical.main()` and `robust` →
  `livewire_scripts.run_ib_fetch_robust.main(argv)`, passing `rest` verbatim.
  No dispatcher change was required — the flag flows through both parsers.
- `run_ib_fetch_robust` spawns one child per ticker:
  `livewire_ingest.py historical --tickers <T> --asset-class <ac> --source <s>
  --batch-size 1 --max-concurrent 1 [--years 0 | --backfill]`.
- `fetch_ib_historical.fetch_ticker_bars` builds the contract via
  `clients.ingestion_common.make_contract` (not owned — untouched) then calls
  `ib.ib.qualifyContractsAsync(contract)`. The same contract object flows to
  `reqHeadTimeStampAsync` (`clients/ib_client.py:722`) and
  `reqHistoricalDataAsync` (`clients/ib_client.py:689`), so setting
  `contract.includeExpired = True` before qualification covers the whole
  per-ticker fetch.
- `is_retired_futures_ticker` rejects only root `BZ`; `CL_202610` and
  `OJ_202609` pass. `ROOT_EXCHANGE_MAP`: CL→NYMEX, OJ→NYBOT.
- Existing tests build `argparse.Namespace` fixtures without the new field,
  so `include_expired` is threaded as an explicit keyword-only parameter with
  default `False` through `_run_normal`/`_run_backfill` rather than read off
  `args`.

## Changed files

- `livewire_scripts/fetch_ib_historical.py` (+26 lines):
  - New `--include-expired` CLI flag (store_true, default off).
  - `main()` rejects it for non-futures: `parser.error("--include-expired is
    only valid with --asset-class futures")` → exit 2.
  - One `[yellow]` console line printed only when the flag is on.
  - `include_expired` kwarg added to `fetch_ticker_bars`,
    `fetch_all_tickers`, `_run_normal`, `_run_backfill` (all default `False`).
  - `fetch_ticker_bars` sets `contract.includeExpired = True` immediately
    before `await ib.ib.qualifyContractsAsync(contract)`, only when requested.
- `livewire_scripts/run_ib_fetch_robust.py` (+20/-4 lines):
  - New `--include-expired` flag; `parse_args` rejects it unless
    `--asset-class futures` (`p.error`, exit 2).
  - `include_expired` kwarg on `_build_worker_cmd` and `run_one_ticker`
    (default `False`); appended to the child argv as `--include-expired`.
  - `main()` forwards `args.include_expired` to `run_one_ticker`.
- `tests/test_fetch_ib_historical.py` (+114): 4 new tests —
  contract flag set before qualification when requested; flag stays `False`
  by default; `fetch_all_tickers` forwards the kwarg; `main()` rejects
  non-futures (SystemExit 2) and threads the flag end-to-end.
- `tests/test_run_ib_fetch_robust.py` (+98): 7 new tests — default off,
  futures-only rejection (SystemExit), parses for futures, worker cmd omits
  the flag by default / appends it when set, `run_one_ticker` forwards it
  into the spawned argv, `main` forwards it into `run_one_ticker`.
- `scripts/livewire_ingest.py`: intentionally unchanged — `rest` argv passes
  through verbatim to both module parsers, and `_requires_ib_preflight` is
  unaffected by a boolean flag.

## Commands and results

| Command | Exit | Result |
|---|---:|---|
| `uv run python -c "<ib_async Contract/includeExpired probe>"` | 0 | ib_async 2.1.0; `includeExpired` dataclass field exists, default `False`, settable on `Future` |
| `uv run pytest tests/test_run_ib_fetch_robust.py tests/test_fetch_ib_historical.py -q` | 0 | 202 passed in 7.07s |
| `uv run pytest <same> -q -W error::RuntimeWarning` | 0 | 202 passed in 0.54s |
| `uv run ruff check <4 changed files>` | 0 | All checks passed |
| `uv run ruff format --check <4 changed files>` | 1 → 0 | Initial diff touched only the newly added lines; `ruff format` applied, then "4 files already formatted" |
| `uv run pytest <2 test files> --cov=<2 modules> --cov-report=term-missing` | 0 | 100.00% on both changed modules (447/447 and 160/160 stmts) |
| `uv run pytest <2 test files> tests/test_livewire_entrypoints.py -q` | 0 | 265 passed in 3.23s |
| `uv run python scripts/livewire_ingest.py historical --help` | 0 | `--include-expired` listed with help text |
| `uv run python scripts/livewire_ingest.py robust --help` | 0 | `--include-expired` listed with help text |
| `uv run python livewire_scripts/fetch_ib_historical.py --tickers AAPL --include-expired` | 2 | `error: --include-expired is only valid with --asset-class futures` |
| `python -c "run_ib_fetch_robust.parse_args(['--tickers','AAPL','--mode','seed','--include-expired'])"` | 2 | `error: --include-expired requires --asset-class futures` |
| `python -c "parse_args + _build_worker_cmd for CL_202610 backfill futures --include-expired"` | 0 | child argv ends `… --backfill --include-expired` |
| `uv run python scripts/livewire_ingest.py historical --tickers AAPL --include-expired` | preflight | `ERROR: IB Gateway not reachable on 127.0.0.1:4001` — entrypoint preflight runs before subcommand parsing (see limitations) |

## Implementation limitations

- No live IB request was made (contract forbids it). `includeExpired`
  attribute existence was verified against installed `ib_async 2.1.0`; actual
  qualification of `CL_202610`/`OJ_202609` and bar availability (IB serves
  expired futures only within ~2 years of expiry) remain to be observed in
  Task 3.
- Entrypoint ordering: `livewire_ingest.py` runs the IB Gateway preflight
  before dispatching to the module parser, so on a host where the Gateway is
  down the futures-only rejection for `historical` is never reached
  (preflight exits first). On the mini with the Gateway up, parse and
  rejection behave as tested.
- `--include-expired` + `--source massive` is not separately rejected at
  parse time; the combination is unreachable in practice because
  include-expired requires futures while `_resolve_historical_source` already
  rejects massive for non-equity.
- Mode asymmetry relevant to the planned run: under `robust`, `--mode seed`
  skips `CL_202610` (parquet exists) and fetches `OJ_202609` (parquet
  absent); `--mode backfill` does the reverse. One invocation cannot cover
  both symbols — the lead needs one seed run for `OJ_202609` and one backfill
  run for `CL_202610`, each with `--include-expired`.
- For an expired contract whose `reqHeadTimeStamp` returns empty, the
  existing `IB_EARLIEST_DATE` fallback applies unchanged.

## Deviations

None. `scripts/livewire_ingest.py` was deliberately not edited because the
dispatcher forwards argv verbatim; the contract permitted owning it only if
plumbing required it.

---

# Task 3 — bounded production run evidence (2026-09-23)

Authorized by `herd-continue 3`. Scope: only `symbol=CL_202610/1d.parquet` and
`symbol=OJ_202609/1d.parquet` under `asset_class=futures`, plus task-local
state. All commands ran on `moremeds@moremeds-Mini` over SSH (production host);
the MacBook's local Gateway probe fails (exit 1) — production lives on the
mini only.

## Pre-run ground truth

- Release: `/Users/moremeds/market-warehouse/current` →
  `releases/65a0d96df1e35937ac99b2c042bb6fddaeb40592` (matches worktree base).
- Prod venv `…/current/.venv/bin/python`: `ib_async 2.1.0`,
  `Contract.includeExpired` field confirmed.
- Gateway `127.0.0.1:4001` reachable from the mini (probe exit 0).
- Daily job running but in its equity lane:
  `livewire_ingest.py daily --asset-class equity` (PID 75177 under
  `run-daily-job` PIDs 18448/18449). No `lsof` holder on
  `symbol=CL_202610/1d.parquet.lock`; the legacy `lake-io.lock` is not taken
  by current lanes (per-symbol `fcntl` locks only).
- Pre-write state: `CL_202610` = 2137 rows, trade_date `2018-01-24`→
  `2026-09-22`, mtime `2026-09-23 05:00:20 UTC`. `OJ_202609` directory absent.
  Task log dir `logs/expired-futures-backfill-20260923` absent → created.

## Staging

The worktree was rsynced (excluding `.venv`, `.git`, caches; ~28 MB) to
`~/tmp/expired-futures-backfill-20260923/livewire` on the mini — a tmp staging
dir, not a production path; no deploy. sha256 of both changed modules verified
byte-identical between local worktree and staged copy:

- `fetch_ib_historical.py` `17b0d1f0…` (local == remote; release `a52f1e3b…`
  differs — release lacks the flag)
- `run_ib_fetch_robust.py` `ee9dbde5…` (local == remote)

Dry-run of `_build_worker_cmd` confirmed child argv ends
`… --years 0 --include-expired`.

## Run commands and results

Environment for every command: cwd = staged repo, prod venv python,
`MDW_LOG_DIR=$HOME/market-warehouse/logs/expired-futures-backfill-20260923`
(redirects cursors, orch logs, telemetry, and quality audit into task-local
state — production cursors/logs untouched).

| # | Command | Exit | Result |
|---|---------|-----:|--------|
| 1 | `python scripts/livewire_ingest.py robust --tickers OJ_202609 --mode seed --asset-class futures --include-expired --bronze-dir ~/market-warehouse/data-lake/bronze --log-dir ~/market-warehouse/logs/expired-futures-backfill-20260923/orch` | 0 | `[1/1 ok] OJ_202609 attempts=1 dt=8s rows +746` |
| 2 | `python scripts/livewire_ingest.py robust --tickers CL_202610 --mode backfill --asset-class futures --include-expired --bronze-dir ~/market-warehouse/data-lake/bronze --log-dir ~/market-warehouse/logs/expired-futures-backfill-20260923/orch` | 0 | `[1/1 ok-noop] CL_202610 attempts=1 dt=7s no older history` |

## Post-write verification

- `OJ_202609/1d.parquet` created: **746 rows, trade_date `2023-10-03`→
  `2026-09-22`**, schema matches the per-contract futures layout
  (`trade_date, contract_id, root_symbol, expiry_date, OHLC, settlement,
  volume, open_interest, asset_class, symbol`); every row carries
  `symbol=OJ_202609`, `root_symbol=OJ`, `expiry_date=2026-09-01`.
  `contract_id` is the internal bronze symbol id (`get_symbol_id`), not the
  IB conId. No `meta.json` sidecar and no `quality_audit.jsonl` — quality
  detection emitted no flags.
- `CL_202610/1d.parquet` **unchanged**: still 2137 rows, `2018-01-24`→
  `2026-09-22`, mtime `05:00:20 UTC` (predates the run ~4 h). All existing
  observations intact; the noop wrote nothing.
- Only futures parquet modified in the last 2 h window: `OJ_202609`
  (mtime `08:55:04 UTC`). No other symbol path touched.
- Production `logs/telemetry.jsonl`: zero `OJ_202609`/`CL_202610` entries;
  last line `2026-09-23T08:01:41Z` is daily-job traffic. Run telemetry went to
  the task-local `telemetry.jsonl`.

## IB head timestamps (read-only probe, same staged env)

`IBClient` + `qualifyContractsAsync` + `get_head_timestamp_async`
(TRADES/useRTH/formatDate=2), `includeExpired=True`, no writes:

| Contract | conId | IB head | Consequence |
|---|---|---|---|
| `CL_202610` | 304037496 | `2018-01-24 14:30 UTC` | head == existing oldest → backfill is a genuine noop; the file already holds all available history |
| `OJ_202609` | 657732422 | `2023-10-03 12:00 UTC` | head == seeded min date → the seed captured all available history |

## Task-local state produced

Under `logs/expired-futures-backfill-20260923/`:

- `cursor_custom.json`: `completed: {OJ_202609: ["1d"]}`
- `cursor_backfill_custom.json`: `completed: {CL_202610: ["1d"]}` plus
  `backfill_depth.CL_202610 = {oldest_date: "2018-01-24", noop: true}` — this
  marker is written only on the genuine empty-fetch path, proving the noop was
  a real `bars == []` result (qualification + head request succeeded), not a
  masked fetch error (which writes no cursor entry).
- `orch/orch_seed_20260923_085458Z/_summary.log`, `orch/orch_backfill_
  20260923_085516Z/_summary.log`, `telemetry.jsonl`.

## Task 3 deviations

None. Two non-write operations beyond the two run commands are disclosed:
staging the worktree under `~/tmp/` on the mini (required to run the new flag
without deploying), and the read-only qualify/head probe above for the
acceptance record. No commits, no push, no schedule/cursor changes outside
task-local state, no Gateway restart.
