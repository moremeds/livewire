# Dividend FX conversion + PIT membership — implementation plan

**Goal:** Bring the two grok-layer writes inside the pipeline: a dividend-currency repair subcommand that supersedes rows with `provider="eod_fx"` and emits to the ledger, and a `membership-sync` job that imports/updates `IndexMembershipStore` and emits to the ledger.

**Spec:** `docs/superpowers/specs/2026-09-13-dividend-fx-and-pit-membership-design.md` — read it in full first; every interface below is defined there.

**Architecture:** No new scripts. Two subcommands on `scripts/livewire_ingest.py`, one read command on `scripts/livewire_ops.py`, one store method + one `apply_repairs` extension, two `status.CHECKS` rows, one plist, one post-mortem. Reference emitter for `runs`/`measurements`: `livewire_scripts/coverage_report.py` (grep `ledger.emit`).

## Execution contract — read before Task 0

You are worker `devin` (kind devin), working in `.worktrees/dividend-fx-pit/` (branch `dividend-fx-pit` from `origin/main`).
The lead is herdr pane `LEAD_PANE=w2:p1`.

1. Scope. Implement only the tasks below, in order. Anything not named: report, do not do.
2. Files. You own: `clients/corporate_action_store.py`, `clients/index_membership_store.py`, `livewire_scripts/`, `scripts/livewire_ingest.py`, `scripts/livewire_ops.py`, `presets/djia.json`, `registry/gaps.json`, `launchd/`, `tests/`, `docs/runbook.md`, `docs/postmortems/`, `CLAUDE.md` (one line only), this plan and its evidence file. You never write: `~/market-warehouse/**` on any host, `data-lake/**`, `.env`, `.github/`, `pyproject.toml`.
   2b. Ground truth: `ssh macmini cat ~/market-warehouse/grok_index/gaps/NOTE_dividend_currency_fx.md`; `ssh macmini python3 -c 'import json;print(json.load(open("/Users/moremeds/market-warehouse/grok_index/gaps/dividend_fx_conversion_applied.json"))["applied"][0])'`; `ssh macmini head -3 ~/market-warehouse/grok_index/pit_membership/sp500/events.jsonl`; `ssh macmini sed -n 1,40p ~/market-warehouse/grok_index/pit_membership/README.md`. Read-only. Fetch before Task 1.
3. Environment. Commands run only in the worktree; temp under `/tmp/dfx-*`. Network: none except the read-only ssh above and `uv sync`. Tests must not hit the network (mock `mediawiki_client`).
4. Evidence. Append `## Task <n>` to `docs/superpowers/plans/2026-09-13-dividend-fx-and-pit-membership.evidence.md`: exact commands, exit codes, pasted output.
5. Commits. One per task, `task <n>: <title>`. No attribution trailers: no `Co-Authored-By`, no `Generated with`. Repeated every dispatch; a commit with one is rejected.
6. Review gate. After each task: `herdr agent prompt w2:p1 "herd-report devin task <n>: commit <sha>, evidence <path>, deviations: <text|none>"`. Wait for `herd-continue <n+1>`.
7. Rejections: `herd-reject <n>: <reason>` → fix on top, new commit, report again. Never amend.
8. Blocked: stop and wait.
9. Deviations: name them in the report line.

Gate for every task: `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q` exit 0.

---

### Task 0: worktree + plan copy

- `git worktree add .worktrees/dividend-fx-pit -b dividend-fx-pit origin/main`
- Copy the spec and this plan from the main checkout into the worktree (same paths); create the empty evidence file. Commit `task 0: plan and spec`.

### Task 1: store — `foreign_currency_dividends` + `convert_dividends`

**Files:** `clients/corporate_action_store.py`, `tests/test_corporate_action_store.py`
**Produces:**

```python
@dataclass(frozen=True)
class DividendConversion:
    action_id: str            # the active foreign-currency dividend
    cash_amount: float        # converted
    currency: str             # equity currency
    source_ref: str           # "eod_fx:<PAIR>@<fx_date> rate=<r> method=<divide|multiply> orig=<amt> <CCY>"
    source_hash: str | None   # sha256 of the FX bar bytes, evidence CAS ref

CorporateActionStore.foreign_currency_dividends(self, symbol: str, equity_currency: str) -> list[CorporateAction]
CorporateActionStore.apply_repairs(..., convert_dividends: list[DividendConversion] = (), ...) -> RepairResult
```

Conversion appends one row: `supersedes_action_id=old.action_id`, old row `status="corrected"`, `event_revision=old+1`, `provider="eod_fx"`, `provider_event_id=old.provider_event_id`, `cash_amount`/`currency`/`source_ref`/`source_hash` from the conversion; `RepairResult` gains `converted: int`. Idempotent: if the active row already has `provider="eod_fx"` and `abs(cash_amount - new) < 1e-9`, no row is written and `converted` does not increase.
Tests (real store on `tmp_path`, real fixture values: use ACR 2008-06-26 0.41 CAD → 0.40517839808341516 USD, USDCAD 1.0118999481201172, from the ground-truth file):

- `test_foreign_currency_dividends_lists_only_active_rows_in_another_currency`
- `test_convert_dividend_supersedes_with_provider_eod_fx`
- `test_convert_is_idempotent`
- **`test_full_reconcile_after_conversion_leaves_the_eod_fx_row_active`**: convert, then `reconcile(symbol, [original CAD event], fetched_at, full_reconcile=True)`; assert the eod_fx row is still the only active dividend on that ex-date and nothing was appended. If the existing reconcile logic fails this, fix reconcile so a non-`massive` active row that supersedes a massive row is treated as the current answer for that `provider_event_id` (document the branch in a comment).

### Task 2: command `corporate-actions convert-dividend-currency`

**Files:** `livewire_scripts/sync_corporate_actions.py` (or a new function module only if the file exceeds its current structure — say so), `scripts/livewire_ingest.py`, `tests/test_sync_corporate_actions.py`, `docs/runbook.md`
**Produces:** `convert_dividend_currency(*, tickers: list[str] | None, apply: bool, output_dir: Path | None, lake_root: Path, now: datetime | None = None, fx_close_fn=None) -> dict` where `fx_close_fn(pair: str, on: date) -> tuple[date, float] | None` defaults to reading FX bronze `1d.parquet` (`asset_class=fx/symbol=<PAIR>`), previous session on a holiday, `None` if no bar within 5 sessions. Equity currency from `security_master` if available else `"USD"` (record `equity_currency_source` in the manifest).

- Dry-run default; `--apply` requires `--output-dir`; manifest JSON matches the grok file's field names (`symbol, ex_date, orig_cash, orig_currency, converted_cash_usd→converted_cash, fx_pair, fx_date, fx_rate, fx_method, source_ref, old_action_id, new_action_id`) plus `skipped: [{symbol, ex_date, reason}]`.
- Ledger: `runs` job `dividend-fx`; `measurements` `dividend_currency_mismatch` (scope all, remaining), `dividend_fx_converted`, `dividend_fx_skipped`, source `measured`; `evidence` row per `HashedRef`. Emitted in dry-run too (remaining count is a fact either way).
- Tests: dry-run writes nothing to the store; apply converts and emits the three measurements (assert via `ledger.query` under `LW_LEDGER_ROOT=tmp`); missing FX bar → skipped with reason `no_fx_bar`.

### Task 3: status check "Foreign-currency dividends"

**Files:** `livewire_scripts/status.py`, `tests/test_status.py`
Latest `dividend_currency_mismatch` value > 0 → WARN with the count; zero rows → UNKNOWN. Test both, plus that the digest body (`nightly_digest` via `--body-out`) carries the line.

### Task 4: post-mortem + CLAUDE.md line

**Files:** `docs/postmortems/2026-09-13-corporate-actions-mutated-outside-the-ledger.md`, `CLAUDE.md`
Post-mortem: what (39 CA rows superseded by an agent with no script, no ledger row), cost (invisible to status/digest; a Sunday full reconcile could revert it unseen), rule. One line under "The one contract" in CLAUDE.md pointing at `tests/test_corporate_action_store.py::test_full_reconcile_after_conversion_leaves_the_eod_fx_row_active` and the pm.

### Task 5: `djia` preset + registry row

**Files:** `presets/djia.json` (30 current DJIA tickers; take them from `ssh macmini python3 ~/market-warehouse/grok_index/pit_membership/djia/query_djia_pit.py --as-of 2026-09-12`, paste the command output into evidence), `registry/gaps.json`, `tests/test_gap_registry.py`
Registry row mirrors the equity-daily row's shape with `presets: ["djia"]`; add the test the registry contract requires.

### Task 6: `membership-sync import`

**Files:** `livewire_scripts/membership_sync.py` (new module, justified: no existing module owns membership fetch/diff), `scripts/livewire_ingest.py`, `tests/test_membership_sync.py`
**Produces:** `import_events(*, index_id: str, events_path: Path, sources: list[Path], data_lake_root: Path, now: datetime, confidence: Literal["B","C","D"]) -> dict` mapping each jsonl line (`effective_date, action, ticker, kind, year`) to `MembershipEvent` per spec §2.2: `security_id` via `SecurityMaster.resolve_symbol`, unresolved → `status="unresolved"` kept; `known_at=now`; `source_refs/source_hashes` = each source file committed to `SourceEvidenceStore` as a `HashedRef`; `status` verified for B, candidate for C/D; `revision` monotonic per security; `event_id` = sha256 of `(index_id, security_id or ticker, action, effective_date, source_hashes)`. Emits `runs` job `membership-sync` and per-index `membership_events_added`, `membership_unresolved`.
Tests use a 6-line jsonl fixture with real tickers (AAPL add 2004-01-01, AEOS add 2004-01-01, AEOS remove 2007-01-01 …) and a `SecurityMaster` on `tmp_path` with AAPL resolvable and AEOS not; assert one unresolved, statuses by confidence, and that a second import of the same file appends nothing.

### Task 7: `membership-sync` live update

**Files:** `livewire_scripts/membership_sync.py`, `tests/test_membership_sync.py`, `docs/runbook.md`
**Produces:** `sync(*, indexes: list[str], data_lake_root: Path, now: datetime, fetch_fn=None, runner=None) -> int` — `fetch_fn(index_id) -> tuple[set[str], HashedRef]` defaults to `clients.mediawiki_client` current-constituents table for `sp500`/`ndx100`/`djia` (reuse whatever `universe_client` already does for these pages; do not add a second fetcher for the same page — if `universe_client` exposes the current set, call it) and to the R2K proxy seed source used by grok for `r2k-proxy` (if that source needs a new client, report it as a deviation and skip `r2k-proxy` live sync). Diff vs `store.events(index_id, as_of=now)` replayed to a current set; append add/remove events (`known_at=now`, `announced_at=None`, status verified for B indexes, candidate for r2k-proxy). Fetch failure → `notify.page_for_lane(run_date, "membership-sync", …)` then `notify.send(...)`, exit 3. Emit `runs`, `membership_events_added`, `membership_events_removed`, `membership_unresolved`, `membership_source_fetch_ok`.
Tests: fake `fetch_fn`; one add + one remove appended; unchanged set appends nothing and still emits `membership_source_fetch_ok=1`; fetch raising → page sent through a fake runner, exit 3, `membership_source_fetch_ok=0`.

### Task 8: read command `livewire_ops.py membership`

**Files:** `scripts/livewire_ops.py`, `livewire_scripts/membership_sync.py`, `tests/test_livewire_entrypoints.py`
`membership --index <id> --effective-at YYYY-MM-DD [--as-of YYYY-MM-DD]` prints sorted symbols (resolve `security_id` back to the symbol via `security_master`; unresolved print as `?<security_id>`), one per line, count on stderr. Exit 0 always.

### Task 9: status checks + plist + template test

**Files:** `livewire_scripts/status.py`, `launchd/com.livewire.membership-sync.plist.example`, `tests/test_launchd_templates.py`, `tests/test_status.py`, `docs/runbook.md`
Checks per spec §2.4: `("Membership sync ran today", …)` (weekday only: on Sat/Sun the SQL returns OK) and `("Unresolved memberships", …)`. Plist: weekdays 01:00Z (09:00 HKT), runs `current/`, logs to `<warehouse>/logs/launchd/com.livewire.membership-sync.{stdout,stderr}.log` (same convention as the seven existing templates after PR #127). Add its entry to the template test's job table.

### Task 10: verification, local + mini read-only

Local: full gate; `LW_LEDGER_ROOT=/tmp/dfx-ledger uv run python scripts/livewire_ingest.py corporate-actions convert-dividend-currency --tickers ACR` against a temp lake seeded with one CAD dividend + one USDCAD bar (script the seed in the evidence); `ledger query` shows the three measurements; `livewire_ops.py status` shows both new checks.
Mini (read-only): `ssh macmini` — run the **dry-run** `convert-dividend-currency` from the worktree copy? No: the worktree is not on the mini. Instead run on the mini from `~/projects/livewire` after `git fetch && git checkout dividend-fx-pit` **only if** the user has approved a checkout switch — otherwise skip and report the deviation. Never `--apply` on the mini; migration (§1.7, §2.5) is an operator step after promote.
Append the acceptance table to the evidence file. Then stop; do not push. The lead reports to the user, who decides on publish/PR.
