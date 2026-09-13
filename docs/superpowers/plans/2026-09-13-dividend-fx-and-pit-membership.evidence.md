# Evidence — dividend-fx-pit

## Task 0

Worktree + plan/spec copy.

```
$ git fetch origin && git rev-parse origin/main
6a680c7e67fd977ee1517cfdcfb67ad018185e6b   # = merged PR #127 (notify rewrite)

$ git worktree add .worktrees/dividend-fx-pit -b dividend-fx-pit origin/main
HEAD is now at 6a680c7 notify rewrite: every email is a ledger row; ... (#127)

$ cp <main>/docs/superpowers/plans/2026-09-13-dividend-fx-and-pit-membership.md docs/superpowers/plans/
$ cp <main>/docs/superpowers/specs/2026-09-13-dividend-fx-and-pit-membership-design.md docs/superpowers/specs/
$ touch docs/superpowers/plans/2026-09-13-dividend-fx-and-pit-membership.evidence.md
$ cmp <worktree copy> <main copy>   # both files
IDENTICAL

$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2710 passed, 2 warnings in 94.59s — Total coverage: 95.04%
```

Deviations: none.

## Task 1

Store: `foreign_currency_dividends` + `apply_repairs(convert_dividends=…)`;
reconcile hardened so an active non-`massive` head is the current answer for
its `provider_event_id`.

Ground truth fetched per plan §2b (read-only, absolute `/Users/moremeds/…`
paths — `~` on the mini resolves to the ssh user `chenxi`, so the plan's
literal `~/market-warehouse` commands needed the expanded path):

```
$ ssh macmini 'cat /Users/moremeds/market-warehouse/grok_index/gaps/NOTE_dividend_currency_fx.md'
# dividend_currency_mismatch — EOD FX conversion
… "Fix by converting the foreign-currency dividend into the equity bronze
   currency using FX bronze EOD prices. The repair/evidence must record that
   the conversion used EOD FX." …

$ ssh macmini 'python3 -c "… json.load(dividend_fx_conversion_applied.json)[applied][0]"'
{"symbol":"ACR","ex_date":"2008-06-26","orig_cash":0.41,"orig_currency":"CAD",
 "converted_cash_usd":0.40517839808341516,"fx_pair":"USDCAD","fx_date":"2008-06-26",
 "fx_rate":1.0118999481201172,"fx_method":"divide",
 "source_ref":"eod_fx:USDCAD@2008-06-26 rate=1.01189995 method=divide orig=0.41 CAD -> 0.40517840 USD",
 "old_action_id":"e17a70a334daca69dfdc1064f2d37189","new_action_id":"4130da5b6d97d19fe5a3c6e1d69ac6f7"}

$ ssh macmini 'head -3 …/pit_membership/sp500/events.jsonl; sed -n 1,40p …/pit_membership/README.md'
{"effective_date":"1996-01-02","action":"add","ticker":"AAL","kind":"bootstrap"} (AAMRQ, AAPL …)
README: SPX B (fja+Wiki) 09:00 HKT, NDX B/C 09:05, DJIA B 09:15, R2K proxy D 09:10 —
"auto-update" claimed, no launchd/crontab exists (diagnosis #3 confirmed).
```

Implementation:

- `EOD_FX_PROVIDER = "eod_fx"` module constant (deliberately ≠ `RECONCILE_PROVIDER`).
- `DividendConversion` frozen dataclass: `action_id, cash_amount, currency, source_ref, source_hash`.
- `RepairResult` gains `converted`; `changed` includes it.
- `apply_repairs` gains `convert_dividends: list[DividendConversion] = ()`.
  Each conversion resolves `action_id` → superseded row; the lineage head is
  then an active `eod_fx` row with `|cash−new| < 1e-9` → skip (idempotent);
  not active → skip; else mark head `corrected`, append `eod_fx` row with
  `event_revision=head+1`, same `provider_event_id`, `source_ref`/`source_hash`
  from the conversion, `payload_hash` = blake2b of the conversion identity.
- `foreign_currency_dividends(symbol, equity_currency)`: `latest_active` rows
  with `action_type="cash_dividend"` and `currency` set ≠ equity currency.
- `reconcile` new elif: active non-massive latest row for an incoming
  `provider_event_id` counts `unchanged` — it reached that slot only by
  superseding a massive row, so a Massive response must not revert it
  (fixes diagnosis #2; comment in code).

Tests (real store on `tmp_path`, real ACR/USDCAD fixture values from the
manifest above):

```
$ uv run pytest tests/test_corporate_action_store.py -q
24 passed in 0.15s

# before the reconcile elif: test_full_reconcile_after_conversion_… fails
# (eod_fx head marked corrected, massive CAD row re-inserted). With the fix:
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2714 passed, 2 warnings in 83.53s — Total coverage: 95.04%
```

Deviations: (1) `~` in the plan's §2b commands expanded to `/Users/chenxi` on
the mini — used `/Users/moremeds/market-warehouse/…` instead; same read-only
commands otherwise. (2) `apply_repairs` has no callers outside tests, so the
new kwarg is source-compatible.

## Task 2

`corporate-actions convert-dividend-currency` — implementation lives in
`livewire_scripts/sync_corporate_actions.py` (the file stays a single CA-lane
module; the subcommand is dispatched from `main()` on `argv[0]`).

- `convert_dividend_currency(*, tickers, apply, output_dir, lake_root, now,
  fx_close_fn)`; `fx_close_fn(pair, on)` defaults to FX bronze `1d.parquet`
  (`asset_class=fx/symbol=<PAIR>`), latest `trade_date <= on`, `None` when no
  bar exists or the nearest bar is >7 calendar days old (FX week has ~5-6
  sessions — documented approximation of "5 sessions").
- Pair direction: `<eq><orig>` file exists → divide, `<orig><eq>` → multiply
  (USDCAD/divide, GBPUSD/multiply); neither → `<eq><orig>`/divide name for the
  `no_fx_bar` skip record.
- Equity currency: verified `security_master` event for the symbol, else
  `"USD"`; `equity_currency`/`equity_currency_source` recorded per manifest row.
- `source_ref` format verified byte-identical to the grok manifest:
  `eod_fx:USDCAD@2008-06-26 rate=1.01189995 method=divide orig=0.41 CAD -> 0.40517840 USD`
- `source_hash` = sha256 of `canonical_bytes(bar, default=str)`; apply commits
  the bytes to `SourceEvidenceStore` and emits one ledger `evidence` row
  (`kind='fx_bar'`) per distinct hash. Dry-run computes the hash but persists
  nothing to the store or CAS.
- Ledger: `runs` job `dividend-fx` (entry + terminal, FAILED on exception);
  measurements `dividend_currency_mismatch` (scope `all`, remaining),
  `dividend_fx_converted`, `dividend_fx_skipped`, `source='measured'` —
  emitted in dry-run too.
- Manifest `{applied: [...], skipped: [...]}` with grok field names
  (`converted_cash` per plan's `converted_cash_usd→converted_cash` rename).
- `--apply` without `--output-dir` → argparse error (SystemExit 2).

Tests (10, all on real store/tmp lakes, real ACR values):

```
$ uv run pytest tests/test_sync_corporate_actions.py -q
47 passed in 0.53s
```

End-to-end smoke (temp lake seeded with the ACR CAD div + real USDCAD bar):

```
$ MDW_DATA_LAKE=/tmp/dfx-t2/lake LW_LEDGER_ROOT=/tmp/dfx-t2/ledger \
    uv run python scripts/livewire_ingest.py corporate-actions convert-dividend-currency --tickers ACR
{"applied": [], "converted": 0, "detected": 1, "manifest": null, "remaining": 1, "skipped": [], "tickers": 1}

$ … --apply --output-dir /tmp/dfx-t2/out
{"applied": [{"converted_cash": 0.40517839808341516, "fx_pair": "USDCAD",
 "fx_method": "divide", "fx_rate": 1.0118999481201172, …}], "converted": 1,
 "detected": 1, "remaining": 0, "skipped": []}
# manifest source_ref matches grok byte-for-byte; new_action_id = the eod_fx row.
```

Full gate:

```
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2724 passed, 2 warnings in 86.18s — Total coverage: 95.04%
```

Deviations: (1) "5 sessions" staleness implemented as >7 calendar days — the
FX week is Sun–Fri so a file's own dates are the session grid; documented at
`_FX_STALE_DAYS`. (2) evidence rows/CAS commit happen on `--apply` only —
dry-run computes `source_hash` but persists nothing (persisting foreign bytes
in a dry-run would be a write side effect); `runs`/`measurements` still emit.

## Task 3

`Foreign-currency dividends` check in `status.CHECKS` (after `Stale
non-equity`): latest `dividend_currency_mismatch` row graded `value > 0 → WARN
else OK`, reported as `mismatched=<count>`; no rows → UNKNOWN via the default
empty-result path (deliberately not in `_EMPTY_IS_OK` — unmeasured is not
green). `_FIXES` points at the Task 2 dry-run command. `mismatched` added to
`_notification_key` fields so a changed count is a changed fault.

Tests (4 status + 1 digest — the digest test runs real `collect`, not a stub):

```
$ uv run pytest tests/test_status.py -k foreign_currency -q
FAILED x4 (StopIteration — no such section)         # pre-implementation FAIL
$ uv run pytest tests/test_nightly_digest.py -k body_out_carries -q
FAILED (assert '[WARN] Foreign-currency dividends' in body)
$ uv run pytest tests/test_status.py tests/test_nightly_digest.py -q
126 passed in 12.24s
```

Full gate:

```
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2729 passed, 2 warnings in 87.13s — Total coverage: 95.05%
```

Deviations: none.

## Task 4

`docs/postmortems/2026-09-13-corporate-actions-mutated-outside-the-ledger.md`
— house format (Rule / Observed / narrative / Cost / Now); the rule is spec
§3 verbatim ("entrypoint subcommand that emits `runs` + `measurements`, or it
did not happen; the ledger is what tells us when Sunday's reconcile reverts
it"), cost = invisible to status/digest and revertable by a provider-scoped
full reconcile.

`CLAUDE.md` — one line under "The one contract" after the Phase-1 read-only
bullet, pointing at
`tests/test_corporate_action_store.py::test_full_reconcile_after_conversion_leaves_the_eod_fx_row_active`
and the pm. `docs/postmortems/README.md` untouched: its index stops before
the 2026-09-12 entries (not maintained per-file).

Gate (docs-only change, still run):

```
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2729 passed, 2 warnings in 88.71s — Total coverage: 95.05%
```

Deviations: none.

## Task 5

`presets/djia.json` — 30 current DJIA constituents, name/description/source/
tickers (no `pairs`/`groups` — the registry path needs only `tickers`).

Constituents taken from the plan's probe (read-only, absolute path since `~`
resolves to `/Users/chenxi` on the mini):

```
$ ssh macmini 'python3 /Users/moremeds/market-warehouse/grok_index/pit_membership/djia/query_djia_pit.py --as-of 2026-09-12'
{"as_of": "2026-09-12", "n": 30, "tickers": ["AAPL", "AMGN", "AMZN", "AXP",
"BA", "CAT", "CRM", "CSCO", "CVX", "DIS", "GOOGL", "GS", "HD", "HON", "IBM",
"JNJ", "JPM", "KO", "MCD", "MMM", "MRK", "MSFT", "NKE", "NVDA", "PG", "SHW",
"TRV", "UNH", "V", "WMT"]}
```

Verified against a second source — Slickcharts DJIA page parsed with the same
`table.table` / `td[2]` convention `universe_client.fetch_r2k` uses (read-only
GET on the mini, livewire venv; writes nothing):

```
SLICKCHARTS 30 ['AAPL', 'AMGN', 'AMZN', 'AXP', 'BA', 'CAT', 'CRM', 'CSCO',
'CVX', 'DIS', 'GOOGL', 'GS', 'HD', 'HON', 'IBM', 'JNJ', 'JPM', 'KO', 'MCD',
'MMM', 'MRK', 'MSFT', 'NKE', 'NVDA', 'PG', 'SHW', 'TRV', 'UNH', 'V', 'WMT']
```

Sets identical. (en.wikipedia.org's DJIA article no longer carries a
components wikitable — checked, only the annual-returns table remains — so
Slickcharts is the independent citation; noted for Task 7's `fetch_djia`.)

`registry/gaps.json` — new row `g1-g2-g3-djia-daily` mirroring the
equity-daily row (`G1/G3/G14`, `equity`/`1d`, `denominator_diff`, tier A),
`universe: ["djia"]`, `since: 2026-09-13`. Its `test` points at the new
registry test below rather than the shared engine test: the row's claim is
"the djia universe exists and resolves", which is what the test proves.

`tests/test_gap_registry.py::test_djia_row_resolves_the_thirty_constituents`
— asserts the row exists, `universe == ["djia"]`, `equity`/`1d`, and the
preset resolves to 30 unique tickers.

```
$ uv run pytest tests/test_gap_registry.py -k djia -q
FAILED (registry must carry a djia equity-daily row)   # pre-implementation
$ uv run pytest tests/test_gap_registry.py tests/test_gap_registry_contract.py tests/test_coverage_denominator.py -q
24 passed in 0.09s
```

Full gate:

```
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2730 passed, 2 warnings in 84.90s — Total coverage: 95.05%
```

Deviations: none.

## Task 6 — `membership-sync import`

**Files:** `livewire_scripts/membership_sync.py` (new — no existing module owns
membership fetch/diff), `scripts/livewire_ingest.py` (COMMANDS row +
scheduled-env set), `tests/test_membership_sync.py` (new),
`tests/test_livewire_entrypoints.py` (env-load parametrization).

**Mapping, per spec §2.2:** each `events.jsonl` line → one `MembershipEvent`;
`security_id` via `SecurityMaster.resolve_symbol("massive", ticker, mic,
effective_at, now)` tried across `(XNAS, XNYS, ARCX)` — the real master keys
equities under `massive` and US constituents list on three venues, so a single
hardcoded MIC would resolve almost nothing (the mini's master today holds one
verified identity, on ARCX); two distinct identities for one ticker ⇒
ambiguity ⇒ unresolved, never a guess. Unresolved keeps the event under a
deterministic `unresolved:<ticker>` security_id with `status="unresolved"`;
`members_effective_at` never counts it. `effective_at` = `effective_date`
00:00 UTC; `announced_at=None` (the panels carry no announce date);
`known_at=now` — an `as_of` before the import returns nothing, because we did
not know it then. `source_refs`/`source_hashes` = each `--source` file's bytes
committed to `SourceEvidenceStore` CAS + one `SourceEvidence` row per artifact.
`status`: B → verified, C/D → candidate, `r2k-proxy` → candidate always.
`event_id` = sha256 of `(index_id, security_id or ticker, action,
effective_date, source_hashes)` — content-addressed, so a repeat import hits
`seen_ids` and appends nothing (the store's `AtomicParquetLog` dedup is the
backstop). `revision` continues monotonically per security from the store's
existing events.

**Ledger:** `runs` row pair job `membership-sync` (entry + terminal; FAILED on
exception, then re-raised). Measurements scoped to the index:
`membership_events_added`, `membership_events_removed`,
`membership_unresolved` — the last is the store's *current* unresolved
backlog, not the run's delta, so a no-op re-import still reports it and the
Task 9 status WARN cannot clear while the hole exists.

**Verification**

`tests/test_membership_sync.py` — 6-line fixture per the plan (AAPL + MSFT
verified in a `SecurityMaster` on tmp_path; AEOS unresolvable):

- resolution + provenance: AAPL revisions 1-3 verified; AEOS add+remove stay
  `unresolved` under `unresolved:AEOS`; every event carries the committed
  `artifact://sha256/<source sha>`; `SourceEvidenceStore.read` returns the
  exact source bytes;
- run + measurements rows land in the ledger;
- second import of the same file appends nothing (`skipped=6`) and still
  reports `unresolved=1`;
- `--confidence C` marks resolved events `candidate`;
- `main(["import", ...])` dispatches end-to-end via `MDW_DATA_LAKE`.

```
$ uv run pytest tests/test_membership_sync.py -q
ImportError: cannot import name 'membership_sync'      # pre-implementation
$ uv run pytest tests/test_membership_sync.py tests/test_livewire_entrypoints.py -q
57 passed in 0.37s
```

`livewire_ingest.py`: `membership-sync` joins the scheduled-env set — its
Task 9 plist invokes this entrypoint directly like `universe-refresh`, and the
parametrized env-load test now covers it.

Full gate:

```
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2736 passed, 2 warnings in 84.21s — Total coverage: 95.04%
```

Deviations: `membership_unresolved` measures the standing store backlog rather
than the run's new-unresolved delta (the plan does not pin the definition;
backlog semantics is what the §2.4 status check needs to stay honest).
Runbook text deferred to Task 7, which owns the `membership-sync` doc block.

## Task 7 — `membership-sync` live update

**Files:** `livewire_scripts/membership_sync.py` (`sync` + `_default_fetch` +
`_current_members` + CLI sync parser), `clients/universe_client.py`
(`fetch_djia` + `DJIA_WIKIPEDIA_TITLE`, `_R2K_URL` promoted to public
`R2K_SLICKCHARTS_URL`), `tests/test_membership_sync.py` (+7 tests),
`docs/runbook.md` (`membership-sync` block under §9 scheduling).

**Design:** `sync(*, indexes, data_lake_root, now, fetch_fn=None,
runner=None, dry_run=False) -> int`. Per index: fetch the live source with
evidence, replay all non-superseded non-`rejected` events to the current
member set (verified + candidate + `unresolved:` placeholders all count — a
placeholder member that stayed unresolved must not be re-added every run),
diff, append add/remove events (`effective_at=known_at=now`,
`announced_at=None`, `event_id` = content hash incl. the day's source hash),
emit per-index measurements, print one JSON summary line per index.

**Sources.** `sp500`/`ndx100`/`djia` → `MediaWikiClient.snapshot(title)` +
shared `parse_constituent_table` (the exact seam `universe_client` uses;
`snapshot.evidence` is the event's `HashedRef`). `r2k-proxy` →
`universe_client.fetch_r2k` (Slickcharts) with grok's own guardrail from
`r2k_proxy_live/update_from_live.py`: a fetched set under 1500 members is a
fetch failure, not a diff (a broken parse would otherwise emit ~1,500 false
removes). Its evidence artifact is the canonical sorted fetched set committed
to the CAS with `source_url` = the Slickcharts page. No preset fallback —
grok's `update_from_live.py` silently substituted `presets/r2k.json` when the
source died, which is the invisible-write pattern this plan removes; a dead
source pages instead.

**Fail closed.** Any per-index fetch error (network, missing table, below
floor, unknown index) → `membership_source_fetch_ok=0` for that index,
processing continues for the rest; after the loop one `page_for_lane` +
`notify.send(runner=runner)`, terminal `runs` row `FAILED`/`exit_code=3`,
return 3. Adds/removes are ledger events and digest lines, never pages.

**djia fails closed today, by design:** the Wikipedia article no longer
carries a components wikitable (verified Task 5; `parse_constituent_table`
raises → `UniverseFetchError` → fetch_ok=0 + page). `fetch_djia` is
implemented in `universe_client` pointing at the Wikipedia title — no
alternative source was wired in.

**Verification** — 7 new tests:

- fake `fetch_fn` adds `unresolved:ORCL` + removes verified MSFT
  (`has_verified_identity` satisfied by the seeded master);
- unchanged set appends nothing, still emits `membership_source_fetch_ok=1`;
- fetch raising → one page through a fake runner, exit 3, `fetch_ok=0`,
  terminal run `FAILED`/`3`, `executions(script='notify')` receipt row;
- djia fail-closed: fake `MediaWikiClient` returns a snapshot whose content
  has no constituents table → exit 3, page sent, `events("djia")` stays
  empty, `fetch_ok=0`;
- r2k-proxy below-floor fetch → exit 3 + `fetch_ok=0`, nothing appended;
- r2k-proxy success → resolved AAPL is `candidate`, unresolved are
  `unresolved:ZZ*`;
- CLI: `--index sp500 --dry-run` appends nothing; apply appends.

```
$ uv run pytest tests/test_membership_sync.py tests/test_universe_client.py -q
32 passed in 0.56s
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2743 passed, 2 warnings in 84.70s — Total coverage: 95.03%
```

**Deviations:**

1. `clients/universe_client.py` is outside the plan's Task-7 file list — it is
   the fetchers' home, so `fetch_djia`/`DJIA_WIKIPEDIA_TITLE` live there and
   `_R2K_URL` was promoted to `R2K_SLICKCHARTS_URL` for the evidence
   `source_url` (rather than importing a private name).
2. **Proposed djia alternative source — decision needed:** Slickcharts
   `https://www.slickcharts.com/dowjones` (verified identical to the grok DJIA
   list in Task 5; same `table.table`/`td[2]` parser family as `fetch_r2k`).
   Not implemented — `fetch_djia` stays on Wikipedia and fails closed until
   decided.
3. `sync()` gained `dry_run` kwarg (plan signature lists
   `fetch_fn`/`runner` only) — spec §2.3 shows `--dry-run`; dry runs emit
   `membership_source_fetch_ok` + `membership_unresolved` + appended counts
   (zero) and print the planned diff.
4. r2k-proxy drops grok's preset fallback: a dead Slickcharts fetch pages
   instead of silently diffing against `presets/r2k.json`.

## Task 7 fix — djia source switched to Slickcharts (approved)

Deviation (2) from the Task 7 report was approved: `fetch_djia` now reads
`https://www.slickcharts.com/dowjones` — the source verified identical to the
grok DJIA list in Task 5 — through the same parser family as `fetch_r2k`.
Deviations (1), (3), (4) accepted.

**Files:** `clients/universe_client.py` (`DJIA_SLICKCHARTS_URL` beside
`R2K_SLICKCHARTS_URL`; `DJIA_WIKIPEDIA_TITLE` and the `_R2K_URL` back-compat
alias removed; shared `_slickcharts_constituents(tree, label)` —
`table.table` / `tbody tr` / symbol in `td[2]` — now used by `fetch_r2k` and
`fetch_djia`; `fetch_djia` fails closed below 30 constituents),
`livewire_scripts/membership_sync.py` (`djia` moved to the Slickcharts
branch sharing `_slickcharts_ref` with `r2k-proxy`; evidence `source_url` =
the Slickcharts page), `docs/runbook.md`, spec §2.3 sentence, both test
files.

**Fail-closed kept:** no `table.table` → `UniverseFetchError`; fewer than 30
parsed constituents → `UniverseFetchError` (partial markup must not emit
~30 removes); HTTP error propagates as fetch failure — all three land on the
existing `fetch_ok=0` + page + exit-3 path.

**Verification** — 9 new/changed tests:

- `TestFetchDJIA` in `test_universe_client.py`: valid 30-row table →
  30-ticker set; HTTP 403 → `UniverseFetchError`; missing table →
  `UniverseFetchError`; 2-row table → "below 30" `UniverseFetchError`;
- `test_membership_sync.py`: djia fetch failure → exit 3 + page + no events
  + `fetch_ok=0`; djia success at exactly 30 → 30 events appended (seeded
  AAPL resolved `verified`, 29 `unresolved:ZZ*`), canonical sorted-set
  evidence with `source_url="https://www.slickcharts.com/dowjones"`;
- plus margin tests added when the first full run sat at exactly 95.00%:
  import blank-line skip + FAILED run on error, evidence verifier rejecting
  an absent artifact, Wikipedia `_default_fetch` happy path via a fake
  `MediaWikiClient`, sync's FAILED-run re-raise on a processing error.

```
$ uv run pytest tests/test_membership_sync.py tests/test_universe_client.py -q
41 passed in 0.65s
$ uv run ruff format ... && uv run ruff check ...
4 files left unchanged; All checks passed!
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2752 passed, 2 warnings in 84.74s — Total coverage: 95.09%
```
