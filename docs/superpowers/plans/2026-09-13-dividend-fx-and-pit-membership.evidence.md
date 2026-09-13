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
