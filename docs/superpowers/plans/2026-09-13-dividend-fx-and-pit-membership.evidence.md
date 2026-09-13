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
