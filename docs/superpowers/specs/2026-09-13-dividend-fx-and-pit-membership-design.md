# Dividend FX conversion and PIT index membership — design

**Date** 2026-09-13 · **Status** design, unimplemented · **Overlays**
`2026-09-02-livewire-ledger-design.md`, `2026-08-31-livewire-gap-autoheal-design.md`

This spec brings two pieces of work done outside the pipeline on 2026-09-12/13
(the `grok_index/` layer on the mini) back inside it. It changes **how those
two writes happen and where their facts go**. Registry, denominator, gap
classes, and the ledger tables are unchanged.

---

## 0. Diagnosis

Facts, all verified on the mini (`ssh macmini`, 2026-09-13):

| #   | Observation                                                                                                                                                                                                                                                                                                                 | Rule it breaks                                                                                                                                                                |
| --- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `grok_index/gaps/dividend_fx_conversion_applied.json` records 39 dividend rows superseded in the corporate-action store (CAD/GBP/… → USD via FX EOD). No script under `grok_index/` performs that write; `executions` has no row for it; `runs` has no job for it.                                                          | "Nothing outside livewire cron ever writes the lake; an agent produces evidence and queue entries, never rows." — "Every job writes its facts to the ledger."                 |
| 2   | The converted rows carry `source_ref = "eod_fx:USDCAD@2008-06-26 rate=… orig=0.41 CAD -> 0.40517840 USD"`. Whether `provider` was changed is not recorded. If it still reads `massive`, the next Sunday `--full-reconcile` sees the same `provider_event_id` with a different `payload_hash` and supersedes it back to CAD. | provider-scoped reconcile (`clients/corporate_action_store.py:248-274`)                                                                                                       |
| 3   | `grok_index/pit_membership/{sp500,ndx100,djia,r2k_proxy_live}/events.jsonl` hold ~30 years of add/remove events keyed by **ticker** with no `security_id`, no evidence hash, no `known_at`. README claims weekday 09:00–09:15 HKT auto-updates; no launchd job or crontab exists on the mini.                               | `IndexMembershipStore` requires `security_id`, `source_refs`, `known_at` (`clients/index_membership_store.py:44-56`). Rule 9: a detector with no output is dead, not healthy. |

Both are the same defect the 09-02 spec named: a write with no ledger row is
invisible to `status`, the digest, and the watchdog, so nobody finds out when
it stops or when it is reverted.

## 1. Dividend FX conversion — a repair, not a one-off

### 1.1 Command

One subcommand on the existing entrypoint, no new script:

```
python scripts/livewire_ingest.py corporate-actions convert-dividend-currency
    [--tickers …] [--apply] [--output-dir <path>]
```

Dry-run by default. `--apply` requires `--output-dir` (the manifest is the
audit trail; the JSON grok wrote by hand becomes this command's output).

### 1.2 Detection

`CorporateActionStore.foreign_currency_dividends(symbol, equity_currency) ->
list[CorporateAction]`: active dividends whose `currency` is set and differs
from the bronze equity currency (from `security_master`; fall back to `USD`
with the fallback recorded in the manifest). No new detector script; this is
one store method and one test.

### 1.3 Conversion

For each row: FX pair from `(currency, equity_currency)`, rate = FX bronze
`1d` close on `ex_date` (previous session if the ex-date is an FX holiday;
the date actually used is recorded). Missing FX bar → row is **skipped with
reason**, never estimated.

### 1.4 Write path

Extend `apply_repairs` (`corporate_action_store.py:283`) with
`convert_dividends: list[DividendConversion]`. Each conversion appends one
row with the store's existing lineage convention:

| field                     | value                                                                                                                                     |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `supersedes_action_id`    | old `action_id`; old row rewritten `status="corrected"`                                                                                   |
| `event_revision`          | old + 1                                                                                                                                   |
| `provider`                | **`eod_fx`** — not the original provider, so `RECONCILE_PROVIDER="massive"` cancellation and re-supersession skip it (fixes diagnosis #2) |
| `provider_event_id`       | old `provider_event_id` (lineage stays queryable)                                                                                         |
| `cash_amount`, `currency` | converted amount, equity currency                                                                                                         |
| `source_ref`              | `eod_fx:<PAIR>@<fx_date> rate=<r> method=<divide                                                                                          | multiply> orig=<amt> <CCY>` |
| `source_hash`             | sha256 of the FX bronze parquet row bytes used, committed to the evidence CAS as a `HashedRef` (`shepherd_repair.py:46`)                  |

Required test (the twin of pm:2026-07-19-cancellation-inference-provider-scoped):
a `reconcile(full_reconcile=True)` run **after** a conversion, fed the original
CAD event again, leaves the `eod_fx` row active and appends nothing.

### 1.5 Ledger

| table          | rows                                                                                                                                                                       |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `runs`         | `job="dividend-fx"`, one per invocation, `verdict` OK/FAILED                                                                                                               |
| `measurements` | `dividend_currency_mismatch` (scope=all, remaining count after apply), `dividend_fx_converted` (applied count), `dividend_fx_skipped` (no FX bar), all `source="measured"` |
| `evidence`     | one row per `HashedRef` committed                                                                                                                                          |

`status.CHECKS` gains `("Foreign-currency dividends", …)`: latest
`dividend_currency_mismatch` value > 0 → WARN, zero rows → UNKNOWN (not in
`_EMPTY_IS_OK`). The digest renders it because the digest renders `collect()`.

### 1.6 Silver

No manual `rebuild-silver`. The nightly silver lane rebuilds from the CA
store; the converted amount reaches silver on the next run. Manual rebuilds
were the second half of what left the ledger blind.

### 1.7 Migration of the 39 rows

`convert-dividend-currency --apply` is idempotent: a symbol whose active
dividend already has `provider="eod_fx"` and the same converted amount is a
no-op that still emits a measurement. Run it once on the mini over the index
residual set; the manifest diff against
`grok_index/gaps/dividend_fx_conversion_applied.json` is the acceptance check
(39 rows same amount within 1e-9, 2 skipped with the same reasons). Rows that
turn out to carry `provider="massive"` are re-superseded to `eod_fx` by the
same run.

## 2. PIT index membership — into the store, with a job

### 2.1 What is kept from the grok layer

The event data and its sources (fja CSV, shardul YAML, Wikipedia HTML
snapshots). Nothing else: the four `update_*.py`, four `query_*_pit.py` and
the per-panel JSON status files are replaced by one command and the ledger.

### 2.2 Identity and evidence

Each `events.jsonl` line becomes a `MembershipEvent`:

| field                         | from                                                                                                                                                              |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `index_id`                    | preset name: `sp500`, `ndx100`, `djia` (new preset, 30 names), `r2k-proxy`                                                                                        |
| `security_id`                 | `SecurityMaster.resolve_symbol(provider, ticker, mic, effective_at, as_of)`; unresolved → `status="unresolved"`, kept, counted                                    |
| `effective_at`                | `effective_date` 00:00 UTC                                                                                                                                        |
| `announced_at`                | source date when present, else `None`                                                                                                                             |
| `known_at`                    | **import time** for bootstrap history; fetch time for live updates. PIT is honest: an `as_of` before the import returns nothing, because we did not know it then. |
| `source_refs`/`source_hashes` | the source file committed to the evidence CAS (`HashedRef`)                                                                                                       |
| `status`                      | confidence B → `verified`; C → `candidate`; R2K proxy → `candidate` always (non-official)                                                                         |

`members_effective_at` (`index_membership_store.py:86`) already counts only
`verified`, so the C/D panels are queryable via `events(as_of=…)` but never
feed the denominator. `djia` is added to `registry/gaps.json` as a normal row
with a test; silver coverage for the 30 names already exists.

### 2.3 Command and schedule

```
python scripts/livewire_ingest.py membership-sync [--index …] [--dry-run]
python scripts/livewire_ingest.py membership-sync import --index <id> --events <jsonl> --source <file>…   # one-time
python scripts/livewire_ops.py membership --index sp500 --effective-at D [--as-of D]                      # read
```

`membership-sync` fetches each index's live source (Wikipedia current table
for `sp500`/`ndx100`; Slickcharts for `djia` and the R2K proxy's live seed),
diffs against `events(as_of=now)`, appends
add/remove events, and pages through `notify.page_for_lane(run_date,
"membership-sync", …)` only on **fetch failure**. Adds/removes are digest
lines, not pages.

Schedule: one launchd job `com.livewire.membership-sync`, weekdays 01:00Z
(09:00 HKT), running `current/` like every lake-writing job, logging to
`logs/launchd/`. Entry added to `tests/test_launchd_templates.py`. This is
the 8th job; the four grok per-panel timers collapse into it.

### 2.4 Ledger

| table          | rows                                                                                                                                            |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `runs`         | `job="membership-sync"`                                                                                                                         |
| `measurements` | per index (scope=index_id): `membership_events_added`, `membership_events_removed`, `membership_unresolved`, `membership_source_fetch_ok` (0/1) |

`status.CHECKS` gains `("Membership sync ran today", …)` — weekday, `runs`
row with `verdict<>'FAILED'` → OK, missing → UNKNOWN, FAILED → BAD; and
`("Unresolved memberships", …)` — latest `membership_unresolved` > 0 → WARN.

### 2.5 Migration

`membership-sync import` once per panel on the mini, from the release. The
acceptance check is `livewire_ops.py membership --index sp500 --effective-at
2026-09-12` equal to `query_sp500_pit.py --as-of 2026-09-12` (same for
ndx100, djia, r2k-proxy), plus `membership_unresolved` reported per index.
`grok_index/pit_membership/` is then read-only history; its README says so.

## 3. Post-mortem

One file, `docs/postmortems/2026-09-13-corporate-actions-mutated-outside-the-ledger.md`,
and one line in `CLAUDE.md` under "The one contract": an agent-driven repair
goes through an entrypoint subcommand that emits `runs` + `measurements`, or
it did not happen; the ledger is what tells us when Sunday's reconcile
reverts it.

## 4. Out of scope

Paid Russell/Norgate membership; NDX 1996–2003; DJIA pre-1991 eventization;
the ~76 pre-2021 silver residuals; WMT history. Unchanged from the grok
summary.
