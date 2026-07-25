# Spec — repair the 2021-06-18 single-bar contamination (Option A / Yahoo)

**Date:** 2026-07-18
**Status:** approved (Option A + Yahoo source + neighbor-continuity gate + dry-run-first)
**Owner audit:** `data-lake/repairs/single-bar-audit/2026-07-18/`

## Problem

A full-universe read-only census (all 13,141 equity bronze `1d.parquet`) found one
systematic single-bar ingest corruption: **trade-date 2021-06-18, 77 symbols**, each
holding the *modern split-adjusted* price in an otherwise-raw series (ratios cluster on
clean split factors 0.2x/0.25x/0.3333x/0.5x and 2x..5x; a few wilder). Silver
back-adjusts on top → double-wrong. It survives every gate: jumps are 3×–5× (< the 6.0
continuity threshold) and 06-18 is after the 2021-06-11 seed floor. **65 of the 77 are
served wrong in silver rev-5 right now**; 2 are trimmed, 10 are quarantined (already
fail-closed). Next-worst date is 7 symbols — no other systematic date exists.

## Goal

Overwrite the single 2021-06-18 bar in bronze for the 77 symbols with the correct
**true-raw** OHLCV recovered from Yahoo, then republish silver for them. Touch nothing
else. Bronze is the system of record → repair all 77 even though only 65 are served.

## Non-goals

- Do **not** lower the 6.0 continuity threshold (would trim real small-cap moves — the
  1,536 scattered non-06-18 flags are mostly genuine).
- Do **not** touch intraday (separate raw-Massive feed; the daily bad bar never reached it).
- Do **not** repair the separate pre-2021-06-11 seed-boundary corruption here.

## Recovery method (per symbol, 2021-06-18)

Yahoo `close`/`open`/`high`/`low` are **split-adjusted**; `events.splits` carries exact
ratios. The true-raw value is deterministic:

    raw_field_t = yahoo_field_t × Π(split.price_multiplier for splits with ex_date > t)   # O/H/L/C
    raw_volume_t = yahoo_volume_t / Π(split.price_multiplier ...)                          # inverse

The multiplier product requires splits **after** 2021-06-18, so the Yahoo fetch spans
`[2021-06-17, today]` (a narrow window would omit later splits and mis-scale the raw).
Volume convention (whether Yahoo chart volume is pre/post-adjusted) is **confirmed
empirically in the dry-run** against the known-good raw neighbor volumes before any write.

## Hard gate (the only thing between a bad value and prod)

For each symbol, the recovered raw **close** must be continuous with the known-good raw
neighbors (bronze 06-17 and 06-21, already correct):

    min(prev, next) × 0.70  ≤  recovered_close  ≤  max(prev, next) × 1.43

(same band the census used to call the neighbors "consistent"). Plus OHLCV sanity:
`high ≥ max(open,close,low)`, `low ≤ min(open,close,high)`, all prices > 0, volume ≥ 0.
Any symbol failing → status `needs-review`, **no write**, listed for manual handling.
Volume uses a loose band (0.1×–10× neighbor mean) — "prices tight, volume loose".

## Write mechanics (mirror `repair_legacy_basis.py`)

- New module `livewire_scripts/repair_single_bar.py`; subcommand
  `scripts/livewire_store.py repair-single-bar` (one-line `COMMANDS` entry).
- Output dir `data-lake/repairs/single-bar-0618/<stamp>/` with:
  `backup/<enc>.1d.parquet` (verbatim pre-mutation copy, sha256 in sidecar),
  `symbols/<enc>.json` (write-ahead `in_progress` sidecar **before** mutation, then
  terminal sidecar with before/after values, `data_lake_root`, `repaired_at`),
  `cursor.json` (resume), `summary.json` (counts + review list).
- Bronze write: `BronzeClient.merge_ticker_rows(symbol, [recovered_row])` (keys by date,
  overwrites the 06-18 row only, flock + temp→validate→os.replace). Recovered row:
  `source="yahoo"`, `price_basis="raw"`, `adj_close=raw_close`, `symbol_id` preserved
  from the existing row.
- **`clients/bronze_client.py`: add `"yahoo"` to `EQUITY_SOURCES`** (honest provenance).
- Dry-run (default): recover + gate all 77, write nothing, emit the before/after table.
  `--apply` performs backup + write. Rollback = restore from `backup/` (mirror
  `rollback_legacy_basis`).

## Republish + verify

- `rebuild-silver --tickers <the repaired served symbols>` (no `--allow-window-regression`
  expected — removing a spike improves continuity, window start cannot move later; the
  dry-run `rebuild-silver --dry-run` confirms zero regressions before commit).
- Apex adopts on next 30s poll. Verify: NOW/ISRG/GE silver 06-18 now continuous with
  neighbors; `/health` still `observed==applied`, `consecutive_failures==0`.

## Tests

- `yahoo_client.get_daily_ohlcv` — mock HTTP (`responses`), assert OHLCV + splits parse.
- `repair_single_bar` — recover math (forward + reverse split fixture), continuity gate
  accept/reject, dry-run writes nothing, apply backs up then merges. Real frozen tickers.
- `bronze_client` — `"yahoo"` now an accepted source.
- Keep the 95% coverage gate green.
