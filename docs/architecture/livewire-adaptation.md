# Livewire — Adaptation Spec

Status: Draft / proposed
Date: 2026-06-14
Part of: the four-system decoupling (livewire · apex · argon · xenon)
Sibling docs: `apex-adaptation.md`, `argon-adaptation.md`, `xenon-adaptation.md`

## 1. Role (single responsibility)

**Data plane.** Livewire is the historical OHLC **system of record** — the single
source of truth for price bars. It does **not** compute TA, does not stream live
ticks, does not execute. It ingests, cleans, stores, and serves bars.

```
  livewire ──── historical OHLC bars (DuckDB-over-parquet) ────┐
 (data plane)                                                  ▼
  xenon ──────── live IB ticks (WS) ─────────────────────────▶ apex ──(WS)──▶ argon
 (feed+exec)                                                  (TA brain)      (UI)
```

## 2. Current state

- Per-ticker Parquet, zstd-3, Hive-partitioned (`asset_class=equity/symbol=AAPL/{1d,1m,5m,30m,1h}.parquet`).
- **41 GB hot bronze = the full US equity universe** (1d 0.77 + 1m 25.66 + 5m 9.96 + 30m 2.89 + 1h 1.92 GB; 20,104 symbols intraday / 20,382 daily). Excludes futures/vol/rates (separate, small) and the multi-TB raw Massive staging. On a **14 TB exFAT/fskit USB HDD** — the same flaky ASMedia enclosure that **dropped off the bus twice** this week.
- **apex reads only subscribed tickers** (tens, not 20K) on demand for the historical seed — a tiny read load. SSD is still recommended for *availability* (no enclosure drops on a live service's path), not throughput.
- Optional Postgres `md.*` analytical publish (rebuilt from bronze).
- IB + Massive ingestion; T+1 freshness via nightly catch-up.

## 3. Target state (how it changes)

Livewire stays what it is — but becomes a **clean read source** for apex and
moves its hot path off the failing hardware.

| Change | Why |
|---|---|
| **Move the 41 GB hot bronze to the internal SSD (APFS)** | Take the flaky enclosure off the live read path (apex is a long-running consumer); kill the seek penalty. Raw staging stays on HDD. |
| **Expose bars via a stable DuckDB-over-parquet read contract** | apex reads parquet directly; the per-ticker file layout is the contract. No bespoke API needed. |
| **Reserve `gold/` for price-derived *data-plane* tables only** | Adjusted returns, factor tables, universe membership — **never TA/signals** (those are apex's, see `apex-adaptation.md`). |
| **(Optional) intraday multi-file migration** | Removes the per-ticker rewrite storm (~35 h / 59% of intraday build) — reliability + speed for the data plane. See `docs/superpowers/specs/2026-06-13-intraday-multifile-migration-design.md`. |

## 4. Concrete changes

- **Add:** nothing structurally new for serving — document the canonical parquet
  layout as the read contract; optionally a thin read helper (`read_symbol_rows`
  equivalent) apex can vendor, or just direct DuckDB.
- **Change:** hot bronze location → SSD/APFS. Keep exFAT HDD copy only if Sift /
  cross-platform still needs it (cold/portable tier).
- **Remove:** nothing. Livewire keeps its full ingestion role.

## 5. Contracts

- **Owns / produces:** the bronze parquet layout (read by apex via DuckDB).
- **Consumes:** nothing from the other three. Pure source.

## 6. Open decisions

1. SSD migration of the 41 GB hot bronze (recommended; ~free given 82 GB SSD free).
2. exFAT → APFS for the hot tier (faster/robust on macOS; only exFAT-justification is cross-platform Sift).
3. Whether to ship the intraday multi-file migration now or after the integration.
4. Does apex read parquet directly, or do we publish the watchlist subset to Postgres `md.*` for it? (Direct DuckDB recommended — zero coupling.)
