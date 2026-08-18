# Argon — Adaptation Spec

Status: Draft / proposed
Date: 2026-06-14
Part of: the four-system decoupling (livewire · apex · argon · xenon)
Sibling docs: `livewire-adaptation.md`, `apex-adaptation.md`, `xenon-adaptation.md`

## 1. Role (single responsibility)

**The face.** Argon is the options analytics cockpit + UI + AI thesis. For TA it
is a **thin renderer**: it consumes apex's prepared signals and draws them. It
does **not** compute TA, does not warehouse bars, does not execute.

```
  apex ──(WS push + REST pull: signal_service_payload)──▶ argon
                                                        consume → persist read-model → render
```

## 2. Current state

- **Next.js 16 + FastAPI + Postgres** (`option_wizard.uw_scan`, role `argon_app`).
- Options-centric: dealer gamma, IV surface, dark-pool flow, AI thesis (Codex+Claude+DeepSeek), regime, gold, **Index Cockpit** (`/cockpit/<TKR>` for SPX/SPY/QQQ/IWM, `?asof=` snapshots).
- OHLC: **daily only** (`OhlcProvider` → `MassiveOhlcProvider` → `daily_ohlc`). **No intraday bar history.**
- Live spot: **Massive WS** pipeline already built — `massive_ws.py` → `ws_tick_buffer.py` (in-mem) → `ws_db_writer.py` (persist). This is the exact consume-WS→persist→render pattern apex's signals will reuse.
- Charts: **d3**. Disciplines: #1 Persistence (everything → Postgres), #3 Source priority IB→UW→FMP→massive (Yahoo banned).

## 3. Target state (how it changes)

Argon gains a **TA signal surface** fed entirely by apex — no new TA logic.

| Change | Detail |
|---|---|
| **Drive the subscription set** | Argon's **watchlist + open pages define what apex computes.** Subscribe(ticker) when a page opens / a name is watched; unsubscribe when the last viewer leaves. Apex computes/persists **only** this set (see `apex-adaptation.md` §3.1) — never the 20K universe. |
| **Consume apex TA signals** | Subscribe to apex's **WS** (same machinery as the existing Massive WS consumer) + **REST** backfill on page load / reconnect / `?asof=`. |
| **Persist a read-model** | Write received signals into argon's own Postgres (Discipline #1) — a **cache** of apex's authoritative `ta_signals`, populated via the contract, **never** by querying apex's schema. |
| **Render** | New TA chart surface (the image-1 MA/slope + image-4 checklist), drawn with d3 from apex's render-ready payload. Likely under the Cockpit or a new route. |
| **Stop reinventing TA** | apex prepares the complete payload (bars + indicators + checklist + score); argon only renders. |

## 4. Concrete changes

- **Add:** an apex-signal WS consumer (clone the `ws_db_writer`/`ws_tick_buffer` shape, new producer = apex), a **subscribe/unsubscribe** protocol tied to watchlist/page lifecycle, a REST backfill client, a `ta_signals` **read-model** table/migration, and the TA chart components (d3).
- **Change:** the TA chart sources its bars+indicators from **apex's payload**, not from argon's own `daily_ohlc`/Massive pull.
- **Remove:** nothing — argon keeps its options/flow/gold/AI surfaces and its own Massive spot feed (raw ticks) alongside apex's derived signals.

## 5. Contracts

- **Consumes:** apex — **WS push** + **REST pull**, `signal_service_payload.schema.json`. Reads via the contract only; apex's tables stay private.
- **Produces:** nothing new for the other three (argon is a terminal/UI). Its read-model copy is internal.

## 6. Open decisions

1. **TA chart home:** extend the Index Cockpit, or a dedicated route?
2. **Raw vs derived:** keep argon's own Massive spot WS (raw ticks) running beside apex's TA-signal WS (derived), or consolidate on apex? (Recommended: keep both — different layers.)
3. **xenon/argon overlap (accepted):** the line is argon = analysis/UI, xenon = execution/feed. Where options-*flow analytics* lives between them remains the one blurry seam — out of scope for this integration, noted for later.
