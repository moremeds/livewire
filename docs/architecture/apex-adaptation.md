# Apex — Adaptation Spec

Status: Draft / proposed
Date: 2026-06-14
Part of: the four-system decoupling (livewire · apex · argon · xenon)
Sibling docs: `livewire-adaptation.md`, `argon-adaptation.md`, `xenon-adaptation.md`

## 1. Role (single responsibility)

**The TA brain.** Apex reads bars (livewire) + live ticks (xenon), computes
TA-Lib indicators + the rule-engine signal checklist + regime, and **serves TA
signals over WS/REST to argon**. It also owns backtesting. It does **not**
warehouse OHLC, does not execute orders, does not render UI.

```
  livewire ──(DuckDB bars)──┐
                            ▼
  xenon ──(WS live ticks)──▶ apex ──(WS + REST: signal_service_payload)──▶ argon
                          TA-Lib + rules + backtest
```

## 2. Current state

- **~148K LOC, 537 modules** — "Live Risk Management & Backtesting System."
  Hexagonal; the TA/backtest domain is **cleanly separable** (verified: `domain/indicators/atr.py` has no infra imports; `ta_signal_service` couples to one `domain.events.EventType` enum; backtest engines depend on `domain.backtest.*`/`domain.strategy.registry`, not infrastructure).
- **Duplicates livewire:** own `parquet_historical_store` + `ib_history_loader` + `futu_history_loader` + DuckDB coverage store.
- TA: **TA-Lib** (authoritative) + custom rule engine (`config/signals/rules.yaml`), `dual_macd`, TrendPulse, 4-regime ML.
- Backtest: **triple engine** — ApexEngine (event-driven, live/backtest parity via clock abstraction) + VectorBT + **Backtrader**.
- Serves **REST** today (`src/api/server.py`: backtest/regime/screener/strategy). Frontend is static (`worker-assets/`, CF). TUI + risk monitor + event bus.
- Toolchain: black/isort/flake8/mypy. Persistence: DuckDB/Postgres/Timescale. Signal table exists: `migrations/005_ta_signals.sql`. Payload contract exists: `config/verification/schemas/signal_service_payload.schema.json`.
- **Assessment:** the app as a whole is stale; **harvest the two cores (TA + backtest), adopt nothing else, rewrite nothing.**

## 3. Target state (how it changes)

Apex becomes a **streaming TA service** built on its harvested cores.

| Layer | Change |
|---|---|
| **Bars in** | Replace own OHLC warehouse with a `LivewireOhlcProvider` (DuckDB-over-parquet, reads livewire on SSD). Keep Futu loader **only** for markets livewire doesn't cover (HK/Asia). |
| **Live in** | New **WS client** to xenon's `ib_realtime_server.js` (ticket auth). Feed ticks into `bar_preloader`/`signal_pipeline` to form the current bar. |
| **Compute** | **Keep TA-Lib (authoritative)** + rule engine → checklist + confidence + regime. |
| **Signals out** | Persist to **Postgres/Timescale** (`005_ta_signals.sql`, own schema). Add a **WS server** (FastAPI WebSocket) + **REST pull** (`GET /signals/{ticker}?since=…`) → argon, using the existing payload schema. |
| **Compute scope** | **Subscription-driven** — compute/persist only what argon subscribes to (§3.1). |
| **Backtest** | **Modernize** (see §4). |

### 3.1 Compute model — subscription-driven (not full-universe)

Apex does **not** pre-compute TA for the 20K-symbol universe. It computes and
persists **only the set argon subscribes to** (the watchlist / open pages — tens,
maybe low hundreds of names), driven by the WS subscription:

- **subscribe(ticker):** apex seeds history from livewire (DuckDB, fast on SSD),
  opens a xenon live sub for that ticker, computes TA, begins publishing. The
  first payload carries the historical seed + indicators so argon draws the full
  chart immediately.
- **steady state:** each new tick/bar → incremental recompute → publish + persist one signal row.
- **unsubscribe (refcount → 0):** stop computing, drop the xenon sub, free resources. Retain persisted signals for a short TTL (fast re-subscribe / audit), then prune.
- **ref-counted:** many argon clients on the same ticker → compute once, fan out.

**Consequence:** the ~50–100 GB "materialize all indicators" trap **disappears** —
apex only ever computes/persists the subscribed handful, so the signal store is
**MB, not GB**. Backtests recompute from livewire bars on demand (deterministic),
so they need no pre-materialized store either.

## 4. Concrete changes

**Keep (harvest):** `domain/indicators/`, `domain/backtest/`, `domain/strategy/`,
`src/backtest/` (incl. `analysis/dual_macd/`), `application/services/ta_signal_service.py`,
`signal_pipeline/`, `config/signals/*.yaml`, `config/regime_*.yaml`, `005_ta_signals.sql`.

**Add:**
- `LivewireOhlcProvider` (DuckDB-over-parquet) behind apex's `market_data_provider` interface.
- WS **client** to xenon (ticket auth, tick→bar stitch).
- WS **server** + REST signal endpoint to argon (publish `signal_service_payload`).
- TA-Lib build step in apex's Dockerfile (C library).

**Change (modernize backtest):**
- **Drop Backtrader** (unmaintained).
- **Keep ApexEngine** (clock-abstraction live/backtest parity — the modern core) + **VectorBT** (decide OSS `vectorbt` vs maintained `vectorbtpro`; or evaluate `nautilus_trader`).
- **Unify the data layer:** livewire bars (replay) + xenon ticks (live) behind **one interface**, so strategy code can't drift between modes.
- Hot paths → Polars/DuckDB; toolchain → **ruff + pyright**; Python 3.13.

**Remove (don't carry forward):** broker adapters (IB/Futu execution), own OHLC
stores, TUI, CF frontend, risk monitor, event bus, orchestrator.

**Storage:** DuckDB for **reading bars** (on SSD); **Postgres/Timescale** for
**storing signals** (append-heavy + multi-process → not DuckDB). See §6.

## 5. Contracts

- **Consumes:** livewire (DuckDB-over-parquet bars) + xenon (WS live ticks, ticket auth) — both **only for subscribed tickers** (§3.1). argon's subscription set drives what apex pulls from each.
- **Produces:** TA signals — **WS push** + **REST pull**, `signal_service_payload.schema.json`. apex's `ta_signals` table is the **authoritative** signal record (subscribed universe only); argon keeps a read-model copy via the contract (never reads apex's tables directly).
- **Hard wall:** apex TA → argon (analysis) **only**. TA signals **never** feed xenon's order path (xenon gate: no TA trades).

## 6. Open decisions

1. **Signal store Postgres:** shared instance + own schema **only if** that instance isn't carrying xenon's order execution; otherwise apex gets its own DB. (See `xenon-adaptation.md` §6.)
2. Backtest engine end-state: ApexEngine + VectorBT vs vectorbtpro vs nautilus_trader.
3. Extraction scope: follow the domain import graph to size the exact carve (a scoping pass before code).
4. HK/Asia bars: keep Futu loader in apex, or extend livewire to warehouse them?
