# Xenon — Adaptation Spec

Status: Draft / proposed
Date: 2026-06-14
Part of: the four-system decoupling (livewire · apex · argon · xenon)
Sibling docs: `livewire-adaptation.md`, `apex-adaptation.md`, `argon-adaptation.md`

## 1. Role (single responsibility)

**Feed + execution.** Xenon is the live IB tick feed and the options **execution
center** (order placement, combos, reconciliation). It is **flow-only and
explicitly anti-TA** ("No narrative trades. No TA trades. Flow signal or
nothing."). In this integration it is mostly a **provider**: it streams live
ticks that apex consumes. It is the **smallest change** of the four.

```
  xenon ──(WS live IB ticks, ticket auth)──▶ apex
 ib_realtime_server.js
```

## 2. Current state

- **Python execution core** (`src/xenon/execution/`: `ib_place_order`, `ib_execute`, `ib_order_manage`, combo wizard, `naked_short_audit`, reconcile, preflight, Kelly sizing) + Next.js 16 frontend + Postgres (Alembic) + Docker. Runs in the trading-stack (`bounce_ibc_xenon.sh`).
- **Live feed already exists:** `scripts/infra/ib_realtime/ib_realtime_server.js` — a Node **WS server** streaming IB ticks (`ib_tick_handler.js`, `ib_connection_status.js`).
- **WS auth:** `src/xenon/api/ws_ticket.py` — `POST /ws-ticket` (JWT) → UUID ticket (30 s TTL, single-use) → connect `?ticket=<UUID>`. Currently **user-JWT** based; the frontend resolves the URL via `/api/ib/ws-config`.
- Four gates: Convexity, Edge, Risk (Kelly), No Naked Shorts.

## 3. Target state (how it changes)

Xenon keeps doing exactly what it does; it just becomes a **first-class WS
provider for apex** with a service-to-service auth path.

| Change | Detail |
|---|---|
| **Service-identity WS auth** | apex needs to mint a ticket as a **service** (service JWT → `/ws-ticket`), or use a dedicated **internal trusted-network** WS path. Today's ticket flow is user-JWT. |
| **Documented tick contract** | Publish the WS tick schema (fields, cadence) so apex's client binds to a stable contract, not the JS internals. |
| **Demand-driven subs** | apex subscribes to xenon ticks **only for tickers argon is watching** (subscription chain: argon → apex → xenon). Xenon's live feed load stays bounded to the active watchlist, not the universe. |
| **Keep the wall** | apex's TA signals **never** enter xenon's order path — enforced by xenon's "no TA trades" gate. apex → argon only. |
| **Postgres isolation** | If xenon's **order execution** runs on the Postgres "shared by xenon + argon," apex's signal writes must **not** land there (order-fill latency must not contend with analytics writes) — apex gets its own DB. |

## 4. Concrete changes

- **Add:** a service-to-service auth path for apex (service JWT or internal WS), and a published tick-schema doc.
- **Change:** possibly expose the `ib_realtime` WS on the internal network for apex (in addition to the browser path).
- **Remove:** nothing.

## 5. Contracts

- **Produces:** live IB ticks — **WS, ticket auth** — consumed by apex (`apex-adaptation.md` §3).
- **Consumes:** nothing from the other three. (It must **not** consume apex TA — gate.)

## 6. Open decisions

1. **Service auth:** service-JWT ticket vs trusted-network internal WS for apex.
2. **The pivotal fact:** does xenon's **order execution** run on the Postgres shared with argon? → if yes, apex signals get their **own** DB (decides `apex-adaptation.md` §6.1). *Needs confirmation.*
3. Tick contract: which fields/cadence apex needs (last/bid/ask, size, timestamp) — define the schema.
