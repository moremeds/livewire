# Task 3 — IB energy futures feasibility via existing GC/CL path

User authorization, 2026-09-23: contact Livewire to check whether RBOB, HO and related energy instruments can be obtained from Interactive Brokers similarly to GC. Existing worker `gold-coverage-scout`, SWE-2 Max, owns this bounded investigation in the Livewire price-discovery worktree. No standalone Livewire lead pane was present in the current Herd inventory; use this existing Livewire worker, not an unrelated Apex session.

Finish Task 2 report corrections first and retain that separate result. Then inspect the actual GC/CL qualification, historical ingestion and nightly-preset code; use existing read-only diagnostics where possible. No further delegation.

## Questions to resolve

For RB, HO and NG, discover the actual IB contract identity rather than assuming exchange ticker equals IB localSymbol. Record qualified conId, symbol/localSymbol, exchange, currency, trading class, multiplier, delivery month and actual last-trade metadata. Contrast with one existing GC/CL control only if needed. Inspect BZ staleness from existing read-only logs/config; do not infer that a current preset contract expired.

Separate contract qualification, current subscription/permission, historical query success, earliest available timestamp, bar frequency support and production ingestion support. A qualified contract does not prove data entitlement; successful recent bars do not prove full history. Probe one unexpired contract per root, serially: head timestamp if supported, one small daily history response, and an optional short intraday response if the existing client supports it. Discover available months from contract details; do not guess a stale preset expiry. Record exact method/parameters, capture time, non-secret error codes/messages, row count and range. Distinguish returned OHLC close from verified settlement; never silently turn absent open interest into measured zero.

Conclude per root: works with current stack/config; supported by IB but needs a bounded preset/code change; or blocked/unverified with the exact reason. Name the minimum follow-up to maintain aligned RB/HO/CL legs for crack research, including month roll and retention needs. Do not implement or compute cracks here.

## Scope and safety

- Read current shared/project rules and GC/CL callers before probing.
- Own only `docs/audits/price-discovery/IB_ENERGY_FEASIBILITY.md` and `evidence/ib-energy/` in this worktree; preserve Task 2 artifacts and all other changes.
- Existing configured IB connection may be used for read-only contract/head/history requests only. Inspect existing connection/client-id coordination first, allocate a non-conflicting diagnostic client id through the established mechanism, and disconnect in `finally`. Never reuse or evict a running client's id.
- No orders, account changes, new subscriptions, market-data purchases, streaming/bulk requests, production/lake/DB writes, Gateway/service restart, VPN/config/.env edits, installs, or ingestion/backfill execution. Do not expose credentials in commands or evidence. No alternate credentials or connection bypass if existing access fails.
- Read-only mini commands and small IB requests explicitly permitted by this task; serialized probes only, respect existing pacing/error handling and stop on pacing/permission failures rather than looping. Persist evidence locally before declaring done.
- Rule 5: no stage, commit, push, PR, merge or deploy; report `commit none`.
- Rule 6: stop after Task 3 and return `herd-report gold-coverage-scout task 3: commit none, evidence <path>, deviations: <text|none>`. Reverse IPC remains unavailable; save disk report plus concise terminal summary.
