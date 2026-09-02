# The Sunday full reconcile cancelled 507 of 1,014 yahoo-added splits

**Rule:** Scope cancellation inference to RECONCILE_PROVIDER (Massive) — absence from a Massive response says nothing about an event Massive was never asked for.

**Incident / measurement:**

#### Cancellation inference is provider-scoped

`reconcile(..., full_reconcile=True)` infers a cancellation from an event's
*absence* in the provider response. `reconcile()` only ever speaks for
`RECONCILE_PROVIDER` (Massive) — `_from_provider` hardcodes it — but the sweep
used to cancel **every** active row regardless of provider. So the Sunday
`--full-reconcile` undid the yahoo splits `apply_repairs` had added, every week:
507 of 1,014 cancelled across 2026-07-19 (418) and 2026-07-26 (89). Absence from
a Massive response says nothing about an event Massive was never asked for.


**Source:** CLAUDE.md section "Cancellation inference is provider-scoped" (moved 2026-09-02)
