# ~90% of the equity universe is price_basis='unknown'

**Rule:** Treat any new split against the legacy `unknown` population as a live quarantine risk, not a hypothetical.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

⚠️ **~90% of the equity universe is `price_basis='unknown'`** (`source='legacy'`).
Those symbols stage today only because they have no splits — `build_factor_intervals`
raises `unknown price_basis for split-affected row` the moment a split touches one,
and the symbol is quarantined and evicted. INTC is exactly this shape (`unknown` ×
11,676 rows). Any new split against that population converts a clean symbol into a
quarantined one, so this is the standing threat to "newly added data is always
silver grade" — not a hypothetical.

**Source:** CLAUDE.md section "IB BarData → Bronze mapping" (moved 2026-09-02)
