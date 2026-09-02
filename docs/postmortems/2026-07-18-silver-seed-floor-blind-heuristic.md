# The blind heuristic missed 63 double-adjusted symbols; the seed floor measures instead

**Rule:** Classify the 2021-06-11→21 seed boundary deterministically against the corporate-action store, and trim a seed-corrupt symbol to its post-seed window rather than quarantining it.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

1. **The seed floor** — `clients/seed_boundary.classify_seed_boundary`, applied to
   **raw bronze before adjustment**. Deterministic: it looks at a known location
   (the 2021-06-11→21 bulk-seed window) and compares the observed step against the
   fold *predicted* from the corporate-action store. No threshold to tune. This is
   the only detector that sees the **2×–5× class** — the blind heuristic missed 63
   such symbols (APH, TSLA, GE, WMT, CSX, SOXX…), which classified `clean` while
   their pre-seed history was double-adjusted. It **measures rather than assumes**:
   KLAC/COO have a predicted fold but a flat boundary and stay clean. A
   seed-corrupt symbol is **trimmed to its post-seed window, not quarantined** —
   its ~5 years of post-2021-06 history are perfectly good.

**Source:** CLAUDE.md section "The silver-grade window" (moved 2026-09-02)
