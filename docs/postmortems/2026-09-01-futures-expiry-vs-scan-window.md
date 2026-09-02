# Futures expiry is judged against the scan window, not as_of

**Rule:** Judge futures contract expiry against the scan window so the same window always yields the same denominator.

**Incident / measurement:**

- **Futures expiry is judged against the scan window, not `as_of`.** Otherwise
  the same window yields a different denominator depending on when you scanned
  it: a May 2026 range scanned in August silently dropped `ES/NQ/RTY/YM_202606`,
  contracts that were live throughout it.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)
