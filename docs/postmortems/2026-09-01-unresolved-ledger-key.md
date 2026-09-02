# Keyed on symbol alone, marking 1d unresolved also silenced 1h

**Rule:** Key the unresolved ledger on (symbol, asset_class, timeframe, session) and reject an entry missing any of them.

**Incident / measurement:**

- **The unresolved ledger is keyed on `(symbol, asset_class, timeframe,
  session)`.** Keyed on symbol alone, marking `1d` unresolved also silenced
  `1h`. An entry missing those fields is rejected, never defaulted — defaulting
  recreates the over-broad suppression. Writes take an `fcntl.flock`.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)
