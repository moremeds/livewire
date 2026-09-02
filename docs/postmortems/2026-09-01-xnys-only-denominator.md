# The denominator is XNYS-only, and that is a known blind spot

**Rule:** Reject any asset_class outside XNYS_CALENDAR_ASSET_CLASSES in load_registry so a new one cannot inherit the blind spot silently.

**Incident / measurement:**

- ⚠️ **The denominator is XNYS-only, and that is a known blind spot.**
  `trading_calendar.trading_dates_in_range` is the NYSE calendar, but FX trades
  ~24/5, CME futures keep their own sessions and FRED publishes on its own
  schedule — so for those a bar expected on an XNYS holiday is not expected at
  all, and its absence is invisible. `load_registry` rejects any `asset_class`
  outside `XNYS_CALENDAR_ASSET_CLASSES` so a new one cannot inherit the blind
  spot silently. Fixing it means real per-asset-class calendars.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)
