# FX and DXY provider floors — Yahoo owns the asset class

**Rule:** Source FX per (symbol, timeframe) from the measured table, probe entitlement boundaries with GET rather than LIST, and never replace an accumulated intraday fx file.

**Incident / measurement:**

Measured 2026-07-27 — re-measure before trusting, the entitlement floors roll:

- **DXY exists only on Yahoo.** IB's `IND DX @NYBOT` returns error 162 (no
  permission); Massive returns 0 rows for `I:DXY`/`C:DXY`/`I:USDX`. Yahoo
  `DX-Y.NYB` daily reaches **1971-01-04**. Yahoo returns 17,219 timestamps but only
  **14,108** carry prices — the rest are null holiday padding and are skipped, never
  back-filled. 14,108 over 55.6 years is 253.9/year, i.e. the trading calendar.
- **Massive REST FX floor is 2 years rolling** (2024-07-24), identical for daily and
  for 1m/5m/30m/1h. Below it, requests 403 — an entitlement boundary, never a
  "no history" signal.
- **1h is Yahoo's even for pairs.** Yahoo's 1h reached 2023-10-09 (EURUSD), *past*
  Massive's floor, in one unthrottled request. Don't "unify" 1h onto Massive.
- **Massive REST allows 5 requests/minute** and sends no `Retry-After`, so reactive
  backoff (1s/2s/4s) cannot clear the window. The lane paces preemptively via
  `MassiveClient(min_interval_seconds=...)`. Nightly ≈12 min; the full 760-day seed
  ≈2 h, dominated by 1m.
- **Massive's S3 `global_forex/` prefix lists back to 2010 but GETs 403.** The
  flat-file entitlement covers `us_stocks_sip` only. Probe permission boundaries with
  GET, never with LIST — the listing alone promises 16 years that cannot be fetched.

Both intraday providers serve rolling windows, so history is **accumulated**:
`merge_ticker_rows` dedups on `bar_timestamp`, and the floor bounds only the initial

**Source:** CLAUDE.md section "FX and DXY — Yahoo owns the asset class, IB does not" (moved 2026-09-02)
