# The interior gap scan measures liquidity, not data loss

**Rule:** Keep the interior gap scan unscheduled and ungraded until a second source separates no-bar from no-trade, and never put a guessed timeout around it.

**Incident / measurement:**

- ⚠️ **The interior gap scan is NOT scheduled, and the number it produces is
  not a data-loss signal.** It measures liquidity. Run it once end-to-end on
  2026-08-17 over the real lake: **14,193 of 14,687 symbols (96.6%)** flagged,
  **318,059,872** "missing" 5m bars, in **3436.8s**. But **SPY, AAPL, NVDA,
  MSFT, QQQ and TSLA are all absent from the result entirely** — the six most
  liquid names have zero gaps, and the flagged 96.6% is the illiquid tail.
  `generate_expected_intraday_timestamps` expects a bar in every 5-minute RTH
  window of every trading day between a symbol's first and last bar, but SIP
  only emits a bar when there was a trade. Within the bar files alone, "no bar"
  and "no trade" are **indistinguishable** — the question is circular.
  Redefining the signal as a whole empty session does not rescue it: measured
  over 120 real symbols, the current rule flags 95.0% and the whole-session
  rule still flags **86.7%** (median 3 empty sessions), because an illiquid
  warrant genuinely does not trade for days at a time.
  The only second source that can separate the two is the day's raw flat-file
  `_symbols.parquet` traded set — which is exactly what `coverage` already uses
  and what "absent from the day's raw traded set is not a gap" already states.
  That is a redesign, not a threshold. **Until it is done the scan is not
  scheduled, is not in `_LAUNCHD_JOBS`, and `status` does not grade it** — a
  standing WARN reading `14193/14687` every week is the exact disease the
  status surface exists to cure. Nothing regressed by turning it off: it has
  never once produced an actionable result. `report_intraday_health` still
  writes `interior_gaps_<date>.log` for a manual full run, which is the
  artifact the redesign builds on.
  ⚠️ Its cost is real and worth keeping: a full 5m pass is **~3437s** — 302s
  for the cold glob, then 133.1 ms/symbol reading whole `bar_timestamp`
  columns and 58.4 ms/symbol detecting gaps, over 14,687 symbols. A 120-symbol
  sample projected 3115s and **understated the real run by 10.3%**, because the
  sampled reads were already warm. Never put a guessed timeout around it.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)
