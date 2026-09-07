# A retired CBOE index alerted every night for five weeks

**Rule:** A non-equity preset ticker has no terminus concept, so an index the
provider retires is indistinguishable from one the fetcher failed on. Before
calling a stale non-equity symbol a livewire bug, query the provider for that
symbol's own last observation — a series livewire has fetched completely is not
a gap, and the fix is to drop the ticker, never to "fix" the lane.

**Incident / measurement:** 2026-09-07, host **macmini**.

`coverage` mailed `[Livewire] coverage_report failed on 2026-09-04` naming
`volatility stale: VIXTLT`, as it had every night since 2026-07-30. The CBOE
lane exited 0 in 67s on every one of those runs.

Queried live at 2026-09-07T12:01Z, CBOE's own API answers for both tickers:

| ticker | HTTP | rows | first | last |
| --- | --- | --- | --- | --- |
| `_VIXTLT` | 200 | 2,150 | 2018-01-02 | **2026-07-30** |
| `_VXTLT`  | 200 | 5,693 | 2004-01-02 | 2026-09-04 |

The lake held **2,150** rows for VIXTLT — an exact match with upstream.
Livewire had fetched every row that exists. CBOE stopped publishing the series;
`VXTLT` covers the same underlying with 14 more years of history and was fresh.
Of the 43 tickers in `presets/volatility.json`, VIXTLT was the only stale one.

The same night's email also named `futures stale: CL_202609` (an expired
contract) and `rates stale: DGS10, DGS3, DGS30, DGS5` — FRED's own API had no
2026-09-04 observation for DGS10 either. Every line in that alert was a
non-defect, which is what five weeks of it had trained the reader to assume.

**Cost:** ~40 nightly alerts. The `stale_non_equity` branch
(`livewire_scripts/coverage_report.py`) sends one undifferentiated email with
the reason `"no recovery path for this asset class"` for retired instruments,
expired contracts, provider publication lag, and real outages alike.

**Fix:** dropped `VIXTLT` from `presets/volatility.json`, with the reason in the
preset's own `notes` so it is not re-added. The alert's inability to tell those
four cases apart is untouched and remains the standing defect.
