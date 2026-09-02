# The flat-file GET floor is a rolling 5 years — LIST lies

**Rule:** Probe permission boundaries with GET, never with LIST, and never delete raw partitions below the rolling floor.

**Incident / measurement:**

⚠️ **The flat-file GET floor is a rolling 5 years. LIST lies.** Measured
2026-07-29 with one GET per calendar year against both prefixes, then a binary
search: **2021-07-27 → 403, 2021-07-28 → OK**, i.e. exactly 1827 days = 5.00
years before the probe date, identical for `day_aggs` and `minute_aggs`. Every
year 2003–2021 returns `403 Forbidden`. The LIST-derived `discovery.earliest`
of `2003-09-10` (5755 days) is the same trap already documented for
`global_forex/` — *probe permission boundaries with GET, never with LIST*.
An earlier version of this file claimed day_aggs reaches "back to 2003"; it
does not, and never did.

Two consequences:

- **`backfill` is not a deep-history tool.** It re-fetches inside the rolling
  window only. As of 2026-07-29 the warehouse already holds the entire entitled
  range (`raw_completed` starts 2021-06-11, *earlier* than the current floor,
  because those files were fetched when the window reached further back).
- **Never delete raw partitions to reclaim space.** Anything older than the
  current floor cannot be re-downloaded, ever — the same standing as the triage
  verdict store. Re-measure before trusting any of this; the floor rolls forward
  one day per day. Result: `logs/probes/2026-07-29-flatfile-get-floor.json`.


**Source:** CLAUDE.md section "Massive day_aggs flat-file ingestion (full-universe equity daily)" (moved 2026-09-02)
