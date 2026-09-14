# One FRED 502 aborted the three series after it

**Rule:** A provider fetch loop retries a 5xx or a timeout a bounded number of
times, then carries on to the next item and fails the phase at the end. One
item's transport error must cost neither the items after it nor a green exit
code — a total outage still reads as failed, never as `inserted 0`, exit 0.

**Incident / measurement:** 2026-09-14, host **macmini**.

`intraday-catchup-20260914T100002Z-96742` exited 1 with `DAILY BACKFILL
COMPLETE with failures: fred_rates`; both watchdogs paged. The other 8 phases
exited 0. `logs/daily_backfill_fred_rates.log` holds the whole story:

```
Fetching FRED Treasury rates: ['DGS3', 'DGS5', 'DGS10', 'DGS30']
  DGS3: fetched 16158 rows, inserted 0, 1962-01-02 -> 2026-09-10
Traceback (most recent call last):
  File ".../clients/fred_client.py", line ..., in fetch_observations
    resp.raise_for_status()
httpx.HTTPStatusError: Server error '502 Bad Gateway' for url
  'https://api.stlouisfed.org/fred/series/observations?se...'
```

DGS3 completed; DGS5 got the 502; DGS10 and DGS30 were never requested. The
phase lasted 3.3s. `FredClient.fetch_observations` had no retry, and
`fetch_fred_rates.run` let the exception leave the loop.

Not the first: the same log (92 runs) holds three tracebacks — two
`httpcore.ReadTimeout` in late July, and this 502. All three are transient
upstream failures that one retry would have absorbed.

**Cost:** two pages and a failed run for an outage that was gone by the next
request. No data was lost: the failing request would have inserted 0 rows.

**What was *not* the cause — verified before touching code:**

- `FRED_API_KEY` is present in `~/market-warehouse/.env` on the mini (32 chars).
- `bronze_rates_1d` stuck at 2026-09-10 is **upstream, not a livewire gap**.
  All four series hold `max(trade_date) = 2026-09-10`
  (DGS3/DGS5/DGS10 16,158 rows, DGS30 12,388), and
  `https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10`, fetched from the
  mini on 2026-09-14, also ends at `2026-09-10,4.95`. FRED had not published
  2026-09-11. The successful 09-12 and 09-13 runs both logged `inserted 0`.
  `Stale non-equity: rates=4` is a claim about the provider and clears itself
  on the next coverage scan after FRED publishes — the same shape as
  pm:2026-09-07-retired-cboe-index-alerted-forever.

**Standing exposure, recorded here because nothing else records it:** rates
have **no lane in the nightly daily-update**. `clients.constants.LANE_ORDER` is
`(futures, cmdty, cboe, fx, corporate-actions, equity, silver, catalog)`; FRED
is fetched only by phase 2 of `sync_runner.run_sync`, i.e. the 10:00Z
intraday-catchup. An all-green daily-update says nothing about rates, and one
un-retried request was the whole of rates' freshness path. Disposition open —
this post-mortem hardens the request; it does not add a lane.

**Fix:** `clients/fred_client.py` retries a 5xx or an `httpx.TransportError`
`fred_retry_attempts` times with an `fred_retry_backoff_s` linear backoff
(both `DECLARED`), and never retries a 4xx — a bad key or a retired series is a
request problem that retrying only makes slower.
`livewire_scripts/fetch_fred_rates.py` catches `httpx.HTTPError` per series,
continues to the next, and returns 1 if any series is still unfetched.

→ test: `tests/test_fred_client.py::test_a_502_is_retried_and_the_next_attempt_is_used`,
`::test_a_4xx_is_never_retried`,
`tests/test_fetch_fred_rates.py::test_one_failing_series_does_not_stop_the_others_and_the_run_still_fails`,
`::test_a_total_outage_is_a_failure_not_an_empty_success`
