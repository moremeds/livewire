# The reference endpoint was paced at the free Currencies tier's 5/min

**Rule:** A rate-limit constant is seeded from the provider's published limit
for _that_ plan and _that_ endpoint, or it is measured — never carried over
from another scope because it is the only number the repo already has. Copying
a free-tier number onto a paid endpoint costs wall-clock nobody budgeted for,
and the cost is invisible because the job looks like it is working.

**Incident / measurement:** 2026-09-16, host **macmini**.

`massive_requests_per_minute/reference` was declared 5/min. It was seeded from
`massive_requests_per_minute/fx`, whose comment correctly says the 6th call
429s — that measurement is real, and it is the **free Currencies tier**. The
livewire key is a paid Stocks plan.

The first security-master backfill therefore slept 12 s before every request.
Measured at a declared 30/min, the run spent 2.04 s per request against a
round-trip of well under a second: the sleep was the entire cost, and the
endpoint was never the constraint. A full pass over the 3,124 distinct
placeholder tickers (measured 2026-09-16; the summed per-index
`membership_unresolved` is 3,455, but a ticker in two indexes is fetched once)
at 3.4 requests each is 35+ hours at 5/min, which does not fit between two
weekday 01:00Z `com.livewire.membership-sync` runs — so the backfill could not
have completed at the declared rate at all. Four runs were started and stopped
before the rate was raised.

Massive's own knowledge base says free-tier subscriptions are limited to 5
requests per minute, paying customers have **unlimited** API requests, and
callers should stay under 100 requests per second to avoid protective
throttling. No per-asset-class limit is documented. The mini's key returns 200
on `/v3/reference/tickers` and on equity and FX aggregates, 403 on indices
aggregates — a paid Stocks plan.

**Cost:** four abandoned runs and roughly three hours of operator time on a job
whose real duration is about 2.5 hours. No data was lost or corrupted; every
stopped run's committed chunks were kept and skipped on the next pass.

**What was _not_ the cause — verified before changing the number:** the
endpoint did not throttle. Zero 429s were observed at 30/min, 120/min or
600/min. The 2.04 s/request at 30/min matches `60 / 30 = 2.0 s` of declared
sleep plus ~0.04 s of everything else, so latency contributed nothing
measurable at that pace.

**Fix:** `massive_requests_per_minute/reference` is 600/min — a tenth of the
documented 100 req/s ceiling, and the rate the 2026-09-16 backfill ran at. At
that pace the sleep is 0.1 s against a ~0.6 s round-trip, so the job is
latency-bound at about 24 tickers/min single-threaded and raising the constant
further changes nothing. The FX scope keeps its measured 5/min. A test asserts
the two scopes do not hold the same number, so the copy cannot silently happen
again.

→ test: `tests/test_constants.py::test_the_reference_endpoint_rate_limit_is_scoped_like_the_fx_one`,
`tests/test_security_master_sync.py::test_the_pace_runs_before_every_request`
