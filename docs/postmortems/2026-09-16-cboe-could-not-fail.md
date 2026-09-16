# The CBOE lane could not fail, however much was missing

**Rule:** A fetch loop that swallows every exception and returns nothing is not
tolerant, it is blind. Per-symbol isolation means the *other* symbols continue,
never that the phase reports success. A total outage must be distinguishable
from a clean night by the exit code alone.

**Incident / measurement:** 2026-09-16, host **macmini**, found while auditing
why `daily-update` and `intraday-catchup` page.

`fetch_cboe_volatility.main()` looped over the preset and wrapped each symbol in
`except Exception as e: console.print(...)`. It had no `return`, so it returned
`None`, and `livewire_ingest.py` turned that into exit 0. Back in
`sync_runner.run_sync`, phase 3 is:

```python
rc = _phase("daily_backfill_volatility_cboe", [py, ingest, "cboe-vol", "--preset", ...])
if rc != 0:
    failures.append("cboe_volatility")
```

`rc` was 0 whether CBOE served every index or none of them. The phase could not
append to `failures`, so it could not fail the run, so nothing could page and
nothing reached the ledger as a failure. Every symbol could error and the night
still read green.

This is the same shape as pm:2026-07-22-ib-not-a-single-point-of-failure's
inverse and of the `fetch_batch` rule in CLAUDE.md — "maps a raised fetch to the
exception, never to `[]` — otherwise a total outage reads as `no_trade`,
`errors=0`, exit 0" — which was written for IB and never applied here.

There was also no retry: a single 502 or read timeout lost that symbol for the
night, silently. FRED had earned a bounded retry three weeks earlier
(pm:2026-09-14-fred-502-aborted-remaining-series); CBOE, the other small
one-shot phase, had none.

**Cost:** unmeasurable by construction, which is the finding. No test covered
`fetch_cboe_volatility`'s exit code, and the ledger holds no failed CBOE phase
to count, because one could not be recorded. The known CBOE damage of the period
was surfaced by a different detector entirely
(pm:2026-09-07-retired-cboe-index-alerted-forever, found by the coverage scan).

**Fix, in two parts, because the two failures are not the same failure:**

- `clients/http_retry.py` is now the repo's one definition of a transient HTTP
  failure — a 5xx or a transport error is retried, a 4xx is raised on the first
  attempt. FRED's local copy was deleted in favour of it, so there is one
  policy, not two. CBOE gets `cboe_retry_attempts` / `cboe_retry_backoff_s`,
  the same shape as FRED's declared bounds.
- `main()` returns 1 when any symbol is still unfetched after its retries, and
  names them. A 4xx does **not** fail the phase: CBOE answering 404 is a claim
  about CBOE, already surfaced by the `Stale non-equity` check, and failing on
  it would page nightly for a retired index — the exact noise
  pm:2026-09-07 describes. A 200 with no bars is likewise not a failure.

So the new page fires only when CBOE is genuinely unreachable after retries,
which is rare, real, and previously invisible.

→ test: `tests/test_fetch_cboe_volatility.py::TestMainExitCode::test_returns_1_when_a_symbol_is_still_unfetched_after_retries`,
`::test_returns_0_when_a_symbol_404s_and_names_it_in_the_output`,
`tests/test_http_retry.py::TestGetWithRetry::test_a_4xx_is_raised_on_the_first_attempt_and_never_retried`
