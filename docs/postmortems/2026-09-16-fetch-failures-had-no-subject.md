# `identity_fetch_failed` counted failures without naming them

**Rule:** A failure measurement records what failed, not only how many. A count
tells the operator to act and withholds what to act on, which turns a targeted
retry into a full rerun.

**Incident / measurement:** 2026-09-16, host **macmini**, during the
security-master identity backfill over ~3,455 placeholder tickers.

`security_master_sync.sync` counted every fetch failure into a single
run-scoped `identity_fetch_failed` measurement and nothing else:

```python
except UniverseFetchError as exc:
    if exc.status_code != 429:
        counts["identity_fetch_failed"] += 1
        continue
    sleep(backoff_s)
    try:
        result = fetch(ticker, probe_dates=probe_dates)
    except UniverseFetchError:          # not bound — the status was discarded
        counts["identity_fetch_failed"] += 1
        continue
```

`ticker` was in scope at both sites and was not recorded. The retry's own
exception was not even bound to a name, so after a 429 backoff the second
attempt's status code existed for one frame and was thrown away. Any failure
count > 0 exits 1, so the operator is told to do something, and the only
available something was to rerun the whole universe and rely on idempotence to
skip what was already covered.

The same file already had the right pattern in it: `identity_probe_empty` files
one row per empty probe, scope `<ticker>:<date>`. Failures were the one fact
that got aggregated away.

**Cost:** none realised — the 2026-09-16 run finished with
`identity_fetch_failed = 0`. This was found by reading the code while auditing
alert noise, before it cost anything. Left alone, the first real failure would
have cost a full re-pass over the universe to recover a handful of tickers.

**Fix:** each failure files its own `identity_fetch_failed_ticker` row, scope
`<ticker>:<http status>` (matching `identity_probe_empty`'s `<ticker>:<date>`
shape), unit `first_attempt` or `after_429_retry`. The retry's exception is now
bound, so the status the second attempt actually saw is what gets recorded. The
aggregate `identity_fetch_failed` keeps its name, scope and meaning, so every
existing reader — `status`, the runbook, the monitor runbook — is unchanged. A
dry run files nothing, because it appends nothing.

→ test: `tests/test_security_master_sync.py::test_a_fetch_failure_names_the_ticker_and_its_http_status`,
`::test_a_failure_after_the_429_backoff_says_so`,
`::test_a_failure_with_no_http_status_is_still_named`,
`::test_a_dry_run_names_nothing_because_it_appends_nothing`
