# T3 independent code review

Verdict: **ACCEPT for the bounded code milestone. No blocking findings.** This is not deployment or data-recovery acceptance.

Reviewer requested/configured: GPT-6 Astra, canonical Astra; observed backend model independently unverified. Author configured: GPT-6 Sol high per worker receipt. Native independent review, not a cross-model tribunal. Worktree: `.worktrees/fix-systematic-ops`; base `956674d4668245d4e39ae1b95f8d75c3b9c856d9`.

Reviewed actual tracked diff and untracked worker evidence: `livewire_scripts/fetch_cboe_volatility.py`, `tests/test_fetch_cboe_volatility.py`, `docs/evidence/systematic-ops/t3.md`. Untracked plan files belong to lead and were excluded from the code finding scope. No new untracked implementation/test files were present.

## Scope and regression checks

- Both existing GET sites enable HTTPX redirects. Shared `clients/http_retry.py`, EIA/FRED callers, daily/sync orchestrators and the canonical writer are unchanged. Existing retry test still verifies a 502 then success, and 404 still raises immediately after one attempt; no unbounded retry or new provider fallback is introduced.
- Remaining HTTP 307/403/404/429 and empty combined payloads fail visibly. Removing the “retired” inference is the approved behavior change, not an accidental regression against the old 404/empty success tests. The code makes no claim that a 404 proves permanent retirement.
- Optional CSV failure continues to preserve/persist valid JSON. VIX/SPX/RUT CSV mappings and newer-CSV append semantics remain unchanged. No freshness detector, new calendar threshold, schema change or alternate write path was added.
- Existing `tests/test_run_daily_update_job.py::TestMain::test_cboe_failure_does_not_block_silver` verifies exit 1 while still running Silver once, so the stricter CBOE outcome does not globally gate unrelated adjusted equity publication.
- Redirect regression tests use HTTPX MockTransport and actually traverse a 307; they fail if either call stops forwarding redirect handling. New valid-JSON/CSV-failure test now uses the observed Sep 29 VIX row from the accepted CBOE probe, with as-of/source comment and no runtime network. An earlier reuse of invented old fixture prices was flagged and corrected before acceptance. Existing unrelated fixtures were not broadened into this patch.

## Independent verification

Executed in the reviewed worktree:

`rtk proxy .venv/bin/python -B -m pytest tests/test_fetch_cboe_volatility.py tests/test_run_daily_update_job.py::TestMain::test_cboe_failure_does_not_block_silver -q -p no:cacheprovider`

Result: **41 passed in 0.30s**, process exit 0. `git diff --check` also passed. Worker evidence separately records baseline 34 passed, seven expected red regressions before implementation, focused 40 passed afterward and Ruff check/format passed. These author reports were read; the 41-test final run above was independently executed. No full suite, CI, live GET, production write or consumer read was performed by this reviewer.

Reviewed SHA-256:

- fetcher: `9318ed07e132deb4097281a9552f22770d5b037e0909a4ab71a080fd8416b859`
- tests: `f62c35af16e824852d76c58925faf283f8cbdf888c97e797708ee5f0eef6129a`
- worker evidence: `199aca39d6e13b80b7feba17947227fdf706b8e4dfe178ac48870f6de3b434da`

## Remaining release gates

Full actual CI-source coverage and exact candidate CI remain required. An unavailable/actually retired preset member will now produce a visible nonzero result until lifecycle evidence supports a separate disposition; validate the whole preset under the planned bounded canary before claiming recovery. Successful nonempty history is still not a freshness guarantee: existing due/coverage checks retain that responsibility. Preserve the approved staged existing-file clone, complete missing-date/schema footprint, backup and normal-cycle/consumer verification gates before production recovery. None of these remaining gates is represented as completed here.

herd-report independent_review T3: commit none, evidence docs/evidence/systematic-ops/review-t3.md, deviations: none. No code edits, nested agents, production queries or production mutations.

Final integration addition reviewed: lead added `docs/evidence/systematic-ops/cboe-provider-probe-20260930.jsonl` and changed the new fixture comment to point to that durable file. Its three records match the earlier reviewed public VIX/VVIX JSON and VIX CSV probe; they contain public URLs, timestamps and observed data, with no private filesystem paths or credentials. File SHA-256 `c6e2d41009a966f5c864fb1e210216e52abaa41ceebe6eb9446a49f1b1cf2f72`. `git diff --check` remains clean. Behavior is unchanged and the ACCEPT verdict stands; no repeat test run was warranted for this comment/evidence-only integration.

## Full-suite sibling-contract follow-up

The lead's first full suite found one old-policy test I had not included in the initial bounded review: `tests/test_livewire_entrypoints.py::test_cboe_vol_still_exits_zero_when_cboe_has_retired_the_index`; lead reported 1 failed / 3,220 passed, coverage 95.11%. My prior 41-test pass did not cover that dispatch-level contract, and did not establish full-suite success. The required full-suite gate correctly prevented delivery at this point.

Reviewed the cumulative three-file source/test diff and the updated entrypoint test. It is now `test_cboe_vol_reports_unverified_404_as_failed_fetch`: it calls the real `livewire_ingest.main` dispatch with a mocked HTTP 404, asserts exit **1**, requires the HTTP code and unfetched symbol in output, and rejects the false retirement label. This is consistent with the approved policy and strengthens observable exit propagation rather than removing or weakening a test. Existing 503 dispatch propagation remains tested; dispatch code itself is unchanged (`scripts/livewire_ingest.py:56-70`). No runtime workaround was added merely to satisfy the obsolete expectation.

Repository-wide searches over tests/runtime for CBOE retirement, 404 and empty-success expectations found no remaining test asserting 404/empty success. Historical postmortems still describe the old policy, which is historical evidence rather than a live test contract. The Sep 7 retirement incident actually documented a full HTTP-200 series with its final observation and removal from the preset; it does not justify equating arbitrary 404s with confirmed retirement.

Independently reran the 40 fetcher tests, both real CBOE entrypoint tests and Silver-isolation test in one invocation: **43 passed in 0.25s**, exit 0; `git diff --check` passed. New entrypoint-test file SHA-256: `9380a1fb441e9371960571054e797209bb8ecd7570dc53802b433e9ba25247c6`. ACCEPT for the corrected bounded milestone remains; a fresh full-suite success at the final candidate remains required and is not claimed by this follow-up.
