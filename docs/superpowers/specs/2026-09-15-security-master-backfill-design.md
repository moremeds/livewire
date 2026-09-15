# Security master backfill from Massive reference data

Status: implemented 2026-09-15 (this plan:
`docs/superpowers/plans/2026-09-15-security-master-backfill.md`); not yet run on
the mini. Phase 1 of two; phase 2 (Russell 2000 history from SEC EDGAR
N-PORT / N-Q / N-CSR / N-30D) gets its own spec once this one has run on the
mini.

## 1. Problem

Every index-membership event on the mini is `status="unresolved"` with a
`security_id` of `unresolved:<ticker>` (measured on the mini 2026-09-15:
sp500 2021 rows, ndx100 592, djia 100, r2k-proxy 1886; `membership_unresolved`
= 3455). The histories themselves are fine (sp500 back to 1996-01, ndx100 to
2004-01, djia to 1991-05). They resolve to nothing because
`security_master/events.parquet` holds one verified row (MUNJ, 2026-08-26).
`members_effective_at` returns only verified rows, so every PIT query answers
the empty set and the apex membership API serves 503.

`membership_sync._resolve` looks a ticker up as
`SecurityMaster.resolve_symbol("massive", ticker, exchange_mic, effective_at, as_of)`
for `exchange_mic in ("XNAS", "XNYS", "ARCX")`. The only existing bulk writer
of identities is `shepherd-universe import-decision`
(`livewire_scripts/shepherd_universe.py`), which appends hand-adjudicated
decision manifests; it has no `runs` row on the mini and no manifest exists
for these tickers. Nothing fetches identities from a provider.

## 2. Source

Massive `GET /v3/reference/tickers` (measured from the mini 2026-09-15 with the
production key; the raw response bodies of these probes are the frozen
fixtures of §7, saved before implementation starts):

| probe                                      | result                                                                                    |
| ------------------------------------------ | ----------------------------------------------------------------------------------------- |
| `ticker=AAPL&date=2010-01-04`              | 1 row: name, `cik`, `composite_figi`, `share_class_figi`, `primary_exchange`, `list_date` |
| `ticker=AAPL&date=2005-01-03`              | 1 row, name "APPLE COMPUTER INC" (period name)                                            |
| `ticker=AAPL&date=1998-01-05`              | 0 rows                                                                                    |
| `ticker=YHOO&active=false`                 | 1 row, `delisted_utc` 2017-06-19                                                          |
| `ticker=WLP&active=false`                  | 1 row, `delisted_utc` 2014-12-03                                                          |
| `ticker=DELL&active=false`, `ticker=AAMRQ` | 0 rows (gaps exist)                                                                       |
| `active=false&sort=delisted_utc&order=asc` | earliest 2003-09-11                                                                       |

So: coverage from about 2003, historical state via `date=`, delistings
listed, FIGI and CIK present, some names missing. Nothing before 2003.

**Observed on capture** — the frozen bodies contradict the table above and the
tests are written to the bodies, not to this section. Seven facts, measured on
the mini 2026-09-15: `tests/fixtures/massive_reference/README.md`. Chiefly F1:
no body carries `list_date`, this list endpoint never returns it, so the
`date=` probe runs for every ticker and `effective_from` always comes from the
probe date or from nothing — the `list_date` in the `ticker=AAPL&date=…` row
above is not there. Also F5 (a delisted record may carry no FIGI, so YHOO →
AABA is not a FIGI-matched rename) and F6 (all 13,181 active US stock listings
are `currency_name: usd`; no non-USD fixture exists).

The endpoint's rate limit is not published for this plan. Two new declared
constants in `clients/constants.py`, same scoped shape as
`massive_requests_per_minute/fx`:
`massive_requests_per_minute/reference` (initial value 5 per_min, the only
Massive REST limit this repo has measured; the first mini run is a
`--tickers` sample that measures the real rate, and the full run is paced
with `LW_DECLARED_MASSIVE_REQUESTS_PER_MINUTE_REFERENCE` set to it) and
`massive_backoff_s/reference` (the single 429 backoff, initial 60 s, override
`LW_DECLARED_MASSIVE_BACKOFF_S_REFERENCE`). `declared()` requires the key to
exist, so both ship with these initial values. Every rate-limit number in this
repo carries a scope (pm:2026-07-27-fx-dxy-provider-floors).

## 3. Components

### 3.1 `clients/universe_client.py` — `fetch_ticker_identity(ticker, api_key, *, probe_date=None)`

One function next to the existing `check_ticker_status` (which uses the
single-ticker `/v3/reference/tickers/{T}` path form; this one uses the list
form with query parameters, a new code path). Calls, in order:

1. `/v3/reference/tickers?ticker=T` (active)
2. `/v3/reference/tickers?ticker=T&active=false` (delisted)
3. only when neither record carries `list_date` and `probe_date` is given:
   `/v3/reference/tickers?ticker=T&date=<probe_date>`

Returns `IdentityRecords(responses: list[bytes], records: list[IdentityRecord])`
where `IdentityRecord` has `ticker, name, cik, composite_figi,
share_class_figi, mic, currency, list_date, delisted_utc, existed_at`
(`existed_at` is the `date=` probe date when call 3 returned the record, else
`None`). Raw response bytes are returned untouched so the caller commits them
as evidence. HTTP errors raise `UniverseFetchError`; a 404/empty result is an
empty list, never an exception (the twin of the `fetch_batch` rule: an outage
must not read as "unknown ticker", so a transport failure is an exception and
an empty `results` is data).

`primary_exchange` is already a MIC in Massive's payload (`XNAS`, `XNYS`,
`ARCX`, `XASE`, `BATS`); it is used as-is. `currency_name` is uppercased.

### 3.2 `livewire_scripts/security_master_sync.py` — `livewire_ingest.py security-master sync`

New `COMMANDS` entry in `scripts/livewire_ingest.py`, and `security-master`
joins the scheduled-env allowlist there (the set that today holds
`universe-sync`, `shepherd-universe`, `membership-sync`), so a cold shell or
launchd gets `MASSIVE_API_KEY` from `~/market-warehouse/.env`.

Flags: `--index <id>...` (default: all four stores present on disk),
`--tickers T...` (explicit subset, for repairs), `--dry-run`.

Per run:

1. Collect the distinct tickers from every current (non-superseded) event
   with `security_id = unresolved:<ticker>` in the chosen index stores, with
   the set of `effective_at` dates each ticker needs. Upper bound today about
   3,500 tickers.
2. For each ticker with at least one needed date not inside a verified
   interval for that symbol in the master, call
   `fetch_ticker_identity(ticker, key, probe_date=earliest_needed_date)`,
   paced by the declared constant. Covering only the earliest date would
   skip a ticker whose later membership falls past the end of an existing
   interval, and the widening rule of §4 could never run.
3. Evidence, once per run: `SourceEvidenceStore.persist_raw(bytes)` per
   response as it arrives, then one `record_many([...])` for all responses
   before any identity event is appended (`record` is not buffered; it calls
   `record_many` with one item, and per-response manifest commits cost 41
   min/night, pm:2026-08-31). If `record_many` raises, nothing is appended and
   the run exits 1.
4. Derive identity events (section 4) and append through
   `SecurityMaster.append` on a master constructed with
   `evidence_verifier=_evidence_verifier(evidence_store)` (the helper in
   `membership_sync.py`; every master `membership_sync` itself builds passes
   `None`, which cannot append, so this sync wires its own). A collision
   raised by the master is recorded as a skipped ticker with its reason, never
   swallowed.
5. Emit ledger rows (section 5).

The command is idempotent: a ticker whose verified intervals already cover every
needed membership date is skipped without a fetch.

### 3.3 `livewire_scripts/membership_sync.py` — `reresolve --index <id> --confidence {B,C,D}`

For each current (non-superseded, non-rejected) event whose `security_id`
starts with `unresolved:`, in `(effective_at, known_at, revision)` order:

1. Resolve. An `add` resolves as `_resolve(master, ticker, event.effective_at,
   now)`. A `remove` takes the `security_id` of the ticker's latest resolved
   `add` in the same index (from this pass or an earlier one); if there is
   none it tries `_resolve` at its own `effective_at`. Master intervals are
   end-exclusive (`_contains`), so a remove effective on the delisting date
   would otherwise never resolve and its add would become a permanent member
   that the next `sync` diff removes a second time.
2. Recover first: the replacement `event_id` is deterministic (step 3). If
   the store already holds it, the replacement was written by an earlier
   interrupted pass; skip to step 4 and only complete the rejection. No
   conflict check runs on a recovery.
3. Fail closed on unbalanced replay: take the resolved id's current
   events in this index **with the same status the proposed event will
   carry** (`verified` events when the run's confidence yields `verified`,
   `candidate` when it yields `candidate`), insert the proposed event at its
   `effective_at`, and replay that sequence in
   `(effective_at, known_at, revision, event_id)` order. PIT Silver replays
   verified events only, so a guard over mixed statuses could pass an
   add-then-remove whose add is `candidate` and leave the verified replay
   with a remove and no open add. If the sequence
   would ever open a second add on an open membership, or remove with no
   open add, skip the placeholder and count `membership_reresolve_conflict`.
   This is the same rule `pit_silver_revision` enforces ("duplicate open
   membership interval", "membership removal has no open interval") and it
   covers a nightly `sync` add that landed *later* than the historical add
   being inserted, which a check at `effective_at` alone would miss. A remove
   that closes an open add passes; an add whose ticker was already added by
   `sync` fails.
3b. Append the resolved event: same `action`, `effective_at`,
   `announced_at`, `source_refs`/`source_hashes`; `known_at = now`;
   `revision` from a per-`security_id` counter carried across the whole pass
   (a rename collapses two placeholders onto one id); `event_id` derived
   deterministically from the placeholder's `event_id` plus the resolved id;
   status per step 5.
4. Then append the `rejected` revision of the placeholder (`security_id`
   unchanged, `revision = max+1`, `supersedes = placeholder event_id`,
   `known_at = now`, same sources), which drops it from replay. This order is
   restart-safe: a crash between 3b and 4 leaves the placeholder current, and
   the retry takes the step 2 path. The reverse order would lose the
   membership on retry. The retry reuses the stored replacement as is; it
   never rebuilds it with a new `known_at` or revision (a deterministic id
   with different row content is rejected by the log).
5. Status: `candidate` for `r2k-proxy`; otherwise `_CONFIDENCE_STATUS[confidence]`
   from the required `--confidence` flag, the same contract `import` uses. A
   placeholder does not record the confidence its history was imported under,
   so the operator asserts it per run; identity resolution alone does not
   raise a C/D history to `verified`.

Two appends because `IndexMembershipStore` only lets an event supersede one
with the same `(index_id, security_id)`. `known_at = now` keeps PIT honest: an
`as_of` before the reresolve still sees the placeholder, as the PIT membership
design requires (`docs/superpowers/specs/2026-09-13-dividend-fx-and-pit-membership-design.md`
§ `known_at`, restated in `membership_sync.py` module docstring, and rule 8
of `docs/contracts/shepherd-security-identity.md`). Idempotent: a second run
finds no placeholder that resolves. Events whose ticker still does not
resolve are left alone and counted.

The backlog measure is one helper used by `import`, `sync` and `reresolve`:
`membership_unresolved` counts distinct `security_id`s among current
(non-superseded) rows with `status="unresolved"`, the same distinct-id
cardinality the checks graded before. Today `import` and `sync` count every
unresolved row's id including superseded ones, so a reresolve that leaves the rejected placeholder
chain in place would drain to zero and the next nightly `sync` would restore
the WARN.

`_RESOLVE_MICS` becomes `("XNAS", "XNYS", "ARCX", "XASE")`: the Russell list
carries 44 NYSE American names today (measured from the TradingView scanner
2026-09-15) and the master will hold them under `XASE`.

The operator view `members_at` (candidate- and unresolved-inclusive replay
with symbol fallback) is unchanged.

## 4. Identity rules

| Massive facts                                                                                                  | Master rows                                                                                                                                                                                              |
| -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| one record, FIGI present                                                                                       | one `security_id`, one verified interval, basis `provider_figi`                                                                                                                                          |
| active + delisted records, same composite FIGI **and** same share-class FIGI, Massive's dates disjoint (rename) | one `security_id`, two symbol rows (`revision` 1 and 2, no `supersedes`)                                                                                                                              |
| same as above but Massive's dates overlap                                                                      | a material effective-time conflict (contract rule 7): two `security_id`s, both `unresolved`, counted `identity_conflict`; nothing is inferred about which date is wrong                                 |
| same composite FIGI, different share-class FIGI                                                                | a material FIGI conflict (contract rule 7): two `security_id`s, both `unresolved`, counted `identity_conflict`; same-id rows would bypass the master's collision checks                                 |
| same composite FIGI, share-class FIGI missing on one side                                                      | incomplete rather than contradictory evidence: two `security_id`s, both `candidate`, counted `identity_conflict`; the contract (priority 3) does not let a composite FIGI alone prove continuity          |
| two records, different composite FIGI (ticker reused by another issuer)                                        | two `security_id`s with disjoint intervals; the master's collision check enforces disjointness                                                                                                           |
| a verified row for this symbol already exists in the master and covers part of the needed interval, same FIGIs | a new revision on the **existing** `security_id` superseding that row with the widened interval; never a second id (the FIGI collision check would reject it)                                            |
| record without any FIGI                                                                                        | `candidate`, basis `provider_reference`; does not resolve; counted                                                                                                                                       |
| record with no `list_date` and an empty `date=` probe (no provable start)                                     | nothing appended; counted `identity_no_start`. A start of `known_at` would be an invented date and, for a delisted record, an interval ending before it starts, which the master rejects            |
| no record at all                                                                                               | nothing appended; counted as `identity_unknown_to_provider`                                                                                                                                              |

Interval bounds: `effective_from = list_date` when present; else the
`date=` probe date when that probe returned the record (`existed_at`); else
nothing is appended (an empty probe proves nothing about the past).
`effective_to = delisted_utc` when present, else open. `known_at` is the fetch
time. `issuer_name`, `cik`, `currency` come from the record.

`security_id` is `SecurityMaster.new_security_id()`, opaque, never derived
from FIGI or ticker (contract `docs/contracts/shepherd-security-identity.md`
lines 12–15). The composite FIGI is the join key only inside one run.

The corporate-actions lane reads currency through
`sync_corporate_actions._equity_currency_resolver`: per `symbol`, the verified
row with the strictly greatest `known_at`, with no provider, MIC or interval
filter; on an equal `known_at` the first row encountered stays. One fetch in
this backfill can append several rows for one symbol with the same `known_at`,
so the currency test in §7 covers the equal-timestamp case and asserts the
resolver's choice, not only that the field survives.

## 5. Ledger and status

`runs`: `job="security-master-sync"` for the sync, `job="membership-reresolve"`
for the reresolve pass. Not `membership-sync`: the "Membership sync ran today"
check grades the latest run of the day, so a later OK reresolve row would hide
a FAILED scheduled sync.

`measurements` (scope = index id, or `all` for the sync totals):

| name                            | meaning                                                                                   |
| ------------------------------- | ----------------------------------------------------------------------------------------- |
| `identity_tickers_requested`    | distinct tickers sent to Massive                                                          |
| `identity_events_appended`      | master rows appended                                                                      |
| `identity_candidate`            | rows appended without FIGI                                                                |
| `identity_no_start`             | records with no `list_date` and an empty `date=` probe; nothing appended                 |
| `identity_conflict`             | composite FIGI shared across differing or missing share classes, or matching FIGIs with overlapping dates |
| `identity_unknown_to_provider`  | tickers with no Massive record                                                            |
| `identity_collisions`           | appends the master rejected                                                               |
| `identity_fetch_failed`         | tickers skipped on a transport error, 5xx, or repeated 429; any > 0 makes the run exit 1  |
| `membership_reresolve_conflict` | placeholders skipped because inserting the resolved event would unbalance the replay      |
| `membership_unresolved`         | after reresolve, per index: distinct `unresolved:*` ids among current rows, the same cardinality `import` and `sync` report today; already graded by "Unresolved memberships" |

`membership_source_fetch_ok` stays as it is on `sync`; `reresolve` does not
emit it.

No new `status` check: the existing "Unresolved memberships" row turns from
WARN toward OK as the backlog drains, which is the acceptance signal apex was
told to watch. No launchd change in this PR: the backfill is one manual run on
the mini; scheduling a weekly refresh is decided after the numbers are in.
The status remediation hint for "Unresolved memberships" names
`livewire_ops.py membership`, which does not exist and stays out of scope; the
hint is corrected to name `security-master sync` + `reresolve`.

## 6. Failure handling

- Massive transport error or 5xx on a ticker: the ticker is skipped and
  counted in `identity_fetch_failed`; the run continues and exits 1 if any
  failed (the fred-rates pattern from PR #132).
- Rate-limit response (429): back off once by `massive_backoff_s/reference`,
  then treat as a fetch failure.
- Evidence commit failure (`record_many` raises): nothing is appended, exit 1.
  The verifier the master runs checks raw bytes only, so this ordering is
  what keeps a manifest-less identity row out of the store.
- The run holds no lake-wide lock; the master and each membership store
  serialize their own appends. Order on the mini: `security-master sync`,
  then `reresolve`, both before the next 01:00Z `membership-sync` (step 3 of
  §3.3 fails closed if that order is broken). Check the ledger for an
  in-flight run before starting. Consumers that replay membership
  (`shepherd_daily`, `pit_silver_revision`) pin explicit membership prefixes,
  and `shepherd_repair` pins a security-master prefix; a new row reaches
  them only when a run pins a longer prefix, never retroactively.

## 7. Tests

All against the real `SecurityMaster` and `IndexMembershipStore` on a temp
lake root and the real `SourceEvidenceStore`; Massive responses are the raw
bodies recorded from the real endpoint for real tickers, frozen with their
as-of date in `tests/fixtures/massive_reference/`. No network in tests.

- rename keeps one `security_id` across two symbol intervals (YHOO → AABA)
- ticker reused by another issuer yields two ids with disjoint intervals
- same composite FIGI with differing share-class FIGIs yields two `unresolved`
  rows and `identity_conflict`; a missing share-class FIGI yields two
  `candidate` rows
- existing partial interval is widened on the same id, not duplicated
- a record without FIGI is `candidate` and does not resolve; an empty
  `date=` probe does not backdate
- a ticker unknown to Massive appends nothing and is counted
- transport error is an exception, empty `results` is an empty list
- evidence: `record_many` called once; a raising `record_many` appends nothing
- reresolve: resolved event appended, placeholder rejected, second run
  appends nothing; `members_effective_at` sees the member only for `as_of`
  after the reresolve
- reresolve restart: a crash after the resolved append and before the
  rejection is completed by the retry with no duplicate
- reresolve add+remove pair where the remove is effective on the delisting
  date: both resolve to the same id; replay has no permanent member
- reresolve fails closed when a nightly `sync` add for the resolved id lies
  *later* than the historical add being inserted (the case a check at
  `effective_at` misses); a remove that closes an open add passes
- PIT Silver replay (`pit_silver_revision`) over a reresolved index with an
  add+remove pair raises nothing, including a `candidate` add followed by a
  remove processed at confidence B (the guard must reject that remove)
- retry after an interrupted pair: `members_at` / `_current_members` never
  hold both the placeholder and the resolved membership once the retry ends
- a delisted record with no `list_date` and an empty probe appends nothing
  and counts `identity_no_start`
- later-date coverage: a ticker whose existing interval covers its first
  membership but not its second is fetched and widened
- `--confidence C` yields `candidate`, `B` yields `verified`, r2k-proxy always
  `candidate`
- backlog: after reresolve, `import` and `sync` report the same
  `membership_unresolved` as `reresolve`
- `XASE` resolves
- `_equity_currency_resolver` picks the expected currency for a symbol with
  two verified rows sharing one `known_at` (real non-USD listing)
- `test_constants.py`: both declared constants exist, scoped, with a unit
- `test_status.py`: a `membership-reresolve` run after a FAILED
  `membership-sync` leaves "Membership sync ran today" BAD
- entrypoint dispatch tests for both subcommands (real argv) and the
  scheduled-env allowlist

## 8. Acceptance on the mini

Measured, not targeted, and reported per index split at 2003-01-01:
`membership_unresolved` before and after, `identity_unknown_to_provider`,
`identity_candidate`, `identity_conflict`, `identity_no_start`,
`membership_reresolve_conflict`,
elapsed time and requests per minute observed (this sets the declared
constant). Pre-2003 events are expected to stay unresolved in this phase.
After the run, apex's `/v1/membership/djia` is the external check.

## 9. Out of scope

SEC `company_tickers.json` / EDGAR as a second identity source, CUSIP and ISIN
mapping (OpenFIGI), Russell 2000 history from N-PORT and older forms, a
weekly refresh job, a read-side `livewire_ops.py membership` command, and
making `_equity_currency_resolver` interval-aware.
