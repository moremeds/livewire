# Security master backfill Implementation Plan

> **For agentic workers:** this plan is executed with the user's `/execute-plan` skill (one linear thread, milestone commits, evidence-based verification). Do NOT use subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drain the 3,455-row `unresolved:<ticker>` index-membership backlog on the mini by fetching real security identities from Massive `/v3/reference/tickers`, writing them into `security_master/events.parquet` as evidence-backed intervals, then rewriting each resolvable placeholder membership event onto its opaque `security_id` — so `members_effective_at` stops returning the empty set and apex's `/v1/membership/*` stops serving 503.

**Architecture:** One new provider function (`clients/universe_client.fetch_ticker_identity`) + one new module with a pure derivation function and a paced sync entrypoint (`livewire_scripts/security_master_sync.py`, dispatched as `livewire_ingest.py security-master sync`) + one new subcommand on the existing membership module (`livewire_ingest.py membership-sync reresolve`). No new stores, no new status check, no launchd change. Evidence goes through the existing `SourceEvidenceStore` (one `record_many` per run); every entrypoint opens and closes a `runs` row and emits `measurements`.

**Tech Stack:** Python 3.13, uv, `requests` (what `clients/universe_client.py` already uses; `responses` is how its tests mock the seam), pyarrow parquet stores, pytest.

**Spec:** docs/superpowers/specs/2026-09-15-security-master-backfill-design.md

## Global Constraints

- `uv run` only — never bare `python`, `pip`, or an activated venv.
- `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning` must pass before every milestone commit (verbatim from `.github/workflows/ci.yml:53`; never pass `--cov=<pkg>`).
- No network in tests. Massive responses are the real bodies recorded from the real endpoint for real tickers, frozen with their as-of date under `tests/fixtures/massive_reference/`.
- Tests run against the real `SecurityMaster`, `IndexMembershipStore` and `SourceEvidenceStore` on `tmp_path`; only the HTTP call is mocked, at the seam `tests/test_universe_client.py` already mocks (`responses`).
- Every new operating constant is one scoped `DECLARED` key in `clients/constants.py` with a unit, overridable by `LW_DECLARED_<KEY>`.
- Every entrypoint writes `runs` (open row, then a terminal row with `ended`/`exit_code`/`verdict`) plus `measurements` to the ledger, or it did not happen.
- Never commit outside the execute-plan milestone contract; never `git push`, merge, or promote without an explicit request.
- CHANGELOG `Unreleased` entry, the one `CLAUDE.md` line with its `→ test:` pointer, and the `docs/runbook.md` entries all belong to the last task (Task 7), not scattered.
- `security_id` is always `SecurityMaster.new_security_id()` — opaque, never derived from FIGI or ticker (`docs/contracts/shepherd-security-identity.md` lines 12–15).

---

## Task 0 — Capture the frozen Massive fixtures on the mini

Nothing in Tasks 2–6 can be written before these files exist: every later test loads them by name. This task produces data files only, no code.

**Files**

- Create: `tests/fixtures/massive_reference/README.md`
- Create: `tests/fixtures/massive_reference/aapl-active-2026-09-15.json`
- Create: `tests/fixtures/massive_reference/aapl-date-2010-01-04-2026-09-15.json`
- Create: `tests/fixtures/massive_reference/aapl-date-1998-01-05-2026-09-15.json`
- Create: `tests/fixtures/massive_reference/yhoo-active-false-2026-09-15.json`
- Create: `tests/fixtures/massive_reference/aaba-active-2026-09-15.json`
- Create: `tests/fixtures/massive_reference/wlp-active-false-2026-09-15.json`
- Create: `tests/fixtures/massive_reference/dell-active-false-2026-09-15.json`
- Create: `tests/fixtures/massive_reference/aamrq-active-false-2026-09-15.json`
- Create: `tests/fixtures/massive_reference/imo-active-2026-09-15.json` (XASE listing — Imperial Oil Ltd trades on NYSE American)
- Create: `tests/fixtures/massive_reference/nonusd-<TICKER>-active-2026-09-15.json` (the executor picks a real Massive-listed non-USD equity and records which one in the README; do not invent a ticker)
- Create: `tests/fixtures/massive_reference/delisted-earliest-2026-09-15.json`

**Interfaces**

- Consumes: Massive `GET https://api.polygon.io/v3/reference/tickers` with `apiKey` from `~/market-warehouse/.env` on the mini.
- Produces: exact response bytes on disk; later tasks load them with a per-test-module helper `_fixture(name) -> bytes`.

### Steps

- [ ] 1. Capture the bodies on the mini. Run exactly this (the mini owns the production key; the MacBook does not — CLAUDE.md "How to work in this repo" rule 1):

```bash
ssh macmini
set -a; source ~/market-warehouse/.env; set +a
mkdir -p /tmp/massive_reference && cd /tmp/massive_reference
B=https://api.polygon.io/v3/reference/tickers
STAMP=2026-09-15

curl -sS "$B?ticker=AAPL&apiKey=$MASSIVE_API_KEY"                    -o aapl-active-$STAMP.json
curl -sS "$B?ticker=AAPL&date=2010-01-04&apiKey=$MASSIVE_API_KEY"    -o aapl-date-2010-01-04-$STAMP.json
curl -sS "$B?ticker=AAPL&date=1998-01-05&apiKey=$MASSIVE_API_KEY"    -o aapl-date-1998-01-05-$STAMP.json
curl -sS "$B?ticker=YHOO&active=false&apiKey=$MASSIVE_API_KEY"       -o yhoo-active-false-$STAMP.json
curl -sS "$B?ticker=AABA&apiKey=$MASSIVE_API_KEY"                    -o aaba-active-$STAMP.json
curl -sS "$B?ticker=WLP&active=false&apiKey=$MASSIVE_API_KEY"        -o wlp-active-false-$STAMP.json
curl -sS "$B?ticker=DELL&active=false&apiKey=$MASSIVE_API_KEY"       -o dell-active-false-$STAMP.json
curl -sS "$B?ticker=AAMRQ&active=false&apiKey=$MASSIVE_API_KEY"      -o aamrq-active-false-$STAMP.json
curl -sS "$B?ticker=IMO&apiKey=$MASSIVE_API_KEY"                     -o imo-active-$STAMP.json
curl -sS "$B?active=false&sort=delisted_utc&order=asc&limit=1&apiKey=$MASSIVE_API_KEY" \
                                                                     -o delisted-earliest-$STAMP.json
```

Sleep 15 s between calls: the only measured Massive REST limit in this repo is 5 req/min (FX-scoped), and this endpoint's limit is unknown until Task 7's sample run measures it.

- [ ] 2. Pick the non-USD listing. There is no fabricated ticker here: query candidates until one returns a record whose `currency_name` is not `usd`, then save that body. Record the ticker, its MIC and its currency in the README.

```bash
for T in IMO CNI SU BCE; do
  curl -sS "$B?ticker=$T&apiKey=$MASSIVE_API_KEY" | python3 -c \
    'import json,sys; r=json.load(sys.stdin).get("results",[]); print([(x["ticker"],x.get("currency_name"),x.get("primary_exchange")) for x in r])'
  sleep 15
done
# then, for the ticker whose currency_name is not "usd":
curl -sS "$B?ticker=<TICKER>&apiKey=$MASSIVE_API_KEY" -o nonusd-<TICKER>-$STAMP.json
```

If every candidate is USD, widen the search rather than inventing one; the §7 currency test needs a real non-USD listing. If no non-USD US listing exists on this endpoint, stop and report that to the user — do not substitute a fabricated body (CLAUDE.md "No synthetic data").

- [ ] 3. Verify each body is a real JSON envelope (`status`, `count`, `results`) and not an error page, then hash:

```bash
for f in *.json; do python3 -m json.tool "$f" >/dev/null || echo "NOT JSON: $f"; done
shasum -a 256 *.json
```

Expected: `aapl-date-1998-01-05-2026-09-15.json`, `dell-active-false-2026-09-15.json` and `aamrq-active-false-2026-09-15.json` carry `"count": 0` and an empty or absent `results`. `yhoo-active-false-2026-09-15.json` carries `delisted_utc` 2017-06-19. If a body contains an API error instead, fix the key/params and recapture — a fixture must be a real response.

- [ ] 4. Copy them into the worktree (from the MacBook):

```bash
mkdir -p /Users/chenxi/projects/livewire/.worktrees/security-master-backfill/tests/fixtures/massive_reference
scp 'macmini:/tmp/massive_reference/*.json' \
  /Users/chenxi/projects/livewire/.worktrees/security-master-backfill/tests/fixtures/massive_reference/
```

- [ ] 5. Write the README that makes each file auditable:

```markdown
# Frozen Massive reference responses

Exact bodies of `GET https://api.polygon.io/v3/reference/tickers`, recorded from
the Mac mini with the production key on 2026-09-15 (spec
`docs/superpowers/specs/2026-09-15-security-master-backfill-design.md` §2, §7).
Real tickers, real values, captured once — tests never hit the network.
Re-record only by rerunning the capture in Task 0 of the plan and updating this
table; never hand-edit a body.

| file                                   | request                                             | as-of      | sha256  |
| -------------------------------------- | --------------------------------------------------- | ---------- | ------- |
| aapl-active-2026-09-15.json            | `?ticker=AAPL`                                      | 2026-09-15 | `<sha>` |
| aapl-date-2010-01-04-2026-09-15.json   | `?ticker=AAPL&date=2010-01-04`                      | 2026-09-15 | `<sha>` |
| aapl-date-1998-01-05-2026-09-15.json   | `?ticker=AAPL&date=1998-01-05` (count 0)            | 2026-09-15 | `<sha>` |
| yhoo-active-false-2026-09-15.json      | `?ticker=YHOO&active=false`                         | 2026-09-15 | `<sha>` |
| aaba-active-2026-09-15.json            | `?ticker=AABA`                                      | 2026-09-15 | `<sha>` |
| wlp-active-false-2026-09-15.json       | `?ticker=WLP&active=false`                          | 2026-09-15 | `<sha>` |
| dell-active-false-2026-09-15.json      | `?ticker=DELL&active=false` (count 0)               | 2026-09-15 | `<sha>` |
| aamrq-active-false-2026-09-15.json     | `?ticker=AAMRQ&active=false` (count 0)              | 2026-09-15 | `<sha>` |
| imo-active-2026-09-15.json             | `?ticker=IMO` (XASE listing)                        | 2026-09-15 | `<sha>` |
| nonusd-<TICKER>-active-2026-09-15.json | `?ticker=<TICKER>` (currency `<CCY>`, MIC `<MIC>`)  | 2026-09-15 | `<sha>` |
| delisted-earliest-2026-09-15.json      | `?active=false&sort=delisted_utc&order=asc&limit=1` | 2026-09-15 | `<sha>` |
```

Replace every `<sha>` with the digest from step 3 and every `<TICKER>`/`<CCY>`/`<MIC>` with the real values from step 2.

- [ ] 6. Milestone commit:

```bash
git add tests/fixtures/massive_reference
git commit -m "test(fixtures): freeze real Massive reference responses for the identity backfill

Bodies recorded from the mini on 2026-09-15 with the production key; every
later test loads these by name so no test touches the network."
```

---

## Task 1 — Two scoped declared constants

**Files**

- Modify: `clients/constants.py` (`DECLARED` dict, after `"massive_requests_per_minute/fx"`)
- Test: `tests/test_constants.py` (append)

**Interfaces**

- Consumes: nothing.
- Produces: `constants.declared("massive_requests_per_minute/reference") -> 5.0`, `constants.declared("massive_backoff_s/reference") -> 60.0`; overrides `LW_DECLARED_MASSIVE_REQUESTS_PER_MINUTE_REFERENCE` and `LW_DECLARED_MASSIVE_BACKOFF_S_REFERENCE`.

### Steps

- [ ] 1. Write the failing test. Append to `tests/test_constants.py`:

```python
def test_the_reference_endpoint_rate_limit_is_scoped_like_the_fx_one():
    """Every rate-limit number in this repo carries a scope
    (pm:2026-07-27-fx-dxy-provider-floors). The /v3/reference/tickers limit is
    not the FX one and must not be read off the FX key."""
    assert constants.DECLARED["massive_requests_per_minute/reference"] == (5, "per_min")
    assert constants.DECLARED["massive_backoff_s/reference"] == (60, "s")
    assert constants.split_scope("massive_requests_per_minute/reference") == (
        "massive_requests_per_minute",
        "reference",
    )
    assert constants.split_scope("massive_backoff_s/reference") == ("massive_backoff_s", "reference")
    # the FX scope is untouched
    assert constants.declared("massive_requests_per_minute/fx") == 5


def test_the_reference_pacing_constants_take_their_own_env_override(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_MASSIVE_REQUESTS_PER_MINUTE_REFERENCE", "30")
    monkeypatch.setenv("LW_DECLARED_MASSIVE_BACKOFF_S_REFERENCE", "90")
    assert constants.declared("massive_requests_per_minute/reference") == 30
    assert constants.declared("massive_backoff_s/reference") == 90
    assert constants.declared("massive_requests_per_minute/fx") == 5
```

- [ ] 2. Run it and see it fail:

```bash
uv run pytest tests/test_constants.py -k reference -x -q
```

Expected failure: `KeyError: 'massive_requests_per_minute/reference'`.

- [ ] 3. Implement. In `clients/constants.py`, directly under the `"massive_requests_per_minute/fx"` entry:

```python
    # Massive REST /v3/reference/tickers. Its published limit is unknown; 5/min
    # is the only Massive REST rate this repo has measured, and it is FX-scoped,
    # so this scope starts at the same number and is re-measured by the first
    # `--tickers` sample run on the mini (spec §2). Pace the full backfill with
    # LW_DECLARED_MASSIVE_REQUESTS_PER_MINUTE_REFERENCE set to what that run saw.
    "massive_requests_per_minute/reference": (5, "per_min"),
    # The single 429 backoff for that endpoint; a second 429 is a fetch failure,
    # not a longer wait.
    "massive_backoff_s/reference": (60, "s"),
```

- [ ] 4. Run and see it pass:

```bash
uv run pytest tests/test_constants.py -q
```

Expected: all pass, including `test_every_declared_entry_is_a_value_and_a_nonempty_unit` and `test_env_keys_are_unique_across_declared`.

- [ ] 5. Milestone commit:

```bash
git add clients/constants.py tests/test_constants.py
git commit -m "feat(constants): declare the Massive reference-endpoint rate and backoff, scoped

The 5/min this repo measured is FX-scoped; the identity backfill needs its own
scope so the mini sample run can override it without touching FX."
```

---

## Task 2 — `fetch_ticker_identity` in `clients/universe_client.py`

**Files**

- Modify: `clients/universe_client.py` (add `IdentityRecord`, `IdentityRecords`, `_REFERENCE_URL`, `fetch_ticker_identity`; `UniverseFetchError` gains an optional `status_code`)
- Test: `tests/test_universe_client.py` (append `TestFetchTickerIdentity`)

**Interfaces**

- Consumes: `GET {_POLYGON_BASE}/v3/reference/tickers` with `ticker`, optionally `active=false` or `date=`, plus `apiKey`.
- Produces:
  - `IdentityRecord(ticker: str, name: str | None, cik: str | None, composite_figi: str | None, share_class_figi: str | None, mic: str | None, currency: str | None, list_date: str | None, delisted_utc: str | None, existed_at: str | None)`
  - `IdentityRecords(responses: list[bytes], records: list[IdentityRecord])`
  - `fetch_ticker_identity(ticker: str, api_key: str | None = None, *, probe_date: str | None = None) -> IdentityRecords`
  - `UniverseFetchError` with `.status_code: int | None`

`clients/__init__.py` is not touched: it exports client _classes_ (`BronzeClient`, `MassiveClient`, …) and no `universe_client` symbol is in `__all__` today — every caller imports from `clients.universe_client` directly, as `membership_sync.py` does.

### Steps

- [ ] 1. Write the failing tests. Append to `tests/test_universe_client.py`:

```python
# ── Frozen Massive reference bodies (tests/fixtures/massive_reference) ──────

MASSIVE_REFERENCE = Path(__file__).parent / "fixtures" / "massive_reference"
REFERENCE_URL = "https://api.polygon.io/v3/reference/tickers"


def _fixture(name: str) -> bytes:
    """Load one frozen real Massive response body by file name."""
    return (MASSIVE_REFERENCE / name).read_bytes()


class TestFetchTickerIdentity:
    @responses.activate
    def test_an_active_ticker_needs_no_date_probe(self):
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aapl-active-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aapl-date-1998-01-05-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("AAPL", api_key="test-key", probe_date="2010-01-04")

        # two calls only: active, then active=false. The record carries list_date.
        assert len(responses.calls) == 2
        assert responses.calls[0].request.params.get("active") is None
        assert responses.calls[1].request.params["active"] == "false"
        assert "date" not in responses.calls[1].request.params
        assert len(result.responses) == 2
        record = result.records[0]
        assert record.ticker == "AAPL"
        assert record.list_date is not None
        assert record.existed_at is None
        assert record.mic == "XNAS"  # primary_exchange is already a MIC, used as-is
        assert record.currency == "USD"  # currency_name uppercased

    @responses.activate
    def test_the_date_probe_runs_only_when_no_record_carries_a_list_date(self):
        # Both listing calls empty, so the date probe is the third call and its
        # record is stamped existed_at.
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aapl-date-2010-01-04-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("DELL", api_key="test-key", probe_date="2010-01-04")

        assert len(responses.calls) == 3
        assert responses.calls[2].request.params["date"] == "2010-01-04"
        assert len(result.responses) == 3
        assert [record.existed_at for record in result.records] == ["2010-01-04"]

    @responses.activate
    def test_without_a_probe_date_there_is_no_third_call(self):
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("DELL", api_key="test-key")

        assert len(responses.calls) == 2
        assert result.records == []

    @responses.activate
    def test_a_ticker_unknown_to_massive_is_an_empty_list_not_an_exception(self):
        for _ in range(3):
            responses.add(responses.GET, REFERENCE_URL, body=_fixture("aamrq-active-false-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("AAMRQ", api_key="test-key", probe_date="2010-01-04")

        assert result.records == []
        assert len(result.responses) == 3

    @responses.activate
    def test_a_404_is_an_empty_result_not_an_exception(self):
        responses.add(responses.GET, REFERENCE_URL, status=404)
        responses.add(responses.GET, REFERENCE_URL, status=404)

        assert fetch_ticker_identity("NOSUCH", api_key="test-key").records == []

    def test_a_transport_error_raises(self, monkeypatch):
        """The fetch_batch rule's twin: an outage must not read as 'unknown ticker'."""
        import requests as req_lib

        def mock_get(*args, **kwargs):
            raise req_lib.exceptions.ConnectionError("network down")

        monkeypatch.setattr(req_lib, "get", mock_get)
        with pytest.raises(UniverseFetchError, match="AAPL"):
            fetch_ticker_identity("AAPL", api_key="test-key")

    @responses.activate
    def test_a_5xx_raises_and_carries_its_status_code(self):
        responses.add(responses.GET, REFERENCE_URL, status=503)
        with pytest.raises(UniverseFetchError) as excinfo:
            fetch_ticker_identity("AAPL", api_key="test-key")
        assert excinfo.value.status_code == 503

    @responses.activate
    def test_a_429_raises_and_is_recognisable_as_a_rate_limit(self):
        responses.add(responses.GET, REFERENCE_URL, status=429)
        with pytest.raises(UniverseFetchError) as excinfo:
            fetch_ticker_identity("AAPL", api_key="test-key")
        assert excinfo.value.status_code == 429

    def test_no_api_key_raises(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        with pytest.raises(UniverseFetchError, match="MASSIVE_API_KEY"):
            fetch_ticker_identity("AAPL")

    @responses.activate
    def test_a_delisted_record_carries_its_delisting_date(self):
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("yhoo-active-false-2026-09-15.json"), status=200)

        record = fetch_ticker_identity("YHOO", api_key="test-key").records[0]

        assert record.ticker == "YHOO"
        assert record.delisted_utc.startswith("2017-06-19")
        assert record.composite_figi is not None

    @responses.activate
    def test_an_nyse_american_listing_keeps_its_mic(self):
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("imo-active-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)

        assert fetch_ticker_identity("IMO", api_key="test-key").records[0].mic == "XASE"
```

Add `from pathlib import Path` and `fetch_ticker_identity` to the module's imports at the top of `tests/test_universe_client.py`.

- [ ] 2. Run and see it fail:

```bash
uv run pytest tests/test_universe_client.py -k FetchTickerIdentity -x -q
```

Expected failure: `ImportError: cannot import name 'fetch_ticker_identity' from 'clients.universe_client'`.

- [ ] 3. Implement. In `clients/universe_client.py`, change `UniverseFetchError` and add the new code below `check_ticker_status`:

```python
class UniverseFetchError(Exception):
    """Failed to fetch index constituent data.

    `status_code` is the provider's HTTP status when the failure was one (a 429
    is a pacing decision for the caller, a 5xx is a fetch failure); it is None
    for a transport error or a missing key.
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
```

```python
_REFERENCE_URL = f"{_POLYGON_BASE}/v3/reference/tickers"


@dataclass(frozen=True)
class IdentityRecord:
    """One Massive `/v3/reference/tickers` listing row, field-for-field.

    `existed_at` is the `date=` probe date when the record came back from that
    probe and from nowhere else — the only proof of a start date this provider
    gives for a record with no `list_date`.
    """

    ticker: str
    name: str | None
    cik: str | None
    composite_figi: str | None
    share_class_figi: str | None
    mic: str | None
    currency: str | None
    list_date: str | None
    delisted_utc: str | None
    existed_at: str | None


@dataclass(frozen=True)
class IdentityRecords:
    """The exact response bytes and the parsed records of one identity fetch.

    The bytes are returned untouched so the caller commits them to the evidence
    CAS; nothing here writes.
    """

    responses: list[bytes]
    records: list[IdentityRecord]


def _reference_page(ticker: str, key: str, params: dict[str, str]) -> tuple[bytes, list[dict]]:
    """One `/v3/reference/tickers` list call: exact bytes plus its results.

    A transport failure or a 5xx raises; a 404 or an empty `results` is data,
    never an exception — the twin of the `fetch_batch` rule, so a provider
    outage can never read as "this ticker does not exist".
    """
    try:
        resp = requests.get(
            _REFERENCE_URL,
            params={"ticker": ticker.upper(), **params, "apiKey": key},
            timeout=_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise UniverseFetchError(f"Massive reference lookup failed for {ticker}: {exc}") from exc
    if resp.status_code == 404:
        return resp.content, []
    try:
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise UniverseFetchError(
            f"Massive reference lookup failed for {ticker}: {exc}",
            status_code=resp.status_code,
        ) from exc
    return resp.content, list(resp.json().get("results") or [])


def _identity_record(row: dict, ticker: str, existed_at: str | None) -> IdentityRecord:
    currency = row.get("currency_name")
    return IdentityRecord(
        ticker=(row.get("ticker") or ticker).upper(),
        name=row.get("name"),
        cik=row.get("cik"),
        composite_figi=row.get("composite_figi"),
        share_class_figi=row.get("share_class_figi"),
        # primary_exchange is already a MIC in Massive's payload (XNAS, XNYS,
        # ARCX, XASE, BATS) — used as-is, never remapped.
        mic=row.get("primary_exchange"),
        currency=None if currency is None else currency.upper(),
        list_date=row.get("list_date"),
        delisted_utc=row.get("delisted_utc"),
        existed_at=existed_at,
    )


def _identity_key(record: IdentityRecord) -> tuple:
    return (record.composite_figi, record.share_class_figi, record.mic, record.delisted_utc)


def fetch_ticker_identity(
    ticker: str,
    api_key: str | None = None,
    *,
    probe_date: str | None = None,
) -> IdentityRecords:
    """Fetch one ticker's listing identity from Massive reference data.

    Three calls at most, in order: the active listing, the delisted listing,
    and — only when neither carries a `list_date` and `probe_date` is given —
    a historical `date=` probe, whose records are stamped `existed_at`. That
    probe is the only start date available for a record Massive lists without
    one; an empty probe proves nothing and appends nothing (spec §4).

    This is the list form of the endpoint with query parameters, a different
    code path from `check_ticker_status`'s single-ticker `/{T}` path form.
    """
    key = api_key or os.environ.get("MASSIVE_API_KEY")
    if not key:
        raise UniverseFetchError("MASSIVE_API_KEY required for ticker identity lookup")

    bodies: list[bytes] = []
    records: list[IdentityRecord] = []
    seen: set[tuple] = set()
    for params in ({}, {"active": "false"}):
        body, rows = _reference_page(ticker, key, params)
        bodies.append(body)
        for row in rows:
            record = _identity_record(row, ticker, None)
            if _identity_key(record) not in seen:
                seen.add(_identity_key(record))
                records.append(record)

    if probe_date is not None and not any(record.list_date for record in records):
        body, rows = _reference_page(ticker, key, {"date": probe_date})
        bodies.append(body)
        for row in rows:
            probed = _identity_record(row, ticker, probe_date)
            index = next(
                (i for i, known in enumerate(records) if _identity_key(known) == _identity_key(probed)),
                None,
            )
            if index is None:
                seen.add(_identity_key(probed))
                records.append(probed)
            else:
                # Same listing, now with a proven date it existed on.
                records[index] = replace(records[index], existed_at=probe_date)

    return IdentityRecords(responses=bodies, records=records)
```

Add `from dataclasses import dataclass, replace` to the module imports (it currently imports `dataclass` only).

- [ ] 4. Run and see it pass:

```bash
uv run pytest tests/test_universe_client.py -q
```

Expected: all pass, including the pre-existing `TestCheckTickerStatus` tests — `UniverseFetchError` keeps its single-positional-argument construction everywhere it is raised today.

- [ ] 5. Milestone commit:

```bash
git add clients/universe_client.py tests/test_universe_client.py
git commit -m "feat(universe): fetch one ticker's Massive listing identity

Three calls at most (active, delisted, then a date= probe only when no record
carries a list_date), exact response bytes returned for the evidence CAS. A
transport error or 5xx raises with its status code; a 404 or empty results is
data, so an outage can never read as 'unknown ticker'."
```

---

## Task 3 — `derive_identity_events`: the §4 table as one pure function

**Files**

- Create: `livewire_scripts/security_master_sync.py` (derivation only; Task 4 adds the entrypoint to the same module)
- Create: `tests/test_security_master_sync.py`

**Interfaces**

- Consumes: `list[IdentityRecord]`, the master's current events (`SecurityMaster.events()`), `now: datetime`, `evidence_refs: tuple[tuple[str, str], ...]` (ref, sha256 pairs).
- Produces: `derive_identity_events(records, existing_master_rows, now, evidence_refs) -> DerivedIdentities(events: list[SecurityIdentityEvent], counts: dict[str, int])` with counts keyed `identity_candidate`, `identity_no_start`, `identity_conflict`, `identity_unknown_to_provider`. No I/O: it never reads or writes the lake.

### Steps

- [ ] 1. Write the failing tests. Create `tests/test_security_master_sync.py`:

```python
"""Tests for livewire_scripts/security_master_sync.py — Massive identities → SecurityMaster."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.universe_client import IdentityRecord, fetch_ticker_identity
from livewire_scripts import security_master_sync

MASSIVE_REFERENCE = Path(__file__).parent / "fixtures" / "massive_reference"
NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
HASH = "b" * 64
REFS = ((f"artifact://sha256/{HASH}", HASH),)


def _fixture(name: str) -> bytes:
    return (MASSIVE_REFERENCE / name).read_bytes()


def _records(name: str, *, existed_at: str | None = None) -> list[IdentityRecord]:
    """Parse a frozen body into IdentityRecords the way fetch_ticker_identity does."""
    rows = json.loads(_fixture(name)).get("results") or []
    return [
        IdentityRecord(
            ticker=row["ticker"],
            name=row.get("name"),
            cik=row.get("cik"),
            composite_figi=row.get("composite_figi"),
            share_class_figi=row.get("share_class_figi"),
            mic=row.get("primary_exchange"),
            currency=(row.get("currency_name") or "").upper() or None,
            list_date=row.get("list_date"),
            delisted_utc=row.get("delisted_utc"),
            existed_at=existed_at,
        )
        for row in rows
    ]


def verifies(ref: str, digest: str) -> bool:
    return ref == f"artifact://sha256/{digest}" and len(digest) == 64


def _master(tmp_path: Path) -> SecurityMaster:
    return SecurityMaster(tmp_path / "lake", evidence_verifier=verifies)


def _derive(records, existing=(), now=NOW):
    return security_master_sync.derive_identity_events(list(records), list(existing), now, REFS)


def test_one_record_with_a_figi_is_one_verified_interval(tmp_path):
    derived = _derive(_records("aapl-active-2026-09-15.json"))

    assert len(derived.events) == 1
    event = derived.events[0]
    assert event.status == "verified"
    assert event.continuity_basis == "provider_figi"
    assert event.symbol == "AAPL"
    assert event.exchange_mic == "XNAS"
    assert event.effective_to is None
    assert derived.counts["identity_candidate"] == 0

    master = _master(tmp_path)
    assert master.append(event) is True
    assert master.resolve_symbol("massive", "AAPL", "XNAS", datetime(2015, 1, 2, tzinfo=UTC), NOW) == event.security_id


def test_a_rename_is_one_security_id_with_two_symbol_rows(tmp_path):
    """YHOO → AABA: same composite and share-class FIGI, disjoint Massive dates."""
    records = _records("yhoo-active-false-2026-09-15.json") + _records("aaba-active-2026-09-15.json")
    derived = _derive(records)

    assert len({event.security_id for event in derived.events}) == 1
    assert sorted(event.revision for event in derived.events) == [1, 2]
    assert {event.symbol for event in derived.events} == {"YHOO", "AABA"}
    assert all(event.supersedes is None for event in derived.events)
    assert derived.counts["identity_conflict"] == 0

    master = _master(tmp_path)
    for event in derived.events:
        assert master.append(event) is True
    yhoo = master.resolve_symbol("massive", "YHOO", "XNAS", datetime(2010, 1, 4, tzinfo=UTC), NOW)
    aaba = master.resolve_symbol("massive", "AABA", "XNAS", datetime(2018, 1, 4, tzinfo=UTC), NOW)
    assert yhoo is not None and yhoo == aaba


def test_matching_figis_with_overlapping_dates_are_two_unresolved_rows(tmp_path):
    left = _records("yhoo-active-false-2026-09-15.json")[0]
    overlapping = [left, left.__class__(**{**left.__dict__, "ticker": "AABA", "delisted_utc": None})]
    derived = _derive(overlapping)

    assert len({event.security_id for event in derived.events}) == 2
    assert {event.status for event in derived.events} == {"unresolved"}
    assert derived.counts["identity_conflict"] == 1
    # nothing is inferred about which date is wrong: neither resolves
    master = _master(tmp_path)
    for event in derived.events:
        master.append(event)
    assert master.resolve_symbol("massive", "YHOO", "XNAS", datetime(2010, 1, 4, tzinfo=UTC), NOW) is None


def test_a_differing_share_class_figi_is_a_material_conflict(tmp_path):
    base = _records("aapl-active-2026-09-15.json")[0]
    other = base.__class__(**{**base.__dict__, "ticker": "AAPL", "share_class_figi": "BBG001S5N8V8"})
    derived = _derive([base, other])

    assert len({event.security_id for event in derived.events}) == 2
    assert {event.status for event in derived.events} == {"unresolved"}
    assert derived.counts["identity_conflict"] == 1


def test_a_missing_share_class_figi_on_one_side_is_incomplete_not_contradictory():
    base = _records("aapl-active-2026-09-15.json")[0]
    partial = base.__class__(**{**base.__dict__, "share_class_figi": None, "list_date": "1990-01-02"})
    derived = _derive([base, partial])

    assert len({event.security_id for event in derived.events}) == 2
    assert {event.status for event in derived.events} == {"candidate"}
    assert derived.counts["identity_conflict"] == 1


def test_a_different_composite_figi_is_two_ids_with_disjoint_intervals(tmp_path):
    delisted = _records("wlp-active-false-2026-09-15.json")[0]
    reused = _records("aapl-active-2026-09-15.json")[0]
    reused = reused.__class__(**{**reused.__dict__, "ticker": "WLP", "list_date": "2020-01-02"})
    derived = _derive([delisted, reused])

    assert len({event.security_id for event in derived.events}) == 2
    master = _master(tmp_path)
    for event in derived.events:
        assert master.append(event) is True  # the collision check accepts disjoint intervals


def test_an_existing_partial_interval_is_widened_on_the_same_id(tmp_path):
    first = _derive(_records("aapl-active-2026-09-15.json")).events[0]
    narrowed = SecurityIdentityEvent(
        **{**first.__dict__, "effective_to": datetime(2015, 1, 1, tzinfo=UTC)}
    )
    master = _master(tmp_path)
    master.append(narrowed)

    derived = _derive(_records("aapl-active-2026-09-15.json"), existing=master.events())

    assert len(derived.events) == 1
    widened = derived.events[0]
    assert widened.security_id == narrowed.security_id
    assert widened.revision == 2
    assert widened.supersedes == narrowed.event_id
    assert widened.effective_to is None
    assert master.append(widened) is True


def test_a_covered_interval_derives_nothing(tmp_path):
    event = _derive(_records("aapl-active-2026-09-15.json")).events[0]
    master = _master(tmp_path)
    master.append(event)

    assert _derive(_records("aapl-active-2026-09-15.json"), existing=master.events()).events == []


def test_a_record_without_any_figi_is_candidate_and_does_not_resolve(tmp_path):
    base = _records("aapl-active-2026-09-15.json")[0]
    bare = base.__class__(**{**base.__dict__, "composite_figi": None, "share_class_figi": None})
    derived = _derive([bare])

    assert derived.events[0].status == "candidate"
    assert derived.events[0].continuity_basis == "provider_reference"
    assert derived.counts["identity_candidate"] == 1

    master = _master(tmp_path)
    master.append(derived.events[0])
    assert master.resolve_symbol("massive", "AAPL", "XNAS", NOW, NOW) is None


def test_no_list_date_and_an_empty_probe_appends_nothing():
    base = _records("wlp-active-false-2026-09-15.json")[0]
    undated = base.__class__(**{**base.__dict__, "list_date": None, "existed_at": None})
    derived = _derive([undated])

    assert derived.events == []
    assert derived.counts["identity_no_start"] == 1


def test_an_empty_probe_does_not_backdate_a_delisted_record():
    """A start of known_at would invent a date and, for a delisted record, an
    interval that ends before it starts — which the master rejects."""
    base = _records("yhoo-active-false-2026-09-15.json")[0]
    undated = base.__class__(**{**base.__dict__, "list_date": None})
    assert _derive([undated]).events == []


def test_a_probed_start_is_used_when_there_is_no_list_date():
    base = _records("wlp-active-false-2026-09-15.json")[0]
    probed = base.__class__(**{**base.__dict__, "list_date": None, "existed_at": "2010-01-04"})
    event = _derive([probed]).events[0]
    assert event.effective_from == datetime(2010, 1, 4, tzinfo=UTC)


def test_no_record_at_all_is_counted_unknown_to_provider():
    derived = _derive(_records("aamrq-active-false-2026-09-15.json"))
    assert derived.events == []
    assert derived.counts["identity_unknown_to_provider"] == 1


def test_a_nyse_american_identity_resolves_under_xase(tmp_path):
    event = _derive(_records("imo-active-2026-09-15.json")).events[0]
    master = _master(tmp_path)
    master.append(event)
    assert master.resolve_symbol("massive", "IMO", "XASE", NOW, NOW) == event.security_id
```

When the non-USD fixture's ticker is known from Task 0, the currency assertion lives in Task 6's test, not here.

- [ ] 2. Run and see it fail:

```bash
uv run pytest tests/test_security_master_sync.py -x -q
```

Expected failure: `ModuleNotFoundError: No module named 'livewire_scripts.security_master_sync'`.

- [ ] 3. Implement. Create `livewire_scripts/security_master_sync.py`:

```python
"""Backfill security identities from Massive reference data.

`membership_sync` writes `unresolved:<ticker>` placeholders for every index
member with no identity; the master held one verified row, so every PIT
membership query answered the empty set. This module turns Massive's
`/v3/reference/tickers` listings into evidence-backed `SecurityMaster`
intervals so those placeholders can be reresolved.

`derive_identity_events` is pure: it reads no lake and writes nothing. The
identity rules it encodes are the table in
`docs/superpowers/specs/2026-09-15-security-master-backfill-design.md` §4.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time

from clients.security_master import SecurityIdentityEvent
from clients.universe_client import IdentityRecord

_PROVIDER = "massive"

COUNT_NAMES = (
    "identity_candidate",
    "identity_no_start",
    "identity_conflict",
    "identity_unknown_to_provider",
)


@dataclass(frozen=True)
class DerivedIdentities:
    events: list[SecurityIdentityEvent]
    counts: dict[str, int] = field(default_factory=dict)


def _midnight(value: str) -> datetime:
    """`2010-01-04` or `2017-06-19T00:00:00Z` -> an aware UTC timestamp."""
    return datetime.combine(date.fromisoformat(value[:10]), time.min, tzinfo=UTC)


def _start_of(record: IdentityRecord) -> datetime | None:
    """`list_date`, else the date the `date=` probe proved it existed, else None.

    An empty probe proves nothing about the past, and `known_at` would be an
    invented date — for a delisted record, one that ends before it starts.
    """
    if record.list_date:
        return _midnight(record.list_date)
    if record.existed_at:
        return _midnight(record.existed_at)
    return None


def _end_of(record: IdentityRecord) -> datetime | None:
    return _midnight(record.delisted_utc) if record.delisted_utc else None


def _event_id(security_id: str, record: IdentityRecord, start: datetime, status: str) -> str:
    payload = "\x00".join(
        [
            security_id,
            _PROVIDER,
            record.ticker,
            record.mic or "",
            record.composite_figi or "",
            record.share_class_figi or "",
            start.date().isoformat(),
            (record.delisted_utc or "")[:10],
            status,
        ]
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _build(
    *,
    security_id: str,
    revision: int,
    record: IdentityRecord,
    start: datetime,
    end: datetime | None,
    status: str,
    now: datetime,
    refs: tuple[tuple[str, str], ...],
    supersedes: str | None = None,
) -> SecurityIdentityEvent:
    cik = record.cik.zfill(10) if record.cik and record.cik.isdigit() else None
    basis = "provider_figi" if (record.composite_figi or record.share_class_figi) else "provider_reference"
    return SecurityIdentityEvent(
        event_id=_event_id(security_id, record, start, status),
        security_id=security_id,
        revision=revision,
        symbol=record.ticker,
        provider=_PROVIDER,
        exchange_mic=record.mic,
        # Massive omits currency_name on some rows; every MIC this backfill
        # touches (XNAS/XNYS/ARCX/XASE) is a US venue, so USD is the listing
        # currency, not a guess about the instrument.
        currency=record.currency or "USD",
        effective_from=start,
        effective_to=end,
        known_at=now,
        # Massive omits `name` on some historical rows (spec §2); the ticker is
        # the only issuer label the provider gave, and the field is non-nullable.
        issuer_name=record.name or record.ticker,
        cik=cik,
        composite_figi=record.composite_figi,
        share_class_figi=record.share_class_figi,
        continuity_basis=basis,
        relationship_type=None,
        related_security_id=None,
        source_refs=tuple(ref for ref, _ in refs),
        source_hashes=tuple(digest for _, digest in refs),
        status=status,
        supersedes=supersedes,
    )


def _current(existing: list[SecurityIdentityEvent]) -> list[SecurityIdentityEvent]:
    superseded = {item.supersedes for item in existing if item.supersedes is not None}
    return [item for item in existing if item.event_id not in superseded]


def _existing_match(
    record: IdentityRecord, existing: list[SecurityIdentityEvent]
) -> SecurityIdentityEvent | None:
    """The master's current verified row for the same listing and the same FIGIs.

    A second `security_id` for this listing would be rejected by the master's
    FIGI collision check, so the only correct move is a new revision on the
    existing id (spec §4).
    """
    return next(
        (
            item
            for item in _current(existing)
            if item.status == "verified"
            and (item.provider, item.symbol, item.exchange_mic) == (_PROVIDER, record.ticker, record.mic)
            and item.composite_figi == record.composite_figi
            and item.share_class_figi == record.share_class_figi
        ),
        None,
    )


def _widen(
    match: SecurityIdentityEvent,
    record: IdentityRecord,
    start: datetime,
    end: datetime | None,
    now: datetime,
    refs: tuple[tuple[str, str], ...],
) -> SecurityIdentityEvent | None:
    covers_start = match.effective_from <= start
    covers_end = match.effective_to is None or (end is not None and end <= match.effective_to)
    if covers_start and covers_end:
        return None
    widened_end = None if match.effective_to is None or end is None else max(match.effective_to, end)
    return _build(
        security_id=match.security_id,
        revision=match.revision + 1,
        record=record,
        start=min(match.effective_from, start),
        end=widened_end,
        status="verified",
        now=now,
        refs=refs,
        supersedes=match.event_id,
    )


def _one_record(
    record: IdentityRecord,
    start: datetime,
    existing: list[SecurityIdentityEvent],
    now: datetime,
    refs: tuple[tuple[str, str], ...],
    counts: dict[str, int],
    new_id,
) -> list[SecurityIdentityEvent]:
    end = _end_of(record)
    if not (record.composite_figi or record.share_class_figi):
        counts["identity_candidate"] += 1
        return [
            _build(
                security_id=new_id(),
                revision=1,
                record=record,
                start=start,
                end=end,
                status="candidate",
                now=now,
                refs=refs,
            )
        ]
    match = _existing_match(record, existing)
    if match is not None:
        widened = _widen(match, record, start, end, now, refs)
        return [widened] if widened is not None else []
    return [
        _build(
            security_id=new_id(),
            revision=1,
            record=record,
            start=start,
            end=end,
            status="verified",
            now=now,
            refs=refs,
        )
    ]


def _split_ids(
    group: list[tuple[IdentityRecord, datetime]],
    status: str,
    now: datetime,
    refs: tuple[tuple[str, str], ...],
    new_id,
) -> list[SecurityIdentityEvent]:
    """One `security_id` per record. Same-id rows would bypass the master's
    collision checks, so a conflicted group never shares an id."""
    return [
        _build(
            security_id=new_id(),
            revision=1,
            record=record,
            start=start,
            end=_end_of(record),
            status=status,
            now=now,
            refs=refs,
        )
        for record, start in group
    ]


def derive_identity_events(
    records: list[IdentityRecord],
    existing_master_rows: list[SecurityIdentityEvent],
    now: datetime,
    evidence_refs: tuple[tuple[str, str], ...],
    *,
    new_security_id=None,
) -> DerivedIdentities:
    """Turn one ticker's Massive listings into master rows, per spec §4.

    `new_security_id` defaults to `SecurityMaster.new_security_id` — opaque ids,
    never derived from FIGI or ticker (contract lines 12-15). It is a parameter
    only so a test can make the ids deterministic.
    """
    from clients.security_master import SecurityMaster

    new_id = new_security_id or SecurityMaster.new_security_id
    counts = dict.fromkeys(COUNT_NAMES, 0)
    if not records:
        counts["identity_unknown_to_provider"] = 1
        return DerivedIdentities([], counts)

    usable: list[tuple[IdentityRecord, datetime]] = []
    for record in records:
        start = _start_of(record)
        if start is None:
            counts["identity_no_start"] += 1
            continue
        if not record.mic:
            # No venue, so no listing any of the four resolve MICs can name.
            counts["identity_unknown_to_provider"] += 1
            continue
        usable.append((record, start))
    if not usable:
        return DerivedIdentities([], counts)

    groups: dict[str, list[tuple[IdentityRecord, datetime]]] = {}
    for index, (record, start) in enumerate(usable):
        # A record with no composite FIGI joins nothing; key it uniquely.
        groups.setdefault(record.composite_figi or f"\x00{index}", []).append((record, start))

    events: list[SecurityIdentityEvent] = []
    for group in groups.values():
        if len(group) == 1:
            record, start = group[0]
            events.extend(_one_record(record, start, existing_master_rows, now, evidence_refs, counts, new_id))
            continue
        share_classes = {record.share_class_figi for record, _ in group}
        if len(group) > 2 or (len(share_classes) > 1 and None not in share_classes):
            # Differing share classes under one composite FIGI (or more listings
            # than this rule describes) is a material FIGI conflict, contract
            # rule 7: two ids, both unresolved, nothing inferred.
            counts["identity_conflict"] += 1
            events.extend(_split_ids(group, "unresolved", now, evidence_refs, new_id))
            continue
        if None in share_classes:
            # Incomplete rather than contradictory: a composite FIGI alone does
            # not prove continuity (contract priority 3).
            counts["identity_conflict"] += 1
            events.extend(_split_ids(group, "candidate", now, evidence_refs, new_id))
            continue
        ordered = sorted(group, key=lambda item: item[1])
        (first, first_start), (second, second_start) = ordered
        first_end = _end_of(first)
        if first_end is None or first_end > second_start:
            counts["identity_conflict"] += 1
            events.extend(_split_ids(ordered, "unresolved", now, evidence_refs, new_id))
            continue
        security_id = new_id()
        events.append(
            _build(
                security_id=security_id,
                revision=1,
                record=first,
                start=first_start,
                end=first_end,
                status="verified",
                now=now,
                refs=evidence_refs,
            )
        )
        events.append(
            _build(
                security_id=security_id,
                revision=2,
                record=second,
                start=second_start,
                end=_end_of(second),
                status="verified",
                now=now,
                refs=evidence_refs,
            )
        )
    return DerivedIdentities(events, counts)
```

`replace` is imported for Task 4's use; if ruff flags it as unused at this point, add it in Task 4 instead.

- [ ] 4. Run and see it pass:

```bash
uv run pytest tests/test_security_master_sync.py -q
```

Expected: all pass. If the rename test fails because YHOO and AABA carry different `share_class_figi` values in the real bodies, that is the provider's answer, not a bug — change the test to assert the `identity_conflict` branch and note the fixture's values in a comment. Never edit a fixture to make a test pass.

- [ ] 5. Milestone commit:

```bash
git add livewire_scripts/security_master_sync.py tests/test_security_master_sync.py
git commit -m "feat(security-master): derive identity intervals from Massive listings

Pure function, no I/O: the spec §4 table as code. A rename collapses to one
opaque security_id with two symbol rows; conflicting or incomplete FIGI
evidence yields two ids that deliberately do not resolve; no provable start
appends nothing."
```

---

## Task 4 — `livewire_ingest.py security-master sync`

**Files**

- Modify: `livewire_scripts/security_master_sync.py` (append `sync`, `needed_dates`, `main`)
- Modify: `scripts/livewire_ingest.py` (`COMMANDS` entry, scheduled-env allowlist)
- Test: `tests/test_security_master_sync.py` (append), `tests/test_livewire_entrypoints.py` (append)

**Interfaces**

- Consumes: `IndexMembershipStore.events(index_id)`, `SecurityMaster.resolve_symbol`, `fetch_ticker_identity`, `SourceEvidenceStore.persist_raw` / `record_many`, `membership_sync._evidence_verifier`, `membership_sync._RESOLVE_MICS`, `clients.constants.declared`, `clients.ledger.emit` / `new_run_id`.
- Produces: `sync(*, indexes, tickers=None, data_lake_root, now, fetch_fn=None, sleep_fn=None, dry_run=False) -> int`; `main(argv) -> int` accepting `sync [--index ...] [--tickers ...] [--dry-run]`; ledger `runs(job="security-master-sync")` + the spec §5 `identity_*` measurements.

### Steps

- [ ] 1. Write the failing tests. Append to `tests/test_security_master_sync.py`:

```python
from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.source_evidence import SourceEvidenceStore
from clients.universe_client import IdentityRecords, UniverseFetchError
from livewire_scripts import membership_sync
from scripts import livewire_ingest

PLACEHOLDER_HASH = "c" * 64
PLACEHOLDER_REF = f"artifact://sha256/{PLACEHOLDER_HASH}"


@pytest.fixture(autouse=True)
def _ledger_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))


def _placeholder_lake(tmp_path: Path, pairs: list[tuple[str, str]], index_id: str = "sp500") -> Path:
    """A lake whose index has one unresolved:<ticker> add per (ticker, date)."""
    lake = tmp_path / "lake"
    evidence = SourceEvidenceStore(lake)
    artifact = evidence.persist_raw(b'{"note": "placeholder membership evidence"}\n')
    evidence.record_many([])
    master = SecurityMaster(lake, evidence_verifier=None)
    store = IndexMembershipStore(
        lake, security_master=master, evidence_verifier=lambda ref, digest: ref.endswith(digest)
    )
    revisions: dict[str, int] = {}
    for ticker, day in pairs:
        security_id = f"unresolved:{ticker}"
        revisions[security_id] = revisions.get(security_id, 0) + 1
        store.append(
            MembershipEvent(
                event_id=f"placeholder-{index_id}-{ticker}-{day}",
                index_id=index_id,
                security_id=security_id,
                action="add",
                announced_at=None,
                effective_at=datetime.fromisoformat(day).replace(tzinfo=UTC),
                known_at=datetime(2026, 9, 13, tzinfo=UTC),
                source_refs=(artifact.ref,),
                source_hashes=(artifact.sha256,),
                revision=revisions[security_id],
                supersedes=None,
                status="unresolved",
            )
        )
    return lake


def _fetcher(bodies: dict[str, list[str]], calls: list[tuple[str, str | None]]):
    """A fetch_fn over the frozen bodies, recording (ticker, probe_date)."""

    def fetch(ticker: str, *, probe_date: str | None = None) -> IdentityRecords:
        calls.append((ticker, probe_date))
        names = bodies[ticker]
        return IdentityRecords(
            responses=[_fixture(name) for name in names],
            records=[record for name in names for record in _records(name)],
        )

    return fetch


def _measurements(tmp_path: Path) -> dict[tuple[str, str], float]:
    from clients import ledger

    rows = ledger.read("measurements", root=Path(tmp_path / "ledger"))
    return {(row["name"], row["scope"]): row["value"] for row in rows}


def test_sync_appends_identities_and_resolves_the_placeholder_ticker(tmp_path):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])
    calls: list[tuple[str, str | None]] = []
    fetch = _fetcher({"AAPL": ["aapl-active-2026-09-15.json"]}, calls)

    assert security_master_sync.sync(
        indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None
    ) == 0

    assert calls == [("AAPL", "2010-01-04")]
    master = SecurityMaster(lake, evidence_verifier=None)
    assert master.resolve_symbol("massive", "AAPL", "XNAS", datetime(2010, 1, 4, tzinfo=UTC), NOW) is not None
    assert _measurements(tmp_path)[("identity_tickers_requested", "all")] == 1
    assert _measurements(tmp_path)[("identity_events_appended", "all")] == 1


def test_a_second_run_makes_no_fetch(tmp_path):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])
    calls: list[tuple[str, str | None]] = []
    fetch = _fetcher({"AAPL": ["aapl-active-2026-09-15.json"]}, calls)
    kwargs = dict(indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None)

    security_master_sync.sync(**kwargs)
    calls.clear()
    assert security_master_sync.sync(**kwargs) == 0
    assert calls == []


def test_a_membership_date_past_the_existing_interval_triggers_a_fetch(tmp_path):
    """Covering only the earliest date would skip a ticker whose later
    membership falls past the end of an existing interval, and the widening
    rule could never run (spec §3.2 step 2)."""
    lake = _placeholder_lake(tmp_path, [("WLP", "2005-01-03"), ("WLP", "2020-01-02")])
    calls: list[tuple[str, str | None]] = []
    fetch = _fetcher({"WLP": ["wlp-active-false-2026-09-15.json"]}, calls)
    kwargs = dict(indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None)

    security_master_sync.sync(**kwargs)
    calls.clear()
    # WLP delisted 2014-12-03, so the 2020 membership date is still uncovered
    assert security_master_sync.sync(**kwargs) == 0
    assert calls == [("WLP", "2005-01-03")]


def test_dry_run_appends_nothing(tmp_path):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])
    fetch = _fetcher({"AAPL": ["aapl-active-2026-09-15.json"]}, [])

    assert security_master_sync.sync(
        indexes=["sp500"],
        data_lake_root=lake,
        now=NOW,
        fetch_fn=fetch,
        sleep_fn=lambda _s: None,
        dry_run=True,
    ) == 0
    assert SecurityMaster(lake, evidence_verifier=None).events() == []


def test_a_raising_record_many_appends_nothing_and_exits_one(tmp_path, monkeypatch):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])
    fetch = _fetcher({"AAPL": ["aapl-active-2026-09-15.json"]}, [])

    def boom(self, evidence):
        raise OSError("manifest write failed")

    monkeypatch.setattr(SourceEvidenceStore, "record_many", boom)

    assert security_master_sync.sync(
        indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None
    ) == 1
    assert SecurityMaster(lake, evidence_verifier=None).events() == []


def test_evidence_is_committed_once_per_run(tmp_path, monkeypatch):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04"), ("IMO", "2010-01-04")])
    fetch = _fetcher(
        {"AAPL": ["aapl-active-2026-09-15.json"], "IMO": ["imo-active-2026-09-15.json"]}, []
    )
    commits: list[int] = []
    original = SourceEvidenceStore.record_many
    monkeypatch.setattr(
        SourceEvidenceStore,
        "record_many",
        lambda self, evidence: (commits.append(len(evidence)), original(self, evidence))[1],
    )

    security_master_sync.sync(
        indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None
    )

    assert len(commits) == 1 and commits[0] == 2


def test_a_fetch_failure_is_counted_and_exits_one(tmp_path):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])

    def fetch(ticker: str, *, probe_date: str | None = None):
        raise UniverseFetchError("Massive reference lookup failed for AAPL: boom", status_code=503)

    assert security_master_sync.sync(
        indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None
    ) == 1
    assert _measurements(tmp_path)[("identity_fetch_failed", "all")] == 1


def test_a_429_backs_off_once_then_counts_a_failure(tmp_path):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])
    slept: list[float] = []
    attempts: list[str] = []

    def fetch(ticker: str, *, probe_date: str | None = None):
        attempts.append(ticker)
        raise UniverseFetchError("rate limited", status_code=429)

    assert security_master_sync.sync(
        indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=slept.append
    ) == 1
    assert attempts == ["AAPL", "AAPL"]  # one retry, no more
    assert 60.0 in slept  # massive_backoff_s/reference
    assert _measurements(tmp_path)[("identity_fetch_failed", "all")] == 1


def test_a_ticker_unknown_to_massive_is_counted_and_appends_nothing(tmp_path):
    lake = _placeholder_lake(tmp_path, [("AAMRQ", "2010-01-04")])
    fetch = _fetcher({"AAMRQ": ["aamrq-active-false-2026-09-15.json"]}, [])

    assert security_master_sync.sync(
        indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None
    ) == 0
    assert SecurityMaster(lake, evidence_verifier=None).events() == []
    assert _measurements(tmp_path)[("identity_unknown_to_provider", "all")] == 1


def test_a_master_collision_is_counted_never_swallowed(tmp_path):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])
    fetch = _fetcher({"AAPL": ["aapl-active-2026-09-15.json"]}, [])
    security_master_sync.sync(
        indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None
    )
    # a second identity for the same listing under a fresh id collides on FIGI
    master = SecurityMaster(lake, evidence_verifier=None)
    first = master.events()[0]
    derived = security_master_sync.derive_identity_events(
        _records("aapl-active-2026-09-15.json"), [], NOW, ((first.source_refs[0], first.source_hashes[0]),)
    )
    appended, collisions = security_master_sync._append_all(
        SecurityMaster(lake, evidence_verifier=lambda ref, digest: ref.endswith(digest)), derived.events
    )
    assert appended == 0 and collisions == 1


def test_the_tickers_flag_files_its_measurements_under_subset(tmp_path):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04"), ("IMO", "2010-01-04")])
    calls: list[tuple[str, str | None]] = []
    fetch = _fetcher({"AAPL": ["aapl-active-2026-09-15.json"]}, calls)

    assert security_master_sync.sync(
        indexes=["sp500"],
        tickers=["AAPL"],
        data_lake_root=lake,
        now=NOW,
        fetch_fn=fetch,
        sleep_fn=lambda _s: None,
    ) == 0
    assert [ticker for ticker, _ in calls] == ["AAPL"]
    assert _measurements(tmp_path)[("identity_tickers_requested", "subset")] == 1


def test_the_run_opens_and_closes_a_security_master_sync_row(tmp_path):
    from clients import ledger

    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])
    fetch = _fetcher({"AAPL": ["aapl-active-2026-09-15.json"]}, [])
    security_master_sync.sync(
        indexes=["sp500"], data_lake_root=lake, now=NOW, fetch_fn=fetch, sleep_fn=lambda _s: None
    )

    rows = [row for row in ledger.read("runs", root=Path(tmp_path / "ledger")) if row["job"] == "security-master-sync"]
    assert len(rows) == 2
    terminal = [row for row in rows if row["ended"] is not None]
    assert len(terminal) == 1 and terminal[0]["verdict"] == "OK" and terminal[0]["exit_code"] == 0


def test_sync_dispatches_from_the_real_entrypoint_argv(tmp_path, monkeypatch):
    lake = _placeholder_lake(tmp_path, [("AAPL", "2010-01-04")])
    monkeypatch.setenv("MDW_DATA_LAKE", str(lake))
    monkeypatch.setattr(livewire_ingest, "load_scheduled_env", lambda repo_root: None)
    monkeypatch.setattr(
        security_master_sync,
        "fetch_ticker_identity",
        lambda ticker, key, probe_date=None: IdentityRecords(
            responses=[_fixture("aapl-active-2026-09-15.json")],
            records=_records("aapl-active-2026-09-15.json"),
        ),
    )
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")

    assert livewire_ingest.main(["security-master", "sync", "--index", "sp500", "--dry-run"]) == 0
```

`ledger.read` is the reader the other suites use; if its signature differs, copy the call exactly as `tests/test_status.py` makes it.

Append to `tests/test_livewire_entrypoints.py`, extending the existing parametrize list:

```python
        ("security-master", "livewire_scripts.security_master_sync"),
```

and add:

```python
def test_ingest_knows_the_security_master_command() -> None:
    assert livewire_ingest.COMMANDS["security-master"] == "livewire_scripts.security_master_sync"
    # it is not an IB command: the whole path is Massive REST
    assert "security-master" not in livewire_ingest.IB_COMMANDS
```

- [ ] 2. Run and see it fail:

```bash
uv run pytest tests/test_security_master_sync.py -k "sync or dispatch" -x -q
```

Expected failure: `AttributeError: module 'livewire_scripts.security_master_sync' has no attribute 'sync'`.

- [ ] 3. Implement. Append to `livewire_scripts/security_master_sync.py`:

```python
import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path

from clients import constants, ledger
from clients.index_membership_store import IndexMembershipStore
from clients.security_master import SecurityMaster
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from clients.universe_client import UniverseFetchError, fetch_ticker_identity
from livewire_scripts.membership_sync import DEFAULT_INDEXES, _evidence_verifier, _resolve
from livewire_scripts.paths import data_lake_dir

_SOURCE_URL = "https://api.polygon.io/v3/reference/tickers"

MEASURE_NAMES = (
    "identity_tickers_requested",
    "identity_events_appended",
    "identity_candidate",
    "identity_no_start",
    "identity_conflict",
    "identity_unknown_to_provider",
    "identity_collisions",
    "identity_fetch_failed",
)


def needed_dates(store: IndexMembershipStore, indexes: list[str]) -> dict[str, set[datetime]]:
    """ticker -> the effective dates its current unresolved events need.

    Current means non-superseded; a rejected placeholder chain is skipped, so a
    reresolved index stops asking for identities it already has.
    """
    wanted: dict[str, set[datetime]] = {}
    for index_id in indexes:
        events = store.events(index_id)
        superseded = {item.supersedes for item in events if item.supersedes is not None}
        for event in events:
            if event.event_id in superseded or event.status != "unresolved":
                continue
            if not event.security_id.startswith("unresolved:"):
                continue
            wanted.setdefault(event.security_id.removeprefix("unresolved:"), set()).add(event.effective_at)
    return wanted


def _covered(master: SecurityMaster, ticker: str, dates: set[datetime], now: datetime) -> bool:
    """Every needed date already sits inside a verified interval for this symbol."""
    return all(_resolve(master, ticker, effective_at, now) is not None for effective_at in dates)


def _append_all(master: SecurityMaster, events: list[SecurityIdentityEvent]) -> tuple[int, int]:
    """Append derived rows; a collision the master raises is counted, never swallowed."""
    appended = collisions = 0
    for event in events:
        try:
            if master.append(event):
                appended += 1
        except ValueError as exc:
            collisions += 1
            print(json.dumps({"skipped": event.symbol, "reason": str(exc)}, sort_keys=True))
    return appended, collisions


def _measure(run_id: str, scope: str, measured_at: datetime, pairs: dict[str, float]) -> None:
    ledger.emit(
        "measurements",
        [
            {
                "name": name,
                "scope": scope,
                "measured_at": measured_at,
                "value": float(value),
                "unit": "count",
                "source": "measured",
                "run_id": run_id,
            }
            for name, value in pairs.items()
        ],
        run_id=run_id,
    )


def sync(
    *,
    indexes: list[str],
    data_lake_root: Path,
    now: datetime,
    tickers: list[str] | None = None,
    fetch_fn=None,
    sleep_fn=None,
    dry_run: bool = False,
) -> int:
    """Fetch Massive identities for every unresolved membership ticker.

    Idempotent: a ticker whose verified intervals already cover every needed
    membership date is skipped without a fetch. Evidence is committed once per
    run, before any identity row is appended — the verifier the master runs
    checks raw bytes only, so this ordering is what keeps a manifest-less
    identity row out of the store (spec §6).
    """
    root = Path(data_lake_root)
    evidence = SourceEvidenceStore(root)
    reader = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=reader, evidence_verifier=None)
    sleep = sleep_fn or time.sleep
    fetch = fetch_fn or (
        lambda ticker, *, probe_date=None: fetch_ticker_identity(
            ticker, os.environ.get("MASSIVE_API_KEY"), probe_date=probe_date
        )
    )
    pace_s = 60.0 / constants.declared("massive_requests_per_minute/reference")
    backoff_s = constants.declared("massive_backoff_s/reference")
    scope = "subset" if tickers else "all"

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("security-master-sync")
    run_row = {
        "run_id": run_id,
        "job": "security-master-sync",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": now,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    ledger.emit("runs", [run_row], run_id=run_id)

    def close(exit_code: int) -> int:
        ledger.emit(
            "runs",
            [run_row | {"ended": datetime.now(UTC), "exit_code": exit_code, "verdict": "OK" if exit_code == 0 else "FAILED"}],
            run_id=run_id,
        )
        return exit_code

    try:
        wanted = needed_dates(store, indexes)
        if tickers:
            selected = {ticker.upper() for ticker in tickers}
            wanted = {ticker: dates for ticker, dates in wanted.items() if ticker in selected}

        counts = dict.fromkeys(MEASURE_NAMES, 0)
        pending: list[tuple[str, list]] = []
        manifest: list[SourceEvidence] = []
        refs: list[tuple[str, str]] = []
        first = True
        for ticker in sorted(wanted):
            dates = wanted[ticker]
            if _covered(reader, ticker, dates, now):
                continue
            if not first:
                sleep(pace_s)
            first = False
            counts["identity_tickers_requested"] += 1
            probe_date = min(dates).date().isoformat()
            try:
                result = fetch(ticker, probe_date=probe_date)
            except UniverseFetchError as exc:
                if getattr(exc, "status_code", None) != 429:
                    counts["identity_fetch_failed"] += 1
                    continue
                sleep(backoff_s)
                try:
                    result = fetch(ticker, probe_date=probe_date)
                except UniverseFetchError:
                    counts["identity_fetch_failed"] += 1
                    continue
            for body in result.responses:
                artifact = evidence.persist_raw(body)
                manifest.append(
                    SourceEvidence(
                        ref=artifact.ref,
                        sha256=artifact.sha256,
                        source_url=f"{_SOURCE_URL}?ticker={ticker}",
                        retrieved_at=now,
                        publication_time=None,
                        mediawiki_revision_id=None,
                        mediawiki_revision_time=None,
                        content_type="application/json",
                    )
                )
                refs.append((artifact.ref, artifact.sha256))
            pending.append((ticker, result.records))

        if not dry_run and manifest:
            # One commit for the whole run: `record` is not buffered and a
            # per-response commit cost 41 min/night
            # (pm:2026-08-31-source-evidence-per-response-cost).
            evidence.record_many(manifest)

        if not dry_run:
            writer = SecurityMaster(root, evidence_verifier=_evidence_verifier(evidence))
            for ticker, records in pending:
                derived = derive_identity_events(records, writer.events(), now, tuple(refs))
                for name, value in derived.counts.items():
                    counts[name] += value
                appended, collisions = _append_all(writer, derived.events)
                counts["identity_events_appended"] += appended
                counts["identity_collisions"] += collisions
        else:
            for _ticker, records in pending:
                derived = derive_identity_events(records, reader.events(), now, tuple(refs))
                for name, value in derived.counts.items():
                    counts[name] += value

        _measure(run_id, scope, now, counts)
        print(json.dumps({"scope": scope, "run_id": run_id, "dry_run": dry_run} | counts, sort_keys=True))
    except Exception:
        close(1)
        raise
    return close(1 if counts["identity_fetch_failed"] else 0)


def main(argv: list[str] | None = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py security-master",
        description="Backfill security identities from Massive reference data",
    )
    parser.add_argument("subcommand", choices=["sync"])
    parser.add_argument(
        "--index",
        action="extend",
        nargs="+",
        choices=sorted(DEFAULT_INDEXES),
        help="Index stores to drain (space-separated and/or repeatable; default: all four)",
    )
    parser.add_argument("--tickers", action="extend", nargs="+", help="Explicit ticker subset, for repairs")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and derive without appending")
    args = parser.parse_args(argv)
    return sync(
        indexes=args.index or list(DEFAULT_INDEXES),
        tickers=args.tickers,
        data_lake_root=data_lake_dir(),
        now=datetime.now(UTC),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

Note the `record_many` failure path: it raises out of the `try`, `close(1)` writes the FAILED terminal row, and the re-raise reaches the entrypoint — so nothing is appended. Wrap the `record_many` call in its own `except Exception: return close(1)` if a non-zero exit without a traceback is preferred; the test asserts the exit code, not the traceback.

In `scripts/livewire_ingest.py`, add the `COMMANDS` entry after `"membership-sync"`:

```python
    "security-master": "livewire_scripts.security_master_sync",
```

and add it to the scheduled-env allowlist (both the `if` set and its comment):

```python
    if args.command in {"universe-sync", "shepherd-universe", "membership-sync", "security-master"}:
```

- [ ] 4. Run and see it pass:

```bash
uv run pytest tests/test_security_master_sync.py tests/test_livewire_entrypoints.py -q
```

Expected: all pass. `test_ingest_other_commands_do_not_load_env` must still pass — check it does not assert an exhaustive allowlist; if it does, add `security-master` to its expected set.

- [ ] 5. Milestone commit:

```bash
git add livewire_scripts/security_master_sync.py scripts/livewire_ingest.py \
        tests/test_security_master_sync.py tests/test_livewire_entrypoints.py
git commit -m "feat(security-master): livewire_ingest.py security-master sync

Collects every unresolved membership ticker with the dates it needs, skips the
ones already covered, paces the fetch by massive_requests_per_minute/reference,
commits every response body once per run before any append, and exits 1 when a
ticker was lost to a transport error, a 5xx or a repeated 429."
```

---

## Task 5 — `membership-sync reresolve`

**Files**

- Modify: `livewire_scripts/membership_sync.py` (`_RESOLVE_MICS`, `unresolved_backlog`, `_replay_is_balanced`, `reresolve`, `_reresolve_main`, `main`; `import_events` and `sync` switch to the shared backlog helper)
- Test: `tests/test_membership_sync.py` (append)

**Interfaces**

- Consumes: `IndexMembershipStore.events/append`, `SecurityMaster.resolve_symbol`, `_CONFIDENCE_STATUS`, `clients.ledger`.
- Produces:
  - `unresolved_backlog(events: list[MembershipEvent]) -> set[str]`
  - `_replay_is_balanced(events: list[MembershipEvent], proposed: MembershipEvent) -> bool`
  - `reresolve(*, index_id: str, data_lake_root: Path, now: datetime, confidence: Confidence) -> dict`
  - argv `membership-sync reresolve --index <id> --confidence {B,C,D}`
  - ledger `runs(job="membership-reresolve")`, measurements `membership_reresolve_conflict` and `membership_unresolved` scoped to the index.

### Steps

- [ ] 1. Write the failing tests. Append to `tests/test_membership_sync.py`:

```python
def _reresolve(tmp_path: Path, *, index_id: str = "sp500", confidence: str = "B", now=None) -> dict:
    return membership_sync.reresolve(
        index_id=index_id,
        data_lake_root=tmp_path / "lake",
        now=now or RERESOLVE_NOW,
        confidence=confidence,
    )


RERESOLVE_NOW = datetime(2026, 9, 15, 3, 0, tzinfo=UTC)


def test_reresolve_replaces_the_placeholder_and_rejects_it(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=verifies)
    aeos = _verified_security(master, "AEOS")

    result = _reresolve(tmp_path)

    assert result["resolved"] == 2  # AEOS add + remove
    events = _store(lake).events("sp500")
    placeholders = [e for e in events if e.security_id == "unresolved:AEOS"]
    assert {e.status for e in placeholders} == {"unresolved", "rejected"}
    resolved = [e for e in events if e.security_id == aeos]
    assert {e.action for e in resolved} == {"add", "remove"}
    assert all(e.status == "verified" for e in resolved)
    assert all(e.known_at == RERESOLVE_NOW for e in resolved)


def test_a_second_reresolve_appends_nothing(tmp_path):
    _import(tmp_path)
    _verified_security(SecurityMaster(tmp_path / "lake", evidence_verifier=verifies), "AEOS")
    _reresolve(tmp_path)
    before = len(_store(tmp_path / "lake").events("sp500"))

    assert _reresolve(tmp_path)["resolved"] == 0
    assert len(_store(tmp_path / "lake").events("sp500")) == before


def test_pit_honesty_an_as_of_before_the_reresolve_still_sees_nothing(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    _reresolve(tmp_path)
    store = _store(lake)

    at = dt("2005-01-01")
    assert aeos not in store.members_effective_at("sp500", at, NOW)
    assert aeos in store.members_effective_at("sp500", at, RERESOLVE_NOW)


def test_an_interrupted_pass_is_completed_by_the_retry_with_no_duplicate(tmp_path, monkeypatch):
    """A crash between the resolved append and the rejection leaves the
    placeholder current; the retry reuses the stored replacement as is."""
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")

    original = IndexMembershipStore.append
    calls = {"n": 0}

    def crash_after_first_resolved(self, item):
        if item.status != "rejected":
            calls["n"] += 1
            result = original(self, item)
            if calls["n"] == 1:
                raise RuntimeError("interrupted")
            return result
        return original(self, item)

    monkeypatch.setattr(IndexMembershipStore, "append", crash_after_first_resolved)
    with pytest.raises(RuntimeError):
        _reresolve(tmp_path)
    monkeypatch.undo()

    _reresolve(tmp_path)
    events = _store(lake).events("sp500")
    resolved_adds = [e for e in events if e.security_id == aeos and e.action == "add"]
    assert len(resolved_adds) == 1
    members = membership_sync._current_members(events)
    assert "unresolved:AEOS" not in members


def test_an_add_and_a_remove_on_the_delisting_date_both_resolve_to_one_id(tmp_path):
    """Master intervals are end-exclusive, so a remove effective on the
    delisting date would never resolve on its own and its add would become a
    permanent member the next sync removes a second time."""
    lake = tmp_path / "lake"
    _import(tmp_path)
    master = SecurityMaster(lake, evidence_verifier=verifies)
    security_id = master.new_security_id()
    master.append(
        SecurityIdentityEvent(
            event_id="identity-AEOS-closed",
            security_id=security_id,
            revision=1,
            symbol="AEOS",
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=dt("2000-01-01"),
            effective_to=dt("2007-01-01"),
            known_at=dt("2000-01-01"),
            issuer_name="AEOS issuer",
            cik="0000000009",
            composite_figi=None,
            share_class_figi=None,
            continuity_basis="provider_figi",
            relationship_type=None,
            related_security_id=None,
            source_refs=("artifact://sha256/" + "a" * 64,),
            source_hashes=("a" * 64,),
            status="verified",
            supersedes=None,
        )
    )

    _reresolve(tmp_path)

    events = _store(lake).events("sp500")
    resolved = [e for e in events if e.security_id == security_id]
    assert {e.action for e in resolved} == {"add", "remove"}
    assert security_id not in membership_sync._current_members(events)


def test_a_later_sync_add_for_the_same_id_makes_the_historical_add_fail_closed(tmp_path):
    """The case a check at effective_at alone misses: the nightly sync already
    opened a membership, later in time, for the id this add resolves to."""
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    store = _store(lake)
    store.append(
        MembershipEvent(
            event_id="sync-add-aeos",
            index_id="sp500",
            security_id=aeos,
            action="add",
            announced_at=None,
            effective_at=dt("2026-09-14"),
            known_at=dt("2026-09-14"),
            source_refs=("artifact://sha256/" + SOURCE_SHA,),
            source_hashes=(SOURCE_SHA,),
            revision=1,
            supersedes=None,
            status="verified",
        )
    )

    result = _reresolve(tmp_path)

    assert result["conflicts"] >= 1
    # the placeholder is untouched: no resolved add, no rejection
    events = store.events("sp500")
    assert not [e for e in events if e.security_id == aeos and e.effective_at == dt("2004-01-01")]
    assert [e for e in events if e.security_id == "unresolved:AEOS" and e.status == "rejected"] == []


def test_a_candidate_add_then_a_remove_at_confidence_b_fails_closed(tmp_path):
    """PIT Silver replays verified events only, so the guard filters to the
    status the proposed event will carry: a remove whose add is candidate must
    not be appended as verified."""
    _import(tmp_path, confidence="C")  # placeholders' resolved status will be candidate
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    _reresolve(tmp_path, confidence="C")  # add + remove land as candidate

    # now a second, verified-confidence pass over a fresh placeholder remove
    store = _store(lake)
    store.append(
        MembershipEvent(
            event_id="placeholder-remove-only",
            index_id="sp500",
            security_id="unresolved:AEOS",
            action="remove",
            announced_at=None,
            effective_at=dt("2009-01-01"),
            known_at=dt("2026-09-14"),
            source_refs=("artifact://sha256/" + SOURCE_SHA,),
            source_hashes=(SOURCE_SHA,),
            revision=max(e.revision for e in store.events("sp500") if e.security_id == "unresolved:AEOS") + 1,
            supersedes=None,
            status="unresolved",
        )
    )

    result = _reresolve(tmp_path, confidence="B", now=RERESOLVE_NOW.replace(hour=4))

    assert result["conflicts"] >= 1
    assert not [e for e in store.events("sp500") if e.security_id == aeos and e.status == "verified"]


def test_pit_silver_replay_over_a_reresolved_index_raises_nothing(tmp_path):
    from clients.pit_silver_revision import PitSilverRevision  # exact name per the module

    _import(tmp_path)
    lake = tmp_path / "lake"
    _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    _reresolve(tmp_path)

    # the replay the consumers run; an unbalanced pair would raise
    # "duplicate open membership interval" / "membership removal has no open interval"
    events = _store(lake).events("sp500")
    PitSilverRevision(lake).resolve(
        index_id="sp500", membership_revision=len(events), as_of=RERESOLVE_NOW, security_revision=None
    )


def test_confidence_maps_to_status_and_r2k_proxy_is_always_candidate(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")

    _reresolve(tmp_path, confidence="C")

    resolved = [e for e in _store(lake).events("sp500") if e.security_id == aeos]
    assert {e.status for e in resolved} == {"candidate"}


def test_the_backlog_measure_agrees_across_import_sync_and_reresolve(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    after = _reresolve(tmp_path)["unresolved"]

    events = _store(lake).events("sp500")
    assert after == len(membership_sync.unresolved_backlog(events))
    # a rejected placeholder chain is out of the backlog, so a re-import and the
    # nightly sync report the same number rather than restoring the WARN
    assert len(membership_sync.unresolved_backlog(events)) == after


def test_xase_is_one_of_the_resolve_mics():
    """The Russell list carries 44 NYSE American names (measured 2026-09-15)."""
    assert membership_sync._RESOLVE_MICS == ("XNAS", "XNYS", "ARCX", "XASE")


def test_a_ticker_that_still_does_not_resolve_is_left_alone_and_counted(tmp_path):
    _import(tmp_path)
    result = _reresolve(tmp_path)
    assert result["resolved"] == 0
    assert result["unresolved"] == 1  # unresolved:AEOS
```

- [ ] 2. Run and see it fail:

```bash
uv run pytest tests/test_membership_sync.py -k reresolve -x -q
```

Expected failure: `AttributeError: module 'livewire_scripts.membership_sync' has no attribute 'reresolve'`.

- [ ] 3. Implement. In `livewire_scripts/membership_sync.py`:

  a. Widen the resolve MICs:

```python
# The Russell list carries 44 NYSE American names (measured from the
# TradingView scanner 2026-09-15) and the master holds them under XASE.
_RESOLVE_MICS = ("XNAS", "XNYS", "ARCX", "XASE")
```

b. Add the one shared backlog helper:

```python
def unresolved_backlog(events: list[MembershipEvent]) -> set[str]:
    """Distinct `security_id`s among current rows still carrying no identity.

    One helper for `import`, `sync` and `reresolve`. Counting superseded rows
    too — which import and sync used to do — would let a reresolve drain the
    number to zero while the rejected placeholder chain sat in the store, and
    the next nightly sync would restore the WARN.
    """
    superseded = {item.supersedes for item in events if item.supersedes is not None}
    return {
        item.security_id for item in events if item.event_id not in superseded and item.status == "unresolved"
    }
```

c. In `import_events`, delete the `unresolved_ids = {...}` seed and the `unresolved_ids.add(security_id)` line in the loop, and compute the measure from the store after the loop:

```python
        unresolved_ids = unresolved_backlog(store.events(index_id))
```

placed immediately before the `ledger.emit("measurements", ...)` call. In `sync`, do the same: drop the seeded `unresolved_ids` and the two `unresolved_ids.add(...)` calls, and read `unresolved_ids = unresolved_backlog(store.events(index_id))` immediately before each `_measure(...)` call (both the fetch-failure branch and the normal branch).

d. Add the guard and the pass:

```python
def _replay_is_balanced(events: list[MembershipEvent], proposed: MembershipEvent) -> bool:
    """Would inserting `proposed` leave a balanced add/remove replay?

    The same rule `pit_silver_revision` enforces ("duplicate open membership
    interval", "membership removal has no open interval"), filtered to the
    status the proposed event will carry: PIT Silver replays verified events
    only, so a guard over mixed statuses could pass an add-then-remove whose
    add is `candidate` and leave the verified replay with a remove and no open
    add. A nightly `sync` add that landed *later* than the historical add being
    inserted is caught here and nowhere else.
    """
    superseded = {item.supersedes for item in events if item.supersedes is not None}
    sequence = [
        item
        for item in events
        if item.event_id not in superseded
        and item.status == proposed.status
        and item.security_id == proposed.security_id
    ]
    sequence.append(proposed)
    sequence.sort(key=lambda item: (item.effective_at, item.known_at, item.revision, item.event_id))
    is_open = False
    for item in sequence:
        if item.action == "add":
            if is_open:
                return False
            is_open = True
        else:
            if not is_open:
                return False
            is_open = False
    return True


def _resolved_event_id(placeholder_id: str, security_id: str) -> str:
    return hashlib.sha256(f"reresolve\x00{placeholder_id}\x00{security_id}".encode()).hexdigest()


def _rejection_event_id(placeholder_id: str) -> str:
    return hashlib.sha256(f"reresolve-reject\x00{placeholder_id}".encode()).hexdigest()


def reresolve(
    *,
    index_id: str,
    data_lake_root: Path,
    now: datetime,
    confidence: Confidence,
) -> dict:
    """Rewrite each resolvable `unresolved:` placeholder onto its security_id.

    Two appends per placeholder because `IndexMembershipStore` only lets an
    event supersede one with the same `(index_id, security_id)`: the resolved
    event first, then the `rejected` revision of the placeholder. That order is
    restart-safe — a crash between them leaves the placeholder current and the
    retry recovers the stored replacement by its deterministic `event_id`; the
    reverse order would lose the membership on retry.

    `known_at = now` keeps PIT honest: an `as_of` before this pass still sees
    the placeholder (spec 2026-09-13-dividend-fx-and-pit-membership §known_at,
    contract rule 8).
    """
    root = Path(data_lake_root)
    evidence = SourceEvidenceStore(root)
    verifier = _evidence_verifier(evidence)
    master = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=master, evidence_verifier=verifier)

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("membership-reresolve")
    run_row = {
        "run_id": run_id,
        "job": "membership-reresolve",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": now,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    ledger.emit("runs", [run_row], run_id=run_id)
    try:
        resolved_status = "candidate" if index_id == "r2k-proxy" else _CONFIDENCE_STATUS[confidence]
        events = store.events(index_id)
        superseded = {item.supersedes for item in events if item.supersedes is not None}
        placeholders = sorted(
            (
                item
                for item in events
                if item.event_id not in superseded
                and item.status == "unresolved"
                and item.security_id.startswith("unresolved:")
            ),
            key=lambda item: (item.effective_at, item.known_at, item.revision, item.event_id),
        )
        by_id = {item.event_id: item for item in events}
        revisions: dict[str, int] = {}
        for item in events:
            revisions[item.security_id] = max(revisions.get(item.security_id, 0), item.revision)

        last_add: dict[str, str] = {}
        resolved_count = conflicts = 0
        for placeholder in placeholders:
            ticker = placeholder.security_id.removeprefix("unresolved:")
            if placeholder.action == "add":
                security_id = _resolve(master, ticker, placeholder.effective_at, now)
            else:
                # Master intervals are end-exclusive, so a remove effective on
                # the delisting date resolves through its own add, not the clock.
                security_id = last_add.get(ticker) or _resolve(master, ticker, placeholder.effective_at, now)
            if security_id is None:
                continue

            replacement_id = _resolved_event_id(placeholder.event_id, security_id)
            recovered = by_id.get(replacement_id)
            if recovered is None:
                # Fail closed on an unbalanced replay; no conflict check runs on
                # a recovery, because the replacement is already in the store.
                current = store.events(index_id)
                proposed = MembershipEvent(
                    event_id=replacement_id,
                    index_id=index_id,
                    security_id=security_id,
                    action=placeholder.action,
                    announced_at=placeholder.announced_at,
                    effective_at=placeholder.effective_at,
                    known_at=now,
                    source_refs=placeholder.source_refs,
                    source_hashes=placeholder.source_hashes,
                    revision=revisions.get(security_id, 0) + 1,
                    supersedes=None,
                    status=resolved_status,
                )
                if not _replay_is_balanced(current, proposed):
                    conflicts += 1
                    continue
                store.append(proposed)
                revisions[security_id] = proposed.revision
                by_id[proposed.event_id] = proposed
                resolved_count += 1

            rejection_id = _rejection_event_id(placeholder.event_id)
            if rejection_id not in by_id:
                revisions[placeholder.security_id] = revisions.get(placeholder.security_id, 0) + 1
                rejection = MembershipEvent(
                    event_id=rejection_id,
                    index_id=index_id,
                    security_id=placeholder.security_id,
                    action=placeholder.action,
                    announced_at=placeholder.announced_at,
                    effective_at=placeholder.effective_at,
                    known_at=now,
                    source_refs=placeholder.source_refs,
                    source_hashes=placeholder.source_hashes,
                    revision=revisions[placeholder.security_id],
                    supersedes=placeholder.event_id,
                    status="rejected",
                )
                store.append(rejection)
                by_id[rejection.event_id] = rejection
            if placeholder.action == "add":
                last_add[ticker] = security_id
            else:
                last_add.pop(ticker, None)

        backlog = len(unresolved_backlog(store.events(index_id)))
        _measure(
            run_id,
            index_id,
            now,
            {"membership_reresolve_conflict": conflicts, "membership_unresolved": backlog},
        )
    except Exception:
        ledger.emit(
            "runs",
            [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}],
            run_id=run_id,
        )
        raise
    ledger.emit(
        "runs",
        [run_row | {"ended": datetime.now(UTC), "exit_code": 0, "verdict": "OK"}],
        run_id=run_id,
    )
    return {
        "index": index_id,
        "resolved": resolved_count,
        "conflicts": conflicts,
        "unresolved": backlog,
        "run_id": run_id,
    }
```

e. Add the argparse front and wire it into `main`:

```python
def _reresolve_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py membership-sync reresolve",
        description="Rewrite resolvable unresolved: placeholders onto their security_id",
    )
    parser.add_argument("--index", required=True, choices=sorted(DEFAULT_INDEXES))
    parser.add_argument(
        "--confidence",
        required=True,
        choices=["B", "C", "D"],
        help="A placeholder does not record the confidence its history was imported under",
    )
    args = parser.parse_args(argv)
    print(
        json.dumps(
            reresolve(
                index_id=args.index,
                data_lake_root=data_lake_dir(),
                now=datetime.now(UTC),
                confidence=args.confidence,
            ),
            sort_keys=True,
        )
    )
    return 0
```

and in `main`, next to the existing `import` branch:

```python
    if argv[:1] == ["reresolve"]:
        return _reresolve_main(argv[1:])
```

- [ ] 4. Run and see it pass:

```bash
uv run pytest tests/test_membership_sync.py tests/test_livewire_entrypoints.py -q
```

Expected: all pass, including the pre-existing import/sync tests — their `membership_unresolved` assertions now read the shared helper. If a pre-existing assertion expected a superseded-inclusive count, update the assertion, not the helper; the distinct-current-id cardinality is the contract (spec §3.3).

- [ ] 5. Run the whole suite with the gate:

```bash
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
```

- [ ] 6. Milestone commit:

```bash
git add livewire_scripts/membership_sync.py tests/test_membership_sync.py
git commit -m "feat(membership): reresolve placeholders onto their security_id

Resolved event first, then the rejected revision of the placeholder, so an
interrupted pass is completed by the retry rather than losing the membership.
The replay guard is status-matched to the event it would append, which is what
keeps PIT Silver's verified-only replay balanced. XASE joins the resolve MICs,
and one backlog helper now serves import, sync and reresolve."
```

---

## Task 6 — Status hint, the reresolve/status interaction, and the equal-`known_at` currency case

**Files**

- Modify: `livewire_scripts/status.py:419-422` (the `"Unresolved memberships"` remediation hint)
- Test: `tests/test_status.py` (append), `tests/test_sync_corporate_actions.py` (append)

**Interfaces**

- Consumes: the ledger rows both commands now write; `sync_corporate_actions._equity_currency_resolver(root, now) -> Callable[[str], tuple[str, str]]` (unchanged code).
- Produces: a remediation hint naming commands that exist.

### Steps

- [ ] 1. Write the failing tests. Append to `tests/test_status.py`:

```python
def _reresolve_run(verdict: str, *, started: datetime):
    run_id = f"membership-reresolve-{started:%Y%m%dT%H%M%S}Z"
    ledger.emit(
        "runs",
        [
            {
                "run_id": run_id,
                "job": "membership-reresolve",
                "host": "macmini",
                "release_sha": "deadbeef",
                "presets_sha": None,
                "registry_sha": None,
                "started": started,
                "ended": started,
                "exit_code": 0 if verdict == "OK" else 1,
                "verdict": verdict,
            }
        ],
        run_id=run_id,
    )


def test_a_reresolve_run_cannot_hide_a_failed_membership_sync():
    """The reresolve pass files under its own job. Under `membership-sync` a
    later OK row would hide the night's FAILED scheduled run, because the check
    grades the latest run of the day."""
    monday = date(2026, 9, 14)
    _membership_run("FAILED", started=datetime(2026, 9, 14, 1, 0, tzinfo=UTC))
    _reresolve_run("OK", started=datetime(2026, 9, 14, 4, 0, tzinfo=UTC))

    assert _membership_section("Membership sync ran today", monday).verdict is status.Verdict.BAD


def test_the_unresolved_memberships_hint_names_commands_that_exist():
    hint = status.REMEDIATION["Unresolved memberships"]
    assert "security-master sync" in hint
    assert "reresolve" in hint
    assert "livewire_ops.py membership --index" not in hint
```

Use the module-level name `status.py` actually gives the remediation dict (read `livewire_scripts/status.py` around line 405 and match it; the test must name the real attribute).

Append to `tests/test_sync_corporate_actions.py`:

```python
def test_the_currency_resolver_picks_a_row_when_two_share_one_known_at(tmp_path):
    """One identity fetch can append several rows for one symbol with the same
    known_at. The resolver keeps the first row it encounters on an equal
    known_at, with no provider, MIC or interval filter — assert the choice, not
    just that the field survived."""
    import json
    from pathlib import Path as _Path

    from clients.security_master import SecurityIdentityEvent, SecurityMaster

    fixture = _Path(__file__).parent / "fixtures" / "massive_reference" / "nonusd-<TICKER>-active-2026-09-15.json"
    row = (json.loads(fixture.read_bytes()).get("results") or [])[0]
    symbol, currency, mic = row["ticker"], row["currency_name"].upper(), row["primary_exchange"]
    assert currency != "USD"  # the fixture is a real non-USD listing

    root = tmp_path / "lake"
    master = SecurityMaster(root, evidence_verifier=lambda ref, digest: ref.endswith(digest))
    known_at = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    for index, (interval_start, interval_end) in enumerate(
        [(datetime(2000, 1, 3, tzinfo=UTC), datetime(2010, 1, 4, tzinfo=UTC)), (datetime(2010, 1, 4, tzinfo=UTC), None)]
    ):
        master.append(
            SecurityIdentityEvent(
                event_id=f"currency-{index}",
                security_id=master.new_security_id(),
                revision=1,
                symbol=symbol,
                provider="massive",
                exchange_mic=mic,
                currency=currency,
                effective_from=interval_start,
                effective_to=interval_end,
                known_at=known_at,
                issuer_name=row["name"],
                cik=None,
                composite_figi=None,
                share_class_figi=None,
                continuity_basis="provider_figi",
                relationship_type=None,
                related_security_id=None,
                source_refs=("artifact://sha256/" + "d" * 64,),
                source_hashes=("d" * 64,),
                status="verified",
                supersedes=None,
            )
        )

    resolve = sync_corporate_actions._equity_currency_resolver(root, known_at)
    assert resolve(symbol) == (currency, "security_master")
```

Replace `<TICKER>` with the ticker chosen in Task 0. If the two derived rows would carry different currencies for that symbol, assert the first-encountered one explicitly and say so in the test's docstring — the resolver has no interval filter and making it interval-aware is out of scope (spec §9).

- [ ] 2. Run and see it fail:

```bash
uv run pytest tests/test_status.py -k "reresolve or hint" tests/test_sync_corporate_actions.py -k known_at -x -q
```

Expected failure: `AssertionError: assert 'security-master sync' in "python scripts/livewire_ops.py membership --index <id> ..."`.

- [ ] 3. Implement. Replace the `"Unresolved memberships"` remediation entry in `livewire_scripts/status.py`:

```python
    "Unresolved memberships": (
        "python scripts/livewire_ingest.py security-master sync --index <id>   "
        "# fetch identities, then: "
        "python scripts/livewire_ingest.py membership-sync reresolve --index <id> --confidence B"
    ),
```

(`livewire_ops.py membership` is a read-side command that does not exist and stays out of scope, spec §5/§9.)

- [ ] 4. Run and see it pass:

```bash
uv run pytest tests/test_status.py tests/test_sync_corporate_actions.py -q
```

- [ ] 5. Milestone commit:

```bash
git add livewire_scripts/status.py tests/test_status.py tests/test_sync_corporate_actions.py
git commit -m "fix(status): the unresolved-memberships hint names commands that exist

It pointed at livewire_ops.py membership, which was never written. It now names
the two commands that actually drain the backlog. Adds the status test that a
membership-reresolve run cannot hide a FAILED membership-sync, and the
equal-known_at currency-resolver case one identity fetch can produce."
```

---

## Task 7 — Docs: CHANGELOG, CLAUDE.md, runbook, spec status

**Files**

- Modify: `CHANGELOG.md` (`## [Unreleased]`, `### Added`)
- Modify: `CLAUDE.md` (one line under "The one contract")
- Modify: `docs/runbook.md` (a `security-master` section; extend the `membership-sync` section with `reresolve`)
- Modify: `docs/superpowers/specs/2026-09-15-security-master-backfill-design.md` (status line)

**Interfaces** — documentation only; no code, no new test.

### Steps

- [ ] 1. CHANGELOG. Add an `### Added` block at the top of `## [Unreleased]`:

```markdown
### Added

- `livewire_ingest.py security-master sync` backfills security identities from
  Massive `/v3/reference/tickers`, and `livewire_ingest.py membership-sync
reresolve --index <id> --confidence {B,C,D}` rewrites each resolvable
  `unresolved:<ticker>` placeholder onto its opaque `security_id`. The mini
  held 3,455 unresolved membership ids against one verified identity row, so
  every PIT membership query answered the empty set and apex's
  `/v1/membership/*` served 503. Identity rules are the spec §4 table: a rename
  collapses to one `security_id` with two symbol rows; conflicting or
  incomplete FIGI evidence yields two ids that deliberately do not resolve; a
  record with no provable start appends nothing. Evidence is committed once per
  run before any append, a lost ticker exits 1, the reresolve pass fails closed
  on a replay its insertion would unbalance, and `known_at = now` keeps an
  `as_of` before the pass seeing the placeholder. Two scoped constants —
  `massive_requests_per_minute/reference` and `massive_backoff_s/reference` —
  pace it. The "Unresolved memberships" remediation hint now names these two
  commands instead of a `livewire_ops.py membership` that was never written.
```

- [ ] 2. CLAUDE.md. Add one line at the end of "The one contract" bullet list:

```markdown
- An index membership resolves through the security master or not at all: `security-master sync` fetches Massive identities and `membership-sync reresolve` rewrites each placeholder onto its `security_id` — resolved event first, rejection second (restart-safe), and the replay guard is filtered to the status it would append, because PIT Silver replays verified events only. → test: `tests/test_membership_sync.py::test_an_interrupted_pass_is_completed_by_the_retry_with_no_duplicate`, `::test_a_candidate_add_then_a_remove_at_confidence_b_fails_closed`
```

- [ ] 3. Runbook. Add a `### security-master` section before the `membership-sync` one, and extend `membership-sync`:

````markdown
### `security-master` — identity backfill from Massive reference data

Fetches `GET /v3/reference/tickers` for every ticker still carrying an
`unresolved:<ticker>` membership event and appends the resulting intervals to
`security_master/events.parquet`.

```bash
python scripts/livewire_ingest.py security-master sync [--index sp500 ndx100 djia r2k-proxy] [--dry-run]
python scripts/livewire_ingest.py security-master sync --tickers AAPL YHOO      # repair subset
```

- **Idempotent**: a ticker whose verified intervals already cover every date its
  memberships need is skipped without a fetch. A membership date past the end of
  an existing interval does trigger a fetch — that is how the widening rule runs.
- **Pacing**: `massive_requests_per_minute/reference` (5/min declared) between
  tickers; one 429 backs off `massive_backoff_s/reference` (60 s) and retries
  once, then counts a fetch failure.
- **Evidence**: every response body goes to the CAS, one `record_many` per run
  before any append. If that commit raises, nothing is appended and the run
  exits 1.
- **Ledger**: `runs` rows `job='security-master-sync'`; measurements scoped
  `all` (or `subset` under `--tickers`): `identity_tickers_requested`,
  `identity_events_appended`, `identity_candidate`, `identity_no_start`,
  `identity_conflict`, `identity_unknown_to_provider`, `identity_collisions`,
  `identity_fetch_failed` (any > 0 → exit 1).
- **Not scheduled.** This is a manual backfill; a weekly refresh is decided
  after the first full run's numbers.

#### The mini run order

Both commands run before the next weekday 01:00Z `com.livewire.membership-sync`
— the reresolve guard fails closed if a nightly sync add lands first.

```bash
ssh macmini
source ~/market-warehouse/.venv/bin/activate
cd ~/market-warehouse/current
set -a; source ~/market-warehouse/.env; set +a

# 0. check the ledger for an in-flight run before starting
python scripts/livewire_ops.py ledger query \
  "select job, started, verdict from runs where ended is null"

# 1. sample first: measure the endpoint's real rate before pacing the full run
time python scripts/livewire_ingest.py security-master sync --tickers AAPL MSFT NVDA AMD INTC

# 2. full run, paced by what step 1 measured (requests / elapsed minutes)
LW_DECLARED_MASSIVE_REQUESTS_PER_MINUTE_REFERENCE=<measured> \
  python scripts/livewire_ingest.py security-master sync

# 3. reresolve each index; r2k-proxy is candidate whatever is passed
for IDX in sp500 ndx100 djia; do
  python scripts/livewire_ingest.py membership-sync reresolve --index $IDX --confidence B
done
python scripts/livewire_ingest.py membership-sync reresolve --index r2k-proxy --confidence C

# 4. the acceptance signal
python scripts/livewire_ops.py status | grep -A2 "Unresolved memberships"
curl -s http://<apex-host>/v1/membership/djia | head
```

Report per index, split at 2003-01-01: `membership_unresolved` before and
after, `identity_unknown_to_provider`, `identity_candidate`,
`identity_conflict`, `identity_no_start`, `membership_reresolve_conflict`,
elapsed time and the observed requests per minute. Pre-2003 events are
expected to stay unresolved in this phase — Massive's earliest delisting is
2003-09-11.
````

In the existing `membership-sync` section add:

````markdown
```bash
python scripts/livewire_ingest.py membership-sync reresolve --index sp500 --confidence B
```
````

- **`reresolve`** rewrites each current `unresolved:<ticker>` event onto the
  `security_id` the master now resolves, then appends the `rejected` revision
  of the placeholder. `--confidence` is required: a placeholder does not record
  the confidence its history was imported under, so the operator asserts it per
  run; `r2k-proxy` is always `candidate`. `known_at = now`, so an `as_of` before
  the pass still sees the placeholder. Idempotent; an interrupted pass is
  completed by the retry.
- **Ledger:** `runs` rows `job='membership-reresolve'` — deliberately not
  `membership-sync`, so a later OK pass cannot hide the night's FAILED
  scheduled run. Measurements `membership_reresolve_conflict` and
  `membership_unresolved`, per index.

````

- [ ] 4. Update the spec's status line:

```markdown
Status: implemented 2026-09-15 (this plan:
`docs/superpowers/plans/2026-09-15-security-master-backfill.md`); not yet run on
the mini. Phase 2 (Russell 2000 history from SEC EDGAR N-PORT / N-Q / N-CSR /
N-30D) gets its own spec once this one has run.
````

- [ ] 5. Run the full gate one last time:

```bash
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning
npm run test:alerts
```

Expected: pytest passes with coverage ≥ 95%; the Node alert suite is untouched and passes.

- [ ] 6. Milestone commit:

```bash
git add CHANGELOG.md CLAUDE.md docs/runbook.md docs/superpowers/specs/2026-09-15-security-master-backfill-design.md
git commit -m "docs: security-master sync and membership reresolve

CHANGELOG entry, one CLAUDE.md rule line pointing at the two tests that
enforce it, runbook sections for both commands with the mini run order and the
sample-then-pace procedure, and the spec status line."
```

---

## Self-review — spec section to task

| Spec           | Requirement                                                                                                                                                                       | Task                                                                                              |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| §1             | Backlog is 3,455 unresolved ids against one verified identity row                                                                                                                 | motivation, stated in Task 7's CHANGELOG entry                                                    |
| §2             | The seven probes' raw bodies frozen before implementation starts                                                                                                                  | Task 0                                                                                            |
| §2             | `massive_requests_per_minute/reference` (5, per_min), `massive_backoff_s/reference` (60, s), scoped, with env overrides                                                           | Task 1                                                                                            |
| §3.1           | `fetch_ticker_identity`, three calls in order, `IdentityRecord`/`IdentityRecords`, raw bytes untouched, 404/empty is data, transport error raises, MIC as-is, currency uppercased | Task 2                                                                                            |
| §3.2 step 1    | Collect distinct unresolved tickers with the dates each needs                                                                                                                     | Task 4 (`needed_dates`)                                                                           |
| §3.2 step 2    | Fetch only when a needed date is uncovered; `probe_date` = earliest needed date; paced                                                                                            | Task 4 (`_covered`, pacing, `test_a_membership_date_past_the_existing_interval_triggers_a_fetch`) |
| §3.2 step 3    | `persist_raw` per response, one `record_many` before any append; a raise appends nothing and exits 1                                                                              | Task 4                                                                                            |
| §3.2 step 4    | Master built with `_evidence_verifier(evidence_store)`; a collision is counted, never swallowed                                                                                   | Task 4 (`_append_all`)                                                                            |
| §3.2 step 5    | Ledger rows                                                                                                                                                                       | Task 4                                                                                            |
| §3.2           | `COMMANDS` entry + scheduled-env allowlist + `--index/--tickers/--dry-run`                                                                                                        | Task 4                                                                                            |
| §3.3 steps 1–5 | Ordering, remove-through-its-add, deterministic recovery, status-matched balanced-replay guard, resolved-then-rejected order, confidence mapping                                  | Task 5                                                                                            |
| §3.3           | `unresolved_backlog` shared by import/sync/reresolve                                                                                                                              | Task 5                                                                                            |
| §3.3           | `_RESOLVE_MICS` gains `XASE`                                                                                                                                                      | Task 5                                                                                            |
| §4             | Every row of the identity table, interval bounds, opaque `security_id`                                                                                                            | Task 3                                                                                            |
| §4             | `_equity_currency_resolver` equal-`known_at` case, resolver unchanged                                                                                                             | Task 6                                                                                            |
| §5             | `runs` jobs `security-master-sync` / `membership-reresolve`; the ten measurements                                                                                                 | Tasks 4, 5                                                                                        |
| §5             | No new status check; remediation hint corrected                                                                                                                                   | Task 6                                                                                            |
| §6             | Fetch failure counted + exit 1; single 429 backoff; evidence-commit failure appends nothing; run order on the mini                                                                | Tasks 4, 7                                                                                        |
| §7             | Every listed test                                                                                                                                                                 | Tasks 2–6 (mapped one-to-one in each task's step 1)                                               |
| §8             | Acceptance measured per index, split at 2003-01-01, apex as the external check                                                                                                    | Task 7 (runbook "The mini run order")                                                             |
| §9             | Out of scope: EDGAR, CUSIP/ISIN, R2K history, weekly refresh, `livewire_ops.py membership`, interval-aware currency resolver                                                      | nothing in this plan implements any of them                                                       |

### Known gaps to raise at execution time

- The spec's rename example (YHOO → AABA) assumes the two real bodies share a
  `composite_figi` **and** a `share_class_figi`. That is unverified until Task 0
  captures them. If they differ, Task 3's rename test becomes a conflict test
  and the rename path stays exercised by a constructed pair — noted inline in
  Task 3 step 4.
- The non-USD listing for §7's currency test has no named ticker in the spec;
  Task 0 step 2 picks a real one and records it, and stops rather than
  fabricating if none is found.
- The endpoint's real rate limit is unknown; the declared 5/min is a starting
  value, re-measured by the sample run in Task 7's runbook procedure before the
  full backfill.
