"""Tests for livewire_scripts/security_master_sync.py — Massive identities → SecurityMaster."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.universe_client import IdentityRecord
from livewire_scripts import security_master_sync

MASSIVE_REFERENCE = Path(__file__).parent / "fixtures" / "massive_reference"
NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
HASH = "b" * 64
REFS = ((f"artifact://sha256/{HASH}", HASH),)


def _fixture(name: str) -> bytes:
    return (MASSIVE_REFERENCE / name).read_bytes()


def _records(name: str, *, existed_at: str | None = None) -> list[IdentityRecord]:
    """Parse a frozen body into IdentityRecords the way fetch_ticker_identity does.

    `existed_at` is passed by every test that needs a derivable start: no body
    from this endpoint carries `list_date` (fixtures README F1), so the `date=`
    probe date is the only start a record can have. It is this pure function's
    input, not a claim about the provider.
    """
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
    # existed_at is the real aapl-date-2010-01-04 probe date (README F1).
    derived = _derive(_records("aapl-active-2026-09-15.json", existed_at="2010-01-04"))

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


def test_the_real_yhoo_aaba_pair_is_two_ids_because_massive_gives_yhoo_no_figi(tmp_path):
    """The spec's rename example does not hold in the real data (README F5).

    Massive's YHOO delisted record carries `cik` only — no composite or
    share-class FIGI — while AABA carries both. Nothing links them, so the
    derivation must not invent a rename: two security_ids, YHOO a candidate on
    provider_reference, AABA verified on provider_figi, and no conflict, because
    two records with no common FIGI do not contradict each other.
    """
    records = _records("yhoo-active-false-2026-09-15.json", existed_at="2015-06-01") + _records(
        "aaba-active-false-2026-09-15.json", existed_at="2018-06-01"
    )
    derived = _derive(records)

    assert len({event.security_id for event in derived.events}) == 2
    by_symbol = {event.symbol: event for event in derived.events}
    assert by_symbol["YHOO"].status == "candidate"
    assert by_symbol["YHOO"].continuity_basis == "provider_reference"
    assert by_symbol["AABA"].status == "verified"
    assert by_symbol["AABA"].continuity_basis == "provider_figi"
    assert derived.counts["identity_conflict"] == 0


def test_a_figi_matched_rename_is_one_security_id_with_two_symbol_rows(tmp_path):
    """The rename rule itself, exercised with a labelled test double.

    Both records below are the real AABA bodies — same composite and share-class
    FIGI, same cik — and the earlier one has its ticker overridden, because the
    real YHOO record carries no FIGI to match on (README F5). The override is
    the test's, never a fixture edit.
    """
    aaba_delisted = _records("aaba-active-false-2026-09-15.json", existed_at="2018-06-01")[0]
    # test double: real AABA FIGIs, ticker overridden — Massive carries no FIGI
    # on the real YHOO record (fixtures README F5). `delisted_utc` is YHOO's own
    # real value from `yhoo-active-false`: the spec's rename row requires the two
    # intervals to be disjoint, and an open-ended earlier record is the overlap
    # case below, not a rename.
    earlier = aaba_delisted.__class__(
        **{
            **aaba_delisted.__dict__,
            "ticker": "YHOO",
            "existed_at": "2015-06-01",
            "delisted_utc": _records("yhoo-active-false-2026-09-15.json")[0].delisted_utc,
        }
    )
    derived = _derive([earlier, aaba_delisted])

    assert len({event.security_id for event in derived.events}) == 1
    assert sorted(event.revision for event in derived.events) == [1, 2]
    assert {event.symbol for event in derived.events} == {"YHOO", "AABA"}
    assert all(event.supersedes is None for event in derived.events)
    assert derived.counts["identity_conflict"] == 0

    master = _master(tmp_path)
    for event in derived.events:
        assert master.append(event) is True  # disjoint intervals on one id
    yhoo = master.resolve_symbol("massive", "YHOO", "XNAS", datetime(2016, 1, 4, tzinfo=UTC), NOW)
    aaba = master.resolve_symbol("massive", "AABA", "XNAS", datetime(2019, 1, 4, tzinfo=UTC), NOW)
    assert yhoo is not None and yhoo == aaba


def test_matching_figis_with_overlapping_dates_are_two_unresolved_rows(tmp_path):
    # test double: real AABA FIGIs (the delisted YHOO body carries none, fixtures
    # README F5); the second record reuses them under the earlier ticker with an
    # interval that overlaps the first.
    left = _records("aaba-active-false-2026-09-15.json", existed_at="2015-06-01")[0]
    overlapping = [left, left.__class__(**{**left.__dict__, "ticker": "YHOO", "delisted_utc": None})]
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
    base = _records("aapl-active-2026-09-15.json", existed_at="2010-01-04")[0]
    # AABA's real share-class FIGI under AAPL's composite: a genuinely different
    # share class. (The plan used "BBG001S5N8V8", which is AAPL's own value, so
    # the two records were identical and the test proved the overlap rule.)
    other = base.__class__(**{**base.__dict__, "ticker": "AAPL", "share_class_figi": "BBG001S8V781"})
    derived = _derive([base, other])

    assert len({event.security_id for event in derived.events}) == 2
    assert {event.status for event in derived.events} == {"unresolved"}
    assert derived.counts["identity_conflict"] == 1


def test_a_missing_share_class_figi_on_one_side_is_incomplete_not_contradictory():
    base = _records("aapl-active-2026-09-15.json", existed_at="2010-01-04")[0]
    partial = base.__class__(**{**base.__dict__, "share_class_figi": None, "existed_at": "1990-01-02"})
    derived = _derive([base, partial])

    assert len({event.security_id for event in derived.events}) == 2
    assert {event.status for event in derived.events} == {"candidate"}
    assert derived.counts["identity_conflict"] == 1


def test_a_different_composite_figi_is_two_ids_with_disjoint_intervals(tmp_path):
    delisted = _records("wlp-active-false-2026-09-15.json", existed_at="2010-01-04")[0]
    reused = _records("aapl-active-2026-09-15.json")[0]
    reused = reused.__class__(**{**reused.__dict__, "ticker": "WLP", "existed_at": "2020-01-02"})
    derived = _derive([delisted, reused])

    assert len({event.security_id for event in derived.events}) == 2
    master = _master(tmp_path)
    for event in derived.events:
        assert master.append(event) is True  # the collision check accepts disjoint intervals


def test_an_existing_partial_interval_is_widened_on_the_same_id(tmp_path):
    first = _derive(_records("aapl-active-2026-09-15.json", existed_at="2010-01-04")).events[0]
    narrowed = SecurityIdentityEvent(**{**first.__dict__, "effective_to": datetime(2015, 1, 1, tzinfo=UTC)})
    master = _master(tmp_path)
    master.append(narrowed)

    derived = _derive(_records("aapl-active-2026-09-15.json", existed_at="2010-01-04"), existing=master.events())

    assert len(derived.events) == 1
    widened = derived.events[0]
    assert widened.security_id == narrowed.security_id
    assert widened.revision == 2
    assert widened.supersedes == narrowed.event_id
    assert widened.effective_to is None
    assert master.append(widened) is True


def test_a_covered_interval_derives_nothing(tmp_path):
    event = _derive(_records("aapl-active-2026-09-15.json", existed_at="2010-01-04")).events[0]
    master = _master(tmp_path)
    master.append(event)

    assert (
        _derive(_records("aapl-active-2026-09-15.json", existed_at="2010-01-04"), existing=master.events()).events == []
    )


def test_a_record_without_any_figi_is_candidate_and_does_not_resolve(tmp_path):
    base = _records("aapl-active-2026-09-15.json", existed_at="2010-01-04")[0]
    bare = base.__class__(**{**base.__dict__, "composite_figi": None, "share_class_figi": None})
    derived = _derive([bare])

    assert derived.events[0].status == "candidate"
    assert derived.events[0].continuity_basis == "provider_reference"
    assert derived.counts["identity_candidate"] == 1

    master = _master(tmp_path)
    master.append(derived.events[0])
    assert master.resolve_symbol("massive", "AAPL", "XNAS", NOW, NOW) is None


def test_a_covered_candidate_interval_derives_nothing_on_refetch(tmp_path):
    """The master's collision check covers verified rows only, so a FIGI-less
    listing (the real delisted YHOO body) re-fetched on a later run must find
    its own candidate row, not append a duplicate every night."""
    records = _records("yhoo-active-false-2026-09-15.json", existed_at="2015-06-01")
    first = _derive(records)
    assert [event.status for event in first.events] == ["candidate"]
    master = _master(tmp_path)
    master.append(first.events[0])

    assert _derive(records, existing=master.events()).events == []
    # an earlier proven start widens the same candidate id instead
    earlier = _derive(_records("yhoo-active-false-2026-09-15.json", existed_at="2010-01-04"), existing=master.events())
    assert [(e.security_id, e.revision, e.status, e.supersedes) for e in earlier.events] == [
        (first.events[0].security_id, 2, "candidate", first.events[0].event_id)
    ]


def test_no_start_and_an_empty_probe_appends_nothing():
    # F1 makes this the ordinary case, not the edge case: no record carries a
    # list_date, so a ticker whose date= probe came back empty has no start.
    base = _records("wlp-active-false-2026-09-15.json")[0]
    undated = base.__class__(**{**base.__dict__, "existed_at": None})
    derived = _derive([undated])

    assert derived.events == []
    assert derived.counts["identity_no_start"] == 1


def test_an_empty_probe_does_not_backdate_a_delisted_record():
    """A start of known_at would invent a date and, for a delisted record, an
    interval that ends before it starts — which the master rejects."""
    base = _records("yhoo-active-false-2026-09-15.json")[0]
    undated = base.__class__(**{**base.__dict__, "existed_at": None})
    assert _derive([undated]).events == []


def test_a_probed_start_is_used_because_there_is_never_a_list_date():
    base = _records("wlp-active-false-2026-09-15.json")[0]
    probed = base.__class__(**{**base.__dict__, "existed_at": "2010-01-04"})
    event = _derive([probed]).events[0]
    assert event.effective_from == datetime(2010, 1, 4, tzinfo=UTC)


def test_no_record_at_all_is_counted_unknown_to_provider():
    derived = _derive(_records("aamrq-active-false-2026-09-15.json"))
    assert derived.events == []
    assert derived.counts["identity_unknown_to_provider"] == 1


def test_a_nyse_american_identity_resolves_under_xase(tmp_path):
    event = _derive(_records("imo-active-2026-09-15.json", existed_at="2010-01-04")).events[0]
    master = _master(tmp_path)
    master.append(event)
    assert master.resolve_symbol("massive", "IMO", "XASE", NOW, NOW) == event.security_id


def test_a_record_with_no_mic_is_unknown_to_provider_not_a_row():
    """A `date=` body may omit `primary_exchange` (fixtures README F4).

    `exchange_mic` is non-nullable and the resolver is keyed on it, so a record
    with no venue names no listing: nothing is appended.
    """
    derived = _derive(_records("aapl-date-2010-01-04-2026-09-15.json", existed_at="2010-01-04"))

    assert derived.events == []
    assert derived.counts["identity_unknown_to_provider"] == 1


def test_a_list_date_outranks_the_probe_date():
    """Spec §4: `effective_from` is `list_date` when present.

    No body from this endpoint carries one (fixtures README F1), so the rule is
    exercised with a labelled test double over the real AAPL record.
    """
    base = _records("aapl-active-2026-09-15.json", existed_at="2010-01-04")[0]
    # test double: this endpoint never returns list_date (README F1)
    listed = base.__class__(**{**base.__dict__, "list_date": "1980-12-12"})

    assert _derive([listed]).events[0].effective_from == datetime(1980, 12, 12, tzinfo=UTC)
