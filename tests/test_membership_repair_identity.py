"""Tests for `membership-sync repair-identity` — R1/R2/R4.

docs/superpowers/specs/2026-09-23-membership-identity-continuity-design.md

Agilent (CIK 0001090872) and GOOGL (CIK 0001652044) are real companies with
real CIKs, used for the R1 merge scenarios per the production facts measured
2026-09-23. Agilent's researched window (2000-06-04..06) and its 2026-09-17
massive twin, and GOOGL's two researched windows and its 2026-06-29 massive
twin, and the sp500 add dates (A 2000-06-05, GOOGL 2006-04-03) and the churn
timestamp (2026-09-17T01:00:05Z) are the exact facts measured on the mini.

The R2 collision-cap/finite-extension tests and the R1 conflict tests need a
*second*, unrelated identity to collide or mismatch against; those use the
same placeholder-CIK convention tests/test_membership_sync.py already
established (`cik="0000000001"`-style fillers for AEOS, a real historical
ticker used there purely as algorithm scaffolding, not a claim about AEOS's
real CIK).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from clients import ledger
from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from livewire_scripts import membership_sync

AGILENT_CIK = "0001090872"
GOOGL_CIK = "0001652044"

T0 = datetime(2026, 9, 16, tzinfo=UTC)
CHURN_T = datetime(2026, 9, 17, 1, 0, 5, tzinfo=UTC)
MASSIVE_KNOWN = datetime(2026, 9, 17, 5, 17, 0, tzinfo=UTC)
RERESOLVE_NOW = datetime(2026, 9, 17, 5, 30, 0, tzinfo=UTC)
REPAIR_NOW = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


@pytest.fixture(autouse=True)
def _ledger_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))


def _evidence(store: SourceEvidenceStore, label: str, url: str, now: datetime) -> tuple[str, str]:
    artifact = store.persist_raw(f"evidence: {label}".encode())
    store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url=url,
            retrieved_at=now,
            publication_time=None,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="text/html",
        )
    )
    return artifact.ref, artifact.sha256


def _identity(refs: tuple[str, str], **kwargs) -> SecurityIdentityEvent:
    ref, sha = refs
    defaults = dict(
        currency="USD",
        composite_figi=None,
        share_class_figi=None,
        continuity_basis="regulator_filing",
        relationship_type=None,
        related_security_id=None,
        source_refs=(ref,),
        source_hashes=(sha,),
        status="verified",
        supersedes=None,
    )
    return SecurityIdentityEvent(**(defaults | kwargs))


def _membership(refs: tuple[str, str], **kwargs) -> MembershipEvent:
    ref, sha = refs
    defaults = dict(
        announced_at=None,
        source_refs=(ref,),
        source_hashes=(sha,),
        revision=1,
        supersedes=None,
        status="verified",
    )
    return MembershipEvent(**(defaults | kwargs))


def _master(lake: Path, *, writable: bool = True) -> SecurityMaster:
    verifier = membership_sync._evidence_verifier(SourceEvidenceStore(lake)) if writable else None
    return SecurityMaster(lake, evidence_verifier=verifier)


def _store(lake: Path, *, writable: bool = True) -> IndexMembershipStore:
    verifier = membership_sync._evidence_verifier(SourceEvidenceStore(lake)) if writable else None
    return IndexMembershipStore(lake, security_master=_master(lake, writable=False), evidence_verifier=verifier)


def _identities(lake: Path) -> list[SecurityIdentityEvent]:
    return _master(lake, writable=False).events()


def _membership_events(lake: Path, index_id: str) -> list[MembershipEvent]:
    return _store(lake, writable=False).events(index_id)


def _agilent_and_googl(lake: Path) -> dict[str, str]:
    """Seed the real Agilent + GOOGL researched identities and their real
    massive twins, plus each company's real sp500 add — no churn yet."""
    evidence = SourceEvidenceStore(lake)
    master = _master(lake)

    researched_a = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "agilent-wiki", "https://en.wikipedia.org/wiki/Agilent_Technologies", T0),
            event_id="agilent-researched",
            security_id=researched_a,
            revision=1,
            symbol="A",
            provider="wikipedia_sec_research",
            exchange_mic="XNYS",
            cik=AGILENT_CIK,
            effective_from=dt("2000-06-04"),
            effective_to=dt("2000-06-06"),
            known_at=T0,
            issuer_name="Agilent Technologies Inc.",
        )
    )
    massive_a = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "agilent-massive", "massive://reference/A", MASSIVE_KNOWN),
            event_id="agilent-massive",
            security_id=massive_a,
            revision=1,
            symbol="A",
            provider="massive",
            exchange_mic="XNYS",
            cik=AGILENT_CIK,
            composite_figi="BBG000C2V3D6",
            share_class_figi="BBG001SCTQY4",
            effective_from=dt("2026-09-17"),
            effective_to=None,
            known_at=MASSIVE_KNOWN,
            issuer_name="Agilent Technologies Inc.",
        )
    )

    researched_g = SecurityMaster.new_security_id()
    for n, (start, end) in enumerate((("2005-12-18", "2005-12-20"), ("2006-04-02", "2006-04-04")), start=1):
        master.append(
            _identity(
                _evidence(evidence, f"googl-wiki-{n}", "https://en.wikipedia.org/wiki/Alphabet_Inc.", T0),
                event_id=f"googl-researched-{n}",
                security_id=researched_g,
                revision=n,
                symbol="GOOGL",
                provider="wikipedia_sec_research",
                exchange_mic="XNAS",
                cik=GOOGL_CIK,
                effective_from=dt(start),
                effective_to=dt(end),
                known_at=T0,
                issuer_name="Alphabet Inc.",
            )
        )
    massive_g = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "googl-massive", "massive://reference/GOOGL", dt("2026-06-29")),
            event_id="googl-massive",
            security_id=massive_g,
            revision=1,
            symbol="GOOGL",
            provider="massive",
            exchange_mic="XNAS",
            cik=GOOGL_CIK,
            composite_figi="BBG009S39JX6",
            share_class_figi="BBG009S39JY5",
            effective_from=dt("2026-06-29"),
            effective_to=None,
            known_at=dt("2026-06-29"),
            issuer_name="Alphabet Inc.",
        )
    )

    store = _store(lake)
    store.append(
        _membership(
            _evidence(evidence, "sp500-add-A", "https://sp500-history.test/A", T0),
            event_id="sp500-add-A",
            index_id="sp500",
            security_id=researched_a,
            action="add",
            effective_at=dt("2000-06-05"),
            known_at=T0,
        )
    )
    store.append(
        _membership(
            _evidence(evidence, "sp500-add-GOOGL", "https://sp500-history.test/GOOGL", T0),
            event_id="sp500-add-GOOGL",
            index_id="sp500",
            security_id=researched_g,
            action="add",
            effective_at=dt("2006-04-03"),
            known_at=T0,
        )
    )
    return {"researched_a": researched_a, "massive_a": massive_a, "researched_g": researched_g, "massive_g": massive_g}


def _add_agilent_churn(lake: Path, ids: dict[str, str]) -> None:
    """The 2026-09-17T01:00:05Z bug: remove the researched id, add a
    placeholder — Massive has no identity for "A" yet at this instant."""
    evidence = SourceEvidenceStore(lake)
    store = _store(lake)
    store.append(
        _membership(
            _evidence(evidence, "sp500-churn-remove-A", "https://sp500-live.test/2026-09-17", CHURN_T),
            event_id="sp500-churn-remove-A",
            index_id="sp500",
            security_id=ids["researched_a"],
            action="remove",
            effective_at=CHURN_T,
            known_at=CHURN_T,
            revision=2,
        )
    )
    store.append(
        _membership(
            _evidence(evidence, "sp500-churn-add-A", "https://sp500-live.test/2026-09-17", CHURN_T),
            event_id="sp500-churn-add-A",
            index_id="sp500",
            security_id="unresolved:A",
            action="add",
            effective_at=CHURN_T,
            known_at=CHURN_T,
            status="unresolved",
        )
    )


def test_sync_r3_no_churn_when_ticker_still_live_under_narrow_identity(tmp_path):
    """The regression: A's researched claim does not cover `now`, but the
    ticker is still on the live list and a massive identity for it now exists
    (from `now`) — R3 must not remove the old id / add a placeholder."""
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)

    code = membership_sync.sync(
        indexes=["sp500"],
        data_lake_root=lake,
        now=RERESOLVE_NOW,
        fetch_fn=lambda index_id: (
            {"A", "GOOGL"},
            membership_sync.HashedRef(
                *_evidence(SourceEvidenceStore(lake), "live", "https://sp500-live.test", RERESOLVE_NOW)
            ),
        ),
    )

    assert code == 0
    events = _membership_events(lake, "sp500")
    assert len(events) == 2  # the two original adds only -- nothing appended
    assert {e.event_id for e in events} == {"sp500-add-A", "sp500-add-GOOGL"}
    assert membership_sync._current_members(events) == {ids["researched_a"], ids["researched_g"]}


def test_repair_identity_merges_by_cik_extends_and_restores_continuity(tmp_path):
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)
    _add_agilent_churn(lake, ids)
    membership_sync.reresolve(index_id="sp500", data_lake_root=lake, now=RERESOLVE_NOW, confidence="B")

    before_identities = len(_identities(lake))
    before_membership = len(_membership_events(lake, "sp500"))

    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)

    merges_by_ticker = {m["ticker"]: m for m in manifest["merges"]}
    assert set(merges_by_ticker) == {"A", "GOOGL"}
    assert merges_by_ticker["A"]["massive_security_id"] == ids["massive_a"]
    assert merges_by_ticker["A"]["researched_security_id"] == ids["researched_a"]
    assert manifest["conflicts"] == []
    assert manifest["dangling_verified_references"] == []

    # R2: Agilent's narrow claim extends open (no non-churn remove exists).
    agilent_ext = next(e for e in manifest["extensions"] if e["security_id"] == ids["researched_a"])
    assert agilent_ext["new_effective_to"] is None

    # R4: both churn events (remove + the resolved replacement add) are rejected.
    rejected_ids = {r["rejected_event_id"] for r in manifest["rejections"]}
    assert rejected_ids == {
        "sp500-churn-remove-A",
        membership_sync._resolved_event_id("sp500-churn-add-A", ids["massive_a"]),
    }

    # continuity: A never left sp500 across the churn, at all three acceptance dates.
    assert (
        manifest["member_counts_before"]["sp500"]["2026-09-01"] < manifest["member_counts_after"]["sp500"]["2026-09-01"]
    )
    store = _store(lake, writable=False)
    for as_of_date in ("2015-01-02", "2025-01-02", "2026-09-01"):
        assert ids["researched_a"] in store.members_effective_at("sp500", dt(as_of_date), REPAIR_NOW)

    # security_master: the old massive claim is superseded by a rejection, and
    # a duplicate under the researched id carries the same figis/window.
    identities = _identities(lake)
    rejection = next(e for e in identities if e.supersedes == "agilent-massive")
    assert rejection.status == "rejected"
    duplicate = next(e for e in identities if e.security_id == ids["researched_a"] and e.provider == "massive")
    assert duplicate.composite_figi == "BBG000C2V3D6" and duplicate.effective_to is None

    # R2 cites the index's own record: the extension carries the claim's refs
    # plus the Agilent add's evidence.
    extension = next(e for e in identities if e.supersedes == "agilent-researched")
    agilent_add = next(
        e for e in _membership_events(lake, "sp500") if e.security_id == ids["researched_a"] and e.action == "add"
    )
    assert set(agilent_add.source_refs) <= set(extension.source_refs)
    assert len(extension.source_refs) == len(extension.source_hashes)

    assert len(_identities(lake)) > before_identities
    assert len(_membership_events(lake, "sp500")) > before_membership


def _agilent_researched_only(lake: Path) -> str:
    """Just the real Agilent researched claim -- no massive twin -- so a
    second, non-matching massive candidate for "A" can be added without
    colliding with Agilent's own (real, matching) massive identity."""
    evidence = SourceEvidenceStore(lake)
    master = _master(lake)
    researched_a = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "agilent-wiki", "https://en.wikipedia.org/wiki/Agilent_Technologies", T0),
            event_id="agilent-researched",
            security_id=researched_a,
            revision=1,
            symbol="A",
            provider="wikipedia_sec_research",
            exchange_mic="XNYS",
            cik=AGILENT_CIK,
            effective_from=dt("2000-06-04"),
            effective_to=dt("2000-06-06"),
            known_at=T0,
            issuer_name="Agilent Technologies Inc.",
        )
    )
    return researched_a


def test_repair_identity_reports_cik_mismatch_without_merging(tmp_path):
    lake = tmp_path / "lake"
    researched_a = _agilent_researched_only(lake)
    evidence = SourceEvidenceStore(lake)
    master = _master(lake)
    # Same ticker/mic as Agilent, but a CIK that does not belong to Agilent,
    # and a non-overlapping window so the fixture setup itself does not
    # collide -- exercises the mismatch-reporting path only; no claim about
    # which company this placeholder CIK really identifies.
    mismatched = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "mismatch", "massive://reference/A-mismatch", MASSIVE_KNOWN),
            event_id="mismatch-massive",
            security_id=mismatched,
            revision=1,
            symbol="A",
            provider="massive",
            exchange_mic="XNYS",
            cik="0000000009",
            effective_from=dt("1990-01-01"),
            effective_to=dt("1990-01-02"),
            known_at=MASSIVE_KNOWN,
            issuer_name="A mismatch issuer",
        )
    )
    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)

    conflicts = {c["massive_security_id"]: c for c in manifest["conflicts"]}
    assert conflicts[mismatched]["reason"] == "cik_mismatch"
    assert conflicts[mismatched]["researched_security_id"] == researched_a
    assert not any(m["massive_security_id"] == mismatched for m in manifest["merges"])
    # a conflict never mutates or rejects the mismatched candidate's own claim
    # (R2's independent extension of the researched claim is not this rule's concern)
    mismatched_claim = next(e for e in _identities(lake) if e.security_id == mismatched)
    assert mismatched_claim.status == "verified" and mismatched_claim.supersedes is None


def test_repair_identity_reports_missing_cik_without_merging(tmp_path):
    lake = tmp_path / "lake"
    _agilent_researched_only(lake)
    evidence = SourceEvidenceStore(lake)
    master = _master(lake)
    no_cik = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "no-cik", "massive://reference/A-no-cik", MASSIVE_KNOWN),
            event_id="no-cik-massive",
            security_id=no_cik,
            revision=1,
            symbol="A",
            provider="massive",
            exchange_mic="XNYS",
            cik=None,
            effective_from=dt("1990-01-01"),
            effective_to=dt("1990-01-02"),
            known_at=MASSIVE_KNOWN,
            issuer_name="A no-cik issuer",
        )
    )

    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)

    conflicts = {c["massive_security_id"]: c for c in manifest["conflicts"]}
    assert conflicts[no_cik]["reason"] == "missing_cik"


def test_repair_identity_r2_extends_to_finite_remove_and_caps_on_collision(tmp_path):
    """Two independent researched claims for ticker AEOS (a real historical
    ticker, already used this way throughout tests/test_membership_sync.py)
    under placeholder CIKs -- pure algorithm scaffolding, no real-company
    claim: one extends to a later genuine remove, the other would extend past
    a second claim's start and gets capped there instead."""
    lake = tmp_path / "lake"
    evidence = SourceEvidenceStore(lake)
    master = _master(lake)
    store = _store(lake)

    finite_id = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "aeos-1", "https://en.wikipedia.org/wiki/American_Eagle_Outfitters", T0),
            event_id="aeos-1-researched",
            security_id=finite_id,
            revision=1,
            symbol="AEOS",
            provider="wikipedia_sec_research",
            exchange_mic="XNAS",
            cik="0000000001",
            effective_from=dt("2004-01-01"),
            effective_to=dt("2004-01-03"),
            known_at=T0,
            issuer_name="AEOS test issuer 1",
        )
    )
    store.append(
        _membership(
            _evidence(evidence, "aeos-1-add", "https://sp500-history.test/AEOS-1", T0),
            event_id="aeos-1-add",
            index_id="sp500",
            security_id=finite_id,
            action="add",
            effective_at=dt("2004-01-02"),
            known_at=T0,
        )
    )
    store.append(
        _membership(
            _evidence(evidence, "aeos-1-remove", "https://sp500-history.test/AEOS-1-remove", T0),
            event_id="aeos-1-remove",
            index_id="sp500",
            security_id=finite_id,
            action="remove",
            effective_at=dt("2007-06-01"),
            known_at=T0,
            revision=2,
        )
    )

    capped_id = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "aeos-2", "https://en.wikipedia.org/wiki/American_Eagle_Outfitters", T0),
            event_id="aeos-2-researched",
            security_id=capped_id,
            revision=1,
            symbol="AEOS",
            provider="wikipedia_sec_research",
            exchange_mic="XNAS",
            cik="0000000002",
            effective_from=dt("2001-01-01"),
            effective_to=dt("2001-01-03"),
            known_at=T0,
            issuer_name="AEOS test issuer 2",
        )
    )
    store.append(
        _membership(
            _evidence(evidence, "aeos-2-add", "https://sp500-history.test/AEOS-2", T0),
            event_id="aeos-2-add",
            index_id="sp500",
            security_id=capped_id,
            action="add",
            effective_at=dt("2001-01-02"),
            known_at=T0,
        )
    )
    # no remove for capped_id -- without the collision it would extend open,
    # past aeos-1's window start (2004-01-01), which is the collision.

    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)

    extensions = {e["security_id"]: e for e in manifest["extensions"]}
    assert extensions[finite_id]["new_effective_to"] == dt("2007-06-01").isoformat()
    assert extensions[capped_id]["new_effective_to"] == dt("2004-01-01").isoformat()
    caps = {c["security_id"]: c for c in manifest["caps"]}
    assert caps[capped_id]["capped_at"] == dt("2004-01-01").isoformat()


def test_repair_identity_r4_keeps_genuine_same_day_add_without_paired_remove(tmp_path):
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)
    _add_agilent_churn(lake, ids)
    membership_sync.reresolve(index_id="sp500", data_lake_root=lake, now=RERESOLVE_NOW, confidence="B")

    evidence = SourceEvidenceStore(lake)
    store = _store(lake)
    orcl_id = SecurityMaster.new_security_id()
    master = _master(lake)
    master.append(
        _identity(
            _evidence(evidence, "orcl", "massive://reference/ORCL", CHURN_T),
            event_id="orcl-massive",
            security_id=orcl_id,
            revision=1,
            symbol="ORCL",
            provider="massive",
            exchange_mic="XNYS",
            cik="0001341439",
            effective_from=dt("1986-01-01"),
            effective_to=None,
            known_at=CHURN_T,
            issuer_name="Oracle Corporation",
        )
    )
    store.append(
        _membership(
            _evidence(evidence, "orcl-add", "https://sp500-live.test/2026-09-17", CHURN_T),
            event_id="sp500-add-ORCL",
            index_id="sp500",
            security_id=orcl_id,
            action="add",
            effective_at=CHURN_T,
            known_at=CHURN_T,
        )
    )

    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)

    assert not any(r["rejected_event_id"] == "sp500-add-ORCL" for r in manifest["rejections"])
    events = {e.event_id: e for e in _membership_events(lake, "sp500")}
    assert events["sp500-add-ORCL"].status == "verified"  # untouched


NVIDIA_CIK = "0001045810"


def test_history_under_a_merged_duplicate_is_repointed_and_one_index_exit_does_not_end_the_identity(tmp_path):
    """NVDA as the mini holds it: ndx100 history (add 2004-01-01, remove
    2004-12-20, add 2005-12-19) sits under the Massive identity, sp500 under
    the researched one (add 2001-11-30), and the 2026-09-17 churn swapped
    sp500 to the Massive id."""
    lake = tmp_path / "lake"
    evidence = SourceEvidenceStore(lake)
    master = _master(lake)
    researched = SecurityMaster.new_security_id()
    massive = SecurityMaster.new_security_id()
    master.append(
        _identity(
            _evidence(evidence, "nvda-wiki", "https://en.wikipedia.org/wiki/Nvidia", T0),
            event_id="nvda-researched",
            security_id=researched,
            revision=1,
            symbol="NVDA",
            provider="wikipedia_sec_research",
            exchange_mic="XNAS",
            cik=NVIDIA_CIK,
            effective_from=dt("2001-11-29"),
            effective_to=dt("2001-12-01"),
            known_at=T0,
            issuer_name="Nvidia Corp",
        )
    )
    master.append(
        _identity(
            _evidence(evidence, "nvda-massive", "https://api.massive.com/v3/reference/tickers/NVDA", T0),
            event_id="nvda-massive",
            security_id=massive,
            revision=1,
            symbol="NVDA",
            provider="massive",
            exchange_mic="XNAS",
            cik=NVIDIA_CIK,
            composite_figi="BBG000BBJQV0",
            share_class_figi="BBG001S5TZJ6",
            continuity_basis="provider_figi",
            effective_from=dt("2004-01-01"),
            effective_to=None,
            known_at=T0,
            issuer_name="Nvidia Corp",
        )
    )
    store = _store(lake)
    sp_ref = _evidence(evidence, "sp500-nvda", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", T0)
    ndx_ref = _evidence(evidence, "ndx100-nvda", "https://en.wikipedia.org/wiki/Nasdaq-100", T0)
    store.append(
        _membership(
            sp_ref,
            event_id="sp500-add-NVDA",
            index_id="sp500",
            security_id=researched,
            action="add",
            effective_at=dt("2001-11-30"),
            known_at=T0,
        )
    )
    for revision, (action, day) in enumerate(
        (("add", "2004-01-01"), ("remove", "2004-12-20"), ("add", "2005-12-19")), start=1
    ):
        store.append(
            _membership(
                ndx_ref,
                event_id=f"ndx100-{action}-NVDA-{day}",
                index_id="ndx100",
                security_id=massive,
                action=action,
                effective_at=dt(day),
                known_at=T0,
                revision=revision,
            )
        )
    churn_ref = _evidence(evidence, "sp500-live", "https://www.slickcharts.com/sp500", CHURN_T)
    store.append(
        _membership(
            churn_ref,
            event_id="sp500-churn-remove-NVDA",
            index_id="sp500",
            security_id=researched,
            action="remove",
            effective_at=CHURN_T,
            known_at=CHURN_T,
            revision=2,
        )
    )
    store.append(
        _membership(
            churn_ref,
            event_id="sp500-churn-add-NVDA",
            index_id="sp500",
            security_id=massive,
            action="add",
            effective_at=CHURN_T,
            known_at=CHURN_T,
            revision=1,
        )
    )

    manifest = membership_sync.repair_identity(
        indexes=["sp500", "ndx100"], data_lake_root=lake, now=REPAIR_NOW, apply=True
    )

    assert [(m["massive_security_id"], m["researched_security_id"]) for m in manifest["merges"]] == [
        (massive, researched)
    ]
    assert manifest["dangling_verified_references"] == []
    assert sorted((r["index_id"], r["action"], r["effective_at"][:10]) for r in manifest["repoints"]) == [
        ("ndx100", "add", "2004-01-01"),
        ("ndx100", "add", "2005-12-19"),
        ("ndx100", "remove", "2004-12-20"),
    ]
    # Leaving ndx100 on 2004-12-20 does not end the identity: NVDA stayed in sp500.
    extension = next(e for e in manifest["extensions"] if e["security_id"] == researched)
    assert extension["new_effective_to"] is None

    reader = _store(lake, writable=False)
    member = lambda index_id, day: researched in reader.members_effective_at(index_id, dt(day), REPAIR_NOW)  # noqa: E731
    assert member("ndx100", "2004-06-01") and not member("ndx100", "2005-06-01") and member("ndx100", "2006-01-03")
    assert member("sp500", "2004-12-21") and member("sp500", "2025-01-02") and member("sp500", "2026-09-22")
    runs = ledger.query("select verdict from runs where job='membership-repair-identity' and ended is not null")
    assert {row["verdict"] for row in runs} == {"OK"}


def test_repair_identity_second_apply_appends_nothing(tmp_path):
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)
    _add_agilent_churn(lake, ids)
    membership_sync.reresolve(index_id="sp500", data_lake_root=lake, now=RERESOLVE_NOW, confidence="B")

    membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)
    identities_after_first = len(_identities(lake))
    membership_after_first = len(_membership_events(lake, "sp500"))

    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)

    assert manifest["merges"] == []
    assert manifest["extensions"] == []
    assert manifest["rejections"] == []
    assert len(_identities(lake)) == identities_after_first
    assert len(_membership_events(lake, "sp500")) == membership_after_first


def test_repair_identity_dry_run_writes_nothing(tmp_path):
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)
    _add_agilent_churn(lake, ids)
    membership_sync.reresolve(index_id="sp500", data_lake_root=lake, now=RERESOLVE_NOW, confidence="B")
    before_identities = _identities(lake)
    before_membership = _membership_events(lake, "sp500")

    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=False)

    assert manifest["merges"]  # the plan is still computed
    assert (
        manifest["member_counts_before"]["sp500"]["2026-09-01"] < manifest["member_counts_after"]["sp500"]["2026-09-01"]
    )
    assert _identities(lake) == before_identities
    assert _membership_events(lake, "sp500") == before_membership


def test_the_dry_run_scratch_copy_never_includes_raw(tmp_path, monkeypatch):
    # On the mini raw/ holds the Massive flat files; copying it would fill the disk.
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)
    _add_agilent_churn(lake, ids)
    membership_sync.reresolve(index_id="sp500", data_lake_root=lake, now=RERESOLVE_NOW, confidence="B")
    (lake / "raw" / "massive").mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    real_copytree = membership_sync.shutil.copytree

    def recording_copytree(src, dst, *args, **kwargs):
        if Path(src).parent == lake:  # shutil recurses through copytree; record top-level subtrees only
            copied.append(Path(src).name)
        return real_copytree(src, dst, *args, **kwargs)

    monkeypatch.setattr(membership_sync.shutil, "copytree", recording_copytree)

    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=False)

    assert sorted(copied) == ["index_membership", "security_master"]
    assert (
        manifest["member_counts_after"]["sp500"]["2026-09-01"] > manifest["member_counts_before"]["sp500"]["2026-09-01"]
    )


def test_repair_identity_writes_the_manifest_and_ledger_run(tmp_path):
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)
    _add_agilent_churn(lake, ids)
    membership_sync.reresolve(index_id="sp500", data_lake_root=lake, now=RERESOLVE_NOW, confidence="B")
    output = tmp_path / "manifest.json"

    membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True, output=output)

    assert output.exists()
    runs = ledger.query("select verdict from runs where job='membership-repair-identity' and ended is not null")
    assert {row["verdict"] for row in runs} == {"OK"}
    rows = ledger.query("select name, value from measurements where name='identity_merges' and scope='sp500'")
    assert {row["value"] for row in rows} == {2.0}


def test_repair_identity_cli_dispatch(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)
    _add_agilent_churn(lake, ids)
    membership_sync.reresolve(index_id="sp500", data_lake_root=lake, now=RERESOLVE_NOW, confidence="B")
    monkeypatch.setenv("MDW_DATA_LAKE", str(lake))

    assert membership_sync.main(["repair-identity", "--index", "sp500"]) == 0
    assert _identities(lake)  # dry run only, but the command runs end to end

    assert membership_sync.main(["repair-identity", "--index", "sp500", "--apply"]) == 0
    assert any(e.supersedes == "agilent-massive" and e.status == "rejected" for e in _identities(lake))
