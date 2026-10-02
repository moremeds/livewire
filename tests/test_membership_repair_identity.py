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


def test_a_churn_pair_across_merged_ids_is_rejected_in_one_pass(tmp_path):
    """GOOGL on the mini: the 2026-09-17 diff removed the researched id (its
    latest claim reads GOOG) and added the Massive duplicate.
    The first pass must reject the pair, not re-point the add; a second pass
    appends nothing."""
    lake = tmp_path / "lake"
    ids = _agilent_and_googl(lake)
    evidence = SourceEvidenceStore(lake)
    # The real id also carries the 2014 GOOG claim. With no claim covering
    # 2026-09-17 the ticker falls back to the latest one, GOOG, so only the
    # R1 merge (not the ticker) pairs this remove with the GOOGL add.
    _master(lake).append(
        _identity(
            _evidence(evidence, "googl-wiki-2014", "https://en.wikipedia.org/wiki/Alphabet_Inc.", T0),
            event_id="googl-researched-goog-2014",
            security_id=ids["researched_g"],
            revision=3,
            symbol="GOOG",
            provider="wikipedia_sec_research",
            exchange_mic="XNAS",
            cik=GOOGL_CIK,
            effective_from=dt("2014-04-02"),
            effective_to=dt("2014-04-04"),
            known_at=T0,
            issuer_name="Alphabet Inc.",
        )
    )
    store = _store(lake)
    live = _evidence(evidence, "sp500-live-googl", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", CHURN_T)
    store.append(
        _membership(
            live,
            event_id="sp500-churn-remove-GOOGL",
            index_id="sp500",
            security_id=ids["researched_g"],
            action="remove",
            effective_at=CHURN_T,
            known_at=CHURN_T,
            revision=2,
        )
    )
    store.append(
        _membership(
            live,
            event_id="sp500-churn-add-GOOGL",
            index_id="sp500",
            security_id=ids["massive_g"],
            action="add",
            effective_at=CHURN_T,
            known_at=CHURN_T,
            revision=1,
        )
    )

    first = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)
    rejected = {row["rejected_event_id"] for row in first["rejections"]}
    assert {"sp500-churn-remove-GOOGL", "sp500-churn-add-GOOGL"} <= rejected
    assert all(row["event_id"] != "sp500-churn-add-GOOGL" for row in first["repoints"])
    counts = (len(_identities(lake)), len(_membership_events(lake, "sp500")))

    second = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)
    assert (len(_identities(lake)), len(_membership_events(lake, "sp500"))) == counts
    assert second["rejections"] == [] and second["repoints"] == []
    reader = _store(lake, writable=False)
    assert ids["researched_g"] in reader.members_effective_at("sp500", dt("2026-09-22"), REPAIR_NOW)


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


def test_a_backfilled_rename_pair_is_history_not_churn(tmp_path):
    """Torchmark renamed Globe Life (CIK 0000320335) on 2019-08-08; the
    backfill recorded it as remove(unresolved:TMK) + add(researched id) on
    that date, known 2026-09-16. Same shape as churn, but not written by the
    live diff, so R4 leaves it alone and GL stays a member (mini, 2026-09-23)."""
    lake = tmp_path / "lake"
    evidence = SourceEvidenceStore(lake)
    refs = _evidence(evidence, "globe-life-wiki", "https://en.wikipedia.org/wiki/Globe_Life", T0)
    gl = SecurityMaster.new_security_id()
    _master(lake).append(
        _identity(
            refs,
            event_id="gl-researched",
            security_id=gl,
            revision=1,
            symbol="TMK",
            provider="wikipedia_sec_research",
            exchange_mic="XNYS",
            cik="0000320335",
            effective_from=dt("2019-08-07"),
            effective_to=dt("2019-08-09"),
            known_at=T0,
            issuer_name="Globe Life Inc.",
        )
    )
    store = _store(lake)
    for event_id, security_id, action, at, status, revision in (
        ("tmk-add", "unresolved:TMK", "add", "1996-01-02", "unresolved", 1),
        ("tmk-remove", "unresolved:TMK", "remove", "2019-08-08", "unresolved", 2),
        ("gl-add", gl, "add", "2019-08-08", "verified", 1),
    ):
        store.append(
            _membership(
                refs,
                event_id=event_id,
                index_id="sp500",
                security_id=security_id,
                action=action,
                effective_at=dt(at),
                known_at=T0,
                status=status,
                revision=revision,
            )
        )

    manifest = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)
    assert manifest["rejections"] == []
    assert gl in _store(lake, writable=False).members_effective_at("sp500", dt("2026-09-22"), REPAIR_NOW)


def test_sync_r3_a_renamed_member_is_neither_removed_nor_added(tmp_path):
    """FleetCor renamed Corpay (CIK 0001175454) on 2024-03-25; its sp500 add
    (2018-06-20) is covered by the FLT claim, the live list says CPAY, and CPAY
    resolves to the same id. Diffing tickers alone would remove and re-add it
    (mini scratch dry-run, 2026-09-23)."""
    lake = tmp_path / "lake"
    evidence = SourceEvidenceStore(lake)
    refs = _evidence(evidence, "corpay-massive", "massive://reference/CPAY", T0)
    cpay = SecurityMaster.new_security_id()
    master = _master(lake)
    for revision, symbol, start, end in ((1, "FLT", "2018-06-20", "2024-03-25"), (2, "CPAY", "2024-03-25", None)):
        master.append(
            _identity(
                refs,
                event_id=f"corpay-{symbol}",
                security_id=cpay,
                revision=revision,
                symbol=symbol,
                provider="massive",
                exchange_mic="XNYS",
                cik="0001175454",
                effective_from=dt(start),
                effective_to=dt(end) if end else None,
                known_at=T0,
                issuer_name="Corpay, Inc.",
            )
        )
    _store(lake).append(
        _membership(
            refs,
            event_id="sp500-add-FLT",
            index_id="sp500",
            security_id=cpay,
            action="add",
            effective_at=dt("2018-06-20"),
            known_at=T0,
        )
    )

    code = membership_sync.sync(
        indexes=["sp500"],
        data_lake_root=lake,
        now=RERESOLVE_NOW,
        fetch_fn=lambda index_id: (
            {"CPAY"},
            membership_sync.HashedRef(*_evidence(evidence, "live", "https://sp500-live.test", RERESOLVE_NOW)),
        ),
    )

    assert code == 0
    assert [e.event_id for e in _membership_events(lake, "sp500")] == ["sp500-add-FLT"]


def test_a_rename_across_merged_ids_keeps_the_member_and_a_second_pass_appends_nothing(tmp_path):
    """Fiserv (CIK 0000798354) moved FISV -> FI on 2023-06-07 and back on
    2025-11-11; the backfill wrote each as remove + add across the researched
    id and its Massive duplicate (FIGI BBG001S5R6Q4, FISV again from
    2026-09-17, which is what R1 joins on). Re-pointed onto one id,
    the later-known remove would win the same-date tie and drop Fiserv from
    sp500 after 2025-11-11 (mini scratch apply, 2026-09-23)."""
    lake = tmp_path / "lake"
    evidence = SourceEvidenceStore(lake)
    refs = _evidence(evidence, "fiserv-wiki", "https://en.wikipedia.org/wiki/Fiserv", T0)
    researched, massive = SecurityMaster.new_security_id(), SecurityMaster.new_security_id()
    master = _master(lake)
    for event_id, security_id, revision, symbol, provider, mic, figi, start, end in (
        ("fisv-2001", researched, 1, "FISV", "wikipedia_sec_research", "XNAS", None, "2001-04-01", "2001-04-03"),
        ("fisv-2025", researched, 2, "FISV", "wikipedia_sec_research", "XNYS", None, "2025-11-10", "2025-11-12"),
        ("fi-massive", massive, 1, "FI", "massive", "XNYS", "BBG001S5R6Q4", "2023-06-07", "2025-11-11"),
        ("fisv-massive", massive, 2, "FISV", "massive", "XNAS", "BBG001S5R6Q4", "2026-09-17", None),
    ):
        master.append(
            _identity(
                refs,
                event_id=event_id,
                security_id=security_id,
                revision=revision,
                symbol=symbol,
                provider=provider,
                exchange_mic=mic,
                share_class_figi=figi,
                cik="0000798354",
                effective_from=dt(start),
                effective_to=dt(end) if end else None,
                known_at=T0,
                issuer_name="Fiserv, Inc.",
            )
        )
    store = _store(lake)
    for event_id, security_id, action, at, revision in (
        ("fisv-add-2001", researched, "add", "2001-04-02", 1),
        ("fisv-remove-2023", researched, "remove", "2023-06-07", 2),
        ("fi-add-2023", massive, "add", "2023-06-07", 1),
        ("fi-remove-2025", massive, "remove", "2025-11-11", 2),
        ("fisv-add-2025", researched, "add", "2025-11-11", 3),
    ):
        store.append(
            _membership(
                refs,
                event_id=event_id,
                index_id="sp500",
                security_id=security_id,
                action=action,
                effective_at=dt(at),
                known_at=T0,
                revision=revision,
            )
        )

    first = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)
    assert first["repoints"] == []
    counts = (len(_identities(lake)), len(_membership_events(lake, "sp500")))
    second = membership_sync.repair_identity(indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True)
    assert (len(_identities(lake)), len(_membership_events(lake, "sp500"))) == counts
    reader = _store(lake, writable=False)
    for day in ("2024-06-03", "2026-09-22"):
        assert researched in reader.members_effective_at("sp500", dt(day), REPAIR_NOW)


def _covered(lake: Path, security_id: str, day: str) -> bool:
    claims = [e for e in membership_sync._active_identities(_identities(lake)) if e.security_id == security_id]
    return any(
        e.status == "verified" and e.effective_from <= dt(day) and (e.effective_to is None or dt(day) < e.effective_to)
        for e in claims
    )


def test_a_successor_registrant_cik_is_one_security_with_its_predecessor(tmp_path):
    """Exxon as the mini holds it (2026-09-26): sp500's researched id carries
    ExxonMobil Holdings Corp's CIK 0002115436 (successor registrant, 8-K12B
    0001193125-26-291990, effective 2026-07-01); djia's carries Exxon's own
    0000034088 (add 1991-05-06, remove 2020-08-31), and the Massive XOM claim
    from 2020-08-31 carries 34088 too. Joined by raw CIK, sp500's claim was
    capped at 2020-08-30 and XOM had no identity after it."""
    lake = tmp_path / "lake"
    evidence = SourceEvidenceStore(lake)
    refs = _evidence(evidence, "exxon-wiki", "https://en.wikipedia.org/wiki/ExxonMobil", T0)
    successor, predecessor, massive = (SecurityMaster.new_security_id() for _ in range(3))
    master = _master(lake)
    for event_id, security_id, revision, provider, cik, name, start, end in (
        (
            "xom-1991",
            predecessor,
            1,
            "wikipedia_sec_research",
            "0000034088",
            "Exxon Corporation",
            "1991-05-05",
            "1991-05-07",
        ),
        (
            "xom-2020",
            predecessor,
            2,
            "wikipedia_sec_research",
            "0000034088",
            "Exxon Mobil Corporation",
            "2020-08-30",
            "2020-09-01",
        ),
        ("xom-1996", successor, 1, "wikipedia_sec_research", "0002115436", "ExxonMobil", "1996-01-01", "1996-01-03"),
        ("xom-massive", massive, 1, "massive", "0000034088", "Exxon Mobil Corp", "2020-08-31", None),
    ):
        master.append(
            _identity(
                refs,
                event_id=event_id,
                security_id=security_id,
                revision=revision,
                symbol="XOM",
                provider=provider,
                exchange_mic="XNYS",
                cik=cik,
                effective_from=dt(start),
                effective_to=dt(end) if end else None,
                known_at=T0,
                issuer_name=name,
            )
        )
    store = _store(lake)
    for event_id, index_id, security_id, action, at, revision in (
        ("sp500-add-XOM", "sp500", successor, "add", "1996-01-02", 1),
        ("djia-add-XOM", "djia", predecessor, "add", "1991-05-06", 1),
        ("djia-remove-XOM", "djia", predecessor, "remove", "2020-08-31", 2),
    ):
        store.append(
            _membership(
                refs,
                event_id=event_id,
                index_id=index_id,
                security_id=security_id,
                action=action,
                effective_at=dt(at),
                known_at=T0,
                revision=revision,
            )
        )

    manifest = membership_sync.repair_identity(
        indexes=["sp500", "djia"], data_lake_root=lake, now=REPAIR_NOW, apply=True
    )

    assert {(m["massive_security_id"], m["researched_security_id"]) for m in manifest["merges"]} == {
        (predecessor, successor),
        (massive, successor),
    }
    assert manifest["conflicts"] == []
    for day in ("1991-06-03", "2005-06-01", "2020-09-15", "2026-09-22"):
        assert _covered(lake, successor, day), day
    reader = _store(lake, writable=False)
    assert successor in reader.members_effective_at("djia", dt("2000-01-03"), REPAIR_NOW)
    assert successor in reader.members_effective_at("sp500", dt("2026-09-22"), REPAIR_NOW)
    counts = (len(_identities(lake)), len(_membership_events(lake, "djia")))
    membership_sync.repair_identity(indexes=["sp500", "djia"], data_lake_root=lake, now=REPAIR_NOW, apply=True)
    assert (len(_identities(lake)), len(_membership_events(lake, "djia"))) == counts


def _seed(lake: Path, claims, memberships=()) -> dict[str, str]:
    """Seed `(security, provider, symbol, mic, cik, issuer, start, end)` claims
    and `(index, security, action, day)` memberships; returns name -> security_id."""
    evidence = SourceEvidenceStore(lake)
    refs = _evidence(evidence, "seed", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", T0)
    ids: dict[str, str] = {}
    revisions: dict[str, int] = {}
    master = _master(lake)
    for n, (name, provider, symbol, mic, cik, issuer, start, end) in enumerate(claims):
        security_id = ids.setdefault(name, SecurityMaster.new_security_id())
        revisions[name] = revisions.get(name, 0) + 1
        master.append(
            _identity(
                refs,
                event_id=f"{name}-{n}",
                security_id=security_id,
                revision=revisions[name],
                symbol=symbol,
                provider=provider,
                exchange_mic=mic,
                cik=cik,
                effective_from=dt(start),
                effective_to=dt(end) if end else None,
                known_at=MASSIVE_KNOWN if provider == "massive" else T0,
                issuer_name=issuer,
            )
        )
    store = _store(lake)
    member_revisions: dict[tuple[str, str], int] = {}
    for index_id, name, action, day in memberships:
        member_revisions[index_id, name] = member_revisions.get((index_id, name), 0) + 1
        store.append(
            _membership(
                refs,
                event_id=f"{index_id}-{action}-{name}-{day}",
                index_id=index_id,
                security_id=ids[name],
                action=action,
                effective_at=dt(day),
                known_at=MASSIVE_KNOWN,  # after every seeded claim, Massive's included
                revision=member_revisions[index_id, name],
            )
        )
    return ids


def _symbols_open(lake: Path, security_id: str, day: str) -> set[str]:
    return {
        e.symbol
        for e in membership_sync._active_identities(_identities(lake))
        if e.security_id == security_id
        and e.status == "verified"
        and e.effective_from <= dt(day)
        and (e.effective_to is None or dt(day) < e.effective_to)
    }


W = "wikipedia_sec_research"


def test_r5_a_renamed_security_carries_todays_ticker_not_the_rename_day_probe(tmp_path):
    """Baker Hughes (CIK 0001701605), BHGE -> BKR on 2019-10-18: the Massive
    probe taken on the rename day still said BHGE, open-ended, so after R1 it
    was the latest claim and BHGE read as current (mini, 2026-09-26). Bronze
    holds BKR from 2017-07-05 and no BHGE at all."""
    lake = tmp_path / "lake"
    ids = _seed(
        lake,
        [
            ("bkr", W, "BHGE", "XNYS", "0001701605", "Baker Hughes, a GE company", "2019-10-17", "2019-10-19"),
            ("bkr", W, "BKR", "XNYS", "0001701605", "Baker Hughes", "2019-10-17", "2019-10-19"),
            ("bkr", W, "BKR", "XNAS", "0001701605", "Baker Hughes Company", "2022-12-18", "2022-12-20"),
            ("probe", "massive", "BHGE", "XNYS", "0001701605", "Baker Hughes Co", "2019-10-18", None),
        ],
        [("sp500", "bkr", "add", "2019-10-18"), ("ndx100", "bkr", "add", "2022-12-19")],
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500", "ndx100"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"BKR"}
    )

    assert {(r["old_symbol"], r["new_symbol"]) for r in manifest["relabels"]} == {("BHGE", "BKR")}
    for day in ("2019-10-18", "2021-06-01", "2026-09-22"):
        assert _symbols_open(lake, ids["bkr"], day) == {"BKR"}, day
    counts = len(_identities(lake))
    membership_sync.repair_identity(
        indexes=["sp500", "ndx100"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"BKR"}
    )
    assert len(_identities(lake)) == counts


def test_r5_frees_todays_ticker_for_the_security_that_holds_it_now(tmp_path):
    """St Paul (CIK 0000086312, TRV today) and Travelers Group (CIK
    0000831001, C since 1998): both researched as TRV in 1997-99, so St
    Paul's sp500 claim was capped at 1997-03-16 and sp500 PIT could not
    publish (identity gap 1997-03-16 .. 2009-06-08, mini 2026-09-24)."""
    lake = tmp_path / "lake"
    ids = _seed(
        lake,
        [
            ("citi", W, "C", "XNYS", "0000831001", "Citigroup Inc.", "1996-01-01", "1996-01-03"),
            ("citi", W, "TRV", "XNYS", "0000831001", "Travelers Group Inc.", "1997-03-16", "1997-03-18"),
            ("citi", W, "C", "XNYS", "0000831001", "Citigroup Inc.", "1999-10-31", "1999-11-02"),
            ("citi", W, "TRV", "XNYS", "0000831001", "Travelers Group Inc.", "1999-10-31", "1999-11-02"),
            ("citi", W, "C", "XNYS", "0000831001", "Citigroup Inc.", "2009-06-07", "2009-06-09"),
            ("citi", "massive", "C", "XNYS", "0000831001", "Citigroup Inc.", "2009-06-08", None),
            ("stpaul", W, "TRV", "XNYS", "0000086312", "St Paul Cos Inc.", "1996-01-01", "1996-01-03"),
            ("stpaul", "massive", "TRV", "XNYS", "0000086312", "The Travelers Companies, Inc.", "2009-06-08", None),
        ],
        [
            ("sp500", "citi", "add", "1996-01-02"),
            ("sp500", "stpaul", "add", "1996-01-02"),
            ("djia", "citi", "add", "1997-03-17"),
            ("djia", "citi", "remove", "2009-06-08"),
            ("djia", "stpaul", "add", "2009-06-08"),
        ],
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500", "djia"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"C", "TRV"}
    )

    assert {(r["security_id"], r["old_symbol"], r["new_symbol"]) for r in manifest["relabels"]} == {
        (ids["citi"], "TRV", "C")
    }
    for day in ("1997-03-17", "2005-06-01", "2026-09-22"):
        assert _symbols_open(lake, ids["stpaul"], day) == {"TRV"}, day
        assert _symbols_open(lake, ids["citi"], day) == {"C"}, day


def test_r5_never_hands_a_security_a_ticker_another_holds_today(tmp_path):
    """The old GM (CIK 0000040730, Motors Liquidation after 2009) left sp500
    on 2009-06-03; GM today is General Motors Company (CIK 0001467858), a
    different security. The old one has no ticker of its own today."""
    lake = tmp_path / "lake"
    ids = _seed(
        lake,
        [
            ("oldgm", W, "GM", "XNYS", "0000040730", "General Motors Corporation", "1991-05-05", "1991-05-07"),
            ("oldgm", W, "MTLQQ", "XNYS", "0000040730", "General Motors Corporation", "1996-01-01", "1996-01-03"),
            ("oldgm", W, "MTLQQ", "XNYS", "0000040730", "General Motors Corporation", "2009-06-02", "2009-06-04"),
            ("oldgm", W, "GM", "XNYS", "0000040730", "General Motors Corporation", "2009-06-07", "2009-06-09"),
            ("newgm", "massive", "GM", "XNYS", "0001467858", "General Motors Company", "2013-06-07", None),
        ],
        [
            ("sp500", "oldgm", "add", "1996-01-02"),
            ("sp500", "oldgm", "remove", "2009-06-03"),
            ("sp500", "newgm", "add", "2013-06-07"),
        ],
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"GM"}
    )

    assert manifest["relabels"] == []
    assert [(r["security_id"], r["reason"]) for r in manifest["relabels_skipped"]] == [
        (ids["oldgm"], "no_current_ticker")
    ]


def test_r5_relabels_claim_by_claim_and_keeps_the_one_that_collides(tmp_path):
    """AT&T Inc. (SBC Communications, CIK 0000732717) took T in 2005 from
    AT&T Corp. (CIK 0000005907). Its 1999 SBC claim overlaps AT&T Corp.'s T
    and stays SBC; the rest become T, so T, not SBC, reads as current
    (claims as the mini holds them after the 2026-09-24 repair)."""
    lake = tmp_path / "lake"
    ids = _seed(
        lake,
        [
            ("attcorp", W, "T", "XNYS", "0000005907", "AT&T Corp.", "1991-05-05", "1996-01-01"),
            ("attcorp", W, "T", "XNYS", "0000005907", "AT&T Corp.", "2004-04-07", "2004-04-09"),
            ("att", W, "T", "XNYS", "0000732717", "S B C Communications Inc.", "1996-01-01", "2004-04-07"),
            ("att", W, "SBC", "XNYS", "0000732717", "SBC Communications Inc.", "1999-10-31", "2015-03-19"),
            ("att", W, "T", "XNYS", "0000732717", "AT&T Inc.", "2005-11-20", "2015-03-19"),
            ("att", W, "SBC", "XNYS", "0000732717", "SBC Communications Inc.", "2005-11-20", "2015-03-19"),
            ("att", "massive", "SBC", "XNYS", "0000732717", "AT&T Inc.", "2005-11-21", None),
            ("att", W, "T", "XNYS", "0000732717", "AT&T Inc.", "2015-03-18", "2015-03-20"),
        ],
        [("sp500", "att", "add", "1996-01-02")],
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"T"}
    )

    assert [(r["event_id"], r["reason"]) for r in manifest["relabels_skipped"]] == [("att-3", "collision")]
    assert len(manifest["relabels"]) == 2
    assert _symbols_open(lake, ids["att"], "2026-09-22") == {"T"}
    assert _symbols_open(lake, ids["att"], "2004-04-08") == {"SBC"}


def test_r5_prefers_the_listed_ticker_the_security_holds_open_today(tmp_path):
    """Alphabet class A (CIK 0001652044) traded as GOOG until the 2014-04-03
    share-class split; GOOG and GOOGL are both sp500 tickers today, and only
    GOOGL is open on this id (claims as on the mini, 2026-09-23)."""
    lake = tmp_path / "lake"
    ids = _seed(
        lake,
        [
            ("alphabet", W, "GOOGL", "XNAS", GOOGL_CIK, "Alphabet Inc.", "2005-12-18", "2005-12-20"),
            ("alphabet", W, "GOOGL", "XNAS", GOOGL_CIK, "Alphabet Inc.", "2006-04-02", "2006-04-04"),
            ("alphabet", W, "GOOG", "XNAS", GOOGL_CIK, "Alphabet Inc.", "2014-04-02", "2014-04-04"),
            ("alphabet", "massive", "GOOGL", "XNAS", GOOGL_CIK, "Alphabet Inc.", "2026-06-29", None),
        ],
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"GOOG", "GOOGL"}
    )

    assert {(r["old_symbol"], r["new_symbol"]) for r in manifest["relabels"]} == {("GOOG", "GOOGL")}
    assert _symbols_open(lake, ids["alphabet"], "2014-04-03") == {"GOOGL"}


# The next three tests are algorithm scaffolding (filler CIKs, real tickers in
# made-up histories), per the module docstring: no claim about these companies.


def test_r5_judges_each_relabel_against_the_whole_plan(tmp_path):
    """One security's relabel frees the ticker another's needs: judged claim
    by claim against the store as it stands, the outcome depended on
    `security_id` order and a second run appended the skipped relabel."""
    lake = tmp_path / "lake"
    ids = _seed(
        lake,
        [
            ("first", W, "SBC", "XNYS", "0000000001", "scaffold", "1996-01-01", "2000-01-01"),
            ("first", W, "T", "XNYS", "0000000001", "scaffold", "2000-01-01", None),
            ("second", W, "T", "XNYS", "0000000002", "scaffold", "1996-01-01", "2000-01-01"),
            ("second", W, "VZ", "XNYS", "0000000002", "scaffold", "2000-01-01", None),
        ],
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"T", "VZ"}
    )

    assert {(r["security_id"], r["new_symbol"]) for r in manifest["relabels"]} == {
        (ids["first"], "T"),
        (ids["second"], "VZ"),
    }
    counts = len(_identities(lake))
    membership_sync.repair_identity(
        indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"T", "VZ"}
    )
    assert len(_identities(lake)) == counts


def test_r5_two_relabels_that_collide_with_each_other_are_both_reported_and_apply_succeeds(tmp_path):
    lake = tmp_path / "lake"
    _seed(
        lake,
        [
            ("first", "massive", "T", "XNYS", "0000000001", "scaffold", "2010-01-01", None),
            ("first", W, "SBC", "XNYS", "0000000001", "scaffold", "2000-01-01", "2002-01-01"),
            ("second", W, "T", "XNAS", "0000000002", "scaffold", "2010-01-01", None),
            ("second", W, "BLS", "XNYS", "0000000002", "scaffold", "2001-01-01", "2003-01-01"),
        ],
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"T"}
    )

    assert manifest["relabels"] == []
    assert sorted(r["reason"] for r in manifest["relabels_skipped"]) == ["collision", "collision"]


def test_r5_by_elimination_only_for_a_current_member_and_never_a_placeholders_ticker(tmp_path):
    """Holding no listed ticker open, a security takes today's ticker by
    elimination only while it is an index member, and a placeholder member's
    ticker is taken: neither is visible as an open identity claim."""
    lake = tmp_path / "lake"
    ids = _seed(
        lake,
        [
            ("delisted", W, "KO", "XNYS", "0000000001", "scaffold", "1996-01-01", "2000-01-01"),
            ("delisted", W, "PEP", "XNYS", "0000000001", "scaffold", "2000-01-01", "2005-01-01"),
            ("member", W, "MO", "XNYS", "0000000002", "scaffold", "1996-01-01", "2000-01-01"),
            ("member", W, "PM", "XNYS", "0000000002", "scaffold", "2000-01-01", "2005-01-01"),
        ],
        [
            ("sp500", "delisted", "add", "1996-01-02"),
            ("sp500", "delisted", "remove", "2005-01-03"),
            ("sp500", "member", "add", "1996-01-02"),
        ],
    )
    refs = _evidence(SourceEvidenceStore(lake), "placeholder", "https://www.slickcharts.com/sp500", CHURN_T)
    _store(lake).append(
        _membership(
            refs,
            event_id="sp500-add-unresolved-PM",
            index_id="sp500",
            security_id="unresolved:PM",
            action="add",
            effective_at=CHURN_T,
            known_at=CHURN_T,
            status="candidate",
        )
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=True, current_tickers={"KO", "MO", "PM"}
    )

    assert {(r["security_id"], r["new_symbol"]) for r in manifest["relabels"]} == {(ids["member"], "MO")}
    assert {r["security_id"] for r in manifest["relabels_skipped"]} == {ids["delisted"]}


def test_r1_never_joins_two_researched_ids_whose_ciks_are_both_ambiguous(tmp_path):
    lake = tmp_path / "lake"
    _seed(
        lake,
        [
            ("successor", W, "XOM", "XNYS", "0002115436", "scaffold", "1996-01-01", "1996-01-03"),
            ("successor", W, "XOM", "XNYS", "0000000003", "scaffold", "1997-01-01", "1997-01-03"),
            ("other", W, "XOM", "XNYS", "0000000004", "scaffold", "1991-05-05", "1991-05-07"),
            ("other", W, "XOM", "XNYS", "0000000005", "scaffold", "1992-05-05", "1992-05-07"),
        ],
    )
    manifest = membership_sync.repair_identity(
        indexes=["sp500"], data_lake_root=lake, now=REPAIR_NOW, apply=False, current_tickers={"XOM"}
    )

    assert manifest["merges"] == []
