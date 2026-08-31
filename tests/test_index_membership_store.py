from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.security_master import SecurityIdentityEvent, SecurityMaster

HASH = "b" * 64
REF = f"artifact://sha256/{HASH}"


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def verifies(ref: str, digest: str) -> bool:
    return ref == f"artifact://sha256/{digest}"


def verified_security(store: SecurityMaster, symbol: str, known_at: datetime) -> str:
    security_id = store.new_security_id()
    store.append(
        SecurityIdentityEvent(
            event_id=f"identity-{symbol}",
            security_id=security_id,
            revision=1,
            symbol=symbol,
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=dt("2000-01-01"),
            effective_to=None,
            known_at=known_at,
            issuer_name=f"{symbol} issuer",
            cik="0000000001",
            composite_figi=f"BBG{symbol:0<9}"[:12],
            share_class_figi=None,
            continuity_basis="provider_figi",
            relationship_type=None,
            related_security_id=None,
            source_refs=(REF,),
            source_hashes=(HASH,),
            status="verified",
            supersedes=None,
        )
    )
    return security_id


def membership(
    *,
    event_id: str,
    security_id: str,
    action: str,
    effective_at: datetime,
    known_at: datetime,
    revision: int = 1,
    supersedes: str | None = None,
    status: str = "verified",
    index_id: str = "sp500",
) -> MembershipEvent:
    return MembershipEvent(
        event_id=event_id,
        index_id=index_id,
        security_id=security_id,
        action=action,
        announced_at=known_at,
        effective_at=effective_at,
        known_at=known_at,
        source_refs=(REF,),
        source_hashes=(HASH,),
        revision=revision,
        supersedes=supersedes,
        status=status,
    )


def stores(tmp_path):
    security = SecurityMaster(tmp_path, evidence_verifier=verifies)
    members = IndexMembershipStore(tmp_path, security_master=security, evidence_verifier=verifies)
    return security, members


def test_membership_respects_both_effective_and_known_time(tmp_path):
    security, members = stores(tmp_path)
    apple = verified_security(security, "AAPL", dt("2024-01-01"))
    members.append(
        membership(
            event_id="aapl-add",
            security_id=apple,
            action="add",
            effective_at=dt("2024-02-01"),
            known_at=dt("2024-01-15"),
        )
    )
    assert members.members_effective_at("sp500", dt("2024-01-31"), dt("2024-02-10")) == set()
    assert members.members_effective_at("sp500", dt("2024-02-01"), dt("2024-01-14")) == set()
    assert members.members_effective_at("sp500", dt("2024-02-01"), dt("2024-01-15")) == {apple}


def test_removal_and_readdition_preserve_history(tmp_path):
    security, members = stores(tmp_path)
    member = verified_security(security, "KEEP", dt("2019-01-01"))
    members.append(
        membership(
            event_id="add-1", security_id=member, action="add", effective_at=dt("2020-01-01"), known_at=dt("2019-12-15")
        )
    )
    members.append(
        membership(
            event_id="remove",
            security_id=member,
            action="remove",
            effective_at=dt("2021-01-01"),
            known_at=dt("2020-12-15"),
            revision=2,
        )
    )
    members.append(
        membership(
            event_id="add-2",
            security_id=member,
            action="add",
            effective_at=dt("2022-01-01"),
            known_at=dt("2021-12-15"),
            revision=3,
        )
    )
    assert members.members_effective_at("sp500", dt("2020-06-01"), dt("2023-01-01")) == {member}
    assert members.members_effective_at("sp500", dt("2021-06-01"), dt("2023-01-01")) == set()
    assert members.members_effective_at("sp500", dt("2022-06-01"), dt("2023-01-01")) == {member}


def test_candidate_identity_cannot_back_verified_membership(tmp_path):
    security, members = stores(tmp_path)
    candidate = security.new_security_id()
    security.append(
        SecurityIdentityEvent(
            event_id="candidate",
            security_id=candidate,
            revision=1,
            symbol="MISSING",
            provider="wiki",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=dt("2024-01-01"),
            effective_to=None,
            known_at=dt("2024-01-01"),
            issuer_name="Missing",
            cik=None,
            composite_figi=None,
            share_class_figi=None,
            continuity_basis="insufficient",
            relationship_type=None,
            related_security_id=None,
            source_refs=(REF,),
            source_hashes=(HASH,),
            status="candidate",
            supersedes=None,
        )
    )
    with pytest.raises(ValueError, match="verified security identity"):
        members.append(
            membership(
                event_id="bad-add",
                security_id=candidate,
                action="add",
                effective_at=dt("2024-02-01"),
                known_at=dt("2024-02-01"),
            )
        )


def test_later_membership_correction_does_not_leak_backward(tmp_path):
    security, members = stores(tmp_path)
    member = verified_security(security, "PIT", dt("2024-01-01"))
    original = membership(
        event_id="original", security_id=member, action="add", effective_at=dt("2024-02-01"), known_at=dt("2024-01-15")
    )
    members.append(original)
    members.append(
        membership(
            event_id="correction",
            security_id=member,
            action="add",
            effective_at=dt("2024-03-01"),
            known_at=dt("2024-04-01"),
            revision=2,
            supersedes="original",
        )
    )
    assert members.members_effective_at("sp500", dt("2024-02-15"), dt("2024-02-15")) == {member}
    assert members.members_effective_at("sp500", dt("2024-02-15"), dt("2024-04-02")) == set()
    assert members.members_effective_at("sp500", dt("2024-03-01"), dt("2024-04-02")) == {member}


def test_later_unresolved_identity_correction_removes_membership_only_after_known(tmp_path):
    security, members = stores(tmp_path)
    member = verified_security(security, "IDENTITY", dt("2024-01-01"))
    members.append(
        membership(
            event_id="identity-add",
            security_id=member,
            action="add",
            effective_at=dt("2024-01-01"),
            known_at=dt("2024-01-01"),
        )
    )
    original = security.events()[0]
    security.append(
        replace(
            original,
            event_id="identity-disputed",
            revision=2,
            status="unresolved",
            known_at=dt("2024-04-01"),
            supersedes=original.event_id,
        )
    )
    assert members.members_effective_at("sp500", dt("2024-02-01"), dt("2024-02-01")) == {member}
    assert members.members_effective_at("sp500", dt("2024-02-01"), dt("2024-04-02")) == set()


def test_unresolved_membership_is_preserved_but_not_queryable(tmp_path):
    security, members = stores(tmp_path)
    member = verified_security(security, "WAIT", dt("2024-01-01"))
    members.append(
        membership(
            event_id="uncertain",
            security_id=member,
            action="add",
            effective_at=dt("2024-02-01"),
            known_at=dt("2024-02-01"),
            status="unresolved",
        )
    )
    assert len(members.events("sp500")) == 1
    assert members.members_effective_at("sp500", dt("2024-03-01"), dt("2024-03-01")) == set()


def test_removal_is_allowed_at_the_identity_interval_end(tmp_path):
    security, members = stores(tmp_path)
    member = security.new_security_id()
    security.append(
        SecurityIdentityEvent(
            event_id="ending-identity",
            security_id=member,
            revision=1,
            symbol="ENDS",
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=dt("2020-01-01"),
            effective_to=dt("2024-03-01"),
            known_at=dt("2020-01-01"),
            issuer_name="Ending issuer",
            cik="0000000001",
            composite_figi="BBG000000099",
            share_class_figi=None,
            continuity_basis="provider_figi",
            relationship_type=None,
            related_security_id=None,
            source_refs=(REF,),
            source_hashes=(HASH,),
            status="verified",
            supersedes=None,
        )
    )
    members.append(
        membership(
            event_id="ending-add",
            security_id=member,
            action="add",
            effective_at=dt("2020-01-01"),
            known_at=dt("2020-01-01"),
        )
    )
    members.append(
        membership(
            event_id="ending-remove",
            security_id=member,
            action="remove",
            effective_at=dt("2024-03-01"),
            known_at=dt("2024-02-15"),
            revision=2,
        )
    )
    assert members.members_effective_at("sp500", dt("2024-03-01"), dt("2024-03-01")) == set()


def test_naive_membership_clock_is_rejected(tmp_path):
    security, members = stores(tmp_path)
    member = verified_security(security, "CLOCK", dt("2024-01-01"))
    with pytest.raises(ValueError, match="timezone-aware"):
        members.append(
            membership(
                event_id="naive",
                security_id=member,
                action="add",
                effective_at=datetime(2024, 2, 1),
                known_at=dt("2024-02-01"),
            )
        )


def test_rejects_index_path_traversal_and_unverified_evidence(tmp_path):
    security, members = stores(tmp_path)
    member = verified_security(security, "SAFE", dt("2024-01-01"))
    with pytest.raises(ValueError, match="index_id"):
        members.events("../escape")
    bad = IndexMembershipStore(tmp_path / "bad", security_master=security, evidence_verifier=lambda _ref, _hash: False)
    with pytest.raises(ValueError, match="evidence verification"):
        bad.append(
            membership(
                event_id="bad",
                security_id=member,
                action="add",
                effective_at=dt("2024-02-01"),
                known_at=dt("2024-02-01"),
            )
        )
