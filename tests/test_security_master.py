from __future__ import annotations

import json
import multiprocessing
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from clients.security_master import SecurityIdentityEvent, SecurityMaster

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "shepherd" / "identity"
HASH = "a" * 64
REF = f"artifact://sha256/{HASH}"


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def verifies(ref: str, digest: str) -> bool:
    return ref == f"artifact://sha256/{digest}" and len(digest) == 64


def event(
    *,
    security_id: str,
    event_id: str,
    revision: int,
    symbol: str,
    effective_from: datetime,
    effective_to: datetime | None = None,
    known_at: datetime | None = None,
    status: str = "verified",
    cik: str | None = "0000000001",
    composite_figi: str | None = None,
    share_class_figi: str | None = None,
    issuer_name: str = "Example Issuer",
    provider: str = "massive",
    exchange_mic: str = "XNAS",
    continuity_basis: str = "responsible_publisher_action",
    supersedes: str | None = None,
    relationship_type: str | None = None,
    related_security_id: str | None = None,
) -> SecurityIdentityEvent:
    return SecurityIdentityEvent(
        event_id=event_id,
        security_id=security_id,
        revision=revision,
        symbol=symbol,
        provider=provider,
        exchange_mic=exchange_mic,
        currency="USD",
        effective_from=effective_from,
        effective_to=effective_to,
        known_at=known_at or effective_from,
        issuer_name=issuer_name,
        cik=cik,
        composite_figi=composite_figi,
        share_class_figi=share_class_figi,
        continuity_basis=continuity_basis,
        relationship_type=relationship_type,
        related_security_id=related_security_id,
        source_refs=(REF,),
        source_hashes=(HASH,),
        status=status,
        supersedes=supersedes,
    )


def _append_identity_in_process(root: str, suffix: str) -> None:
    store = SecurityMaster(root, evidence_verifier=verifies)
    store.append(
        event(
            security_id=store.new_security_id(),
            event_id=f"process-{suffix}",
            revision=1,
            symbol=f"PROC{suffix}",
            share_class_figi=f"BBG{suffix:0<9}"[:12],
            effective_from=dt("2024-01-01"),
        )
    )


def test_generated_security_id_is_opaque_and_not_ticker_derived(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    first = store.new_security_id()
    second = store.new_security_id()
    assert first.startswith("sec_")
    assert first != second
    assert "AAPL" not in first


def test_same_ticker_can_resolve_to_different_issuers_in_disjoint_intervals(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    old_id, new_id = store.new_security_id(), store.new_security_id()
    store.append(
        event(
            security_id=old_id,
            event_id="old-meta",
            revision=1,
            symbol="META",
            issuer_name="Roundhill ETF",
            cik="0001683471",
            effective_from=dt("2021-06-30"),
            effective_to=dt("2022-01-31"),
        )
    )
    store.append(
        event(
            security_id=new_id,
            event_id="new-meta",
            revision=1,
            symbol="META",
            issuer_name="Meta Platforms",
            cik="0001326801",
            share_class_figi="BBG001SQCQC5",
            effective_from=dt("2022-06-09"),
        )
    )
    assert store.resolve_symbol("massive", "META", "XNAS", dt("2021-08-01"), dt("2026-01-01")) == old_id
    assert store.resolve_symbol("massive", "META", "XNAS", dt("2023-01-01"), dt("2026-01-01")) == new_id


def test_symbol_rename_retains_security_id_and_preserves_exact_case(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    security_id = store.new_security_id()
    store.append(
        event(
            security_id=security_id,
            event_id="fb",
            revision=1,
            symbol="Fb",
            effective_from=dt("2012-05-18"),
            effective_to=dt("2022-06-09"),
        )
    )
    store.append(
        event(
            security_id=security_id,
            event_id="meta",
            revision=2,
            symbol="META",
            effective_from=dt("2022-06-09"),
            share_class_figi="BBG001SQCQC5",
        )
    )
    assert store.resolve_symbol("massive", "Fb", "XNAS", dt("2020-01-01"), dt("2026-01-01")) == security_id
    assert store.resolve_symbol("massive", "FB", "XNAS", dt("2020-01-01"), dt("2026-01-01")) is None
    assert store.resolve_symbol("massive", "META", "XNAS", dt("2023-01-01"), dt("2026-01-01")) == security_id


def test_overlapping_verified_symbol_or_figi_collision_is_rejected(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    first, second = store.new_security_id(), store.new_security_id()
    store.append(
        event(
            security_id=first,
            event_id="first",
            revision=1,
            symbol="SAME",
            share_class_figi="BBG000000001",
            effective_from=dt("2024-01-01"),
        )
    )
    with pytest.raises(ValueError, match="symbol interval collision"):
        store.append(
            event(
                security_id=second,
                event_id="second",
                revision=1,
                symbol="SAME",
                share_class_figi="BBG000000002",
                effective_from=dt("2024-02-01"),
            )
        )
    with pytest.raises(ValueError, match="share_class_figi collision"):
        store.append(
            event(
                security_id=second,
                event_id="figi-collision",
                revision=1,
                symbol="OTHER",
                share_class_figi="BBG000000001",
                effective_from=dt("2024-02-01"),
            )
        )


def test_same_cik_different_share_classes_remain_distinct(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    class_a, class_b = store.new_security_id(), store.new_security_id()
    store.append(
        event(
            security_id=class_a,
            event_id="brka",
            revision=1,
            symbol="BRK.A",
            cik="0001067983",
            composite_figi="BBG000DWCFL4",
            share_class_figi="BBG001S902J2",
            effective_from=dt("1980-03-17"),
        )
    )
    store.append(
        event(
            security_id=class_b,
            event_id="brkb",
            revision=1,
            symbol="BRK.B",
            cik="0001067983",
            composite_figi="BBG000DWG505",
            share_class_figi="BBG001S90346",
            effective_from=dt("1996-05-09"),
        )
    )
    assert class_a != class_b
    assert store.resolve_symbol("massive", "BRK.A", "XNAS", dt("2026-01-01"), dt("2026-01-01")) == class_a
    assert store.resolve_symbol("massive", "BRK.B", "XNAS", dt("2026-01-01"), dt("2026-01-01")) == class_b


def test_missing_identifiers_remain_candidate_and_bad_evidence_cannot_verify(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    security_id = store.new_security_id()
    store.append(
        event(
            security_id=security_id,
            event_id="candidate",
            revision=1,
            symbol="MISSING",
            status="candidate",
            cik=None,
            continuity_basis="insufficient",
            effective_from=dt("2024-01-01"),
        )
    )
    assert store.is_verified(security_id, dt("2024-02-01"), dt("2024-02-01")) is False
    with pytest.raises(ValueError, match="strong continuity evidence"):
        store.append(
            event(
                security_id=store.new_security_id(),
                event_id="unevidenced",
                revision=1,
                symbol="NOPE",
                status="verified",
                cik=None,
                continuity_basis="insufficient",
                effective_from=dt("2024-01-01"),
            )
        )
    rejecting_store = SecurityMaster(tmp_path / "bad", evidence_verifier=lambda _ref, _hash: False)
    with pytest.raises(ValueError, match="evidence verification"):
        rejecting_store.append(
            event(
                security_id=rejecting_store.new_security_id(),
                event_id="bad-source",
                revision=1,
                symbol="BAD",
                effective_from=dt("2024-01-01"),
            )
        )


def test_naive_clocks_and_malformed_external_identifiers_are_rejected(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    with pytest.raises(ValueError, match="timezone-aware"):
        store.append(
            event(
                security_id=store.new_security_id(),
                event_id="naive",
                revision=1,
                symbol="NAIVE",
                effective_from=datetime(2024, 1, 1),
            )
        )
    with pytest.raises(ValueError, match="CIK"):
        store.append(
            event(
                security_id=store.new_security_id(),
                event_id="bad-cik",
                revision=1,
                symbol="CIK",
                cik="123",
                effective_from=dt("2024-01-01"),
            )
        )


def test_merger_spinoff_and_provider_disagreement_preserve_distinct_claims(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    target, buyer = store.new_security_id(), store.new_security_id()
    store.append(
        event(
            security_id=target,
            event_id="target",
            revision=1,
            symbol="TARGET",
            effective_from=dt("2020-01-01"),
            effective_to=dt("2024-03-01"),
        )
    )
    store.append(
        event(
            security_id=buyer,
            event_id="buyer",
            revision=1,
            symbol="BUYER",
            effective_from=dt("2010-01-01"),
            relationship_type="merger_consideration_for",
            related_security_id=target,
        )
    )
    assert store.resolve_symbol("massive", "TARGET", "XNAS", dt("2024-03-01"), dt("2024-04-01")) is None
    assert target != buyer

    parent, child = store.new_security_id(), store.new_security_id()
    store.append(
        event(
            security_id=parent,
            event_id="parent",
            revision=1,
            symbol="PARENT",
            effective_from=dt("2000-01-01"),
        )
    )
    store.append(
        event(
            security_id=child,
            event_id="child",
            revision=1,
            symbol="CHILD",
            effective_from=dt("2023-01-04"),
            relationship_type="spinoff_child_of",
            related_security_id=parent,
        )
    )
    assert parent != child
    conflict_a, conflict_b = store.new_security_id(), store.new_security_id()
    store.append(
        event(
            security_id=conflict_a,
            event_id="conflict-a",
            revision=1,
            symbol="CONFLICT",
            status="unresolved",
            effective_from=dt("2024-01-01"),
        )
    )
    store.append(
        event(
            security_id=conflict_b,
            event_id="conflict-b",
            revision=1,
            symbol="CONFLICT",
            status="unresolved",
            effective_from=dt("2024-01-01"),
        )
    )
    assert store.resolve_symbol("massive", "CONFLICT", "XNAS", dt("2024-02-01"), dt("2024-02-01")) is None


def test_concurrent_processes_do_not_lose_identity_events(tmp_path):
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(target=_append_identity_in_process, args=(str(tmp_path), str(index))) for index in range(4)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    assert len(SecurityMaster(tmp_path, evidence_verifier=verifies).events()) == 4


def test_later_correction_is_visible_only_after_its_known_at(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    security_id = store.new_security_id()
    original = event(
        security_id=security_id,
        event_id="original",
        revision=1,
        symbol="OLD",
        effective_from=dt("2024-01-01"),
        known_at=dt("2024-01-02"),
        share_class_figi="BBG000000010",
    )
    store.append(original)
    store.append(
        replace(
            original,
            event_id="correction",
            revision=2,
            symbol="REVISED",
            known_at=dt("2024-03-05"),
            share_class_figi="BBG000000011",
            supersedes="original",
        )
    )
    assert store.resolve_symbol("massive", "OLD", "XNAS", dt("2024-02-01"), dt("2024-02-01")) == security_id
    assert store.resolve_symbol("massive", "OLD", "XNAS", dt("2024-04-01"), dt("2024-04-01")) is None
    assert store.resolve_symbol("massive", "REVISED", "XNAS", dt("2024-04-01"), dt("2024-04-01")) == security_id


def test_rejected_correction_can_release_a_misattributed_figi(tmp_path):
    store = SecurityMaster(tmp_path, evidence_verifier=verifies)
    wrong_id = store.new_security_id()
    original = event(
        security_id=wrong_id,
        event_id="wrong-original",
        revision=1,
        symbol="WRONG",
        share_class_figi="BBG000000077",
        effective_from=dt("2024-01-01"),
        known_at=dt("2024-01-02"),
    )
    store.append(original)
    store.append(
        replace(
            original,
            event_id="wrong-rejected",
            revision=2,
            status="rejected",
            known_at=dt("2024-03-01"),
            supersedes="wrong-original",
        )
    )
    correct_id = store.new_security_id()
    store.append(
        event(
            security_id=correct_id,
            event_id="correct",
            revision=1,
            symbol="RIGHT",
            share_class_figi="BBG000000077",
            effective_from=dt("2024-01-01"),
            known_at=dt("2024-03-02"),
        )
    )
    assert store.resolve_symbol("massive", "WRONG", "XNAS", dt("2024-02-01"), dt("2024-02-01")) == wrong_id
    assert store.resolve_symbol("massive", "RIGHT", "XNAS", dt("2024-02-01"), dt("2024-02-01")) is None
    assert store.resolve_symbol("massive", "RIGHT", "XNAS", dt("2024-02-01"), dt("2024-04-01")) == correct_id


@pytest.mark.parametrize("case", json.loads((FIXTURE_ROOT / "cases.json").read_text())["cases"])
def test_frozen_contract_publication_state_is_representable(tmp_path, case):
    store = SecurityMaster(tmp_path / case["case_id"], evidence_verifier=verifies)
    security_id = store.new_security_id()
    expected_state = case["expected"]["publication_state"]
    item = event(
        security_id=security_id,
        event_id=case["case_id"],
        revision=1,
        symbol=case["observations"][0]["symbol"],
        status=expected_state,
        cik=case["observations"][0].get("cik"),
        share_class_figi=case["observations"][0].get("share_class_figi"),
        continuity_basis="responsible_publisher_action" if expected_state == "verified" else "insufficient",
        effective_from=dt("2024-01-01"),
    )
    store.append(item)
    assert store.is_verified(security_id, dt("2024-02-01"), dt("2024-02-01")) is (expected_state == "verified")
