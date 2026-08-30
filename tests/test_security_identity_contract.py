from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "shepherd" / "identity"
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def load(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text())


def test_fixture_manifest_freezes_exact_contract_bytes():
    manifest = load("manifest.json")
    assert manifest["schema_version"] == 1
    assert set(manifest["files"]) == {"sources.json", "cases.json"}
    for name, expected_hash in manifest["files"].items():
        assert SHA256.fullmatch(expected_hash)
        assert hashlib.sha256((FIXTURE_ROOT / name).read_bytes()).hexdigest() == expected_hash


def test_source_inventory_records_obtainable_fields_and_gaps():
    document = load("sources.json")
    required_kinds = {
        "wikipedia_revision",
        "sec_company_tickers",
        "sec_submission",
        "massive_ticker_details",
        "issuer_or_corporate_action",
    }
    assert required_kinds <= {source["kind"] for source in document["sources"]}
    assert document["observed_collisions"]
    for source in document["sources"]:
        assert source["source_id"]
        assert source["source_url"].startswith("https://")
        assert datetime.fromisoformat(source["retrieved_at"]).tzinfo is not None
        assert source["source_revision"]
        assert SHA256.fullmatch(source["raw_sha256"])
        assert isinstance(source["missing_fields"], list)
        assert source["selected_records"]
        assert "api_key" not in json.dumps(source).lower()


def test_policy_cases_cover_every_required_collision_and_expected_disposition():
    sources = {source["source_id"] for source in load("sources.json")["sources"]}
    cases = load("cases.json")["cases"]
    required_scenarios = {
        "same_ticker_different_issuer",
        "symbol_rename_same_security",
        "share_classes",
        "merger",
        "spinoff",
        "missing_cik_and_figi",
        "provider_disagreement",
        "retrospective_identifier_change",
    }
    assert {case["scenario"] for case in cases} == required_scenarios
    for case in cases:
        assert case["case_id"]
        assert case["expected"]["identity_disposition"]
        assert case["expected"]["publication_state"] in {"verified", "candidate", "unresolved"}
        assert set(case["evidence_refs"]) <= sources
        assert case["observations"]
        assert "ticker" not in case["continuity_keys"]


def test_cik_is_issuer_evidence_not_a_share_class_identity():
    case = next(case for case in load("cases.json")["cases"] if case["scenario"] == "share_classes")
    assert case["observations"][0]["cik"] == case["observations"][1]["cik"]
    assert case["observations"][0]["share_class_figi"] != case["observations"][1]["share_class_figi"]
    assert case["expected"]["identity_disposition"] == "distinct_identities"


def test_unresolved_or_candidate_identity_cannot_enter_pit_silver():
    cases = load("cases.json")["cases"]
    blocked = [case for case in cases if case["expected"]["publication_state"] != "verified"]
    assert blocked
    assert all(case["expected"]["pit_silver_allowed"] is False for case in blocked)


def test_retrospective_identifier_change_never_rewrites_prior_knowledge():
    case = next(case for case in load("cases.json")["cases"] if case["scenario"] == "retrospective_identifier_change")
    assert case["expected"]["identity_disposition"] == "append_correction"
    assert case["expected"]["rewrite_prior_known_at"] is False
