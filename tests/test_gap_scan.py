import json
from datetime import date

from clients.gap_engine import Finding
from livewire_scripts.gap_scan import (
    write_decision_requests,
    write_tier_a_manifest,
)

FLOOR = date(2021, 7, 12)


def _finding(symbol: str, session: date, tier: str = "A") -> Finding:
    return Finding(
        symbol=symbol,
        asset_class="equity",
        timeframe="1d",
        gap="G2",
        sessions=(session,),
        heal_by_days=(session - FLOOR).days,
        tier=tier,
        source="massive",
    )


def test_tier_a_manifest_is_ordered_by_heal_by(tmp_path):
    """Sessions nearest the rolling Massive floor lose the cheap repair path first."""
    urgent = _finding("MUNJ", date(2021, 8, 2))
    relaxed = _finding("AAPL", date(2026, 8, 27))
    path = tmp_path / "manifest.json"
    write_tier_a_manifest([relaxed, urgent], path)
    manifest = json.loads(path.read_text())
    assert [entry["symbol"] for entry in manifest["repairs"]] == ["MUNJ", "AAPL"]


def test_tier_b_uses_the_triage_breaks_verdict_vocabulary(tmp_path):
    """Spec 15: adopt the existing vocabulary rather than inventing a schema."""
    path = tmp_path / "decisions.json"
    write_decision_requests([_finding("MUNJ", date(2026, 8, 27), tier="B")], path)
    requests = json.loads(path.read_text())
    assert requests[0]["verdict"] == "inconclusive"
    assert requests[0]["symbol"] == "MUNJ"


def test_tier_b_findings_never_enter_the_repair_manifest(tmp_path):
    path = tmp_path / "manifest.json"
    write_tier_a_manifest([_finding("MUNJ", date(2026, 8, 27), tier="B")], path)
    assert json.loads(path.read_text())["repairs"] == []


def test_a_finding_with_no_expiry_sorts_last_not_first(tmp_path):
    """heal_by_days is None when the repair source has no rolling window.
    Sorting None first would put the never-expiring repairs at the top of the
    urgency queue, which is exactly backwards."""
    expiring = _finding("MUNJ", date(2021, 8, 2))
    never = Finding(
        symbol="DGS10",
        asset_class="rates",
        timeframe="1d",
        gap="G1",
        sessions=(date(2026, 8, 27),),
        heal_by_days=None,
        tier="A",
        source="fred",
    )
    path = tmp_path / "manifest.json"
    write_tier_a_manifest([never, expiring], path)
    assert [e["symbol"] for e in json.loads(path.read_text())["repairs"]] == [
        "MUNJ",
        "DGS10",
    ]
