from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from livewire_scripts import audit_split_basis


def _seed(root):
    rows = []
    for day, close in ((date(2020, 8, 28), 25.0), (date(2020, 8, 31), 26.0)):
        rows.append(
            {
                "trade_date": day.isoformat(),
                "symbol_id": 1,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "adj_close": close,
                "volume": 400,
                "source": "legacy",
                "price_basis": "unknown",
            }
        )
    bronze = root / "bronze/asset_class=equity"
    BronzeClient(bronze, "equity").replace_ticker_rows("AAPL", rows)
    split = MassiveSplit(
        provider_event_id="aapl-split",
        ticker="AAPL",
        execution_date=date(2020, 8, 31),
        split_from=Decimal("1"),
        split_to=Decimal("4"),
        payload_hash="split",
    )
    CorporateActionStore(root).reconcile("AAPL", [split], datetime(2020, 8, 31, tzinfo=UTC))
    return bronze / "symbol=AAPL/1d.parquet"


def test_audit_is_read_only_and_proposes_raw_rows(tmp_path):
    path = _seed(tmp_path)
    before = path.read_bytes()
    output = tmp_path / "audit.json"

    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(output)], data_lake_root=tmp_path) == 0

    assert path.read_bytes() == before
    manifest = json.loads(output.read_text())
    item = manifest["symbols"][0]
    assert item["source_sha256"] == hashlib.sha256(before).hexdigest()
    assert item["eligible"] is True
    assert item["classifications"][0]["treatment"] == "adjusted"
    assert item["replacements"][0]["original"]["close"] == 25.0
    assert item["replacements"][0]["proposed"]["close"] == 100.0
    assert item["replacements"][0]["proposed"]["source"] == "legacy"
    assert item["replacements"][0]["proposed"]["price_basis"] == "raw"


def test_ambiguous_boundary_is_not_repair_eligible(tmp_path):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[1]["close"] = rows[1]["open"] = rows[1]["high"] = rows[1]["low"] = rows[1]["adj_close"] = 17.5
    client.replace_ticker_rows("AAPL", rows)
    output = tmp_path / "audit.json"

    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(output)], data_lake_root=tmp_path) == 1

    item = json.loads(output.read_text())["symbols"][0]
    assert item["eligible"] is False
    assert item["replacements"] == []


def test_invalid_normalized_price_is_recorded_without_aborting_full_audit(tmp_path):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[0]["low"] = 0.0
    client.replace_ticker_rows("AAPL", rows)
    output = tmp_path / "audit.json"

    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(output)], data_lake_root=tmp_path) == 1

    item = json.loads(output.read_text())["symbols"][0]
    assert item["eligible"] is False
    assert item["error"] == "normalized low must be positive"
    assert item["replacements"] == []


def test_symbol_without_split_evidence_has_no_proposed_replacements(tmp_path):
    bronze = tmp_path / "bronze/asset_class=equity"
    BronzeClient(bronze, "equity").replace_ticker_rows(
        "MSFT",
        [
            {
                "trade_date": "2026-01-02",
                "symbol_id": 2,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "adj_close": 100.0,
                "volume": 1000,
                "source": "legacy",
                "price_basis": "unknown",
            }
        ],
    )
    output = tmp_path / "audit.json"

    assert audit_split_basis.run(["--tickers", "MSFT", "--output", str(output)], data_lake_root=tmp_path) == 0

    item = json.loads(output.read_text())["symbols"][0]
    assert item["classifications"] == []
    assert item["eligible"] is True
    assert item["replacements"] == []


def test_split_before_stored_history_does_not_block_audit(tmp_path):
    bronze = tmp_path / "bronze/asset_class=equity"
    BronzeClient(bronze, "equity").replace_ticker_rows(
        "TEST",
        [
            {
                "trade_date": "2026-01-02",
                "symbol_id": 3,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "adj_close": 100.0,
                "volume": 1000,
                "source": "legacy",
                "price_basis": "unknown",
            }
        ],
    )
    split = MassiveSplit(
        provider_event_id="test-prehistory-split",
        ticker="TEST",
        execution_date=date(2020, 8, 31),
        split_from=Decimal("1"),
        split_to=Decimal("4"),
        payload_hash="prehistory-split",
    )
    CorporateActionStore(tmp_path).reconcile("TEST", [split], datetime(2026, 1, 2, tzinfo=UTC))
    output = tmp_path / "audit.json"

    assert audit_split_basis.run(["--tickers", "TEST", "--output", str(output)], data_lake_root=tmp_path) == 0

    item = json.loads(output.read_text())["symbols"][0]
    assert item["classifications"] == []
    assert item["eligible"] is True
    assert item["replacements"] == []


def test_audit_replays_ib_evidence_to_resolve_ambiguous_boundary(tmp_path):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[1]["close"] = rows[1]["open"] = rows[1]["high"] = rows[1]["low"] = rows[1]["adj_close"] = 17.5
    client.replace_ticker_rows("AAPL", rows)
    initial = tmp_path / "initial.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(initial)], data_lake_root=tmp_path) == 1
    item = json.loads(initial.read_text())["symbols"][0]
    action_id = item["classifications"][0]["action_id"]
    provider_rows = [
        {"trade_date": "2020-08-28", "close": 25.0},
        {"trade_date": "2020-08-31", "close": 17.5},
    ]
    evidence = tmp_path / "evidence/symbols"
    evidence.mkdir(parents=True)
    (evidence / "AAPL.json").write_text(
        json.dumps(
            {
                "data_lake_root": str(tmp_path.resolve()),
                "events": [
                    {
                        "action_id": action_id,
                        "provider": "massive",
                        "provider_runs": [provider_rows, provider_rows],
                        "status": "resolved",
                    }
                ],
                "source_sha256": item["source_sha256"],
                "status": "resolved",
                "symbol": "AAPL",
            },
            sort_keys=True,
        )
    )
    output = tmp_path / "resolved.json"

    assert (
        audit_split_basis.run(
            ["--tickers", "AAPL", "--output", str(output), "--evidence-dir", str(evidence.parent)],
            data_lake_root=tmp_path,
        )
        == 0
    )

    resolved = json.loads(output.read_text())["symbols"][0]
    assert resolved["eligible"] is True
    assert resolved["classifications"][0]["treatment"] == "adjusted"
    assert resolved["resolution_evidence_sha256"] is not None
    assert resolved["replacements"]


def test_audit_replays_raw_ib_evidence_using_its_detected_basis(tmp_path, monkeypatch):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[0].update({column: 100.0 for column in ("open", "high", "low", "close", "adj_close")})
    client.replace_ticker_rows("AAPL", rows)
    initial = tmp_path / "initial.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(initial)], data_lake_root=tmp_path) == 0
    item = json.loads(initial.read_text())["symbols"][0]
    action_id = item["classifications"][0]["action_id"]
    provider_rows = [
        {"trade_date": "2020-08-28", "close": 100.0},
        {"trade_date": "2020-08-31", "close": 26.0},
    ]
    evidence = tmp_path / "evidence/symbols"
    evidence.mkdir(parents=True)
    (evidence / "AAPL.json").write_text(
        json.dumps(
            {
                "data_lake_root": str(tmp_path.resolve()),
                "events": [
                    {
                        "action_id": action_id,
                        "provider": "ib",
                        "provider_runs": [provider_rows, provider_rows],
                        "status": "resolved",
                    }
                ],
                "source_sha256": item["source_sha256"],
                "status": "resolved",
                "symbol": "AAPL",
            },
            sort_keys=True,
        )
    )
    original_classifier = audit_split_basis.classify_split_events

    def force_ambiguous(*args, **kwargs):
        return [replace(item, treatment="ambiguous") for item in original_classifier(*args, **kwargs)]

    monkeypatch.setattr(audit_split_basis, "classify_split_events", force_ambiguous)
    output = tmp_path / "resolved.json"

    assert (
        audit_split_basis.run(
            ["--tickers", "AAPL", "--output", str(output), "--evidence-dir", str(evidence.parent)],
            data_lake_root=tmp_path,
        )
        == 0
    )
    resolved = json.loads(output.read_text())["symbols"][0]
    assert resolved["classifications"][0]["treatment"] == "raw"
