from __future__ import annotations

import hashlib
import json
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
