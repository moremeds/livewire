from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from livewire_scripts import rebuild_silver, validate_silver_canary


def _bronze(root, symbol, closes):
    rows = [
        {
            "trade_date": date(2026, 1, index + 1).isoformat(),
            "symbol_id": index + 1,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "adj_close": close,
            "volume": 1_000,
        }
        for index, close in enumerate(closes)
    ]
    BronzeClient(root / "bronze/asset_class=equity", "equity").replace_ticker_rows(symbol, rows)


def _build_fixture(root):
    _bronze(root, "NVDA", (100.0, 100.0, 50.0))
    for symbol in ("AAPL", "SPY", "CONTROL"):
        _bronze(root, symbol, (100.0, 101.0, 102.0))
    split = MassiveSplit(
        provider_event_id="nvda-split",
        ticker="NVDA",
        execution_date=date(2026, 1, 3),
        split_from=Decimal("1"),
        split_to=Decimal("2"),
        payload_hash="split",
    )
    CorporateActionStore(root).reconcile("NVDA", [split], datetime(2026, 1, 4, tzinfo=UTC))
    assert rebuild_silver.run(["--full"], data_lake_root=root, silver_root=root / "silver") == 0


def test_canary_validates_named_symbols_control_and_bronze_immutability(tmp_path, capsys):
    _build_fixture(tmp_path)

    assert (
        validate_silver_canary.run(
            ["--tickers", "NVDA", "AAPL", "SPY", "--control", "CONTROL"],
            data_lake_root=tmp_path,
            silver_root=tmp_path / "silver",
        )
        == 0
    )

    report = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert report["passed"] is True
    assert report["bronze_unchanged"] is True
    assert set(report["symbols"]) == {"NVDA", "AAPL", "SPY", "CONTROL"}
    assert report["symbols"]["NVDA"]["ex_date_returns"]
    assert report["symbols"]["CONTROL"]["identity_control"] is True


def test_canary_fails_when_silver_volume_disagrees_with_factor(tmp_path, capsys):
    _build_fixture(tmp_path)
    path = tmp_path / "silver/asset_class=equity/symbol=NVDA/1d.parquet"
    table = pq.ParquetFile(path).read()
    volumes = table.column("volume").to_pylist()
    volumes[0] += 1
    table = table.set_column(table.schema.get_field_index("volume"), "volume", pa.array(volumes, pa.int64()))
    pq.write_table(table, path)

    assert (
        validate_silver_canary.run(
            ["--tickers", "NVDA", "AAPL", "SPY", "--control", "CONTROL"],
            data_lake_root=tmp_path,
            silver_root=tmp_path / "silver",
        )
        == 1
    )
    report = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert report["symbols"]["NVDA"]["passed"] is False
