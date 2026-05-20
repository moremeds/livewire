"""Tests for FRED Treasury rates ingestion."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq

from clients.fred_client import FredObservation
from livewire_scripts.fetch_fred_rates import (
    DEFAULT_SERIES,
    main,
    observations_to_rate_rows,
    parse_args,
    run,
)


def test_default_series_are_requested():
    assert DEFAULT_SERIES == {
        "DGS3": 3.0,
        "DGS5": 5.0,
        "DGS10": 10.0,
        "DGS30": 30.0,
    }


def test_observations_to_rate_rows():
    rows = observations_to_rate_rows(
        "DGS10",
        10.0,
        [
            FredObservation(date="2026-05-14", value=4.47),
            FredObservation(date="2026-05-15", value=4.59),
        ],
    )

    assert rows == [
        {
            "trade_date": "2026-05-14",
            "symbol_id": rows[0]["symbol_id"],
            "tenor_years": 10.0,
            "yield_pct": 4.47,
            "source": "fred",
        },
        {
            "trade_date": "2026-05-15",
            "symbol_id": rows[0]["symbol_id"],
            "tenor_years": 10.0,
            "yield_pct": 4.59,
            "source": "fred",
        },
    ]


def test_parse_args_defaults_to_daily_all_series(tmp_path):
    args = parse_args(["--warehouse", str(tmp_path)])

    assert args.series == list(DEFAULT_SERIES)
    assert args.frequency == "d"
    assert args.warehouse == tmp_path


def test_run_fetches_and_persists_rates(tmp_path):
    client = MagicMock()
    client.fetch_observations.return_value = [
        FredObservation(date="2026-05-14", value=4.47),
        FredObservation(date="2026-05-15", value=4.59),
    ]

    rc = run(
        [
            "--warehouse", str(tmp_path),
            "--series", "DGS10",
            "--start", "2026-05-01",
            "--end", "2026-05-31",
            "--frequency", "d",
        ],
        client=client,
    )

    assert rc == 0
    client.fetch_observations.assert_called_once_with(
        "DGS10",
        observation_start="2026-05-01",
        observation_end="2026-05-31",
        frequency="d",
        aggregation_method="eop",
    )
    parquet_path = (
        Path(tmp_path)
        / "data-lake"
        / "bronze"
        / "asset_class=rates"
        / "symbol=DGS10"
        / "1d.parquet"
    )
    table = pq.ParquetFile(parquet_path).read()
    assert table.column_names == ["trade_date", "symbol_id", "tenor_years", "yield_pct", "source"]
    assert table.num_rows == 2


def test_run_constructs_default_fred_client(monkeypatch, tmp_path):
    client = MagicMock()
    client.fetch_observations.return_value = []

    with patch("livewire_scripts.fetch_fred_rates.FredClient", return_value=client) as client_cls:
        rc = run(["--warehouse", str(tmp_path), "--series", "DGS3"])

    assert rc == 0
    client_cls.assert_called_once_with()


def test_main_delegates_to_run(tmp_path):
    client = MagicMock()
    client.fetch_observations.return_value = []

    with patch("livewire_scripts.fetch_fred_rates.FredClient", return_value=client):
        rc = main(["--warehouse", str(tmp_path), "--series", "DGS5"])

    assert rc == 0
    client.fetch_observations.assert_called_once()
