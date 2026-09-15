"""Tests for FRED Treasury rates ingestion."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pyarrow.parquet as pq

from clients.constants import declared
from clients.fred_client import FRED_OBSERVATIONS_URL, FredClient, FredObservation
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
            "--warehouse",
            str(tmp_path),
            "--series",
            "DGS10",
            "--start",
            "2026-05-01",
            "--end",
            "2026-05-31",
            "--frequency",
            "d",
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
    parquet_path = Path(tmp_path) / "data-lake" / "bronze" / "asset_class=rates" / "symbol=DGS10" / "1d.parquet"
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


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.stlouisfed.org/fred/series/observations")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"Server error '{status_code}'", request=request, response=response)


def test_one_failing_series_does_not_stop_the_others_and_the_run_still_fails(tmp_path):
    """2026-09-14: a 502 on DGS5 aborted the loop, so DGS10/DGS30 were never requested."""
    observations = [FredObservation(date="2026-09-10", value=4.95)]

    def fetch(series_id, **_kwargs):
        if series_id == "DGS5":
            raise _status_error(502)
        return observations

    client = MagicMock()
    client.fetch_observations.side_effect = fetch

    rc = run(["--warehouse", str(tmp_path)], client=client)

    assert rc == 1
    assert [call.args[0] for call in client.fetch_observations.call_args_list] == list(DEFAULT_SERIES)
    bronze = Path(tmp_path) / "data-lake" / "bronze" / "asset_class=rates"
    assert (bronze / "symbol=DGS10" / "1d.parquet").exists()
    assert (bronze / "symbol=DGS30" / "1d.parquet").exists()
    assert not (bronze / "symbol=DGS5" / "1d.parquet").exists()


def test_a_total_outage_is_a_failure_not_an_empty_success(tmp_path):
    """The fetch_batch rule: a raised fetch never reads as errors=0, exit 0."""
    client = MagicMock()
    client.fetch_observations.side_effect = _status_error(502)

    rc = run(["--warehouse", str(tmp_path)], client=client)

    assert rc == 1
    assert client.fetch_observations.call_count == len(DEFAULT_SERIES)


def test_a_series_with_no_observations_is_not_a_failure(tmp_path):
    """FRED publishing nothing new is the normal weekend/lag state, not an error."""
    client = MagicMock()
    client.fetch_observations.return_value = []

    rc = run(["--warehouse", str(tmp_path)], client=client)

    assert rc == 0


def test_the_real_client_failing_at_the_transport_seam_is_caught_per_series(tmp_path, monkeypatch):
    """Rule 6: exercise the real FredClient, not a MagicMock, at the seam run() catches.

    DGS5 502s on every attempt; the other three answer. run() must publish the
    three, request all four, and still return 1.
    """
    monkeypatch.setattr("clients.fred_client.time.sleep", lambda _s: None)
    request = httpx.Request("GET", FRED_OBSERVATIONS_URL)
    payload = {"observations": [{"date": "2026-09-10", "value": "4.95"}]}

    class _Http:
        def __init__(self) -> None:
            self.series: list[str] = []

        def get(self, url, *, params, timeout):
            self.series.append(params["series_id"])
            if params["series_id"] == "DGS5":
                return httpx.Response(502, json={}, request=request)
            return httpx.Response(200, json=payload, request=request)

    http = _Http()
    rc = run(
        ["--warehouse", str(tmp_path)],
        client=FredClient(api_key="test-key", http_client=http),
    )

    assert rc == 1
    assert http.series.count("DGS5") == int(declared("fred_retry_attempts"))
    assert set(http.series) == set(DEFAULT_SERIES)
    bronze = Path(tmp_path) / "data-lake" / "bronze" / "asset_class=rates"
    assert (bronze / "symbol=DGS30" / "1d.parquet").exists()
    assert not (bronze / "symbol=DGS5" / "1d.parquet").exists()
