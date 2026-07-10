from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from clients.massive_client import MassiveClient
from clients.massive_daily_flatfile_store import _us_ts_to_iso_date
from clients.massive_time import massive_timestamp_to_trade_date

_OBSERVED_AAPL_TIMESTAMPS = (
    (date(2024, 1, 3), 1704315600000, 1704258000000000000),
    (date(2024, 3, 8), 1709931600000, 1709874000000000000),
    (date(2024, 3, 11), 1710187200000, 1710129600000000000),
    (date(2024, 6, 3), 1717444800000, 1717387200000000000),
    (date(2024, 11, 1), 1730491200000, 1730433600000000000),
    (date(2024, 11, 4), 1730754000000, 1730696400000000000),
)


@pytest.mark.parametrize(("expected", "rest_ms", "s3_ns"), _OBSERVED_AAPL_TIMESTAMPS)
def test_observed_rest_and_s3_timestamps_map_to_same_trade_date(expected, rest_ms, s3_ns):
    rest_instant = datetime.fromtimestamp(rest_ms / 1000, UTC)
    s3_instant = datetime.fromtimestamp(s3_ns / 1_000_000_000, UTC)

    assert massive_timestamp_to_trade_date(rest_instant) == expected
    assert massive_timestamp_to_trade_date(s3_instant) == expected


def test_naive_provider_timestamp_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        massive_timestamp_to_trade_date(datetime(2024, 6, 3, 4, 0))


@pytest.mark.parametrize(("expected", "rest_ms", "s3_ns"), _OBSERVED_AAPL_TIMESTAMPS)
def test_rest_and_flatfile_entrypoints_agree(expected, rest_ms, s3_ns):
    rest_bar = MassiveClient.normalize_daily_bar(
        {"t": rest_ms, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 10},
        ticker="AAPL",
    )
    s3_timestamp = datetime.fromtimestamp(s3_ns / 1_000_000_000, UTC)

    assert rest_bar.trade_date == expected
    assert _us_ts_to_iso_date(s3_timestamp) == expected.isoformat()
