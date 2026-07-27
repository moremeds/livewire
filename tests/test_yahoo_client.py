"""Unit tests for the read-only Yahoo chart client.

Fixture is REAL AMC (AMC Entertainment) split-adjusted closes across its real
2023-08-24 1:10 reverse split, fetched from Yahoo and frozen 2026-07-17. No network
— the HTTP is mocked with `responses`.
"""

from datetime import UTC, date, datetime, time

import pytest
import requests
import responses

from clients.yahoo_client import YahooClient, YahooError, YahooNotFound

# Real AMC Yahoo `close` (split-adjusted; == adjclose, AMC pays no dividend), frozen.
_AMC = [
    ("2023-08-17", 40.40),
    ("2023-08-18", 40.90),
    ("2023-08-21", 31.20),
    ("2023-08-22", 25.50),
    ("2023-08-23", 19.60),  # last pre-split session
    ("2023-08-24", 14.37),  # reverse-split ex-date
    ("2023-08-25", 12.43),
    ("2023-08-28", 11.07),
]


def _epoch(iso: str) -> int:
    # Yahoo stamps bars at market open (~13:30 UTC); .date() still yields the US date.
    return int(datetime.combine(date.fromisoformat(iso), time(13, 30), tzinfo=UTC).timestamp())


def _amc_payload(*, null_at: str | None = None):
    closes = [None if iso == null_at else px for iso, px in _AMC]
    return {
        "chart": {
            "result": [
                {
                    "timestamp": [_epoch(iso) for iso, _ in _AMC],
                    "indicators": {
                        "quote": [{"close": closes}],
                        "adjclose": [{"adjclose": closes}],
                    },
                    "events": {
                        "splits": {
                            "1692864000": {
                                "date": _epoch("2023-08-24"),
                                "numerator": 1,
                                "denominator": 10,
                                "splitRatio": "1:10",
                            }
                        }
                    },
                }
            ],
            "error": None,
        }
    }


@responses.activate
def test_get_daily_parses_split_adjusted_bars_and_the_split():
    responses.add(responses.GET, "https://query1.finance.yahoo.com/v8/finance/chart/AMC", json=_amc_payload())
    bars, splits = YahooClient().get_daily("amc", date(2023, 8, 17), date(2023, 8, 28))

    assert len(bars) == 8
    assert bars[0].trade_date == date(2023, 8, 17)
    assert bars[0].close == pytest.approx(40.40)
    assert bars[4].trade_date == date(2023, 8, 23)
    assert bars[4].close == pytest.approx(19.60)

    assert len(splits) == 1
    split = splits[0]
    assert split.ex_date == date(2023, 8, 24)
    assert (split.numerator, split.denominator) == (1.0, 10.0)
    # Reverse 1:10 → pre-split raw is one-tenth of the split-adjusted close.
    assert split.price_multiplier == pytest.approx(0.1)


@responses.activate
def test_reconstructed_raw_matches_the_known_values_across_the_split():
    # The whole point: raw = yahoo_close * product(multiplier for splits ex_date > t).
    # Real AMC raw was ~$1.96 pre-split (2023-08-23) and $14.37 on the ex-date.
    responses.add(responses.GET, "https://query1.finance.yahoo.com/v8/finance/chart/AMC", json=_amc_payload())
    bars, splits = YahooClient().get_daily("AMC", date(2023, 8, 17), date(2023, 8, 28))

    def raw(bar):
        factor = 1.0
        for split in splits:
            if split.ex_date > bar.trade_date:
                factor *= split.price_multiplier
        return bar.close * factor

    by_date = {bar.trade_date: raw(bar) for bar in bars}
    assert by_date[date(2023, 8, 23)] == pytest.approx(1.96)  # pre-split raw
    assert by_date[date(2023, 8, 24)] == pytest.approx(14.37)  # ex-date, no factor


@responses.activate
def test_null_closes_are_skipped_not_fabricated():
    responses.add(
        responses.GET,
        "https://query1.finance.yahoo.com/v8/finance/chart/AMC",
        json=_amc_payload(null_at="2023-08-21"),
    )
    bars, _ = YahooClient().get_daily("AMC", date(2023, 8, 17), date(2023, 8, 28))
    assert len(bars) == 7
    assert date(2023, 8, 21) not in {bar.trade_date for bar in bars}


@responses.activate
def test_not_found_symbol_raises_yahoo_not_found():
    responses.add(
        responses.GET,
        "https://query1.finance.yahoo.com/v8/finance/chart/NOTATICKER",
        json={"chart": {"result": None, "error": {"code": "Not Found", "description": "No data found"}}},
    )
    with pytest.raises(YahooNotFound):
        YahooClient().get_daily("NOTATICKER", date(2023, 1, 1), date(2023, 2, 1))


@responses.activate
def test_http_error_raises_yahoo_error():
    responses.add(responses.GET, "https://query1.finance.yahoo.com/v8/finance/chart/AMC", status=500)
    with pytest.raises(YahooError):
        YahooClient().get_daily("AMC", date(2023, 1, 1), date(2023, 2, 1))


@responses.activate
def test_http_404_raises_not_found():
    responses.add(responses.GET, "https://query1.finance.yahoo.com/v8/finance/chart/GONE", status=404)
    with pytest.raises(YahooNotFound):
        YahooClient().get_daily("GONE", date(2023, 1, 1), date(2023, 2, 1))


@responses.activate
def test_other_chart_error_raises_yahoo_error():
    responses.add(
        responses.GET,
        "https://query1.finance.yahoo.com/v8/finance/chart/AMC",
        json={"chart": {"result": None, "error": {"code": "Unauthorized", "description": "bad"}}},
    )
    with pytest.raises(YahooError):
        YahooClient().get_daily("AMC", date(2023, 1, 1), date(2023, 2, 1))


@responses.activate
def test_empty_result_list_raises_not_found():
    responses.add(
        responses.GET,
        "https://query1.finance.yahoo.com/v8/finance/chart/AMC",
        json={"chart": {"result": [], "error": None}},
    )
    with pytest.raises(YahooNotFound):
        YahooClient().get_daily("AMC", date(2023, 1, 1), date(2023, 2, 1))


@responses.activate
def test_request_exception_raises_yahoo_error():
    responses.add(
        responses.GET,
        "https://query1.finance.yahoo.com/v8/finance/chart/AMC",
        body=requests.exceptions.ConnectionError("boom"),
    )
    with pytest.raises(YahooError):
        YahooClient().get_daily("AMC", date(2023, 1, 1), date(2023, 2, 1))


# ---------------------------------------------------------------------------
# OHLCV + intraday (FX / DXY lane)
#
# Fixture is REAL DX-Y.NYB (ICE US Dollar Index) daily OHLCV for 2025-07-09..11,
# fetched from Yahoo and frozen 2026-07-27. DXY carries no volume — Yahoo really
# does send 0, which is why the lane must not treat a zero as a missing bar.
# ---------------------------------------------------------------------------

_DXY = [
    # (iso, open, high, low, close)
    ("2025-07-09", 97.55000305175781, 97.75, 97.45999908447266, 97.47000122070312),
    ("2025-07-10", 97.44000244140625, 97.91999816894531, 97.2699966430664, 97.6500015258789),
    ("2025-07-11", 97.56999969482422, 97.95999908447266, 97.55999755859375, 97.8499984741211),
]


def _dxy_payload(*, null_at: str | None = None, volume=None):
    def column(index):
        return [None if iso == null_at else row[index] for iso, *row in ((r[0], *r[1:]) for r in _DXY)]

    return {
        "chart": {
            "result": [
                {
                    "timestamp": [_epoch(row[0]) for row in _DXY],
                    "indicators": {
                        "quote": [
                            {
                                "open": column(0),
                                "high": column(1),
                                "low": column(2),
                                "close": column(3),
                                "volume": [0, 0, 0] if volume is None else volume,
                            }
                        ]
                    },
                }
            ],
            "error": None,
        }
    }


@responses.activate
def test_get_daily_ohlcv_parses_full_bars():
    responses.add(responses.GET, "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB", json=_dxy_payload())
    bars = YahooClient().get_daily_ohlcv("DX-Y.NYB")

    assert [bar.timestamp.date() for bar in bars] == [date(2025, 7, 9), date(2025, 7, 10), date(2025, 7, 11)]
    assert bars[0].open == pytest.approx(97.55000305175781)
    assert bars[0].high == pytest.approx(97.75)
    assert bars[0].low == pytest.approx(97.45999908447266)
    assert bars[0].close == pytest.approx(97.47000122070312)
    # An index has no volume. Zero must survive as a real value, not become a skip.
    assert [bar.volume for bar in bars] == [0, 0, 0]


@responses.activate
def test_get_daily_ohlcv_without_start_requests_full_history_not_range_max():
    """`range=max` silently downsamples (168 rows for DXY's 41 years); period1=0 does not."""
    responses.add(responses.GET, "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB", json=_dxy_payload())
    YahooClient().get_daily_ohlcv("DX-Y.NYB")

    query = responses.calls[0].request.params
    assert query["period1"] == "0"
    assert query["interval"] == "1d"
    assert "range" not in query


@responses.activate
def test_get_intraday_uses_range_never_period_bounds():
    """Yahoo rejects period1/period2 on intraday intervals with 'Unprocessable Entity'."""
    responses.add(responses.GET, "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB", json=_dxy_payload())
    YahooClient().get_intraday("DX-Y.NYB", "1h")

    query = responses.calls[0].request.params
    assert query["range"] == "730d"
    assert query["interval"] == "1h"
    assert "period1" not in query and "period2" not in query


@responses.activate
def test_get_intraday_rejects_unsupported_interval():
    with pytest.raises(ValueError, match="unsupported interval"):
        YahooClient().get_intraday("DX-Y.NYB", "15m")


@responses.activate
def test_ohlcv_bar_missing_a_price_is_skipped_not_fabricated():
    responses.add(
        responses.GET,
        "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB",
        json=_dxy_payload(null_at="2025-07-10"),
    )
    bars = YahooClient().get_daily_ohlcv("DX-Y.NYB")

    assert [bar.timestamp.date() for bar in bars] == [date(2025, 7, 9), date(2025, 7, 11)]


@responses.activate
def test_null_volume_becomes_zero_not_a_skip():
    """FX pairs quote no volume; Yahoo sends null. The bar's prices are still real."""
    responses.add(
        responses.GET,
        "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB",
        json=_dxy_payload(volume=[None, None, None]),
    )
    bars = YahooClient().get_daily_ohlcv("DX-Y.NYB")

    assert len(bars) == 3
    assert [bar.volume for bar in bars] == [0, 0, 0]
