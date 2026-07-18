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


# Real AMC split-adjusted OHLCV for the same window, frozen 2026-07-18 (o, h, l, v).
_AMC_OHLCV = {
    "2023-08-17": (36.70, 40.60, 36.40, 7147800),
    "2023-08-18": (40.60, 41.90, 38.60, 5165850),
    "2023-08-21": (40.00, 40.30, 30.50, 11371260),
    "2023-08-22": (31.00, 31.10, 24.60, 10872410),
    "2023-08-23": (19.50, 22.00, 19.40, 19985440),
    "2023-08-24": (16.31, 16.60, 13.33, 26408000),
    "2023-08-25": (13.44, 14.45, 12.40, 24061600),
    "2023-08-28": (11.99, 12.35, 11.05, 24658900),
}


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
                        "quote": [
                            {
                                "close": closes,
                                "open": [_AMC_OHLCV[iso][0] for iso, _ in _AMC],
                                "high": [_AMC_OHLCV[iso][1] for iso, _ in _AMC],
                                "low": [_AMC_OHLCV[iso][2] for iso, _ in _AMC],
                                "volume": [_AMC_OHLCV[iso][3] for iso, _ in _AMC],
                            }
                        ],
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
def test_get_daily_ohlcv_parses_full_bars_and_reconstructs_raw():
    from clients.yahoo_client import YahooOHLCVBar

    responses.add(responses.GET, "https://query1.finance.yahoo.com/v8/finance/chart/AMC", json=_amc_payload())
    bars, splits = YahooClient().get_daily_ohlcv("amc", date(2023, 8, 17), date(2023, 8, 28))

    assert len(bars) == 8
    assert isinstance(bars[0], YahooOHLCVBar)
    ex = next(b for b in bars if b.trade_date == date(2023, 8, 24))
    assert (ex.open, ex.high, ex.low, ex.close) == pytest.approx((16.31, 16.60, 13.33, 14.37))
    assert ex.volume == 26408000
    # raw across the reverse split: pre-split 2023-08-23 raw = split-adjusted x 0.1.
    pre = next(b for b in bars if b.trade_date == date(2023, 8, 23))
    mult = 1.0
    for split in splits:
        if split.ex_date > pre.trade_date:
            mult *= split.price_multiplier
    assert pre.close * mult == pytest.approx(1.96, abs=0.01)


@responses.activate
def test_get_daily_ohlcv_skips_bar_missing_any_field():
    responses.add(
        responses.GET,
        "https://query1.finance.yahoo.com/v8/finance/chart/AMC",
        json={
            "chart": {
                "result": [
                    {
                        "timestamp": [_epoch("2023-08-17"), _epoch("2023-08-18")],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [36.70, 40.60],
                                    "high": [40.60, 41.90],
                                    "low": [36.40, 38.60],
                                    "close": [40.40, None],  # second bar has no close
                                    "volume": [7147800, 5165850],
                                }
                            ]
                        },
                    }
                ],
                "error": None,
            }
        },
    )
    bars, _ = YahooClient().get_daily_ohlcv("AMC", date(2023, 8, 17), date(2023, 8, 18))
    assert [b.trade_date for b in bars] == [date(2023, 8, 17)]


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
