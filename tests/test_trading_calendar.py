from datetime import date

from clients.trading_calendar import is_trading_day, session_close_time, trading_dates_in_range


def test_trading_dates_in_range_is_inclusive_and_rejects_reverse_range():
    assert trading_dates_in_range(date(2026, 6, 1), date(2026, 6, 2)) == [date(2026, 6, 1), date(2026, 6, 2)]
    assert trading_dates_in_range(date(2026, 6, 2), date(2026, 6, 1)) == []


def test_historical_emergency_closures_and_early_close_policy():
    for closed in (
        date(2001, 9, 11),
        date(2001, 9, 12),
        date(2001, 9, 13),
        date(2001, 9, 14),
        date(2012, 10, 29),
        date(2012, 10, 30),
        date(2018, 12, 5),
    ):
        assert is_trading_day(closed) is False
    assert session_close_time(date(2025, 11, 28)).hour == 13
    assert session_close_time(date(2026, 8, 31)).hour == 16


def test_historical_holiday_regimes_do_not_apply_modern_rules_backward():
    assert is_trading_day(date(1990, 1, 15)) is True  # MLK not observed by XNYS yet
    assert is_trading_day(date(1998, 1, 19)) is False
    assert is_trading_day(date(2021, 12, 31)) is True  # Saturday New Year was not moved to Friday
    assert is_trading_day(date(1980, 11, 4)) is False  # historical presidential election closure
    assert is_trading_day(date(1968, 6, 12)) is False  # paperwork-crisis Wednesday
    assert is_trading_day(date(1977, 7, 14)) is False  # New York City blackout
