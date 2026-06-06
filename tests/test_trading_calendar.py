from datetime import date

from clients.trading_calendar import trading_dates_in_range


def test_trading_dates_in_range_is_inclusive_and_rejects_reverse_range():
    assert trading_dates_in_range(date(2026, 6, 1), date(2026, 6, 2)) == [date(2026, 6, 1), date(2026, 6, 2)]
    assert trading_dates_in_range(date(2026, 6, 2), date(2026, 6, 1)) == []
