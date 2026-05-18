"""NYSE trading calendar helpers shared by ingestion and quality checks."""
from __future__ import annotations

from datetime import date, timedelta


def get_nyse_holidays(year: int) -> set[date]:
    """Compute NYSE observed holidays for *year*.

    Covers: New Year's, MLK Day, Presidents Day, Good Friday,
    Memorial Day, Juneteenth, Independence Day, Labor Day,
    Thanksgiving, Christmas. Applies weekend-observed rules.
    """
    holidays: set[date] = set()

    def _observed(d: date) -> date:
        """Shift Saturday→Friday, Sunday→Monday for observed holidays."""
        if d.weekday() == 5:  # Saturday
            return d - timedelta(days=1)
        if d.weekday() == 6:  # Sunday
            return d + timedelta(days=1)
        return d

    # New Year's Day
    holidays.add(_observed(date(year, 1, 1)))

    # MLK Day — 3rd Monday of January
    jan1 = date(year, 1, 1)
    first_monday = jan1 + timedelta(days=(7 - jan1.weekday()) % 7)
    mlk = first_monday + timedelta(weeks=2)
    holidays.add(mlk)

    # Presidents Day — 3rd Monday of February
    feb1 = date(year, 2, 1)
    first_monday_feb = feb1 + timedelta(days=(7 - feb1.weekday()) % 7)
    presidents = first_monday_feb + timedelta(weeks=2)
    holidays.add(presidents)

    # Good Friday — 2 days before Easter Sunday
    holidays.add(_easter(year) - timedelta(days=2))

    # Memorial Day — last Monday of May
    may31 = date(year, 5, 31)
    memorial = may31 - timedelta(days=(may31.weekday()) % 7)
    holidays.add(memorial)

    # Juneteenth — observed since 2022
    if year >= 2021:
        holidays.add(_observed(date(year, 6, 19)))

    # Independence Day
    holidays.add(_observed(date(year, 7, 4)))

    # Labor Day — 1st Monday of September
    sep1 = date(year, 9, 1)
    labor = sep1 + timedelta(days=(7 - sep1.weekday()) % 7)
    holidays.add(labor)

    # Thanksgiving — 4th Thursday of November
    nov1 = date(year, 11, 1)
    first_thu = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    thanksgiving = first_thu + timedelta(weeks=3)
    holidays.add(thanksgiving)

    # Christmas
    holidays.add(_observed(date(year, 12, 25)))

    return holidays


def _easter(year: int) -> date:
    """Compute Easter Sunday using the Anonymous Gregorian algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7  # noqa: E741
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def is_trading_day(d: date) -> bool:
    """Return True if *d* is a NYSE trading day (not weekend, not holiday)."""
    if d.weekday() >= 5:
        return False
    return d not in get_nyse_holidays(d.year)


def previous_trading_day(d: date) -> date:
    """Walk backwards from *d* to find the most recent trading day."""
    d = d - timedelta(days=1)
    while not is_trading_day(d):
        d = d - timedelta(days=1)
    return d


def trading_days_between(start: date, end: date) -> int:
    """Count trading days in the half-open range (start, end]."""
    count = 0
    d = start + timedelta(days=1)
    while d <= end:
        if is_trading_day(d):
            count += 1
        d += timedelta(days=1)
    return count
