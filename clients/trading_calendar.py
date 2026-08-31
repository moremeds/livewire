"""NYSE trading calendar helpers shared by ingestion and quality checks."""

from __future__ import annotations

from datetime import date, time, timedelta

XNYS_SESSION_POLICY = "XNYS-close-and-early-close-v2"

# One-off NYSE closures that the rule-based calendar can't derive.
# Add new entries here when the Exchange declares an unscheduled closure
# (presidential funerals, national days of mourning, weather, etc.).
SPECIAL_CLOSURES: frozenset[date] = frozenset(
    {
        date(1954, 12, 24),  # Christmas Eve special closure
        date(1956, 12, 24),
        date(1958, 12, 26),  # Day after Christmas
        date(1961, 5, 29),  # Day before Decoration Day
        date(1963, 11, 25),  # President Kennedy funeral
        date(1965, 12, 24),
        date(1968, 4, 9),  # Martin Luther King Jr. day of mourning
        date(1968, 7, 5),  # Day after Independence Day
        date(1969, 2, 10),  # Heavy snow
        date(1969, 3, 31),  # President Eisenhower funeral
        date(1969, 7, 21),  # National Day of Participation
        date(1972, 12, 28),  # President Truman funeral
        date(1973, 1, 25),  # President Johnson funeral
        date(1977, 7, 14),  # New York City blackout
        date(1985, 9, 27),  # Hurricane Gloria
        date(1994, 4, 27),  # President Nixon funeral
        date(2001, 9, 11),  # September 11 market closure
        date(2001, 9, 12),
        date(2001, 9, 13),
        date(2001, 9, 14),
        date(2004, 6, 11),  # President Reagan funeral
        date(2007, 1, 2),  # President Ford funeral
        date(2012, 10, 29),  # Hurricane Sandy
        date(2012, 10, 30),
        date(2018, 12, 5),  # President George H.W. Bush funeral
        date(2025, 1, 9),  # National Day of Mourning for President Carter
    }
)


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

    # New Year's Day. XNYS observes Sunday on Monday but does not move a
    # Saturday New Year to the preceding Friday.
    new_year = date(year, 1, 1)
    holidays.add(new_year + timedelta(days=1) if new_year.weekday() == 6 else new_year)

    # MLK Day — XNYS began observing it in 1998.
    if year >= 1998:
        jan1 = date(year, 1, 1)
        first_monday = jan1 + timedelta(days=(7 - jan1.weekday()) % 7)
        holidays.add(first_monday + timedelta(weeks=2))

    # Washington's Birthday changed from February 22 to the third Monday in 1971.
    if year >= 1971:
        feb1 = date(year, 2, 1)
        first_monday_feb = feb1 + timedelta(days=(7 - feb1.weekday()) % 7)
        holidays.add(first_monday_feb + timedelta(weeks=2))
    else:
        holidays.add(_observed(date(year, 2, 22)))

    # Good Friday — 2 days before Easter Sunday
    holidays.add(_easter(year) - timedelta(days=2))

    # Memorial Day changed from May 30 to the last Monday in May in 1971.
    if year >= 1971:
        may31 = date(year, 5, 31)
        holidays.add(may31 - timedelta(days=may31.weekday()))
    else:
        holidays.add(_observed(date(year, 5, 30)))

    # Juneteenth — a FEDERAL holiday from 2021, but a MARKET holiday only from 2022.
    # The law was signed 2021-06-17, too late for the exchanges to close for the
    # 2021-06-18 observance: NYSE and Nasdaq traded that Friday, and it was a
    # quadruple-witching session (SPY 118.7M shares, the week's highest volume).
    # The NYSE first closed for Juneteenth on 2022-06-20.
    if year >= 2022:
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

    # XNYS closed for presidential elections through 1968 and in leap-year
    # elections through 1980.
    if year <= 1968 or (year <= 1980 and year % 4 == 0):
        nov1 = date(year, 11, 1)
        holidays.add(nov1 + timedelta(days=(1 - nov1.weekday()) % 7))

    # From June 12 through year-end 1968, the paperwork crisis closed XNYS on
    # Wednesdays to clear the settlement backlog.
    if year == 1968:
        current = date(1968, 6, 12)
        while current.year == 1968:
            if current.weekday() == 2:
                holidays.add(current)
            current += timedelta(days=1)

    # Special one-off NYSE closures for this year.
    holidays.update(d for d in SPECIAL_CLOSURES if d.year == year)

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


def session_close_time(day: date) -> time:
    """Return the scheduled XNYS close under the versioned daily policy."""
    nov1 = date(day.year, 11, 1)
    thanksgiving = nov1 + timedelta(days=(3 - nov1.weekday()) % 7, weeks=3)
    if day == thanksgiving + timedelta(days=1):
        return time(13)
    if (day.month, day.day) == (12, 24) and is_trading_day(day):
        return time(13)
    july_4 = date(day.year, 7, 4)
    if (day.month, day.day) == (7, 3) and july_4.weekday() in (1, 2, 3, 4) and is_trading_day(day):
        return time(13)
    return time(16)


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


def trading_dates_in_range(start: date, end: date) -> list[date]:
    """Return NYSE trading dates in the inclusive range."""
    if end < start:
        return []
    return [
        start + timedelta(days=i) for i in range((end - start).days + 1) if is_trading_day(start + timedelta(days=i))
    ]
