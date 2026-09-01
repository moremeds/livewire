"""Tell an instrument that left the tape apart from one that did not trade.

`coverage` exempts a symbol absent from the day's raw traded set -- no-trade is
not missing. That rule is load-bearing: without it the interior gap scan flags
96.6% of the universe. It is also what hid three S&P 500 members that stopped
printing and never returned (docs/audits/2026-09-01-terminus-vs-no-trade.md).

The separation is a suffix test over the traded sets coverage already opens, not
a threshold on a bar file. See section 4.4 of
docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from clients.corporate_action_store import CorporateActionStore

RAW_MINUTE_AGGS = "raw/massive/us_stocks_sip/minute_aggs_v1"

# Spec 4.4 says the terminus test has "no threshold to tune". That was written
# about the SHAPE of the test -- a suffix, not a severity cutoff -- and it stays
# true: this is not a "how many missing bars is too many" dial like the 5m scan's,
# which is the circular question 4.4 rejects. But it IS a calibrated constant,
# and calling it thresholdless would be dishonest. Spec 4.4 is amended to say so.
# ponytail: one trading week. A listed instrument that does not print for five
# consecutive sessions is not having a quiet day. Measured 2026-09-01, the four
# real termini had runs of 21/19/11/10 sessions and the other 511 members of the
# sp500+ndx100 universe had none, so anything from 2 to 10 separates them here.
# Raise it if a calibration run produces a false positive; never lower it below
# 2, where a single no-trade day becomes a delisting.
MIN_TERMINUS_SESSIONS = 5

# How far either side of the terminus an explaining event may sit. A reorganisation
# does not land on the exact session the tape goes quiet: the ex-date and the last
# print differ by settlement and by when the new line starts trading.
TERMINUS_CA_WINDOW_DAYS = 10


def terminus_of(
    traded_by_session: dict[date, set[str]],
    symbol: str,
    min_sessions: int = MIN_TERMINUS_SESSIONS,
) -> date | None:
    """First session of *symbol*'s trailing run of absences, or None.

    None means "still on the tape, or not absent for long enough to tell". An
    empty tape returns None: failing to measure must never render as delisted.
    """
    sessions = sorted(traded_by_session)
    if not sessions:
        return None
    absent_from = len(sessions)
    while absent_from > 0 and symbol not in traded_by_session[sessions[absent_from - 1]]:
        absent_from -= 1
    if len(sessions) - absent_from < min_sessions:
        return None
    return sessions[absent_from]


def traded_by_session(raw_root: Path, sessions: Sequence[date]) -> dict[date, set[str]]:
    """Per-session traded sets read from the raw flat-file partitions.

    A session with no partition is omitted rather than recorded as an empty set:
    an absent file is "we did not fetch that day", and an empty set would read as
    "nothing traded" -- which would terminate the entire universe at once.
    """
    out: dict[date, set[str]] = {}
    for session in sessions:
        path = raw_root / f"date={session.isoformat()}" / "_symbols.parquet"
        if not path.exists():
            continue
        out[session] = set(pq.read_table(path, columns=["ticker"]).column("ticker").to_pylist())
    return out


def terminus_is_unexplained(
    store: CorporateActionStore,
    symbol: str,
    terminus: date,
    window_days: int = TERMINUS_CA_WINDOW_DAYS,
) -> bool:
    """Spec section 3's clause: absence with no corporate action explaining it.

    FAIL-CLOSED in both directions, which is the whole point:
    - the store was not asked about this symbol on or after the terminus -> we
      never looked at the window the explanation would live in, so we cannot
      assert its absence. Withhold.
    - an ACTIVE SPLIT sits inside the window -> the store does offer an
      explanation. Withhold.

    A cash dividend never removes a ticker from the tape and is ignored; those
    two action_type values are all this store carries
    (corporate_action_store.py:421), so "no split in the window" is the strongest
    exculpatory statement it can make. That is a real limit on what G14 asserts,
    not a reason to skip the check: a plain delisting leaves no event here at all,
    and that is exactly the case G14 exists to report.
    """
    fetches = store.fetch_history(symbol)
    if not fetches:
        return False
    if max(f.fetched_at for f in fetches).date() < terminus:
        return False
    lo, hi = terminus - timedelta(days=window_days), terminus + timedelta(days=window_days)
    return not any(
        action.action_type == "split" and lo <= action.ex_date <= hi for action in store.latest_active(symbol)
    )


def raw_tape_covers(raw_root: Path, through: date) -> bool:
    """Does the newest raw partition reach *through*?

    The other half of criterion 8, and the one whose failure is loudest: a
    flat-file lane that stopped a week ago makes EVERY symbol a trailing run of
    absences at once. traded_by_session already omits a session with no partition
    rather than recording an empty set, so a total outage yields an empty tape and
    terminus_of returns None -- but a PARTIAL outage does not, and that is the
    case this guards. ponytail: directory names, no footer reads.
    """
    dates = [
        date.fromisoformat(child.name.removeprefix("date=")) for child in raw_root.glob("date=*") if child.is_dir()
    ]
    return bool(dates) and max(dates) >= through
