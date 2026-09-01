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

import logging
from collections.abc import Sequence
from dataclasses import dataclass
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

log = logging.getLogger(__name__)


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
    # Never present in the window is NOT a terminus. Leaving the tape is a
    # transition, and a symbol that never printed here has not been observed
    # making one -- "we have no data for it" is G3's fact, not G14's.
    # Measured on the real tape 2026-09-01: BK is absent from all 30 sessions
    # while BNY prints in all 30. BK did not delist, it was renamed, and the
    # action store carries only splits and cash dividends -- a ticker change is
    # not in its vocabulary, so terminus_is_unexplained can only ever return
    # True for one. All three gates then pass on silence rather than evidence,
    # and a live S&P 500 member renders as a delisting. The stale preset row is
    # a separate fix; this is the one that stops G14 asserting what it cannot know.
    if absent_from == 0:
        return None
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
        try:
            out[session] = set(pq.read_table(path, columns=["ticker"]).column("ticker").to_pylist())
        except Exception:  # noqa: BLE001 - a corrupt partition must not kill the detector
            # Same epistemic state as an absent file: we cannot read that day's
            # traded set. The repo has been here before -- one truncated
            # 1m.parquet aborted every nightly publish for a month -- and the
            # footer pass already refuses to die of its own inputs.
            # ponytail: this leaves raw_tape_covers (which only stats the file)
            # able to pass while this session is absent from the tape. That can
            # only ADD a terminus candidate, and the corporate-action gate still
            # has to clear it before any G14 is emitted.
            log.warning("unreadable raw traded set for %s at %s -- skipping the session", session, path)
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
    # <= , not <. A reconciliation stamped the same day as the terminus may have
    # run before that session closed, so it cannot speak for it. The boundary is
    # unreachable in production (a terminus is at least MIN_TERMINUS_SESSIONS
    # sessions old and the reconcile is nightly), which is exactly why it would
    # never have been caught by observation.
    if max(f.fetched_at for f in fetches).date() <= terminus:
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
    # The partition FILE, not the directory. traded_by_session omits a session
    # whose _symbols.parquet is absent, so a directory that exists without its
    # parquet -- an interrupted fetch -- passed this gate while contributing no
    # traded set, and every symbol that printed only in that session read as a
    # trailing absence. Both functions now key on the same artifact.
    dates = [
        date.fromisoformat(child.parent.name.removeprefix("date="))
        for child in raw_root.glob("date=*/_symbols.parquet")
    ]
    return bool(dates) and max(dates) >= through


@dataclass(frozen=True)
class TerminusVerdict:
    """The suffix test's answer and whether the gates could confirm it.

    Three states, and collapsing the last two is the bug this type exists to
    prevent:
    - `when is None and candidate is None` -- no qualifying absence run. Ordinary
      symbol; the no-trade exemption applies as it always has.
    - `when` set -- all three gates passed. It left the tape.
    - `withheld` -- a qualifying absence run the store could not explain OR could
      not be asked about. "We could not check" is not "it did not trade today":
      taking the no-trade exemption here restores the mechanism that hid
      EA/AVB/EQR, and routing it to an unattended Tier A repair queues a fetch of
      an instrument that is not printing.
    """

    when: date | None
    candidate: date | None

    @property
    def withheld(self) -> bool:
        return self.when is None and self.candidate is not None


def confirmed_terminus(
    tape: dict[date, set[str]],
    symbol: str,
    store: CorporateActionStore,
    tape_ok: bool,
) -> TerminusVerdict:
    """All three gates of spec criterion 8, as one decision.

    This exists because the decision has two callers -- the coverage denominator
    and the windowed classifier -- and they diverged: one applied the suffix test
    alone and dropped the symbol from its denominator on that basis, which is
    "we could not check" rendering as "delisted", the one thing this module says
    must never happen. Three separate functions in a caller-defined order is an
    invariant nothing enforces; one function is enforced by there being nothing
    else to call.
    """
    if not tape_ok:
        return TerminusVerdict(None, None)
    candidate = terminus_of(tape, symbol)
    if candidate is None:
        return TerminusVerdict(None, None)
    if not terminus_is_unexplained(store, symbol, candidate):
        return TerminusVerdict(None, candidate)
    return TerminusVerdict(candidate, candidate)
