from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveDividend, MassivePageEvidence, MassiveSplit
from clients.terminus import (
    confirmed_terminus,
    raw_tape_covers,
    terminus_is_unexplained,
    terminus_of,
    traded_by_session,
)

# Five real August 2026 NYSE sessions. Ticker sets are membership lists, not
# prices -- no market values are asserted here.
S = [date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 31)]


def _tape(**presence: str) -> dict[date, set[str]]:
    """presence maps a symbol to a 5-char mask of 'x' (traded) / '.' (absent)."""
    return {d: {sym for sym, mask in presence.items() if mask[i] == "x"} for i, d in enumerate(S)}


def test_a_symbol_trading_on_the_latest_session_has_no_terminus():
    assert terminus_of(_tape(AAPL="xxxxx"), "AAPL", min_sessions=1) is None


def test_a_single_absent_day_between_two_present_days_is_not_a_terminus():
    # The no-trade case the exemption exists for: an illiquid name that did not
    # print on one session. It printed again afterwards.
    assert terminus_of(_tape(SLND="xx.xx"), "SLND", min_sessions=1) is None


def test_a_trailing_run_of_absences_is_a_terminus_at_its_first_session():
    # EQR stopped printing after 2026-08-17 and never returned; the shape here is
    # that run, compressed into the five-session fixture.
    assert terminus_of(_tape(EQR="xxx.."), "EQR", min_sessions=2) == S[3]


def test_a_symbol_absent_from_every_session_terminates_at_the_window_start():
    # BK: an sp500 member with no 1d.parquet and no row on the tape at all.
    assert terminus_of(_tape(AAPL="xxxxx"), "BK", min_sessions=1) == S[0]


def test_a_trailing_run_shorter_than_the_minimum_is_not_yet_a_terminus():
    # One absent session at the end is indistinguishable from a no-trade day.
    # Calling it a terminus is how a detector starts paging on illiquid names.
    assert terminus_of(_tape(EQR="xxxx."), "EQR", min_sessions=2) is None


def test_an_empty_tape_yields_no_terminus():
    # No raw partitions on disk means the test cannot answer, and "cannot answer"
    # must never render as "delisted".
    assert terminus_of({}, "AAPL") is None


def _write_tape(root: Path, session: date, tickers: list[str]) -> None:
    d = root / f"date={session.isoformat()}"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"ticker": tickers}), d / "_symbols.parquet")


def test_traded_by_session_omits_a_session_with_no_partition(tmp_path):
    _write_tape(tmp_path, S[0], ["AAPL", "BK"])
    _write_tape(tmp_path, S[2], ["AAPL"])

    got = traded_by_session(tmp_path, S)

    # S[1], S[3] and S[4] have no partition and must be absent from the result,
    # not present as empty sets -- an empty set would terminate every symbol.
    assert set(got) == {S[0], S[2]}
    assert got[S[0]] == {"AAPL", "BK"}


def _hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _dividend(symbol: str, ex_date: date) -> MassiveDividend:
    """An ordinary quarterly cash dividend. Carries an event date, never a price."""
    return MassiveDividend(
        provider_event_id=f"div-{symbol}-{ex_date.isoformat()}",
        ticker=symbol,
        ex_dividend_date=ex_date,
        cash_amount=Decimal("0.675"),
        currency="USD",
        declaration_date=None,
        record_date=None,
        pay_date=None,
        payload_hash=_hash("div", symbol, ex_date.isoformat()),
    )


def _split(symbol: str, ex_date: date, split_from: int, split_to: int) -> MassiveSplit:
    """A real reverse-split ratio; CLAUDE.md records 1:300 (LIME) and 1:3000 (TTSH)."""
    return MassiveSplit(
        provider_event_id=f"split-{symbol}-{ex_date.isoformat()}",
        ticker=symbol,
        execution_date=ex_date,
        split_from=Decimal(split_from),
        split_to=Decimal(split_to),
        payload_hash=_hash("split", symbol, ex_date.isoformat()),
    )


def _store_with(tmp_path, symbol, actions, fetched_at):
    """A store holding real events for a real ticker, at a recorded as-of date.

    Both halves are required: reconcile() writes the events, record_fetch()
    writes the receipt terminus_is_unexplained reads for freshness. Writing only
    the first leaves fetch_history empty, which fails the gate closed for every
    symbol and would make these assertions vacuous.
    """
    store = CorporateActionStore(tmp_path)
    store.reconcile(symbol, actions, fetched_at=fetched_at)
    store.record_fetch(
        symbol,
        [MassivePageEvidence("splits", "artifact://x", "a" * 64, fetched_at, "sha256:" + "1" * 64)],
        fetched_at,
        full_reconcile=True,
    )
    return store


def test_an_ordinary_dividend_does_not_explain_a_terminus(tmp_path):
    # EQR, measured 2026-09-01: last tape print 2026-08-17, and the newest event
    # in the store is a 2026-06-29 cash dividend. A dividend never takes a ticker
    # off the tape, so nothing here explains the absence -> this IS a G14.
    store = _store_with(
        tmp_path,
        "EQR",
        [_dividend("EQR", date(2026, 6, 29))],
        fetched_at=datetime(2026, 8, 31, 6, 0, tzinfo=UTC),
    )
    assert terminus_is_unexplained(store, "EQR", date(2026, 8, 18)) is True


def test_a_split_at_the_terminus_explains_it(tmp_path):
    # A reverse split IS a reorganisation that can take a ticker off the tape --
    # CLAUDE.md records real ones at 1:300 (LIME) and 1:3000 (TTSH). The store
    # offers an explanation, so the symbol is withheld from G14.
    store = _store_with(
        tmp_path,
        "LIME",
        [_split("LIME", date(2026, 8, 19), 300, 1)],
        fetched_at=datetime(2026, 8, 31, 6, 0, tzinfo=UTC),
    )
    assert terminus_is_unexplained(store, "LIME", date(2026, 8, 18)) is False


def test_a_stale_store_withholds_g14_rather_than_emitting_one(tmp_path):
    # Criterion 8, fail-closed. Lane 1 wedged on 2026-07-28 and is the unbudgeted
    # one (issue #94). If the store was last asked BEFORE the terminus, the engine
    # never looked at the window the explanation would live in and cannot assert
    # its absence. "We did not measure" must not render as "delisted".
    store = _store_with(
        tmp_path,
        "EQR",
        [_dividend("EQR", date(2026, 6, 29))],
        fetched_at=datetime(2026, 7, 28, 6, 0, tzinfo=UTC),
    )
    assert terminus_is_unexplained(store, "EQR", date(2026, 8, 18)) is False


def test_a_symbol_never_asked_about_withholds_g14(tmp_path):
    # No fetch history at all is the same failure as a stale one, and it is the
    # likelier one: reconcile() skips a symbol that errored.
    assert terminus_is_unexplained(CorporateActionStore(tmp_path), "EQR", date(2026, 8, 18)) is False


def test_a_tape_that_stops_short_of_the_window_blocks_every_g14(tmp_path):
    # The loud failure: one stalled flat-file lane would otherwise report the
    # whole universe delisted on the same morning.
    raw = tmp_path / "raw"
    _write_tape(raw, date(2026, 8, 20), ["AAPL"])
    assert raw_tape_covers(raw, date(2026, 8, 28)) is False
    _write_tape(raw, date(2026, 8, 28), ["AAPL"])
    assert raw_tape_covers(raw, date(2026, 8, 28)) is True


def test_a_partition_directory_without_its_parquet_does_not_count_as_coverage(tmp_path):
    """An interrupted fetch leaves the directory and no _symbols.parquet.

    traded_by_session skips such a session, so counting it here made tape_ok
    true while the tape silently omitted the session -- and every symbol that
    printed only in it read as a trailing absence. The two functions must key on
    the same artifact.
    """
    raw = tmp_path / "raw"
    _write_tape(raw, date(2026, 8, 20), ["AAPL"])
    (raw / "date=2026-08-28").mkdir()
    assert raw_tape_covers(raw, date(2026, 8, 28)) is False


def test_an_absent_raw_tree_blocks_every_g14(tmp_path):
    assert raw_tape_covers(tmp_path / "nothing", date(2026, 8, 28)) is False


def test_confirmed_terminus_needs_all_three_gates(tmp_path):
    """Pin the COMPOSITION, not just the parts.

    The three gates stayed importable individually and a caller assembled two of
    them in the wrong order -- coverage dropped symbols from its denominator on
    the suffix test alone. The parts each have their own test; without this one
    nothing tests the only thing production calls.
    """
    # confirmed_terminus takes MIN_TERMINUS_SESSIONS as shipped (5), so the
    # fixture is ten real sessions: EQR prints for five and then stops.
    sessions = [date(2026, 8, 17 + i) for i in range(10)]
    tape = {d: ({"EQR"} if i < 5 else set()) for i, d in enumerate(sessions)}
    terminus = sessions[5]
    fresh = datetime(2026, 8, 31, 6, 0, tzinfo=UTC)
    div = [_dividend("EQR", date(2026, 6, 29))]

    ok = _store_with(tmp_path / "ok", "EQR", div, fetched_at=fresh)
    assert confirmed_terminus(tape, "EQR", ok, tape_ok=True).when == terminus

    # Gate 1: the raw tape does not reach the window. No candidate is even formed.
    v = confirmed_terminus(tape, "EQR", ok, tape_ok=False)
    assert (v.when, v.candidate, v.withheld) == (None, None, False)

    # Gate 2: the store was never asked about this symbol.
    v = confirmed_terminus(tape, "EQR", CorporateActionStore(tmp_path / "empty"), tape_ok=True)
    assert (v.when, v.candidate, v.withheld) == (None, terminus, True)

    # Gate 3: an active split explains the departure.
    split = _store_with(tmp_path / "split", "EQR", [_split("EQR", terminus, 300, 1)], fetched_at=fresh)
    v = confirmed_terminus(tape, "EQR", split, tape_ok=True)
    assert (v.when, v.candidate, v.withheld) == (None, terminus, True)

    # No candidate at all: an ordinary symbol still trading is not "withheld".
    v = confirmed_terminus({d: {"AAPL"} for d in sessions}, "AAPL", ok, tape_ok=True)
    assert (v.when, v.candidate, v.withheld) == (None, None, False)


def test_a_corrupt_traded_set_skips_its_session_instead_of_killing_the_run(tmp_path, caplog):
    """One truncated partition must not abort the whole coverage job.

    CLAUDE.md records the precedent: a single truncated 1m.parquet aborted every
    nightly publish for a month. traded_by_session is read outside any guard in
    compute_coverage, so an unreadable file here took the detector down with it.
    """
    _write_tape(tmp_path, S[0], ["AAPL", "BK"])
    (tmp_path / f"date={S[1].isoformat()}").mkdir()
    (tmp_path / f"date={S[1].isoformat()}" / "_symbols.parquet").write_bytes(b"PAR1truncated")

    got = traded_by_session(tmp_path, S)

    assert set(got) == {S[0]}
    assert any("unreadable raw traded set" in r.message for r in caplog.records)
