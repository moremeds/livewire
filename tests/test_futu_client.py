from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from clients import futu_client as fc


class Frame:
    """The two DataFrame calls futu_client makes, over a list of dicts."""

    def __init__(self, rows):
        self.rows = rows

    def to_dict(self, orient):
        assert orient == "records"
        return self.rows

    def __getitem__(self, column):
        return [row[column] for row in self.rows]


class Clock:
    def __init__(self):
        self.t = 0.0
        self.slept: list[float] = []

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.t += seconds


def _session(ctx, clock=None):
    clock = clock or Clock()
    session = fc.QuoteSession(context_factory=lambda **_: ctx, clock=clock, sleep=clock.sleep)
    session._ctx = ctx
    return session, clock


def test_the_limiter_waits_out_the_rolling_window():
    clock = Clock()
    limiter = fc.RateLimiter(2, clock=clock, sleep=clock.sleep)
    for _ in range(3):
        limiter.wait()
    assert len(clock.slept) == 1 and clock.slept[0] == pytest.approx(30.05)


def test_a_throttled_call_is_retried_once_and_counted():
    answers = iter([(-1, "high frequency of requests"), (0, Frame([{"code": "US.A"}]))])

    class Ctx:
        def get_market_snapshot(self, codes):
            return next(answers)

    session, clock = _session(Ctx())
    rows, refused = session.snapshot(["US.A"])
    assert [r["code"] for r in rows] == ["US.A"] and refused == []
    assert rows[0]["received_at"].tzinfo is UTC
    assert session.throttled == 1 and 30.0 in clock.slept


def test_a_second_throttle_is_a_request_error():
    class Ctx:
        def get_option_expiration_date(self, code):
            return -1, "high frequency of requests"

    session, _ = _session(Ctx())
    with pytest.raises(fc.FutuRequestError):
        session.expiries("US..SPX")


def test_one_unknown_code_is_bisected_out_and_the_rest_are_kept(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_FUTU_CODES_PER_CALL_SNAPSHOT", "4")

    class Ctx:
        calls = 0

        def get_global_state(self):
            return 0, {"qot_logined": True}

        def get_market_snapshot(self, codes):
            self.calls += 1
            if "US.BAD" in codes:
                return -1, "unknown stock US.BAD"
            return 0, Frame([{"code": c} for c in codes])

    ctx = Ctx()
    session, _ = _session(ctx)
    rows, refused = session.snapshot(["US.A", "US.B", "US.BAD", "US.C", "US.D"])
    assert [r["code"] for r in rows] == ["US.A", "US.B", "US.C", "US.D"]
    assert refused == ["US.BAD"]


def test_expiries_and_chain_return_plain_rows():
    class Ctx:
        def get_option_expiration_date(self, code):
            return 0, Frame([{"strike_time": "2026-10-23"}, {"strike_time": "2026-10-16"}])

        def get_option_chain(self, code, start, end):
            assert (start, end) == ("2026-10-16", "2026-11-14")
            return 0, Frame([{"code": "US.SPXW261016C7000000", "lot_size": 100, "x": "N/A"}])

    session, _ = _session(Ctx())
    assert session.expiries("US..SPX") == [date(2026, 10, 16), date(2026, 10, 23)]
    assert session.chain("US..SPX", date(2026, 10, 16), date(2026, 11, 14)) == [
        {"code": "US.SPXW261016C7000000", "lot_size": 100, "x": None}
    ]


def test_cells_futu_uses_for_missing_become_none():
    class Scalar:
        def item(self):
            return 7

    assert [fc._clean(v) for v in ("N/A", " ", float("nan"), [1], Scalar(), 1.5, "LV1")] == [
        None,
        None,
        None,
        None,
        7,
        1.5,
        "LV1",
    ]


def test_a_us_quote_time_is_new_york_wall_clock():
    assert fc.us_time("2026-10-16 15:45:00") == datetime(2026, 10, 16, 19, 45, tzinfo=UTC)
    assert fc.us_time("2026-12-16 15:45:00") == datetime(2026, 12, 16, 20, 45, tzinfo=UTC)
    assert fc.us_time("") is None


def test_a_dropped_session_is_unavailable_not_bisected_into_single_calls():
    class Ctx:
        calls = 0

        def get_global_state(self):
            return 0, {"qot_logined": False}

        def get_market_snapshot(self, codes):
            self.calls += 1
            return -1, "connection lost"

    ctx = Ctx()
    session, _ = _session(ctx)
    with pytest.raises(fc.FutuUnavailable):
        session.snapshot([f"US.C{i}" for i in range(400)])
    assert ctx.calls == 1


def test_a_second_throttle_fails_the_batch_without_bisecting_it():
    class Ctx:
        calls = 0

        def get_market_snapshot(self, codes):
            self.calls += 1
            return -1, "Get Market Snapshot request failed due to high frequency. Maximum 60 times per 30 seconds."

    ctx = Ctx()
    session, _ = _session(ctx)
    with pytest.raises(fc.FutuRequestError):
        session.snapshot(["US.A", "US.B"])
    assert ctx.calls == 2 and session.throttled == 1


@pytest.fixture
def listening_port():
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        yield server.getsockname()[1]


def test_a_handshake_that_never_completes_is_unavailable_within_the_connect_timeout(listening_port, monkeypatch):
    """The SDK constructor retries forever on a TCP-open OpenD that never answers."""
    monkeypatch.setenv("LW_DECLARED_FUTU_CONNECT_TIMEOUT_S", "0.2")
    hang = threading.Event()
    session = fc.QuoteSession("127.0.0.1", listening_port, context_factory=lambda **_: hang.wait(5))
    started = time.monotonic()
    with pytest.raises(fc.FutuUnavailable, match="no session within"), session:
        pass
    hang.set()
    assert time.monotonic() - started < 2


def test_a_constructor_error_is_unavailable(listening_port):
    def refuse(**_):
        raise ConnectionError("handshake refused")

    with (
        pytest.raises(fc.FutuUnavailable, match="handshake refused"),
        fc.QuoteSession("127.0.0.1", listening_port, context_factory=refuse),
    ):
        pass


def test_a_closed_port_never_reaches_the_sdk():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    built = []
    session = fc.QuoteSession("127.0.0.1", port, context_factory=lambda **kw: built.append(kw))
    with pytest.raises(fc.FutuUnavailable), session:
        pass
    assert built == []


def test_an_open_port_builds_one_context_and_closes_it(listening_port):
    closed = []

    class Ctx:
        def close(self):
            closed.append(True)

    with fc.QuoteSession("127.0.0.1", listening_port, context_factory=lambda **_: Ctx()):
        pass
    assert closed == [True]


def test_a_failed_user_info_is_unavailable_not_a_crash():
    class Ctx:
        def get_user_info(self):
            return -1, "not logged in"

    session, _ = _session(Ctx())
    with pytest.raises(fc.FutuUnavailable):
        session.user_info()


def test_a_service_wide_error_while_logged_in_stops_after_one_split_path_not_every_code():
    class Ctx:
        calls = 0

        def get_global_state(self):
            return 0, {"qot_logined": True}

        def get_market_snapshot(self, codes):
            self.calls += 1
            return -1, "upstream quote service unavailable"

    ctx = Ctx()
    session, _ = _session(ctx)
    with pytest.raises(fc.FutuRequestError, match="no code of a"):
        session.snapshot([f"US.C{i}" for i in range(400)])
    assert ctx.calls < 30  # one path down the bisection, not ~800 calls


SILENT_OPEND = """
import socket, sys, time
from clients.futu_client import FutuUnavailable, QuoteSession
server = socket.socket(); server.bind(("127.0.0.1", 0)); server.listen()
started = time.monotonic()
try:
    with QuoteSession("127.0.0.1", server.getsockname()[1]):
        pass
except FutuUnavailable as exc:
    print("unavailable", round(time.monotonic() - started, 1), exc)
"""


def test_the_real_sdk_against_a_silent_opend_gives_up_and_the_process_still_exits(tmp_path):
    """The SDK constructor never returns and its threads are non-daemon by default."""
    env = {**os.environ, "HOME": str(tmp_path), "LW_DECLARED_FUTU_CONNECT_TIMEOUT_S": "2"}
    started = time.monotonic()
    done = subprocess.run(
        [sys.executable, "-c", SILENT_OPEND],
        cwd=Path(fc.__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr[-500:]
    assert "unavailable" in done.stdout
    assert time.monotonic() - started < 30


def test_one_refused_code_in_each_half_keeps_the_served_codes_and_both_refusals(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_FUTU_CODES_PER_CALL_SNAPSHOT", "4")

    class Ctx:
        def get_global_state(self):
            return 0, {"qot_logined": True}

        def get_market_snapshot(self, codes):
            if {"US.A", "US.C"} & set(codes):
                return -1, "unknown stock"
            return 0, Frame([{"code": c} for c in codes])

    session, _ = _session(Ctx())
    rows, refused = session.snapshot(["US.A", "US.B", "US.C", "US.D"])
    assert [r["code"] for r in rows] == ["US.B", "US.D"] and sorted(refused) == ["US.A", "US.C"]
    rows, refused = session.snapshot(["US.A", "US.C", "US.B", "US.D"])  # a refused pair
    assert sorted(refused) == ["US.A", "US.C"] and len(rows) == 2


def test_history_1m_shifts_the_end_label_to_the_bar_start_and_drops_padded_minutes():
    """Futu labels 09:31 New York for the 09:30 bar and pads no-trade minutes with volume 0."""
    calls = []

    class Ctx:
        def request_history_kline(self, code, *, start, end, ktype, autype, max_count, page_req_key, extended_time):
            calls.append(page_req_key)
            assert (code, start, end, extended_time) == ("US.MSTR", "2018-01-01", "2021-08-01", False)
            from futu import AuType, KLType

            assert (autype, ktype) == (AuType.NONE, KLType.K_1M)
            if page_req_key is None:
                bars = [
                    {
                        "time_key": "2021-06-01 09:31:00",
                        "open": 1.0,
                        "high": 2.0,
                        "low": 0.5,
                        "close": 1.5,
                        "volume": 10,
                    },
                    {
                        "time_key": "2021-06-01 09:32:00",
                        "open": 1.5,
                        "high": 1.5,
                        "low": 1.5,
                        "close": 1.5,
                        "volume": 0,
                    },
                ]
                return 0, Frame(bars), b"next"
            return (
                0,
                Frame([{"time_key": "2021-12-01 16:00:00", "open": 3, "high": 3, "low": 3, "close": 3, "volume": 7}]),
                None,
            )

    session, clock = _session(Ctx())
    rows = session.history_1m("US.MSTR", date(2018, 1, 1), date(2021, 8, 1))
    assert calls == [None, b"next"]
    assert [r["bar_timestamp"] for r in rows] == [
        datetime(2021, 6, 1, 13, 30, tzinfo=UTC),  # EDT
        datetime(2021, 12, 1, 20, 59, tzinfo=UTC),  # EST
    ]
    assert rows[0] == rows[0] | {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10}
    assert len(session._limits["history_kline"].calls) == 1  # follow-up pages are not paced


def test_a_refused_history_request_is_a_request_error():
    class Ctx:
        def request_history_kline(self, code, **_):
            return -1, "Unknown stock. US.TWTR", None

    session, _ = _session(Ctx())
    with pytest.raises(fc.FutuRequestError, match="Unknown stock"):
        session.history_1m("US.TWTR", date(2018, 1, 1), date(2021, 8, 1))
