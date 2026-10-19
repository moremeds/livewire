from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime, timedelta

import pyarrow.parquet as pq
import pytest

from clients import ledger
from clients.futu_client import NEW_YORK, FutuUnavailable
from livewire_scripts import fetch_futu_options as f
from livewire_scripts import status

#: Friday 2026-10-16, a full day: official slot 15:45 New York = 19:45Z, deadline 16:30 = 20:30Z.
OFFICIAL = datetime(2026, 10, 16, 19, 45, tzinfo=UTC)


class Clock:
    """One clock for the job and the fake vendor, so discovery and capture times line up."""

    at = OFFICIAL


class Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return Clock.at.astimezone(tz) if tz else Clock.at.replace(tzinfo=None)


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.delenv("LW_RUN_ID", raising=False)
    monkeypatch.setattr(f, "data_lake_dir", lambda: tmp_path / "lake")
    monkeypatch.setattr(f, "datetime", Frozen)
    Clock.at = OFFICIAL
    return tmp_path / "lake" / "bronze"


def _code(root, expiry, right, strike):
    return f"US.{root}{expiry:%y%m%d}{right}{int(strike * 1000)}"


def _day(lake, underlying, day="2026-10-16"):
    return lake / "asset_class=option" / f"underlying={underlying}" / f"date={day}"


class FakeSession:
    """Two SPX expiries 40 days apart (two chain windows); VIX one; NDX/RUT one each."""

    def __init__(
        self, *, right="LV1", refuse=(), drop=(), fail=(), quote_age_s=2.0, strikes=(100.0, 105.0), unquoted=False
    ):
        self.right, self.refuse, self.drop, self.fail = right, set(refuse), set(drop), set(fail)
        self.quote_age_s, self.strikes, self.unquoted = quote_age_s, strikes, unquoted
        self.throttled = 0
        self.chain_calls: list[tuple] = []
        self.snapshots: list[list[str]] = []
        self.listed = {
            "US..SPX": [date(2026, 10, 15), date(2026, 10, 16), date(2026, 11, 25)],
            "US..VIX": [date(2026, 10, 21)],
            "US..NDX": [date(2026, 10, 16)],
            "US..RUT": [date(2026, 10, 16)],
        }

    def __call__(self, host, port):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def user_info(self):
        return {"us_option_qot_right": self.right}

    def expiries(self, underlying):
        if underlying in self.fail:
            raise RuntimeError("chain endpoint down")
        return self.listed[underlying]

    def chain(self, underlying, start, end):
        self.chain_calls.append((underlying, start, end))
        root = {"US..SPX": "SPXW", "US..VIX": "VIX", "US..NDX": "NDXP", "US..RUT": "RUTW"}[underlying]
        return [
            {
                "code": _code(root, expiry, right, strike),
                "strike_time": expiry.isoformat(),
                "strike_price": strike,
                "option_type": "CALL" if right == "C" else "PUT",
                "option_settlement_mode": "PM",
                "expiration_cycle": "WEEK",
                "lot_size": 100,
            }
            for expiry in self.listed[underlying]
            if start <= expiry <= end
            for right in "CP"
            for strike in self.strikes
        ]

    def snapshot(self, codes):
        self.snapshots.append(list(codes))
        received = Frozen.now(UTC)
        stamp = (received - timedelta(seconds=self.quote_age_s)).astimezone(NEW_YORK)
        rows = [
            {
                "code": code,
                "update_time": stamp.strftime("%Y-%m-%d %H:%M:%S"),
                "bid_price": None if self.unquoted else 1.0,
                "ask_price": None if self.unquoted else 1.2,
                "bid_vol": 10,
                "ask_vol": 12,
                "last_price": 1.1,
                "volume": 5,
                "option_open_interest": 50,
                "option_implied_volatility": 0.0 if code.endswith("105000") else 18.5,
                "option_contract_multiplier": 100.0,
                "received_at": received,
            }
            for code in codes
            if code not in self.refuse and code not in self.drop
        ]
        return rows, [c for c in codes if c in self.refuse]


def _runs():
    return ledger.query(
        "select verdict, exit_code from runs where job = 'futu-options' and ended is not null order by ended"
    )


def _measured(name):
    return {
        r["scope"]: r["value"]
        for r in ledger.query(f"select scope, value from measurements where name = '{name}' order by measured_at")
    }


@pytest.mark.parametrize(
    ("ny", "expected"),
    [
        (datetime(2026, 10, 16, 9, 30), []),
        (datetime(2026, 10, 16, 9, 40), ["SPX", "VIX"]),
        (datetime(2026, 10, 16, 9, 45), []),
        (datetime(2026, 10, 16, 10, 0), ["SPX", "VIX", "NDX", "RUT"]),
        (datetime(2026, 10, 16, 10, 10), ["SPX", "VIX"]),
        (datetime(2026, 10, 16, 15, 45), ["SPX", "VIX", "NDX", "RUT"]),
        (datetime(2026, 10, 16, 16, 10), ["SPX", "VIX"]),
        (datetime(2026, 10, 16, 16, 20), []),
        (datetime(2026, 10, 17, 10, 0), []),  # Saturday
        (datetime(2026, 11, 27, 12, 45), ["SPX", "VIX", "NDX", "RUT"]),  # half day: close 13:00
        (datetime(2026, 11, 27, 13, 20), []),
    ],
)
def test_the_new_york_clock_decides_what_is_due(ny, expected):
    assert f.due(ny.replace(tzinfo=NEW_YORK)) == expected


def test_a_tick_is_floored_to_its_five_minute_slot():
    assert f.slot_of(datetime(2026, 10, 16, 19, 49, 59, tzinfo=UTC)) == datetime(2026, 10, 16, 15, 45, tzinfo=NEW_YORK)


def test_a_late_tick_before_the_close_takes_the_missing_official_capture_first(lake):
    """A :45 wake that ran at 15:52 (previous tick still running, Mac asleep) must not lose the day."""
    official = datetime(2026, 10, 16, 15, 45, tzinfo=NEW_YORK)
    late = datetime(2026, 10, 16, 15, 52, tzinfo=NEW_YORK)
    assert f.targets(late, lake) == [
        *[(u, official) for u in ("SPX", "VIX", "NDX", "RUT")],
        ("SPX", late.replace(minute=50)),
        ("VIX", late.replace(minute=50)),
    ]
    # 16:04 floors to the 16:00 slot, but the instant is after the close: no official capture.
    assert official not in {slot for _, slot in f.targets(datetime(2026, 10, 16, 16, 4, tzinfo=NEW_YORK), lake)}
    # The 16:00 wake, in the close minute, still recovers it.
    assert (("SPX", official)) in f.targets(datetime(2026, 10, 16, 16, 0, 20, tzinfo=NEW_YORK), lake)
    after_close = datetime(2026, 10, 16, 16, 10, tzinfo=NEW_YORK)
    # After the close an official capture would be a different market: only the due slot.
    assert f.targets(after_close, lake) == [("SPX", after_close), ("VIX", after_close)]


def test_a_late_tick_files_the_official_ratio(lake):
    Clock.at = datetime(2026, 10, 16, 19, 52, tzinfo=UTC)
    assert f.main([], session_factory=FakeSession(), now=Clock.at) == 0
    assert set(_measured("futu_official_capture_ratio")) == {"SPX", "VIX", "NDX", "RUT"}
    assert (_day(lake, "SPX") / "snap_1545.parquet").exists() and (_day(lake, "SPX") / "snap_1550.parquet").exists()


def test_a_tick_with_nothing_due_writes_no_run(lake):
    assert f.main([], session_factory=FakeSession(), now=datetime(2026, 10, 17, 14, 0, tzinfo=UTC)) == 0
    assert ledger.query("select count(*) as n from runs")[0]["n"] == 0


def test_the_official_capture_stores_the_chain_and_one_snapshot_per_underlying(lake):
    session = FakeSession()
    assert f.main([], session_factory=session, now=OFFICIAL) == 0

    contracts = pq.read_table(_day(lake, "SPX") / "contracts.parquet").to_pylist()
    # The expired 10-15 contract is not expected; the 11-25 expiry needed its own 30-day window.
    assert {c["expiry"] for c in contracts} == {date(2026, 10, 16), date(2026, 11, 25)}
    assert [c[1:] for c in session.chain_calls if c[0] == "US..SPX"] == [
        (date(2026, 10, 15), date(2026, 11, 13)),
        (date(2026, 11, 25), date(2026, 12, 24)),
    ]
    snap = pq.read_table(_day(lake, "SPX") / "snap_1545.parquet").to_pylist()
    assert len(snap) == 8 and {r["root"] for r in snap} == {"SPXW"}
    assert snap[0]["oi_asof"] == date(2026, 10, 15)
    assert snap[0]["capture_ts_utc"] == OFFICIAL  # the batch's receipt time, not the run's start
    assert _measured("futu_official_capture_ratio") == {"SPX": 1.0, "VIX": 1.0, "NDX": 1.0, "RUT": 1.0}
    assert _measured("futu_iv_null_share")["SPX"] == 0.5
    assert _runs() == [{"verdict": "OK", "exit_code": 0}]


def test_each_index_carries_its_own_etf_and_vix_carries_none(lake):
    session = FakeSession()
    f.main([], session_factory=session, now=OFFICIAL)
    proxies = {
        u: {(r["proxy_code"], r["proxy_bid"]) for r in pq.read_table(_day(lake, u) / "snap_1545.parquet").to_pylist()}
        for u in ("SPX", "NDX", "RUT", "VIX")
    }
    assert proxies == {
        "SPX": {("US.SPY", 1.0)},
        "NDX": {("US.QQQ", 1.0)},
        "RUT": {("US.IWM", 1.0)},
        "VIX": {(None, None)},
    }
    assert ["US.SPY"] in session.snapshots and [None] not in session.snapshots


def test_the_half_hour_before_the_official_slot_adds_strikes_listed_since_the_open(lake):
    Clock.at = datetime(2026, 10, 16, 13, 40, tzinfo=UTC)  # 09:40 NY: first discovery
    f.main(["--underlying", "SPX"], session_factory=FakeSession(), now=Clock.at)
    Clock.at = datetime(2026, 10, 16, 19, 30, tzinfo=UTC)  # 15:30 NY
    f.main(["--underlying", "SPX"], session_factory=FakeSession(strikes=(100.0, 105.0, 110.0)), now=Clock.at)

    contracts = pq.read_table(_day(lake, "SPX") / "contracts.parquet").to_pylist()
    assert len(contracts) == 12 and sum(c["strike"] == 110.0 for c in contracts) == 4
    assert {c["discovered_at"] for c in contracts if c["strike"] == 110.0} == {Clock.at}
    assert pq.read_table(_day(lake, "SPX") / "snap_1530.parquet").num_rows == 12
    assert _measured("futu_contracts_added_intraday") == {"SPX": 4.0}


def test_rerunning_a_short_slot_grades_it_again_and_still_fails(lake):
    """A row OpenD silently left out (no error, no row) is missing."""
    vix_call = _code("VIX", date(2026, 10, 21), "C", 100.0)
    assert f.main(["--underlying", "VIX"], session_factory=FakeSession(drop={vix_call}), now=OFFICIAL) == 1
    path = _day(lake, "VIX") / "snap_1545.parquet"
    before = path.stat().st_mtime_ns

    assert f.main(["--underlying", "VIX"], session_factory=FakeSession(), now=OFFICIAL) == 1
    assert path.stat().st_mtime_ns == before  # append-only: never rewritten
    ratios = ledger.query(
        "select value from measurements where name = 'futu_official_capture_ratio' order by measured_at"
    )
    assert [r["value"] for r in ratios] == [0.75, 0.75]
    assert _runs() == [{"verdict": "FAILED", "exit_code": 1}] * 2


def test_a_code_opend_refuses_by_name_is_recorded_not_missing_and_warns(lake):
    vix_call = _code("VIX", date(2026, 10, 21), "C", 100.0)
    assert f.main(["--underlying", "VIX"], session_factory=FakeSession(refuse={vix_call}), now=OFFICIAL) == 0
    stored = pq.read_table(_day(lake, "VIX") / "snap_1545.parquet").to_pylist()
    assert [(r["code"], r["bid"]) for r in stored if r["refused"]] == [(vix_call, None)]
    assert f.main(["--underlying", "VIX"], session_factory=FakeSession(), now=OFFICIAL) == 0  # regrade agrees
    assert _measured("futu_official_capture_ratio") == {"VIX": 1.0}
    assert _measured("futu_refused_codes") == {"VIX": 1.0}
    f.main(["--underlying", "SPX", "NDX", "RUT"], session_factory=FakeSession(), now=OFFICIAL)
    assert _grade(NEXT_DIGEST).verdict is status.Verdict.WARN


def test_an_am_settled_option_is_not_expected_on_its_own_expiry_day():
    """SPX/NDX/RUT monthlies and VIX stop trading the session before; they settle at the open."""
    day = date(2026, 10, 16)
    assert not f._trading(day, "AM", day)
    assert f._trading(day, "PM", day) and f._trading(day + timedelta(days=1), "AM", day)
    assert not f._trading(day - timedelta(days=1), "PM", day)


def test_a_failed_rediscovery_still_captures_the_held_set(lake):
    Clock.at = datetime(2026, 10, 16, 13, 40, tzinfo=UTC)
    f.main(["--underlying", "SPX"], session_factory=FakeSession(), now=Clock.at)
    Clock.at = datetime(2026, 10, 16, 19, 30, tzinfo=UTC)  # 15:30: the refresh tick
    assert f.main(["--underlying", "SPX"], session_factory=FakeSession(fail={"US..SPX"}), now=Clock.at) == 0
    assert pq.read_table(_day(lake, "SPX") / "snap_1530.parquet").num_rows == 8
    assert _measured("futu_discovery_failed") == {"SPX": 1.0}


def test_regrading_an_early_slot_ignores_strikes_listed_after_it(lake):
    Clock.at = datetime(2026, 10, 16, 13, 40, tzinfo=UTC)
    f.main(["--underlying", "SPX"], session_factory=FakeSession(), now=Clock.at)
    Clock.at = datetime(2026, 10, 16, 19, 30, tzinfo=UTC)
    f.main(["--underlying", "SPX"], session_factory=FakeSession(strikes=(100.0, 105.0, 110.0)), now=Clock.at)
    early = datetime(2026, 10, 16, 13, 40, tzinfo=UTC)
    assert f.main(["--underlying", "SPX"], session_factory=FakeSession(), now=early) == 0


def test_two_captures_of_one_slot_at_once_publish_once(lake):
    """The lock covers check and publish: the second waits, then grades what the first stored."""

    calls = []

    class Slow(FakeSession):
        def snapshot(self, codes):
            calls.append(codes)
            time.sleep(0.3)
            return super().snapshot(codes)

    results = []
    workers = [
        threading.Thread(target=lambda: results.append(f.capture(Slow(), lake, "VIX", f.slot_of(OFFICIAL), "r")))
        for _ in range(2)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    assert [ok for ok, _ in results] == [True, True]
    assert len(calls) == 1  # the second capture found the slot taken and asked the vendor nothing


def test_one_broken_underlying_costs_no_other(lake):
    assert f.main([], session_factory=FakeSession(fail={"US..NDX"}), now=OFFICIAL) == 1
    assert _measured("futu_official_capture_ratio") == {"SPX": 1.0, "VIX": 1.0, "RUT": 1.0}
    assert _measured("futu_capture_failed") == {"NDX": 1}
    assert _runs() == [{"verdict": "FAILED", "exit_code": 1}]


def test_rows_with_no_bid_and_no_ask_anywhere_fail_the_underlying(lake):
    """Every row lands and the ratio is 1.0 when a vendor field is renamed; the quotes do not."""
    assert f.main(["--underlying", "SPX"], session_factory=FakeSession(unquoted=True), now=OFFICIAL) == 1


@pytest.mark.parametrize("session", [FakeSession(right="NO_AUTHORITY")])
def test_no_us_option_quote_right_is_a_skipped_run(lake, session):
    assert f.main([], session_factory=session, now=OFFICIAL) == f.EXIT_UNAVAILABLE
    assert _runs() == [{"verdict": "DEGRADED", "exit_code": 86}]
    assert _measured("futu_us_option_right_ok") == {"futu": 0.0}


def test_an_unreachable_opend_is_a_skipped_run(lake):
    def factory(host, port):
        raise FutuUnavailable("OpenD unreachable")

    assert f.main([], session_factory=factory, now=OFFICIAL) == f.EXIT_UNAVAILABLE
    assert _runs() == [{"verdict": "DEGRADED", "exit_code": 86}]


def test_a_session_lost_mid_run_is_a_skipped_run_not_a_capture_failure(lake):
    class Lost(FakeSession):
        def snapshot(self, codes):
            raise FutuUnavailable("OpenD quote session lost")

    assert f.main([], session_factory=Lost(), now=OFFICIAL) == f.EXIT_UNAVAILABLE
    assert _runs() == [{"verdict": "DEGRADED", "exit_code": 86}]


def test_an_interrupt_still_closes_the_run(lake):
    class Interrupted(FakeSession):
        def snapshot(self, codes):
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        f.main(["--underlying", "SPX"], session_factory=Interrupted(), now=OFFICIAL)
    assert _runs() == [{"verdict": "FAILED", "exit_code": 1}]


def test_a_hung_call_is_closed_failed_by_the_run_budget(lake, monkeypatch):
    monkeypatch.setenv("LW_DECLARED_RUN_BUDGET_S_FUTU_OPTIONS", "1")

    class Hung(FakeSession):
        def snapshot(self, codes):
            time.sleep(3)

    with pytest.raises(f.RunBudgetExceeded):
        f.main(["--underlying", "SPX"], session_factory=Hung(), now=OFFICIAL)
    assert _runs() == [{"verdict": "FAILED", "exit_code": 1}]


def _grade(now: datetime):
    params = {**status._futu_params(now), "now": now.strftime("%Y-%m-%d %H:%M:%S")}
    return status.run_check("Futu option snapshots", dict(status.CHECKS)["Futu option snapshots"], params)


#: The scheduled readers: the 10:30Z watchdog and the 15:45Z digest, the next morning and on Saturday.
NEXT_WATCHDOG = datetime(2026, 10, 17, 10, 30, tzinfo=UTC)
NEXT_DIGEST = datetime(2026, 10, 17, 15, 45, tzinfo=UTC)


def test_the_next_mornings_readers_grade_the_previous_sessions_official_capture(lake):
    """Producer and grader through the real measurement names, not a hand-written row."""
    assert _grade(NEXT_WATCHDOG).verdict is status.Verdict.UNKNOWN  # never ran here: not deployed

    f.main(["--underlying", "SPX"], session_factory=FakeSession(), now=OFFICIAL)
    for reader in (NEXT_WATCHDOG, NEXT_DIGEST):
        graded = _grade(reader)
        assert graded.verdict is status.Verdict.BAD  # VIX/NDX/RUT have no official capture
        assert any("session=2026-10-16  underlying=SPX  official_capture_ratio=1.0" in line for line in graded.lines)

    f.main(["--underlying", "VIX", "NDX", "RUT"], session_factory=FakeSession(), now=OFFICIAL)
    assert _grade(NEXT_DIGEST).verdict is status.Verdict.OK


def test_a_delayed_feed_is_warn(lake):
    f.main([], session_factory=FakeSession(quote_age_s=900), now=OFFICIAL)
    assert _grade(NEXT_DIGEST).verdict is status.Verdict.WARN


def test_a_later_skipped_run_is_warn_even_after_a_good_official_capture(lake):
    f.main([], session_factory=FakeSession(), now=OFFICIAL)
    Clock.at = OFFICIAL + timedelta(minutes=15)
    f.main([], session_factory=FakeSession(right="NO_AUTHORITY"), now=Clock.at)
    assert _grade(NEXT_WATCHDOG).verdict is status.Verdict.WARN


def test_missing_quote_times_are_unknown_not_ok(lake):
    class NoTimes(FakeSession):
        def snapshot(self, codes):
            rows, refused = super().snapshot(codes)
            return [r | {"update_time": None} for r in rows], refused

    f.main([], session_factory=NoTimes(), now=OFFICIAL)
    assert _grade(NEXT_DIGEST).verdict is status.Verdict.UNKNOWN


@pytest.mark.parametrize(
    ("now", "session"),
    [
        (datetime(2026, 10, 16, 20, 29, tzinfo=UTC), "2026-10-15"),  # before Friday's deadline
        (datetime(2026, 10, 16, 20, 30, tzinfo=UTC), "2026-10-16"),
        (datetime(2026, 10, 18, 15, 45, tzinfo=UTC), "2026-10-16"),  # Sunday grades Friday
        (datetime(2026, 11, 27, 18, 30, tzinfo=UTC), "2026-11-27"),  # half day: close 13:00 EST
        (datetime(2026, 12, 26, 10, 30), "2026-12-24"),  # naive is UTC; Christmas is skipped
    ],
)
def test_the_graded_session_is_the_latest_whose_deadline_passed(now, session):
    assert status._futu_params(now)["futu_session"] == session


def test_a_quoteless_official_file_is_bad_even_after_a_later_ok_run(lake):
    f.main([], session_factory=FakeSession(unquoted=True), now=OFFICIAL)
    Clock.at = datetime(2026, 10, 19, 13, 40, tzinfo=UTC)  # Monday 09:40: an OK run
    f.main([], session_factory=FakeSession(), now=Clock.at)
    assert _grade(datetime(2026, 10, 19, 15, 45, tzinfo=UTC)).verdict is status.Verdict.BAD  # still grades Friday


def test_a_failed_latest_run_is_warn_even_when_no_lag_was_measured(lake):
    class NoTimes(FakeSession):
        def snapshot(self, codes):
            rows, refused = super().snapshot(codes)
            return [r | {"update_time": None} for r in rows], refused

    f.main([], session_factory=NoTimes(), now=OFFICIAL)
    Clock.at = OFFICIAL + timedelta(minutes=15)
    f.main([], session_factory=FakeSession(right="NO_AUTHORITY"), now=Clock.at)
    assert _grade(NEXT_DIGEST).verdict is status.Verdict.WARN


def test_the_lag_graded_is_the_official_captures_not_a_later_ones(lake):
    class NoTimes(FakeSession):
        def snapshot(self, codes):
            rows, refused = super().snapshot(codes)
            return [r | {"update_time": None} for r in rows], refused

    f.main([], session_factory=NoTimes(), now=OFFICIAL)
    Clock.at = OFFICIAL + timedelta(minutes=5)  # 15:50: SPX/VIX with fresh quotes
    f.main([], session_factory=FakeSession(), now=Clock.at)
    assert _grade(NEXT_DIGEST).verdict is status.Verdict.UNKNOWN


def test_a_late_tick_cannot_lend_its_regular_slots_lag_to_the_official_capture(lake):
    class UndatedOfficial(FakeSession):
        """The first SPX chain call (the late official capture) carries no vendor timestamps."""

        def snapshot(self, codes):
            rows, refused = super().snapshot(codes)
            if any("SPXW" in c for c in codes) and not any("SPXW" in c for c in sum(self.snapshots[:-1], [])):
                rows = [r | {"update_time": None} for r in rows]
            return rows, refused

    Clock.at = datetime(2026, 10, 16, 19, 52, tzinfo=UTC)  # 15:52: the official SPX slot, then the 15:50 slot
    f.main([], session_factory=UndatedOfficial(), now=Clock.at)
    assert "SPX" in _measured("futu_quote_lag_s")  # the regular slot had a lag ...
    assert "SPX" not in _measured("futu_official_quote_lag_s")  # ... the official one did not
    # The other three are OK, so only SPX's missing official lag can make the section UNKNOWN.
    assert _grade(NEXT_WATCHDOG).verdict is status.Verdict.UNKNOWN


@pytest.mark.parametrize("slow", ["snapshot", "expiries"])  # earlier captures, or this one's own discovery
def test_an_official_capture_that_would_start_after_the_close_minute_is_not_taken(lake, slow):
    real = getattr(FakeSession, slow)

    def tick(self, *args):
        Clock.at += timedelta(seconds=20)
        return real(self, *args)

    Slow = type("Slow", (FakeSession,), {slow: tick})
    Clock.at = datetime(2026, 10, 16, 20, 0, 30, tzinfo=UTC)  # 16:00:30: all four official slots still missing
    assert f.main([], session_factory=Slow(), now=Clock.at) == 1
    taken = set(_measured("futu_official_capture_ratio"))
    skipped = set(_measured("futu_official_past_close"))
    # Either way the clock crosses 16:01 inside VIX's capture (its discovery, or SPX's two snapshots
    # before it), so a check placed before discovery would still take VIX.
    assert taken == {"SPX"} and skipped == {"VIX", "NDX", "RUT"}
    assert all(not (_day(lake, u) / "snap_1545.parquet").exists() for u in skipped)
    assert (_day(lake, "SPX") / "snap_1600.parquet").exists()  # the regular slot is still taken
