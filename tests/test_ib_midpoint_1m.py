from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients import ledger
from livewire_scripts import ib_midpoint_1m as m


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.delenv("LW_RUN_ID", raising=False)
    monkeypatch.setattr(m, "data_lake_dir", lambda: tmp_path / "lake")
    monkeypatch.setattr(m, "REQUEST_INTERVAL_S", 0)
    return tmp_path / "lake" / "bronze"


def _stored(lake: Path, asset_class: str, symbol: str) -> pa.Table:
    return pq.read_table(lake / f"asset_class={asset_class}" / f"symbol={symbol}" / "1m.parquet")


@pytest.mark.parametrize(
    ("now", "cutoff"),
    [
        # EDT: 17:00 NY = 21:00Z. Before it the previous day's close is the cutoff.
        (datetime(2026, 7, 15, 21, 20, tzinfo=UTC), datetime(2026, 7, 15, 21, 0, tzinfo=UTC)),
        (datetime(2026, 7, 15, 20, 59, tzinfo=UTC), datetime(2026, 7, 14, 21, 0, tzinfo=UTC)),
        # EST: 17:00 NY = 22:00Z, so 21:20Z (16:20 EST) still belongs to the open session.
        (datetime(2026, 12, 15, 21, 20, tzinfo=UTC), datetime(2026, 12, 14, 22, 0, tzinfo=UTC)),
        (datetime(2026, 12, 15, 22, 20, tzinfo=UTC), datetime(2026, 12, 15, 22, 0, tzinfo=UTC)),
    ],
)
def test_the_cutoff_is_the_last_17_00_new_york_close(now, cutoff):
    assert m.session_cutoff(now) == cutoff


def test_a_bar_belongs_to_the_session_that_closes_after_it():
    assert m.session_of(datetime(2026, 10, 12, 21, 15, tzinfo=UTC)) == date(2026, 10, 13)  # 17:15 EDT Monday
    assert m.session_of(datetime(2026, 10, 13, 20, 59, tzinfo=UTC)) == date(2026, 10, 13)  # 16:59 EDT Tuesday


def test_the_universe_is_the_two_ib_midpoint_presets():
    owned = m.universe()
    assert owned["XAUUSD"] == "cmdty" and owned["XAGUSD"] == "cmdty"
    assert owned["EURUSD"] == "fx_ib" and owned["USDKRW"] == "fx_ib"
    assert "DXY" not in owned  # Yahoo-only; IB never serves it
    with pytest.raises(ValueError, match="USDBRL"):
        m.universe(["USDBRL"])


def _archive(root: Path, symbol: str, files: dict[str, list[datetime]], *, partial=(), tamper=()) -> Path:
    (root / symbol).mkdir(parents=True, exist_ok=True)
    lines = ["symbol,file,partial,rows,first_utc,last_utc,sha256"]
    for name, times in files.items():
        path = root / symbol / name
        n = len(times)
        pq.write_table(
            pa.table(
                {
                    "time": pa.array(times, type=pa.timestamp("us", tz="UTC")),
                    "open": [1.1] * n,
                    "high": [1.2] * n,
                    "low": [1.0] * n,
                    "close": [1.15] * n,
                    "volume": [-1.0] * n,
                }
            ),
            path,
        )
        digest = "0" * 64 if name in tamper else hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{symbol},{name},{name in partial},{n},{times[0]},{times[-1]},{digest}")
    (root / "MANIFEST.csv").write_text("\n".join(lines) + "\n")
    return root


def _minutes(start: datetime, n: int) -> list[datetime]:
    return [start + timedelta(minutes=i) for i in range(n)]


def test_the_archive_import_verifies_skips_partials_and_never_overwrites(lake, tmp_path):
    archive = _archive(
        tmp_path / "ib-1m",
        "EURUSD",
        {
            "EURUSD_1m_2010.parquet": _minutes(datetime(2010, 6, 8, tzinfo=UTC), 3),
            "EURUSD_1m_2011.partial.parquet": _minutes(datetime(2011, 1, 3, tzinfo=UTC), 3),
            "EURUSD_1m_2012.parquet": _minutes(datetime(2012, 1, 3, tzinfo=UTC), 2),
        },
        partial={"EURUSD_1m_2011.partial.parquet"},
    )
    now = datetime(2026, 10, 14, 22, 0, tzinfo=UTC)
    assert m.main(["--archive", str(archive), "--symbols", "EURUSD"], now=now) == 0

    table = _stored(lake, "fx_ib", "EURUSD")
    assert table.num_rows == 5  # the partial year is refused
    assert set(table.column("volume").to_pylist()) == {0}
    assert table.schema.field("bar_timestamp").type == pa.timestamp("us", tz="UTC")
    measured = ledger.query("select name, value from measurements where scope = 'EURUSD' order by name")
    assert {r["name"]: r["value"] for r in measured} == {"ib_1m_bars_added": 5.0, "ib_1m_holes": 1.0}
    run = ledger.query("select verdict from runs where job = 'ib-1m' and ended is not null")
    assert [r["verdict"] for r in run] == ["OK"]

    assert m.main(["--archive", str(archive), "--symbols", "EURUSD"], now=now) == 0
    assert _stored(lake, "fx_ib", "EURUSD").num_rows == 5  # idempotent


def test_a_file_whose_hash_disagrees_with_the_manifest_is_never_read(lake, tmp_path, capsys):
    archive = _archive(
        tmp_path / "ib-1m",
        "XAUUSD",
        {"XAUUSD_1m_2010.parquet": _minutes(datetime(2010, 6, 8, 21, 15, tzinfo=UTC), 3)},
        tamper={"XAUUSD_1m_2010.parquet"},
    )
    assert m.main(["--archive", str(archive), "--symbols", "XAUUSD"], now=datetime(2026, 10, 14, tzinfo=UTC)) == 1
    assert "sha256" in capsys.readouterr().out
    assert not (lake / "asset_class=cmdty" / "symbol=XAUUSD" / "1m.parquet").exists()


class _FakeIB:
    def __init__(self, bars):
        self.bars, self.requests = bars, []

    def connect(self, **kwargs):
        pass

    def disconnect(self):
        pass

    def qualify_contract(self, contract):
        return contract

    def get_historical_data(self, contract, **kwargs):
        self.requests.append(kwargs)
        end = kwargs["end_date"]
        return [b for b in self.bars if end - m.CHUNK <= b.date < end]


def _bar(ts: datetime):
    return SimpleNamespace(date=ts, open=1.1, high=1.2, low=1.0, close=1.15)


def test_the_ib_sync_stores_only_completed_sessions_as_midpoint(lake):
    # 16:59 EDT is the last bar of session 10-13; 17:15 EDT opens 10-14, still trading at 17:20.
    closed = datetime(2026, 10, 13, 20, 59, tzinfo=UTC)
    trading = datetime(2026, 10, 13, 21, 15, tzinfo=UTC)
    ib = _FakeIB([_bar(closed - timedelta(minutes=1)), _bar(closed), _bar(trading)])
    now = datetime(2026, 10, 13, 21, 20, tzinfo=UTC)

    assert m.main(["--symbols", "EURUSD"], ib_factory=lambda: ib, now=now) == 0

    stored = _stored(lake, "fx_ib", "EURUSD").column("bar_timestamp").to_pylist()
    assert stored == [closed - timedelta(minutes=1), closed]
    assert ib.requests[0]["what_to_show"] == "MIDPOINT" and ib.requests[0]["use_rth"] is False
    last = ledger.query("select value from measurements where name = 'last_session' and scope = 'ib_1m'")
    assert [date(1970, 1, 1) + timedelta(days=int(r["value"])) for r in last] == [date(2026, 10, 13)]


def test_a_naive_ib_bar_time_is_refused_not_read_in_the_host_zone(lake):
    ib = _FakeIB([])
    ib.get_historical_data = lambda contract, **kwargs: [_bar(datetime(2026, 10, 13, 12, 0))]
    assert m.main(["--symbols", "EURUSD"], ib_factory=lambda: ib, now=datetime(2026, 10, 13, 22, tzinfo=UTC)) == 1
    assert not (lake / "asset_class=fx_ib" / "symbol=EURUSD" / "1m.parquet").exists()


def test_validate_rejects_duplicates_and_impossible_ohlc():
    ts = datetime(2026, 1, 2, tzinfo=UTC)
    dup = m._table([ts, ts], [1.0, 1.0], [1.0, 1.0], [1.0, 1.0], [1.0, 1.0], 1, False)
    with pytest.raises(ValueError, match="strictly increasing"):
        m.validate(dup, "X")
    bad = m._table([ts], [2.0], [1.5], [1.0], [1.2], 1, False)
    with pytest.raises(ValueError, match="open outside"):
        m.validate(bad, "X")


def test_a_history_seed_walks_back_in_two_week_chunks_paced_between_requests(lake, monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(m.clock, "sleep", slept.append)
    monkeypatch.setattr(m, "REQUEST_INTERVAL_S", 10)
    ib = _FakeIB([])
    now = datetime(2026, 10, 13, 21, 20, tzinfo=UTC)
    assert m.main(["--symbols", "XAUUSD", "--since", "2026-09-01"], ib_factory=lambda: ib, now=now) == 0
    assert len(ib.requests) == 4 and slept == [10, 10, 10]  # 42 days back from the cutoff, 14 per request


def test_an_interrupt_still_closes_the_run(lake):
    class _Interrupting(_FakeIB):
        def get_historical_data(self, contract, **kwargs):
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        m.main(
            ["--symbols", "EURUSD"], ib_factory=lambda: _Interrupting([]), now=datetime(2026, 10, 13, 22, tzinfo=UTC)
        )
    closed = ledger.query("select verdict from runs where job = 'ib-1m' and ended is not null")
    assert [r["verdict"] for r in closed] == ["FAILED"]
