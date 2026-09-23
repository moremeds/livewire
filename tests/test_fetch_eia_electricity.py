"""The EIA electricity lane end to end over a temp lake and ledger; HTTP via MockTransport."""

from __future__ import annotations

import copy
from datetime import date

import httpx
import pyarrow.parquet as pq
import pytest

from clients import ledger
from clients.eia_client import EiaClient
from clients.source_evidence import SourceEvidenceStore
from livewire_scripts import fetch_eia_electricity as lane
from tests.test_eia_client import KEY, ROWS

REGION_2026_09 = "bronze/asset_class=energy/product=electricity/dataset=region/month=2026-09/1d.parquet"


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "lake" / "ledger"))
    monkeypatch.delenv("LW_RUN_ID", raising=False)
    monkeypatch.setenv("LW_DECLARED_EIA_RETRY_ATTEMPTS", "1")
    return tmp_path / "lake"


def eia(rows=ROWS, *, fail_start=None, seen=None):
    """Answers every window with `rows` (filtered to the window), 503 for a window starting at `fail_start`."""

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        if seen is not None:
            seen.append(request)
        if params["start"] == fail_start:
            return httpx.Response(503, request=request)
        window = [r for r in rows if params["start"] <= r["period"] <= params["end"]]
        offset, length = int(params["offset"]), int(params["length"])
        return httpx.Response(
            200, json={"response": {"total": len(window), "data": window[offset : offset + length]}}, request=request
        )

    return EiaClient(KEY, http_client=httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda _: None)


def run(*argv, client):
    return lane.run(["--dataset", "region", *argv], client=client)


def test_a_month_publishes_every_timezone_row_under_the_composite_key(lake):
    assert run("--start", "2026-09-14", "--end", "2026-09-15", client=eia()) == 0

    table = pq.ParquetFile(lake / REGION_2026_09).read()
    assert table.num_rows == 40
    assert table.schema.names == [
        "period",
        "respondent",
        "type",
        "timezone",
        "respondent_name",
        "type_name",
        "value",
        "units",
        "source",
    ]
    rows = table.to_pylist()
    assert {r["timezone"] for r in rows} == {"Arizona", "Central", "Eastern", "Mountain", "Pacific"}
    first = rows[0]
    assert (first["period"], first["respondent"], first["type"], first["timezone"]) == (
        date(2026, 9, 14),
        "AECI",
        "D",
        "Arizona",
    )
    assert first["value"] == 72346.0 and first["units"] == "megawatthours" and first["source"] == "eia"


def test_a_rerun_upserts_by_key_and_keeps_rows_eia_no_longer_serves(lake):
    run("--start", "2026-09-14", "--end", "2026-09-15", client=eia())
    revised = copy.deepcopy(ROWS[:5])
    revised[0]["value"] = None  # EIA withdrew a value: null stays null, never 0
    run("--start", "2026-09-14", "--end", "2026-09-15", client=eia(revised))

    rows = pq.ParquetFile(lake / REGION_2026_09).read().to_pylist()
    assert len(rows) == 40
    assert rows[0]["value"] is None


def test_a_standalone_run_opens_and_closes_its_own_ledger_run_and_commits_evidence(lake):
    run("--start", "2026-09-14", "--end", "2026-09-15", client=eia())

    runs = ledger.query(
        "select verdict, exit_code, ended from runs where job = 'eia-electricity' order by ended nulls first"
    )
    assert [(r["verdict"], r["exit_code"]) for r in runs] == [(None, None), ("OK", 0)]
    measured = {r["name"]: r for r in ledger.query("select name, scope, value from measurements")}
    assert measured["eia_rows_fetched"]["scope"] == "electricity/region:2026-09"
    assert measured["eia_rows_fetched"]["value"] == 40
    assert measured["eia_staleness_days"]["scope"] == "electricity/region"

    evidence = SourceEvidenceStore(lake).list_verified()
    assert len(evidence) == 1
    assert evidence[0].content_type == "application/gzip" and "api_key" not in evidence[0].source_url


def test_under_sync_runner_it_writes_measurements_under_the_parent_run_and_no_runs_row(lake, monkeypatch):
    monkeypatch.setenv("LW_RUN_ID", "intraday-catchup-20260923T100000Z-1")
    run("--start", "2026-09-14", "--end", "2026-09-15", client=eia())

    assert ledger.query("select * from runs") == []
    assert {r["run_id"] for r in ledger.query("select run_id from measurements")} == {
        "intraday-catchup-20260923T100000Z-1"
    }


def test_a_failed_month_is_named_the_others_publish_and_the_run_fails(lake):
    rows = copy.deepcopy(ROWS)
    rows[0]["period"] = "2026-08-31"  # one row in August so both windows have data

    assert run("--start", "2026-08-31", "--end", "2026-09-15", client=eia(rows, fail_start="2026-08-31")) == 1

    assert (lake / REGION_2026_09).exists()
    assert not (lake / REGION_2026_09.replace("2026-09", "2026-08")).exists()
    failed = ledger.query("select scope from measurements where name = 'eia_fetch_failed'")
    assert [r["scope"] for r in failed] == ["electricity/region:2026-08:503"]
    assert ledger.query("select verdict from runs where ended is not null")[0]["verdict"] == "FAILED"


def test_the_default_window_is_the_declared_lookback(lake, monkeypatch):
    monkeypatch.setenv("LW_DECLARED_EIA_ELECTRICITY_LOOKBACK_DAYS", "14")
    seen: list[httpx.Request] = []
    run("--end", "2026-09-15", client=eia(seen=seen))
    assert (seen[0].url.params["start"], seen[0].url.params["end"]) == ("2026-09-01", "2026-09-15")


def test_a_row_without_a_key_field_fails_its_month_instead_of_publishing(lake):
    rows = copy.deepcopy(ROWS)
    del rows[3]["timezone"]
    assert run("--start", "2026-09-14", "--end", "2026-09-15", client=eia(rows)) == 1
    assert not (lake / REGION_2026_09).exists()


def test_month_windows_split_on_calendar_months_and_clip_to_the_range():
    assert lane.month_windows(date(2026, 1, 30), date(2026, 3, 2)) == [
        (date(2026, 1, 30), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 28)),
        (date(2026, 3, 1), date(2026, 3, 2)),
    ]


def test_the_backfill_never_starts_before_the_routes_first_period(lake):
    seen: list[httpx.Request] = []
    run("--start", "2018-06-01", "--end", "2019-01-02", client=eia(seen=seen))
    assert seen[0].url.params["start"] == "2019-01-01"


def test_every_dataset_is_keyed_to_include_the_timezone():
    assert all(dataset.keys[-1] == "timezone" for dataset in lane.DATASETS.values())


def test_ctrl_c_mid_backfill_still_closes_the_run_and_commits_what_was_fetched(lake, monkeypatch):
    def interrupted(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(lane, "upsert", interrupted)
    with pytest.raises(KeyboardInterrupt):
        run("--start", "2026-09-14", "--end", "2026-09-15", client=eia())

    closed = ledger.query("select verdict from runs where ended is not null")
    assert [r["verdict"] for r in closed] == ["FAILED"]
    assert len(SourceEvidenceStore(lake).list_verified()) == 1


def test_an_unreadable_month_file_fails_that_month_only(lake):
    path = lake / REGION_2026_09
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not parquet")
    assert run("--start", "2026-09-14", "--end", "2026-09-15", client=eia()) == 1
    failed = ledger.query("select scope from measurements where name = 'eia_fetch_failed'")
    assert [r["scope"] for r in failed] == ["electricity/region:2026-09:ArrowInvalid"]
    assert path.read_bytes() == b"not parquet"  # never overwritten blind
