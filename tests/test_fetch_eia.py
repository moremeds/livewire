"""The EIA lane end to end over a temp lake and ledger; HTTP via MockTransport."""

from __future__ import annotations

import copy
import json
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from clients import ledger
from clients.eia_client import EiaClient
from clients.source_evidence import SourceEvidenceStore
from livewire_scripts import fetch_eia as lane
from tests.test_eia_client import KEY, ROWS

ENERGY = "bronze/asset_class=energy"
REGION_2026_09 = f"{ENERGY}/product=electricity/dataset=region/month=2026-09/1d.parquet"
# One real row per route, fetched from the mini on 2026-09-23.
ONE_ROW = json.loads((Path(__file__).parent / "fixtures/eia/one-row-per-route-2026-09-23.json").read_text())


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "lake" / "ledger"))
    monkeypatch.delenv("LW_RUN_ID", raising=False)
    monkeypatch.setenv("LW_DECLARED_EIA_RETRY_ATTEMPTS", "1")
    return tmp_path / "lake"


def eia(rows=ROWS, *, fail_start=None, seen=None, facet_names=None, route=None):
    """Answers every window with `rows` (filtered to the window, any facet filter, and `route` if given)."""

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        if seen is not None:
            seen.append(request)
        if "/facet/" in request.url.path:
            facet = request.url.path.rsplit("/", 1)[-1]
            names = (facet_names or {}).get(facet, {})
            facets = [{"id": i, "name": n} for i, n in names.items()]
            return httpx.Response(200, json={"response": {"facets": facets}}, request=request)
        if params["start"] == fail_start:
            return httpx.Response(503, request=request)
        served = rows if route is None or f"/{route}/" in request.url.path else []
        window = [r for r in served if params["start"] <= r["period"] <= params["end"] + "~"]
        for name in set(params.keys()):
            if name.startswith("facets["):
                facet = name[len("facets[") : name.index("]")]
                wanted = params.get_list(name)
                window = [r for r in window if r.get(facet) in wanted]
        offset, length = int(params["offset"]), int(params["length"])
        # Like EIA, echo the request (minus the key): distinct requests never share a body.
        echo = {"command": request.url.path, "params": [item for item in params.multi_items() if item[0] != "api_key"]}
        page = {"total": len(window), "data": window[offset : offset + length]}
        return httpx.Response(200, json={"response": page, "request": echo}, request=request)

    return EiaClient(KEY, http_client=httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda _: None)


def run(*argv, client):
    return lane.run(list(argv), client=client)


def read(lake, relative):
    return pq.ParquetFile(lake / relative).read()


# --- electricity daily -------------------------------------------------------


def test_a_month_publishes_every_timezone_row_under_the_composite_key(lake):
    assert run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia()) == 0

    table = read(lake, REGION_2026_09)
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
    run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia())
    revised = copy.deepcopy(ROWS[:5])
    revised[0]["value"] = None  # EIA withdrew a value: null stays null, never 0
    run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia(revised))

    rows = read(lake, REGION_2026_09).to_pylist()
    assert len(rows) == 40
    assert rows[0]["value"] is None


def test_a_standalone_run_opens_and_closes_its_own_ledger_run_and_commits_evidence(lake):
    run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia())

    runs = ledger.query("select verdict, exit_code, ended from runs where job = 'eia' order by ended nulls first")
    assert [(r["verdict"], r["exit_code"]) for r in runs] == [(None, None), ("OK", 0)]
    measured = {r["name"]: r for r in ledger.query("select name, scope, value from measurements")}
    assert measured["eia_rows_fetched"]["scope"] == "electricity/region/1d:2026-09"
    assert measured["eia_rows_fetched"]["value"] == 40

    evidence = SourceEvidenceStore(lake).list_verified()
    assert len(evidence) == 1
    assert evidence[0].content_type == "application/gzip" and "api_key" not in evidence[0].source_url


def test_under_sync_runner_it_writes_measurements_under_the_parent_run_and_no_runs_row(lake, monkeypatch):
    monkeypatch.setenv("LW_RUN_ID", "intraday-catchup-20260923T100000Z-1")
    run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia())

    assert ledger.query("select * from runs") == []
    assert {r["run_id"] for r in ledger.query("select run_id from measurements")} == {
        "intraday-catchup-20260923T100000Z-1"
    }


def test_a_failed_window_is_named_the_others_publish_and_the_run_fails(lake):
    rows = copy.deepcopy(ROWS)
    rows[0]["period"] = "2026-08-31"  # one row in August so both windows have data

    rc = run(
        "--dataset",
        "electricity/region/1d",
        "--start",
        "2026-08-31",
        "--end",
        "2026-09-15",
        client=eia(rows, fail_start="2026-08-31"),
    )
    assert rc == 1
    assert (lake / REGION_2026_09).exists()
    assert not (lake / REGION_2026_09.replace("2026-09", "2026-08")).exists()
    failed = ledger.query("select scope from measurements where name = 'eia_fetch_failed'")
    assert [r["scope"] for r in failed] == ["electricity/region/1d:2026-08:503"]
    assert ledger.query("select verdict from runs where ended is not null")[0]["verdict"] == "FAILED"


def test_only_a_window_reaching_today_measures_staleness(lake):
    run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia())
    assert ledger.query("select * from measurements where name = 'eia_staleness_days'") == []

    run("--dataset", "electricity/region/1d", "--start", "2026-09-14", client=eia())
    (row,) = ledger.query("select scope, value from measurements where name = 'eia_staleness_days'")
    assert row["scope"] == "electricity/region/1d"
    assert row["value"] == (datetime.now(UTC).date() - date(2026, 9, 15)).days


def test_the_default_window_is_the_declared_lookback(lake, monkeypatch):
    monkeypatch.setenv("LW_DECLARED_EIA_LOOKBACK_DAYS", "14")
    seen: list[httpx.Request] = []
    run("--dataset", "electricity/region/1d", "--end", "2026-09-15", client=eia(seen=seen))
    assert (seen[0].url.params["start"], seen[0].url.params["end"]) == ("2026-09-01", "2026-09-15")


def test_a_row_without_a_key_field_fails_its_window_instead_of_publishing(lake):
    rows = copy.deepcopy(ROWS)
    del rows[3]["timezone"]
    assert (
        run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia(rows)) == 1
    )
    assert not (lake / REGION_2026_09).exists()


def test_windows_split_on_calendar_months_or_years_and_clip_to_the_range():
    monthly = lane.DATASETS["electricity/region/1d"]
    yearly = lane.DATASETS["petroleum/stocks/1w"]
    assert lane.windows(monthly, date(2026, 1, 30), date(2026, 3, 2)) == [
        (date(2026, 1, 30), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 28)),
        (date(2026, 3, 1), date(2026, 3, 2)),
    ]
    assert lane.windows(yearly, date(2025, 12, 20), date(2026, 1, 5)) == [
        (date(2025, 12, 20), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 1, 5)),
    ]


def test_a_backfill_never_starts_before_the_datasets_first_period(lake):
    seen: list[httpx.Request] = []
    run("--dataset", "electricity/region/1d", "--start", "2018-06-01", "--end", "2019-01-02", client=eia(seen=seen))
    assert seen[0].url.params["start"] == "2019-01-01"


def test_every_daily_grid_dataset_is_keyed_to_include_the_timezone():
    daily_grid = [d for d in lane.DATASETS.values() if d.product == "electricity" and d.frequency == "daily"]
    assert len(daily_grid) == 4
    assert all(d.keys[-1] == "timezone" for d in daily_grid)


def test_ctrl_c_mid_backfill_still_closes_the_run_and_commits_what_was_fetched(lake, monkeypatch):
    def interrupted(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(lane, "upsert", interrupted)
    with pytest.raises(KeyboardInterrupt):
        run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia())

    closed = ledger.query("select verdict from runs where ended is not null")
    assert [r["verdict"] for r in closed] == ["FAILED"]
    assert len(SourceEvidenceStore(lake).list_verified()) == 1


def test_an_unreadable_file_fails_that_window_only(lake):
    path = lake / REGION_2026_09
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not parquet")
    assert run("--dataset", "electricity/region/1d", "--start", "2026-09-14", "--end", "2026-09-15", client=eia()) == 1
    failed = ledger.query("select scope from measurements where name = 'eia_fetch_failed'")
    assert [r["scope"] for r in failed] == ["electricity/region/1d:2026-09:ArrowInvalid"]
    assert path.read_bytes() == b"not parquet"  # never overwritten blind


# --- series, nuclear, hourly -------------------------------------------------


def test_dataset_prefixes_select_a_product_or_one_dataset():
    assert {d.product for d in lane.select(["petroleum"])} == {"petroleum"}
    assert [d.id for d in lane.select(["natural_gas/storage"])] == ["natural_gas/storage/1w"]
    assert len(lane.select(None)) == len(lane.DATASETS)
    with pytest.raises(SystemExit):
        lane.select(["coal"])


def test_a_weekly_inventory_series_lands_in_its_year_file_with_the_product_facet_renamed(lake):
    row = ONE_ROW["petroleum/stoc/wstk:weekly"]
    assert run("--dataset", "petroleum/stocks", "--start", "2026-08-15", "--end", "2026-08-31", client=eia([row])) == 0

    table = read(lake, f"{ENERGY}/product=petroleum/dataset=stocks/year=2026/1w.parquet")
    assert "product" not in table.schema.names  # would collide with the product= partition
    (stored,) = table.to_pylist()
    assert stored["series"] == "W_EPOBGRR_SAE_NUS_MBBL"
    assert (stored["period"], stored["value"], stored["units"]) == (date(2026, 8, 21), 38902.0, "MBBL")
    assert (stored["product_code"], stored["process"], stored["process_name"]) == ("EPOBGRR", "SAE", "Ending Stocks")


def test_henry_hub_is_fetched_by_its_series_facet(lake):
    seen: list[httpx.Request] = []
    row = ONE_ROW["natural-gas/pri/fut:daily"]
    run(
        "--dataset",
        "natural_gas/spot_price",
        "--start",
        "2026-08-01",
        "--end",
        "2026-08-05",
        client=eia([row], seen=seen),
    )

    assert seen[0].url.params.get_list("facets[series][]") == ["RNGWHHD"]
    (stored,) = read(lake, f"{ENERGY}/product=natural_gas/dataset=spot_price/year=2026/1d.parquet").to_pylist()
    assert (stored["value"], stored["units"]) == (2.81, "$/MMBTU")


def test_nuclear_outages_keep_all_three_measures_with_their_units(lake):
    row = ONE_ROW["nuclear-outages/generator-nuclear-outages:daily"]
    seen: list[httpx.Request] = []
    run(
        "--dataset",
        "nuclear/outages_generator",
        "--start",
        "2026-09-22",
        "--end",
        "2026-09-22",
        client=eia([row], seen=seen),
    )

    assert seen[0].url.params.get_list("data[]") == ["capacity", "outage", "percentOutage"]
    (stored,) = read(lake, f"{ENERGY}/product=nuclear/dataset=outages_generator/year=2026/1d.parquet").to_pylist()
    assert (stored["facility"], stored["generator"], stored["facility_name"]) == ("46", "1", "Browns Ferry")
    assert (stored["capacity"], stored["outage"], stored["percent_outage"]) == (1227.4, 1227.4, 100.0)
    assert (stored["capacity_units"], stored["percent_outage_units"]) == ("megawatts", "percent")


def test_hourly_electricity_is_utc_timestamped_in_a_1h_file_beside_the_daily_one(lake):
    row = ONE_ROW["electricity/rto/region-data:hourly"]
    seen: list[httpx.Request] = []
    run(
        "--dataset",
        "electricity/region/1h",
        "--start",
        "2026-09-24",
        "--end",
        "2026-09-24",
        client=eia([row], seen=seen),
    )

    assert (seen[0].url.params["start"], seen[0].url.params["end"]) == ("2026-09-24T00", "2026-09-24T23")
    (stored,) = read(lake, f"{ENERGY}/product=electricity/dataset=region/month=2026-09/1h.parquet").to_pylist()
    assert stored["period"] == datetime(2026, 9, 24, 4, tzinfo=UTC)
    assert (stored["respondent"], stored["type"], stored["value"]) == ("FLA", "DF", 2632.0)


# --- bulk files ----------------------------------------------------------------

# Real series lines from the bulk zips (downloaded on the mini 2026-09-23), each cut to a few points.
BULK = json.loads((Path(__file__).parent / "fixtures/eia/bulk-lines-2026-09-23.json").read_text())
MANIFEST = {
    "dataset": {
        "PET": {"last_updated": "2026-09-22T14:07:41-04:00"},
        "NG": {"last_updated": "2026-09-22T14:07:41-04:00"},
        "TOTAL": {"last_updated": "2026-08-26T13:49:00-04:00"},
        "STEO": {"last_updated": "2026-09-09T16:03:00-04:00"},
        "EBA": {"last_updated": "2026-09-22T15:15:33-04:00"},
    }
}


@pytest.mark.parametrize(
    ("series_id", "expected"),
    [
        ("EBA.PJM-ALL.D.H", ("region", {"respondent": "PJM", "type": "D"})),
        ("EBA.CENT-ALL.NG.COL.H", ("fuel_type", {"respondent": "CENT", "fueltype": "COL"})),
        ("EBA.CISO-PGAE.D.H", ("sub_ba", {"parent": "CISO", "subba": "PGAE"})),
        ("EBA.TEC-FPC.ID.H", ("interchange", {"fromba": "TEC", "toba": "FPC"})),
        ("EBA.PJM-ALL.D.HL", None),  # local-time twin of the UTC series
        ("EBA.BHBA.CO2.CER.H", None),  # emissions: no API route
    ],
)
def test_bulk_series_ids_map_to_the_api_datasets_keys(series_id, expected):
    assert lane.bulk_series(series_id) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("20260915", date(2026, 9, 15)),
        ("202606", date(2026, 6, 1)),
        ("2026Q3", date(2026, 7, 1)),
        ("2025", date(2025, 1, 1)),
    ],
)
def test_bulk_periods_become_the_periods_first_day(raw, expected):
    assert lane.bulk_period(raw) == expected


def _zip(family: str, *, extra_lines=()) -> bytes:
    import io

    lines = [{"category_id": "1", "name": "a category line", "childseries": []}, *BULK[family], *extra_lines]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        body = "\n".join(json.dumps(line) for line in lines)
        archive.writestr(f"{family}.txt", ("discontinued\n" if family == "TOTAL" else "") + body + "\n")
    return buffer.getvalue()


def web(zips: dict[str, bytes], manifest=MANIFEST, seen=None) -> httpx.Client:
    """EIA's bulk host: the manifest and the zips; a family missing from `zips` answers 503."""

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(str(request.url))
        if request.url.path.endswith("manifest.txt"):
            return httpx.Response(200, json=manifest, request=request)
        code = request.url.path.rsplit("/", 1)[-1].removesuffix(".zip")
        if code not in zips:
            return httpx.Response(503, request=request)
        return httpx.Response(200, content=zips[code], request=request)

    return httpx.Client(transport=httpx.MockTransport(handler))


def bulk_run(*argv, client=None, http):
    return lane.run(list(argv), client=client or eia([]), http=http)


def test_a_bulk_family_publishes_by_year_with_markers_kept_and_its_series_metadata(lake):
    assert bulk_run("--bulk", "PET", "TOTAL", http=web({"PET": _zip("PET"), "TOTAL": _zip("TOTAL")})) == 0

    pet = f"{ENERGY}/product=petroleum/dataset=pet"
    monthly = read(lake, f"{pet}/year=2026/1mo.parquet").to_pylist()
    assert [(r["period"], r["value"]) for r in monthly] == [
        (date(2026, 4, 1), 13981.0),
        (date(2026, 5, 1), 13755.0),
        (date(2026, 6, 1), 13792.0),
    ]
    assert read(lake, f"{pet}/year=2025/1y.parquet").to_pylist()[0]["value"] == 13662.0
    assert not list((lake / pet).glob("year=*/1d.parquet"))  # PET daily comes from the API datasets
    (meta,) = read(lake, f"{pet}/series.parquet").to_pylist()[:1]
    assert meta["units"] == "Thousand Barrels per Day"

    (marked,) = read(lake, f"{ENERGY}/product=total_energy/dataset=total/year=1982/1mo.parquet").to_pylist()
    assert (marked["value"], marked["value_flag"]) == (None, "NA")  # not a number, not zero, not lost


def test_a_forecast_family_keeps_every_release_whole(lake):
    bulk_run("--bulk", "STEO", http=web({"STEO": _zip("STEO")}))
    later = {"dataset": {"STEO": {"last_updated": "2026-10-07T16:00:00-04:00"}}}
    bulk_run("--bulk", "STEO", http=web({"STEO": _zip("STEO")}, manifest=later))

    steo = lake / ENERGY / "product=steo/dataset=steo"
    assert sorted(p.parent.name for p in steo.glob("vintage=*/1mo.parquet")) == [
        "vintage=2026-09-09",
        "vintage=2026-10-07",
    ]


def test_a_duplicated_period_is_published_once(lake):
    bulk_run("--bulk", "NG", http=web({"NG": _zip("NG")}))
    rows = read(lake, f"{ENERGY}/product=natural_gas/dataset=ng/year=2025/1mo.parquet").to_pylist()
    assert [(r["period"], r["value"]) for r in rows] == [(date(2025, 3, 1), 486.0), (date(2025, 4, 1), 475.0)]


def test_a_bulk_import_is_a_ledger_evidence_row_and_the_zip_is_kept_under_raw(lake):
    bulk_run("--bulk", "PET", http=web({"PET": _zip("PET")}))

    (row,) = ledger.query("select subject, payload_json, source_url from evidence where kind = 'eia_bulk'")
    payload = json.loads(row["payload_json"])
    assert row["subject"] == "PET" and row["source_url"] == "https://www.eia.gov/opendata/bulk/PET.zip"
    assert payload["last_updated"] == "2026-09-22T14:07:41-04:00"
    assert payload["path"] == "raw/eia/bulk/PET/2026-09-22T140741.zip"
    assert (lake / payload["path"]).read_bytes() == _zip("PET")


def test_a_reimport_counts_every_value_it_changes(lake):
    bulk_run("--bulk", "PET", http=web({"PET": _zip("PET")}))
    revised = copy.deepcopy(BULK["PET"])
    monthly = next(s for s in revised if s["series_id"] == "PET.MCRFPUS2.M")
    monthly["data"][0][1] = 13800  # EIA revised June
    later = {"dataset": {"PET": {"last_updated": "2026-09-29T14:00:00-04:00"}}}
    original, BULK["PET"] = BULK["PET"], revised
    try:
        bulk_run("--bulk", "PET", http=web({"PET": _zip("PET")}, manifest=later))
    finally:
        BULK["PET"] = original

    counted = ledger.query("select scope, value from measurements where name = 'eia_values_revised'")
    assert [(r["scope"], r["value"]) for r in counted] == [("petroleum/pet/1mo:2026", 1.0)]


def test_the_scheduled_run_imports_only_families_that_moved_and_are_due(lake, monkeypatch):
    monkeypatch.setenv("LW_DECLARED_EIA_BULK_REFRESH_DAYS", "7")
    seen: list[str] = []
    bulk_run("--bulk", "PET", http=web({"PET": _zip("PET")}))  # PET is current with the manifest
    only_ng = {"dataset": {"PET": MANIFEST["dataset"]["PET"], "NG": MANIFEST["dataset"]["NG"]}}

    assert bulk_run("--dataset", "nuclear/outages_us", http=web({}, manifest=only_ng, seen=seen)) == 0
    assert not any(url.endswith(".zip") for url in seen)  # --dataset alone never touches bulk

    rc = lane.run([], client=eia([]), http=web({"NG": _zip("NG")}, manifest=only_ng, seen=seen))
    assert rc == 0
    assert [url.rsplit("/", 1)[-1] for url in seen if url.endswith(".zip")] == ["NG.zip"]
    behind = {
        r["scope"]: r["value"]
        for r in ledger.query("select scope, value from measurements where name = 'eia_bulk_behind_days'")
    }
    assert behind == {"PET": 0.0, "NG": 0.0}


def test_a_moved_family_waits_for_its_refresh_interval():
    imported = datetime(2026, 9, 20, tzinfo=UTC)
    previous = {"PET": {"last_updated": "2026-09-15T14:00:00-04:00", "fetched_at": imported}}
    manifest = {"PET": {"last_updated": "2026-09-22T14:07:41-04:00"}}
    assert lane.due_families(manifest, previous, imported + timedelta(days=6)) == []
    assert lane.due_families(manifest, previous, imported + timedelta(days=7)) == ["PET"]


def test_a_failed_bulk_download_is_named_writes_no_evidence_and_stays_due(lake):
    assert bulk_run("--bulk", "PET", http=web({})) == 1
    failed = ledger.query("select scope from measurements where name = 'eia_fetch_failed'")
    assert [r["scope"] for r in failed] == ["bulk/PET:503"]
    assert ledger.query("select * from evidence") == []


def test_eba_publishes_hourly_by_month_names_from_the_api_then_fills_facets_the_bulk_lacks(lake):
    api_only = copy.deepcopy(ONE_ROW["electricity/rto/region-data:hourly"])  # FLA: not in the bulk file
    names = {
        "respondent": {"PJM": "PJM Interconnection, LLC", "FLA": "Florida"},
        "type": {"D": "Demand", "DF": "Day-ahead demand forecast"},
    }
    seen: list[httpx.Request] = []
    client = eia([api_only], seen=seen, facet_names=names, route="electricity/rto/region-data")

    assert bulk_run("--bulk", "EBA", client=client, http=web({"EBA": _zip("EBA")})) == 0

    month = read(lake, f"{ENERGY}/product=electricity/dataset=region/month=2026-09/1h.parquet").to_pylist()
    pjm = [r for r in month if r["respondent"] == "PJM"]
    assert [r["period"] for r in pjm] == [datetime(2026, 9, 22, h, tzinfo=UTC) for h in (17, 18, 19)]
    assert (pjm[-1]["value"], pjm[-1]["respondent_name"], pjm[-1]["type_name"], pjm[-1]["source"]) == (
        94038.0,
        "PJM Interconnection, LLC",
        "Demand",
        "eia_bulk",
    )
    assert any(r["respondent"] == "FLA" and r["source"] == "eia" for r in month)
    fills = [r.url.params for r in seen if "/data/" in r.url.path and "region-data" in r.url.path]
    assert fills and all(
        p.get_list("facets[respondent][]") == ["FLA"] or p.get_list("facets[type][]") == ["DF"] for p in fills
    )
    sub_ba = read(lake, f"{ENERGY}/product=electricity/dataset=sub_ba/month=2026-09/1h.parquet").to_pylist()
    assert {(r["subba"], r["parent"]) for r in sub_ba} == {("PGAE", "CISO")}
