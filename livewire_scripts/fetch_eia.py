#!/usr/bin/env python3
"""Fetch EIA energy data into bronze parquet: the API for daily/weekly/hourly, bulk files for the rest.

Layout: bronze/asset_class=energy/product=<p>/dataset=<d>/<month=YYYY-MM|year=YYYY|vintage=YYYY-MM-DD>/<tf>.parquet

Two channels, one lake:

- API datasets (`DATASETS`): petroleum and natural-gas daily/weekly series,
  nuclear outages, grid-monitor electricity daily + hourly. The scheduled run
  re-fetches the last `eia_lookback_days` of each; a backfill is `--start`.
- Bulk families (`BULK_FAMILIES`, `EBA`): every monthly/quarterly/annual series
  EIA publishes, plus hourly electricity history (EBA.zip: one download against
  ~15,000 API pages). The scheduled run re-imports a family when EIA's
  manifest says it changed and the last import (a ledger `evidence` row) is
  older than its refresh interval.

Rows are keyed by (period, *facets) and upserted: the incoming row wins, no row
is ever deleted, and every value an upsert replaces with a different one is
counted as `eia_values_revised` — the measurement that says whether EIA revises
history beyond the lookback, and how far the two channels disagree.
`BronzeClient` is not used: it keys on `trade_date` alone.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import zipfile
from array import array
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients import ledger
from clients.constants import declared
from clients.eia_client import EiaClient, EiaPage
from clients.parquet_io import publish_parquet, symbol_lock
from clients.source_evidence import SourceEvidence, SourceEvidenceStore, sha256_file
from livewire_scripts.paths import data_lake_dir

JOB = "eia"
MANIFEST_URL = "https://api.eia.gov/bulk/manifest.txt"
TIMEFRAME = {"hourly": "1h", "daily": "1d", "weekly": "1w"}
BULK_TIMEFRAME = {"H": "1h", "D": "1d", "W": "1w", "4": "4w", "M": "1mo", "Q": "1q", "A": "1y"}

Measure = Callable[[str, str, float, str], None]

# Series routes (petroleum, natural gas) share one row shape. EIA's `product`
# facet is renamed: `product` is also the lake's hive partition key, and a
# reader with hive partitioning on would get two columns of that name.
SERIES_NAMES = {
    "series-description": "series_description",
    "duoarea": "duoarea",
    "area-name": "area_name",
    "product": "product_code",
    "product-name": "product_name",
    "process": "process",
    "process-name": "process_name",
}
NUCLEAR_VALUES = {"capacity": "capacity", "outage": "outage", "percentOutage": "percent_outage"}


@dataclass(frozen=True)
class Dataset:
    product: str
    name: str
    route: str
    frequency: str
    keys: tuple[str, ...]
    names: dict[str, str]  # EIA label field -> column
    earliest: date  # first period EIA serves (probed 2026-09-23)
    # Newest row older than this many days is WARN in `status`. Provisional: one
    # observation (2026-09-23) plus EIA's weekly release days, not yet measured
    # over several releases. None = not graded (seasonal).
    max_lag_days: int | None
    partition: str = "year"
    values: dict[str, str] = field(default_factory=lambda: {"value": "value"})  # data column -> column
    facets: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return f"{self.product}/{self.name}/{TIMEFRAME[self.frequency]}"


def _series(product: str, name: str, route: str, frequency: str, earliest: date, lag: int | None, **kw) -> Dataset:
    return Dataset(product, name, route, frequency, ("series",), SERIES_NAMES, earliest, lag, **kw)


def _grid(name: str, route: str, frequency: str, keys: tuple[str, ...], names: dict[str, str], lag: int) -> Dataset:
    return Dataset("electricity", name, route, frequency, keys, names, date(2019, 1, 1), lag, partition="month")


def _nuclear(name: str, route: str, keys: tuple[str, ...], names: dict[str, str]) -> Dataset:
    return Dataset("nuclear", name, route, "daily", keys, names, date(2007, 1, 1), 4, values=NUCLEAR_VALUES)


_REGION = {"respondent-name": "respondent_name", "type-name": "type_name"}
_FUEL = {"respondent-name": "respondent_name", "type-name": "fueltype_name"}
_SUB_BA = {"subba-name": "subba_name", "parent-name": "parent_name"}
_INTERCHANGE = {"fromba-name": "fromba_name", "toba-name": "toba_name"}
_FACILITY = {"facilityName": "facility_name"}

DATASETS = {
    d.id: d
    for d in (
        # Spot prices are published weekly (Wednesday) for the prior week: 8 days behind on 2026-09-23.
        _series("petroleum", "spot_price", "petroleum/pri/spt", "daily", date(1986, 1, 1), 14),
        _series("petroleum", "retail_price", "petroleum/pri/gnd", "weekly", date(1990, 8, 20), 10),
        # The full Weekly Petroleum Status Report; a superset of the routes below
        # except `stocks`, which is fetched on its own so inventory has a directory.
        # Week ending Friday, released the next Wednesday: 12 days behind just before a release.
        _series("petroleum", "weekly_supply", "petroleum/sum/sndw", "weekly", date(1982, 8, 20), 14),
        _series("petroleum", "stocks", "petroleum/stoc/wstk", "weekly", date(1982, 8, 20), 14),
        _series("petroleum", "refiner_production", "petroleum/pnp/wprodr", "weekly", date(2010, 6, 4), 14),
        _series("petroleum", "blender_production", "petroleum/pnp/wprodb", "weekly", date(2010, 6, 4), 14),
        _series("petroleum", "imports_by_country", "petroleum/move/wimpc", "weekly", date(2010, 6, 4), 14),
        # Ended 2026-03-30; looks like an October-March survey (unverified), so not graded.
        _series("petroleum", "heating_oil_propane", "petroleum/pri/wfr", "weekly", date(1990, 10, 1), None),
        # natural-gas/pri/fut also holds NYMEX futures that ended 2024-04-05; futures come from IB.
        _series(
            "natural_gas",
            "spot_price",
            "natural-gas/pri/fut",
            "daily",
            date(1997, 1, 1),
            14,
            facets={"series": ("RNGWHHD",)},
        ),
        # Week ending Friday, released the next Thursday.
        _series("natural_gas", "storage", "natural-gas/stor/wkly", "weekly", date(2010, 1, 1), 14),
        _nuclear("outages_us", "nuclear-outages/us-nuclear-outages", (), {}),
        _nuclear("outages_facility", "nuclear-outages/facility-nuclear-outages", ("facility",), _FACILITY),
        _nuclear(
            "outages_generator", "nuclear-outages/generator-nuclear-outages", ("facility", "generator"), _FACILITY
        ),
        # Daily grid data: 1-3 days behind on 2026-09-23 (endPeriod 09-20..09-22).
        _grid("region", "electricity/rto/daily-region-data", "daily", ("respondent", "type", "timezone"), _REGION, 5),
        _grid(
            "fuel_type",
            "electricity/rto/daily-fuel-type-data",
            "daily",
            ("respondent", "fueltype", "timezone"),
            _FUEL,
            5,
        ),
        _grid(
            "sub_ba", "electricity/rto/daily-region-sub-ba-data", "daily", ("subba", "parent", "timezone"), _SUB_BA, 5
        ),
        _grid(
            "interchange",
            "electricity/rto/daily-interchange-data",
            "daily",
            ("fromba", "toba", "timezone"),
            _INTERCHANGE,
            5,
        ),
        # Hourly periods are UTC; there is no timezone facet.
        _grid("region", "electricity/rto/region-data", "hourly", ("respondent", "type"), _REGION, 3),
        _grid("fuel_type", "electricity/rto/fuel-type-data", "hourly", ("respondent", "fueltype"), _FUEL, 3),
        _grid("sub_ba", "electricity/rto/region-sub-ba-data", "hourly", ("subba", "parent"), _SUB_BA, 3),
        _grid("interchange", "electricity/rto/interchange-data", "hourly", ("fromba", "toba"), _INTERCHANGE, 3),
    )
}


@dataclass(frozen=True)
class BulkFamily:
    """One EIA bulk zip (`https://www.eia.gov/opendata/bulk/<code>.zip`) of series-per-line JSON."""

    code: str
    product: str
    # Frequencies to import; None = every one in the file. PET and NG leave
    # daily/weekly to the API datasets, which carry the facet columns.
    frequencies: tuple[str, ...] | None = None
    # A forecast is rewritten by every release, so an upsert by (period, series)
    # would erase the previous forecast; each release is kept whole instead.
    vintage: bool = False

    @property
    def dataset(self) -> str:
        return self.code.lower()


BULK_FAMILIES = {
    f.code: f
    for f in (
        BulkFamily("PET", "petroleum", ("M", "Q", "A", "4")),
        BulkFamily("PET_IMPORTS", "petroleum"),
        BulkFamily("NG", "natural_gas", ("M", "Q", "A")),
        BulkFamily("ELEC", "electricity"),
        BulkFamily("COAL", "coal"),
        BulkFamily("TOTAL", "total_energy"),
        BulkFamily("SEDS", "state_energy"),
        BulkFamily("INTL", "international"),
        BulkFamily("EMISS", "emissions"),  # discontinued upstream (file dated 2025-06-24); history only
        BulkFamily("STEO", "steo", vintage=True),
    )
}
EBA = "EBA"  # hourly grid monitor: mapped onto the four hourly API datasets, not a family of its own
# AEO.* and IEO.* (long-range outlooks, one frozen zip per release year) are not imported.


def _units_column(dataset: Dataset, column: str) -> str:
    return "units" if len(dataset.values) == 1 else f"{column}_units"


def schema_for(dataset: Dataset) -> pa.Schema:
    period = pa.timestamp("s", tz="UTC") if dataset.frequency == "hourly" else pa.date32()
    fields = [("period", period)]
    fields += [(key, pa.string()) for key in dataset.keys]
    fields += [(column, pa.string()) for column in dataset.names.values()]
    for column in dataset.values.values():
        fields += [(column, pa.float64()), (_units_column(dataset, column), pa.string())]
    return pa.schema([*fields, ("source", pa.string())])


BULK_SCHEMA = pa.schema(
    [
        ("period", pa.date32()),
        ("series", pa.string()),
        ("value", pa.float64()),
        ("value_flag", pa.string()),  # EIA's marker when the value is not a number: NA, W, --, ie, ...
        ("source", pa.string()),
    ]
)
SERIES_META = ("name", "units", "f", "description", "geography", "start", "end", "last_updated")
SERIES_SCHEMA = pa.schema([("series", pa.string())] + [(column, pa.string()) for column in SERIES_META])


def parse_period(dataset: Dataset, raw: str) -> date | datetime:
    if dataset.frequency == "hourly":
        return datetime.strptime(raw, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    return date.fromisoformat(raw)


def normalize(dataset: Dataset, raw: dict) -> dict:
    """One EIA row -> one bronze row. A missing key field raises: it cannot be keyed."""
    row: dict = {"period": parse_period(dataset, raw["period"])}
    for key in dataset.keys:
        if raw.get(key) in (None, ""):
            raise ValueError(f"{dataset.route}: row without {key!r}: {raw}")
        row[key] = str(raw[key])
    for label, column in dataset.names.items():
        row[column] = raw.get(label)
    for field_name, column in dataset.values.items():
        # EIA sends numbers as strings on most routes; null stays null, never 0.
        row[column] = None if raw.get(field_name) is None else float(raw[field_name])
        row[_units_column(dataset, column)] = raw.get(f"{field_name}-units", raw.get("units"))
    row["source"] = "eia"
    return row


def windows(dataset: Dataset, start: date, end: date) -> list[tuple[date, date]]:
    """Calendar months (or years) intersected with [start, end]; one window = one file."""
    out = []
    cursor = start
    while cursor <= end:
        if dataset.partition == "month":
            following = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
        else:
            following = date(cursor.year + 1, 1, 1)
        out.append((cursor, min(end, following - timedelta(days=1))))
        cursor = following
    return out


def partition_path(root: Path, dataset: Dataset, window_start: date) -> Path:
    period = f"month={window_start:%Y-%m}" if dataset.partition == "month" else f"year={window_start:%Y}"
    return (
        root
        / f"product={dataset.product}"
        / f"dataset={dataset.name}"
        / period
        / f"{TIMEFRAME[dataset.frequency]}.parquet"
    )


def _changed(new: pa.Table, old: pa.Table, keys: Sequence[str], values: Sequence[str]) -> int:
    """How many incoming rows replace a stored row of the same key with a different value (null-aware)."""
    joined = new.select([*keys, *values]).join(
        old.select([*keys, *values]), keys=list(keys), join_type="inner", right_suffix="__old"
    )
    changed = pa.array([False] * joined.num_rows, pa.bool_())
    for column in values:
        a, b = joined[column], joined[f"{column}__old"]
        differs = pc.fill_null(pc.not_equal(a, b), False)
        one_null = pc.xor(pc.is_null(a), pc.is_null(b))
        changed = pc.or_(changed, pc.or_(differs, one_null))
    return int(pc.sum(changed).as_py() or 0)


def upsert(path: Path, table: pa.Table, keys: Sequence[str], values: Sequence[str]) -> tuple[int, int]:
    """Merge `table` into the file at `path` by `keys`; an incoming row wins, no row is removed.

    Returns (rows in the file, stored values replaced by a different value).
    """
    with symbol_lock(path):
        revised = 0
        if path.exists():
            old = pq.ParquetFile(path).read().select(table.schema.names).cast(table.schema)
            revised = _changed(table, old, keys, values)
            table = pa.concat_tables([old, table])
        numbered = table.append_column("__row", pa.array(range(table.num_rows), pa.int64()))
        newest = numbered.group_by(list(keys), use_threads=False).aggregate([("__row", "max")])
        merged = table.take(newest["__row_max"]).sort_by([(key, "ascending") for key in keys])
        publish_parquet(path, merged, tuple(keys))
    return merged.num_rows, revised


def _query_bounds(dataset: Dataset, start: date, end: date) -> tuple[str, str]:
    if dataset.frequency == "hourly":
        return f"{start.isoformat()}T00", f"{end.isoformat()}T23"
    return start.isoformat(), end.isoformat()


# --- bulk files --------------------------------------------------------------


def bulk_url(code: str) -> str:
    return f"https://www.eia.gov/opendata/bulk/{code}.zip"


def _series_lines(zip_path: Path):
    """Each series object of a bulk file; category lines and non-JSON lines (EMISS opens with `discontinued`) skipped."""
    with zipfile.ZipFile(zip_path) as archive, archive.open(archive.namelist()[0]) as lines:
        for line in lines:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and "series_id" in record:
                yield record


def bulk_series(series_id: str) -> tuple[str, dict[str, str]] | None:
    """Map an EBA bulk series id to (dataset name, key values); None for series with no API twin.

    Only UTC series (`.H`) are read; `.HL` repeats them in local time. Checked
    against the API on 2026-09-14..15: every bulk key exists in the API, names and units agree.
    """
    parts = series_id.split(".")
    if parts[0] != "EBA" or parts[-1] != "H":
        return None
    body = parts[1:-1]
    area, _, other = body[0].partition("-")
    if not other:
        return None  # e.g. EBA.BHBA.CO2.CER.H: emissions, no API route
    if len(body) == 2 and other == "ALL" and body[1] in ("D", "DF", "NG", "TI"):
        return "region", {"respondent": area, "type": body[1]}
    if len(body) == 3 and other == "ALL" and body[1] == "NG":
        return "fuel_type", {"respondent": area, "fueltype": body[2]}
    if len(body) == 2 and body[1] == "D":
        return "sub_ba", {"parent": area, "subba": other}
    if len(body) == 2 and body[1] == "ID":
        return "interchange", {"fromba": area, "toba": other}
    return None


def eba_table(zip_path: Path, dataset: Dataset, labels: dict[str, dict[str, str]]) -> pa.Table:
    """Every UTC point of hourly `dataset` in EBA.zip, in `schema_for(dataset)`.

    `labels[facet][id]` fills the name columns the bulk file does not carry.
    """
    schema = schema_for(dataset)
    tables = []
    for record in _series_lines(zip_path):
        mapped = bulk_series(record["series_id"])
        if mapped is None or mapped[0] != dataset.name or not record.get("data"):
            continue
        keys = mapped[1]
        periods, values = zip(*record["data"], strict=True)
        n = len(periods)
        period = pc.strptime(pa.array(periods, pa.string()), format="%Y%m%dT%H", unit="s")
        columns = {"period": period.cast(schema.field("period").type)}
        columns |= {key: pa.array([keys[key]] * n, pa.string()) for key in dataset.keys}
        for column in dataset.names.values():
            facet = column.removesuffix("_name")
            columns[column] = pa.array([labels.get(facet, {}).get(keys[facet])] * n, pa.string())
        columns["value"] = pa.array([None if v is None else float(v) for v in values], pa.float64())
        # EIA's API labels every grid-monitor value megawatthours; the bulk file
        # says "MWh" on some series. Same unit, one spelling.
        columns["units"] = pa.array(["megawatthours"] * n, pa.string())
        columns["source"] = pa.array(["eia_bulk"] * n, pa.string())
        tables.append(pa.table(columns, schema=schema))
    return pa.concat_tables(tables) if tables else schema.empty_table()


_EPOCH = date(1970, 1, 1)


def bulk_period(raw: str) -> date:
    """EIA bulk period text -> the period's first day: YYYYMMDD, YYYYMM, YYYYQn, YYYY."""
    if "Q" in raw:
        year, quarter = raw.split("Q")
        return date(int(year), 3 * int(quarter) - 2, 1)
    if len(raw) == 8:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    if len(raw) == 6:
        return date(int(raw[:4]), int(raw[4:]), 1)
    if len(raw) == 4:
        return date(int(raw), 1, 1)
    raise ValueError(f"unrecognised EIA bulk period {raw!r}")


@dataclass
class _Points:
    """One frequency's points, columnar and compact (ELEC alone is 60M points)."""

    days: array = field(default_factory=lambda: array("i"))
    codes: array = field(default_factory=lambda: array("i"))
    values: array = field(default_factory=lambda: array("d"))
    flags: dict[int, str] = field(default_factory=dict)

    def table(self) -> tuple[pa.Table, pa.Array]:
        n = len(self.days)
        period = pa.Array.from_buffers(pa.date32(), n, [None, pa.py_buffer(self.days)])
        raw = pa.Array.from_buffers(pa.float64(), n, [None, pa.py_buffer(self.values)])
        value = pc.if_else(pc.is_nan(raw), pa.scalar(None, pa.float64()), raw)
        if self.flags:
            flag = pa.array([self.flags.get(i) for i in range(n)], pa.string())
        else:
            flag = pa.nulls(n, pa.string())
        codes = pa.Array.from_buffers(pa.int32(), n, [None, pa.py_buffer(self.codes)])
        table = pa.table({"period": period, "value": value, "value_flag": flag})
        return table, codes


def read_family(zip_path: Path, family: BulkFamily) -> tuple[dict[str, _Points], list[str], pa.Table, Counter]:
    """Points by frequency, the series id list the codes index, series metadata, and counters."""
    points: dict[str, _Points] = {}
    series_ids: list[str] = []
    meta: list[dict] = []
    counts: Counter = Counter()
    period_days: dict[str, int] = {}
    for record in _series_lines(zip_path):
        frequency = record.get("f")
        if family.frequencies is not None and frequency not in family.frequencies:
            continue
        code = len(series_ids)
        series_ids.append(record["series_id"])
        meta.append({"series": record["series_id"]} | {c: _text(record.get(c)) for c in SERIES_META})
        bucket = points.setdefault(frequency, _Points())
        seen: dict[str, object] = {}
        for raw_period, raw_value in record.get("data") or []:
            if raw_period in seen:
                # The same period listed twice (PET 30, NG 6 on 2026-09-23, all equal).
                counts["duplicate_conflicts"] += seen[raw_period] != raw_value
                continue
            seen[raw_period] = raw_value
            days = period_days.get(raw_period)
            if days is None:
                days = period_days[raw_period] = (bulk_period(raw_period) - _EPOCH).days
            bucket.days.append(days)
            bucket.codes.append(code)
            if isinstance(raw_value, int | float):
                bucket.values.append(float(raw_value))
            else:
                try:
                    bucket.values.append(float("nan") if raw_value is None else float(raw_value))
                except ValueError:
                    bucket.flags[len(bucket.values)] = str(raw_value)
                    bucket.values.append(float("nan"))
                    counts["flagged"] += 1
            counts["points"] += 1
    return points, series_ids, pa.Table.from_pylist(meta, schema=SERIES_SCHEMA), counts


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def family_root(root: Path, family: BulkFamily) -> Path:
    return root / f"product={family.product}" / f"dataset={family.dataset}"


def import_family(
    zip_path: Path, family: BulkFamily, root: Path, vintage: str, measure: Measure, failures: list[str]
) -> dict:
    """Publish one bulk family; returns a summary for its ledger evidence row."""
    points, series_ids, meta, counts = read_family(zip_path, family)
    dictionary = pa.array(series_ids, pa.string())
    base = family_root(root, family)
    upsert(base / "series.parquet", meta, ("series",), SERIES_META)
    summary: dict = {"series": len(series_ids), **counts, "rows": {}, "revised": 0}
    for frequency in sorted(points):
        tf = BULK_TIMEFRAME[frequency]
        table, codes = points.pop(frequency).table()  # popped: ELEC's monthly points alone are 42M
        table = table.append_column("code", codes)
        if family.vintage:
            parts = [(f"vintage={vintage}", table)]
        else:
            years = pc.year(table["period"])
            parts = [
                (f"year={year}", table.filter(pc.equal(years, year))) for year in sorted(pc.unique(years).to_pylist())
            ]
        del table
        for partition, part in parts:
            # Series ids are materialised one partition at a time: as one column they
            # were the peak (2 GB for PET's 15.6M points, measured on the mini).
            part = part.append_column("series", dictionary.take(part["code"])).append_column(
                "source", pa.array(["eia_bulk"] * part.num_rows, pa.string())
            )
            part = part.select(BULK_SCHEMA.names).cast(BULK_SCHEMA)
            scope = f"{family.product}/{family.dataset}/{tf}:{partition.split('=')[1]}"
            try:
                file_rows, revised = upsert(base / partition / f"{tf}.parquet", part, ("period", "series"), ("value",))
            except Exception as exc:
                failures.append(f"{scope}:{type(exc).__name__}")
                measure("eia_fetch_failed", f"{scope}:{type(exc).__name__}", 1, "count")
                print(f"  {scope}: FAILED {exc}", flush=True)
                continue
            summary["rows"][tf] = summary["rows"].get(tf, 0) + part.num_rows
            summary["revised"] += revised
            measure("eia_rows_fetched", scope, part.num_rows, "rows")
            if revised:
                measure("eia_values_revised", scope, revised, "rows")
        print(f"  {family.code} {tf}: {summary['rows'].get(tf, 0)} rows", flush=True)
    if counts["duplicate_conflicts"]:
        measure("eia_bulk_duplicate_conflicts", family.code, counts["duplicate_conflicts"], "count")
    return summary


def import_eba(
    zip_path: Path, datasets: list[Dataset], client: EiaClient, root: Path, measure: Measure, failures: list[str]
) -> tuple[dict, list[tuple[Dataset, dict[str, tuple[str, ...]]]]]:
    """Publish hourly electricity from EBA.zip, month by month.

    Returns a summary and, per dataset, the facet values the API lists but the
    bulk file lacks (on 2026-09-23: BA SWPW, the battery and pumped-storage fuel
    types, several sub-BAs) — the caller fetches those from the API.
    """
    missing = []
    summary: dict = {"rows": {}, "revised": 0}
    for dataset in datasets:
        labels = {
            facet: client.facet_names(dataset.route, facet)
            for facet in (column.removesuffix("_name") for column in dataset.names.values())
        }
        table = eba_table(zip_path, dataset, labels)
        months = pc.strftime(table["period"], format="%Y-%m")
        print(f"  {dataset.id}: {table.num_rows} bulk rows", flush=True)
        for month in sorted(pc.unique(months).to_pylist()):
            scope = f"{dataset.id}:{month}"
            path = partition_path(root, dataset, date.fromisoformat(f"{month}-01"))
            try:
                part = table.filter(pc.equal(months, month))
                file_rows, revised = upsert(path, part, ("period", *dataset.keys), tuple(dataset.values.values()))
            except Exception as exc:
                failures.append(f"{scope}:{type(exc).__name__}")
                measure("eia_fetch_failed", f"{scope}:{type(exc).__name__}", 1, "count")
                print(f"  {scope}: FAILED {exc}", flush=True)
                continue
            summary["rows"][dataset.id] = summary["rows"].get(dataset.id, 0) + part.num_rows
            summary["revised"] += revised
            measure("eia_rows_fetched", scope, part.num_rows, "rows")
            if revised:
                measure("eia_values_revised", scope, revised, "rows")
            print(f"  {scope}: {part.num_rows} rows, {revised} revised, file now {file_rows}", flush=True)
        for facet, ids in labels.items():
            absent = tuple(sorted(set(ids) - set(pc.unique(table[facet]).to_pylist())))
            if absent:
                missing.append((dataset, {facet: absent}))
    return summary, missing


def last_imports() -> dict[str, dict]:
    """The newest ledger `evidence(kind='eia_bulk')` row per family: the bulk import cursor."""
    try:
        rows = ledger.query(
            "select subject, payload_json, fetched_at from evidence where kind = 'eia_bulk' "
            "qualify row_number() over (partition by subject order by fetched_at desc) = 1"
        )
    except Exception:  # no evidence table yet: nothing imported
        return {}
    return {row["subject"]: {**json.loads(row["payload_json"]), "fetched_at": row["fetched_at"]} for row in rows}


def due_families(manifest: dict, previous: dict[str, dict], now: datetime) -> list[str]:
    """Families whose manifest `last_updated` moved and whose last import is older than the refresh interval."""
    due = []
    for code in [*BULK_FAMILIES, EBA]:
        entry = manifest.get(code)
        if entry is None:
            continue
        last = previous.get(code)
        if last is None:
            due.append(code)
            continue
        if last.get("last_updated") == entry.get("last_updated"):
            continue
        interval = declared("eia_eba_refresh_days" if code == EBA else "eia_bulk_refresh_days")
        if now - last["fetched_at"] >= timedelta(days=float(interval)):
            due.append(code)
    return due


def download(url: str, target: Path, http: httpx.Client) -> None:
    """Stream `url` to `target` (temp file, then rename); an existing target is reused."""
    if target.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".part")
    with http.stream("GET", url, follow_redirects=True, timeout=600) as response:
        response.raise_for_status()
        with partial.open("wb") as out:
            for chunk in response.iter_bytes(1 << 20):
                out.write(chunk)
    partial.replace(target)


# --- run ---------------------------------------------------------------------


def _evidence(store: SourceEvidenceStore, pages: list[EiaPage]) -> list[SourceEvidence]:
    records = []
    for page in pages:
        artifact = store.persist_raw(page.body_gzip, hashlib.sha256(page.body_gzip).hexdigest())
        records.append(
            SourceEvidence(
                ref=artifact.ref,
                sha256=artifact.sha256,
                source_url=page.url,
                retrieved_at=page.retrieved_at,
                publication_time=None,
                mediawiki_revision_id=None,
                mediawiki_revision_time=None,
                content_type="application/gzip",
            )
        )
    return records


def select(patterns: Sequence[str] | None) -> list[Dataset]:
    """Datasets whose id starts with any pattern (`electricity`, `petroleum/stocks`, `electricity/region/1h`)."""
    if not patterns:
        return list(DATASETS.values())
    chosen = [d for d in DATASETS.values() if any(d.id.startswith(p.rstrip("/")) for p in patterns)]
    if not chosen:
        raise SystemExit(f"no dataset matches {list(patterns)}; ids: {', '.join(DATASETS)}")
    return chosen


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="livewire_ingest.py eia", description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", nargs="+", help="API dataset id prefixes, e.g. petroleum electricity/region/1h")
    parser.add_argument(
        "--bulk",
        nargs="+",
        choices=[*BULK_FAMILIES, EBA],
        help="Import these bulk families now, whatever the manifest says (EBA = hourly electricity history)",
    )
    parser.add_argument(
        "--start", type=date.fromisoformat, help="First period, YYYY-MM-DD (clamped to each dataset's first)"
    )
    parser.add_argument("--end", type=date.fromisoformat, help="Last period, YYYY-MM-DD (default: tomorrow UTC)")
    return parser.parse_args(list(argv) if argv is not None else None)


def run(argv: Sequence[str] | None = None, *, client: EiaClient | None = None, http: httpx.Client | None = None) -> int:
    """With no arguments (the scheduled run): every API dataset's lookback, then every bulk family that is due.

    `--dataset` alone runs only those API datasets; `--bulk` alone only those families.
    """
    args = parse_args(argv)
    datasets = select(args.dataset) if args.dataset or not args.bulk else []
    now = datetime.now(UTC)
    today = now.date()
    # Tomorrow, not today: the grid monitor's day-ahead demand forecast (type DF) runs a day ahead.
    end = args.end or today + timedelta(days=1)
    start = args.start or end - timedelta(days=int(declared("eia_lookback_days")))
    lake = data_lake_dir()
    root = lake / "bronze" / "asset_class=energy"
    bulk_dir = lake / "raw" / "eia" / "bulk"

    # Under sync_runner the phase's lane_results row is the run record and
    # LW_RUN_ID is the parent's; opening a second `runs` row under that id would
    # close the parent. Standalone (a backfill), this process owns its run.
    inherited = os.environ.get("LW_RUN_ID")
    run_id = inherited or ledger.new_run_id(JOB)
    run_row = {
        "run_id": run_id,
        "job": JOB,
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": now,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    if not inherited:
        ledger.open_run(run_row)

    store = SourceEvidenceStore(lake)
    evidence: list[SourceEvidence] = []
    measurements: list[dict] = []
    failures: list[str] = []

    def measure(name: str, scope: str, value: float, unit: str) -> None:
        measurements.append(
            {
                "name": name,
                "scope": scope,
                "measured_at": datetime.now(UTC),
                "value": float(value),
                "unit": unit,
                "source": "measured",
                "run_id": run_id,
            }
        )

    def finish(exit_code: int) -> int:
        # One evidence commit per run, success or not (pm:2026-08-31-source-evidence-per-response-cost).
        store.record_many(evidence)
        if measurements:
            ledger.emit("measurements", measurements, run_id=run_id)
        if not inherited:
            verdict = "OK" if exit_code == 0 else "FAILED"
            ledger.emit(
                "runs",
                [run_row | {"ended": datetime.now(UTC), "exit_code": exit_code, "verdict": verdict}],
                run_id=run_id,
            )
        return exit_code

    try:
        eia = client or EiaClient()
        web = http or httpx.Client()

        def fetch_dataset(dataset: Dataset, first_day: date, facets: dict[str, tuple[str, ...]]) -> None:
            latest = None
            for window_start, window_end in windows(dataset, max(first_day, dataset.earliest), end):
                scope = f"{dataset.id}:{window_start:%Y-%m}"
                try:
                    first, last = _query_bounds(dataset, window_start, window_end)
                    raw_rows, pages = eia.fetch(
                        dataset.route,
                        frequency=dataset.frequency,
                        start=first,
                        end=last,
                        sort_columns=("period", *dataset.keys),
                        data_columns=tuple(dataset.values),
                        facets=facets,
                    )
                    evidence.extend(_evidence(store, pages))
                    rows = [normalize(dataset, raw) for raw in raw_rows]
                    file_rows, revised = 0, 0
                    if rows:
                        table = pa.Table.from_pylist(rows, schema=schema_for(dataset))
                        path = partition_path(root, dataset, window_start)
                        file_rows, revised = upsert(
                            path, table, ("period", *dataset.keys), tuple(dataset.values.values())
                        )
                except Exception as exc:
                    # One dataset-window failing never costs the others, and never
                    # exits 0. Named by subject so the operator reruns this window,
                    # not the range (pm:2026-09-16-fetch-failures-had-no-subject).
                    status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
                    failures.append(f"{scope}:{status}")
                    measure("eia_fetch_failed", f"{scope}:{status}", 1, "count")
                    print(f"  {scope}: FAILED {status}: {exc}", flush=True)
                    continue
                # Staleness is judged on observations: a day-ahead forecast is always "fresh".
                observed = [row["period"] for row in rows if row.get("type") != "DF"]
                if observed:
                    newest = max(observed)
                    newest = newest.date() if isinstance(newest, datetime) else newest
                    latest = max(latest or newest, newest)
                measure("eia_rows_fetched", scope, len(rows), "rows")
                if revised:
                    measure("eia_values_revised", scope, revised, "rows")
                print(
                    f"  {scope}: {len(rows)} rows, {len(pages)} pages, {revised} revised, file now {file_rows}",
                    flush=True,
                )
            # Only a window that reaches today says how far behind the dataset is; a
            # rerun of 2024-03 measured "906 days behind" (mini, 2026-09-23).
            if end >= today and (not facets or facets == dataset.facets):
                # Nothing observed in the whole window still measures: at least that old.
                behind = (today - latest).days if latest else (today - max(first_day, dataset.earliest)).days + 1
                measure("eia_staleness_days", dataset.id, behind, "days")

        if datasets:
            print(f"EIA {start} -> {end}: {len(datasets)} datasets (run {run_id})", flush=True)
            for dataset in datasets:
                fetch_dataset(dataset, start, dataset.facets)

        if args.bulk or not args.dataset:
            try:
                manifest = web.get(MANIFEST_URL, follow_redirects=True, timeout=60).json()["dataset"]
            except Exception as exc:
                failures.append(f"bulk/manifest:{type(exc).__name__}")
                measure("eia_fetch_failed", f"bulk/manifest:{type(exc).__name__}", 1, "count")
                print(f"  bulk/manifest: FAILED {exc}", flush=True)
                manifest = {}
            previous = last_imports()
            codes = [code for code in (args.bulk or due_families(manifest, previous, now)) if code in manifest]
            print(f"EIA bulk: {', '.join(codes) or 'nothing due'}", flush=True)
            imported: dict[str, str] = {}
            for code in codes:
                entry = manifest[code]
                failed_before = len(failures)
                stamp = entry["last_updated"][:19].replace(":", "")
                target = bulk_dir / code / f"{stamp}.zip"
                try:
                    download(bulk_url(code), target, web)
                    sha = sha256_file(target)
                    if code == EBA:
                        hourly = [
                            d for d in DATASETS.values() if d.product == "electricity" and d.frequency == "hourly"
                        ]
                        summary, missing = import_eba(target, hourly, eia, root, measure, failures)
                        # Facets the bulk file lacks: their whole history on the first
                        # import; afterwards the daily lookback keeps them current.
                        if code not in previous:
                            for dataset, facets in missing:
                                print(f"  {dataset.id}: not in the bulk file, from the API: {facets}", flush=True)
                                fetch_dataset(dataset, dataset.earliest, facets)
                    else:
                        summary = import_family(
                            target, BULK_FAMILIES[code], root, entry["last_updated"][:10], measure, failures
                        )
                except Exception as exc:
                    status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
                    failures.append(f"bulk/{code}:{status}")
                    measure("eia_fetch_failed", f"bulk/{code}:{status}", 1, "count")
                    print(f"  bulk/{code}: FAILED {status}: {exc}", flush=True)
                    continue
                if len(failures) > failed_before:
                    continue  # no evidence row: the family stays due and the next run retries it
                payload = {
                    "last_updated": entry["last_updated"],
                    "path": str(target.relative_to(lake)),
                    "bytes": target.stat().st_size,
                    **summary,
                }
                imported[code] = entry["last_updated"]
                ledger.emit(
                    "evidence",
                    [
                        {
                            "evidence_hash": sha,
                            "kind": "eia_bulk",
                            "subject": code,
                            "payload_json": json.dumps(payload, sort_keys=True, default=str),
                            "source_url": bulk_url(code),
                            "fetched_at": datetime.now(UTC),
                            "proposer": JOB,
                            "run_id": run_id,
                        }
                    ],
                    run_id=run_id,
                )
            # How long each family has trailed EIA's manifest: 0 when current, else
            # the age of our last import. Never imported: no row, which `status` reads as UNKNOWN.
            for code in [*BULK_FAMILIES, EBA]:
                if code not in manifest:
                    continue
                if imported.get(code) == manifest[code]["last_updated"]:
                    behind = 0
                elif code in previous:
                    current = previous[code]["last_updated"] == manifest[code]["last_updated"]
                    behind = 0 if current else (now - previous[code]["fetched_at"]).days
                else:
                    continue
                measure("eia_bulk_behind_days", code, behind, "days")
    except BaseException:
        # BaseException, not Exception: a backfill is exactly the run an operator
        # interrupts, and Ctrl-C must still close it (pm:2026-09-16-interrupted-runs-never-closed).
        finish(1)
        raise
    if failures:
        print(f"Unfetched: {', '.join(failures)}", flush=True)
    return finish(1 if failures else 0)


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
