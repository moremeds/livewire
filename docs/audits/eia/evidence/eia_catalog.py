"""Catalog every EIA v2 leaf route: frequencies, period range, facets, total rows per frequency,
and rows in a recent window for hourly/daily/weekly. Metadata + counts only, no data download.

Run on the mini (key read from ~/market-warehouse/.env):
    python3 eia_catalog.py [root/ ...] > catalog-<date>.jsonl     # default: every root under /v2/; one JSON line per leaf
Series-level lists for petroleum/natural-gas are eia_discover.py.
"""
import datetime as dt, json, pathlib, sys, urllib.parse, urllib.request

K = next(l.split("=", 1)[1].strip() for l in (pathlib.Path.home() / "market-warehouse/.env").read_text().splitlines() if l.startswith("EIA_API_KEY="))
NOW = dt.datetime.now(dt.UTC)
RECENT = {"hourly": (2, "%Y-%m-%dT%H"), "daily": (14, "%Y-%m-%d"), "weekly": (21, "%Y-%m-%d")}

def get(path, **q):
    q["api_key"] = K
    with urllib.request.urlopen(f"https://api.eia.gov/v2/{path}?" + urllib.parse.urlencode(q, doseq=True), timeout=90) as r:
        return json.load(r)["response"]

def count(path, fq, data, **q):
    try:
        return int(get(f"{path}data/", frequency=fq, length=1, **{"data[]": data}, **q)["total"])
    except Exception as e:  # ponytail: one bad leaf is recorded, never aborts the catalog
        return f"err {e}"

def walk(path):
    try:
        r = get(path)
    except Exception as e:
        emit({"route": path, "error": str(e)}); print(path, "ERR", e, file=sys.stderr); return
    if r.get("routes"):
        for x in r["routes"]:
            walk(f"{path}{x['id']}/")
        return
    try:
        data = list(r.get("data", {}))
        leaf = {"route": path, "name": r.get("name"), "desc": r.get("description"),
                "freq": {f["id"]: f.get("format") for f in r.get("frequency", [])},
                "start": r.get("startPeriod"), "end": r.get("endPeriod"),
                "facets": {f["id"]: f.get("description") for f in r.get("facets", [])},
                "data": {k: ((v.get("alias"), v.get("units")) if isinstance(v, dict) else v) for k, v in r.get("data", {}).items()},
                "total_rows": {}, "recent_rows": {}}
        for fq in leaf["freq"]:
            if fq == "local-hourly" or not data:  # local-hourly = same rows as hourly in local time; its count 500s
                continue
            leaf["total_rows"][fq] = count(path, fq, data[0])
            if fq in RECENT:
                days, fmt = RECENT[fq]
                leaf["recent_rows"][fq] = count(path, fq, data[0], start=(NOW - dt.timedelta(days=days)).strftime(fmt))
        emit(leaf)
        print(path, leaf["total_rows"], leaf["end"], file=sys.stderr)
    except Exception as e:  # an unexpected metadata shape is recorded, never aborts the catalog
        emit({"route": path, "error": f"{type(e).__name__}: {e}"}); print(path, "ERR", e, file=sys.stderr)

def emit(obj):
    print(json.dumps(obj), flush=True)

for root in sys.argv[1:] or [f"{x['id']}/" for x in get("")["routes"]]:
    walk(root)
