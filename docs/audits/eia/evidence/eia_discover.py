"""Walk EIA v2 route trees; for each leaf list live daily/weekly series.

Run on the mini: python3 eia_discover.py > routes.json  (key read from ~/market-warehouse/.env)
"live" = has an observation at that frequency in the last 21 days.
"""
import datetime as dt, json, pathlib, sys, urllib.parse, urllib.request

K = next(l.split("=", 1)[1].strip() for l in (pathlib.Path.home() / "market-warehouse/.env").read_text().splitlines() if l.startswith("EIA_API_KEY="))
SINCE = (dt.date.today() - dt.timedelta(days=21)).isoformat()

def get(path, **q):
    q["api_key"] = K
    with urllib.request.urlopen(f"https://api.eia.gov/v2/{path}?" + urllib.parse.urlencode(q, doseq=True), timeout=60) as r:
        return json.load(r)["response"]

def walk(path, out):
    r = get(path)
    if r.get("routes"):
        for x in r["routes"]:
            walk(f"{path}{x['id']}/", out)
        return
    leaf = {"route": path, "name": r.get("name"), "freq": [f["id"] for f in r.get("frequency", [])],
            "start": r.get("startPeriod"), "end": r.get("endPeriod"), "facets": [f["id"] for f in r.get("facets", [])],
            "data": list(r.get("data", {})), "live": {}}
    for fq in ("daily", "weekly"):
        if fq not in leaf["freq"] or "series" not in leaf["facets"]:
            continue
        rows = get(f"{path}data/", frequency=fq, start=SINCE, length=5000, **{"data[]": leaf["data"][0]})["data"]
        leaf["live"][fq] = sorted({(x["series"], x.get("series-description"), x.get("units")) for x in rows})
    out.append(leaf)
    print(path, leaf["freq"], {k: len(v) for k, v in leaf["live"].items()}, file=sys.stderr)

out = []
for root in sys.argv[1:] or ["petroleum/", "natural-gas/"]:
    walk(root, out)
json.dump(out, sys.stdout, indent=1)
