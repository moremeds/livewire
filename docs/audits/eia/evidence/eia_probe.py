import json
import sys
import urllib.parse
import urllib.request

K = sys.stdin.readline().strip()


def get(path, **q):
    q["api_key"] = K
    url = f"https://api.eia.gov/v2/{path}?" + urllib.parse.urlencode(q, doseq=True)
    with urllib.request.urlopen(url, timeout=40) as r:
        return r.status, dict(r.headers), json.load(r)


for p in ["", "petroleum/pri/spt/", "natural-gas/pri/fut/"]:
    s, h, d = get(p)
    r = d["response"]
    print(f"== /v2/{p} {s} ratelimit={ {k: v for k, v in h.items() if 'limit' in k.lower()} }")
    for x in r.get("routes", []):
        print("  ", x["id"], "-", x["name"])
    if "facets" in r:
        print(
            "  facets",
            [f["id"] for f in r["facets"]],
            "freq",
            [f["id"] for f in r["frequency"]],
            "data",
            list(r["data"]),
            r.get("startPeriod"),
            r.get("endPeriod"),
        )
# series facet values for daily spot
s, h, d = get("petroleum/pri/spt/facet/series/")
print("== spt series count", d["response"]["totalFacets"])
for f in d["response"]["facets"][:40]:
    print("  ", f["id"], "|", f.get("name"))
# one data pull: RWTC daily, latest 5
s, h, d = get(
    "petroleum/pri/spt/data/",
    frequency="daily",
    **{
        "data[]": "value",
        "facets[series][]": "RWTC",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 5,
    },
)
r = d["response"]
print("== RWTC total", r["total"], "warnings", d.get("warnings") or r.get("warnings"))
for row in r["data"]:
    print("  ", row)
# backward-compat seriesid route
s, h, d = get("seriesid/NG.RNGWHHD.D", length=3)
print("== seriesid NG.RNGWHHD.D total", d["response"]["total"])
[print("  ", x) for x in d["response"]["data"][:3]]
