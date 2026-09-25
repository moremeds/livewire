"""Bounded read-only IB probe: common softs/grains/livestock futures.

Same mechanism as the energy probe (IBClient → Gateway 127.0.0.1:4001,
clientId 7 base with 326-escalation, serial requests, 2 s spacing, TRADES /
useRTH=1, disconnect in finally).

Per root: reqContractDetails on the expected exchange (fallback: bare symbol,
then one alternate), then on the first unexpired contract:
reqHeadTimeStamp + reqHistoricalData '1 M'/'1 day' + '2 D'/'1 hour'.
"""

import datetime
import json
import sys
import time

RELEASE = "/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2"
sys.path.insert(0, RELEASE)

from clients.ib_client import IBClient  # noqa: E402
from ib_async import Future  # noqa: E402

# root -> (primary exchange, [fallback exchanges]) ; bare-symbol retry on empty
ROOTS = {
    "SB": ("ICEUS", []),   # Sugar No.11
    "KC": ("ICEUS", []),   # Coffee C
    "CC": ("ICEUS", []),   # Cocoa
    "CT": ("ICEUS", []),   # Cotton No.2
    "OJ": ("ICEUS", []),   # Frozen concentrated orange juice
    "ZS": ("ECBOT", ["CBOT"]),  # Soybeans
    "ZM": ("ECBOT", ["CBOT"]),  # Soybean meal
    "ZL": ("ECBOT", ["CBOT"]),  # Soybean oil
    "ZC": ("ECBOT", ["CBOT"]),  # Corn
    "ZW": ("ECBOT", ["CBOT"]),  # Wheat
    "LE": ("CME", []),     # Live cattle
    "HE": ("CME", []),     # Lean hogs
    "FCPO": ("BMD", ["MYX"]),  # Bursa Malaysia crude palm oil
}

result = {
    "probe_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "roots": {},
}
_errbuf = []


def _collect(reqId, code, msg, contract=None):
    try:
        code = int(code)
    except Exception:
        code = 0
    _errbuf.append({"reqId": reqId, "code": code, "msg": str(msg)[:160]})


def _drain():
    out, _errbuf[:] = _errbuf[:], []
    return out


def _det(d):
    c = d.contract
    return {
        "conId": c.conId, "sym": c.symbol, "local": c.localSymbol,
        "exch": c.exchange, "cur": c.currency,
        "month": c.lastTradeDateOrContractMonth, "tc": c.tradingClass,
        "mult": c.multiplier, "mkt": getattr(d, "marketName", None),
        "name": getattr(d, "longName", None),
        "exp": getattr(d, "realExpirationDate", None),
        "ltt": str(getattr(d, "lastTradeTime", "") or ""),
    }


def _pick(dets):
    today = datetime.date.today()
    for d in dets:
        e = getattr(d, "realExpirationDate", None) or ""
        try:
            if datetime.datetime.strptime(e, "%Y%m%d").date() > today:
                return d
        except ValueError:
            continue
    return dets[0] if dets else None


client = IBClient()
try:
    client.connect(host="127.0.0.1", port=4001, client_id=7, timeout=10)
    client.ib.errorEvent += _collect
    result["connected_client_id"] = client._last_client_id

    for root, (prim, alts) in ROOTS.items():
        r = {"attempts": {}}
        result["roots"][root] = r
        dets = []
        tries = [(prim,)] + [(a,) for a in alts] + [("",)]
        for (exch,) in tries:
            try:
                d = client.ib.reqContractDetails(
                    Future(symbol=root, exchange=exch, currency="USD")
                )
                r["attempts"][exch or "(any)"] = {
                    "count": len(d), "first_4": [_det(x) for x in d[:4]],
                    "errors": _drain(),
                }
                if d:
                    dets = d
                    break
            except Exception as exc:
                r["attempts"][exch or "(any)"] = {
                    "exc": repr(exc)[:200], "errors": _drain()
                }
            time.sleep(2)

        chosen = _pick(dets)
        if chosen is None:
            r["status"] = "no contract details"
            continue
        qc = chosen.contract
        r["qualified"] = _det(chosen)
        client.ib.qualifyContracts(qc)
        r["qualify_errors"] = _drain()
        time.sleep(2)
        try:
            r["head_ts"] = str(
                client.get_head_timestamp(qc, "TRADES", use_rth=True)
            )
            r["head_errors"] = _drain()
        except Exception as exc:
            r["head_ts"] = {"exc": repr(exc)[:200], "errors": _drain()}
        time.sleep(2)
        try:
            bars = client.get_historical_data(
                qc, duration="1 M", bar_size="1 day",
                what_to_show="TRADES", use_rth=True,
            )
            r["daily_1m"] = {
                "rows": len(bars),
                "first": str(bars[0].date) if bars else None,
                "last": str(bars[-1].date) if bars else None,
                "errors": _drain(),
            }
        except Exception as exc:
            r["daily_1m"] = {"exc": repr(exc)[:200], "errors": _drain()}
        time.sleep(2)
        try:
            ib = client.get_historical_data(
                qc, duration="2 D", bar_size="1 hour",
                what_to_show="TRADES", use_rth=True,
            )
            r["hourly_2d"] = {
                "rows": len(ib),
                "first": str(ib[0].date) if ib else None,
                "last": str(ib[-1].date) if ib else None,
                "errors": _drain(),
            }
        except Exception as exc:
            r["hourly_2d"] = {"exc": repr(exc)[:200], "errors": _drain()}
        time.sleep(2)
finally:
    result["trailing_errors"] = _drain()
    client.disconnect()
    result["probe_end_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(json.dumps(result, indent=2, default=str))
