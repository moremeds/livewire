"""Task 3 bounded read-only IB energy-futures probe.

Runs on the mini via the release checkout's own venv + clients.ib_client.IBClient
(the existing connection mechanism: Gateway 127.0.0.1:4001, clientId escalation
on error 326 starting at base id 7 — 7 is unused in the CLIENT_IDS registry;
escalation skips any in-use id).

Per root in {RB, HO, NG}, serially:
  1. reqContractDetails(Future(root, exchange='NYMEX'))  — discover real months,
     conId/localSymbol/tradingClass/multiplier/last-trade metadata. No guessed
     expiry.
  2. reqHeadTimeStamp(qualified, 'TRADES', useRTH=1)     — earliest history.
  3. reqHistoricalData(qualified, '1 M', '1 day', 'TRADES', useRTH=1) — one small
     daily response (mirrors production daily_update params).
  4. reqHistoricalData(qualified, '2 D', '1 hour', 'TRADES', useRTH=1) — short
     intraday probe.

Control: reqContractDetails for BZ 202610 on NYMEX vs CME (contract-details only)
to verify the wrong-exchange hypothesis from nightly lane logs (Error 200,
exchange='CME' while preset declares NYMEX).

Stops early on pacing/permission errors. Read-only: no orders, no subscriptions,
no writes to lake/DB. Disconnects in finally.
"""

import datetime
import json
import sys
import time

RELEASE = "/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2"
sys.path.insert(0, RELEASE)

from clients.ib_client import IBClient  # noqa: E402
from ib_async import Future  # noqa: E402

STOP_CODES = {162, 165, 10167, 10168, 10197}  # pacing / entitlement codes

result = {
    "probe_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "host": "127.0.0.1",
    "port": 4001,
    "roots": {},
    "bz_control": {},
    "errors": [],
}
_errbuf = []


def _collect(reqId, code, msg, contract=None):
    try:
        code = int(code)
    except Exception:
        code = 0
    _errbuf.append({"reqId": reqId, "code": code, "msg": str(msg)[:200]})


def _drain():
    out, _errbuf[:] = _errbuf[:], []
    return out


def _det(d):
    c = d.contract
    g = lambda o, *a: next((getattr(o, a) for a in a if getattr(o, a, None)), None)
    return {
        "conId": c.conId,
        "symbol": c.symbol,
        "localSymbol": c.localSymbol,
        "exchange": c.exchange,
        "currency": c.currency,
        "month": c.lastTradeDateOrContractMonth,
        "tradingClass": c.tradingClass,
        "multiplier": c.multiplier,
        "marketName": getattr(d, "marketName", None),
        "longName": getattr(d, "longName", None),
        "realExpirationDate": getattr(d, "realExpirationDate", None),
        "lastTradeTime": str(getattr(d, "lastTradeTime", "") or ""),
    }


client = IBClient()
try:
    client.connect(host="127.0.0.1", port=4001, client_id=7, timeout=10)
    client.ib.errorEvent += _collect
    result["connected_client_id"] = client._last_client_id
    result["connect_errors"] = _drain()

    abort = False
    for root in ("RB", "HO", "NG"):
        r = {"steps": {}}
        result["roots"][root] = r
        try:
            dets = client.ib.reqContractDetails(
                Future(symbol=root, exchange="NYMEX", currency="USD")
            )
            r["steps"]["reqContractDetails"] = {
                "args": f"Future(symbol={root}, exchange=NYMEX, currency=USD)",
                "count": len(dets),
                "first_12": [_det(d) for d in dets[:12]],
                "errors": _drain(),
            }
        except Exception as exc:
            r["steps"]["reqContractDetails"] = {"exc": repr(exc)[:300], "errors": _drain()}
            dets = []

        today = datetime.date.today()
        chosen = None
        for d in dets:
            exp = getattr(d, "realExpirationDate", None) or ""
            try:
                if datetime.datetime.strptime(exp, "%Y%m%d").date() > today:
                    chosen = d
                    break
            except ValueError:
                continue
        if chosen is None and dets:
            chosen = dets[0]
            r["expiry_note"] = "no realExpirationDate parsed > today; used first listed"
        if chosen is None:
            r["steps"]["qualification"] = "no contract details returned"
            continue
        qc = chosen.contract
        r["qualified_contract"] = _det(chosen)
        client.ib.qualifyContracts(qc)
        r["qualify_errors"] = _drain()
        time.sleep(2)

        try:
            r["steps"]["head_ts"] = {
                "args": "reqHeadTimeStamp TRADES useRTH=1",
                "value": str(client.get_head_timestamp(qc, "TRADES", use_rth=True)),
                "errors": _drain(),
            }
        except Exception as exc:
            r["steps"]["head_ts"] = {"exc": repr(exc)[:300], "errors": _drain()}
        time.sleep(2)

        try:
            bars = client.get_historical_data(
                qc, duration="1 M", bar_size="1 day", what_to_show="TRADES", use_rth=True
            )
            r["steps"]["daily_1m"] = {
                "args": "reqHistoricalData duration='1 M' bar='1 day' TRADES useRTH=1",
                "rows": len(bars),
                "first": str(bars[0].date) if bars else None,
                "last": str(bars[-1].date) if bars else None,
                "errors": _drain(),
            }
        except Exception as exc:
            r["steps"]["daily_1m"] = {"exc": repr(exc)[:300], "errors": _drain()}
        time.sleep(2)

        try:
            ibars = client.get_historical_data(
                qc, duration="2 D", bar_size="1 hour", what_to_show="TRADES", use_rth=True
            )
            r["steps"]["hourly_2d"] = {
                "args": "reqHistoricalData duration='2 D' bar='1 hour' TRADES useRTH=1",
                "rows": len(ibars),
                "first": str(ibars[0].date) if ibars else None,
                "last": str(ibars[-1].date) if ibars else None,
                "errors": _drain(),
            }
        except Exception as exc:
            r["steps"]["hourly_2d"] = {"exc": repr(exc)[:300], "errors": _drain()}
        time.sleep(2)

        flat = [e["code"] for s in r["steps"].values() for e in s.get("errors", [])]
        if any(c in STOP_CODES for c in flat):
            abort = True
        if abort:
            result["aborted_after"] = root
            break

    for exch in ("NYMEX", "CME"):
        try:
            dets = client.ib.reqContractDetails(
                Future(symbol="BZ", lastTradeDateOrContractMonth="202610",
                       exchange=exch, currency="USD")
            )
            result["bz_control"][exch] = {
                "count": len(dets),
                "first": _det(dets[0]) if dets else None,
                "errors": _drain(),
            }
        except Exception as exc:
            result["bz_control"][exch] = {"exc": repr(exc)[:300], "errors": _drain()}
        time.sleep(2)
finally:
    result["trailing_errors"] = _drain()
    client.disconnect()
    result["probe_end_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(json.dumps(result, indent=2, default=str))
