"""Bounded follow-up probe: IB Brent listing after BZ failure.

User steering: "brt ticker is lco at ice". Serially try reqContractDetails for
Future(LCO|B, exchange in {ICEEU, IPE}) — first non-empty result wins; then on
the first unexpired contract: reqHeadTimeStamp + 1M daily + 2D hourly, same
TRADES/useRTH=1 params as the Task 3 probe. Read-only; disconnect in finally.
"""

import datetime
import json
import sys
import time

RELEASE = "/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2"
sys.path.insert(0, RELEASE)

from clients.ib_client import IBClient  # noqa: E402
from ib_async import Future  # noqa: E402

result = {
    "probe_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "attempts": {},
    "qualified": None,
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

    chosen = None
    for sym, exch in (("LCO", "ICEEU"), ("B", "ICEEU"), ("LCO", "IPE"), ("B", "IPE")):
        try:
            dets = client.ib.reqContractDetails(
                Future(symbol=sym, exchange=exch, currency="USD")
            )
            result["attempts"][f"{sym}@{exch}"] = {
                "count": len(dets),
                "first_6": [_det(d) for d in dets[:6]],
                "errors": _drain(),
            }
            if dets:
                today = datetime.date.today()
                for d in dets:
                    exp = getattr(d, "realExpirationDate", None) or ""
                    try:
                        if datetime.datetime.strptime(exp, "%Y%m%d").date() > today:
                            chosen = d
                            break
                    except ValueError:
                        continue
                if chosen is None:
                    chosen = dets[0]
                break
        except Exception as exc:
            result["attempts"][f"{sym}@{exch}"] = {"exc": repr(exc)[:300], "errors": _drain()}
        time.sleep(2)

    if chosen is not None:
        qc = chosen.contract
        result["qualified"] = _det(chosen)
        client.ib.qualifyContracts(qc)
        result["qualify_errors"] = _drain()
        time.sleep(2)
        try:
            result["head_ts"] = {
                "value": str(client.get_head_timestamp(qc, "TRADES", use_rth=True)),
                "errors": _drain(),
            }
        except Exception as exc:
            result["head_ts"] = {"exc": repr(exc)[:300], "errors": _drain()}
        time.sleep(2)
        try:
            bars = client.get_historical_data(
                qc, duration="1 M", bar_size="1 day", what_to_show="TRADES", use_rth=True
            )
            result["daily_1m"] = {
                "rows": len(bars),
                "first": str(bars[0].date) if bars else None,
                "last": str(bars[-1].date) if bars else None,
                "errors": _drain(),
            }
        except Exception as exc:
            result["daily_1m"] = {"exc": repr(exc)[:300], "errors": _drain()}
        time.sleep(2)
        try:
            ibars = client.get_historical_data(
                qc, duration="2 D", bar_size="1 hour", what_to_show="TRADES", use_rth=True
            )
            result["hourly_2d"] = {
                "rows": len(ibars),
                "first": str(ibars[0].date) if ibars else None,
                "last": str(ibars[-1].date) if ibars else None,
                "errors": _drain(),
            }
        except Exception as exc:
            result["hourly_2d"] = {"exc": repr(exc)[:300], "errors": _drain()}
finally:
    result["trailing_errors"] = _drain()
    client.disconnect()
    result["probe_end_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(json.dumps(result, indent=2, default=str))
