"""Task 1 (price-discovery-runner): read-only IB commodity contract inventory.

Scope (user, as of 2026-09-23):
  energy roots CL, NG, COIL, RB, HO  -> every currently available delivery month
                                       through 2027-12
  metals GC, SI, HG                  -> first two currently available months
  ags SB, KC, CC, CT, OJ, ZS, ZM, ZL, ZC, ZW, LE, HE -> first two months
  FCPO and BZ excluded by decision.

Method: serial reqContractDetails per root on the evidence-proven exchange
(fallback: one bare-symbol attempt), then qualifyContracts + reqHeadTimeStamp
(TRADES, useRTH=1) per selected contract. Delivery month is taken ONLY from
ContractDetails.contractMonth; if it is absent/unparseable for a root's chain
the root is reported UNKNOWN — realExpirationDate / lastTradeDateOrContractMonth
are recorded raw and never used to infer the month. No historical bars are
requested. 2 s spacing between calls; disconnect in finally.

Runs via the release venv interpreter with the worktree on sys.path (worktree
HEAD == release sha f83e3df0). Emits JSON on stdout.
"""

import datetime
import json
import re
import sys
import time

WORKTREE = "/Users/moremeds/projects/livewire/.worktrees/price-discovery-runner"
sys.path.insert(0, WORKTREE)

import ib_async  # noqa: E402
from clients.ib_client import IBClient  # noqa: E402
from ib_async import Future  # noqa: E402

AS_OF = "2026-09-23"
MAX_MONTH = "202712"  # energy roots: through 2027-12 inclusive
MONTH_RE = re.compile(r"^\d{6}$")
STOP_CODES = {162, 165, 10167, 10168, 10197}  # pacing / entitlement
PACING_CODE = 162
STEP_SPACING = 2        # between details/qualify calls
HEAD_TS_SPACING = 12    # reqHeadTimeStamp is paced as historical data (~60/10min observed)
COOLDOWN_S = 65         # single retry wait after a 162
MAX_COOLDOWNS = 5       # abort the run past this many pacing hits

# root -> (primary exchange, scope rule)
ENERGY = {"CL": "NYMEX", "NG": "NYMEX", "COIL": "IPE", "RB": "NYMEX", "HO": "NYMEX"}
METALS = {"GC": "COMEX", "SI": "COMEX", "HG": "COMEX"}
AGS = {
    "SB": "NYBOT", "KC": "NYBOT", "CC": "NYBOT", "CT": "NYBOT", "OJ": "NYBOT",
    "ZS": "CBOT", "ZM": "CBOT", "ZL": "CBOT", "ZC": "CBOT", "ZW": "CBOT",
    "LE": "CME", "HE": "CME",
}

result = {
    "task": "COMMODITY_CONTRACT_INVENTORY_2026-09-23",
    "probe_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "as_of": AS_OF,
    "host": "127.0.0.1",
    "port": 4001,
    "client_id_base": 7,
    "worktree": WORKTREE,
    "ib_async_version": ib_async.__version__,
    "scope": {
        "energy_all_months_through": MAX_MONTH,
        "energy_roots": list(ENERGY),
        "metals_first2": list(METALS),
        "ags_first2": list(AGS),
        "excluded_roots": ["FCPO", "BZ"],
    },
    "pacing": {"step_spacing_s": STEP_SPACING,
               "head_ts_spacing_s": HEAD_TS_SPACING,
               "cooldown_s_on_162": COOLDOWN_S,
               "max_cooldowns": MAX_COOLDOWNS},
    "roots": {},
    "selected_tickers": [],
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


def _san(o):
    if hasattr(o, "__dict__"):
        return {k: _san(v) for k, v in o.__dict__.items()}
    if isinstance(o, (list, tuple)):
        return [_san(v) for v in o]
    if isinstance(o, dict):
        return {k: _san(v) for k, v in o.items()}
    try:
        json.dumps(o)
        return o
    except TypeError:
        return str(o)


def _det(d):
    c = d.contract
    return {
        "conId": c.conId,
        "symbol": c.symbol,
        "localSymbol": c.localSymbol,
        "exchange": c.exchange,
        "currency": c.currency,
        "lastTradeDateOrContractMonth": c.lastTradeDateOrContractMonth,
        "tradingClass": c.tradingClass,
        "multiplier": c.multiplier,
        "contractMonth": getattr(d, "contractMonth", None),
        "realExpirationDate": getattr(d, "realExpirationDate", None),
        "lastTradeTime": str(getattr(d, "lastTradeTime", "") or ""),
        "marketName": getattr(d, "marketName", None),
        "longName": getattr(d, "longName", None),
        "raw": _san(d),
    }


def _valid_month(m):
    return bool(m) and bool(MONTH_RE.match(str(m)))


client = IBClient()
aborted = False
cooldowns = 0
try:
    client.connect(host="127.0.0.1", port=4001, client_id=7, timeout=10)
    client.ib.errorEvent += _collect
    result["connected_client_id"] = client._last_client_id
    result["connect_errors"] = _drain()

    groups = [("energy", ENERGY), ("metals", METALS), ("ags", AGS)]
    for group, roots in groups:
        if aborted:
            break
        for root, prim_exch in roots.items():
            r = {"group": group, "exchange_used": None, "attempts": {},
                 "all_contracts": [], "selected": [], "excluded": [],
                 "status": "PENDING"}
            result["roots"][root] = r
            print(f"[{group}] {root} @{prim_exch}", file=sys.stderr, flush=True)

            dets = []
            for exch in (prim_exch, ""):
                tag = exch or "(any)"
                try:
                    d = client.ib.reqContractDetails(
                        Future(symbol=root, exchange=exch, currency="USD")
                    )
                    r["attempts"][tag] = {"count": len(d), "errors": _drain()}
                    if d:
                        dets = d
                        r["exchange_used"] = exch or d[0].contract.exchange
                        break
                except Exception as exc:
                    r["attempts"][tag] = {"exc": repr(exc)[:300], "errors": _drain()}
                time.sleep(2)

            r["all_contracts"] = [_det(x) for x in dets]
            if not dets:
                r["status"] = "NO_DETAILS"
                continue

            # verified-month pool: own symbol + parseable YYYYMM contractMonth
            pool = []
            for x, raw in zip(dets, r["all_contracts"]):
                cm = raw["contractMonth"]
                if x.contract.symbol != root:
                    r["excluded"].append({"conId": raw["conId"],
                                          "reason": f"symbol_mismatch:{x.contract.symbol}",
                                          "contractMonth": cm})
                elif not _valid_month(cm):
                    r["excluded"].append({"conId": raw["conId"],
                                          "reason": "no_valid_contractMonth",
                                          "contractMonth": cm})
                else:
                    pool.append((str(cm), x, raw))
            if not pool:
                r["status"] = "UNKNOWN"  # cannot distinguish delivery month
                continue

            by_month = {}
            for cm, x, raw in pool:
                by_month.setdefault(cm, []).append((x, raw))
            for cm, lst in by_month.items():
                if len(lst) > 1:
                    # deterministic pick: queried exchange first, then lowest conId
                    lst.sort(key=lambda t: (t[0].contract.exchange != r["exchange_used"],
                                            t[0].contract.conId or 0))
                    r["excluded"].append({"reason": "duplicate_contractMonth",
                                          "contractMonth": cm,
                                          "conIds": [x.contract.conId for x, _ in lst]})

            months = sorted(by_month)
            if group == "energy":
                pick = [m for m in months if m <= MAX_MONTH]
            else:
                pick = months[:2]
            for m in months:
                if m not in pick:
                    r["excluded"].append({"reason": "outside_scope",
                                          "contractMonth": m})

            for cm in pick:
                x, raw = by_month[cm][0]
                rec = {"contractMonth": cm, "ticker": f"{root}_{cm}",
                       "conId": raw["conId"], "localSymbol": raw["localSymbol"],
                       "exchange": raw["exchange"], "tradingClass": raw["tradingClass"],
                       "lastTradeDateOrContractMonth": raw["lastTradeDateOrContractMonth"],
                       "realExpirationDate": raw["realExpirationDate"],
                       "qualified": False, "head_timestamp": None}
                qc = x.contract
                try:
                    client.ib.qualifyContracts(qc)
                    rec["qualify_errors"] = _drain()
                    bad = [e for e in rec["qualify_errors"]
                           if isinstance(e, dict) and "code" in e and e["code"] < 2100]
                    rec["qualified"] = bool(qc.conId) and not bad
                except Exception as exc:
                    rec["qualify_errors"] = [{"exc": repr(exc)[:300]}] + _drain()
                rec["conId_after_qualify"] = qc.conId
                time.sleep(STEP_SPACING)

                rec["head_attempts"] = []
                for attempt in range(2):
                    try:
                        val = client.get_head_timestamp(qc, "TRADES", use_rth=True)
                        errs = _drain()
                    except Exception as exc:
                        val, errs = None, [{"exc": repr(exc)[:300]}] + _drain()
                    rec["head_attempts"].append(
                        {"attempt": attempt + 1,
                         "value": str(val) if val else None, "errors": errs})
                    hit_pacing = any(
                        isinstance(e, dict) and e.get("code") == PACING_CODE
                        for e in errs)
                    hit_other_stop = any(
                        isinstance(e, dict) and e.get("code") in STOP_CODES - {PACING_CODE}
                        for e in errs)
                    if hit_other_stop:
                        aborted = True
                        result["aborted_at"] = rec["ticker"]
                        break
                    if not hit_pacing:
                        break
                    cooldowns += 1
                    if cooldowns > MAX_COOLDOWNS:
                        aborted = True
                        result["aborted_at"] = f"{rec['ticker']}:cooldown_cap"
                        break
                    print(f"  pacing 162 on {rec['ticker']}; cooling {COOLDOWN_S}s",
                          file=sys.stderr, flush=True)
                    time.sleep(COOLDOWN_S)
                rec["head_timestamp"] = rec["head_attempts"][-1]["value"]
                rec["head_errors"] = rec["head_attempts"][-1]["errors"]
                time.sleep(HEAD_TS_SPACING)
                if rec["qualified"]:
                    r["selected"].append(rec)
                else:
                    r["excluded"].append({"reason": "qualify_failed",
                                          "contractMonth": cm,
                                          "conId": raw["conId"],
                                          "errors": rec["qualify_errors"]})
                print(f"  {rec['ticker']} qual={rec['qualified']} head={rec['head_timestamp']}",
                      file=sys.stderr, flush=True)
                if aborted:
                    break

            r["status"] = "OK" if r["selected"] else "UNKNOWN"
            if aborted:
                break

    result["selected_tickers"] = [
        f"{root}_{s['contractMonth']}"
        for root, rr in result["roots"].items()
        for s in rr["selected"]
    ]
finally:
    result["trailing_errors"] = _drain()
    result["pacing"]["cooldowns_used"] = cooldowns
    client.disconnect()
    result["probe_end_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(json.dumps(result, indent=2, default=str))
