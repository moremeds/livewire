import datetime, json, sys, time
sys.path.insert(0, "/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2")
from clients.ib_client import IBClient
from ib_async import Future
errs=[]
def col(r,c,m,ct=None): errs.append({"reqId":r,"code":c,"msg":str(m)[:160]})
def det(d):
    c=d.contract
    return {"conId":c.conId,"sym":c.symbol,"local":c.localSymbol,"exch":c.exchange,"cur":c.currency,
            "month":c.lastTradeDateOrContractMonth,"tc":c.tradingClass,"mult":c.multiplier,
            "mkt":getattr(d,"marketName",None),"name":getattr(d,"longName",None),
            "exp":getattr(d,"realExpirationDate",None),"ltt":str(getattr(d,"lastTradeTime","") or "")}
client=IBClient(); out={"probe_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"attempts":{}}
try:
    client.connect(host="127.0.0.1",port=4001,client_id=7,timeout=10)
    client.ib.errorEvent += col
    chosen=None
    for sym,exch in (("COIL","ICEEU"),("BZ","IPE"),("BZ","ICEEU"),("COIL","IPE")):
        try:
            dets=client.ib.reqContractDetails(Future(symbol=sym,exchange=exch,currency="USD"))
            out["attempts"][f"{sym}@{exch}"]={"count":len(dets),"first_8":[det(d) for d in dets[:8]],"errors":errs[:]}
            errs.clear()
            if dets:
                today=datetime.date.today()
                for d in dets:
                    e=getattr(d,"realExpirationDate",None) or ""
                    try:
                        if datetime.datetime.strptime(e,"%Y%m%d").date()>today: chosen=d; break
                    except ValueError: pass
                if chosen is None: chosen=dets[0]
                break
        except Exception as e:
            out["attempts"][f"{sym}@{exch}"]={"exc":repr(e)[:200],"errors":errs[:]}; errs.clear()
        time.sleep(2)
    if chosen is not None:
        qc=chosen.contract; out["qualified"]=det(chosen)
        client.ib.qualifyContracts(qc); out["qualify_errors"]=errs[:]; errs.clear(); time.sleep(2)
        try:
            out["head_ts"]={"value":str(client.get_head_timestamp(qc,"TRADES",use_rth=True)),"errors":errs[:]}; errs.clear()
        except Exception as e: out["head_ts"]={"exc":repr(e)[:200],"errors":errs[:]}; errs.clear()
        time.sleep(2)
        try:
            bars=client.get_historical_data(qc,duration="1 M",bar_size="1 day",what_to_show="TRADES",use_rth=True)
            out["daily_1m"]={"rows":len(bars),"first":str(bars[0].date) if bars else None,"last":str(bars[-1].date) if bars else None,"errors":errs[:]}; errs.clear()
        except Exception as e: out["daily_1m"]={"exc":repr(e)[:200],"errors":errs[:]}; errs.clear()
        time.sleep(2)
        try:
            ib=client.get_historical_data(qc,duration="2 D",bar_size="1 hour",what_to_show="TRADES",use_rth=True)
            out["hourly_2d"]={"rows":len(ib),"first":str(ib[0].date) if ib else None,"last":str(ib[-1].date) if ib else None,"errors":errs[:]}; errs.clear()
        except Exception as e: out["hourly_2d"]={"exc":repr(e)[:200],"errors":errs[:]}; errs.clear()
finally:
    client.disconnect()
    print(json.dumps(out,indent=2,default=str))
