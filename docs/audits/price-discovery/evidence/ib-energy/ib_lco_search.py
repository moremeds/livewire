import datetime, json, sys, time
sys.path.insert(0, "/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2")
from clients.ib_client import IBClient
from ib_async import Future
errs=[]
def col(r,c,m,ct=None): errs.append({"reqId":r,"code":c,"msg":str(m)[:160]})
client=IBClient(); out={"probe_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"search":{}}
try:
    client.connect(host="127.0.0.1",port=4001,client_id=7,timeout=10)
    client.ib.errorEvent += col
    for sym in ("LCO","BRN","B"):
        try:
            dets=client.ib.reqContractDetails(Future(symbol=sym))
            out["search"][sym]={"count":len(dets),"contracts":[
                {"conId":d.contract.conId,"sym":d.contract.symbol,"local":d.contract.localSymbol,
                 "exch":d.contract.exchange,"cur":d.currency if hasattr(d,'currency') else d.contract.currency,
                 "month":d.contract.lastTradeDateOrContractMonth,"tc":d.contract.tradingClass,
                 "mult":d.contract.multiplier,"mkt":getattr(d,"marketName",None),
                 "name":getattr(d,"longName",None),"exp":getattr(d,"realExpirationDate",None)}
                for d in dets[:8]],"errors":errs[:]}
            errs.clear()
        except Exception as e:
            out["search"][sym]={"exc":repr(e)[:200],"errors":errs[:]}; errs.clear()
        time.sleep(2)
finally:
    client.disconnect()
    print(json.dumps(out,indent=2,default=str))
