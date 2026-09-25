import datetime, json, sys, time
sys.path.insert(0, "/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2")
from clients.ib_client import IBClient
from ib_async import Contract
errs=[]
def col(r,c,m,ct=None): errs.append({"reqId":r,"code":c,"msg":str(m)[:160]})
client=IBClient(); out={"probe_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"probes":[]}
try:
    client.connect(host="127.0.0.1",port=4001,client_id=7,timeout=10)
    client.ib.errorEvent += col
    c=Contract(conId=304037511); client.ib.qualifyContracts(c)
    for label,dur,bs,end in (("1h@2026-06","1 M","1 hour","20260615-00:00:00"),
                             ("1h@2025-12","1 M","1 hour","20251215-00:00:00"),
                             ("1h@2026-08","1 M","1 hour","20260815-00:00:00"),
                             ("1m@2026-08","1 D","1 min","20260815-00:00:00"),
                             ("1m@2026-09-15","1 D","1 min","20260915-00:00:00")):
        try:
            bars=client.get_historical_data(c,duration=dur,bar_size=bs,what_to_show="TRADES",use_rth=True,end_date=end)
            out["probes"].append({"contract":"CLX6","req":label,"rows":len(bars),
                "first":str(bars[0].date) if bars else None,
                "last":str(bars[-1].date) if bars else None,"errors":errs[:]})
        except Exception as e:
            out["probes"].append({"contract":"CLX6","req":label,"exc":repr(e)[:200],"errors":errs[:]})
        errs.clear(); time.sleep(3)
finally:
    client.disconnect()
    print(json.dumps(out,indent=2,default=str))
