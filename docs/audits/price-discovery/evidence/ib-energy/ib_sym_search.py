import datetime, json, sys, time
sys.path.insert(0, "/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2")
from clients.ib_client import IBClient
errs=[]
def col(r,c,m,ct=None): errs.append({"reqId":r,"code":c,"msg":str(m)[:160]})
client=IBClient(); out={"probe_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"search":{}}
try:
    client.connect(host="127.0.0.1",port=4001,client_id=7,timeout=10)
    client.ib.errorEvent += col
    for pat in ("Brent","LCO"):
        try:
            res=client.ib.reqMatchingSymbols(pat)
            out["search"][pat]={"count":len(res),"matches":[
                {"conId":m.contract.conId,"sym":m.contract.symbol,"secType":m.contract.secType,
                 "exch":m.contract.exchange,"cur":m.contract.currency,
                 "desc":getattr(m,"description",None),"derSecTypes":list(getattr(m,"derivativeSecTypes",[]) or [])}
                for m in res[:10]],"errors":errs[:]}
            errs.clear()
        except Exception as e:
            out["search"][pat]={"exc":repr(e)[:200],"errors":errs[:]}; errs.clear()
        time.sleep(2)
finally:
    client.disconnect()
    print(json.dumps(out,indent=2,default=str))
