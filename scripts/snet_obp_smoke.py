"""S-net OBP smoke v2: probe which S-net network code (0120A vs 0120) exposes the
ocean-bottom PRESSURE channels. For each candidate, dump the distinct channel
components + units from the win32 channel table. Confirms whether OBP pressure is
win32-accessible via HinetPy at all, or needs the separate auth/download/cont portal."""
import os, collections
from datetime import datetime
from HinetPy import Client
USER=os.environ["HINET_USER"]; PW=os.environ["HINET_PASS"]
cl=Client(USER,PW)
for NET in ["0120A","0120"]:
    print("######## NET=%s ########"%NET,flush=True)
    try:
        st=cl.get_station_list(NET)
    except Exception as e:
        print("  get_station_list err:",repr(e)[:160],flush=True); continue
    print("  station_list len:",len(st),flush=True)
    if not st: continue
    # pick one offshore station
    pick=None
    for s in st:
        la=getattr(s,"latitude",None); lo=getattr(s,"longitude",None)
        nm=getattr(s,"name",None) or getattr(s,"code",None)
        if la is None or lo is None or nm is None: continue
        if 37.5<=float(la)<=38.5 and 142<=float(lo)<=143: pick=str(nm); break
    if pick is None: pick=str(getattr(st[0],"name",None) or getattr(st[0],"code",None))
    print("  pick station:",pick,flush=True)
    try:
        cl.select_stations(NET,[pick])
    except Exception as e:
        print("  select err:",repr(e)[:120],flush=True)
        try: cl.select_stations(NET,[pick.split(".")[-1]])
        except Exception as e2: print("  select2 err:",repr(e2)[:120],flush=True); continue
    os.makedirs("work",exist_ok=True)
    try:
        data=cl.get_continuous_waveform(NET,datetime(2022,6,1,0,0),5,outdir="work")
    except Exception as e:
        print("  get_waveform err:",repr(e)[:200],flush=True); continue
    w32,cht=data
    print("  cnt:",w32," ch:",cht,flush=True)
    comp=collections.Counter(); unit=collections.Counter(); rows=0
    try:
        for ln in open(cht):
            p=ln.split()
            if len(p)>=8 and p[0].startswith(("e","E")) and len(p[0])==4:
                comp[p[4]]+=1; unit[p[7]]+=1; rows+=1
    except Exception as e:
        print("  ch parse err:",repr(e)[:120],flush=True)
    print("  channel rows:",rows," components:",dict(comp.most_common(12)),flush=True)
    print("  units:",dict(unit.most_common(12)),flush=True)
print("SMOKE2_DONE",flush=True)
