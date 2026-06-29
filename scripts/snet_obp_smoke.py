"""S-net OBP smoke: confirm S-net (0120A) is fetchable with our NIED creds and
identify the ocean-bottom PRESSURE channels (codes / unit / sampling) by dumping
the win32 channel table. Self-contained; does not touch the production fetcher."""
import os
from datetime import datetime
from HinetPy import Client, win32
USER=os.environ["HINET_USER"]; PW=os.environ["HINET_PASS"]
NET="0120A"  # S-net
cl=Client(USER,PW)
st=cl.get_station_list(NET)
print("S-net station_list len:",len(st),flush=True)
for s in st[:5]:
    print("  sample:",getattr(s,"name",None),getattr(s,"latitude",None),getattr(s,"longitude",None),flush=True)
cand=[]
for s in st:
    la=getattr(s,"latitude",None); lo=getattr(s,"longitude",None)
    nm=getattr(s,"name",None) or getattr(s,"code",None)
    if la is None or lo is None or nm is None: continue
    la=float(la); lo=float(lo)
    if 36<=la<=41 and 140<=lo<=145: cand.append((str(nm),la,lo))
cand.sort(key=lambda v:(v[1]-38.0)**2+((v[2]-142.5)*0.77)**2)
print("offshore-Tohoku in-box:",len(cand)," nearest5:",cand[:5],flush=True)
codes=[c for c,_,_ in cand[:3]] if cand else [str(getattr(st[0],"name",None))]
print("selecting:",codes,flush=True)
try:
    cl.select_stations(NET,codes)
except Exception as e:
    print("select_stations err:",repr(e)[:120],flush=True)
    cl.select_stations(NET,[c.split(".")[-1] for c in codes])
os.makedirs("work",exist_ok=True)
seg=datetime(2022,6,1,0,0)  # HinetPy/JST convention, S-net continuous era
data=cl.get_continuous_waveform(NET,seg,10,outdir="work")
w32,cht=data
print("cnt:",w32," ch:",cht,flush=True)
print("=== CHANNEL TABLE (.ch) ===",flush=True)
try:
    print(open(cht).read(),flush=True)
except Exception as e:
    print("ch read err:",repr(e)[:120],flush=True)
print("=== extract_sac ===",flush=True)
try:
    sacs=win32.extract_sac(w32,cht,outdir="work")
    print("sac count:",len(sacs),flush=True)
    for p in sorted(sacs)[:60]: print("  ",os.path.basename(p),flush=True)
except Exception as e:
    print("extract_sac err:",repr(e)[:200],flush=True)
print("SMOKE_DONE",flush=True)
