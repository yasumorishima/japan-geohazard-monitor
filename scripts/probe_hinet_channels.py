"""Diagnostic probe: does the NIED Hi-net "0101" continuous-waveform stream
expose the co-located borehole TILTMETER channels, or only the 3-component
velocity seismometer? Fetches a short Kumamoto-area segment, dumps the full
win32 channel table, and lists every extracted SAC component. One-off check
for the geodetic-channel nucleation pivot (tilt is the proper detector for the
Kato et al. 2016 slow-slip mechanism). Credentials: HINET_USER/HINET_PASS."""
import os, glob
from datetime import datetime
from HinetPy import Client, win32

cl = Client(os.environ["HINET_USER"], os.environ["HINET_PASS"])
NET = "0101"
LAT0, LAT1, LON0, LON1 = 32.4, 33.2, 130.3, 131.3
allst = cl.get_station_list(NET)
sel = []
for s in allst:
    sid = getattr(s, "name", None) or getattr(s, "code", None)
    la = getattr(s, "latitude", None); lo = getattr(s, "longitude", None)
    if sid is None or la is None or lo is None:
        continue
    la = float(la); lo = float(lo)
    if LAT0 <= la <= LAT1 and LON0 <= lo <= LON1:
        sel.append(str(sid))
print("kumamoto-box 0101 stations:", len(sel), sel[:6], flush=True)
codes = sel[:2]
try:
    cl.select_stations(NET, codes)
except Exception:
    cl.select_stations(NET, [c.split(".")[-1] for c in codes])

os.makedirs("work", exist_ok=True)
seg = datetime(2016, 4, 15, 12, 0)
data = cl.get_continuous_waveform(NET, seg, 5, outdir="work")
w32, cht = data
print("=== CHANNEL TABLE FILE:", cht, "===", flush=True)
with open(cht) as f:
    print(f.read(), flush=True)
sacs = win32.extract_sac(w32, cht, outdir="work")
if not sacs:
    sacs = glob.glob("work/*.SAC")
print("=== EXTRACTED SAC FILES (", len(sacs), ") ===", flush=True)
for p in sorted(sacs):
    print(os.path.basename(p), flush=True)
print("PROBE DONE", flush=True)
