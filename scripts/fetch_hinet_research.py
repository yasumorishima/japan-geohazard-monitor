"""One-off research fetch: NIED Hi-net continuous waveforms for a chosen
window/region, extracted to SAC and tarred as a GHA artifact. Drives the
raw-waveform nucleation concept test (separate from the production feature
fetcher). Credentials come from HINET_USER/HINET_PASS (repo secrets).
win32tools (catwin32/win2sac_32) must be built and on PATH by the workflow."""
import os, sys, time, glob, tarfile, tempfile, shutil
from datetime import datetime, timedelta

from HinetPy import Client, win32

NET = "0101"
USER = os.environ["HINET_USER"]
PW = os.environ["HINET_PASS"]
START = os.environ.get("FETCH_START", "2011-03-09T11:00")
HOURS = int(os.environ.get("FETCH_HOURS", "1"))
MAX_STA = int(os.environ.get("FETCH_MAX_STA", "10"))
LAT0 = float(os.environ.get("FETCH_LAT0", "36.0"))
LAT1 = float(os.environ.get("FETCH_LAT1", "41.0"))
LON0 = float(os.environ.get("FETCH_LON0", "140.0"))
LON1 = float(os.environ.get("FETCH_LON1", "143.5"))
OUTTAR = os.environ.get("FETCH_OUTTAR", "hinet_research.tar.gz")

start_dt = datetime.fromisoformat(START.replace("Z", ""))
print("window start (HinetPy local/JST convention):", start_dt, "hours", HOURS, flush=True)

cl = Client(USER, PW)

allst = cl.get_station_list(NET)
sel = []
for s in allst:
    sid = getattr(s, "name", None) or getattr(s, "code", None)
    la = getattr(s, "latitude", None)
    lo = getattr(s, "longitude", None)
    if sid is None or la is None or lo is None:
        continue
    la = float(la); lo = float(lo)
    if LAT0 <= la <= LAT1 and LON0 <= lo <= LON1:
        sel.append((str(sid), la, lo))
sel = sorted(sel)[:MAX_STA]
print("selected", len(sel), "stations:", sel, flush=True)
if not sel:
    print("NO STATIONS IN BOX", flush=True)
    sys.exit(1)
codes = [c for c, _, _ in sel]

with open("station_coords.csv", "w") as f:
    f.write("station,latitude,longitude\n")
    for c, la, lo in sel:
        f.write(c + "," + str(la) + "," + str(lo) + "\n")

try:
    cl.select_stations(NET, codes)
except Exception as e:
    stripped = [c.split(".")[-1] for c in codes]
    print("select as-is failed, retry stripped:", repr(e)[:100], flush=True)
    cl.select_stations(NET, stripped)

sacdir = "hinet_sac"
os.makedirs(sacdir, exist_ok=True)
total = 0
for h in range(HOURS):
    seg = start_dt + timedelta(hours=h)
    tag = seg.strftime("%Y%m%d%H%M")
    work = tempfile.mkdtemp(prefix="hn_")
    try:
        data = cl.get_continuous_waveform(NET, seg, 60, outdir=work)
        if not (isinstance(data, tuple) and len(data) == 2) or data[0] is None:
            print(tag, "no data", flush=True)
            continue
        w32, cht = data
        sacs = win32.extract_sac(w32, cht, outdir=work)
        if not sacs:
            sacs = glob.glob(os.path.join(work, "*.SAC"))
        n = 0
        for p in sacs:
            shutil.move(p, os.path.join(sacdir, tag + "." + os.path.basename(p)))
            n += 1
        total += n
        print(tag, "sac files", n, flush=True)
    except Exception as e:
        print(tag, "fetch fail", repr(e)[:180], flush=True)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    time.sleep(2)

with tarfile.open(OUTTAR, "w:gz") as t:
    t.add(sacdir)
    if os.path.exists("station_coords.csv"):
        t.add("station_coords.csv")
sz = os.path.getsize(OUTTAR) if os.path.exists(OUTTAR) else 0
print("wrote", OUTTAR, "bytes", sz, "total_sac", total, flush=True)
print("DONE", flush=True)
