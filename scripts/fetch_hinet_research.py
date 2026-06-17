"""One-off research fetch: NIED Hi-net continuous waveforms for a chosen
window/region, extracted to SAC and tarred as a GHA artifact. Drives the
raw-waveform nucleation concept test (separate from the production feature
fetcher). Credentials come from HINET_USER/HINET_PASS (repo secrets).
win32tools (catwin32/win2sac_32) must be built and on PATH by the workflow.
FETCH_HOURS_LIST (comma-separated start datetimes) overrides START/HOURS to
re-fetch specific failed segments only."""
import os, sys, time, glob, tarfile, tempfile, shutil
from datetime import datetime, timedelta

from HinetPy import Client, win32

NET = os.environ.get("FETCH_NET", "0101")
USER = os.environ["HINET_USER"]
PW = os.environ["HINET_PASS"]
START = os.environ.get("FETCH_START", "2011-03-09T11:00")
HOURS = int(os.environ.get("FETCH_HOURS", "1"))
HOURS_LIST = os.environ.get("FETCH_HOURS_LIST", "").strip()
MAX_STA = int(os.environ.get("FETCH_MAX_STA", "10"))
RETRIES = int(os.environ.get("FETCH_RETRIES", "3"))
RETRY_SLEEP = int(os.environ.get("FETCH_RETRY_SLEEP", "60"))
LAT0 = float(os.environ.get("FETCH_LAT0", "36.0"))
LAT1 = float(os.environ.get("FETCH_LAT1", "41.0"))
LON0 = float(os.environ.get("FETCH_LON0", "140.0"))
LON1 = float(os.environ.get("FETCH_LON1", "143.5"))
OUTTAR = os.environ.get("FETCH_OUTTAR", "hinet_research.tar.gz")

if HOURS_LIST:
    seg_starts = [datetime.fromisoformat(x.strip().replace("Z", ""))
                  for x in HOURS_LIST.split(",") if x.strip()]
    print("explicit segment list (HinetPy local/JST convention):",
          len(seg_starts), "segments", flush=True)
else:
    start_dt = datetime.fromisoformat(START.replace("Z", ""))
    seg_starts = [start_dt + timedelta(hours=h) for h in range(HOURS)]
    print("window start (HinetPy local/JST convention):", start_dt,
          "hours", HOURS, flush=True)

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
sel_all = sorted(sel)
sel = sel_all[:MAX_STA]
print("selected", len(sel), "of", len(sel_all), "in-box stations:", sel, flush=True)
if not sel:
    print("NO STATIONS IN BOX", flush=True)
    sys.exit(1)
codes = [c for c, _, _ in sel]

# coords for ALL in-box stations: Hi-net can return more stations than
# selected (account-global selection state), and association silently
# drops picks from stations without coordinates
with open("station_coords.csv", "w") as f:
    f.write("station,latitude,longitude\n")
    for c, la, lo in sel_all:
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
ok_segs = 0
for seg in seg_starts:
    tag = seg.strftime("%Y%m%d%H%M")
    got = False
    for attempt in range(RETRIES):
        work = tempfile.mkdtemp(prefix="hn_")
        # re-assert station selection right before each request: the production
        # backfill hinet cron mutates the account-global selection mid-run
        # (2026-06-10: 3.5h run raced 3-hourly cron, late segments returned a
        # nationwide station set with only 3 in-box stations)
        try:
            cl.select_stations(NET, codes)
        except Exception:
            try:
                cl.select_stations(NET, [c.split(".")[-1] for c in codes])
            except Exception as e2:
                print(tag, "re-select failed", repr(e2)[:100], flush=True)
        try:
            data = cl.get_continuous_waveform(NET, seg, 60, outdir=work)
            if (not (isinstance(data, tuple) and len(data) == 2)
                    or data[0] is None or data[1] is None):
                print(tag, "attempt", attempt + 1, "no data (quota/throttle?)", flush=True)
            else:
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
                got = True
        except Exception as e:
            print(tag, "attempt", attempt + 1, "fetch fail", repr(e)[:180], flush=True)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if got:
            break
        if attempt + 1 < RETRIES:
            time.sleep(RETRY_SLEEP)
    if got:
        ok_segs += 1
    time.sleep(2)

print("segments ok", ok_segs, "/", len(seg_starts), flush=True)
with tarfile.open(OUTTAR, "w:gz") as t:
    t.add(sacdir)
    if os.path.exists("station_coords.csv"):
        t.add("station_coords.csv")
sz = os.path.getsize(OUTTAR) if os.path.exists(OUTTAR) else 0
print("wrote", OUTTAR, "bytes", sz, "total_sac", total, flush=True)
print("DONE", flush=True)
