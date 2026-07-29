"""Dense Hi-net rupture-directivity extractor (research one-off, driven by GHA).

For each event in an event plan, download NIED Hi-net (NET=0101) continuous velocity
waveforms from the nearest stations within a radius, and reduce them on the runner to a
COMPACT per-event bundle: 3-band envelopes (10 Hz) plus S-window and pre-event-noise
log spectra, per station, with distance/azimuth metadata. Raw SAC never leaves the runner
(the full cohort would be several GB); the bundles are about 1 MB per event so the azimuthal
directivity features can be iterated offline without re-fetching.

TIME CONVENTION: the JMA-derived event plan carries JST wall-clock (ot_jst), which is the
same convention HinetPy uses for starttime, so no offset is applied anywhere in this file.
The earlier EarthScope extractors treated the catalogue time as UTC, which put their windows
9 h late; verified 2026-07-26 against the Kumamoto M7.3 and Tohoku M9.0 origins.

Credentials come from HINET_USER / HINET_PASS. win32tools must be on PATH.
"""
import os, sys, json, glob, time, math, tarfile, tempfile, shutil
from datetime import datetime, timedelta

import numpy as np
from HinetPy import Client, win32
from obspy import read
from scipy.signal import butter, sosfiltfilt

NET = os.environ.get("DIR_NET", "0101")
USER = os.environ["HINET_USER"]
PW = os.environ["HINET_PASS"]
EVENTS = os.environ.get("DIR_EVENTS", "scripts/hinet_dir_events.json")
I0 = int(os.environ.get("DIR_I0", "0"))
I1 = int(os.environ.get("DIR_I1", "0")) or None
MAX_STA = int(os.environ.get("DIR_MAX_STA", "40"))     # stations REQUESTED (nearest first)
MIN_STA = int(os.environ.get("DIR_MIN_STA", "8"))      # usable stations REQUIRED to keep an event
SPAN_PAD = float(os.environ.get("DIR_SPAN_PAD", "60")) # request margin (s) over the window
RADIUS = float(os.environ.get("DIR_RADIUS_KM", "200"))
PRE_S = float(os.environ.get("DIR_PRE_S", "30"))
POST_S = float(os.environ.get("DIR_POST_S", "180"))
RETRIES = int(os.environ.get("DIR_RETRIES", "3"))
RETRY_SLEEP = int(os.environ.get("DIR_RETRY_SLEEP", "45"))
OUTTAR = os.environ.get("DIR_OUTTAR", "hinet_dirfeat.tar.gz")
# Wall-clock budget. Round 38 slice 0-200 was killed by the job timeout at
# 5h50m with the tar never written, so every fetched event in it was lost.
# Stopping the loop ourselves keeps the tar+progress write reachable.
MAX_SECONDS = float(os.environ.get("DIR_MAX_SECONDS", "0"))
T_START = time.time()

ENV_SR = float(os.environ.get("DIR_ENV_SR", "10"))   # envelope sample rate (Hz)
# A 45-min post-origin window (round 38) is fetched at 1 Hz: the shape features bin to
# 10 s, so 1 Hz is already 10x finer than they read, and 10 Hz would be 27,300 samples
# per band per station.
BANDS = [(1.0, 3.0), (3.0, 8.0), (8.0, 20.0)]
NFREQ = 64
FMIN, FMAX = 0.5, 40.0
VS = 3.5                            # km/s, S-arrival estimate
SPEC_LEN = 30.0                     # S-window length (s)

plan = json.load(open(EVENTS))
plan = plan[I0:I1] if I1 else plan[I0:]
print("plan events %d (slice %d:%s) max_sta=%d radius=%.0fkm" % (
    len(plan), I0, I1, MAX_STA, RADIUS), flush=True)

cl = Client(USER, PW)
allst = []
for s in cl.get_station_list(NET):
    sid = getattr(s, "name", None) or getattr(s, "code", None)
    la = getattr(s, "latitude", None); lo = getattr(s, "longitude", None)
    if sid is None or la is None or lo is None:
        continue
    allst.append((str(sid), float(la), float(lo)))
print("Hi-net inventory:", len(allst), flush=True)


def dist_az(la1, lo1, la2, lo2):
    r1, r2 = math.radians(la1), math.radians(la2)
    dl = math.radians(lo2 - lo1)
    dd = math.acos(max(-1.0, min(1.0, math.sin(r1) * math.sin(r2) +
                                 math.cos(r1) * math.cos(r2) * math.cos(dl))))
    az = math.degrees(math.atan2(math.sin(dl) * math.cos(r2),
                                 math.cos(r1) * math.sin(r2) -
                                 math.sin(r1) * math.cos(r2) * math.cos(dl))) % 360.0
    return dd * 6371.0, az


def bin_rms(x, sr, out_sr):
    n = max(1, int(round(sr / out_sr)))
    m = (len(x) // n) * n
    if m == 0:
        return np.zeros(0, dtype=np.float32)
    return np.sqrt((x[:m].reshape(-1, n) ** 2).mean(axis=1)).astype(np.float32)


def logspec(x, sr):
    if len(x) < 64:
        return np.full(NFREQ, np.nan, dtype=np.float32)
    x = x - x.mean()
    w = np.hanning(len(x))
    a = np.abs(np.fft.rfft(x * w)) / sr
    f = np.fft.rfftfreq(len(x), 1.0 / sr)
    tgt = np.logspace(np.log10(FMIN), np.log10(min(FMAX, 0.45 * sr)), NFREQ)
    return np.interp(tgt, f[1:], a[1:]).astype(np.float32)


os.makedirs("dirfeat", exist_ok=True)
logf = open("dirfeat/extract.jsonl", "a")
nok = 0
stopped_early = False
next_pos = len(plan)
for pos, ev in enumerate(plan):
    if MAX_SECONDS and (time.time() - T_START) > MAX_SECONDS:
        stopped_early = True
        next_pos = pos
        print("budget %.0fs reached at plan position %d; stopping cleanly"
              % (MAX_SECONDS, pos), flush=True)
        break
    idx = int(ev["idx"])
    outp = "dirfeat/ev%05d.npz" % idx
    if os.path.exists(outp):
        print(idx, "already done", flush=True)
        continue
    ot = datetime.fromisoformat(ev["ot_jst"])       # JST wall clock, HinetPy convention
    sel = []
    for sid, la, lo in allst:
        d, az = dist_az(ev["lat"], ev["lon"], la, lo)
        if d <= RADIUS:
            sel.append((d, az, sid, la, lo))
    sel.sort()
    sel = sel[:MAX_STA]
    if len(sel) < MIN_STA:
        logf.write(json.dumps({"idx": idx, "ok": 0, "reason": "few_sta",
                               "n": len(sel)}) + "\n"); logf.flush()
        continue
    codes = [s[2] for s in sel]
    seg = (ot - timedelta(seconds=PRE_S)).replace(second=0, microsecond=0)
    span = int(math.ceil((PRE_S + POST_S + SPAN_PAD) / 60.0))
    got = False
    for attempt in range(RETRIES):
        work = tempfile.mkdtemp(prefix="dir_")
        try:
            try:
                cl.select_stations(NET, codes)
            except Exception:
                cl.select_stations(NET, [c.split(".")[-1] for c in codes])
            data = cl.get_continuous_waveform(NET, seg, span, outdir=work)
            if not (isinstance(data, tuple) and len(data) == 2) or data[0] is None:
                raise RuntimeError("no data returned")
            sacs = win32.extract_sac(data[0], data[1], outdir=work)
            if not sacs:
                sacs = glob.glob(os.path.join(work, "*.SAC"))
            byst = {}
            for p in sacs:
                parts = os.path.basename(p).split(".")
                if len(parts) < 4:
                    continue
                sta = parts[0] + "." + parts[1]
                byst.setdefault(sta, {})[parts[2]] = p
            rows, env, spS, spN = [], [], [], []
            nbin = int(round((PRE_S + POST_S) * ENV_SR))
            for d, az, sid, sla, slo in sel:
                comp = byst.get(sid)
                if not comp or "U" not in comp:
                    continue
                try:
                    trU = read(comp["U"])[0]
                    trN = read(comp["N"])[0] if "N" in comp else None
                    trE = read(comp["E"])[0] if "E" in comp else None
                except Exception:
                    continue
                sr = float(trU.stats.sampling_rate)
                t_off = (ot - datetime.strptime(str(trU.stats.starttime)[:19],
                                                "%Y-%m-%dT%H:%M:%S")).total_seconds()
                i0 = int(round((t_off - PRE_S) * sr))
                i1 = i0 + int(round((PRE_S + POST_S) * sr))
                if i0 < 0 or i1 > trU.stats.npts:
                    continue
                zz = trU.data.astype(float)[i0:i1]
                if trN is not None and trE is not None and \
                        trN.stats.npts >= i1 and trE.stats.npts >= i1:
                    hh = np.sqrt(trN.data.astype(float)[i0:i1] ** 2 +
                                 trE.data.astype(float)[i0:i1] ** 2)
                else:
                    hh = np.zeros_like(zz)
                zz -= zz[: int(5 * sr)].mean()
                e = np.zeros((6, nbin), dtype=np.float32)
                for bi, (lo_f, hi_f) in enumerate(BANDS):
                    sos = butter(4, [lo_f / (sr / 2), min(hi_f, 0.45 * sr) / (sr / 2)],
                                 btype="band", output="sos")
                    for ci, sig in enumerate((zz, hh)):
                        v = bin_rms(sosfiltfilt(sos, sig), sr, ENV_SR)
                        e[bi * 2 + ci, : min(nbin, len(v))] = v[:nbin]
                ts = PRE_S + d / VS
                a = int(round((ts - 2.0) * sr)); b = min(int(round((ts + SPEC_LEN) * sr)), len(zz))
                nb = int(round((PRE_S - 3.0) * sr))
                spS.append(np.vstack([logspec(zz[a:b], sr), logspec(hh[a:b], sr)]))
                spN.append(np.vstack([logspec(zz[0:nb], sr), logspec(hh[0:nb], sr)]))
                env.append(e)
                rows.append((sid, d, az, sla, slo, sr))
            if len(rows) < MIN_STA:
                raise RuntimeError("only %d usable stations (need %d of %d requested)"
                                   % (len(rows), MIN_STA, len(sel)))
            _earr = np.array(env, dtype=np.float32)
            _escale = np.maximum(_earr.reshape(len(_earr), -1).max(axis=1), 1e-12).astype(np.float32)
            _env16 = (_earr / _escale[:, None, None]).astype(np.float16)
            np.savez_compressed(
                outp,
                sta=np.array([r[0] for r in rows]),
                dist=np.array([r[1] for r in rows], dtype=np.float32),
                az=np.array([r[2] for r in rows], dtype=np.float32),
                sla=np.array([r[3] for r in rows], dtype=np.float32),
                slo=np.array([r[4] for r in rows], dtype=np.float32),
                env=_env16, envscale=_escale,
                specS=np.array(spS, dtype=np.float32),
                specN=np.array(spN, dtype=np.float32),
                meta=np.array([ev["lat"], ev["lon"], ev["mag"], PRE_S, POST_S, ENV_SR],
                              dtype=np.float64),
                ot=np.array([ev["ot_jst"]]),
                bands=np.array(BANDS, dtype=np.float32))
            azs = sorted(r[2] for r in rows)
            gaps = [(azs[(k + 1) % len(azs)] - azs[k]) % 360 for k in range(len(azs))]
            gaps[-1] = azs[0] + 360 - azs[-1]
            logf.write(json.dumps({"idx": idx, "ok": 1, "nsta": len(rows),
                                   "azgap": round(max(gaps), 1),
                                   "dmin": round(rows[0][1], 1)}) + "\n"); logf.flush()
            print(idx, "ok nsta", len(rows), "azgap %.0f" % max(gaps), flush=True)
            got = True
            nok += 1
        except Exception as e:
            print(idx, "attempt", attempt + 1, "fail", repr(e)[:160], flush=True)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if got:
            break
        if attempt + 1 < RETRIES:
            time.sleep(RETRY_SLEEP)
    if not got:
        logf.write(json.dumps({"idx": idx, "ok": 0, "reason": "fetch_fail"}) + "\n")
        logf.flush()
    time.sleep(1)

logf.close()
progress = {"i0": I0, "i1": I1, "n_planned": len(plan),
            "n_attempted": next_pos, "nok": nok,
            "complete": not stopped_early,
            "next_i0": I0 + next_pos,
            "elapsed_s": round(time.time() - T_START, 1)}
json.dump(progress, open("dirfeat/progress.json", "w"))
print("PROGRESS", json.dumps(progress), flush=True)
with tarfile.open(OUTTAR, "w:gz") as t:
    t.add("dirfeat")
print("events ok", nok, "of", len(plan), "->", OUTTAR,
      os.path.getsize(OUTTAR) if os.path.exists(OUTTAR) else 0, flush=True)
print("DONE", flush=True)
