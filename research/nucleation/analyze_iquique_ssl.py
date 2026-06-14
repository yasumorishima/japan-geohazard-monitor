"""Pre-registered verdict for the 2014 Iquique self-supervised waveform-
embedding nucleation probe (test 10 of the arc; a multi-case extension of the
Kumamoto knuc15 test, which was null). Iquique is a NEAR-POSITIVE CONTROL: a
foreshock migration toward the eventual Mw8.1 rupture was reported (Kato &
Nakagawa 2014; Ruiz et al. 2014), yet the naive auto micro-catalogue used here
shows none (corr day vs median-distance +0.104, daily rate decreasing). This
asks whether the learned (self-supervised, label-free) representation detects
the reported nucleation where naive catalogue statistics do not.

Same harness as analyze_knuc15.py (committed before that kernel ran). This
file is committed BEFORE the knuc16 kernel (yasunorim/ssl-iquique) is pushed or
any embedding is computed.

KERNEL OUTPUT (out/embeddings.csv): idx,time,mag,depth,lat,lon,dist_ms,snr,
e0..e{D-1}; one row per micro-catalogue event recorded by the CORE fixed CX
station set; time = epoch s (UTC); dist_ms = epicentral distance (km) to the
Mw8.1 mainshock epicentre -19.61/-70.77; embedding = L2-normalised SimCLR
encoder trained EARLY-ONLY (first 30 percent of [FS, MS)).

CONFOUND CONTROL (identical rationale to knuc15): event-aligned (1 embedding
per event => rate-free); M- and depth-MATCHED binning (late-time magnitude
shifts cannot masquerade as drift); SNR negation gate.

DRIFT = euclidean distance of a time bin matched centroid from the early
baseline centroid (matched centroid of the first 30 percent of [FS, MS)).

CRITERIA (events in [FS, MS), BIN_H-hour M/depth-matched bins):
  P1 trend : Theil-Sen slope of drift vs hours. PASS if |slope*span| >=
     2*noise (noise = median per-bin bootstrap std) AND monotone-frac >= 0.60.
  P2 final step : mean drift in final FINAL_H h >= 1.5 * mean drift before.
CONTROLS (positive detection requires ALL):
  C1 time-shuffle : observed slope > 95th pct of N_SHUF time-shuffled nulls.
  C2 surrogate-prospective : observed full-window slope > the slope measured
     WITHIN the early-only window [FS, FS+0.30*span] (and > 0).
  C3 spatial null : drift slope for NEAR (dist_ms <= NEAR_KM) > FAR
     (dist_ms >= FAR_KM), and slope_near > 0.
Sanity gate: >= 6 bins with >= 20 events and >= D+10 baseline events.
SNR gate (report-only): |r(bin drift, bin SNR)| >= 0.8 flags artifact risk.
VERDICT = PASS only if P1 AND C1 AND C2 AND C3 (and SNR gate not tripped).
All thresholds fixed a priori. NOTE (registered): C2 has a known FAIL-direction
bias (early-only encoder drift inflates the early-internal slope); a NULL is
therefore carried primarily by the bias-free C3 spatial null and by P1.
"""
import csv, math, sys
from datetime import datetime, timedelta
import numpy as np

EMB = sys.argv[1] if len(sys.argv) > 1 else "/home/yasu/geo-ml/knuc16/out_k/embeddings.csv"
MS_T = datetime(2014, 4, 1, 23, 46, 0)
FS_T = datetime(2014, 3, 16, 23, 48, 0)
BIN_H = 12.0
FINAL_H = 24.0
N_SHUF = 1000
NEAR_KM = 20.0
FAR_KM = 40.0
N_MAGQ = 3
N_DEPQ = 3
RNG = np.random.default_rng(0)

rows = []
with open(EMB) as f:
    rd = csv.DictReader(f)
    ecols = [c for c in rd.fieldnames if c and c[0] == "e" and c[1:].isdigit()]
    ecols.sort(key=lambda c: int(c[1:]))
    for r in rd:
        try:
            t = datetime.utcfromtimestamp(float(r["time"]))
            mg = float(r["mag"]); dp = float(r["depth"]); ds = float(r["dist_ms"])
            sn = float(r["snr"])
            ev = np.array([float(r[c]) for c in ecols], dtype=np.float64)
        except (ValueError, KeyError):
            continue
        if FS_T <= t < MS_T:
            rows.append((t, mg, dp, ds, sn, ev))
rows.sort(key=lambda z: z[0])
D = len(ecols)
print("input:", EMB, "| D=%d | events in [FS,MS): %d" % (D, len(rows)))
if len(rows) < 120:
    print("VERDICT: INSUFFICIENT DATA (events=%d <120)" % len(rows)); sys.exit(0)

th = np.array([(t - FS_T).total_seconds() / 3600.0 for t, *_ in rows])
mag = np.array([z[1] for z in rows]); dep = np.array([z[2] for z in rows])
dms = np.array([z[3] for z in rows]); snr = np.array([z[4] for z in rows])
E = np.array([z[5] for z in rows], dtype=np.float64)
nE = np.linalg.norm(E, axis=1, keepdims=True); nE[nE == 0] = 1.0
E = E / nE
span = th[-1] - th[0]

def qbins(v, nq):
    qs = np.quantile(v, np.linspace(0, 1, nq + 1))
    qs[0] -= 1e-9; qs[-1] += 1e-9
    return np.clip(np.digitize(v, qs[1:-1]), 0, nq - 1)
mq = qbins(mag, N_MAGQ); dq = qbins(dep, N_DEPQ)
strat = mq * N_DEPQ + dq
NS = N_MAGQ * N_DEPQ
wref = np.array([np.mean(strat == s) for s in range(NS)])

def matched_centroid(mask):
    idx = np.where(mask)[0]
    if len(idx) == 0:
        return None
    cen = np.zeros(D); wsum = 0.0
    for s in range(NS):
        sel = idx[strat[idx] == s]
        if len(sel) == 0 or wref[s] == 0:
            continue
        cen += wref[s] * E[sel].mean(axis=0); wsum += wref[s]
    if wsum == 0:
        return None
    return cen / wsum

def theil(x, y):
    s = [(y[j] - y[i]) / (x[j] - x[i])
         for i in range(len(x)) for j in range(i + 1, len(x)) if x[j] != x[i]]
    return float(np.median(s)) if s else 0.0

def drift_series(times, base_mask, bin_edges):
    base = matched_centroid(base_mask)
    if base is None:
        return None, None, None
    bb = []; dd = []; nn = []
    for k in range(len(bin_edges) - 1):
        m = (times >= bin_edges[k]) & (times < bin_edges[k + 1])
        if m.sum() < 1:
            continue
        c = matched_centroid(m)
        if c is None:
            continue
        bb.append(0.5 * (bin_edges[k] + bin_edges[k + 1]))
        dd.append(float(np.linalg.norm(c - base))); nn.append(int(m.sum()))
    return np.array(bb), np.array(dd), np.array(nn)

edges = np.arange(0.0, span + BIN_H, BIN_H)
base_mask = th < (0.30 * span)
bc, dr, nev = drift_series(th, base_mask, edges)
nbin = 0 if bc is None else len(bc)
nbase = int(base_mask.sum())
print("bins=%d | baseline events=%d | per-bin events min/med/max=%s/%s/%s"
      % (nbin, nbase, nev.min() if nbin else "-", int(np.median(nev)) if nbin else "-",
         nev.max() if nbin else "-"))
goodbins = nbin >= 6 and (nev >= 20).sum() >= 6 if nbin else False
if not goodbins or nbase < D + 10:
    print("VERDICT: INSUFFICIENT DATA (good-bins gate or baseline<%d)" % (D + 10)); sys.exit(0)

slope = theil(bc, dr); total = slope * (bc[-1] - bc[0])
boot = []
for k in range(len(edges) - 1):
    m = np.where((th >= edges[k]) & (th < edges[k + 1]))[0]
    if len(m) < 5:
        continue
    base = matched_centroid(base_mask)
    ds_ = []
    for _ in range(120):
        bs = RNG.choice(m, size=len(m), replace=True)
        msk = np.zeros(len(th), bool); msk[bs] = True
        c = matched_centroid(msk)
        if c is not None:
            ds_.append(np.linalg.norm(c - base))
    if len(ds_) > 3:
        boot.append(np.std(ds_))
noise = float(np.median(boot)) if boot else float("nan")
side = np.sign(dr - dr[0]); mono = float(np.mean(side == np.sign(slope))) if slope != 0 else 0.0
p1 = abs(total) >= 2 * noise and mono >= 0.60
print("P1 trend: Theil-Sen=%.5f /h total=%.4f (need|total|>=%.4f, noise=%.4f) mono=%.2f -> %s | %s"
      % (slope, total, 2 * noise, noise, mono, "PASS" if p1 else "FAIL",
         "RISE (precursor prior)" if total > 0 else "FALL"))

fin = dr[bc >= (span - FINAL_H)]; basw = dr[bc < (span - FINAL_H)]
p2 = len(fin) >= 1 and len(basw) >= 1 and fin.mean() >= 1.5 * basw.mean()
print("P2 final-step: final%gh drift=%.4f vs baseline drift=%.4f ratio=%.2f -> %s"
      % (FINAL_H, fin.mean() if len(fin) else float("nan"),
         basw.mean() if len(basw) else float("nan"),
         (fin.mean() / basw.mean()) if len(basw) and basw.mean() else float("nan"),
         "PASS" if p2 else "FAIL"))

shuf = []
for _ in range(N_SHUF):
    ts = th[RNG.permutation(len(th))]
    bc2, dr2, _ = drift_series(ts, ts < (0.30 * span), edges)
    if bc2 is not None and len(bc2) >= 4:
        shuf.append(theil(bc2, dr2))
shuf = np.array(shuf); thr95 = float(np.percentile(shuf, 95)) if len(shuf) else float("nan")
c1 = slope > thr95
print("C1 shuffle: observed=%.5f vs null p95=%.5f (n=%d) -> %s"
      % (slope, thr95, len(shuf), "PASS" if c1 else "FAIL"))

ew = 0.30 * span
edges_e = np.arange(0.0, ew + BIN_H, BIN_H)
bce, dre, _ = drift_series(th, th < (0.15 * span), edges_e)
sur = theil(bce, dre) if (bce is not None and len(bce) >= 3) else float("nan")
c2 = (not math.isnan(sur)) and slope > sur and slope > 0
print("C2 surrogate: full-window slope=%.5f vs early-internal slope=%.5f -> %s"
      % (slope, sur, "PASS" if c2 else "FAIL"))

def drift_slope_subset(sub):
    base = matched_centroid(base_mask & sub)
    if base is None:
        return float("nan")
    bb = []; dd = []
    for k in range(len(edges) - 1):
        m = (th >= edges[k]) & (th < edges[k + 1]) & sub
        if m.sum() < 5:
            continue
        c = matched_centroid(m)
        if c is None:
            continue
        bb.append(0.5 * (edges[k] + edges[k + 1])); dd.append(np.linalg.norm(c - base))
    return theil(np.array(bb), np.array(dd)) if len(bb) >= 3 else float("nan")
near = dms <= NEAR_KM; far = dms >= FAR_KM
sn_near = drift_slope_subset(near); sn_far = drift_slope_subset(far)
c3 = (not math.isnan(sn_near)) and (math.isnan(sn_far) or sn_near > sn_far) and sn_near > 0
print("C3 spatial: slope_near(<=%gkm,n=%d)=%.5f vs slope_far(>=%gkm,n=%d)=%.5f -> %s"
      % (NEAR_KM, int(near.sum()), sn_near, FAR_KM, int(far.sum()), sn_far, "PASS" if c3 else "FAIL"))

binsnr = []
for k in range(len(edges) - 1):
    m = (th >= edges[k]) & (th < edges[k + 1])
    if m.sum() >= 5:
        binsnr.append(snr[m].mean())
binsnr = np.array(binsnr[:len(dr)])
rg = float(np.corrcoef(dr[:len(binsnr)], binsnr)[0, 1]) if len(binsnr) >= 3 else float("nan")
snr_trip = (not math.isnan(rg)) and abs(rg) >= 0.8
print("SNR gate (report-only): r(drift,snr)=%.3f -> %s" % (rg, "TRIPPED" if snr_trip else "clean"))

verdict = p1 and c1 and c2 and c3 and not snr_trip
print("VERDICT P1=%s C1=%s C2=%s C3=%s SNRgate=%s -> %s"
      % (p1, c1, c2, c3, "clean" if not snr_trip else "TRIPPED",
         "DETECTION (nucleation signal survives all controls)" if verdict else "NULL"))
