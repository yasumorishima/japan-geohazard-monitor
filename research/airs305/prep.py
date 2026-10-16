# -*- coding: utf-8 -*-
"""Round 305 quantities (label-free): causal AIRS L3 anomalies for every 33-day issue of windows 17..35.

SUDDEN AXIS: isolated M>=6 label on all 2,280 global arena cells, 33-day issues; R2 ISO 0.771474 (size272/304).
DIMENSION CEILING (oracle upper bounds): per-(cell, 33-day block) 0.99961; per-window cell 0.98718.
WHY THIS ROUND: see research/airs305/fetch.py.  This script reads only the AIRS files and the issue times
  (research/airs305/issues305.txt, block starts of the arena; no label) and writes the quantities.

Fields: co5, ch46 (declared), t5, t6, q5, q6 (rival arm), each stored per orbit A / D.
Per field, per orbit, on grid rows 0..159 (centre 89.5 - r; arena rows are 4..155), per 10-row band:
  1 per day, minus the median of the orbit's finite values in the band (all longitudes);
  2 minus the mean over strictly earlier calendar years of the same grid point's (year, 7-day slot) means,
    slot = min(doy // 7, 51), needing >= 3 earlier years with a value (else NaN).
  3 the daily anomaly is the mean of the finite A and D anomalies (deseasonalised separately: opus plan audit).
Issue t (days since 1970-01-01): E = floor(t) - 3 (AIRS L3 days end at most 36 h after the file date: fetch gate C).
  Window = days E-29..E; reference = the 365 days before the window.  TA = (mean window - mean ref) / sd ref
  (ddof 0); TS = sd window / sd ref; needs >= 5 window days, >= 60 reference days, sd ref > 0.  Sums are taken
  directly over the days in float64 (never as differences of running sums).
Gates (each can fail):
  P0 the AIRS day axis has no duplicate day; every issue's reference starts on or after the first AIRS day.
  P1 causality, 2 bands x 3 issues per field: raw days > E of both orbits set to 1e6 -> TA bit-identical; also
     setting day E changes more than half of the finite TA values.
  P2 an independent loop-based recomputation of TA for 30 random (issue, grid point) pairs per field agrees to
     2e-5 relative (TA is stored as float32; measured, not inherited).
Usage: python prep.py AIRS_DIR ISSUES OUT.npz
"""
import datetime as dt
import glob
import hashlib
import sys
import time

import numpy as np

SRC, ISS, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
FIELDS = ("co5", "ch46", "t5", "t6", "q5", "q6")
TSF = ("co5", "ch46")
NROW = 160
NWIN, NREF, MINW, MINR, MINY, LAG = 30, 365, 5, 60, 3, 3
EPOCH = dt.date(1970, 1, 1)


def dnum(s):
    return (dt.date(*map(int, str(s).split("-"))) - EPOCH).days


def load(files, d0, nd, key):
    V = np.full((nd, NROW, 360), np.nan, np.float16)
    for f in files:
        Z = np.load(f)
        ix = np.array([dnum(s) for s in Z["days"]]) - d0
        V[ix] = Z[key][:, :NROW]
    return V


def anomaly(Vb, yr, slot):
    x = Vb.astype(np.float64)
    with np.errstate(all="ignore"):
        med = np.nanmedian(x.reshape(len(x), -1), axis=1)
    x = x - med[:, None, None]
    ys = np.unique(yr)
    m = np.full((len(ys), 52) + x.shape[1:], np.nan)
    for i, y in enumerate(ys):
        for s in range(52):
            sel = (yr == y) & (slot == s)
            if sel.any():
                with np.errstate(all="ignore"):
                    m[i, s] = np.nanmean(x[sel], axis=0)
    out = np.full_like(x, np.nan)
    for i, y in enumerate(ys):
        prev = m[:i]
        cnt = np.isfinite(prev).sum(0)
        with np.errstate(all="ignore"):
            clim = np.where(cnt >= MINY, np.nansum(prev, 0) / np.maximum(cnt, 1), np.nan)
        for s in range(52):
            sel = (yr == y) & (slot == s)
            if sel.any():
                out[sel] = x[sel] - clim[s]
    return out


def combine(xa, xd):
    n = np.isfinite(xa).astype(np.float64) + np.isfinite(xd)
    with np.errstate(all="ignore"):
        return np.where(n > 0, (np.nan_to_num(xa) + np.nan_to_num(xd)) / np.maximum(n, 1), np.nan)


def ta_ts(X, e):
    w = X[e - NWIN + 1:e + 1]
    r = X[e - NWIN - NREF + 1:e - NWIN + 1]
    nw, nr = np.isfinite(w).sum(0), np.isfinite(r).sum(0)
    with np.errstate(all="ignore"):
        mw = np.nansum(w, 0) / nw
        mr = np.nansum(r, 0) / nr
        sw = np.sqrt(np.nansum((w - mw) ** 2, 0) / nw)
        sr = np.sqrt(np.nansum((r - mr) ** 2, 0) / nr)
        ok = (nw >= MINW) & (nr >= MINR) & (sr > 0)
        return np.where(ok, (mw - mr) / sr, np.nan), np.where(ok, sw / sr, np.nan)


def slow_ta(VA, VD, yr, slot, e, rr, cc):
    """Loop-based TA at band-local row rr, column cc (P2)."""
    meds = {}

    def med(o, d):
        if (o, d) not in meds:
            v = (VA, VD)[o][d].astype(np.float64)
            v = v[np.isfinite(v)]
            meds[(o, d)] = float(np.median(v)) if v.size else np.nan
        return meds[(o, d)]

    def anom1(o, d):
        V = (VA, VD)[o]
        v = float(V[d, rr, cc])
        if not np.isfinite(v):
            return np.nan
        ms = []
        for y in range(int(yr[0]), int(yr[d])):
            vals = [float(V[k, rr, cc]) - med(o, k) for k in np.flatnonzero((yr == y) & (slot == slot[d]))
                    if np.isfinite(float(V[k, rr, cc]))]
            if vals:
                ms.append(sum(vals) / len(vals))
        return v - med(o, d) - sum(ms) / len(ms) if len(ms) >= MINY else np.nan

    def anom(d):
        a = [x for x in (anom1(0, d), anom1(1, d)) if np.isfinite(x)]
        return sum(a) / len(a) if a else np.nan

    w = [x for x in (anom(d) for d in range(e - NWIN + 1, e + 1)) if np.isfinite(x)]
    r = [x for x in (anom(d) for d in range(e - NWIN - NREF + 1, e - NWIN + 1)) if np.isfinite(x)]
    if len(w) < MINW or len(r) < MINR:
        return np.nan
    mr = sum(r) / len(r)
    sr = (sum((x - mr) ** 2 for x in r) / len(r)) ** 0.5
    return (sum(w) / len(w) - mr) / sr if sr > 0 else np.nan


def main():
    t0 = time.time()
    files = sorted(glob.glob(SRC + "/airs305_[0-9][0-9][0-9][0-9].npz"))
    days = [dnum(s) for f in files for s in np.load(f)["days"]]
    assert len(set(days)) == len(days), "P0 duplicate day"
    d0, d1 = min(days), max(days)
    nd = d1 - d0 + 1
    tiss = np.loadtxt(ISS)
    E = np.floor(tiss).astype(np.int64) - LAG - d0
    assert E.min() - NWIN - NREF + 1 >= 0, "P0 reference before the first AIRS day"
    assert E.max() <= nd - 1, "P0 issue beyond the fetched days"
    dates = [EPOCH + dt.timedelta(days=d0 + i) for i in range(nd)]
    yr = np.array([x.year for x in dates])
    slot = np.minimum(np.array([x.timetuple().tm_yday - 1 for x in dates]) // 7, 51)
    print("files %d  days %d (%s .. %s, %d present)  issues %d" % (len(files), nd, dates[0], dates[-1], len(days),
                                                                  len(tiss)), flush=True)
    out = {"tiss": tiss, "E": E + d0, "rows": np.arange(NROW)}
    rng = np.random.default_rng(305)
    for g in FIELDS:
        VA, VD = load(files, d0, nd, g + "_A"), load(files, d0, nd, g + "_D")
        TA = np.full((len(tiss), NROW, 360), np.nan, np.float32)
        TS = np.full_like(TA, np.nan)
        for b in range(NROW // 10):
            sl = slice(10 * b, 10 * b + 10)
            X = combine(anomaly(VA[:, sl], yr, slot), anomaly(VD[:, sl], yr, slot))
            for i, e in enumerate(E):
                TA[i, sl], TS[i, sl] = ta_ts(X, e)
            if b in (6, 12):
                for i in rng.choice(len(E), 3, replace=False):
                    e = E[i]
                    PA, PD = VA[:, sl].copy(), VD[:, sl].copy()
                    PA[e + 1:] = 1e6
                    PD[e + 1:] = 1e6
                    same, _ = ta_ts(combine(anomaly(PA, yr, slot), anomaly(PD, yr, slot)), e)
                    assert np.array_equal(same, TA[i, sl].astype(np.float64).astype(same.dtype), equal_nan=True) \
                        or np.array_equal(same.astype(np.float32), TA[i, sl], equal_nan=True), "P1 future %s" % g
                    PA[e] = 1e6
                    PD[e] = 1e6
                    diff, _ = ta_ts(combine(anomaly(PA, yr, slot), anomaly(PD, yr, slot)), e)
                    fin = np.isfinite(TA[i, sl])
                    assert np.mean(diff.astype(np.float32)[fin] != TA[i, sl][fin]) > 0.5, "P1 day E %s" % g
                print("%s band %d P1 PASS" % (g, b), flush=True)
            if b == 9:
                worst = 0.0
                for _ in range(30):
                    i = int(rng.integers(0, len(E)))
                    rr, cc = int(rng.integers(0, 10)), int(rng.integers(0, 360))
                    v = slow_ta(VA[:, sl], VD[:, sl], yr, slot, int(E[i]), rr, cc)
                    ref = float(TA[i, 10 * b + rr, cc])
                    if not np.isfinite(v):
                        assert not np.isfinite(ref), "P2 coverage %s" % g
                        continue
                    rel = abs(v - ref) / max(1.0, abs(v))
                    worst = max(worst, rel)
                    assert rel <= 2e-5, "P2 %s %.9g vs %.9g" % (g, v, ref)
                print("%s P2 PASS (30 pairs, worst relative %.2e)" % (g, worst), flush=True)
            del X
        out["TA_" + g] = TA
        if g in TSF:
            out["TS_" + g] = TS
        print("%s done %.0f s  finite TA %.4f" % (g, time.time() - t0, np.isfinite(TA[:, 4:156]).mean()),
              flush=True)
        del VA, VD
    np.savez_compressed(OUT, **out)
    print("md5 %s  DONE %.0f s" % (hashlib.md5(open(OUT, "rb").read()).hexdigest(), time.time() - t0), flush=True)


if __name__ == "__main__":
    main()
