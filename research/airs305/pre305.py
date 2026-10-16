# -*- coding: utf-8 -*-
"""ROUND 305 (pre-registered; frozen before any AIRS quantity is read together with a label)
Do causal AIRS L3 CO (500 hPa) and CH4 (400 hPa) anomalies order isolated M>=6 mainshocks block by block?

SUDDEN AXIS: isolated M>=6 (GK) label on all 2,280 global arena cells, 33-day issues: the 33-day refit R2 has ISO
  0.771474 (size272 / build305 reproduce it on every window).  Goal 0.90.
DIMENSION CEILING (oracle upper bounds, not limits): per-(cell, 33-day block) 0.99961; per-window cell 0.98718.
WHY THIS ROUND: sizing 304 found that the arena's own catalogue history before an issue adds ~0 to R2, so the
  block-by-block information must come from a new observation system.  AIRS L3 is daily, global, and covers the
  ocean, where 47 % of isolated M>=6 events lie (size305a).  Case studies report CO / CH4 increases around large
  earthquakes, mostly in column totals and with co- and post-seismic timing (Cui et al. 2017, Chem. Geol. 469,
  185-191, AIRS CH4 / CO, Wenchuan and Lushan; Cui et al. 2013, NHESS 13, 2513, CO up in 12 of 35 events, irregular
  timing; Singh et al. 2010, Appl. Geochem. 25, 580, CO before Gujarat 2001, pre-AIRS).  None tested a 3-to-67-day
  lead on a global population; this round does.  If a gas holds, it is the first catalogue-free time-varying
  carrier on this arena and the next round builds an arm on it; if not, the result is recorded and the next
  observation system is sized.

DATA: AIRS3STD v7.0 (doi:10.5067/UO3Q64CTTS1U; NASA data CC0), research/airs305/fetch.py, public on HF
  yasumorishima/japan-geohazard:inputs/round305_airs/ (24 yearly files 2002-2025); quantities by
  research/airs305/prep.py into q305.npz (label-free; gates P0-P3 in its docstring).  Every input and script
  is md5-listed in research/airs305/FREEZE305.md5; the run step checks it with md5sum -c and pre305 checks it again
  (G0) before reading any label together with a field.
  Labels and R2 scores: build305.npz (RPi5 ~/geo-ml/build305.py from size304's code path; G9 reproduces size272's
  ISO R2 on every window to 0.0e+00; the bundle reproduces R2 per window to 1e-12), public on HF under
  derived/round305/ (USGS catalogue, public domain, and quantities derived from it).
QUANTITY: TA (prep.py): A and D deseasonalised separately against strictly earlier years and averaged; window =
  30 days ending E = floor(issue) - 3; reference = the 365 days before.  The value of unit (cell, block) is TA at
  the cell's grid point for the block's issue.
DECLARED: TA_co5 and TA_ch46, larger -> more events (direction from the literature above, not from these data).
POPULATION: all units (cell, block) of windows 17..35 (2007-2025; window j = year 1990 + j), 2,280 cells x 223
  blocks.  Row weights as size304: positive rows = isolated M>=6 label rows in the block, negatives = rows without
  any label.  Pairs inside one unit (forced ties) are excluded from every AUC (opus plan audit).
STRATA: (window, quintile of the unit's R2 ISO score among the window's units; ties broken by seed 3050 + j).
STATISTIC: SA = pair-weighted mean over strata of the within-stratum AUC (midranks) on covered units.
FLOORS (1,000 worlds each; in every world the real SA and the null SA are computed on the SAME units: those
  covered by both the real and the transformed value -- opus plan audit; p = (1 + #{null >= real}) / 1001):
  T  circular shift of the block axis by k, same k for every cell, uniform over the shifts that put every source
     block at least 395 days from its target (no day shared between one block's 30-day window and the other's
     window or 365-day reference; opus code audit); the number of such shifts is printed.
  L  longitude rotation: the value is read from grid column (c + k) mod 360, same row, k uniform in [20, 340].
  Y  season-keeping year derangement of the 19 windows with |source year - target year| >= 2 (no shared window or
     reference day): a unit takes the value of the same cell in the block of the source year nearest in circular
     day of year (gate G4: within 5 days).
  A  covered values permuted within each stratum (anti-conservative alone; kept in the max).
VERDICT per gas: SA > 0.5 and p_max = max over T, L, Y, A; Holm over the two gases (smaller p_max <= 0.025,
  the other <= 0.05).  SUPPORTED only if also
  (i)  the ANY-not-ISO control (positives = rows with an M>=6 label that are not isolated-M>=6 rows) has
       SA >= 0.5 on the same covered units (round 284 rule; ANY minus ISO so the control does not re-count ISO);
  (ii) the rival-arm residual holds: the gas TA minus, per window, its least-squares fit on the TA of air
       temperature and water vapour at the same level (t5 / q5 for CO, t6 / q6 for CH4) on the jointly covered
       units has SA > 0.5 with p_max <= 0.05 (all four floors applied to the three fields jointly; the fit uses
       every block of the window, i.e. field values after the issue -- field only, no label; disclosed);
  (iii) no single window and no single 60-degree longitude sector carries it: SA with any one of the 19 windows
       removed, and SA with any one of the six sectors [-180,-120), ..., [120,180) removed, all stay > 0.5
       (opus code audit: one regional coincidence, e.g. a fire-CO episode over a region with isolated events in
       the same year, is not broken by the timing floors when it is real co-occurrence in the sample).
REPORTED (no verdict): SA of t5, q5, t6, q6 and |TA|, TS of each gas (floor T and A p); land classes (ocean /
  mixed / land by the 0.1-degree land fraction); 2007-2015 vs 2016-2025 and 2007-2021 vs 2022-2025 (Aqua's
  orbit drifts after 2022); R2 quintiles; per-window SA; SA without the units whose cell had an M>=5 in the 60
  days before the issue (rec305.npz; a field that responds to recent quakes) and SA on those units only.
CALIBRATION (label-free, run before the freeze, cal305.py; see its docstring): 100 synthetic label sets drawn
  independently of the field as event times (rate proportional to the R2 score, times a lognormal region x year
  burst) painted onto the rows exactly as the arena paints labels (a row is positive if an event falls in the 34
  days after it, so one event marks two consecutive blocks), 100 worlds per floor, for the plain gas and for the
  residual of both gases: the share of label sets with p <= 0.05 must be in [0.01, 0.10] for floors T, L and Y
  and <= 0.10 for p_max (floor A alone is anti-conservative and only reported); combine305.py prints PASS / FAIL
  and its output is part of the freeze.  POWER (same script): synthetic labels with log-odds tilt beta per sd of
  TA_co5 (beta 0.10, 0.20), probability that SA exceeds the 95 % point of the real-field floor-A null (an upper
  bound for the max-p verdict).
GATES (none reads the real labels together with a field): G0 the md5 of every file in FREEZE305.md5 (verdict run
  only), 223 blocks, 2,280 cells on exact 1-degree centres, arena rows inside 0..159; G1 every allowed T shift and
  every L rotation 20..340 and 20 Y draws change more than 50 % of the jointly finite values (the k = 0 identities
  are sanity checks that cannot fail); G2 SA equals a brute-force pair recount (u != v) on a 400-unit subsample of
  3 strata (1e-12), with synthetic row counts; G3 floors T, L, Y on a planted label (one positive row on the top
  10 % of TA_co5 in each stratum, one negative row elsewhere) have mean < 0.6 (the planted SA > 0.95 is built
  from the field and cannot fail); G4 Y's nearest-day-of-year match is within 5 days for every block.  Every real
  SA in a floor must be finite.
NOT CLAIMED: nothing about any single earthquake; NOT SUPPORTED does not exclude an effect below the printed
  power, nor an effect confined to one region.
Usage: python pre305.py BUNDLE Q305 REC305 [--worlds N] [--gates-only]
"""
import hashlib
import os
import sys
import time

import numpy as np

NW_DEFAULT = 1000
MIN_GAP = 395.0          # days: one block's window + reference (30 + 365) never meets the other's window
FREEZE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "FREEZE305.md5")
GASES = (("co5", "t5", "q5"), ("ch46", "t6", "q6"))
FLOORS = ("T", "L", "Y", "A")


def md5(p):
    return hashlib.md5(open(p, "rb").read()).hexdigest()


def check_freeze(paths):
    """G0: every file listed in FREEZE305.md5 matches, and every given input is listed (by basename)."""
    base = os.path.dirname(FREEZE)
    listed = {}
    for ln in open(FREEZE, encoding="utf-8"):
        if not ln.strip() or ln.startswith("#"):
            continue
        h, f = ln.split(None, 1)
        f = f.strip().lstrip("*")
        assert md5(os.path.join(base, f)) == h, "G0 md5 %s" % f
        listed[os.path.basename(f)] = h
    for p in paths:
        assert listed.get(os.path.basename(p)) == md5(p), "G0 input not frozen: %s" % p
    for f in ("pre305.py", "prep.py", "cal305.py", "combine305.py", "build305.npz", "q305.npz", "rec305.npz",
              "issues305.txt", "fetch.py"):
        assert f in listed, "G0 %s missing from the freeze" % f
    print("G0 PASS freeze: %d files match FREEZE305.md5" % len(listed), flush=True)


class Arena:
    def __init__(self, bundle, q305):
        B = np.load(bundle)
        self.win, self.tiss = B["win"].astype(int), B["tiss"]
        self.P, self.X, self.N = (B[k].astype(np.float64) for k in ("pos_iso", "pos_anyx", "neg"))
        self.S = B["r2_iso"]
        clat, clon = B["clat"], B["clon"]
        self.NB, self.NC = self.P.shape
        self.r = np.rint(89.5 - clat).astype(int)
        self.c = np.rint(clon + 179.5).astype(int)
        assert self.NB == 223 and self.NC == 2280, "G0 shape"
        assert np.all(np.abs(89.5 - clat - self.r) < 1e-9) and np.all(np.abs(clon + 179.5 - self.c) < 1e-9), \
            "G0 cell centres off the 1-degree grid"
        assert self.r.min() >= 0 and self.r.max() < 160 and self.c.min() >= 0 and self.c.max() < 360, "G0 rows"
        self.Q = np.load(q305)
        self._g = {}
        assert np.allclose(self.Q["tiss"], self.tiss), "G0 issue times differ"
        self.wins = np.unique(self.win)
        # strata: (window, R2 quintile)
        self.strat = np.zeros((self.NB, self.NC), np.int64)
        for j in self.wins:
            m = self.win == j
            s = self.S[m].ravel()
            tie = np.random.default_rng(3050 + int(j)).random(s.size)
            rk = np.empty(s.size)
            rk[np.lexsort((tie, s))] = np.arange(s.size)
            self.strat[m] = (int(j) * 10 + np.minimum((rk * 5 // s.size).astype(int), 4)).reshape(m.sum(), -1)
        # Y floor: nearest day-of-year block in each window
        self.doy = (self.tiss - np.array([self.year_start(j) for j in self.win]))
        self.ymap = {}
        g4 = 0.0
        for j in self.wins:
            for k in self.wins:
                bj, bk = np.flatnonzero(self.win == j), np.flatnonzero(self.win == k)
                d = np.abs(self.doy[bj][:, None] - self.doy[bk][None, :])
                d = np.minimum(d, 365.25 - d)   # circular day of year: a late-December block matches early January
                pick = bk[np.argmin(d, axis=1)]
                g4 = max(g4, d.min(axis=1).max())
                assert d.min(axis=1).max() <= 5, "G4 day-of-year match %d->%d" % (j, k)
                self.ymap[(int(j), int(k))] = (bj, pick)
        # T shifts allowed: every source block at least MIN_GAP days from its target
        b = np.arange(self.NB)
        self.tk = np.array([k for k in range(1, self.NB)
                            if np.min(np.abs(self.tiss[(b + k) % self.NB] - self.tiss)) >= MIN_GAP])
        assert len(self.tk) >= 100, "G0 too few T shifts %d" % len(self.tk)
        print("G4 PASS worst day-of-year match %.2f days;  T: %d allowed shifts (%d..%d)"
              % (g4, len(self.tk), self.tk.min(), self.tk.max()), flush=True)

    @staticmethod
    def year_start(j):
        import datetime as dt
        return (dt.date(1990 + int(j), 1, 1) - dt.date(1970, 1, 1)).days

    def grid(self, name):
        if name not in self._g:   # NpzFile decompresses on every access: read each grid once
            self._g[name] = self.Q[name]
        return self._g[name]

    def units(self, G, kshift=0, lrot=0):
        """unit values from grid G (NB, 160, 360) with optional block shift and longitude rotation."""
        bi = (np.arange(self.NB) + kshift) % self.NB
        return np.ascontiguousarray(G[bi][:, self.r, (self.c + lrot) % 360])   # C order: ravel() must be a view

    def year_swap(self, V, perm):
        out = np.full_like(V, np.nan)
        for j, k in perm.items():
            bj, pick = self.ymap[(j, k)]
            out[bj] = V[pick]
        return out


def sa(score, P, N, strat, cov):
    """pair-weighted within-stratum midrank AUC over units u != v, covered units only."""
    s, p, n, st = score[cov], P[cov], N[cov], strat[cov]
    o = np.lexsort((s, st))
    s, p, n, st = s[o], p[o], n[o], st[o]
    # tie groups within stratum
    newg = np.ones(len(s), bool)
    newg[1:] = (st[1:] != st[:-1]) | (s[1:] != s[:-1])
    gid = np.cumsum(newg) - 1
    newst = np.ones(len(s), bool)
    newst[1:] = st[1:] != st[:-1]
    sid = np.cumsum(newst) - 1
    gn = np.bincount(gid, weights=n)
    # negatives strictly below the group within its stratum
    gst = sid[newg]
    cum = np.cumsum(gn)
    stfirst = np.flatnonzero(np.r_[True, gst[1:] != gst[:-1]])
    offset = np.r_[0.0, cum][stfirst]             # negatives before the stratum's first group
    base = cum - gn - offset[gst]
    below = base[gid]
    num = np.sum(p * (below + 0.5 * (gn[gid] - n)))
    tp = np.bincount(sid, weights=p)
    tn = np.bincount(sid, weights=n)
    den = np.sum(tp * tn) - np.sum(p * n)
    return num / den if den > 0 else np.nan


def brute(score, P, N, strat, cov):
    num = den = 0.0
    idx = np.flatnonzero(cov)
    for u in idx:
        for v in idx:
            if u == v or strat[u] != strat[v]:
                continue
            w = P[u] * N[v]
            if w == 0:
                continue
            den += w
            num += w * (1.0 if score[u] > score[v] else 0.5 if score[u] == score[v] else 0.0)
    return num / den


def residual(A, y, rivals):
    """per window, y minus its least-squares fit on the rival fields (jointly covered units)."""
    out = np.full_like(y, np.nan)
    for j in A.wins:
        m = A.win == j
        Y = y[m].ravel()
        R = np.stack([r[m].ravel() for r in rivals], 1)
        ok = np.isfinite(Y) & np.all(np.isfinite(R), 1)
        if ok.sum() < 50:
            continue
        D = np.c_[np.ones(ok.sum()), R[ok]]
        beta, *_ = np.linalg.lstsq(D, Y[ok], rcond=None)
        r_ = np.full(Y.shape, np.nan)
        r_[ok] = Y[ok] - D @ beta
        out[m] = r_.reshape(m.sum(), -1)
    return out


def derangement(rng, items):
    """year map target -> source with |source - target| >= 2 (adjacent years share reference days)."""
    while True:
        p = rng.permutation(items)
        if np.all(np.abs(p - items) >= 2):
            return dict(zip(map(int, items), map(int, p)))


def transform(A, grids, kind, k, rng):
    """apply one floor draw to a list of grids -> list of unit arrays (joint for rival residuals)."""
    if kind == "T":
        return [A.units(G, kshift=k) for G in grids]
    if kind == "L":
        return [A.units(G, lrot=k) for G in grids]
    if kind == "Y":
        return [A.year_swap(A.units(G), k) for G in grids]
    raise ValueError(kind)


def permute_within(V, strat, rng):
    out = np.array(V, order="C", copy=True)
    idx = np.flatnonzero(np.isfinite(V.ravel()))
    flat = out.ravel()
    keys = strat.ravel()[idx]
    o = np.lexsort((rng.random(len(idx)), keys))
    ids = idx[o]
    ks = keys[o]
    starts = np.flatnonzero(np.r_[True, ks[1:] != ks[:-1]])
    for a0, a1 in zip(starts, np.r_[starts[1:], len(ids)]):
        flat[ids[a0:a1]] = flat[ids[a0:a1]][rng.permutation(a1 - a0)]
    return out


def draw(kind, rng, A):
    if kind == "T":
        return int(A.tk[rng.integers(0, len(A.tk))])
    if kind == "L":
        return int(rng.integers(20, 341))
    if kind == "Y":
        return derangement(rng, A.wins)
    return None


def floor_p(A, make, P, N, nworld, seed, real_units):
    """make(kind, k, rng) -> null unit array; returns p per floor and the null SAs."""
    res = {}
    for f, kind in enumerate(FLOORS):
        rng = np.random.default_rng(seed + 1000 * f)
        nulls, reals = [], []
        for _ in range(nworld):
            k = draw(kind, rng, A)
            V = make(kind, k, rng)
            cov = np.isfinite(V) & np.isfinite(real_units)
            nulls.append(sa(V, P, N, A.strat, cov))
            if kind == "A" and reals:   # floor A keeps the covered set: the real SA is the same every world
                reals.append(reals[-1])
            else:
                reals.append(sa(real_units, P, N, A.strat, cov))
        nulls, reals = np.array(nulls), np.array(reals)
        assert np.all(np.isfinite(reals)), "real SA not finite (floor %s)" % kind
        res[kind] = ((1 + np.sum(nulls >= reals)) / (nworld + 1), nulls, reals)
    return res


def gas_makers(A, g, rivals=None):
    G = [A.grid("TA_" + g)] + ([A.grid("TA_" + r) for r in rivals] if rivals else [])
    real = [A.units(x) for x in G]

    def finish(us):
        return residual(A, us[0], us[1:]) if rivals else us[0]

    real_u = finish(real)

    def make(kind, k, rng):
        if kind == "A":
            return permute_within(real_u, A.strat, rng)
        return finish(transform(A, G, kind, k, rng))
    return real_u, make


def gates(A):
    t0 = time.time()
    G = A.grid("TA_co5")
    V0 = A.units(G)
    rng = np.random.default_rng(1)
    assert np.array_equal(A.units(G, kshift=0), V0, equal_nan=True), "G1 T0"
    assert np.array_equal(A.units(G, lrot=0), V0, equal_nan=True), "G1 L0"
    ident = {int(j): int(j) for j in A.wins}
    assert np.array_equal(A.year_swap(V0, ident), V0, equal_nan=True), "G1 Y0"
    worst = {}
    for kind, ks in (("T", list(A.tk)), ("L", list(range(20, 341))), ("Y", [draw("Y", rng, A) for _ in range(20)])):
        ch = []
        for k in ks:
            V = transform(A, [G], kind, k, rng)[0]
            both = np.isfinite(V) & np.isfinite(V0)
            ch.append(np.mean(V[both] != V0[both]))
        worst[kind] = min(ch)
        assert worst[kind] > 0.5, "G1 %s k changes too little %.3f" % (kind, worst[kind])
    print("G1 PASS every allowed k changes > 50 %% of values (min T %.3f L %.3f Y %.3f)"
          % (worst["T"], worst["L"], worst["Y"]), flush=True)
    cov = np.isfinite(V0)
    st = np.unique(A.strat)
    sub = np.zeros(cov.shape, bool)
    for s in rng.choice(st, 3, replace=False):
        ids = np.flatnonzero((A.strat.ravel() == s) & cov.ravel())
        sub.ravel()[rng.choice(ids, min(400, len(ids)), replace=False)] = True
    tie = np.round(V0, 1)   # coarse values so ties occur
    # synthetic row counts (the gates never read the real labels together with the field)
    Ps = rng.binomial(3, 0.05, A.P.shape).astype(np.float64)
    Ns = rng.integers(0, 12, A.P.shape).astype(np.float64)
    for sc in (V0, tie):
        a_, b_ = sa(sc, Ps, Ns, A.strat, sub), brute(sc.ravel(), Ps.ravel(), Ns.ravel(), A.strat.ravel(),
                                                      sub.ravel())
        assert abs(a_ - b_) <= 1e-12, "G2 %.15f %.15f" % (a_, b_)
    print("G2 PASS brute-force recount (with and without ties)", flush=True)
    top = np.zeros(V0.shape)
    for s in st:
        m = (A.strat == s) & cov
        if m.sum() == 0:
            continue
        q = np.quantile(V0[m], 0.9)
        top[m & (V0 >= q)] = 1.0
    Pp = top            # one row per unit: no label-derived weight meets the field (opus code audit)
    Np = 1.0 - top
    v = sa(V0, Pp, Np, A.strat, cov)
    assert v > 0.95, "G3 planted %.4f" % v
    real_u, make = gas_makers(A, "co5")
    fl = floor_p(A, make, Pp, Np, 20, 77, real_u)
    for kind in ("T", "L", "Y"):
        assert fl[kind][1].mean() < 0.6, "G3 floor %s mean %.3f" % (kind, fl[kind][1].mean())
    print("G3 PASS planted SA %.4f; floor means T %.3f L %.3f Y %.3f (%.0f s)"
          % (v, fl["T"][1].mean(), fl["L"][1].mean(), fl["Y"][1].mean(), time.time() - t0), flush=True)


def main():
    args = sys.argv[1:]
    bundle, q305, rec = args[0], args[1], args[2]
    nworld = int(args[args.index("--worlds") + 1]) if "--worlds" in args else NW_DEFAULT
    print("bundle md5 %s  q305 md5 %s  rec md5 %s" % (md5(bundle), md5(q305), md5(rec)), flush=True)
    if "--gates-only" not in args:
        check_freeze([bundle, q305, rec, os.path.abspath(__file__)])
    A = Arena(bundle, q305)
    print("G0 PASS  blocks %d cells %d  windows %d..%d  positive rows %d  ANY-not-ISO rows %d"
          % (A.NB, A.NC, A.wins[0], A.wins[-1], A.P.sum(), A.X.sum()), flush=True)
    gates(A)
    if "--gates-only" in args:
        return
    t0 = time.time()
    out = {}
    for f, (g, t, q) in enumerate(GASES):
        real_u, make = gas_makers(A, g)
        cov = np.isfinite(real_u)
        s_iso = sa(real_u, A.P, A.N, A.strat, cov)
        s_any = sa(real_u, A.X, A.N, A.strat, cov)
        fl = floor_p(A, make, A.P, A.N, nworld, 3050 + 10 * f, real_u)
        pmax = max(fl[k][0] for k in FLOORS)
        rr, rmake = gas_makers(A, g, rivals=(t, q))
        rcov = np.isfinite(rr)
        s_res = sa(rr, A.P, A.N, A.strat, rcov)
        rfl = floor_p(A, rmake, A.P, A.N, nworld, 3550 + 10 * f, rr)
        rpmax = max(rfl[k][0] for k in FLOORS)
        lowo = min(sa(real_u, A.P, A.N, A.strat, cov & (A.win != j)[:, None]) for j in A.wins)
        sec = A.c // 60                       # grid column c = lon + 179.5 -> sector [-180 + 60 q, -120 + 60 q)
        assert set(np.unique(sec).tolist()) <= set(range(6))
        loso = min(sa(real_u, A.P, A.N, A.strat, cov & (sec != q)[None, :]) for q in range(6))
        out[g] = dict(sa=s_iso, any=s_any, pmax=pmax, res=s_res, rpmax=rpmax, lowo=lowo, loso=loso)
        print("%-4s SA %.5f  coverage %.4f  pos rows %d  ANY-not-ISO SA %.5f" %
              (g, s_iso, cov.mean(), A.P[cov].sum(), s_any), flush=True)
        for k in FLOORS:
            p_, nl, re = fl[k]
            print("     floor %s p %.4f  null mean %.5f sd %.5f q97.5 %.5f  (real on shared units mean %.5f)"
                  % (k, p_, nl.mean(), nl.std(), np.quantile(nl, 0.975), re.mean()), flush=True)
        print("     residual on %s,%s: SA %.5f  p_max %.4f  (%s)" % (t, q, s_res, rpmax, "  ".join(
            "%s %.4f" % (k, rfl[k][0]) for k in FLOORS)), flush=True)
        print("     robustness: min SA leaving one window out %.5f; leaving one 60-degree sector out %.5f"
              % (lowo, loso), flush=True)
    ps = sorted(((out[g]["pmax"], g) for g in out))
    holm = {ps[0][1]: ps[0][0] <= 0.025 and out[ps[0][1]]["sa"] > 0.5}
    holm[ps[1][1]] = holm[ps[0][1]] and ps[1][0] <= 0.05 and out[ps[1][1]]["sa"] > 0.5
    for g in out:
        o = out[g]
        sup = (holm[g] and o["any"] >= 0.5 and o["res"] > 0.5 and o["rpmax"] <= 0.05
               and o["lowo"] > 0.5 and o["loso"] > 0.5)
        print("VERDICT %s: %s  (SA %.5f p_max %.4f Holm %s  ANY-not-ISO %.5f  residual %.5f p %.4f  "
              "leave-one-window %.5f  leave-one-sector %.5f)"
              % (g, "SUPPORTED" if sup else "NOT SUPPORTED", o["sa"], o["pmax"], holm[g], o["any"], o["res"],
                 o["rpmax"], o["lowo"], o["loso"]), flush=True)
    reported(A, nworld, np.load(rec)["rec60"])
    print("DONE %.0f s" % (time.time() - t0), flush=True)


def reported(A, nworld, rec60):
    from global_land_mask import globe
    nw = max(100, nworld // 5)
    off = np.arange(-0.45, 0.5, 0.1)
    lat = 89.5 - A.r
    lon = A.c - 179.5
    lf = np.array([globe.is_land(*np.meshgrid(np.clip(lat[k] + off, -89.95, 89.95), ((lon[k] + off + 180) % 360) - 180,
                                              indexing="ij")).mean() for k in range(A.NC)])
    land = {"ocean": lf == 0, "mixed": (lf > 0) & (lf < 0.5), "land": lf >= 0.5}
    for name in ("TA_t5", "TA_q5", "TA_t6", "TA_q6", "TA_co5", "TA_ch46", "TS_co5", "TS_ch46"):
        G = A.grid(name)
        for tag, tr in (("", lambda x: x), ("|.|", np.abs)):
            if tag and not name.startswith("TA_c"):
                continue
            V = tr(A.units(G))
            cov = np.isfinite(V)
            s_ = sa(V, A.P, A.N, A.strat, cov)
            rng = np.random.default_rng(91)
            nl_t, nl_a = [], []
            for _ in range(nw):
                k = draw("T", rng, A)
                W = tr(A.units(G, kshift=k))
                c2 = cov & np.isfinite(W)
                nl_t.append(sa(W, A.P, A.N, A.strat, c2) - sa(V, A.P, A.N, A.strat, c2))
                nl_a.append(sa(permute_within(V, A.strat, rng), A.P, A.N, A.strat, cov) - s_)
            pt = (1 + np.sum(np.array(nl_t) >= 0)) / (nw + 1)
            pa = (1 + np.sum(np.array(nl_a) >= 0)) / (nw + 1)
            print("REPORTED %-9s%-3s SA %.5f  floor T p %.3f  A p %.3f" % (name, tag, s_, pt, pa), flush=True)
    for g in ("co5", "ch46"):
        V = A.units(A.grid("TA_" + g))
        cov = np.isfinite(V)
        for nm, m in land.items():
            print("REPORTED %s %-5s SA %.5f" % (g, nm, sa(V, A.P, A.N, A.strat, cov & m[None, :])), flush=True)
        for nm, lo, hi in (("2007-15", 17, 25), ("2016-25", 26, 35), ("2007-21", 17, 31), ("2022-25", 32, 35)):
            m = (A.win >= lo) & (A.win <= hi)
            print("REPORTED %s %s SA %.5f" % (g, nm, sa(V, A.P, A.N, A.strat, cov & m[:, None])), flush=True)
        assert rec60.shape == V.shape
        print("REPORTED %s without units with an M>=5 in the cell in the 60 days before the issue SA %.5f  "
              "(those units only %.5f; share %.4f)" % (g, sa(V, A.P, A.N, A.strat, cov & ~rec60),
                                                       sa(V, A.P, A.N, A.strat, cov & rec60), rec60[cov].mean()),
              flush=True)
        for qn in range(5):
            m = (A.strat % 10) == qn
            print("REPORTED %s R2 quintile %d SA %.5f" % (g, qn + 1, sa(V, A.P, A.N, A.strat, cov & m)), flush=True)
        print("REPORTED %s per window: " % g + " ".join(
            "%d:%.3f" % (j, sa(V, A.P, A.N, A.strat, cov & (A.win == j)[:, None])) for j in A.wins), flush=True)


if __name__ == "__main__":
    main()
