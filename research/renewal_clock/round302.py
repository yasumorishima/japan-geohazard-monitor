# -*- coding: utf-8 -*-
"""ROUND 302 -- does a century-long renewal clock (ISC-GEM 1918-1969 + 1970+) order classes 1-2 per window?

SUDDEN AXIS: global ACTIVE; FULL 0.62086 over 25 seen windows (arm 0.5785 over 33).
DIMENSION CEILING: WHICH CELL, PER WINDOW; per-window cell oracle 0.95017; static per-cell maps add <= +0.002
  (sizing 301); no bound measured for functions of (elapsed time, interval).
WHY THIS ROUND: the room is per-window and largest in classes 1-2; round 260's clock reached only class 3
  because it needed 3 isolated M>=6 since 1970; ISC-GEM gives classes 1-2 an interval; 0.90 needs class 3 plus
  two of classes 0-2.

Implements FREEZE_302.md exactly (md5 asserted).  Nothing is tuned.  Verdict printed only after all gates pass.
"""
import hashlib
import sys
import time
from datetime import datetime

import numpy as np
from scipy import stats
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, "/home/yasu/geo-ml")
import chk238 as C8  # noqa: E402
import ctl264  # noqa: E402
import size196 as SZ  # noqa: E402
import size208 as S8  # noqa: E402
from pre238c import auc_fast as auc  # noqa: E402

R = "/home/yasu/claude-scratch/roundA2/"
FREEZE_MD5 = "3c7bdce8e63b72977476650456560c5b"  # with amendments 1-4
ISC = "/home/yasu/claude-scratch/roundA/iscgem_pre1970_m55.csv"
ISC_MD5 = "21616b179fe5849a187164113458c0be"
ORIGIN = 719163.0720601851
D1918 = datetime(1918, 1, 1).toordinal() - ORIGIN
D1970 = datetime(1970, 1, 1).toordinal() - ORIGIN
A, MCLK, NPERM = 0.5, 6.25, 20
t00 = time.time()


def md5(p):
    return hashlib.md5(open(p, "rb").read()).hexdigest()


# ---------------- G0 inputs
assert md5(R + "FREEZE_302.md") == FREEZE_MD5, "G0 freeze file changed"
assert md5(ISC) == ISC_MD5, "G0 ISC-GEM md5"
ti, lai, loi, mgi = SZ.read_text(ISC, "csv")
assert len(ti) == 12072, "G0 ISC-GEM rows %d" % len(ti)
assert ti.max() < datetime(1970, 1, 1).toordinal(), "G0 ISC-GEM row on/after 1970"
tg = SZ.read_text("/home/yasu/geo-ml/boxes/global_m45.csv", "csv")[0]
assert tg.min() == ORIGIN, "G0 origin %r" % tg.min()
tg = None
ti = ti - ORIGIN
print("G0 PASS", flush=True)

D = C8.build()
C = ctl264.Control(D)
jj, ev, grp, feats, pcs, ncs = C.jj, C.ev, D["grp"], D["feats"], D["pcs"], D["ncs"]
tcuts = D["tcuts"]
_, tdays, _, _, _, _, wins, _, NC, _ = S8.load_arena()
MD = np.load("/home/yasu/geo-ml/rung197/meta197.npz")
cells = MD["cells"]
assert len(cells) == NC

# ---------------- clock events
iso_i = SZ.gk_isolated(ti, lai, loi, mgi, ti, lai, loi, mgi)
gid = (np.floor(lai).astype(np.int64) + 90) * 360 + (np.floor(loi).astype(np.int64) + 180)
p = np.searchsorted(cells, gid)
inar = (p < NC) & (cells[np.minimum(p, NC - 1)] == gid)
sa = iso_i & (mgi >= MCLK) & (ti >= D1918) & inar
s7 = MD["keep_iso"] & (MD["dmag"] >= MCLK)
ET = np.r_[ti[sa], MD["dtd"][s7]].astype(np.float64)
EM = np.r_[mgi[sa], MD["dmag"][s7]].astype(np.float64)
EC = np.r_[p[sa], MD["dcell"][s7]].astype(np.int64)
print("clock events: ISC-GEM %d + 1970+ %d" % (sa.sum(), s7.sum()), flush=True)


def gk_T(m):
    return float(np.where(m >= 6.5, 10.0 ** (0.032 * m + 2.7389), 10.0 ** (0.5409 * m - 0.547)))


def clusters(tt, mm):
    """cluster starts for one cell's events (sorted by time, larger first on ties)."""
    o = np.lexsort((-mm, tt))
    tt, mm = tt[o], mm[o]
    starts = []
    s = big = None
    for x, m in zip(tt, mm):
        if s is not None and x - s <= gk_T(big):
            big = max(big, m)
        else:
            starts.append(x)
            s, big = x, m
    return np.array(starts)


CS = {int(c): clusters(ET[EC == c], EM[EC == c]) for c in np.unique(EC)}
EMPTY = np.zeros(0)


def state(c, tc):
    s = CS.get(c, EMPTY)
    s = s[s < tc]
    n = len(s)
    if n < 2:
        return n, np.nan, np.nan, np.nan, np.nan
    return n, s[0], s[-1], (s[-1] - s[0]) / (n - 1), tc - s[-1]


def lbpt(mu, t, s0, e):
    ig = stats.invgauss(mu=A ** 2, scale=mu / A ** 2)
    l0, la, lb = ig.logsf(t), ig.logsf(t + s0), ig.logsf(t + e)
    lpb = la - l0 + np.log1p(-np.exp(lb - la))
    lpp = -s0 / mu + np.log1p(-np.exp(-(e - s0) / mu))
    return np.clip(lpb - lpp, -20, 20)


def lbpt_lin(mu, t, s0, e):
    """round 260's linear-space formula."""
    ig = stats.invgauss(mu=A ** 2, scale=mu / A ** 2)
    s_t = ig.sf(t)
    pb = (ig.sf(t + s0) - ig.sf(t + e)) / np.maximum(s_t, 1e-300)
    pp = np.exp(-s0 / mu) - np.exp(-e / mu)
    return np.clip(np.log(np.maximum(pb, 1e-300) / np.maximum(pp, 1e-300)), -20, 20)


def lasym(mu, s0, e):
    """exponential-hazard asymptote h = 1/(2 a^2 mu), clipped like the column."""
    h = 1.0 / (2 * A ** 2 * mu)
    v = -h * s0 + np.log1p(-np.exp(-h * (e - s0))) - (-s0 / mu + np.log1p(-np.exp(-(e - s0) / mu)))
    return np.clip(v, -20, 20)


# ---------------- G3 BPT numerics
xs = np.array([50.0, 300.0, 900.0, 2000.0, 5000.0, 12000.0])
ig0 = stats.invgauss(mu=A ** 2, scale=1500.0 / A ** 2)
r_ = np.sqrt(xs / 1500.0)
cdf = stats.norm.cdf((r_ - 1 / r_) / A) + np.exp(2 / A ** 2) * stats.norm.cdf(-(r_ + 1 / r_) / A)
assert np.abs(ig0.cdf(xs) - cdf).max() <= 1e-9 and abs(ig0.mean() - 1500.0) <= 1e-6, "G3 cdf"
v = float(lbpt(np.array([4.7]), np.array([4.7 * 4500]), 36.0, 430.0)[0])
assert abs(v - (-7.66)) <= 0.01 and abs(v - float(lasym(4.7, 36.0, 430.0))) <= 0.01, "G3 planted %f" % v

# ---------------- per-window columns
ST, COL, SPAN = {}, {}, {}
for j in jj:
    tr, lo, hi = wins[j]
    tc = float(tcuts[j])
    s0, e = float(tdays[lo]) - tc, float(tdays[hi - 1]) + 34.0 - tc
    SPAN[j] = (s0, e)
    el = C.EL[j]
    st = np.array([state(int(c), tc) for c in el])
    n, first, last, mu, t = st.T
    elig = np.isin(grp[j], (1, 2)) & (n >= 2)
    ST[j] = dict(n=n, first=first, last=last, mu=mu, t=t, elig=elig)
    X = np.zeros((len(el), 4))
    X[elig, 0] = np.log(n[elig] / (tc - first[elig]))
    X[elig, 1] = (last[elig] < D1970).astype(float)
    X[elig, 2] = (grp[j][elig] == 1).astype(float)
    X[elig, 3] = (grp[j][elig] == 2).astype(float)
    k = np.zeros(len(el))
    if elig.any():
        k[elig] = lbpt(mu[elig], t[elig], s0, e)
        ok = stats.invgauss(mu=A ** 2, scale=mu[elig] / A ** 2).sf(t[elig]) > 1e-200
        if ok.any():
            assert np.abs(k[elig][ok] - lbpt_lin(mu[elig][ok], t[elig][ok], s0, e)).max() <= 1e-5, "G3 lin %d" % j
        big = t[elig] / mu[elig] > 1000
        if big.any():
            assert np.abs(k[elig][big] - lasym(mu[elig][big], s0, e)).max() <= 0.01, "G3 asym %d" % j
    COL[j] = (X, k)
import mpmath as mp  # noqa: E402
mp.mp.dps = 60


def mp_clk(mu, t, s0, e):
    def sf(x):
        r = mp.sqrt(x / mu)
        return 1 - (mp.ncdf((r - 1 / r) / A) + mp.e ** (2 / A ** 2) * mp.ncdf(-(r + 1 / r) / A))
    mu, t = mp.mpf(mu), mp.mpf(t)
    pb = (sf(t + s0) - sf(t + e)) / sf(t)
    pp = mp.e ** (-s0 / mu) - mp.e ** (-e / mu)
    return min(20.0, max(-20.0, float(mp.log(pb / pp))))


rb = np.random.default_rng(3021)
pool = [(j, i) for j in ev for i in np.flatnonzero(ST[j]["elig"])]
for q in rb.choice(len(pool), 20, replace=False):
    j, i = pool[q]
    ref = mp_clk(float(ST[j]["mu"][i]), float(ST[j]["t"][i]), *SPAN[j])
    assert abs(COL[j][1][i] - ref) <= 1e-8, "G3b window %d row %d: %r vs %r" % (j, i, COL[j][1][i], ref)
print("G3 PASS (incl. G3b 60-digit check on 20 rows)", flush=True)


# ---------------- G2 independent recount (separate loop over raw arrays)
def recount(c, tc, et, em, ecl):
    m = (ecl == c) & (et < tc)
    tt, mm = et[m], em[m]
    o = np.lexsort((-mm, tt))
    tt, mm = tt[o], mm[o]
    st_ = []
    s = big = None
    for i in range(len(tt)):
        if s is not None:
            T = 10.0 ** (0.032 * big + 2.7389) if big >= 6.5 else 10.0 ** (0.5409 * big - 0.547)
        if s is not None and tt[i] - s <= T:
            big = max(big, mm[i])
        else:
            st_.append(tt[i])
            s, big = tt[i], mm[i]
    n = len(st_)
    if n < 2:
        return n, None
    return n, (st_[0], st_[-1], (st_[-1] - st_[0]) / (n - 1), tc - st_[-1])


rng = np.random.default_rng(302)
done = 0
while done < 40:
    j = int(rng.choice(ev))
    i = int(rng.integers(len(C.EL[j])))
    c, tc = int(C.EL[j][i]), float(tcuts[j])
    if ST[j]["n"][i] < 2:
        continue
    n, q = recount(c, tc, ET, EM, EC)
    assert n == ST[j]["n"][i] and np.allclose(q, [ST[j][k_][i] for k_ in ("first", "last", "mu", "t")],
                                              rtol=0, atol=1e-9), "G2 recount"
    n2, q2 = recount(c, tc, np.r_[ET, tc + 1], np.r_[EM, 7.0], np.r_[EC, c])
    assert n2 == n and np.allclose(q2, q, rtol=0, atol=1e-9), "G2 future event entered"
    tp = q[0] - 3000.0
    n3, q3 = recount(c, tc, np.r_[ET, tp], np.r_[EM, 6.3], np.r_[EC, c])
    assert n3 == n + 1 and abs(q3[0] - tp) <= 1e-9 and np.allclose(q3[1::2], q[1::2], rtol=0, atol=1e-9),         "G2 planted past event not seen"
    done += 1
print("G2 PASS (40 pairs; future event ignored, past event seen)", flush=True)

# ---------------- G6 coverage and strata (before any CLK AUC)
fixed = tot = 0
for j in ev:
    el = ST[j]["elig"]
    assert (pcs[j][el] > 0).any() and (pcs[j][el] == 0).any(), "G6 window %d" % j
    key = grp[j] * 4 + COL[j][0][:, 1] * 2 + (ST[j]["n"] == 2)
    for kk in np.unique(key[el]):
        m = el & (key == kk)
        tot += int(m.sum())
        fixed += int(m.sum()) if m.sum() == 1 else 0
assert fixed <= 0.10 * tot, "G6 strata of size 1 hold %d of %d" % (fixed, tot)
for c in (1, 2):
    a_ = sum(int((ST[j]["elig"] & (grp[j] == c)).sum()) for j in ev)
    b_ = sum(int((ST[j]["elig"] & (grp[j] == c) & (pcs[j] > 0)).sum()) for j in ev)
    print("class %d ELIG rows %d, positive rows %d" % (c, a_, b_))
print("G6 PASS  strata of size 1: %d of %d ELIG rows" % (fixed, tot), flush=True)


def perm_col(w):
    r = np.random.default_rng(1000 + w)
    out = {}
    for j in jj:
        s = ST[j]
        el = s["elig"]
        key = grp[j] * 4 + COL[j][0][:, 1] * 2 + (s["n"] == 2)
        t2 = s["t"].copy()
        for kk in np.unique(key[el]):
            ix = np.flatnonzero(el & (key == kk))
            t2[ix] = s["t"][ix][r.permutation(len(ix))]
        k = np.zeros(len(el))
        if el.any():
            k[el] = lbpt(s["mu"][el], t2[el], *SPAN[j])
        out[j] = k
    return out


# ---------------- arms
LAB = {nm: {j: (P[j] > 0).astype(np.int8) for j in jj} for nm, P in (("ISO", C.PI), ("ANY", C.PA))}
MASS = {"ISO": C.PI, "ANY": C.PA}


def arm(nm, extra):
    out, cf = {}, []
    for j in ev:
        ks = C.strict[j]
        Xtr = np.vstack([extra(k) for k in ks])
        sc = StandardScaler().fit(Xtr)
        md = LogisticRegression(max_iter=2000, C=1.0).fit(sc.transform(Xtr),
                                                          np.concatenate([LAB[nm][k] for k in ks]))
        out[j] = md.predict_proba(sc.transform(extra(j)))[:, 1]
        cf.append(md.coef_[0, -1])
    a_ = np.array([auc(MASS[nm][j], ncs[j], out[j]) for j in ev])
    return a_, out, float(np.mean(cf))


def within(nm, sc, c):
    num = den = 0.0
    for j in ev:
        m = grp[j] == c
        pa, na = MASS[nm][j][m], ncs[j][m]
        if pa.sum() <= 0 or na.sum() <= 0:
            continue
        w = pa.sum() * na.sum()
        num += w * auc(pa, na, sc[j][m])
        den += w
    return num / den


def basef(k):
    return np.c_[feats[k], COL[k][0]]


fullI = arm("ISO", lambda k: feats[k])[0]
print("G1 FULL %.8f" % fullI.mean(), flush=True)
assert abs(fullI.mean() - 0.62086354) <= 1e-8, "G1"
B = {nm: arm(nm, basef) for nm in ("ISO", "ANY")}
z = arm("ISO", lambda k: np.c_[basef(k), np.zeros(len(COL[k][1]))])[0]
assert abs(z.mean() - B["ISO"][0].mean()) <= 1e-9, "G4"
print("G4 PASS", flush=True)
assert md5(R + "shad263.npz") == "be90312d8c42b962e41956da6d054705", "G5 shad263.npz md5"
Z = np.load(R + "shad263.npz")
for j in jj:
    assert np.array_equal(Z["el%d" % j], C.EL[j]), "G5 cells"
sh = arm("ANY", lambda k: np.c_[basef(k), Z["sh%d" % k]])[0]
gsh = float((sh - B["ANY"][0]).mean())
assert gsh <= 0, "G5 SHAD ANY gain %+.6f > 0: R3 cannot reject a definitional column" % gsh
print("G5 PASS  SHAD ANY gain %+.6f" % gsh, flush=True)
print("ALL GATES PASS %.0f s" % (time.time() - t00), flush=True)
if len(sys.argv) > 1 and sys.argv[1] == "--gates-only":
    sys.exit(0)

PC = [perm_col(w) for w in range(NPERM)]
res = {}
for nm in ("ISO", "ANY"):
    a_, sc, cf = arm(nm, lambda k: np.c_[basef(k), COL[k][1]])
    g = a_ - B[nm][0]
    pg = np.array([(arm(nm, lambda k, P=P: np.c_[basef(k), P[k]])[0] - B[nm][0]).mean() for P in PC])
    res[nm] = dict(a=a_, sc=sc, cf=cf, g=g, pg=pg)
    print("%s BASE %.6f ARM %.6f gain %+.6f t %.2f up %d/25 | PERM mean %+.6f sd %.6f max %+.6f | coef %+.4f"
          % (nm, B[nm][0].mean(), a_.mean(), g.mean(), g.mean() / (g.std(ddof=1) / 5.0), int((g > 0).sum()),
             pg.mean(), pg.std(ddof=1), pg.max(), cf), flush=True)
print("BASE - FULL (ISO) %+.6f" % (B["ISO"][0].mean() - fullI.mean()))
gi, ga = res["ISO"]["g"], res["ANY"]["g"]
r2 = bool(gi.mean() > 0 and gi.mean() / (gi.std(ddof=1) / 5.0) > 2 and gi.mean() > res["ISO"]["pg"].max())
r3 = bool(ga.mean() > 0 and ga.mean() > res["ANY"]["pg"].max())
for c in (1, 2, 3, 0):
    print("class %d ISO  BASE %.5f  ARM %.5f" % (c, within("ISO", B["ISO"][1], c), within("ISO", res["ISO"]["sc"], c)))
ca = []
for j in ev:
    el = ST[j]["elig"]
    if (C.PI[j][el] > 0).any() and (C.PI[j][el] == 0).any():
        ca.append((C.PI[j][el].sum() * ncs[j][el].sum(), auc(C.PI[j][el], ncs[j][el], COL[j][1][el])))
ca = np.array(ca)
print("descriptive: CLK AUC among ELIG rows (pair-weighted) %.5f" % (np.sum(ca[:, 0] * ca[:, 1]) / ca[:, 0].sum()))
print("R2 %s  R3 %s" % (r2, r3))
verdict = "REAL" if (r2 and r3) else "NULL"
if verdict == "REAL":
    verdict += " (renewal: coef > 0)" if res["ISO"]["cf"] > 0 else " (coef <= 0: not renewal)"
print("VERDICT %s" % verdict)
np.savez(R + "out302.npz", full=fullI, baseI=B["ISO"][0], baseA=B["ANY"][0], armI=res["ISO"]["a"],
         armA=res["ANY"]["a"], pgI=res["ISO"]["pg"], pgA=res["ANY"]["pg"], cfI=res["ISO"]["cf"], gsh=gsh)
print("DONE %.0f s" % (time.time() - t00))
