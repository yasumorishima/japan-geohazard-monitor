# post-run audit of round302: independent rebuild of clock columns, fit loop, AUC, PERM, diagnostics.
import sys, csv, warnings, time
from datetime import datetime
import numpy as np
from scipy.special import log_ndtr
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.exceptions import ConvergenceWarning
sys.path.insert(0, "/home/yasu/geo-ml")
import chk238, ctl264, size208
t0 = time.time()
ORIGIN = 719163.0720601851
A = 0.5


def pt(ts):
    ts = ts.strip().rstrip("Z").split(".")[0]
    d = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
    return d.toordinal() + (d.hour * 3600 + d.minute * 60 + d.second) / 86400.0


mn = 1e99
with open("/home/yasu/geo-ml/boxes/global_m45.csv") as f:
    r = csv.reader(f); next(r)
    for row in r:
        try:
            mn = min(mn, pt(row[0]))
        except Exception:
            pass
print("origin from file %.10f equal %s" % (mn, mn == ORIGIN))
T_, LA, LO, MG = [], [], [], []
with open("/home/yasu/claude-scratch/roundA/iscgem_pre1970_m55.csv") as f:
    r = csv.reader(f); next(r)
    for row in r:
        T_.append(pt(row[0]) - ORIGIN); LA.append(float(row[1])); LO.append(float(row[2])); MG.append(float(row[4]))
T_, LA, LO, MG = map(np.array, (T_, LA, LO, MG))
o = np.argsort(T_, kind="stable"); T_, LA, LO, MG = T_[o], LA[o], LO[o], MG[o]
print("ISC rows", len(T_))


def gkT(m):
    return np.where(m >= 6.5, 10 ** (0.032 * m + 2.7389), 10 ** (0.5409 * m - 0.547))


def gkL(m):
    return 10 ** (0.1238 * m + 0.983)


def hav(a1, o1, a2, o2):
    a1, o1, a2, o2 = map(np.radians, (a1, o1, a2, o2))
    h = np.sin((a2 - a1) / 2) ** 2 + np.cos(a1) * np.cos(a2) * np.sin((o2 - o1) / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(np.minimum(h, 1)))


TT, LL = gkT(MG), gkL(MG)
iso = np.ones(len(T_), bool)
for k in range(len(T_)):
    m = (T_ < T_[k]) & (T_ >= T_[k] - 1200)
    m &= (MG >= MG[k]) & (T_[k] - T_ <= TT)
    if m.any() and (hav(LA[m], LO[m], LA[k], LO[k]) <= LL[m]).any():
        iso[k] = False
MD = np.load("/home/yasu/geo-ml/rung197/meta197.npz")
cells = MD["cells"]; cmap = {int(c): i for i, c in enumerate(cells)}
gid = (np.floor(LA).astype(int) + 90) * 360 + (np.floor(LO).astype(int) + 180)
cix = np.array([cmap.get(int(g), -1) for g in gid])
D1918 = datetime(1918, 1, 1).toordinal() - ORIGIN
D1970 = datetime(1970, 1, 1).toordinal() - ORIGIN
sa = iso & (MG >= 6.25) & (T_ >= D1918) & (cix >= 0)
s7 = MD["keep_iso"] & (MD["dmag"] >= 6.25)
print("my clock events ISC %d + 1970+ %d (printed 1780 + 2491)" % (sa.sum(), s7.sum()))
evt = {}
for t, m, c in zip(np.r_[T_[sa], MD["dtd"][s7]], np.r_[MG[sa], MD["dmag"][s7]], np.r_[cix[sa], MD["dcell"][s7]]):
    evt.setdefault(int(c), []).append((float(t), -float(m)))
starts = {}
for c, L in evt.items():
    L.sort()
    st, s, big = [], None, None
    for t, nm in L:
        m = -nm
        if s is not None and t - s <= float(gkT(big)):
            big = max(big, m)
        else:
            st.append(t); s, big = t, m
    starts[c] = np.array(st)


def logsf(x, mu):
    r = np.sqrt(x / mu); u1 = (r - 1 / r) / A; u2 = (r + 1 / r) / A
    l1 = log_ndtr(-u1); l2 = 2 / A ** 2 + log_ndtr(-u2)
    return l1 + np.log1p(-np.exp(l2 - l1))


def clk(mu, t, s0, e):
    with np.errstate(divide="ignore"):
        lpb = logsf(t + s0, mu) - logsf(t, mu) + np.log1p(-np.exp(logsf(t + e, mu) - logsf(t + s0, mu)))
    lpp = -s0 / mu + np.log1p(-np.exp(-(e - s0) / mu))
    return np.clip(lpb - lpp, -20, 20)


D = chk238.build()
C = ctl264.Control(D)
_, tdays, _, _, _, _, wins, _, NC, _ = size208.load_arena()
jj, grp, feats, ncs, tcuts, hiT = D["jj"], D["grp"], D["feats"], D["ncs"], D["tcuts"], D["hiT"]
ev = [j for j in jj if j >= 11]
print("len jj %d ev %d (%d..%d)" % (len(jj), len(ev), ev[0], ev[-1]))
strict = {j: [k for k in jj if k < j and hiT[k] <= tcuts[j]] for j in ev}
assert all(strict[j] == C.strict[j] for j in ev)
X4, K, ST, SP = {}, {}, {}, {}
for j in jj:
    tr, lo, hi = wins[j]; tc = float(tcuts[j])
    s0, e = float(tdays[lo]) - tc, float(tdays[hi - 1]) + 34 - tc; SP[j] = (s0, e)
    el = C.EL[j]; nr = len(el)
    n = np.zeros(nr); first = np.full(nr, np.nan); last = first.copy()
    for i, c in enumerate(el):
        s = starts.get(int(c), np.zeros(0)); s = s[s < tc]
        n[i] = len(s)
        if len(s) >= 2:
            first[i], last[i] = s[0], s[-1]
    elig = ((grp[j] == 1) | (grp[j] == 2)) & (n >= 2)
    mu = np.where(elig, (last - first) / np.maximum(n - 1, 1), np.nan); t = tc - last
    x = np.zeros((nr, 4)); k = np.zeros(nr)
    x[elig, 0] = np.log(n[elig] / (tc - first[elig])); x[elig, 1] = last[elig] < D1970
    x[elig, 2] = grp[j][elig] == 1; x[elig, 3] = grp[j][elig] == 2
    k[elig] = clk(mu[elig], t[elig], s0, e)
    X4[j], K[j], ST[j] = x, k, dict(n=n, mu=mu, t=t, elig=elig)

# compare to round302's own columns: exec its top part with chk238.build returning the same D (memory)
chk238.build = lambda: D
src = open("/home/yasu/claude-scratch/roundA2/round302.py").read().split("\n")
cut = [i for i, l in enumerate(src) if l.startswith("import mpmath as mp")][0]
ns = {"__name__": "aud"}
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    exec("\n".join(src[:cut]), ns)
dx = max(np.abs(ns["COL"][j][0] - X4[j]).max() for j in jj)
dk = max(np.abs(ns["COL"][j][1] - K[j]).max() for j in jj)
de = sum(int((ns["ST"][j]["elig"] != ST[j]["elig"]).sum()) for j in jj)
print("vs round302 columns: max|dX4| %.3e max|dCLK| %.3e elig mismatches %d" % (dx, dk, de), flush=True)
ns = None

print("j  nELIG lowclip hiclip  sdCLK(elig) sdCLK(all) fracELIG")
tot = low = 0
for j in ev:
    el = ST[j]["elig"]; kk = K[j][el]
    tot += el.sum(); low += (kk <= -20).sum()
    print("%2d %4d %4d %4d %8.3f %8.3f %.3f" % (j, el.sum(), (kk <= -20).sum(), (kk >= 20).sum(), kk.std(), K[j].std(), el.mean()))
allel = sum(int(ST[j]["elig"].sum()) for j in jj)
alllow = sum(int((K[j][ST[j]["elig"]] <= -20).sum()) for j in jj)
print("scored: ELIG %d at low clip %d ; all 33 windows: ELIG %d low clip %d" % (tot, low, allel, alllow), flush=True)


def my_auc(p, n, s):
    d = s[:, None] - s[None, :]
    w = p[:, None] * n[None, :]
    return float((w * ((d > 0) + 0.5 * (d == 0))).sum() / (p.sum() * n.sum()))


LAB = {"ISO": C.PI, "ANY": C.PA}


def fit(nm, colf):
    out, cf, nit, cw = {}, [], [], 0
    for j in ev:
        Xtr = np.vstack([colf(k) for k in strict[j]])
        y = np.concatenate([(LAB[nm][k] > 0).astype(int) for k in strict[j]])
        sc = StandardScaler().fit(Xtr)
        with warnings.catch_warnings(record=True) as W:
            warnings.simplefilter("always")
            md = LogisticRegression(C=1.0, max_iter=2000).fit(sc.transform(Xtr), y)
        cw += sum(issubclass(w.category, ConvergenceWarning) for w in W)
        out[j] = md.predict_proba(sc.transform(colf(j)))[:, 1]
        cf.append(md.coef_[0, -1]); nit.append(int(md.n_iter_[0]))
    a = np.array([my_auc(LAB[nm][j], ncs[j], out[j]) for j in ev])
    return a, out, np.array(cf), max(nit), cw


def within(nm, sc, c):
    num = den = 0.0
    for j in ev:
        m = grp[j] == c; pa, na = LAB[nm][j][m], ncs[j][m]
        if pa.sum() <= 0 or na.sum() <= 0:
            continue
        w = pa.sum() * na.sum(); num += w * my_auc(pa, na, sc[j][m]); den += w
    return num / den


O = np.load("/home/yasu/claude-scratch/roundA2/out302.npz")
full = fit("ISO", lambda k: feats[k])[0]
print("FULL mine %.8f vs printed %.8f maxdiff/window %.2e" % (full.mean(), O["full"].mean(), np.abs(full - O["full"]).max()))
R = {}
for nm, bk, ak in (("ISO", "baseI", "armI"), ("ANY", "baseA", "armA")):
    b, bs, _, bit, bcw = fit(nm, lambda k: np.c_[feats[k], X4[k]])
    a, as_, cf, ait, acw = fit(nm, lambda k: np.c_[feats[k], X4[k], K[k]])
    g = a - b
    R[nm] = dict(b=b, a=a, bs=bs, as_=as_, g=g)
    print("%s mine BASE %.6f ARM %.6f gain %+.6f t %.3f up %d | maxdiff BASE %.2e ARM %.2e | coef mean %+.4f min %+.4f max %+.4f pos %d/25 | maxiter %d/%d convwarn %d/%d"
          % (nm, b.mean(), a.mean(), g.mean(), g.mean() / (g.std(ddof=1) / 5), (g > 0).sum(),
             np.abs(b - O[bk]).max(), np.abs(a - O[ak]).max(), cf.mean(), cf.min(), cf.max(), (cf > 0).sum(), bit, ait, bcw, acw), flush=True)
print("BASE-FULL mine %+.6f" % (R["ISO"]["b"].mean() - full.mean()))
for c in (1, 2, 3, 0):
    bb, aa = within("ISO", R["ISO"]["bs"], c), within("ISO", R["ISO"]["as_"], c)
    print("class %d ISO mine BASE %.5f ARM %.5f diff %+.5f" % (c, bb, aa, aa - bb), flush=True)
for tag, msk in (("all", lambda j: ST[j]["elig"]), ("unclipped", lambda j: ST[j]["elig"] & (K[j] > -20))):
    num = den = 0.0
    for j in ev:
        el = msk(j); p, n_ = C.PI[j][el], ncs[j][el]
        if p.sum() > 0 and n_.sum() > 0:
            w = p.sum() * n_.sum(); num += w * my_auc(p, n_, K[j][el]); den += w
    print("CLK AUC among ELIG (%s) %.5f" % (tag, num / den))
pg = {"ISO": [], "ANY": []}; pc = {1: [], 2: []}
for w in range(20):
    r = np.random.default_rng(1000 + w); P = {}
    for j in jj:
        s = ST[j]; el = s["elig"]; key = grp[j] * 4 + X4[j][:, 1] * 2 + (s["n"] == 2)
        t2 = s["t"].copy()
        for kk in np.unique(key[el]):
            ix = np.flatnonzero(el & (key == kk)); t2[ix] = s["t"][ix][r.permutation(len(ix))]
        k = np.zeros(len(el)); k[el] = clk(s["mu"][el], t2[el], *SP[j]); P[j] = k
    for nm in ("ISO", "ANY"):
        a, sc_, _, _, cw = fit(nm, lambda k, P=P: np.c_[feats[k], X4[k], P[k]])
        pg[nm].append((a - R[nm]["b"]).mean())
        if nm == "ISO":
            for c in (1, 2):
                pc[c].append(within("ISO", sc_, c) - within("ISO", R["ISO"]["bs"], c))
    print("perm %d ISO %+.6f ANY %+.6f c1 %+.5f c2 %+.5f" % (w, pg["ISO"][-1], pg["ANY"][-1], pc[1][-1], pc[2][-1]), flush=True)
for nm, k in (("ISO", "pgI"), ("ANY", "pgA")):
    x = np.array(pg[nm])
    print("%s PERM mine mean %+.6f sd %.6f max %+.6f | maxdiff vs saved %.2e" % (nm, x.mean(), x.std(ddof=1), x.max(), np.abs(x - O[k]).max()))
for c in (1, 2):
    x = np.array(pc[c])
    print("class %d PERM within diff mean %+.5f sd %.5f min %+.5f max %+.5f" % (c, x.mean(), x.std(ddof=1), x.min(), x.max()))
print("DONE %.0f s" % (time.time() - t0))
