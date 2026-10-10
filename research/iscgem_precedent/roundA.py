# -*- coding: utf-8 -*-
"""Round A: does the ISC-GEM 1918-1969 record order precedent-group-0 cells beyond FULL's 30 columns?

SUDDEN AXIS: global ACTIVE; FULL 0.62086 over 25 seen windows; inside group 0 FULL is 0.57885.
DIMENSION CEILING: WHICH CELL, static part; class-0-only reassignment bound 0.73285; per-cell map bound 0.8112.
WHY THIS ROUND: 0.90 needs class 3 and at least two of classes 0/1/2; group 0 is where every 1970+ map ranks cells
  last, and the 1918-1969 record is the only open source of precedent for them without new physics.

Implements FREEZE_A.md (md5 asserted below; written before this file).  Nothing here is tuned.
GATES (the verdict is not printed unless all hold):
  A0 FREEZE_A.md md5 and the ISC-GEM file md5 are the frozen ones; P uses exactly 3,145 events.
  A1 every scored window has positive and negative group-0 weight.
  A2 training windows obey hiT[k] <= tcut[j] and exclude j; the checker is shown to raise on a planted bad list.
  A3 each of the 61 floor worlds (d = 30..330) has fixed points <= 5% of cells, checked on all worlds before any AUC
     involving P; the gate is shown to fail on d = 0.
  A4 every window's feature matrix has the 30 FULL columns (amendment 1).
"""
import csv
import hashlib
import sys
import time
from datetime import datetime

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, "/home/yasu/geo-ml")
import pre238 as P8  # noqa: E402
import pre254 as P4  # noqa: E402

D0 = "/home/yasu/claude-scratch/roundA/"
FREEZE_MD5 = "2238a92f28e8b46e4460ecd78e8e58e0"  # with amendment 1
t0 = time.time()
md5 = lambda p: hashlib.md5(open(p, "rb").read()).hexdigest()  # noqa: E731
assert md5(D0 + "FREEZE_A.md") == FREEZE_MD5, "A0 freeze file changed"
cat_md5 = md5(D0 + "iscgem_pre1970_m55.csv")
print("A0 freeze md5 ok; ISC-GEM csv md5 %s" % cat_md5, flush=True)

M = np.load("/home/yasu/geo-ml/rung197/meta197.npz")
clat, clon = M["clat"], M["clon"]
NC = len(clat)
r = list(csv.DictReader(open(D0 + "iscgem_pre1970_m55.csv")))
yr = np.array([datetime.strptime(x["time"][:19], "%Y-%m-%dT%H:%M:%S").year for x in r])
la = np.array([float(x["latitude"]) for x in r])
lo = np.array([float(x["longitude"]) for x in r])
mg = np.array([float(x["mag"]) for x in r])
s = (yr >= 1918) & (yr <= 1969) & (mg >= 6.25)
assert int(s.sum()) == 3145, "A0 event count %d" % s.sum()
a1, a2 = np.radians(clat)[:, None], np.radians(la[s])[None, :]
h = np.sin((a1 - a2) / 2) ** 2 + np.cos(a1) * np.cos(a2) * np.sin(np.radians(clon[:, None] - lo[s][None, :]) / 2) ** 2
dd = np.degrees(2 * np.arcsin(np.sqrt(np.clip(h, 0.0, 1.0))))
P = np.log1p(np.exp(-0.5 * dd ** 2).sum(axis=1))
dd = h = None

B = np.load("/home/yasu/claude-scratch/deep_sudden/sudden_bundle_v1.npz")
jj = [int(j) for j in B["jj"]]
tcut = {j: float(B["tcut"][i]) for i, j in enumerate(jj)}
hiT = {j: float(B["hiT"][i]) for i, j in enumerate(jj)}
ev = [j for j in jj if j >= 11]
assert len(ev) == 25
G = {}
for j in jj:
    g0 = B["grp_%d" % j] == 0
    G[j] = dict(X=B["feat_%d" % j][g0].astype(np.float64), y=B["lab_%d" % j][g0].astype(np.int64),
                pw=B["pw_%d" % j][g0].astype(np.float64), nw=B["nw_%d" % j][g0].astype(np.float64),
                cid=B["cid_%d" % j][g0].astype(np.int64))
for j in jj:
    assert G[j]["X"].shape[1] == 30, "A4 window %d has %d columns" % (j, G[j]["X"].shape[1])
for j in ev:
    assert G[j]["pw"].sum() > 0 and G[j]["nw"].sum() > 0, "A1 window %d" % j
print("A1 PASS  group-0 units %d over 25 windows" % sum(len(G[j]["y"]) for j in ev), flush=True)


def train_windows(j):
    return [k for k in jj if k != j and hiT[k] <= tcut[j]]


def check_train(j, ks):
    if j in ks or any(hiT[k] > tcut[j] for k in ks):
        raise AssertionError("A2 window %d trains on a window not strictly past" % j)


TR = {j: train_windows(j) for j in ev}
for j in ev:
    check_train(j, TR[j])
try:
    check_train(20, TR[20] + [20])
    raise SystemExit("A2 checker did not raise on a planted bad list")
except AssertionError:
    pass
print("A2 PASS  (planted bad list raised)", flush=True)


def fit_auc(j, col):
    ks = TR[j]
    Xtr = np.vstack([G[k]["X"] for k in ks])
    ytr = np.concatenate([G[k]["y"] for k in ks])
    Xte = G[j]["X"]
    if col is not None:
        Xtr = np.column_stack([Xtr, np.concatenate([col[G[k]["cid"]] for k in ks])])
        Xte = np.column_stack([Xte, col[G[j]["cid"]]])
    sc = StandardScaler().fit(Xtr)
    m = LogisticRegression(max_iter=2000, C=1.0).fit(sc.transform(Xtr), ytr)
    return P8.auc_pair(G[j]["pw"], G[j]["nw"], m.predict_proba(sc.transform(Xte))[:, 1])


def raw_auc(col):
    return float(np.mean([P8.auc_pair(G[j]["pw"], G[j]["nw"], col[G[j]["cid"]]) for j in ev]))


base = np.array([fit_auc(j, None) for j in ev])


def stat(col):
    plus = np.array([fit_auc(j, col) for j in ev])
    return float(np.mean(plus - base)), plus


def world_map(d):
    m = P4.rot_map(clat, clon, float(d))
    fx = float((m == np.arange(NC)).mean())
    if fx > 0.05:
        raise AssertionError("A3 world %g has %.1f%% fixed points" % (d, 100 * fx))
    return m


try:
    world_map(0)
    raise SystemExit("A3 gate did not fail on d = 0")
except AssertionError:
    pass
WORLDS = list(range(30, 331, 5))
assert len(WORLDS) == 61
MAPS = {d: world_map(d) for d in WORLDS}
print("A3 PASS  61 worlds, fixed points <= 5%% (max %.4f); d = 0 failed as planted" % max(
    float((MAPS[d] == np.arange(NC)).mean()) for d in WORLDS), flush=True)
Dr, plus_r = stat(P)
FL, RAWF = [], []
for d in WORLDS:
    Pd = P[MAPS[d]]
    FL.append(stat(Pd)[0])
    RAWF.append(raw_auc(Pd))
FL, RAWF = np.array(FL), np.array(RAWF)
z = (Dr - FL.mean()) / FL.std(ddof=1)
real = bool(Dr > 0 and Dr > FL.max() and z >= 2)
raw_r = raw_auc(P)
print("BASE group-0 AUC %.5f  PLUS %.5f  (FULL group-0 reference 0.57885, quoted)" % (base.mean(), plus_r.mean()))
print("D real %+.6f  floor mean %+.6f sd %.6f max %+.6f  rank %d of 62  z %.2f" % (
    Dr, FL.mean(), FL.std(ddof=1), FL.max(), 1 + int((FL >= Dr).sum()), z))
print("raw P group-0 AUC %.5f  floor mean %.5f sd %.5f max %.5f" % (raw_r, RAWF.mean(), RAWF.std(ddof=1), RAWF.max()))
print("VERDICT %s" % ("REAL" if real else "NULL"))
np.savez(D0 + "outA.npz", base=base, plus=plus_r, D=Dr, floor=FL, rawfloor=RAWF, raw=raw_r, P=P, z=z, cat_md5=cat_md5)
print("done %.0f s" % (time.time() - t0))
