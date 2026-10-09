# -*- coding: utf-8 -*-
"""Extend the sudden-axis bundle with window 36, so the frozen feature-only network (FREEZE_notok.md) can be scored
on a window it has never seen.

SUDDEN AXIS: FULL 0.62086 over the 25 seen windows; the frozen network gave +0.00636 there (exploratory).
DIMENSION CEILING: WHICH CELL (per-window cell oracle 0.95017).
WHY THIS ROUND: the gain was found on seen windows; window 36 (rows 6818..6889, 213 days of 2026) was never touched by
  this learner, whose settings were frozen (ac12c44) before this file was written.  This file only builds data.

Window 36 is built exactly as round 258 did (pre258.window36 and pre258.make_build_ext, which reuse chk238.build with
window 36 appended; the lmp choice of window 35 is carried, made before window 36).

GATES (the bundle is not written unless all hold):
  X1 every array of windows 3..35 equals sudden_bundle_v1 (feat, lab, pw, nw, arm, grp, cell index), and the window
     list is v1's plus 36.
  X2 FULL refit on window 36 (strictly past windows, as chk238) reproduces round 258's logged 0.672587 to 5e-7.
  X3 window 36's training windows exclude window 35 (the hiT <= cut clause is true by construction and binds nothing).
"""
import sys
import time

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, "/home/yasu/geo-ml")
import chk238 as C  # noqa: E402
import pre258 as P  # noqa: E402

V1 = "/home/yasu/claude-scratch/deep_sudden/sudden_bundle_v1.npz"
OUT = "/home/yasu/claude-scratch/deep_sudden/sudden_bundle_v2.npz"

t0 = time.time()
w36, NW = P.window36()
J = NW
assert J == 36 and w36[1] == 6818 and w36[2] == 6889, ("declared window", w36, NW)
ELS = {}
D = P.make_build_ext(w36, ELS)()
Z = np.load(V1)
B1 = {k: Z[k] for k in Z.files}
jj1 = [int(j) for j in B1["jj"]]
jj, tcuts, hiT = D["jj"], D["tcuts"], D["hiT"]
assert jj == jj1 + [J], "X1 window list"
for i, j in enumerate(jj1):
    pairs = [("feat", D["feats"][j]), ("lab", D["labs"][j]), ("pw", D["pcs"][j].astype(np.float32)),
             ("nw", D["ncs"][j].astype(np.float32)), ("arm", D["armflat"][j]), ("grp", D["grp"][j].astype(np.int8)),
             ("cid", ELS[j].astype(np.int32))]
    for key, a in pairs:
        assert np.array_equal(B1["%s_%d" % (key, j)], a), "X1 %s window %d" % (key, j)
    assert float(B1["tcut"][i]) == tcuts[j] and float(B1["hiT"][i]) == hiT[j], "X1 times %d" % j
print("X1 PASS  windows 3..35 equal v1", flush=True)

strict = [k for k in jj if k < J and hiT[k] <= tcuts[J]]
assert (J - 1) not in strict and all(hiT[k] <= tcuts[J] for k in strict), "X3"
Xtr = np.vstack([D["feats"][k] for k in strict])
ytr = np.concatenate([D["labs"][k] for k in strict])
sc = StandardScaler().fit(Xtr)
mdl = LogisticRegression(max_iter=2000, C=1.0).fit(sc.transform(Xtr), ytr)
f36 = C.auc_pair(D["pcs"][J], D["ncs"][J], mdl.predict_proba(sc.transform(D["feats"][J]))[:, 1])
print("X2 FULL window 36 %.7f (round 258 logged 0.672587); trains on %d windows, last %d" % (f36, len(strict),
                                                                                                max(strict)), flush=True)
assert abs(f36 - 0.672587) < 5e-7, "X2 FULL on window 36 does not reproduce round 258"

out = dict(B1)
out["jj"] = np.array(jj)
out["tcut"] = np.array([tcuts[j] for j in jj])
out["hiT"] = np.array([hiT[j] for j in jj])
out["new"] = np.array([J])
out["ref_full_new"] = np.array([f36])
out["cid_%d" % J] = ELS[J].astype(np.int32)
out["lab_%d" % J] = D["labs"][J]
out["pw_%d" % J] = D["pcs"][J].astype(np.float32)
out["nw_%d" % J] = D["ncs"][J].astype(np.float32)
out["arm_%d" % J] = D["armflat"][J]
out["feat_%d" % J] = D["feats"][J]
out["grp_%d" % J] = D["grp"][J].astype(np.int8)
np.savez_compressed(OUT, **out)
print("written %s  %.0f s  window 36: %d cells, positive weight %.0f" % (OUT, time.time() - t0, len(ELS[J]),
                                                                        D["pcs"][J].sum()), flush=True)
