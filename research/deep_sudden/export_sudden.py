# -*- coding: utf-8 -*-
"""Export the global sudden-axis (ACTIVE) evaluation as one self-contained bundle, so that models can be trained
and scored off the RPi5 (GitHub Actions CPU, Colab GPU) with exactly the scoring of chk238/pre238.

Built by chk238.build() itself (its source is reused with one added line that records the eligible cell ids), so
rows, labels, weights, arm and features are the frozen run's.  Everything comes from the USGS FDSN catalogue
(public domain) and quantities derived from it.

GATES (the bundle is not written unless both hold):
  E1 the arm's per-cell map gives chk238's recorded 25-window ACTIVE mean 0.59033 (lambda 1 row of chk238.log) to
     5e-6.  (0.5785 is a different quantity: the row-level arm over 33 windows.)
  E2 refitting FULL exactly as chk238.main (StandardScaler + LogisticRegression C=1, strictly past windows) gives
     0.62086 to 5e-6 (the recorded value is 0.62086 at 5 decimals).
"""
import sys
import time

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, "/home/yasu/geo-ml")
import chk238 as C  # noqa: E402

OUT = "/home/yasu/claude-scratch/deep_sudden/sudden_bundle_v1.npz"

src = open("/home/yasu/geo-ml/chk238.py", encoding="utf-8").read()
b0 = src.index("def build():")
b1 = src.index("\ndef main():")
bsrc = src[b0:b1]
old = "        jj.append(j)\n"
assert bsrc.count(old) == 1
bsrc = bsrc.replace(old, "        CIDS[j] = np.flatnonzero(el)\n" + old)
ns = dict(vars(C))
ns["CIDS"] = {}
exec(bsrc, ns)
t0 = time.time()
D = ns["build"]()
CIDS = ns["CIDS"]
jj, tcuts, hiT = D["jj"], D["tcuts"], D["hiT"]
ev = [j for j in jj if j >= C.START]
assert len(ev) == 25, len(ev)
for j in jj:
    assert len(CIDS[j]) == len(D["labs"][j]) == len(D["pcs"][j])

arm = np.array([C.auc_pair(D["pcs"][j], D["ncs"][j], D["armflat"][j]) for j in ev])
print("E1 arm per-cell map, 25-window ACTIVE mean %.5f (recorded by chk238 at lambda 1: 0.59033)" % arm.mean(), flush=True)

strict = {j: [k for k in jj if k < j and hiT[k] <= tcuts[j]] for j in ev}
full = []
for j in ev:
    Xtr = np.vstack([D["feats"][k] for k in strict[j]])
    ytr = np.concatenate([D["labs"][k] for k in strict[j]])
    sc = StandardScaler().fit(Xtr)
    mdl = LogisticRegression(max_iter=2000, C=1.0).fit(sc.transform(Xtr), ytr)
    p = mdl.predict_proba(sc.transform(D["feats"][j]))[:, 1]
    full.append(C.auc_pair(D["pcs"][j], D["ncs"][j], p))
full = np.array(full)
print("E2 FULL 25-window mean %.6f (recorded 0.62086)" % full.mean(), flush=True)
assert abs(full.mean() - 0.62086) < 5e-6, "E2 FULL does not reproduce"
assert abs(arm.mean() - 0.59033) < 5e-6, "E1 arm map does not reproduce"  # 0.5785 is the row-level arm over 33 windows

MD = np.load(C.W + "rung197/meta197.npz")
out = dict(names=np.array(D["names"]), jj=np.array(jj), ev=np.array(ev),
           tcut=np.array([tcuts[j] for j in jj]), hiT=np.array([hiT[j] for j in jj]),
           clat=MD["clat"], clon=MD["clon"],
           ev_t=MD["dtd"], ev_cell=MD["dcell"], ev_mag=MD["dmag"], ev_iso=MD["keep_iso"],
           ref_arm=arm, ref_full=full)
for j in jj:
    out["cid_%d" % j] = CIDS[j].astype(np.int32)
    out["lab_%d" % j] = D["labs"][j]
    out["pw_%d" % j] = D["pcs"][j].astype(np.float32)
    out["nw_%d" % j] = D["ncs"][j].astype(np.float32)
    out["arm_%d" % j] = D["armflat"][j]
    out["feat_%d" % j] = D["feats"][j]
    out["grp_%d" % j] = D["grp"][j].astype(np.int8)
np.savez_compressed(OUT, **out)
print("written %s  %.0f s" % (OUT, time.time() - t0), flush=True)
