# -*- coding: utf-8 -*-
"""Rebuild of round 263's SHAD column (the file audit263post/R.npz is gone) for round 302's gate G5.

SUDDEN AXIS: global ACTIVE; FULL 0.62086 over 25 seen windows.
DIMENSION CEILING: not a round; SHAD is the known definitional column (ISO gain from the GK rule only).
WHY THIS ROUND: round 302's R3 must be shown able to reject a definitional column; the stored SHAD is lost, so it
  is rebuilt with round 263's own field() code and accepted only if FULL+SHAD reproduces out263.npz["shad"].
"""
import sys
import time

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, "/home/yasu/geo-ml")
import chk238 as C8  # noqa: E402
import ctl264  # noqa: E402
import pre263 as P3  # noqa: E402
import size196 as SZ  # noqa: E402
import size208 as S8  # noqa: E402
from pre238c import auc_fast as auc  # noqa: E402

W = "/home/yasu/geo-ml/"
t0 = time.time()
tu, la, lo, mg = SZ.read_text(W + "boxes/global_m45.csv", "csv")
ORIG = float(tu.min())
k6 = mg >= 6.0
et, ela, elo, em = tu[k6] - ORIG, la[k6], lo[k6], mg[k6]
eL, eT = P3.gkL(em), P3.gkT(em)
D = C8.build()
C = ctl264.Control(D)
jj, ev, feats, labs, pcs, ncs, tcuts = C.jj, C.ev, D["feats"], D["labs"], D["pcs"], D["ncs"], D["tcuts"]
_, tdays, clat, clon, _, _, wins, _, NC, _ = S8.load_arena()
OFF = P3.OFF


def field(j, tlim):  # verbatim logic of pre263.main.field
    tr, lo_, hi_ = wins[j]
    days = np.arange(np.floor(tdays[lo_]), np.floor(tdays[hi_ - 1]) + 1)
    act = (et < tlim) & (et + eT >= days[0])
    out = np.zeros(len(C.EL[j]))
    for q, c in enumerate(C.EL[j]):
        PL, PO = np.meshgrid(clat[c] + OFF, clon[c] + OFF)
        PL, PO = PL.ravel(), PO.ravel()
        idx = np.flatnonzero(act & (np.abs(ela - clat[c]) < 2.5))
        if len(idx) == 0:
            continue
        cov = np.zeros((len(PL), len(days)), bool)
        for e in idx:
            ins = SZ.km(np.full(len(PL), ela[e]), np.full(len(PL), elo[e]), PL, PO) <= eL[e]
            if ins.any():
                live = (days - et[e] <= eT[e]) & (days >= et[e])
                cov[np.ix_(ins, live)] = True
        out[q] = cov.mean()
    return out


SH = {j: field(j, tcuts[j]) for j in jj}
print("SHAD rebuilt %.0f s" % (time.time() - t0), flush=True)
out = []
for j in ev:
    ks = C.strict[j]
    Xtr = np.vstack([np.c_[feats[k], SH[k]] for k in ks])
    sc = StandardScaler().fit(Xtr)
    md = LogisticRegression(max_iter=2000, C=1.0).fit(sc.transform(Xtr), np.concatenate([labs[k] for k in ks]))
    out.append(auc(pcs[j], ncs[j], md.predict_proba(sc.transform(np.c_[feats[j], SH[j]]))[:, 1]))
ref = np.load(W + "out263.npz")["shad"]
d = np.abs(np.array(out) - ref).max()
print("FULL+SHAD per-window AUC vs out263.npz max diff %.2e" % d, flush=True)
assert d <= 1e-9, "rebuilt SHAD does not reproduce round 263"
np.savez("/home/yasu/claude-scratch/roundA2/shad263.npz", **{"sh%d" % j: SH[j] for j in jj},
         **{"el%d" % j: C.EL[j] for j in jj})
print("saved shad263.npz")
