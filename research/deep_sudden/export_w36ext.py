# -*- coding: utf-8 -*-
"""Rebuild window 36 on the extended arena (ext197.py: stored catalogue before 2026 + the 2026 rows fetched
2026-10-10), so the frozen feature-only network (FREEZE_notok.md) can be scored on the longer, revised window 36.

SUDDEN AXIS: ACTIVE; FULL 0.62086 over the 25 seen windows; window 36 as first exported (rows 6818..6889, 2026-09-02
  catalogue): NOTOK 0.68657 vs FULL 0.67259.
DIMENSION CEILING: WHICH CELL (per-window cell oracle 0.95017).
WHY THIS ROUND: window 36 is the only window outside v1 the global arena has; a later catalogue makes 12 more steps
  of it exist and revises its July-September labels.  No learner setting is touched; this only builds data.

Same construction as export_w36.py (pre258.window36 + chk238.build with window 36 appended, lmp choice of window 35
carried), pointed at the extended arena: size208.WORK -> rung197ext and chk238's meta197 path -> rung197ext.  The
size208 count gates that cover windows 0..35 (wins, rows, pos, ISOROWS) are kept unchanged and act as prefix gates;
the whole-catalogue counts (steps, drive, iso_ev, BIGEV) are set from ext197's output.

GATES (the bundle is not written unless all hold):
  X1 every array of windows 3..35 equals sudden_bundle_v1, and the window list is v1's plus 36.
  X3 window 36's training windows exclude window 35.
  X4 window 36 starts at step 6818 as before and ends at the extended arena's NDEF (> 6889); for every cell present in
     both v2's and this window 36, the feature rows are identical to v2 (features are fixed at the cut, before 2026).

BEFORE THE COMPLETE-WINDOW REBUILD (fetch on or after 2027-05-05; code audit 2026-10-10), this file must change:
  - once the catalogue ends after about 2027-02-05, load_arena finds window 36 by itself (37 windows): take
    wins[36] (hi = searchsorted(tdays, window end)) instead of appending pre258.window36, set G wins = 37 while
    keeping pos / ISOROWS as gates on windows 0..35, and read SEL31[:, 35] explicitly for window 36 (out231 has 36
    windows); assert the catalogue end is at least tdays[hi - 1] + 34 + 90 days.
  - replace the 6889 / > 6889 checks with the declared bounds of the complete window.
  - add an E3-style check of window 36's own early events against this 2026-10-10 build (rung197ext), and write to
    a new output directory rather than overwriting rung197ext.
  - fetch with fetch_ext.sh (count-checked) and pin the stored pre-2026 catalogue by md5.
"""
import inspect
import sys
import time

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, "/home/yasu/geo-ml")
import size208 as S8  # noqa: E402

EXT = "/home/yasu/claude-scratch/deep_sudden/rung197ext/"
S8.WORK = EXT
_m = np.load(EXT + "meta197.npz")
_ki, _dm = _m["keep_iso"], _m["dmag"]
S8.G = dict(S8.G, steps=int(np.load(EXT + "t197.npy").shape[0]), drive=int(len(_m["dtd"])), iso_ev=int(_ki.sum()))
S8.BIGEV = [int((_ki & (_dm >= t)).sum()) for t in S8.THRS]
import chk238 as C  # noqa: E402
import pre258 as P  # noqa: E402

V1 = "/home/yasu/claude-scratch/deep_sudden/sudden_bundle_v1.npz"
V2 = "/home/yasu/claude-scratch/deep_sudden/sudden_bundle_v2.npz"
OUT = "/home/yasu/claude-scratch/deep_sudden/sudden_bundle_v3.npz"


def make_build_ext(w36, ELS):
    src = inspect.getsource(C.build)
    NL = chr(10)
    reps = [("    for j in range(WARM, NW):" + NL + "        tr, lo, hi = wins[j]" + NL,
             "    wins = list(wins) + [W36]" + NL
             + "    for j in range(WARM, NW + 1):" + NL + "        tr, lo, hi = wins[j]" + NL),
            ("gi, si, ai = SEL31[q31, j]", "gi, si, ai = SEL31[q31, min(j, NW - 1)]"),
            ("        feats[j] = F[el]" + NL,
             "        feats[j] = F[el]" + NL + "        ELS[j] = np.flatnonzero(el)" + NL),
            ('np.load(W + "rung197/meta197.npz")', "np.load(EXTMETA)"),
            ("def build():", "def build_ext():")]
    for a, b in reps:
        assert src.count(a) == 1, "build source changed: " + a[:30]
        src = src.replace(a, b)
    ns = dict(C.__dict__)
    ns["W36"], ns["ELS"], ns["EXTMETA"] = w36, ELS, EXT + "meta197.npz"
    exec(compile(src, "build_ext", "exec"), ns)
    return ns["build_ext"]


t0 = time.time()
w36, NW = P.window36()
J = NW
print("extended window 36 (tr, lo, hi) =", w36, flush=True)
assert J == 36 and w36[1] == 6818 and w36[2] > 6889, ("X4 declared window", w36, NW)
ELS = {}
D = make_build_ext(w36, ELS)()
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

V = np.load(V2)
c2, f2 = V["cid_36"], V["feat_36"]
c3, f3 = ELS[J].astype(np.int32), D["feats"][J]
com, i2, i3 = np.intersect1d(c2, c3, return_indices=True)
assert len(com) > 0.9 * len(c2), "X4 too few common cells %d of %d" % (len(com), len(c2))
assert np.array_equal(f2[i2], f3[i3]), "X4 window 36 features moved"
assert float(V["tcut"][-1]) == tcuts[J], "X4 cut moved"
print("X4 PASS  window 36 cells %d (v2 %d, common %d, v2-only %d, new %d); features of common cells identical" % (
    len(c3), len(c2), len(com), len(np.setdiff1d(c2, c3)), len(np.setdiff1d(c3, c2))), flush=True)

strict = [k for k in jj if k < J and hiT[k] <= tcuts[J]]
assert (J - 1) not in strict and all(hiT[k] <= tcuts[J] for k in strict), "X3"
Xtr = np.vstack([D["feats"][k] for k in strict])
ytr = np.concatenate([D["labs"][k] for k in strict])
sc = StandardScaler().fit(Xtr)
mdl = LogisticRegression(max_iter=2000, C=1.0).fit(sc.transform(Xtr), ytr)
f36 = C.auc_pair(D["pcs"][J], D["ncs"][J], mdl.predict_proba(sc.transform(D["feats"][J]))[:, 1])
print("FULL extended window 36 %.7f (v2 window 36: %.7f); trains on %d windows, last %d" % (
    f36, float(V["ref_full_new"][0]), len(strict), max(strict)), flush=True)

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
print("written %s  %.0f s  window 36: steps %d..%d, %d cells, positive weight %.0f (v2 %.0f)" % (
    OUT, time.time() - t0, w36[1], w36[2], len(ELS[J]), D["pcs"][J].sum(), float(V["pw_36"].sum())), flush=True)
