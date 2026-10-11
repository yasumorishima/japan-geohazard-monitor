# -*- coding: utf-8 -*-
"""SIZING 301 (no verdict, no ISC-GEM column touched): how much can a static per-cell column, entered into FULL
as [c, c*1(group 0)], move the whole ACTIVE axis?  Run before any plan for round 301 is frozen.

SUDDEN AXIS: global ACTIVE; FULL 0.62086 over 25 seen windows (round 300 moved group 0 only: splice +0.0006..0.0009).
DIMENSION CEILING: WHICH CELL, static map; per-cell static map bound 0.8112; class-0-only bound 0.73285.
WHY THIS ROUND: 85.7% of class 0's share is between classes, which a group-0-only model cannot reach by construction;
  before testing the 1918-1969 record in the whole model, measure what this form could deliver with label knowledge.

Columns (all self-knowledge oracles, used only for sizing):
  ORC   count of scored windows (11..35) in which the cell has an isolated M>=6 positive (includes the test window)
  TWO   same count excluding windows j-1, j, j+1 for test window j (reachable-ish width)
  PLC   ORC permuted across cells once (placebo: free key, no information)
Forms: A = [c, c*g0]; B = [c*g0] only.  Reported under ISO and ANY labels via ctl264.Control.
"""
import sys
import time

import numpy as np

sys.path.insert(0, "/home/yasu/geo-ml")
import chk238 as C8  # noqa: E402
import ctl264  # noqa: E402

t0 = time.time()
D = C8.build()
C = ctl264.Control(D)
jj, ev = C.jj, C.ev
print("built %.0f s" % (time.time() - t0), flush=True)
NC = 2280
cnt = np.zeros(NC)
hit = {}
for j in ev:
    h = np.zeros(NC)
    h[C.EL[j][C.PI[j] > 0]] = 1
    hit[j] = h
    cnt += h
rng = np.random.default_rng(301)
perm = rng.permutation(NC)


def form(colfun, mode):
    out = {}
    for j in jj:
        c = colfun(j)[C.EL[j]]
        g0 = (D["grp"][j] == 0).astype(np.float64)
        out[j] = np.c_[c, c * g0] if mode == "A" else (c * g0)[:, None]
    return out


def two(j):
    if j not in ev:
        return cnt
    return cnt - sum(hit[k] for k in (j - 1, j, j + 1) if k in hit)


cols = {"ORC": lambda j: cnt, "TWO": two, "PLC": lambda j: cnt[perm]}
base = None
for nm, f in cols.items():
    for mode in ("A", "B"):
        r = C.run(form(f, mode), base=base)
        base = {k: r[k]["base"] for k in r}
        ctl264.show("%s-%s" % (nm, mode), r)
print("done %.0f s" % (time.time() - t0))
