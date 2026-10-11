# -*- coding: utf-8 -*-
"""SIZING 302 (no verdict; no column AUC): how many class 0-2 rows would a century-long renewal clock reach?

SUDDEN AXIS: global ACTIVE; FULL 0.62086 over 25 seen windows.
DIMENSION CEILING: WHICH CELL, PER WINDOW; per-window cell oracle 0.95017; sizing 301b: knowing which repeating
  cells fire this window lifts class 1 0.530->0.650 and class 2 0.546->0.704, while static counts give +0.002.
WHY THIS ROUND: round 260's clock needed 3 isolated shocks since 1970, so it reached only class 3 (NO GAIN);
  the 1918-1969 ISC-GEM record is the only open source that gives classes 0-2 an interval; count the reach first.

Clock events (homogeneous M>=6.25 in both eras): ISC-GEM 1918-1969 M>=6.25 that are GK-isolated against all
  earlier ISC-GEM rows (M>=5.5, 1904-1969), plus 1970+ meta197 keep_iso & dmag>=6.25.  Eligible row: class 0-2
  row whose cell has >= 2 clock events strictly before tcut.  Printed: eligible rows and positive mass per class.
"""
import sys

import numpy as np

sys.path.insert(0, "/home/yasu/geo-ml")
import chk238 as C8  # noqa: E402
import size196 as SZ  # noqa: E402

D = C8.build()
jj, tc, grp, pcs = D["jj"], D["tcuts"], D["grp"], D["pcs"]
ev = [j for j in jj if j >= 11]
MD = np.load("/home/yasu/geo-ml/rung197/meta197.npz")
cells = MD["cells"]
NC = len(cells)
tg = SZ.read_text("/home/yasu/geo-ml/boxes/global_m45.csv", "csv")[0]
t0g = tg.min()
ti, lai, loi, mgi = SZ.read_text("/home/yasu/claude-scratch/roundA/iscgem_pre1970_m55.csv", "csv")
ti = ti - t0g
print("ISC-GEM rows %d, last day %.1f (global origin; must be < 0)" % (len(ti), ti.max()))
assert ti.max() < 0
iso_i = SZ.gk_isolated(ti, lai, loi, mgi, ti, lai, loi, mgi)
yr1918 = -(1970 - 1918) * 365.25 - 30
sel_i = iso_i & (mgi >= 6.25) & (ti >= yr1918)
gid = (np.floor(lai).astype(np.int64) + 90) * 360 + (np.floor(loi).astype(np.int64) + 180)
p = np.searchsorted(cells, gid)
inar = (p < NC) & (cells[np.minimum(p, NC - 1)] == gid)
print("ISC-GEM isolated M>=6.25 from ~1918: %d, in arena cells %d (all M>=6.25 in arena %d)" % (
    sel_i.sum(), (sel_i & inar).sum(), ((mgi >= 6.25) & (ti >= yr1918) & inar).sum()))
s7 = MD["keep_iso"] & (MD["dmag"] >= 6.25)
et = np.r_[ti[sel_i & inar], MD["dtd"][s7]]
ec = np.r_[p[sel_i & inar], MD["dcell"][s7]].astype(np.int64)
for c in range(4):
    er = pm = tr = tm = 0.0
    for j in ev:
        n = np.bincount(ec[et < tc[j]], minlength=NC)[C8.build.__globals__.get("EL", {}).get(j, None)] if False else None
    print()
EL = {}
import ctl264  # noqa: E402
C = ctl264.Control(D)
tot = {c: [0, 0.0, 0, 0.0] for c in range(4)}
for j in ev:
    n = np.bincount(ec[et < tc[j]], minlength=NC)[C.EL[j]]
    for c in range(4):
        m = grp[j] == c
        e = m & (n >= 2)
        tot[c][0] += int(m.sum()); tot[c][1] += float((pcs[j][m] > 0).sum())
        tot[c][2] += int(e.sum()); tot[c][3] += float((pcs[j][e] > 0).sum())
for c in range(4):
    a, b, e, f = tot[c]
    print("class %d: rows %d, positive rows %d | eligible rows %d (%.1f%%), positive eligible %d (%.1f%%)" % (
        c, a, b, e, 100 * e / a, f, 100 * f / max(b, 1)))
