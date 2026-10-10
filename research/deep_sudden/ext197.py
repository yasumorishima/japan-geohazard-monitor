# -*- coding: utf-8 -*-
"""Extend the global arena (rung197) to a newer USGS catalogue, so window 36 can carry the rows that a later catalogue
has made available.  Builds y197/t197/meta197 only (chk238 never reads x197).

SUDDEN AXIS: ACTIVE; FULL 0.62086 over 25 seen windows; frozen NOTOK scored window 36 once (0.68657 vs FULL 0.67259).
DIMENSION CEILING: WHICH CELL (per-window cell oracle 0.95017).
WHY THIS ROUND: no learner is fitted here; this only makes the next unseen rows exist.  The frozen contract needs
  windows outside v1, and the global arena only grows when the catalogue grows.

Catalogue: rows of ~/geo-ml/boxes/global_m45.csv before 2026-01-01 (the copy every earlier round used) plus the 2026
rows fetched now (y2026_now.csv).  Before 2026 USGS counts match the stored file in every year 1970-2025 (checked
2026-10-10); 2025 content is compared row by row below.

GATES (nothing is written unless all hold):
  E1 2025 rows of the stored file equal a fresh 2025 fetch (id, time, lat, lon, mag), so the spliced prefix is the
     current USGS prefix as far as the last seen window's labels can reach.
  E2 the cell list is exactly rung197's (seed years only, unchanged by construction; asserted anyway).
  E3 every driving event of rung197 before 2026-06-01 (rung197 was built from the 2026-09-02 fetch; the first driving event that differs from it is 2026-06-21, a 1 s time revision) is present with identical
     (dtd, dcell, dmag, keep_iso) in the same order.
  E4 tdays prefix equals rung197's t197 exactly; Y rows of rung197 before step 6818 (window 36's start) are equal,
     NaN pattern included.
"""
import csv
import hashlib
import sys

import numpy as np

sys.path.insert(0, "/home/yasu/geo-ml")
import size196 as SZ  # noqa: E402

OLD = "/home/yasu/geo-ml/boxes/global_m45.csv"
NEW26 = "/home/yasu/claude-scratch/cat_ext/y2026_now.csv"
NEW25 = "/home/yasu/claude-scratch/cat_ext/y2025_now.csv"
CAT = "/home/yasu/claude-scratch/cat_ext/global_m45_ext.csv"
OW = "/home/yasu/geo-ml/rung197/"
WORK = "/home/yasu/claude-scratch/deep_sudden/rung197ext/"
HOR, STEP, YEAR = 34.0, 3.0, 365.25
MDRV, SEED_YR, SEED_MIN = 5.0, 15.0, 2


def rows(path, year):
    out = []
    with open(path) as f:
        r = csv.reader(f)
        next(r)
        for p in r:
            if p[0][:4] == year:
                out.append((p[11], p[0], p[1], p[2], p[4]))
    return out


OLD_MD5 = "86f209f267e6e2430690e8011cb4ba23"  # the 2026-09-17 refetch (fetch221.sh); pinned 2026-10-10
assert hashlib.md5(open(OLD, "rb").read()).hexdigest() == OLD_MD5, "stored catalogue changed"
a, b = rows(OLD, "2025"), rows(NEW25, "2025")
assert sorted(a) == sorted(b), "E1 2025 content moved: %d vs %d rows, %d differ" % (
    len(a), len(b), len(set(a) ^ set(b)))
print("E1 PASS  2025 rows %d identical to a fresh fetch" % len(a), flush=True)

with open(CAT, "w") as fo, open(OLD) as fi:
    head = fi.readline()
    fo.write(head)
    n_old = 0
    for line in fi:
        if line[:4] < "2026":
            fo.write(line)
            n_old += 1
    with open(NEW26) as fn:
        h2 = fn.readline()
        assert h2 == head, "header differs"
        n_new = 0
        for line in fn:
            if line[:4] >= "2026":
                fo.write(line)
                n_new += 1
print("spliced catalogue: %d rows before 2026 + %d rows of 2026" % (n_old, n_new), flush=True)

t, la, lo, mg = SZ.read_text(CAT, "csv")
t = t - t.min()
span = float(t.max())
gid = (np.floor(la).astype(np.int64) + 90) * 360 + (np.floor(lo).astype(np.int64) + 180)
seed = (t < SEED_YR * YEAR) & (mg >= MDRV)
u, cnt = np.unique(gid[seed], return_counts=True)
cells = u[cnt >= SEED_MIN]
NC = len(cells)
OM = np.load(OW + "meta197.npz")
assert np.array_equal(cells, OM["cells"]), "E2 cell list"
p = np.searchsorted(cells, gid)
inar = (p < NC) & (cells[np.minimum(p, NC - 1)] == gid)
cell = np.where(inar, p, -1)
clat = (cells // 360 - 90).astype(np.float64) + 0.5
clon = (cells % 360 - 180).astype(np.float64) + 0.5
tdays = np.arange(0, span + 0.1, STEP)
T = len(tdays)
ot = np.load(OW + "t197.npy")
assert np.array_equal(tdays[:len(ot)], ot), "E4 tdays prefix"

sel = (mg >= MDRV) & inar
dtd, dcell, dla, dlo, dmag = t[sel], cell[sel], la[sel], lo[sel], mg[sel]
spn = np.empty((len(dtd), 2), np.int64)
spn[:, 0] = np.searchsorted(tdays, dtd - HOR, side="right")
spn[:, 1] = np.searchsorted(tdays, dtd, side="right")
ymat = SZ.paint(spn, dcell, T, NC, np.ones(len(dtd), bool))
Y = np.where(tdays[:, None] + HOR <= span + 0.5, ymat.astype(np.float32), np.nan)
pm = mg >= MDRV
keep_iso = SZ.gk_isolated(dtd, dla, dlo, dmag, t[pm], la[pm], lo[pm], mg[pm])
Mi = SZ.paint(spn, dcell, T, NC, keep_iso)

from datetime import date  # noqa: E402
t0 = date(1970, 1, 1).toordinal()
o = SZ.read_text(OLD, "csv")[0]
torg = o.min()
rev = date(2026, 6, 1).toordinal() - torg
ko = OM["dtd"] < rev
kn = dtd < rev
assert ko.sum() == kn.sum(), "E3 count before the revision date %d vs %d" % (ko.sum(), kn.sum())
for key, new in (("dtd", dtd), ("dcell", dcell), ("dmag", dmag), ("keep_iso", keep_iso)):
    assert np.array_equal(OM[key][ko], new[kn]), "E3 " + key
print("E3 PASS  %d driving events before 2026-06-01 identical" % kn.sum(), flush=True)

OY = np.load(OW + "y197.npy")
assert np.array_equal(OY[:6818], Y[:6818], equal_nan=True), "E4 Y prefix"
print("E4 PASS  tdays prefix and Y rows < 6818 identical", flush=True)

np.save(WORK + "y197.npy", Y)
np.save(WORK + "t197.npy", tdays)
np.savez(WORK + "meta197.npz", cells=cells, clat=clat, clon=clon, Mi=Mi, dtd=dtd, dcell=dcell, dmag=dmag,
         keep_iso=keep_iso, span=spn)
nanrow = np.isnan(Y).any(axis=1)
NDEF = int(np.argmax(nanrow)) if nanrow.any() else T
print("written  steps %d (was %d)  NDEF %d  driving %d  isolated %d  catalogue end day %.2f" % (
    T, len(ot), NDEF, len(dtd), int(keep_iso.sum()), span), flush=True)
