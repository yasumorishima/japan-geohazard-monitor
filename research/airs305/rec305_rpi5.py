# -*- coding: utf-8 -*-
"""REC 305 (REPORTED-only covariate for round 305; no AIRS field is read here; RPi5 ~/geo-ml/rec305.py).
For every unit (cell, block) of build305.npz: did the cell have a driving event (M>=5.0, any, USGS, meta197) in the
60 days strictly before the issue?  Used only by a REPORTED line of pre305.py (opus code audit: a field that
responds to recent quakes in the cell).  Also writes the label-free block length in rows (block start rows of
build305; 11 rows per block, the last block of a window ends at the window end) for cal305.py.
Usage: python rec305.py BUNDLE OUT.npz
"""
import sys

import numpy as np

sys.path.insert(0, "/home/yasu/geo-ml")
import size208 as S8

W = "/home/yasu/geo-ml/"
B = np.load(sys.argv[1])
tiss, win, row = B["tiss"], B["win"].astype(int), B["row"].astype(int)
ymat, tdays, clat, clon, MT, NDEF, wins, T, NC, NW = S8.load_arena()
assert np.array_equal(clat, B["clat"]) and np.array_equal(clon, B["clon"])
assert np.array_equal(tdays[row], tiss), "issue time = tdays[row]"
nrow = np.zeros(len(row), np.int16)
for j in np.unique(win):
    m = np.flatnonzero(win == j)
    tr, lo, hi = wins[j]
    assert row[m[0]] == lo
    ends = np.r_[row[m[1:]], hi]
    nrow[m] = ends - row[m]
    assert np.all(nrow[m][:-1] == 11), "block length %d" % j
dt = np.diff(tdays)
print("row spacing days: min %.4f max %.4f" % (dt.min(), dt.max()))
print("last-block lengths:", sorted(set(nrow[np.r_[np.flatnonzero(np.diff(win)), len(win) - 1]].tolist())))
MD = np.load(W + "rung197/meta197.npz")
dtd, dcell, dmag = MD["dtd"], MD["dcell"], MD["dmag"]
assert dmag.min() >= 5.0 - 1e-9
o = np.argsort(dtd, kind="stable")
dtd, dcell = dtd[o], dcell[o]
rec = np.zeros((len(tiss), NC), bool)
for i, t in enumerate(tiss):
    a = np.searchsorted(dtd, t - 60.0, side="left")
    b = np.searchsorted(dtd, t, side="left")      # strictly before the issue
    rec[i, dcell[a:b]] = True
    assert b == 0 or dtd[b - 1] < t
print("units with M>=5 in the 60 days before the issue: %.4f" % rec.mean())
np.savez_compressed(sys.argv[2], rec60=rec, nrow=nrow, tday_step=np.float64(np.median(dt)))
print("DONE")
