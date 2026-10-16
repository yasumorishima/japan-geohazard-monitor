# -*- coding: utf-8 -*-
"""Combine the SETP lines of all cal305.py shards (read from log files) into the calibration verdict.
Required (pre305 docstring): share of label sets with p <= 0.05 in [0.01, 0.10] for floors T, L, Y and <= 0.10
for p_max, for every test (co5, ch46 and their residuals); floor A is printed only (anti-conservative alone).
Every set 0..NSET-1 must be present exactly once per test.
Usage: python combine305.py NSET LOG [LOG ...]"""
import sys

import numpy as np

NSET = int(sys.argv[1])
P = {}
for f in sys.argv[2:]:
    for ln in open(f, encoding="utf-8", errors="replace"):
        if "SETP " not in ln:
            continue
        t = ln[ln.index("SETP "):].split()
        key = (int(t[1]), t[2])
        assert key not in P, "duplicate %s" % (key,)
        P[key] = {kv.split("=")[0]: float(kv.split("=")[1]) for kv in t[3:]}
tests = sorted({k[1] for k in P})
assert tests == ["ch46", "ch46_res", "co5", "co5_res"], tests
ok = True
for g in tests:
    sets = sorted(k[0] for k in P if k[1] == g)
    assert sets == list(range(NSET)), "test %s: sets %d of %d" % (g, len(sets), NSET)
    rows = [P[(s, g)] for s in sets]
    line = []
    for fl in ("T", "L", "Y", "A", "max"):
        p = np.array([max(r.values()) if fl == "max" else r[fl] for r in rows])
        sh = np.mean(p <= 0.05)
        if fl in ("T", "L", "Y"):
            ok &= 0.01 <= sh <= 0.10
        elif fl == "max":
            ok &= sh <= 0.10
        line.append("%s %.3f" % (fl, sh))
    print("CALIBRATION %-8s sets %d share p<=0.05: %s" % (g, len(rows), "  ".join(line)))
print("CALIBRATION", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
