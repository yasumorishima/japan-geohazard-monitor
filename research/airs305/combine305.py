# -*- coding: utf-8 -*-
"""Combine the SETP lines of all cal305.py shards (read from log files) into the calibration verdict.
Required (pre305 docstring): share of label sets with p <= 0.05 in [0.01, 0.10] for every floor, <= 0.10 for p_max.
Usage: python combine305.py LOG [LOG ...]"""
import sys

import numpy as np

P = {}
for f in sys.argv[1:]:
    for ln in open(f, encoding="utf-8", errors="replace"):
        if "SETP " not in ln:
            continue
        t = ln[ln.index("SETP "):].split()
        P[(int(t[1]), t[2])] = {kv.split("=")[0]: float(kv.split("=")[1]) for kv in t[3:]}
ok = True
for g in sorted({k[1] for k in P}):
    rows = [v for k, v in P.items() if k[1] == g]
    line = []
    for fl in ("T", "L", "Y", "A", "max"):
        p = np.array([max(r.values()) if fl == "max" else r[fl] for r in rows])
        sh = np.mean(p <= 0.05)
        ok &= sh <= 0.10 and (fl == "max" or sh >= 0.01)
        line.append("%s %.3f" % (fl, sh))
    print("CALIBRATION %s sets %d share p<=0.05: %s" % (g, len(rows), "  ".join(line)))
print("CALIBRATION", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
