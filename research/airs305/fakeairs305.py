# test-only: fake yearly AIRS files 2002..2025 (values only in grid rows 30-39, 60-69, 90-99, 120-129, 140-149;
# NaN elsewhere; t5 is 15 K warmer in rows 30-39 so the orientation gate P3 passes) for prep.py.  No information about any label.
import datetime as dt
import sys

import numpy as np

out = sys.argv[1]
for y in range(2002, 2026):
    d0 = dt.date(2002, 8, 31) if y == 2002 else dt.date(y, 1, 1)
    n = (dt.date(y + 1, 1, 1) - d0).days
    days = np.array([(d0 + dt.timedelta(i)).isoformat() for i in range(n)])
    rng = np.random.default_rng(y)
    doy = np.array([(d0 + dt.timedelta(i)).timetuple().tm_yday for i in range(n)])[:, None, None]
    Z = {}
    for k in ("co5", "ch46", "t5", "t6", "q5", "q6"):
        for o in "AD":
            V = np.full((n, 180, 360), np.nan, np.float16)
            for r0 in (30, 60, 90, 120, 140):
                v = 10 * np.sin(2 * np.pi * doy / 365.25) + rng.standard_normal((n, 10, 360))
                if k == "t5" and r0 == 30:
                    v = v + 15.0
                v[rng.random(v.shape) < 0.3] = np.nan
                V[:, r0:r0 + 10] = v.astype(np.float16)
            Z["%s_%s" % (k, o)] = V
    np.savez_compressed("%s/airs305_%d.npz" % (out, y), days=days, fails=np.array([]), **Z)
    print(y, flush=True)
