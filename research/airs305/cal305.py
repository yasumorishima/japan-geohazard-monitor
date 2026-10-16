# -*- coding: utf-8 -*-
"""Round 305 calibration and power (label-free: every label here is synthetic and drawn without the field).

SUDDEN AXIS / DIMENSION / WHY: see pre305.py.  Run before the freeze; the printed shares are part of the freeze.
CALIBRATION: NSET synthetic label sets.  A unit (cell, block) is positive with probability equal to its R2 ISO
  score (R2 is fitted on past windows only); a positive unit's number of positive rows is drawn from the empirical
  distribution of positive-row counts of positive units; negatives = the unit's rows minus its positive rows.
  For each set, p of TA_co5 and TA_ch46 under each floor (NWORLD worlds, shared-unit comparison exactly as in
  pre305) and p_max.  Required: the share with p <= 0.05 in [0.01, 0.10] for every floor, <= 0.10 for p_max.
POWER: synthetic sets with logit(score) + beta * z (z = TA_co5 standardised over covered units; uncovered units
  untilted), beta 0.10 and 0.20 per sd; share of sets whose SA exceeds the 95 % point of that set's floor-A null
  (NWORLD worlds) -- an upper bound for the max-p verdict, whose other floors are printed by the calibration.
Usage: python cal305.py BUNDLE Q305 NSET NWORLD SHARD NSHARD   (sets s with s % NSHARD == SHARD; each set's
  p values are printed as "SETP" lines so the shards are combined by combine305.py; power runs in shard 0 only)
"""
import sys
import time

import numpy as np

import pre305 as R


def synth(A, rng, cnt, tilt=None):
    lo = np.log(np.clip(A.S, 1e-9, 1 - 1e-9)) - np.log1p(-np.clip(A.S, 1e-9, 1 - 1e-9))
    if tilt is not None:
        lo = lo + np.nan_to_num(tilt)
    pos = rng.random(A.S.shape) < 1.0 / (1.0 + np.exp(-lo))
    rows = A.P + A.N
    P = np.where(pos, np.minimum(rng.choice(cnt, size=A.S.shape), rows), 0.0)
    return P, rows - P


def main():
    bundle, q305 = sys.argv[1], sys.argv[2]
    nset, nworld, shard, nshard = (int(x) for x in sys.argv[3:7])
    t0 = time.time()
    A = R.Arena(bundle, q305)
    cnt = A.P[A.P > 0]
    rng = np.random.default_rng(30500)
    mk = {g: R.gas_makers(A, g) for g in ("co5", "ch46")}
    share = {g: {k: [] for k in R.FLOORS + ("max",)} for g in mk}
    for s in range(nset):
        if s % nshard != shard:
            continue
        rng = np.random.default_rng(30500 + s)
        P, N = synth(A, rng, cnt)
        for gi, (g, (real_u, make)) in enumerate(mk.items()):
            fl = R.floor_p(A, make, P, N, nworld, 50000 + 100 * s + gi, real_u)
            for k in R.FLOORS:
                share[g][k].append(fl[k][0])
            share[g]["max"].append(max(fl[k][0] for k in R.FLOORS))
            print("SETP %d %s %s" % (s, g, " ".join("%s=%.6f" % (k, fl[k][0]) for k in R.FLOORS)), flush=True)
        print("set %d  %.0f s" % (s + 1, time.time() - t0), flush=True)
    ok = True
    for g in share:
        line = []
        for k, v in share[g].items():
            r = np.mean(np.array(v) <= 0.05)
            line.append("%s %.3f" % (k, r))
            ok &= (r <= 0.10) and (k == "max" or r >= 0.01)
        print("CALIBRATION %s share p<=0.05: %s" % (g, "  ".join(line)), flush=True)
    print("CALIBRATION (this shard only; combine305.py decides) %s" % ("ok" if ok else "outside"), flush=True)
    if shard != 0:
        print("DONE %.0f s" % (time.time() - t0), flush=True)
        return
    rng = np.random.default_rng(30599)
    real_u, _ = mk["co5"]
    cov = np.isfinite(real_u)
    z = (real_u - np.nanmean(real_u)) / np.nanstd(real_u)
    for beta in (0.10, 0.20):
        hit = []
        for s in range(max(1, nset // 2)):
            P, N = synth(A, rng, cnt, tilt=beta * z)
            v = R.sa(real_u, P, N, A.strat, cov)
            nl = [R.sa(R.permute_within(real_u, A.strat, rng), P, N, A.strat, cov) for _ in range(nworld)]
            hit.append(v > np.quantile(nl, 0.95))
        print("POWER beta %.2f per sd: share above floor-A q95 %.3f" % (beta, np.mean(hit)), flush=True)
    print("DONE %.0f s" % (time.time() - t0), flush=True)


if __name__ == "__main__":
    main()
