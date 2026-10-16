# -*- coding: utf-8 -*-
"""Round 305 calibration and power (label-free: every label here is synthetic and drawn without the field).

SUDDEN AXIS / DIMENSION / WHY: see pre305.py.  Run before the freeze; combine305.py's output is part of the freeze.
SYNTHETIC LABELS (opus code audit: labels independent per unit cannot test what matters -- the arena paints a row
  positive when an event falls in the 34 days after it, so one event marks two consecutive blocks of one cell):
  events, at most one per unit, with probability c0 x R2 score x burst (R2 is fitted on past windows only; burst =
  lognormal, mean 1, one value per 30 x 30-degree region and window, sd 0.7 in log, times one per region and
  block, sd 0.5: regional bursts of activity at the year and the month scale),
  event time uniform inside the unit's block span; a row (block start + 3 days x i) is positive if an event of its
  cell falls in [row time, row time + 34 days); positive rows of a unit = its marked rows, negatives = the other
  rows of the block (block lengths in rows from rec305.npz, label-free).  c0 is set so that the expected number of
  positive units equals the real one (the only label quantity used: one count).  Not modelled (disclosed): the
  arena also drops negative rows with any M>=5 in the 34 days after them.
CALIBRATION: NSET sets; for each, p of TA_co5, TA_ch46 and of their t/q residuals under each floor (NWORLD worlds,
  shared-unit comparison exactly as in pre305) and p_max.  Required (combine305.py): share with p <= 0.05 in
  [0.01, 0.10] for floors T, L, Y and <= 0.10 for p_max; floor A is printed only (anti-conservative alone).
POWER (shard 0): synthetic sets with the event probability times exp(beta z) (z = TA_co5 standardised over covered
  units; uncovered units untilted), beta 0.10 and 0.20; share of sets whose SA exceeds the 95 % point of that set's
  floor-A null (NWORLD worlds) -- an upper bound for the max-p verdict.
Usage: python cal305.py BUNDLE Q305 REC305 NSET NWORLD SHARD NSHARD   (sets s with s % NSHARD == SHARD; each set's
  p values are printed as "SETP" lines so the shards are combined by combine305.py; power runs in shard 0 only)
"""
import sys
import time

import numpy as np

import pre305 as R

H = 34.0
STEP = 3.0
BURST_SD = 0.7
BLOCK_SD = 0.5


class Synth:
    def __init__(self, A, nrow):
        self.A = A
        self.nrow = nrow.astype(np.int64)
        assert np.all(self.nrow >= 1) and len(self.nrow) == A.NB
        st = np.r_[0, np.cumsum(self.nrow)]
        self.bstart = st[:-1]
        self.times = np.concatenate([A.tiss[b] + STEP * np.arange(self.nrow[b]) for b in range(A.NB)])
        assert np.all(np.diff(self.times) > 0), "row times must increase"
        reg = (A.r // 30) * 12 + A.c // 30
        self.reg = reg
        self.wi = np.searchsorted(A.wins, A.win)
        self.npos = float((A.P > 0).sum())
        self.c0 = 1.0
        rng = np.random.default_rng(1)
        for _ in range(3):                       # match the expected number of positive units
            P, _ = self.draw(rng)
            self.c0 *= self.npos / max(1.0, float((P > 0).sum()))
        print("synthetic c0 %.4f  positive units real %d" % (self.c0, self.npos), flush=True)

    def draw(self, rng, tilt=None):
        A = self.A
        z = rng.standard_normal((len(A.wins), 72))
        zb = rng.standard_normal((A.NB, 72))
        burst = (np.exp(BURST_SD * z - 0.5 * BURST_SD ** 2)[self.wi]
                 * np.exp(BLOCK_SD * zb - 0.5 * BLOCK_SD ** 2))[:, self.reg]
        pr = self.c0 * A.S * burst
        if tilt is not None:
            pr = pr * np.exp(np.nan_to_num(tilt))
        pr = np.clip(pr, 0.0, 0.9)
        eb, ec = np.nonzero(rng.random(pr.shape) < pr)
        te = A.tiss[eb] + rng.random(len(eb)) * STEP * self.nrow[eb]
        lo = np.searchsorted(self.times, te - H, side="left")
        hi = np.searchsorted(self.times, te, side="left")
        D = np.zeros((len(self.times) + 1, A.NC), np.int32)
        np.add.at(D, (lo, ec), 1)
        np.add.at(D, (hi, ec), -1)
        M = (np.cumsum(D[:-1], axis=0) > 0).astype(np.float64)
        P = np.add.reduceat(M, self.bstart, axis=0)
        return P, self.nrow[:, None] - P


def main():
    bundle, q305, rec = sys.argv[1], sys.argv[2], sys.argv[3]
    nset, nworld, shard, nshard = (int(x) for x in sys.argv[4:8])
    t0 = time.time()
    A = R.Arena(bundle, q305)
    SY = Synth(A, np.load(rec)["nrow"])
    tests = {}
    for g, t, q in R.GASES:
        tests[g] = R.gas_makers(A, g)
        tests[g + "_res"] = R.gas_makers(A, g, rivals=(t, q))
    for s in range(nset):
        if s % nshard != shard:
            continue
        P, N = SY.draw(np.random.default_rng(30500 + s))
        for gi, (g, (real_u, make)) in enumerate(tests.items()):
            fl = R.floor_p(A, make, P, N, nworld, 50000 + 100000 * s + 10 * gi, real_u)   # floor f adds 1000 f
            print("SETP %d %s %s" % (s, g, " ".join("%s=%.6f" % (k, fl[k][0]) for k in R.FLOORS)), flush=True)
        print("set %d  positive units %d rows %d  %.0f s" % (s, (P > 0).sum(), P.sum(), time.time() - t0),
              flush=True)
    if shard != 0:
        print("DONE %.0f s" % (time.time() - t0), flush=True)
        return
    rng = np.random.default_rng(990001)          # distinct from every set seed 30500 + s
    real_u, _ = tests["co5"]
    cov = np.isfinite(real_u)
    z = (real_u - np.nanmean(real_u)) / np.nanstd(real_u)
    for beta in (0.10, 0.20):
        hit = []
        for s in range(max(1, nset // 2)):
            P, N = SY.draw(rng, tilt=beta * z)
            v = R.sa(real_u, P, N, A.strat, cov)
            nl = [R.sa(R.permute_within(real_u, A.strat, rng), P, N, A.strat, cov) for _ in range(nworld)]
            hit.append(v > np.quantile(nl, 0.95))
        print("POWER beta %.2f per sd: share above floor-A q95 %.3f" % (beta, np.mean(hit)), flush=True)
    print("DONE %.0f s" % (time.time() - t0), flush=True)


if __name__ == "__main__":
    main()
