# -*- coding: utf-8 -*-
"""Score the frozen feature-only network (FREEZE_notok.md) on windows it has never seen.

SUDDEN AXIS: FULL 0.62086 over the 25 seen windows; the frozen network gave NOTOK-FULL +0.00636 there (exploratory).
DIMENSION CEILING: WHICH CELL (per-window cell oracle 0.95017; a per-cell map 0.8112).
WHY THIS ROUND: the gain was found on windows every earlier round had seen.  The frozen contract says it is real only
  if it holds on windows outside sudden_bundle_v1; this file scores such windows with train.py's own functions,
  imported unchanged, and the settings frozen in FREEZE_notok.md.  Nothing is chosen here.

Reading (from the contract): each RNG stream (--seed-offset 0..7) averages its three seeds' predictions, the window
AUC is taken per stream, and the network's value is the mean of the eight stream AUCs (as in the seen-window record).
No verdict before five new windows; each window is reported as it lands.

GATES: F0 the imported train.py is byte-identical to f824698 (md5); R0 FULL over the 25 seen windows from this bundle
  reproduces 0.620864 (1e-4); N1 FULL on each new window reproduces the bundle's ref_full_new to 1e-4 -- the same
  cross-machine tolerance as R0, fixed before the first run (the reference was fitted on the RPi5, aarch64, and this
  runs on x86); N2 the token encoder is never called (train.py asserts it).
Commits: train.py last changed at f824698; the contract FREEZE_notok.md was committed at ac12c44.
"""
import argparse
import hashlib
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
import train as T  # noqa: E402

FROZEN_TRAIN_MD5 = "63eec5d857819a852e93fd739edd9eb7"  # research/deep_sudden/train.py at f824698


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle", required=True)
    ap.add_argument("--seed-offset", type=int, required=True)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    T.torch.set_num_threads(a.threads)
    t0 = time.time()

    def log(s):
        print("[%6.0fs] %s" % (time.time() - t0, s), flush=True)

    h = hashlib.md5(open(T.__file__, "rb").read()).hexdigest()
    log("train.py md5 %s" % h)
    assert h == FROZEN_TRAIN_MD5, "F0 train.py is not the frozen one (f824698)"
    B, jj, ev, W = T.load(a.bundle)
    new = [int(j) for j in B["new"]]
    full = T.gate_full(jj, ev, W)
    log("R0 FULL 25 seen windows %.6f" % full.mean())
    assert abs(full.mean() - 0.620864) < 1e-4, "R0"
    fnew = {}
    for i, j in enumerate(new):
        fnew[j] = T.full_scores(jj, j, W)
        v = T.auc_pair(W[j]["pw"], W[j]["nw"], fnew[j])
        log("N1 FULL window %d %.7f (bundle %.7f)" % (j, v, float(B["ref_full_new"][i])))
        assert abs(v - float(B["ref_full_new"][i])) < 1e-4, "N1"
    # frozen settings (FREEZE_notok.md, ac12c44): --no-tokens --ntok 128 --epochs 8 --seeds 3, defaults otherwise
    args = argparse.Namespace(ntok=128, rad=600.0, d=64, layers=2, epochs=8, batch=256, lr=1e-3, seeds=3,
                              shuffle_tokens=-1, no_tokens=True, seed_offset=a.seed_offset)
    TK = T.Tokens(B, args.ntok, args.rad)
    deep, dsc = T.train_eval(TK, jj, new, W, args, log)
    res = dict(seed_offset=a.seed_offset, windows=new, notok=deep.tolist(),
               full=[T.auc_pair(W[j]["pw"], W[j]["nw"], fnew[j]) for j in new],
               grp_notok={str(j): T.by_group(W[j], dsc[j]) for j in new},
               grp_full={str(j): T.by_group(W[j], fnew[j]) for j in new})
    json.dump(res, open(a.out, "w"), indent=1)
    log("stream %d: %s" % (a.seed_offset, ", ".join("window %d NOTOK %.5f FULL %.5f" % (j, x, y)
                                                     for j, x, y in zip(new, res["notok"], res["full"]))))


if __name__ == "__main__":
    main()
