# -*- coding: utf-8 -*-
"""Deep model on the global sudden-earthquake (ACTIVE) axis -- exploratory, NO verdict.

1. SUDDEN NUMBER THIS MOVES: the 25-window ACTIVE mean of the per-cell predictor, FULL 0.62086 (logistic regression
   on 27 catalogue features per cell); the arm's per-cell map is 0.59033 on the same windows (row-level arm over 33
   windows 0.5785).
2. DIMENSION AND ITS BOUND: WHERE -- the order of cells inside each window.  The family that may reorder cells per
   window is bounded by 0.9482 (per-window cell oracle 0.95017); a map fixed per cell is bounded by 0.8112.
3. WHY THIS ROUND: the measured loss sits in cells with no or one precedent (33.6% of held-out positive mass in cells
   that never fired in training, AUC 0.2965 there).  A per-cell map must rank those cells last by construction;
   a model that reads each cell's neighbourhood event history and shares its weights across all cells can learn
   from the neighbours what the cell's own record cannot say.  No per-cell model has been trained on this axis.

Data: derived/deep_sudden/sudden_bundle_v1.npz on the public HF dataset yasumorishima/japan-geohazard (USGS FDSN
catalogue, public domain, and quantities derived from it), exported by chk238.build() on the RPi5 with the gates
arm 0.59033 / FULL 0.620864 reproduced.

Protocol (the same as FULL): for each evaluation window j (11..35) the model is trained only on windows k < j whose
last scored day is <= the training cut of j, and scored by the weighted pair AUC of chk238/pre238 (positive and
negative row counts per cell as weights).  Inputs of window k use only events strictly before its training cut.
The 25 windows have all been seen by earlier rounds, so this is exploration: a promising result is re-run under a
frozen contract on unseen windows before anything is claimed.

GATE R0: refitting FULL from the bundle reproduces 0.620864 to 1e-4 (else stop: the bundle is not the frozen run's).
"""
import argparse
import hashlib
import json
import os
import time

import numpy as np
import torch
import torch.nn as nn
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

DEG_KM = 111.195


def auc_pair(p, n, s):
    P, N = p.sum(), n.sum()
    if P <= 0 or N <= 0:
        return float("nan")
    o = np.argsort(s, kind="stable")
    ps, ns, ss = p[o], n[o], np.asarray(s)[o]
    cn = np.concatenate([[0.0], np.cumsum(ns)])
    a = tie = 0.0
    i, m = 0, len(ss)
    while i < m:
        k = i
        while k + 1 < m and ss[k + 1] == ss[i]:
            k += 1
        a += ps[i:k + 1].sum() * cn[i]
        tie += ps[i:k + 1].sum() * ns[i:k + 1].sum()
        i = k + 1
    return float((a + 0.5 * tie) / (P * N))


def load(path):
    Z = np.load(path)
    B = {k: Z[k] for k in Z.files}
    jj = [int(j) for j in B["jj"]]
    ev = [int(j) for j in B["ev"]]
    W = {}
    for i, j in enumerate(jj):
        W[j] = dict(cid=B["cid_%d" % j], lab=B["lab_%d" % j], pw=B["pw_%d" % j].astype(np.float64),
                    nw=B["nw_%d" % j].astype(np.float64), arm=B["arm_%d" % j], feat=B["feat_%d" % j],
                    tcut=float(B["tcut"][i]), hiT=float(B["hiT"][i]))
    return B, jj, ev, W


def strict_past(jj, W, j):
    return [k for k in jj if k < j and W[k]["hiT"] <= W[j]["tcut"]]


def gate_full(jj, ev, W):
    v = []
    for j in ev:
        ks = strict_past(jj, W, j)
        X = np.vstack([W[k]["feat"] for k in ks])
        y = np.concatenate([W[k]["lab"] for k in ks])
        sc = StandardScaler().fit(X)
        m = LogisticRegression(max_iter=2000, C=1.0).fit(sc.transform(X), y)
        v.append(auc_pair(W[j]["pw"], W[j]["nw"], m.predict_proba(sc.transform(W[j]["feat"]))[:, 1]))
    return np.array(v)


class Tokens:
    """Neighbourhood event tokens of a cell at a cut: the NTOK most recent events (strictly before the cut) within
    RAD km of the cell centre.  Token = (log days before cut, magnitude - 5, isolated flag, east km/RAD, north km/RAD,
    distance/RAD).  Padding is masked."""

    def __init__(self, B, ntok, rad):
        self.clat, self.clon = B["clat"].astype(np.float64), B["clon"].astype(np.float64)
        o = np.argsort(B["ev_t"], kind="stable")
        self.t, self.c = B["ev_t"][o].astype(np.float64), B["ev_cell"][o].astype(np.int64)
        self.m, self.iso = B["ev_mag"][o].astype(np.float64), B["ev_iso"][o].astype(np.float64)
        self.ntok, self.rad = ntok, rad
        la, lo = np.radians(self.clat), np.radians(self.clon)
        # great-circle distance between cell centres (km) and local east/north offsets
        dla = la[:, None] - la[None, :]
        dlo = lo[None, :] - lo[:, None]
        h = np.sin(dla / 2) ** 2 + np.cos(la[:, None]) * np.cos(la[None, :]) * np.sin(dlo / 2) ** 2
        self.dist = 2 * 6371.0 * np.arcsin(np.sqrt(np.clip(h, 0, 1)))
        dlo_w = (self.clon[None, :] - self.clon[:, None] + 180.0) % 360.0 - 180.0
        self.east = dlo_w * DEG_KM * np.cos(la)[:, None]
        self.north = (self.clat[None, :] - self.clat[:, None]) * DEG_KM
        self.nbr = [np.flatnonzero(self.dist[c] <= rad) for c in range(len(self.clat))]

    def build(self, cells, tcut):
        kc = int(np.searchsorted(self.t, tcut, side="left"))  # events strictly before the cut
        t, c, m, iso = self.t[:kc], self.c[:kc], self.m[:kc], self.iso[:kc]
        X = np.zeros((len(cells), self.ntok, 6), np.float32)
        M = np.zeros((len(cells), self.ntok), bool)
        order = np.argsort(c, kind="stable")
        cs = c[order]
        starts = np.searchsorted(cs, np.arange(len(self.clat)), side="left")
        ends = np.searchsorted(cs, np.arange(len(self.clat)), side="right")
        for r, cell in enumerate(cells):
            nb = self.nbr[cell]
            parts = [order[starts[q]:ends[q]] for q in nb if ends[q] > starts[q]]
            if not parts:
                continue
            ii = np.concatenate(parts)
            ii = ii[np.argsort(t[ii])[::-1][: self.ntok]]
            q = c[ii]
            n = len(ii)
            X[r, :n, 0] = np.log1p(tcut - t[ii])
            X[r, :n, 1] = m[ii] - 5.0
            X[r, :n, 2] = iso[ii]
            X[r, :n, 3] = self.east[cell, q] / self.rad
            X[r, :n, 4] = self.north[cell, q] / self.rad
            X[r, :n, 5] = self.dist[cell, q] / self.rad
            M[r, :n] = True
        return X, M


class Net(nn.Module):
    def __init__(self, nfeat, d=64, heads=4, layers=2):
        super().__init__()
        self.tok = nn.Sequential(nn.Linear(6, d), nn.GELU(), nn.Linear(d, d))
        enc = nn.TransformerEncoderLayer(d, heads, 2 * d, dropout=0.1, batch_first=True, norm_first=True)
        self.enc = nn.TransformerEncoder(enc, layers)
        self.feat = nn.Sequential(nn.Linear(nfeat, d), nn.GELU())
        self.head = nn.Sequential(nn.Linear(2 * d + 1, d), nn.GELU(), nn.Dropout(0.1), nn.Linear(d, 1))

    def forward(self, x, mask, f):
        h = self.enc(self.tok(x), src_key_padding_mask=~mask)
        w = mask.float().unsqueeze(-1)
        pooled = (h * w).sum(1) / w.sum(1).clamp(min=1.0)
        empty = (mask.sum(1, keepdim=True) == 0).float()
        return self.head(torch.cat([pooled, self.feat(f), empty], 1)).squeeze(1)


def train_eval(TK, jj, ev, W, args, log):
    cache = {}

    def data(k):
        if k not in cache:
            X, M = TK.build(W[k]["cid"], W[k]["tcut"])
            cache[k] = (X, M)
        return cache[k]

    res = []
    for j in ev:
        ks = strict_past(jj, W, j)
        Xs = np.concatenate([data(k)[0] for k in ks])
        Ms = np.concatenate([data(k)[1] for k in ks])
        F = np.vstack([W[k]["feat"] for k in ks])
        y = np.concatenate([W[k]["lab"] for k in ks]).astype(np.float32)
        mu, sd = F.mean(0), F.std(0) + 1e-9
        preds = []
        for seed in range(args.seeds):
            torch.manual_seed(1000 * j + seed)
            net = Net(F.shape[1], d=args.d, layers=args.layers)
            opt = torch.optim.AdamW(net.parameters(), lr=args.lr, weight_decay=1e-2)
            pos = max(y.mean(), 1e-3)
            lossf = nn.BCEWithLogitsLoss(pos_weight=torch.tensor((1 - pos) / pos))
            xt, mt = torch.from_numpy(Xs), torch.from_numpy(Ms)
            ft, yt = torch.from_numpy(((F - mu) / sd).astype(np.float32)), torch.from_numpy(y)
            n = len(yt)
            net.train()
            for ep in range(args.epochs):
                perm = torch.randperm(n)
                for b in range(0, n, args.batch):
                    ib = perm[b:b + args.batch]
                    opt.zero_grad()
                    lossf(net(xt[ib], mt[ib], ft[ib]), yt[ib]).backward()
                    opt.step()
            net.eval()
            Xj, Mj = data(j)
            with torch.no_grad():
                fj = torch.from_numpy(((W[j]["feat"] - mu) / sd).astype(np.float32))
                preds.append(net(torch.from_numpy(Xj), torch.from_numpy(Mj), fj).numpy())
        s = np.mean(preds, 0)
        a = auc_pair(W[j]["pw"], W[j]["nw"], s)
        res.append(a)
        log("window %d  train windows %d  rows %d  DEEP %.5f" % (j, len(ks), n, a))
    return np.array(res)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle", required=True)
    ap.add_argument("--ntok", type=int, default=64)
    ap.add_argument("--rad", type=float, default=600.0)
    ap.add_argument("--d", type=int, default=64)
    ap.add_argument("--layers", type=int, default=2)
    ap.add_argument("--epochs", type=int, default=6)
    ap.add_argument("--batch", type=int, default=256)
    ap.add_argument("--lr", type=float, default=1e-3)
    ap.add_argument("--seeds", type=int, default=1)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--nshard", type=int, default=1)
    ap.add_argument("--out", default="deep_sudden_result.json")
    args = ap.parse_args()
    torch.set_num_threads(args.threads)
    t0 = time.time()

    def log(s):
        print("[%6.0fs] %s" % (time.time() - t0, s), flush=True)

    md5 = hashlib.md5(open(args.bundle, "rb").read()).hexdigest()
    log("bundle md5 %s" % md5)
    assert md5 == "3a6ebb64dde9f8ca5e57863f0dcd8fd7", "bundle is not v1 as exported"
    B, jj, ev, W = load(args.bundle)
    full = gate_full(jj, ev, W)
    log("R0 FULL refit 25-window mean %.6f (bundle export 0.620864)" % full.mean())
    assert abs(full.mean() - 0.620864) < 1e-4, "R0 FAIL: bundle does not reproduce FULL"
    arm = np.array([auc_pair(W[j]["pw"], W[j]["nw"], W[j]["arm"]) for j in ev])
    log("arm per-cell map 25-window mean %.5f" % arm.mean())
    sel = [i for i in range(len(ev)) if i % args.nshard == args.shard]
    evs = [ev[i] for i in sel]
    full, arm = full[sel], arm[sel]
    log("shard %d/%d windows %s" % (args.shard, args.nshard, evs))
    TK = Tokens(B, args.ntok, args.rad)
    deep = train_eval(TK, jj, evs, W, args, log)
    ev = evs
    d = deep - full
    se = d.std(ddof=1) / np.sqrt(len(d))
    log("DEEP shard mean %.5f   FULL %.5f   DEEP-FULL %+.5f (se %.5f, windows better %d/%d)"
        % (deep.mean(), full.mean(), d.mean(), se, int((d > 0).sum()), len(d)))
    json.dump(dict(args=vars(args), windows=ev, deep=deep.tolist(), full=full.tolist(), arm=arm.tolist(),
                   deep_mean=float(deep.mean()), full_mean=float(full.mean()), arm_mean=float(arm.mean()),
                   diff_mean=float(d.mean()), diff_se=float(se)), open(args.out, "w"), indent=1)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write("| | shard ACTIVE mean |\n|---|---|\n| DEEP | %.5f |\n| FULL | %.5f |\n| arm map | %.5f |\n"
                    "\nShard windows %s: DEEP-FULL %+.5f (se %.5f), better in %d/%d. Exploratory: all 25 windows "
                    "seen before.\n"
                    % (deep.mean(), full.mean(), arm.mean(), ev, d.mean(), se, int((d > 0).sum()), len(d)))


if __name__ == "__main__":
    main()
