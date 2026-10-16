# -*- coding: utf-8 -*-
"""BUILD 305 (label bundle for round 305; no AIRS field is read here)

derived from SIZING 304 -- where in time is the per-block information, on the 33-day all-cell arena?

SUDDEN AXIS: isolated M>=6 label.  Annual FULL on all 2,280 cells (33-day issue scoring, size272) R0 0.772669;
  the 33-day refit R2 0.771474; ACTIVE FULL 0.62086 (not comparable: future-selected population).  Goal 0.90.
DIMENSION CEILING: per-(cell, 33-day block) oracle on all cells 0.99961 (size272); per-window cell oracle 0.98718.
WHY THIS ROUND: sizing 303 split the annual self-knowledge lift (+0.32 SELF) and found the previous years add
  nothing over FULL (P1 +0.0024 t 1.14 with 90 % of j-1 before the cut, P2 -0.00006).  The year is a long lead:
  short-term clustering of isolated mainshocks (weeks to months) is averaged away inside one window.  On the
  33-day arena the same decomposition says whether the information about "which cell this block" sits in the
  recent past of the catalogue (then a short-lead catalogue lever exists that R2/R3 do not use) or only in the
  block itself (then it must come from a new observation system, and the lead that system must have is set by
  where the lift starts: 33 days, 66 days, a year).  ANY control is printed next to ISO (ctl264 rule: a
  definitional column -- the GK shadow -- shows ISO-only effects; a negative ISO with positive ANY is the shadow).

Columns (indicator that the cell has a positive row of the given label in a row range; oracles, sizing only):
  SELF rows of the block itself            F1 the 11 rows after the block (future)
  P1   rows a-11..a-1 (label period partly after the issue; the fraction is printed)
  P2   rows a-22..a-12 (label complete before the issue -- asserted)
  PY   rows a-121..a-1 (the last year, partly after the issue)      PY2 rows a-132..a-12 (the last year, causal)
Each column is added to R2's 30 columns and the logistic refit on the same strict training blocks (columns built
  the same way for the training blocks); gain is read against R2 on the same 25 windows.
PREDICTION (frozen before the run): SELF lifts ISO by > +0.15; F1 under ISO in (-0.005, +0.005); P1 under ISO in
  (-0.005, +0.005) and ANY above ISO; P2 and PY2 under ISO in (-0.003, +0.003).
GATES: size272's G1 G2 G3 G4 G5 unchanged (columns, masses, chk238 reproduction 0.62086354, oracle, causality);
  G7 P2/PY2 label periods end strictly before the issue day (from the arrays); G8 SELF must lift ISO by > 0.05
  (the refit path reads the added column); G9 R2 here reproduces size272's ISO 0.771474 and ANY 0.801455 to 1e-6.
NOT CLAIMED: sizing on seen windows, no floor; nothing here is a result.
"""
import sys
import time

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, "/home/yasu/geo-ml")
import size196 as SZ
import size208 as S8
import size219 as S9
import pre238 as P8
import chk238 as C8
from pre238c import auc_fast as auc

W = "/home/yasu/geo-ml/"
EPS = 1e-9
H = 34.0
B = 11
START = 11
SEED = 2710
NPERM = 20


def main():
    t0 = time.time()
    rng = np.random.default_rng(SEED)
    D = C8.build()
    jj, ev = D["jj"], [j for j in D["jj"] if j >= START]
    ymat, tdays, clat, clon, MT, NDEF, wins, T, NC, NW = S8.load_arena()
    MD = np.load(W + "rung197/meta197.npz")
    dtd, dcell, dmag = MD["dtd"], MD["dcell"], MD["dmag"]
    kiso, spn = MD["keep_iso"], MD["span"].astype(np.int64)
    LANY = P8.lagfield(spn, dcell, dtd, tdays, np.ones(len(dmag), bool), T, NC)
    L6 = P8.lagfield(spn, dcell, dtd, tdays, kiso & (dmag >= S8.THRS[S8.PRIM]), T, NC)
    Y = (LANY <= H)
    Y[NDEF:] = False
    Mt = (L6 <= H)
    LANY = L6 = None
    A6 = SZ.paint(spn, dcell, T, NC, dmag >= 6.0)
    nbs = np.minimum(np.searchsorted(tdays, tdays - H, side="right"), NDEF)
    cumY = np.vstack([np.zeros((1, NC)), np.cumsum(Y[:NDEF], axis=0, dtype=np.float64)])
    BACK = np.zeros((T, NC), np.float32)
    nzr = nbs > 0
    BACK[nzr] = (cumY[nbs[nzr]] / nbs[nzr][:, None]).astype(np.float32)
    cumY = None
    ps, pc = np.nonzero(Y[:NDEF])
    f60 = Mt[ps, pc].astype(np.float64)
    order = np.argsort(dtd, kind="stable")
    dts, dcs, dms, dis = dtd[order], dcell[order], dmag[order], kiso[order]
    o31 = np.load(W + "out231.npz")
    SEL31, HORS31 = o31["SEL"], o31["HORS"]
    q31 = int(np.flatnonzero(HORS31 == H)[0])
    dist, _, _ = S8.geometry(clat, clon, NC)
    KS = [np.exp(-0.5 * (dist / s) ** 2) for s in P8.SIG]
    gm = KS[1].sum(axis=1)
    NF = len(D["names"])
    used_max = {}

    def cols(j, tev, trl, rb, short=False):
        """chk238's 30 columns with events cut at tev, labels complete before row trl, BACK row rb."""
        m = ps < trl
        n0 = np.bincount(pc[m], minlength=NC).astype(np.float64)
        k60 = np.bincount(pc[m], weights=f60[m], minlength=NC)
        gi, si, ai = SEL31[q31, j]
        lmp = np.log(S9.smooth(k60, n0, KL[j], S8.SHRINK[si]))
        kc = int(np.searchsorted(dts, tev, side="left"))
        used_max[(j, tev)] = (float(dts[kc - 1]) if kc > 0 else -1.0,
                              float(tdays[trl - 1]) + H if trl > 0 else -1.0)
        cc, dd, mm, ii = dcs[:kc], dts[:kc], dms[:kc], dis[:kc]
        F = np.zeros((NC, NF + (4 if short else 0)), np.float64)
        col = 0
        for t in P8.THR:
            sel = mm >= t
            F[:, col] = np.log1p(np.bincount(cc[sel], minlength=NC))
            F[:, col + 1] = np.log1p(np.bincount(cc[sel & ii], minlength=NC))
            last = np.full(NC, -1e6)
            if sel.any():
                np.maximum.at(last, cc[sel], dd[sel])
            F[:, col + 2] = np.log1p(np.clip(tev - last, 0.0, 1e5))
            col += 3
        for L in P8.TRAIL:
            k2 = int(np.searchsorted(dts, tev - L, side="left"))
            F[:, col] = np.log1p(np.bincount(dcs[k2:kc], minlength=NC))
            col += 1
        mx = np.zeros(NC)
        if kc > 0:
            np.maximum.at(mx, cc, mm)
        F[:, col] = mx
        c50 = np.bincount(cc[mm >= 5.0], minlength=NC).astype(np.float64)
        c55 = np.bincount(cc[mm >= 5.5], minlength=NC).astype(np.float64)
        c60i = np.bincount(cc[(mm >= 6.0) & ii], minlength=NC).astype(np.float64)
        F[:, col + 1] = np.log((c50 + 0.5) / (c55 + 0.5))
        F[:, col + 2] = (c60i + 0.5) / (c50 + 0.5)
        col += 3
        for a in range(len(P8.SIG)):
            F[:, col] = np.log1p(KS[a] @ c50)
            F[:, col + 1] = np.log1p(KS[a] @ c60i)
            col += 2
        F[:, col] = np.log1p(gm)
        F[:, col + 1] = np.abs(clat)
        F[:, col + 2] = np.log1p(n0)
        F[:, col + 3] = np.log1p(k60)
        F[:, col + 4] = lmp
        F[:, col + 5] = np.log(np.clip(BACK[rb], EPS, 1 - EPS))
        col += 6
        if short:
            for L in (34.0, 102.0):
                k2 = int(np.searchsorted(dts, tev - L, side="left"))
                c = np.bincount(dcs[k2:kc], minlength=NC).astype(np.float64)
                F[:, col] = np.log1p(c)
                F[:, col + 1] = np.log1p(KS[1] @ c)
                col += 2
        return F

    KL = {}
    for j in jj:
        gi = SEL31[q31, j][0]
        KL[j] = np.exp(-0.5 * (dist / float(S8.SIGMAS[gi])) ** 2)

    # ---- blocks, masses, columns per issue
    EL, blk, PI, PA, NN, FX, FS = {}, {}, {}, {}, {}, {}, {}
    ACT, FW = {}, {}
    for j in jj:
        tr, lo, hi = wins[j]
        yw = Y[lo:hi]
        act = np.flatnonzero(yw.any(axis=0))
        assert len(act) == len(D["pcs"][j]), "cells %d" % j
        ACT[j] = act
        el = np.arange(NC)
        EL[j] = el
        F0 = cols(j, float(tdays[tr]), tr, lo)
        FW[j] = F0
        dv = np.max(np.abs(F0[act] - D["feats"][j]))
        assert dv == 0.0, "G1 window %d dev %.3e" % (j, dv)
        starts = list(range(lo, hi, B))
        blk[j] = [(a, min(a + B, hi)) for a in starts]
        PI[j], PA[j], NN[j], FX[j], FS[j] = [], [], [], [], []
        for a, b in blk[j]:
            y = Y[a:b][:, el]
            mi = Mt[a:b][:, el]
            a6 = A6[a:b][:, el]
            PI[j].append((y & mi).sum(0).astype(np.float64))
            PA[j].append((y & a6).sum(0).astype(np.float64))
            NN[j].append((~y).sum(0).astype(np.float64))
            trl = int(np.searchsorted(tdays, tdays[a] - H, side="left"))
            F = cols(j, float(tdays[a]), trl, a, short=True)[el]
            FX[j].append(F[:, :NF])
            FS[j].append(F)
        dp = np.max(np.abs(np.sum(PI[j], axis=0)[act] - D["pcs"][j]))
        dn = np.max(np.abs(np.sum(NN[j], axis=0)[act] - D["ncs"][j]))
        assert dp == 0 and dn == 0, "G2 window %d" % j
    print("G1 G2 PASS  issues %d  (%.0f s)" % (sum(len(blk[j]) for j in jj), time.time() - t0),
          flush=True)

    strict = {j: [k for k in jj if k < j and D["hiT"][k] <= D["tcuts"][j]] for j in ev}
    # G5 causality from the arrays
    for j in jj:
        for a, b in blk[j]:
            last_ev, _ = used_max[(j, float(tdays[a]))]
            assert last_ev < tdays[a], "G5 event at issue %d" % a
    for j in ev:
        for k in strict[j]:
            lastrow = blk[k][-1][1] - 1
            assert tdays[lastrow] + H <= D["tcuts"][j], \
                "G5 training labels of window %d incomplete at cut of %d" % (k, j)
    print("G5 PASS causality from arrays", flush=True)

    def pool(j, P, s_list):
        return auc(np.concatenate(P[j]), np.concatenate(NN[j]), np.concatenate(s_list))

    def fit(Xtr, ytr):
        sc = StandardScaler().fit(Xtr)
        md = LogisticRegression(max_iter=2000, C=1.0).fit(sc.transform(Xtr), ytr)
        return lambda X: md.predict_proba(sc.transform(X))[:, 1]

    ref = np.load(W + "out272.npz")
    OUTJ = [j for j in ev if j >= 17]
    # ANY rows that are not ISO rows (opus plan audit: ANY control must not re-count ISO pairs)
    PX = {}
    for j in jj:
        PX[j] = [((Y[a:b] & A6[a:b]) & ~Mt[a:b]).sum(0).astype(np.float64) for a, b in blk[j]]
    base = {}
    for j in ev:
        ks = strict[j]
        yb = np.concatenate([(p > 0).astype(np.int8) for k in ks for p in PI[k]])
        Xtr = np.vstack([X for k in ks for X in FX[k]])
        f2 = fit(Xtr, yb)
        base[j] = [f2(X) for X in FX[j]]
    vb = np.array([pool(j, PI, base[j]) for j in ev])
    dg9 = np.max(np.abs(vb - ref["ISO_R2"]))
    assert dg9 <= 1e-6, "G9 R2 dev %.3e" % dg9
    print("G9 PASS ISO R2 %.6f (size272 %.6f, max window dev %.1e)" % (vb.mean(), ref["ISO_R2"].mean(), dg9),
          flush=True)
    rows = []
    for j in OUTJ:
        for q, (a, b) in enumerate(blk[j]):
            rows.append((j, q, a, float(tdays[a])))
    nb = len(rows)
    P_ = np.zeros((nb, NC), np.int16)
    X_ = np.zeros((nb, NC), np.int16)
    N_ = np.zeros((nb, NC), np.int16)
    S_ = np.zeros((nb, NC), np.float64)
    i = 0
    for j in OUTJ:
        for q in range(len(blk[j])):
            P_[i], X_[i], N_[i], S_[i] = PI[j][q], PX[j][q], NN[j][q], base[j][q]
            i += 1
    # self-check: the bundle reproduces R2 per window through a plain pooled AUC
    rr = np.array(rows)
    for j in OUTJ:
        m = rr[:, 0] == j
        v = auc(P_[m].ravel().astype(np.float64), N_[m].ravel().astype(np.float64), S_[m].ravel())
        assert abs(v - vb[ev.index(j)]) <= 1e-12, "bundle window %d" % j
    print("bundle reproduces R2 on windows %d..%d" % (OUTJ[0], OUTJ[-1]), flush=True)
    np.savez_compressed(sys.argv[1], win=rr[:, 0].astype(np.int16), blk=rr[:, 1].astype(np.int16),
                        row=rr[:, 2].astype(np.int32), tiss=rr[:, 3], pos_iso=P_, pos_anyx=X_, neg=N_,
                        r2_iso=S_, clat=clat, clon=clon)
    print("DONE %.0f s  blocks %d" % (time.time() - t0, nb), flush=True)


if __name__ == "__main__":
    main()
