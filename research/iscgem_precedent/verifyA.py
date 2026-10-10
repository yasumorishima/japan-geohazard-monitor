"""Independent recomputation of Round A's D_real (and two floor worlds) without importing roundA.py or pre238.auc_pair.

SUDDEN AXIS: global ACTIVE; FULL 0.62086; inside group 0 0.57885.
DIMENSION CEILING: WHICH CELL, static part; class-0-only bound 0.73285.
WHY THIS ROUND: the printed REAL is not taken until a second implementation reproduces it.
"""
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

cat = pd.read_csv("/home/yasu/claude-scratch/roundA/iscgem_pre1970_m55.csv", usecols=["time", "latitude", "longitude", "mag"])
cat = cat[(cat.time.str[:4].astype(int).between(1918, 1969)) & (cat.mag >= 6.25)]
assert len(cat) == 3145
m = np.load("/home/yasu/geo-ml/rung197/meta197.npz")
xyz = lambda la, lo: np.stack([np.cos(np.radians(la)) * np.cos(np.radians(lo)), np.cos(np.radians(la)) * np.sin(np.radians(lo)), np.sin(np.radians(la))], 1)  # noqa: E731
C, E = xyz(m["clat"], m["clon"]), xyz(cat.latitude.values, cat.longitude.values)
ang = np.degrees(np.arccos(np.clip(C @ E.T, -1, 1)))
P = np.log1p(np.exp(-0.5 * ang ** 2).sum(1))
ref = np.load("/home/yasu/claude-scratch/roundA/outA.npz")
print("max |P - roundA P| %.2e" % np.abs(P - ref["P"]).max())


def wauc(pw, nw, s):
    """weighted Mann-Whitney with midranks over tied scores."""
    df = pd.DataFrame(dict(s=s, p=pw, n=nw)).groupby("s", sort=True).sum()
    cn = np.concatenate([[0.0], np.cumsum(df.n.values)[:-1]])
    return float(((cn + 0.5 * df.n.values) * df.p.values).sum() / (df.p.sum() * df.n.sum()))


B = np.load("/home/yasu/claude-scratch/deep_sudden/sudden_bundle_v1.npz")
jj = list(B["jj"])
tc, hi = dict(zip(jj, B["tcut"])), dict(zip(jj, B["hiT"]))
g = {j: B["grp_%d" % j] == 0 for j in jj}
X = {j: B["feat_%d" % j][g[j]] for j in jj}
y = {j: B["lab_%d" % j][g[j]] for j in jj}
c = {j: B["cid_%d" % j][g[j]] for j in jj}


def D_of(col):
    out = []
    for j in [k for k in jj if k >= 11]:
        tr = [k for k in jj if k < j and hi[k] <= tc[j]]
        a = []
        for use in (False, True):
            Xt = np.vstack([np.column_stack([X[k], col[c[k]]]) if use else X[k] for k in tr])
            yt = np.concatenate([y[k] for k in tr])
            Xs = np.column_stack([X[j], col[c[j]]]) if use else X[j]
            s = StandardScaler().fit(Xt)
            pr = LogisticRegression(C=1.0, max_iter=2000).fit(s.transform(Xt), yt).predict_proba(s.transform(Xs))[:, 1]
            a.append(wauc(B["pw_%d" % j][g[j]], B["nw_%d" % j][g[j]], pr))
        out.append(a[1] - a[0])
    return float(np.mean(out))


Dr = D_of(P)
print("independent D_real %+.6f  (roundA %+.6f)" % (Dr, float(ref["D"])))
