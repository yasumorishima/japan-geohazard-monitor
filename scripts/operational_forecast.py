import time, json, os, base64, subprocess, urllib.request
import numpy as np
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score
from scipy.stats import rankdata
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

REPO = "yasumorishima/japan-geohazard-monitor"
WORK = "/home/yasu/geo-ml/forecast"
CATALOG = "/home/yasu/geo-ml/usgs_catalog.csv"
LAT0, LON0, CS, GH, GW = 26.0, 128.0, 2.0, 11, 11
NCELL = GH * GW
HOR = 34
Mc = 4.0
os.makedirs(WORK, exist_ok=True)


def log(s):
    print(s, flush=True)


def fetch_usgs():
    base = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    header = None
    rows = []
    this_year = datetime.utcnow().year
    for yr in range(2009, this_year + 2):
        url = ("%s?format=csv&starttime=%d-01-01&endtime=%d-01-01&minmagnitude=4&minlatitude=24&maxlatitude=48&minlongitude=126&maxlongitude=150&orderby=time-asc" % (base, yr, yr + 1))
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "geo-research"})
                data = urllib.request.urlopen(req, timeout=120).read().decode("utf-8")
                lines = data.strip().split(chr(10))
                if header is None:
                    header = lines[0]
                rows.extend(lines[1:])
                break
            except Exception as e:
                log("fetch yr=%d attempt=%d err=%s" % (yr, attempt, str(e)[:60]))
                time.sleep(5)
    with open(CATALOG, "w") as f:
        f.write(header + chr(10))
        f.write(chr(10).join(rows) + chr(10))
    log("fetched USGS events=%d" % len(rows))


def parse():
    et, la, lo, mg = [], [], [], []
    with open(CATALOG) as f:
        hdr = f.readline().strip().split(",")
        ix = {n: i for i, n in enumerate(hdr)}
        for line in f:
            p = line.rstrip(chr(10)).split(",")
            if len(p) < 5:
                continue
            try:
                tt = datetime.fromisoformat(p[ix["time"]].replace("Z", "+00:00"))
                et.append(tt.toordinal() + (tt.hour * 3600 + tt.minute * 60 + tt.second) / 86400.0)
                la.append(float(p[ix["latitude"]]))
                lo.append(float(p[ix["longitude"]]))
                mg.append(float(p[ix["mag"]]))
            except Exception:
                continue
    et = np.array(et)
    o = np.argsort(et)
    return et[o], np.array(la)[o], np.array(lo)[o], np.array(mg)[o]


def build(ev_t, ev_la, ev_lo, ev_m, o0, cat_end):
    clat = np.array([LAT0 + CS * (k // GW) for k in range(NCELL)])
    clon = np.array([LON0 + CS * (k % GW) for k in range(NCELL)])
    ei = np.round((ev_la - LAT0) / CS).astype(int)
    ej = np.round((ev_lo - LON0) / CS).astype(int)
    ing = (ei >= 0) & (ei < GH) & (ej >= 0) & (ej < GW)
    ecell = np.where(ing, ei * GW + ej, -1)
    ev_td = ev_t - o0
    tdays = np.arange(0, cat_end + 0.1, 3.0)
    T = len(tdays)
    m5 = ev_m >= 5.0
    m5_td = ev_td[m5 & ing]
    m5_cell = ecell[m5 & ing]
    Y = np.full((T, NCELL), np.nan)
    for t in range(T):
        if tdays[t] + HOR <= cat_end + 0.5:
            sel = (m5_td >= tdays[t]) & (m5_td < tdays[t] + HOR)
            Y[t, :] = 0.0
            cc = m5_cell[sel]
            if len(cc):
                Y[t, np.unique(cc)] = 1.0
    ev_bin = np.searchsorted(tdays, ev_td, side="right")
    v = ev_bin < T
    eb, ela, elo, em, ec = ev_bin[v], ev_la[v], ev_lo[v], ev_m[v], ecell[v]
    coslat = np.cos(np.deg2rad(clat))[:, None]
    dist = np.sqrt((clat[:, None] - ela[None, :]) ** 2 + ((clon[:, None] - elo[None, :]) * coslat) ** 2)
    feats = []
    for nm, al, sg, p, c in [("std", 1.0, 1.0, 1.1, 0.1), ("near", 0.8, 0.5, 1.1, 0.1), ("broad", 1.0, 2.0, 1.0, 0.5), ("slow", 1.0, 1.5, 0.6, 1.0), ("big", 1.5, 1.5, 1.1, 0.1), ("sharp", 1.0, 1.0, 1.3, 0.05)]:
        prod = 10.0 ** (al * (em - Mc))
        sw = np.exp(-(dist * dist) / (2 * sg * sg))
        lag = np.arange(300)
        K = 1.0 / ((3.0 * lag) + c) ** p
        lam = np.zeros((T, NCELL))
        for ci in range(NCELL):
            src = np.bincount(eb, weights=prod * sw[ci], minlength=T)
            lam[:, ci] = np.convolve(src, K)[:T]
        feats.append(np.log1p(lam))
    bg = np.zeros((T, NCELL))
    for ci in range(NCELL):
        src = np.bincount(eb[ec == ci], minlength=T)
        bg[:, ci] = np.log1p(np.cumsum(src))
    feats.append(bg)
    feats.append(np.tile(clat, (T, 1)))
    feats.append(np.tile(clon, (T, 1)))
    X = np.stack(feats, axis=-1)
    return X, Y, tdays, clat, clon


def gh_put(path, content_bytes, msg):
    b64 = base64.b64encode(content_bytes).decode("ascii")
    r = subprocess.run(["gh", "api", "repos/%s/contents/%s?ref=master" % (REPO, path)], capture_output=True, text=True)
    body = {"message": msg, "content": b64, "branch": "master"}
    if r.returncode == 0:
        body["sha"] = json.loads(r.stdout)["sha"]
    open("/tmp/opf_put.json", "w").write(json.dumps(body))
    r2 = subprocess.run(["gh", "api", "-X", "PUT", "repos/%s/contents/%s" % (REPO, path), "--input", "/tmp/opf_put.json"], capture_output=True, text=True)
    if r2.returncode != 0:
        log("PUT FAIL %s: %s" % (path, r2.stderr[:200]))
        return None
    return json.loads(r2.stdout)["commit"]["sha"][:10]


def score_past(ev_t, ev_la, ev_lo, ev_m, o0, cat_end):
    r = subprocess.run(["gh", "api", "repos/%s/contents/forecasts?ref=master" % REPO], capture_output=True, text=True)
    if r.returncode != 0:
        return
    files = [f["name"] for f in json.loads(r.stdout) if f["name"].startswith("forecast_") and f["name"].endswith(".json")]
    scores = {}
    rs = subprocess.run(["gh", "api", "repos/%s/contents/forecasts/scores.json?ref=master" % REPO], capture_output=True, text=True)
    sha = None
    if rs.returncode == 0:
        meta = json.loads(rs.stdout)
        sha = meta["sha"]
        scores = json.loads(base64.b64decode(meta["content"]).decode("utf-8"))
    ei = np.round((ev_la - LAT0) / CS).astype(int)
    ej = np.round((ev_lo - LON0) / CS).astype(int)
    ing = (ei >= 0) & (ei < GH) & (ej >= 0) & (ej < GW)
    ecell = np.where(ing, ei * GW + ej, -1)
    ev_td = ev_t - o0
    changed = False
    for fname in files:
        key = fname.replace("forecast_", "").replace(".json", "")
        if key in scores:
            continue
        rf = subprocess.run(["gh", "api", "repos/%s/contents/forecasts/%s?ref=master" % (REPO, fname)], capture_output=True, text=True)
        if rf.returncode != 0:
            continue
        fc = json.loads(base64.b64decode(json.loads(rf.stdout)["content"]).decode("utf-8"))
        w = fc["forecast_window"]
        end_ord = datetime.fromisoformat(w["end"]).toordinal()
        if end_ord > o0 + cat_end:
            continue
        start_td = datetime.fromisoformat(w["start"]).toordinal() - o0
        realized = np.zeros(NCELL)
        m5sel = (ev_m >= 5.0) & ing & (ev_td >= start_td) & (ev_td < start_td + HOR)
        for cc in np.unique(ecell[m5sel]):
            realized[int(cc)] = 1.0
        probs = np.array([c["prob"] for c in fc["cells"]])
        auc = None
        if 0 < realized.sum() < NCELL:
            auc = round(float(roc_auc_score(realized, probs)), 4)
        brier = round(float(np.mean((probs - realized) ** 2)), 4)
        scores[key] = {"window": [w["start"], w["end"]], "n_cells_hit": int(realized.sum()), "auc": auc, "brier": brier}
        changed = True
        log("scored %s auc=%s hits=%d" % (key, str(auc), int(realized.sum())))
    if changed:
        b64 = base64.b64encode(json.dumps(scores, indent=1).encode("utf-8")).decode("ascii")
        body = {"message": "forecasts: score elapsed outlooks", "content": b64, "branch": "master"}
        if sha:
            body["sha"] = sha
        open("/tmp/opf_sc.json", "w").write(json.dumps(body))
        subprocess.run(["gh", "api", "-X", "PUT", "repos/%s/contents/forecasts/scores.json" % REPO, "--input", "/tmp/opf_sc.json"], capture_output=True, text=True)


def main():
    fetch_usgs()
    ev_t, ev_la, ev_lo, ev_m = parse()
    o0 = datetime(2011, 1, 1).toordinal()
    cat_end = (ev_t - o0).max()
    X, Y, tdays, clat, clon = build(ev_t, ev_la, ev_lo, ev_m, o0, cat_end)
    T = len(tdays)
    tstar = T - 1
    nf = X.shape[-1]
    Xall = X.reshape(T * NCELL, nf)
    yall = Y.reshape(-1)
    tid = np.repeat(np.arange(T), NCELL)
    row_day = tdays[tid]
    complete = ~np.isnan(yall)
    splits = []
    tstart = tdays[0] + 5 * 365.25
    while tstart + 365.25 <= cat_end - HOR:
        splits.append((tstart, tstart + 365.25))
        tstart += 365.25
    oos_s, oos_y, fa = [], [], []
    for s0, e0 in splits:
        tr = complete & (row_day >= tdays[0]) & (row_day < (s0 - HOR))
        te = complete & (row_day >= s0) & (row_day < e0)
        if yall[tr].sum() < 5 or yall[te].sum() < 5:
            continue
        sc = StandardScaler()
        Xtr = sc.fit_transform(Xall[tr])
        Xte = sc.transform(Xall[te])
        en = LogisticRegression(penalty="elasticnet", solver="saga", l1_ratio=0.5, C=0.1, max_iter=200, tol=1e-3)
        en.fit(Xtr, yall[tr])
        gb = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31, l2_regularization=1.0, early_stopping=True, validation_fraction=0.1, random_state=0)
        gb.fit(Xtr, yall[tr])
        r = 0.5 * rankdata(en.predict_proba(Xte)[:, 1]) / te.sum() + 0.5 * rankdata(gb.predict_proba(Xte)[:, 1]) / te.sum()
        oos_s.append(r)
        oos_y.append(yall[te])
        fa.append(roc_auc_score(yall[te], r))
    oos_s = np.concatenate(oos_s)
    oos_y = np.concatenate(oos_y)
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(oos_s, oos_y)
    skill = roc_auc_score(oos_y, oos_s)
    sc = StandardScaler()
    Xc = sc.fit_transform(Xall[complete])
    yc = yall[complete]
    enF = LogisticRegression(penalty="elasticnet", solver="saga", l1_ratio=0.5, C=0.1, max_iter=300, tol=1e-3)
    enF.fit(Xc, yc)
    gbF = HistGradientBoostingClassifier(max_iter=400, learning_rate=0.05, max_leaf_nodes=31, l2_regularization=1.0, early_stopping=True, validation_fraction=0.1, random_state=0)
    gbF.fit(Xc, yc)
    fc_idx = np.arange(tstar * NCELL, (tstar + 1) * NCELL)
    Xf = sc.transform(Xall[fc_idx])
    rscore = 0.5 * rankdata(enF.predict_proba(Xf)[:, 1]) / NCELL + 0.5 * rankdata(gbF.predict_proba(Xf)[:, 1]) / NCELL
    prob = iso.predict(rscore)
    base = np.nanmean(Y[:tstar], axis=0)
    ratio = prob / np.maximum(base, 1e-4)
    fc_date = datetime.fromordinal(int(o0 + tdays[tstar]))
    w0 = fc_date.strftime("%Y-%m-%d")
    w1 = (fc_date + timedelta(days=HOR)).strftime("%Y-%m-%d")
    cells = [{"lat": float(clat[k]), "lon": float(clon[k]), "prob": round(float(prob[k]), 4), "baseline": round(float(base[k]), 4), "elevation_ratio": round(float(ratio[k]), 2)} for k in range(NCELL)]
    out = {"forecast_window": {"start": w0, "end": w1, "days": HOR}, "target": "M5+ (USGS Mw, per 2deg cell)", "oos_skill_auc": round(skill, 4), "climatology_baseline_auc": 0.854, "generated": fc_date.strftime("%Y-%m-%d"), "method": "USGS catalog ETAS ensemble (ENET+GBT), isotonic-calibrated", "cells": cells}
    tag = fc_date.strftime("%Y-%m")
    jp = os.path.join(WORK, "forecast_%s.json" % tag)
    json.dump(out, open(jp, "w"), indent=1)
    P = prob.reshape(GH, GW)
    R = ratio.reshape(GH, GW)
    fig, ax = plt.subplots(1, 2, figsize=(13, 6))
    im0 = ax[0].imshow(P, origin="lower", extent=[127, 149, 25, 47], cmap="YlOrRd", vmin=0, aspect="auto")
    ax[0].set_title("M5+ probability, next 34 days" + chr(10) + "%s to %s" % (w0, w1), fontsize=13)
    ax[0].set_xlabel("Longitude")
    ax[0].set_ylabel("Latitude")
    plt.colorbar(im0, ax=ax[0], fraction=0.046)
    im1 = ax[1].imshow(R, origin="lower", extent=[127, 149, 25, 47], cmap="RdBu_r", vmin=0, vmax=3, aspect="auto")
    ax[1].set_title("Elevation vs baseline (x normal)" + chr(10) + "blue=quiet  red=elevated", fontsize=13)
    ax[1].set_xlabel("Longitude")
    ax[1].set_ylabel("Latitude")
    plt.colorbar(im1, ax=ax[1], fraction=0.046)
    fig.suptitle("Japan M5+ monthly outlook (USGS catalog)  OOS AUC=%.3f" % skill, fontsize=14)
    plt.tight_layout()
    pp = os.path.join(WORK, "forecast_%s.png" % tag)
    plt.savefig(pp, dpi=110)
    c1 = gh_put("forecasts/forecast_%s.json" % tag, open(jp, "rb").read(), "forecasts: %s M5+ outlook (auto)" % tag)
    c2 = gh_put("forecasts/forecast_%s.png" % tag, open(pp, "rb").read(), "forecasts: %s map (auto)" % tag)
    log("forecast %s window %s..%s skill=%.4f json=%s png=%s" % (tag, w0, w1, skill, c1, c2))
    score_past(ev_t, ev_la, ev_lo, ev_m, o0, cat_end)
    log("DONE")


if __name__ == "__main__":
    main()
