# -*- coding: utf-8 -*-
"""Round 305 input fetch (label-free): one calendar year of AIRS L3 v7 daily fields on the full 1-degree grid.

SUDDEN AXIS: isolated M>=6 label on all 2,280 global arena cells, 33-day issues; R2 ISO 0.771474 (size272/304).
DIMENSION CEILING (oracle upper bounds): per-(cell, 33-day block) 0.99961; per-window cell 0.98718.
WHY THIS ROUND: catalogue history before an issue adds ~0 to R2 (sizing 304), so block-by-block information must
  come from a new observation system.  AIRS L3 (2002-08-31 onward, daily, ocean included) is untested on this
  arena.  CO (500 hPa) and CH4 (400 hPa) are the declared quantities; air temperature and water vapour at the same
  levels are the rival arm (opus plan audit 2026-10-16: shared temperature / cloud-clearing errors).  This script
  only fetches; it reads no earthquake data.

Source: AIRS3STD v7.0, collection C1701805652-GES_DISC, Earthdata Cloud OPeNDAP (bearer token from env EDL_TOKEN,
  never printed).  NASA Earth science data: CC0 unless marked (Earthdata data use guidance); cite AIRS project
  (2019), AIRS/Aqua L3 Daily Standard Physical Retrieval 1 degree V7.0, GES DISC, doi:10.5067/UO3Q64CTTS1U.
Stored per orbit (A = ascending ~13:30 local, D = descending ~01:30 local), float16, NaN = no retrieval (fill or
  _ct == 0), converted in float64 first:
  co5  CO_VMR level 5 x 1e9 (ppbv)          ch46 CH4_VMR level 6 x 1e9 - 1800 (ppbv - 1800)
  t5   Temperature level 5 - 250 (K - 250)  t6   Temperature level 6 - 250
  q5   H2O_MMR level 5 (g/kg)               q6   H2O_MMR level 6
Gates (each can fail and stops the job):
  L  StdPressureLev[5, 6] == 500, 400 hPa and H2OPressureLev[5, 6] == 500, 400 hPa (read from the granule).
  F  every stored float16 decodes to within half a float16 step of the float64 value, and no finite value overflows.
  C  CMR time_end of every granule is no later than the file date + 1 day + 12 h (the L3 day is split at the
     date line; the round's lag rule E = floor(issue) - 3 relies on this bound).
Usage: python fetch.py YEAR OUT.npz   (env LIMIT=n fetches only the first n days, for smoke tests)
"""
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import netCDF4
import numpy as np

YEAR, OUT = int(sys.argv[1]), sys.argv[2]
COLL = "C1701805652-GES_DISC"
TOK = os.environ["EDL_TOKEN"].strip()
# (key, AIRS variable, level index, scale, offset)
FIELDS = (("co5", "CO_VMR", 5, 1e9, 0.0), ("ch46", "CH4_VMR", 6, 1e9, 1800.0),
          ("t5", "Temperature", 5, 1.0, 250.0), ("t6", "Temperature", 6, 1.0, 250.0),
          ("q5", "H2O_MMR", 5, 1.0, 0.0), ("q6", "H2O_MMR", 6, 1.0, 0.0))
ORB = (("A", "ascending"), ("D", "descending"))


def get(url, timeout=180, tries=1):
    for att in range(tries):
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + TOK})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            body = e.read()[:400]
            title = body.split(b"<title>")[1].split(b"</title>")[0] if b"<title>" in body else body[:120]
            print("HTTP %d attempt %d: %s" % (e.code, att + 1, title.decode("utf-8", "replace")), flush=True)
            if att == tries - 1:
                raise
            time.sleep(20 * (att + 1))


def granules(year):
    out, page = {}, 1
    while True:
        q = urllib.parse.urlencode({"short_name": "AIRS3STD", "version": "7.0", "page_size": 400, "page_num": page,
                                    "temporal": "%d-01-01T00:00:00Z,%d-12-31T23:59:59Z" % (year, year)})
        r = urllib.request.urlopen("https://cmr.earthdata.nasa.gov/search/granules.json?" + q, timeout=60)
        ent = json.load(r)["feed"]["entry"]
        if not ent:
            return out
        for e in ent:
            t = e["title"].split(":", 1)[1]
            d = t.split(".")[1:4]
            if int(d[0]) == year:
                out["%s-%s-%s" % tuple(d)] = (e["title"], e.get("time_start", ""), e.get("time_end", ""))
        page += 1


RANGE = {"CO_VMR": (5, 5), "CH4_VMR": (6, 6), "Temperature": (5, 6), "H2O_MMR": (5, 6)}


def ce_for():
    parts = []
    for v, (k0, k1) in RANGE.items():
        for s, o in ORB:
            for suf in ("", "_ct"):
                parts.append("/%s/Data_Fields/%s_%s%s%%5B%d:1:%d%%5D%%5B0:1:179%%5D%%5B0:1:359%%5D"
                             % (o, v, s, suf, k0, k1))
    return ";".join(parts)


def url_for(title, ce):
    return ("https://opendap.earthdata.nasa.gov/collections/%s/granules/%s.dap.nc4?dap4.ce=%s"
            % (COLL, urllib.parse.quote(title, safe=""), ce))


def main():
    gl = granules(YEAR)
    days = sorted(gl)[:int(os.environ.get("LIMIT", "0")) or None]
    print("granules %d  first %s last %s" % (len(days), days[0], days[-1]), flush=True)
    # gate C from CMR metadata
    for d in days:
        _, ts, te = gl[d]
        lim = dt.datetime.fromisoformat(d) + dt.timedelta(days=1, hours=12)
        tend = dt.datetime.fromisoformat(te.replace("Z", "")).replace(tzinfo=None)
        assert tend <= lim, "gate C %s time_end %s" % (d, te)
    print("gate C PASS (%d granules, time_end <= date + 36 h)" % len(days), flush=True)
    tmp = OUT + ".part.nc4"
    # gate L on the first granule
    open(tmp, "wb").write(get(url_for(gl[days[0]][0], "/StdPressureLev_ascending;/H2OPressureLev_ascending"), tries=4))
    ds = netCDF4.Dataset(tmp)
    sp = np.asarray(ds.variables["StdPressureLev_ascending"][:], float)
    hp = np.asarray(ds.variables["H2OPressureLev_ascending"][:], float)
    ds.close()
    assert abs(sp[5] - 500) < 1e-3 and abs(sp[6] - 400) < 1e-3, "gate L std %s" % sp[:8]
    assert abs(hp[5] - 500) < 1e-3 and abs(hp[6] - 400) < 1e-3, "gate L h2o %s" % hp[:8]
    print("gate L PASS  StdPressureLev %s  H2OPressureLev %s" % (sp[5:7], hp[5:7]), flush=True)
    VAL = {"%s_%s" % (f[0], s): np.full((len(days), 180, 360), np.nan, np.float16) for f in FIELDS for s, _ in ORB}
    fails = []
    ce = ce_for().split(";")
    for i, d in enumerate(days):
        for att in range(5):
            try:
                for vreq in RANGE:   # one request per variable (Hyrax returns 500 for all four in one request)
                    open(tmp, "wb").write(get(url_for(gl[d][0], ";".join(p for p in ce if "/" + vreq + "_" in p))))
                    ds = netCDF4.Dataset(tmp)
                    for key, v, k, sc, off in FIELDS:
                        if v != vreq:
                            continue
                        for s, o in ORB:
                            F = ds.groups[o].groups["Data_Fields"].variables
                            li = k - RANGE[v][0]
                            a = np.ma.filled(F["%s_%s" % (v, s)][li].astype(np.float64), np.nan)
                            c = np.ma.filled(F["%s_%s_ct" % (v, s)][li], 0)
                            m = np.where(np.isfinite(a) & (c > 0) & (a > -9000), a * sc - off, np.nan)
                            h = m.astype(np.float16)
                            fin = np.isfinite(m)
                            assert np.array_equal(fin, np.isfinite(h)), "gate F overflow %s %s" % (key, d)
                            step = np.spacing(np.abs(h[fin])).astype(np.float64)
                            assert np.all(np.abs(h[fin].astype(np.float64) - m[fin]) <= 0.5 * step + 1e-12),                                 "gate F %s %s" % (key, d)
                            VAL["%s_%s" % (key, s)][i] = h
                    ds.close()
                break
            except AssertionError:
                raise
            except Exception as e:  # noqa: BLE001
                if att == 4:
                    fails.append(d)
                    print("FAIL %s %s" % (d, type(e).__name__), flush=True)
                time.sleep(15 * (att + 1))
        if i % 30 == 0:
            print("%s done %d/%d" % (d, i + 1, len(days)), flush=True)
    if os.path.exists(tmp):
        os.remove(tmp)
    np.savez_compressed(OUT, days=np.array(days), fails=np.array(fails),
                        time_start=np.array([gl[d][1] for d in days]), time_end=np.array([gl[d][2] for d in days]),
                        **VAL)
    for k, v in VAL.items():
        print("%s finite %.3f" % (k, np.isfinite(v).mean()))
    print("DONE days %d fails %d" % (len(days), len(fails)), flush=True)
    assert len(fails) <= 3, "too many failed days"


if __name__ == "__main__":
    main()
