"""Round 129 -- reduce NGL five-minute kinematic GNSS to per-read-step summaries.

Runs on a public GitHub Actions runner, not on the Raspberry Pi: it downloads about 100 MB per
station from the open NGL archive, keeps nothing raw, and writes one small npz per chunk of
stations.  The research design is fixed in prereg_prod129.txt; this file only implements it.

For each station and each of five worlds -- the real one and four floors that shift the whole
network circularly in time by one offset each -- it summarises, at every read step:
    0  the mean horizontal position over the last  6 hours, minus the daily reference model
    1  the same over the last 24 hours
    2  the largest step between two consecutive epochs inside the last 24 hours
Values are stored as hundredths of a millimetre in int16 with a validity mask.
"""
import datetime as DT
import gzip
import io
import os
import sys
import time
import urllib.request
import zipfile

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
TENV = "https://geodesy.unr.edu/gps_timeseries/IGS20/tenv3/IGS20/%s.tenv3"
KENV = "https://geodesy.unr.edu/gps_timeseries/IGS20/kenv/%s/%s.%d.kenv.zip"
YEARS = list(range(2001, 2026))
SPAN0 = DT.date(2001, 1, 1)
SPAN1 = DT.date(2026, 1, 1)
SPAN = (SPAN1 - SPAN0).days
SEEDS = (0, 1, 2, 3)
W6, W24 = 6 * 3600.0, 24 * 3600.0
MIN6, MIN24 = 36, 144
EPOCH = 300.0
J2000 = DT.datetime(2000, 1, 1, 12, 0, 0)
SIG_EN, SIG_U = 20.0, 60.0
REF_A, REF_B, REF_MIN = 1095, 90, 400
CAP = 32760

CHUNK = int(os.environ.get("CHUNK", "0"))
CHUNKS = int(os.environ.get("CHUNKS", "12"))
OUT = os.environ.get("OUT", "round129_chunk%02d.npz" % CHUNK)


def offsets():
    """one circular offset per floor seed, drawn the same way in every job"""
    return [int(np.random.default_rng(1290 + s).integers(366, SPAN - 366)) for s in SEEDS]


def get(url, tries=4):
    for k in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=180) as h:
                return h.read()
        except Exception as e:
            if k + 1 == tries:
                raise
            time.sleep(5 * (k + 1))


def daily(sta):
    """the daily product: day number from SPAN0, east and north in mm, quality cut"""
    txt = get(TENV % sta).decode("ascii", "replace").split("\n")
    mon = {m: i + 1 for i, m in enumerate(
        ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"])}
    d, e, n = [], [], []
    for ln in txt[1:]:
        p = ln.split()
        if len(p) < 17:
            continue
        try:
            yy = int(p[1][:2]); yy += 2000 if yy < 80 else 1900
            o = DT.date(yy, mon[p[1][2:5]], int(p[1][5:])).toordinal() - SPAN0.toordinal()
        except (KeyError, ValueError):
            continue
        if float(p[14]) * 1000 > SIG_EN or float(p[15]) * 1000 > SIG_EN:
            continue
        if float(p[16]) * 1000 > SIG_U:
            continue
        d.append(o)
        e.append((float(p[7]) + float(p[8])) * 1000.0)
        n.append((float(p[9]) + float(p[10])) * 1000.0)
    return np.array(d, float), np.array(e), np.array(n)


def basis(x, phase):
    """constant, linear in years, annual and semiannual"""
    return np.stack([np.ones_like(x), x, np.sin(phase), np.cos(phase),
                     np.sin(2 * phase), np.cos(2 * phase)], axis=1)


def reference_co(dd, de, dn, T):
    """the six-parameter daily fit over [T-1095, T-90) days, anchored at T (days from SPAN0).
    Returns the two coefficient vectors, or None when the window is too thin."""
    a = np.searchsorted(dd, T - REF_A, "left")
    b = np.searchsorted(dd, T - REF_B, "left")
    if b - a < REF_MIN:
        return None
    x = (dd[a:b] - T) / 365.25
    ph = 2.0 * np.pi * dd[a:b] / 365.25
    B = basis(x, ph)
    return [np.linalg.lstsq(B, y, rcond=None)[0] for y in (de[a:b], dn[a:b])]


def kinematic(sta):
    """every five-minute epoch of the span: seconds from SPAN0, east and north in mm"""
    S, E, N = [], [], []
    for y in YEARS:
        try:
            raw = get(KENV % (sta, sta, y))
        except Exception as e:
            print("  %s %d missing (%s)" % (sta, y, repr(e)[:60]), flush=True)
            continue
        z = zipfile.ZipFile(io.BytesIO(raw))
        for nm in z.namelist():
            try:
                body = gzip.decompress(z.read(nm))
            except Exception:
                continue
            nl = body.find(bytes([10]))
            if nl < 0:
                continue
            head = body[:nl].split()
            if not head or head[0] != b"site":
                continue
            ncol = len(head)
            tok = body[nl + 1:].split()
            m = len(tok) // ncol
            if m == 0 or len(tok) % ncol:
                continue
            arr = np.array(tok[:m * ncol]).reshape(m, ncol)
            try:
                sec = arr[:, 1].astype(np.float64)
                e = arr[:, 8].astype(np.float64) * 1000.0
                n = arr[:, 9].astype(np.float64) * 1000.0
            except ValueError:
                continue
            S.append(sec); E.append(e); N.append(n)
    if not S:
        return None
    S = np.concatenate(S); E = np.concatenate(E); N = np.concatenate(N)
    k = np.argsort(S, kind="mergesort")
    S, E, N = S[k], E[k], N[k]
    # seconds since J2000 -> seconds since SPAN0 midnight UTC
    base = (DT.datetime(SPAN0.year, SPAN0.month, SPAN0.day) - J2000).total_seconds()
    return S - base, E, N


def predict(co, T, Tm):
    x = np.array([(Tm - T) / 365.25])
    ph = 2.0 * np.pi * np.array([Tm]) / 365.25
    B = basis(x, ph)
    return float(B[0].dot(co[0])), float(B[0].dot(co[1]))


def datum_series(dd, de, dn, S, E, N):
    """the constant that separates the two products, as a function of the past only.

    The kinematic file measures from a station reference longitude and latitude and the daily
    file from its own first solution, so the two differ by a constant per station.  It is removed
    with the two observed series and no model: the daily mean of the kinematic epochs minus the
    daily product, day by day, which a rolling median over the reference window then reduces to
    one number for each row."""
    day = np.floor(S / 86400.0).astype(np.int64)
    edge = np.flatnonzero(np.r_[True, day[1:] != day[:-1]])
    ends = np.r_[edge[1:], len(day)]
    kd = day[edge]
    ke = np.add.reduceat(E, edge) / (ends - edge)
    kn = np.add.reduceat(N, edge) / (ends - edge)
    pos = np.searchsorted(dd, kd)
    ok = (pos < len(dd)) & (dd[np.minimum(pos, len(dd) - 1)] == kd)
    return kd[ok], ke[ok] - de[pos[ok]], kn[ok] - dn[pos[ok]]


def summarise(sta, steps, offs):
    dd, de, dn = daily(sta)
    kin = kinematic(sta)
    if kin is None or len(dd) < REF_MIN:
        return None
    S, E, N = kin
    gd, ge, gn = datum_series(dd, de, dn, S, E, N)
    ns = len(steps)
    A = np.zeros((5, ns, 3), np.int16)
    M = np.zeros((5, ns, 3), bool)
    capped = 0
    total_sec = SPAN * 86400.0
    for w in range(5):
        off = 0.0 if w == 0 else offs[w - 1] * 86400.0
        for i, t0 in enumerate(steps):
            T = (t0 + off) % total_sec
            Td = T / 86400.0
            a24 = np.searchsorted(S, T - W24, "left")
            b24 = np.searchsorted(S, T, "left")
            if b24 - a24 >= MIN24:
                ds = np.diff(S[a24:b24])
                ok = np.abs(ds - EPOCH) < 1.0
                if ok.any():
                    j = np.hypot(np.diff(E[a24:b24])[ok], np.diff(N[a24:b24])[ok]).max()
                    q = int(round(j * 100.0))
                    if q > CAP:
                        capped += 1
                    else:
                        A[w, i, 2] = q
                        M[w, i, 2] = True
            co = reference_co(dd, de, dn, Td)
            if co is None:
                continue
            g0 = np.searchsorted(gd, Td - REF_A, "left")
            g1 = np.searchsorted(gd, Td - REF_B, "left")
            if g1 - g0 < REF_MIN:
                continue
            oe, on = np.median(ge[g0:g1]), np.median(gn[g0:g1])
            for k, (win, need) in enumerate(((W6, MIN6), (W24, MIN24))):
                a = np.searchsorted(S, T - win, "left") if k == 0 else a24
                b = np.searchsorted(S, T, "left") if k == 0 else b24
                if b - a < need:
                    continue
                pe, pn = predict(co, Td, (T - win / 2.0) / 86400.0)
                v = np.hypot(E[a:b].mean() - oe - pe, N[a:b].mean() - on - pn)
                q = int(round(v * 100.0))
                if q > CAP:
                    capped += 1
                    continue
                A[w, i, k] = q
                M[w, i, k] = True
    return A, M, capped, len(S)


def main():
    t0 = time.time()
    stations = [l.split(",")[0] for l in
                open(os.path.join(HERE, "round129_stations.csv")).read().split("\n")[1:] if l.strip()]
    dates = [l.split(",")[1] for l in
             open(os.path.join(HERE, "round129_steps.csv")).read().split("\n")[1:] if l.strip()]
    steps = np.array([(DT.date.fromisoformat(d).toordinal() - SPAN0.toordinal()) * 86400.0
                      - 9 * 3600.0 for d in dates])
    offs = offsets()
    mine = stations[CHUNK::CHUNKS]
    print("chunk %d of %d: %d stations, %d steps, offsets %s"
          % (CHUNK, CHUNKS, len(mine), len(steps), offs), flush=True)
    names, AA, MM = [], [], []
    for s in mine:
        try:
            r = summarise(s, steps, offs)
        except Exception as e:
            print("  %s failed %s" % (s, repr(e)[:120]), flush=True)
            continue
        if r is None:
            print("  %s no usable data" % s, flush=True)
            continue
        A, M, capped, nep = r
        names.append(s); AA.append(A); MM.append(M)
        print("  %s epochs %d defined %.1f%% capped %d [%.0fs]"
              % (s, nep, 100.0 * M.mean(), capped, time.time() - t0), flush=True)
    np.savez_compressed(OUT, station=np.array(names), value=np.array(AA, np.int16),
                        mask=np.array(MM), offsets=np.array(offs), steps=steps)
    print("wrote %s with %d stations [%.0fs]" % (OUT, len(names), time.time() - t0), flush=True)


if __name__ == "__main__":
    main()
