"""Comprehensive S-net data pipeline test.

Tests every step from authentication to data storage, with full
visibility into data content, coverage, and quality at each stage.

Test items:
  1.  Authentication
  2.  Station list + geographic coverage
  3.  Data latency (most recent available date)
  4.  Waveform download (5 min segment)
  5.  WIN32 decode + channel table analysis
  6.  SAC file parsing + sampling rate
  7.  Channel type classification (pressure vs seismometer)
  8.  Per-station data availability
  9.  Pressure value physical validation
  10. SQLite write/read round-trip
  11. Temporal coverage probe (2016-present, 9 dates)
  12. fetch_snet_pressure.py compatibility check
"""

import os
import shutil
import struct
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

PASS = 0
FAIL = 0
WARN = 0
TOTAL = 12


def ok(n, msg):
    global PASS
    PASS += 1
    print(f"OK   [{n}/{TOTAL}] {msg}")


def fail(n, msg):
    global FAIL
    FAIL += 1
    print(f"FAIL [{n}/{TOTAL}] {msg}")


def warn(n, msg):
    global WARN
    WARN += 1
    print(f"WARN [{n}/{TOTAL}] {msg}")


def read_sac(filepath):
    """Read SAC binary file, return (npts, sampling_rate, data_array) or None."""
    try:
        with open(filepath, "rb") as f:
            header = f.read(632)
            if len(header) < 632:
                return None
            # Sampling interval (DELTA) is float header index 0
            delta = struct.unpack_from("<f", header, 0)[0]
            if delta <= 0 or delta > 100:
                delta = struct.unpack_from(">f", header, 0)[0]
            sampling_rate = 1.0 / delta if delta > 0 else 0

            # NPTS is int header index 9 (offset 280 + 9*4 = 316)
            npts = struct.unpack_from("<i", header, 316)[0]
            endian = "<"
            if npts <= 0 or npts > 10_000_000:
                npts = struct.unpack_from(">i", header, 316)[0]
                endian = ">"
                if npts <= 0 or npts > 10_000_000:
                    return None

            arr = np.frombuffer(f.read(npts * 4), dtype=f"{endian}f4")
            if len(arr) != npts:
                return None
            return npts, sampling_rate, arr
    except Exception:
        return None


def main():
    user = os.environ.get("HINET_USER", "").strip()
    pwd = os.environ.get("HINET_PASS", "").strip()

    print("=" * 70)
    print("S-NET DATA PIPELINE TEST")
    print("=" * 70)
    print()

    # --- 1. Authentication ---
    if not user or not pwd:
        fail(1, "HINET_USER/HINET_PASS not set")
        sys.exit(1)

    from HinetPy import Client
    try:
        client = Client(user, pwd)
        ok(1, "Authentication successful")
    except Exception as e:
        fail(1, f"Authentication failed: {e}")
        sys.exit(1)

    # --- 2. Station list + geographic coverage ---
    station_coords = {}
    try:
        stations = client.get_station_list("0120A")
        n_stations = len(stations) if stations else 0
        if n_stations == 0:
            fail(2, "Station list: 0 stations")
            sys.exit(1)

        # Debug: inspect station object structure
        sample_st = stations[0] if stations else None
        if sample_st:
            print(f"       Station object type: {type(sample_st)}")
            if hasattr(sample_st, "__dict__"):
                print(f"       Attributes: {list(sample_st.__dict__.keys())}")
            # Show first 5 stations with all attributes
            none_lat_count = 0
            for i, s in enumerate(stations[:5]):
                lat = getattr(s, "latitude", "N/A")
                lon = getattr(s, "longitude", "N/A")
                code = getattr(s, "code", "N/A")
                name = getattr(s, "name", "N/A")
                elev = getattr(s, "elevation", "N/A")
                print(f"       [{i}] code={code} name={name} lat={lat} lon={lon} elev={elev}")
            for s in stations:
                if getattr(s, "latitude", None) is None:
                    none_lat_count += 1
            print(f"       Stations with lat=None: {none_lat_count}/{n_stations}")

        for st in stations:
            # Use 'name' (e.g. N.S1N01) as station ID, not 'code' (0120A for all)
            sid = getattr(st, "name", None) or getattr(st, "code", None)
            lat = getattr(st, "latitude", None)
            lon = getattr(st, "longitude", None)
            if sid and lat is not None:
                station_coords[str(sid)] = (float(lat), float(lon))

        ok(2, f"Station list: {n_stations} stations, {len(station_coords)} with coordinates")
        if station_coords:
            lats = [v[0] for v in station_coords.values()]
            lons = [v[1] for v in station_coords.values()]
            print(f"       Lat:  {min(lats):.2f} - {max(lats):.2f}")
            print(f"       Lon:  {min(lons):.2f} - {max(lons):.2f}")
            # Cable segment grouping
            segments = defaultdict(int)
            for sid in station_coords:
                prefix = sid[:4] if len(sid) >= 4 else sid
                segments[prefix] += 1
            print(f"       Cable segments: {dict(sorted(segments.items()))}")
    except Exception as e:
        fail(2, f"Station list error: {e}")
        sys.exit(1)

    # --- 3. Data latency check ---
    print()
    print("--- Data latency check ---")
    latency_work = tempfile.mkdtemp(prefix="snet_latency_")
    latest_available = None
    for days_ago in [0, 1, 2, 3, 5]:
        dt = datetime.utcnow() - timedelta(days=days_ago)
        probe = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            pdata = client.get_continuous_waveform("0120A", probe, 1, outdir=latency_work)
            if pdata and isinstance(pdata, tuple) and pdata[0] is not None:
                fsize = Path(pdata[0]).stat().st_size if Path(pdata[0]).exists() else 0
                print(f"  {days_ago}d ago ({probe.strftime('%Y-%m-%d')}): OK  {fsize:>10,} bytes")
                if latest_available is None:
                    latest_available = probe
            else:
                print(f"  {days_ago}d ago ({probe.strftime('%Y-%m-%d')}): no data")
        except Exception as e:
            print(f"  {days_ago}d ago ({probe.strftime('%Y-%m-%d')}): error - {str(e)[:60]}")
    shutil.rmtree(latency_work, ignore_errors=True)

    if latest_available:
        lag = (datetime.utcnow() - latest_available).days
        ok(3, f"Data latency: latest available = {latest_available.strftime('%Y-%m-%d')} ({lag}d lag)")
    else:
        warn(3, "Data latency: could not determine latest available date")

    # --- 4. Waveform download (main test segment) ---
    print()
    target = datetime.utcnow() - timedelta(days=1)
    start = target.replace(hour=12, minute=0, second=0, microsecond=0)
    work_dir = tempfile.mkdtemp(prefix="snet_main_")
    print(f"--- Main test: {start.strftime('%Y-%m-%d %H:%M')} UTC, 5 min ---")

    try:
        data = client.get_continuous_waveform("0120A", start, 5, outdir=work_dir)
        if data is None or not isinstance(data, tuple) or data[0] is None:
            fail(4, "Waveform download: no data returned")
            sys.exit(1)
        win32_file, ch_table = data
        fsize = Path(win32_file).stat().st_size
        ok(4, f"Waveform download: {fsize:,} bytes")
        print(f"       File: {Path(win32_file).name}")
    except Exception as e:
        fail(4, f"Waveform download: {e}")
        sys.exit(1)

    # --- 5. WIN32 decode + channel table ---
    ch_table_lines = []
    if ch_table and Path(ch_table).exists():
        ch_table_lines = Path(ch_table).read_text().strip().split("\n")

    try:
        from HinetPy import win32
        import subprocess
        # Verify win32tools are available
        catwin32_path = shutil.which("catwin32")
        win2sac_path = shutil.which("win2sac_32")
        print(f"       catwin32:   {catwin32_path}")
        print(f"       win2sac_32: {win2sac_path}")
        if not win2sac_path:
            fail(5, "WIN32 decode: win2sac_32 not found in PATH")
            sys.exit(1)

        # List work_dir before extract
        pre_files = set(os.listdir(work_dir))

        sac_files = win32.extract_sac(win32_file, ch_table, outdir=work_dir)

        # Check what files were actually created
        post_files = set(os.listdir(work_dir))
        new_files = post_files - pre_files
        sac_on_disk = [f for f in new_files if f.endswith(".SAC") or f.endswith(".sac")]
        print(f"       New files created: {len(new_files)} (SAC: {len(sac_on_disk)})")
        if new_files and not sac_on_disk:
            print(f"       Non-SAC files: {sorted(new_files)[:10]}")

        if not sac_files:
            # Try listing all SAC files manually
            all_sac = list(Path(work_dir).glob("**/*.SAC")) + list(Path(work_dir).glob("**/*.sac"))
            if all_sac:
                sac_files = all_sac
                print(f"       extract_sac returned empty but found {len(all_sac)} SAC files on disk")
            else:
                fail(5, f"WIN32 decode: 0 SAC files (new files: {sorted(new_files)[:5]})")
                sys.exit(1)
        ok(5, f"WIN32 decode: {len(sac_files)} SAC files from {len(ch_table_lines)} channels")
        sizes = [Path(str(f)).stat().st_size for f in sac_files if Path(str(f)).exists()]
        if sizes:
            print(f"       SAC sizes: min={min(sizes):,}  max={max(sizes):,}  total={sum(sizes):,} bytes")
    except Exception as e:
        fail(5, f"WIN32 decode: {e}")
        sys.exit(1)

    # --- 6. SAC parsing + sampling rate ---
    parsed = []
    parse_errors = 0
    sampling_rates = set()

    for sac_path in sac_files:
        basename = Path(str(sac_path)).stem
        parts = basename.split(".")
        station = parts[1] if len(parts) > 1 else "?"
        channel = parts[3] if len(parts) > 3 else parts[-1] if parts else "?"

        result = read_sac(str(sac_path))
        if result is None:
            parse_errors += 1
            continue
        npts, sr, arr = result
        sampling_rates.add(round(sr, 1))
        parsed.append({
            "station": station, "channel": channel,
            "npts": npts, "sr": sr,
            "mean": float(np.mean(arr)), "std": float(np.std(arr)),
            "min": float(np.min(arr)), "max": float(np.max(arr)),
        })

    if parsed:
        ok(6, f"SAC parsing: {len(parsed)}/{len(sac_files)} OK, {parse_errors} errors")
        print(f"       Sampling rates found: {sorted(sampling_rates)} Hz")
        print(f"       Expected: 10 Hz (S-net continuous), 100 Hz (triggered)")
    else:
        fail(6, f"SAC parsing: 0 files parsed, {parse_errors} errors")
        sys.exit(1)

    # --- 7. Channel type classification ---
    print()
    channel_types = Counter(p["channel"] for p in parsed)
    channel_suffixes = Counter(p["channel"][-1] for p in parsed)

    # Classify channels
    pressure_ch = [p for p in parsed if p["channel"].endswith("U")]
    accel_ch = [p for p in parsed if p["channel"][-1] in ("X", "Y", "Z") and not p["channel"].endswith("U")]
    other_ch = [p for p in parsed if p not in pressure_ch and p not in accel_ch]

    print("--- Channel classification ---")
    print(f"  Suffix breakdown: {dict(channel_suffixes.most_common())}")
    print(f"  Full channel types ({len(channel_types)}):")
    for ch, count in channel_types.most_common():
        label = "PRESSURE" if ch.endswith("U") else "ACCEL/VEL" if ch[-1] in "XYZ" else "OTHER"
        print(f"    {ch:>8}: {count:>4} files  [{label}]")

    if pressure_ch:
        ok(7, f"Pressure channels: {len(pressure_ch)} (suffix 'U'), "
              f"accel/vel: {len(accel_ch)}, other: {len(other_ch)}")
    else:
        warn(7, f"No channels ending with 'U' found. "
               f"Available suffixes: {dict(channel_suffixes)}. "
               f"fetch_snet_pressure.py filter needs update!")

    # --- 8. Per-station data availability ---
    print()
    station_data = defaultdict(lambda: {"pressure": 0, "accel": 0, "other": 0, "channels": []})
    for p in parsed:
        st = p["station"]
        if p["channel"].endswith("U"):
            station_data[st]["pressure"] += 1
        elif p["channel"][-1] in "XYZ":
            station_data[st]["accel"] += 1
        else:
            station_data[st]["other"] += 1
        station_data[st]["channels"].append(p["channel"])

    stations_with_pressure = sum(1 for v in station_data.values() if v["pressure"] > 0)
    stations_with_any = len(station_data)

    ok(8, f"Per-station: {stations_with_any}/{n_stations} have data, "
          f"{stations_with_pressure} have pressure channel")
    print(f"       Stations with pressure data: {stations_with_pressure}/{n_stations} "
          f"({100*stations_with_pressure/n_stations:.0f}%)")
    print(f"       Stations with any data:      {stations_with_any}/{n_stations} "
          f"({100*stations_with_any/n_stations:.0f}%)")

    # Show per-station detail (first 10 + last 5)
    sorted_stations = sorted(station_data.items())
    print(f"       Station detail ({len(sorted_stations)} stations):")
    print(f"       {'Station':<12} {'P':>3} {'A':>3} {'O':>3}  Channels")
    show = sorted_stations[:8]
    if len(sorted_stations) > 13:
        show += [("...", {"pressure": 0, "accel": 0, "other": 0, "channels": []})]
        show += sorted_stations[-5:]
    elif len(sorted_stations) > 8:
        show = sorted_stations
    for st, info in show:
        if st == "...":
            print(f"       {'...':>12}")
            continue
        chs = ",".join(sorted(set(info["channels"])))
        print(f"       {st:<12} {info['pressure']:>3} {info['accel']:>3} {info['other']:>3}  {chs}")

    # --- 9. Pressure value physical validation ---
    print()
    if pressure_ch:
        means = [p["mean"] for p in pressure_ch]
        stds = [p["std"] for p in pressure_ch]
        mins = [p["min"] for p in pressure_ch]
        maxs = [p["max"] for p in pressure_ch]

        zero_std = sum(1 for s in stds if s == 0)
        has_nan = sum(1 for m in means if np.isnan(m))
        valid = len(pressure_ch) - zero_std - has_nan

        print("--- Pressure value analysis ---")
        print(f"  Raw value statistics (from SAC, likely in counts or Pa):")
        print(f"    Mean:  min={min(means):.2f}  max={max(means):.2f}  median={np.median(means):.2f}")
        print(f"    Std:   min={min(stds):.4f}  max={max(stds):.4f}")
        print(f"    Range: min={min(mins):.2f}  max={max(maxs):.2f}")
        print(f"    Valid: {valid}/{len(pressure_ch)} (zero-std={zero_std}, NaN={has_nan})")

        # Show distribution
        print(f"  Per-station pressure (first 10):")
        print(f"  {'Station':<12} {'Npts':>8} {'Mean':>14} {'Std':>12} {'Min':>14} {'Max':>14}")
        for p in sorted(pressure_ch, key=lambda x: x["station"])[:10]:
            print(f"  {p['station']:<12} {p['npts']:>8,} {p['mean']:>14.2f} {p['std']:>12.2f} "
                  f"{p['min']:>14.2f} {p['max']:>14.2f}")

        # Physical interpretation
        print()
        median_mean = np.median(means)
        if abs(median_mean) > 1e6:
            print(f"  Interpretation: values ~{median_mean:.0f} → likely raw ADC counts")
            print(f"  fetch_snet_pressure.py divides by 100 (Pa→hPa) — may need calibration factor")
        elif abs(median_mean) > 1000:
            print(f"  Interpretation: values ~{median_mean:.0f} → likely in Pa")
            print(f"  As hPa: {median_mean/100:.1f} hPa")
            depth_m = median_mean / 100 / 1.01325 * 10  # rough: 1 atm ≈ 10m water
            print(f"  Implied depth: ~{depth_m:.0f} m (if absolute pressure in Pa)")
        else:
            print(f"  Interpretation: values ~{median_mean:.2f} → unit unclear, needs investigation")

        if valid > 0:
            ok(9, f"Pressure validation: {valid}/{len(pressure_ch)} valid channels")
        else:
            warn(9, f"Pressure validation: all channels have zero-std or NaN")
    else:
        warn(9, "Pressure validation skipped: no pressure channels identified")

    # --- 10. SQLite write/read round-trip ---
    print()
    import sqlite3
    db_path = os.environ.get("GEOHAZARD_DB_PATH", "./data/geohazard.db")
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE IF NOT EXISTS snet_pressure (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            pressure_mean_hpa REAL,
            pressure_std_hpa REAL,
            latitude REAL, longitude REAL,
            n_samples INTEGER,
            received_at TEXT NOT NULL,
            UNIQUE(station_id, observed_at))""")

        write_data = pressure_ch if pressure_ch else parsed[:10]
        inserted = 0
        for p in write_data:
            lat, lon = station_coords.get(p["station"], (None, None))
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO snet_pressure "
                    "(station_id, observed_at, pressure_mean_hpa, pressure_std_hpa, "
                    "latitude, longitude, n_samples, received_at) VALUES (?,?,?,?,?,?,?,?)",
                    (p["station"], start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                     p["mean"] / 100.0, p["std"] / 100.0,
                     lat, lon, p["npts"], datetime.utcnow().isoformat()),
                )
                inserted += 1
            except Exception:
                pass
        conn.commit()

        # Read back and verify
        count = conn.execute("SELECT COUNT(*) FROM snet_pressure").fetchone()[0]
        sample = conn.execute(
            "SELECT station_id, pressure_mean_hpa, pressure_std_hpa, n_samples "
            "FROM snet_pressure LIMIT 3"
        ).fetchall()
        conn.close()

        ok(10, f"SQLite round-trip: {inserted} written, {count} in table")
        for row in sample:
            print(f"       {row[0]}: mean={row[1]:.2f} hPa, std={row[2]:.4f}, n={row[3]}")
    except Exception as e:
        fail(10, f"SQLite: {e}")

    # --- 11. Temporal coverage probe ---
    print()
    print("--- Temporal coverage probe ---")
    print("  Testing data availability across S-net history (2016-present)...")
    probe_dates = [
        ("Yesterday",     datetime.utcnow() - timedelta(days=1)),
        ("1 week ago",    datetime.utcnow() - timedelta(days=7)),
        ("1 month ago",   datetime.utcnow() - timedelta(days=30)),
        ("3 months ago",  datetime.utcnow() - timedelta(days=90)),
        ("6 months ago",  datetime.utcnow() - timedelta(days=180)),
        ("1 year ago",    datetime.utcnow() - timedelta(days=365)),
        ("2 years ago",   datetime.utcnow() - timedelta(days=730)),
        ("2020-01-15",    datetime(2020, 1, 15, 12, 0)),
        ("2018-01-15",    datetime(2018, 1, 15, 12, 0)),
        ("2016-08-15",    datetime(2016, 8, 15, 12, 0)),
        ("2016-01-15",    datetime(2016, 1, 15, 12, 0)),
    ]
    probe_work = tempfile.mkdtemp(prefix="snet_probe_")
    available_count = 0
    for label, dt in probe_dates:
        probe_start = dt.replace(hour=12, minute=0, second=0, microsecond=0)
        try:
            pdata = client.get_continuous_waveform("0120A", probe_start, 1, outdir=probe_work)
            if pdata and isinstance(pdata, tuple) and pdata[0] is not None:
                psize = Path(pdata[0]).stat().st_size if Path(pdata[0]).exists() else 0
                status = "OK"
                available_count += 1
            else:
                psize = 0
                status = "MISS"
        except Exception as e:
            psize = 0
            status = f"ERR({str(e)[:40]})"
        print(f"  {status:>6}  {label:<16} ({probe_start.strftime('%Y-%m-%d')})  {psize:>10,} bytes")
    shutil.rmtree(probe_work, ignore_errors=True)

    if available_count >= 8:
        ok(11, f"Temporal coverage: {available_count}/{len(probe_dates)} dates available")
    elif available_count >= 4:
        warn(11, f"Temporal coverage: {available_count}/{len(probe_dates)} dates (partial)")
    else:
        fail(11, f"Temporal coverage: only {available_count}/{len(probe_dates)} dates available")

    # --- 12. fetch_snet_pressure.py compatibility ---
    print()
    issues = []
    # Check channel filter matches reality
    if not pressure_ch:
        issues.append("Channel filter 'endswith U' found 0 pressure channels — needs update")
    # Check coordinate availability
    stations_no_coords = [st for st in station_data if st not in station_coords]
    if stations_no_coords:
        issues.append(f"{len(stations_no_coords)} stations missing coordinates")
    # Check sampling rate assumption
    if sampling_rates and 10.0 not in sampling_rates:
        issues.append(f"Expected 10 Hz sampling, found {sorted(sampling_rates)} Hz — "
                      f"npts calculation may be wrong")
    # Check timezone handling (already fixed)
    # Check data volume estimate for CI
    if pressure_ch:
        est_daily = len(pressure_ch) * 6  # 6 segments/day in fetch script
        est_monthly = est_daily * 30
        print(f"  Estimated daily rows:   {est_daily} (6 segments × {len(pressure_ch)} stations)")
        print(f"  Estimated monthly rows: {est_monthly}")

    if not issues:
        ok(12, "fetch_snet_pressure.py compatibility: no issues found")
    else:
        warn(12, f"fetch_snet_pressure.py: {len(issues)} issue(s)")
        for issue in issues:
            print(f"       ⚠ {issue}")

    # ====== FINAL SUMMARY ======
    print()
    print("=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    print(f"  Results:  {PASS} OK  /  {WARN} WARN  /  {FAIL} FAIL  (of {TOTAL})")
    print(f"  Network:            0120A (S-net)")
    print(f"  Stations:           {n_stations} total, {stations_with_any} with data, "
          f"{stations_with_pressure} with pressure")
    print(f"  SAC files:          {len(sac_files)} decoded, {len(parsed)} parsed")
    print(f"  Channels:           {dict(channel_suffixes.most_common())}")
    print(f"  Pressure channels:  {len(pressure_ch)}")
    print(f"  Sampling rates:     {sorted(sampling_rates)} Hz")
    print(f"  Temporal coverage:  {available_count}/{len(probe_dates)} probe dates")
    print(f"  DB rows:            {inserted} inserted")
    print("=" * 70)

    # Clean up
    shutil.rmtree(work_dir, ignore_errors=True)

    if FAIL > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
