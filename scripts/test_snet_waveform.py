"""Test S-net multi-sensor waveform pipeline (Phase 19).

Validates:
    1. HinetPy authentication & data download
    2. WIN32 decode → SAC parsing → 3-component grouping
    3. Spectral analysis: RMS, H/V ratio, band power, spectral slope
    4. VLF spectral analysis: 200s window, 0.01-0.1 Hz band (velocity)
    5. Per-station feature completeness
    6. Cable segment classification
    7. SQLite round-trip with sensor_type column
    8. ML loader integration (multi-sensor)
    9. Network code survey: 0120/0120A/0120C

Run: python scripts/test_snet_waveform.py
"""

import asyncio
import os
import struct
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_sac_channel_name(filepath: str) -> str:
    """Read KCMPNM (component name) from SAC header.

    SAC string section starts at byte 440 (after 70 floats + 40 ints = 440 bytes).
    Each string field is 8 bytes. KCMPNM is the 21st string field.
    Actually: strings start at 440. Layout:
        440: KSTNM (8 bytes) - station name
        448: KEVNM (16 bytes) - event name
        464: KHOLE (8 bytes)
        472: KO (8 bytes)
        480: KA (8 bytes)
        488-536: KT0-KT9 (8 bytes each = 80 bytes)
        536: KF (8 bytes)
        544: KUSER0 (8 bytes)
        552: KUSER1 (8 bytes)
        560: KCMPNM (8 bytes) - component name
        568: KNETWK (8 bytes) - network name
    """
    try:
        with open(filepath, "rb") as f:
            header = f.read(632)
            if len(header) < 632:
                return ""
            # KCMPNM at offset 560, 8 bytes
            raw = header[560:568]
            return raw.decode("ascii", errors="replace").strip().rstrip("\x00").rstrip("-")
    except Exception:
        return ""


def read_sac_station_name(filepath: str) -> str:
    """Read KSTNM from SAC header (offset 440, 8 bytes)."""
    try:
        with open(filepath, "rb") as f:
            header = f.read(632)
            if len(header) < 448:
                return ""
            raw = header[440:448]
            return raw.decode("ascii", errors="replace").strip().rstrip("\x00").rstrip("-")
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("S-NET WAVEFORM FEATURE EXTRACTION TEST (Phase 18)")
    print("=" * 70)

    ok_count = 0
    warn_count = 0
    fail_count = 0
    total = 0

    def report(status, num, msg):
        nonlocal ok_count, warn_count, fail_count, total
        total += 1
        prefix = {"OK": "OK  ", "WARN": "WARN", "FAIL": "FAIL"}[status]
        print(f"{prefix} [{num}/{12}] {msg}")
        if status == "OK":
            ok_count += 1
        elif status == "WARN":
            warn_count += 1
        else:
            fail_count += 1

    # ---- Test 1: Authentication ----
    user = os.environ.get("HINET_USER", "").strip()
    password = os.environ.get("HINET_PASS", "").strip()
    if not user or not password:
        report("FAIL", 1, "HINET_USER/HINET_PASS not set")
        print(f"\n{'=' * 70}")
        print(f"FINAL: {ok_count} OK / {warn_count} WARN / {fail_count} FAIL (of {total})")
        print(f"{'=' * 70}")
        return

    try:
        from HinetPy import Client
        client = Client(user, password)
        report("OK", 1, "Authentication successful")
    except Exception as e:
        report("FAIL", 1, f"Authentication failed: {e}")
        return

    # ---- Test 2: Station metadata ----
    try:
        stations = client.get_station_list("0120A")
        station_coords = {}
        if stations:
            for st in stations:
                sid = getattr(st, "name", None)
                lat = getattr(st, "latitude", None)
                lon = getattr(st, "longitude", None)
                if sid and lat is not None:
                    station_coords[str(sid)] = (float(lat), float(lon))
        report("OK", 2, f"Station metadata: {len(station_coords)} stations with coordinates")
    except Exception as e:
        report("FAIL", 2, f"Station metadata failed: {e}")
        station_coords = {}

    # ---- Test 3: Waveform download (1 segment, yesterday) ----
    target = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=2)
    target = target.replace(hour=12, minute=0, second=0, microsecond=0)
    work_dir = tempfile.mkdtemp(prefix="snet_wf_test_")

    try:
        data = client.get_continuous_waveform("0120A", target, 5, outdir=work_dir)
        if data and isinstance(data, tuple) and data[0]:
            win32_file, ch_table = data
            fsize = Path(win32_file).stat().st_size
            report("OK", 3, f"Waveform download: {fsize:,} bytes")
        else:
            report("FAIL", 3, "No data returned")
            return
    except Exception as e:
        report("FAIL", 3, f"Download failed: {e}")
        return

    # ---- Test 4: WIN32 decode ----
    try:
        from HinetPy import win32 as hinetwin32
        sac_files = hinetwin32.extract_sac(win32_file, ch_table, outdir=work_dir)
        if not sac_files:
            sac_files = list(Path(work_dir).glob("*.SAC"))
        report("OK", 4, f"WIN32 decode: {len(sac_files)} SAC files")
    except Exception as e:
        report("FAIL", 4, f"WIN32 decode failed: {e}")
        return

    # ---- Test 5: SAC header channel name survey ----
    channel_names = {}
    for sac_path in sac_files[:30]:  # Sample first 30
        kcmpnm = read_sac_channel_name(str(sac_path))
        kstnm = read_sac_station_name(str(sac_path))
        basename = Path(sac_path).stem
        parts = basename.split(".")
        # Handle both 4-part (N.STA.LOC.CHA) and 3-part (N.STA.CHA) formats
        channel = parts[3] if len(parts) > 3 else parts[-1] if parts else ""
        suffix = channel[-1].upper() if channel else "?"
        channel_names.setdefault(suffix, set()).add(kcmpnm)

    print(f"       SAC header channel names by suffix:")
    for suffix, names in sorted(channel_names.items()):
        print(f"         {suffix}: KCMPNM = {names}")
    report("OK", 5, f"SAC header survey: {len(channel_names)} suffixes found")

    # ---- Test 6: 3-component grouping ----
    import numpy as np

    station_files = {}
    for sac_path in sac_files:
        basename = Path(sac_path).stem
        parts = basename.split(".")
        if len(parts) < 3:
            continue
        station_id = parts[1]
        # Handle both 4-part (N.STA.LOC.CHA) and 3-part (N.STA.CHA) formats
        channel = parts[3] if len(parts) > 3 else parts[-1]
        suffix = channel[-1].upper()
        if suffix in ("Z", "X", "Y"):
            station_files.setdefault(station_id, {})[suffix] = str(sac_path)

    complete_stations = {k: v for k, v in station_files.items() if len(v) >= 3}
    report(
        "OK" if len(complete_stations) > 100 else "WARN",
        6,
        f"3-component grouping: {len(complete_stations)}/{len(station_files)} stations complete",
    )

    # ---- Test 7: Waveform feature extraction (standard + VLF) ----
    sys.path.insert(0, str(Path(__file__).parent))
    from fetch_snet_waveform import compute_waveform_features, read_sac_data

    features_computed = 0
    features_failed = 0
    vlf_computed = 0
    sample_features = None
    sample_vlf_features = None

    for station_id, comps in list(complete_stations.items())[:20]:
        data_z, info_z = read_sac_data(comps["Z"])
        data_x, info_x = read_sac_data(comps["X"])
        data_y, info_y = read_sac_data(comps["Y"])

        if data_z is None or data_x is None or data_y is None:
            features_failed += 1
            continue

        fs = info_z.get("fs", 100.0) if info_z else 100.0

        # Standard extraction (acceleration mode)
        features = compute_waveform_features(data_z, data_x, data_y, fs)
        if features:
            features_computed += 1
            if sample_features is None:
                sample_features = (station_id, features)

            # VLF extraction (velocity mode — test with same data)
            vlf_features = compute_waveform_features(
                data_z, data_x, data_y, fs, vlf_analysis=True)
            if vlf_features and vlf_features.get("vlf_power") is not None:
                vlf_computed += 1
                if sample_vlf_features is None:
                    sample_vlf_features = (station_id, vlf_features)
        else:
            features_failed += 1

    if features_computed > 0:
        report("OK", 7, f"Feature extraction: {features_computed} OK, {features_failed} failed, VLF: {vlf_computed}")
        sid, sf = sample_features
        print(f"       Sample ({sid}):")
        for k, v in sf.items():
            print(f"         {k}: {v:.6f}" if v is not None else f"         {k}: None")
        if sample_vlf_features:
            vsid, vsf = sample_vlf_features
            print(f"       VLF sample ({vsid}):")
            print(f"         vlf_power: {vsf['vlf_power']:.6f}")
            print(f"         vlf_hv_ratio: {vsf['vlf_hv_ratio']:.6f}")
    else:
        report("FAIL", 7, "Feature extraction: all failed")

    # ---- Test 8: Feature value sanity checks ----
    issues = []
    if sample_features:
        _, sf = sample_features
        if sf["rms_z"] <= 0:
            issues.append("rms_z <= 0")
        if sf["rms_h"] <= 0:
            issues.append("rms_h <= 0")
        if sf["hv_ratio"] <= 0:
            issues.append("hv_ratio <= 0 (should be positive)")
        if sf["spectral_slope"] > 0:
            issues.append(f"spectral_slope > 0 ({sf['spectral_slope']:.2f}, expected negative for physical signals)")
        if sf["lf_power"] == sf["hf_power"]:
            issues.append("lf_power == hf_power (suspiciously identical)")

    if issues:
        report("WARN", 8, f"Sanity checks: {len(issues)} issues: {'; '.join(issues)}")
    else:
        report("OK", 8, "Sanity checks passed")

    # ---- Test 9: Cable segment classification ----
    from fetch_snet_waveform import classify_cable_segment, classify_cable_segment_by_coords

    classified = 0
    unclassified = 0
    seg_counts = {}
    for station_id in complete_stations:
        seg = classify_cable_segment(station_id)
        if seg is None and station_id in station_coords:
            lat, lon = station_coords[station_id]
            seg = classify_cable_segment_by_coords(lat, lon)
        if seg:
            classified += 1
            seg_counts[seg] = seg_counts.get(seg, 0) + 1
        else:
            unclassified += 1

    print(f"       Segments: {seg_counts}")
    report(
        "OK" if classified > unclassified else "WARN",
        9,
        f"Cable segment: {classified} classified, {unclassified} unclassified",
    )

    # ---- Test 10: SQLite round-trip (Phase 19 schema with sensor_type) ----
    import sqlite3
    test_db = os.path.join(work_dir, "test.db")
    try:
        conn = sqlite3.connect(test_db)
        conn.execute("""CREATE TABLE snet_waveform (
            id INTEGER PRIMARY KEY, station_id TEXT, date_str TEXT,
            segment_hour INTEGER, sensor_type TEXT NOT NULL DEFAULT '0120A',
            rms_z REAL, rms_h REAL, hv_ratio REAL,
            lf_power REAL, hf_power REAL, spectral_slope REAL,
            vlf_power REAL, vlf_hv_ratio REAL,
            n_samples INTEGER, latitude REAL, longitude REAL,
            cable_segment TEXT, received_at TEXT,
            UNIQUE(station_id, date_str, segment_hour, sensor_type))""")

        if sample_features:
            sid, sf = sample_features
            # Insert acceleration row
            conn.execute(
                "INSERT INTO snet_waveform VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sid, "2026-03-21", 12, "accel",
                 sf["rms_z"], sf["rms_h"], sf["hv_ratio"],
                 sf["lf_power"], sf["hf_power"], sf["spectral_slope"],
                 None, None,
                 30000, 35.0, 142.0, "S3", "2026-03-23T10:00:00Z"),
            )
            # Insert velocity row (same station, same time, different sensor_type)
            vlf_p = sample_vlf_features[1]["vlf_power"] if sample_vlf_features else None
            vlf_hv = sample_vlf_features[1]["vlf_hv_ratio"] if sample_vlf_features else None
            conn.execute(
                "INSERT INTO snet_waveform VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sid, "2026-03-21", 12, "velocity",
                 sf["rms_z"], sf["rms_h"], sf["hv_ratio"],
                 sf["lf_power"], sf["hf_power"], sf["spectral_slope"],
                 vlf_p, vlf_hv,
                 30000, 35.0, 142.0, "S3", "2026-03-23T10:00:00Z"),
            )
            conn.commit()
            rows = conn.execute("SELECT sensor_type, COUNT(*) FROM snet_waveform GROUP BY sensor_type").fetchall()
            sensor_summary = ", ".join(f"{r[0]}: {r[1]}" for r in rows)
            report("OK", 10, f"SQLite round-trip: {sensor_summary}")
        else:
            report("WARN", 10, "SQLite round-trip skipped (no features)")
        conn.close()
    except Exception as e:
        report("FAIL", 10, f"SQLite round-trip failed: {e}")

    # ---- Test 11: ML loader integration ----
    try:
        from ml_prediction import load_phase18_snet_waveform
        # Test with the temporary DB (has 1 row)
        os.environ["GEOHAZARD_DB_PATH"] = test_db
        result = asyncio.run(load_phase18_snet_waveform(test_db))
        if result:
            sample_date = list(result.keys())[0]
            sample_entry = result[sample_date]
            expected_keys = ["rms_combined", "hv_ratio", "lf_power", "spatial_gradient", "segment_max_anomaly"]
            missing_keys = [k for k in expected_keys if k not in sample_entry]
            if missing_keys:
                report("WARN", 11, f"ML loader: missing keys {missing_keys}")
            else:
                report("OK", 11, f"ML loader: {len(result)} dates, {len(sample_entry)} fields/date")
                print(f"       Sample keys: {sorted(sample_entry.keys())[:10]}...")
        else:
            report("WARN", 11, "ML loader returned empty (expected with 1 row)")
    except Exception as e:
        report("FAIL", 11, f"ML loader failed: {e}")

    # ---- Test 12: Network code channel survey (0120/B/C) ----
    print("\n--- Network code channel survey (0120/0120B/0120C) ---")
    survey_results = {}
    for code in ["0120", "0120B", "0120C"]:
        survey_dir = tempfile.mkdtemp(prefix=f"snet_survey_{code}_")
        try:
            data = client.get_continuous_waveform(code, target, 1, outdir=survey_dir)
            if data and isinstance(data, tuple) and data[0]:
                sac_files_survey = hinetwin32.extract_sac(data[0], data[1], outdir=survey_dir)
                if not sac_files_survey:
                    sac_files_survey = list(Path(survey_dir).glob("*.SAC"))

                # Read channel names from SAC headers
                channels = {}
                for sf in sac_files_survey[:30]:
                    kcmpnm = read_sac_channel_name(str(sf))
                    basename = Path(sf).stem
                    parts = basename.split(".")
                    file_channel = parts[3] if len(parts) > 3 else parts[-1] if parts else "?"
                    channels[file_channel] = kcmpnm

                survey_results[code] = {
                    "n_files": len(sac_files_survey),
                    "channels": channels,
                }
                unique_kcmpnm = set(channels.values())
                print(f"  {code}: {len(sac_files_survey)} SAC files, KCMPNM = {unique_kcmpnm}")
                # Show sample file→channel mapping
                for fc, kc in list(channels.items())[:5]:
                    print(f"    file_channel={fc} → KCMPNM={kc}")
            else:
                print(f"  {code}: No data returned")
                survey_results[code] = {"n_files": 0, "channels": {}}
        except Exception as e:
            print(f"  {code}: Error: {e}")
            survey_results[code] = {"error": str(e)}
        finally:
            import shutil
            shutil.rmtree(survey_dir, ignore_errors=True)

    report("OK", 12, f"Network survey: {len(survey_results)} codes tested")

    # ---- Cleanup ----
    import shutil
    shutil.rmtree(work_dir, ignore_errors=True)

    # ---- Summary ----
    print(f"\n{'=' * 70}")
    print("FINAL SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Results:  {ok_count} OK  /  {warn_count} WARN  /  {fail_count} FAIL  (of {total})")
    print(f"  Feature extraction: {features_computed}/{features_computed + features_failed} stations")
    if sample_features:
        _, sf = sample_features
        print(f"  Feature ranges:")
        for k, v in sf.items():
            print(f"    {k}: {v:.6f}")
    print(f"  Cable segments: {seg_counts}")
    print(f"  Network codes surveyed: {list(survey_results.keys())}")
    for code, info in survey_results.items():
        if "channels" in info:
            unique = set(info["channels"].values())
            print(f"    {code}: {info['n_files']} files, channels = {unique}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
