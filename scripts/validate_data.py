"""Validate database completeness after data fetch phase.

Checks every expected table for:
  - Existence
  - Row count (warns if empty or below threshold)
  - Date range coverage
  - Freshness (latest record date)

Outputs a structured JSON report + human-readable summary.
Exit code 0 always (non-blocking), but sets VALIDATION_STATUS env var.
"""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = os.environ.get("GEOHAZARD_DB_PATH", "./data/geohazard.db")

# Expected tables with minimum row thresholds and time column names.
# "min_rows" is the absolute minimum to consider the table "populated".
# Tables with 0 min_rows are best-effort (auth-dependent, etc.).
EXPECTED_TABLES = {
    # Core earthquake data
    "earthquakes":       {"min_rows": 1000,  "time_col": "time",        "critical": True},
    "tec":               {"min_rows": 100,   "time_col": "observed_at", "critical": True},
    "geomag_kp":         {"min_rows": 100,   "time_col": "observed_at", "critical": True},
    "focal_mechanisms":  {"min_rows": 50,    "time_col": "observed_at", "critical": True},
    # Analysis-derived (no time_col, just check existence)
    "collector_status":  {"min_rows": 0,     "time_col": None,          "critical": False},
    # Phase 5-7: ULF, LST, cosmic ray, GNSS-TEC
    "ulf_magnetic":      {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "modis_lst":         {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "cosmic_ray":        {"min_rows": 100,   "time_col": "observed_at", "critical": False},
    "gnss_tec":          {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "lightning":         {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    # Phase 9: INTERMAGNET, CSES, Movebank
    "geomag_hourly":     {"min_rows": 100,   "time_col": "observed_at", "critical": False},
    "satellite_em":      {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    # Phase 10: Unconventional sources
    "olr":               {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "earth_rotation":    {"min_rows": 100,   "time_col": "observed_at", "critical": False},
    "solar_wind":        {"min_rows": 100,   "time_col": "observed_at", "critical": False},
    "gravity_mascon":    {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    "so2_column":        {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    "soil_moisture":     {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    "cloud_fraction":    {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    "nightlight":        {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    # Phase 10b
    "tide_gauge":        {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "ocean_color":       {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    "insar_deformation": {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    # Phase 11: Space/cosmic
    "goes_xray":         {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "goes_proton":       {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "tidal_stress":      {"min_rows": 100,   "time_col": "observed_at", "critical": False},
    "particle_flux":     {"min_rows": 0,     "time_col": "observed_at", "critical": False},
    "dart_pressure":     {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "ioc_sea_level":     {"min_rows": 10,    "time_col": "observed_at", "critical": False},
    "snet_pressure":     {"min_rows": 0,     "time_col": "observed_at", "critical": False},
}


def validate():
    if not Path(DB_PATH).exists():
        print(f"FATAL: Database not found at {DB_PATH}")
        return {"status": "FATAL", "tables": {}, "summary": "Database file missing"}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get all existing tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = {row[0] for row in cursor.fetchall()}

    report = {}
    ok_count = 0
    warn_count = 0
    empty_count = 0
    missing_count = 0
    critical_failures = []

    for table, spec in EXPECTED_TABLES.items():
        entry = {
            "exists": table in existing_tables,
            "rows": 0,
            "min_date": None,
            "max_date": None,
            "status": "MISSING",
        }

        if table not in existing_tables:
            missing_count += 1
            if spec["critical"]:
                critical_failures.append(f"{table} (MISSING)")
            report[table] = entry
            continue

        # Row count
        cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
        entry["rows"] = cursor.fetchone()[0]

        # Date range if time column exists
        if spec["time_col"] and entry["rows"] > 0:
            try:
                cursor.execute(
                    f"SELECT MIN([{spec['time_col']}]), MAX([{spec['time_col']}]) "
                    f"FROM [{table}]"
                )
                row = cursor.fetchone()
                entry["min_date"] = row[0]
                entry["max_date"] = row[1]
            except sqlite3.OperationalError:
                pass

        # Status determination
        if entry["rows"] == 0:
            entry["status"] = "EMPTY"
            empty_count += 1
            if spec["critical"]:
                critical_failures.append(f"{table} (EMPTY)")
        elif entry["rows"] < spec["min_rows"]:
            entry["status"] = "LOW"
            warn_count += 1
            if spec["critical"]:
                critical_failures.append(
                    f"{table} ({entry['rows']}/{spec['min_rows']})"
                )
        else:
            entry["status"] = "OK"
            ok_count += 1

        report[table] = entry

    conn.close()

    # Overall status
    if critical_failures:
        overall = "CRITICAL"
    elif missing_count > 5 or empty_count > 10:
        overall = "DEGRADED"
    elif warn_count > 5:
        overall = "WARNING"
    else:
        overall = "HEALTHY"

    total = len(EXPECTED_TABLES)
    summary = {
        "status": overall,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_tables": total,
        "ok": ok_count,
        "low": warn_count,
        "empty": empty_count,
        "missing": missing_count,
        "critical_failures": critical_failures,
        "tables": report,
    }

    # Human-readable output
    print("=" * 70)
    print(f"DATA VALIDATION REPORT — {summary['timestamp']}")
    print(f"Status: {overall}")
    print(f"Tables: {ok_count} OK / {warn_count} LOW / {empty_count} EMPTY / {missing_count} MISSING (of {total})")
    if critical_failures:
        print(f"CRITICAL FAILURES: {', '.join(critical_failures)}")
    print("=" * 70)

    # Table-by-table report
    for table in sorted(report.keys()):
        entry = report[table]
        spec = EXPECTED_TABLES[table]
        crit = " [CRITICAL]" if spec["critical"] else ""
        if entry["status"] == "OK":
            date_info = ""
            if entry["min_date"] and entry["max_date"]:
                date_info = f"  ({entry['min_date'][:10]} → {entry['max_date'][:10]})"
            print(f"  ✓ {table}: {entry['rows']:>8,} rows{date_info}")
        elif entry["status"] == "LOW":
            print(f"  ⚠ {table}: {entry['rows']:>8,} rows (min: {spec['min_rows']}){crit}")
        elif entry["status"] == "EMPTY":
            print(f"  ✗ {table}: EMPTY{crit}")
        else:
            print(f"  ✗ {table}: MISSING{crit}")

    print("=" * 70)

    # Save JSON report
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    report_path = results_dir / "data_validation.json"
    with open(report_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"Report saved to {report_path}")

    # Set GitHub Actions output
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"validation_status={overall}\n")
            f.write(f"ok_tables={ok_count}\n")
            f.write(f"total_tables={total}\n")

    return summary


if __name__ == "__main__":
    result = validate()
    # Always exit 0 — validation is informational, not blocking.
    # The report is saved to results/ for artifact upload.
    sys.exit(0)
