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

# Expected tables with minimum row thresholds, time column, and expected date range.
# "min_rows" is the absolute minimum to consider the table "populated".
# "expected_range" is (start_year, end_year) for coverage calculation.
#   None means no coverage check (event-based or analysis-derived tables).
# Tables with 0 min_rows are best-effort (auth-dependent, etc.).

# Analysis period: 2011-01-01 to present (~5,500 days)
FULL_RANGE = (2011, 2026)

EXPECTED_TABLES = {
    # Core earthquake data
    "earthquakes":       {"min_rows": 1000,  "time_col": "time",        "critical": True,  "expected_range": FULL_RANGE},
    "tec":               {"min_rows": 100,   "time_col": "observed_at", "critical": True,  "expected_range": FULL_RANGE},
    "geomag_kp":         {"min_rows": 100,   "time_col": "observed_at", "critical": True,  "expected_range": FULL_RANGE},
    "focal_mechanisms":  {"min_rows": 50,    "time_col": "observed_at", "critical": True,  "expected_range": FULL_RANGE},
    # Phase 5-7: ULF, LST, cosmic ray, GNSS-TEC
    # (collector_status removed: legacy table populated only by the old
    #  src/collectors/base.py architecture. Current fetcher-based workflow
    #  never writes to it, so validating it just generates false EMPTYs.)
    "ulf_magnetic":      {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "modis_lst":         {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": None},  # event-based
    "cosmic_ray":        {"min_rows": 100,   "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "gnss_tec":          {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "lightning":         {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": None},  # event-based
    "iss_lis_lightning": {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": (2017, 2023)},
    # Phase 9: INTERMAGNET, CSES, Movebank
    "geomag_hourly":     {"min_rows": 100,   "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "satellite_em":      {"min_rows": 0,     "time_col": "observed_at", "critical": False, "expected_range": (2018, 2026)},
    # Phase 10: Unconventional sources
    "olr":               {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "earth_rotation":    {"min_rows": 100,   "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "solar_wind":        {"min_rows": 100,   "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "gravity_mascon":    {"min_rows": 0,     "time_col": "observed_at", "critical": False, "expected_range": (2002, 2026)},
    "so2_column":        {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": (2004, 2026)},
    "soil_moisture":     {"min_rows": 0,     "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "cloud_fraction":    {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "nightlight":        {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": (2012, 2026)},
    # Phase 10b
    "tide_gauge":        {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "ocean_color":       {"min_rows": 0,     "time_col": "observed_at", "critical": False, "expected_range": (2018, 2026)},
    # InSAR disabled (LiCSAR has no processed interferograms for Japan, 2026-03-20)
    # "insar_deformation": {"min_rows": 0,     "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    # Phase 11: Space/cosmic
    "goes_xray":         {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "goes_proton":       {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "tidal_stress":      {"min_rows": 100,   "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "particle_flux":     {"min_rows": 0,     "time_col": "observed_at", "critical": False, "expected_range": None},  # rolling 7-day
    "dart_pressure":     {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": None},  # rolling recent
    "ioc_sea_level":     {"min_rows": 10,    "time_col": "observed_at", "critical": False, "expected_range": None},  # rolling recent
    "snet_pressure":     {"min_rows": 0,     "time_col": "observed_at", "critical": False, "expected_range": FULL_RANGE},
    "snet_waveform":     {"min_rows": 0,     "time_col": "date_str",    "critical": False, "expected_range": FULL_RANGE},
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

        # Coverage calculation (what % of expected date range is covered)
        entry["coverage_pct"] = None
        expected = spec.get("expected_range")
        if expected and entry["min_date"] and entry["max_date"]:
            try:
                actual_start = datetime.fromisoformat(entry["min_date"][:10])
                actual_end = datetime.fromisoformat(entry["max_date"][:10])
                expected_start = datetime(expected[0], 1, 1)
                expected_end = datetime(expected[1], 12, 31)
                expected_days = (expected_end - expected_start).days
                if expected_days > 0:
                    actual_days = (actual_end - actual_start).days
                    entry["coverage_pct"] = round(
                        min(actual_days / expected_days * 100, 100), 1
                    )
            except (ValueError, TypeError):
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
            cov = ""
            if entry.get("coverage_pct") is not None and entry["coverage_pct"] < 80:
                cov = f"  ⚠ coverage {entry['coverage_pct']}%"
            print(f"  ✓ {table}: {entry['rows']:>8,} rows{date_info}{cov}")
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

    # Write Job Summary (visible on GitHub Actions Run page)
    github_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if github_summary:
        with open(github_summary, "a") as f:
            f.write(f"## Data Validation: {overall}\n\n")
            f.write(f"**{ok_count} OK** / {warn_count} LOW / ")
            f.write(f"**{empty_count} EMPTY** / **{missing_count} MISSING** (of {total})\n\n")

            # Show problems first — EMPTY and MISSING tables
            problems = [
                (t, e) for t, e in report.items()
                if e["status"] in ("EMPTY", "MISSING")
            ]
            if problems:
                f.write("### ❌ Data Gaps (action needed)\n\n")
                f.write("| Table | Status | Reason |\n")
                f.write("|---|---|---|\n")
                for t, e in sorted(problems):
                    reason = _gap_reason(t)
                    f.write(f"| `{t}` | {e['status']} | {reason} |\n")
                f.write("\n")

            # Show low-coverage tables (have data but far from complete)
            LOW_COV_THRESHOLD = 80  # percent
            low_cov = [
                (t, e) for t, e in report.items()
                if e["status"] == "OK"
                and e.get("coverage_pct") is not None
                and e["coverage_pct"] < LOW_COV_THRESHOLD
            ]
            if low_cov:
                # Sort by coverage ascending (worst first)
                low_cov.sort(key=lambda x: x[1]["coverage_pct"])
                f.write("### ⚠️ Low Coverage (data exists but incomplete)\n\n")
                f.write("| Table | Coverage | Date Range | Expected | Rows |\n")
                f.write("|---|---|---|---|---|\n")
                for t, e in low_cov:
                    dates = f"{e['min_date'][:10]} → {e['max_date'][:10]}"
                    exp = EXPECTED_TABLES[t].get("expected_range")
                    exp_str = f"{exp[0]}–{exp[1]}" if exp else "—"
                    pct = e["coverage_pct"]
                    bar = _coverage_bar(pct)
                    f.write(f"| `{t}` | {bar} **{pct}%** | {dates} | {exp_str} | {e['rows']:,} |\n")
                f.write("\n")

            # Show OK tables compactly
            ok_tables = [
                (t, e) for t, e in report.items()
                if e["status"] == "OK"
            ]
            if ok_tables:
                f.write("<details><summary>✅ OK tables ({} sources)</summary>\n\n".format(len(ok_tables)))
                f.write("| Table | Rows | Date Range | Coverage |\n")
                f.write("|---|---|---|---|\n")
                for t, e in sorted(ok_tables):
                    dates = ""
                    if e["min_date"] and e["max_date"]:
                        dates = f"{e['min_date'][:10]} → {e['max_date'][:10]}"
                    cov = ""
                    if e.get("coverage_pct") is not None:
                        cov = f"{e['coverage_pct']}%"
                    f.write(f"| `{t}` | {e['rows']:,} | {dates} | {cov} |\n")
                f.write("\n</details>\n")

    return summary


def _coverage_bar(pct: float) -> str:
    """Return a visual coverage bar using Unicode block chars."""
    filled = int(pct / 10)
    empty = 10 - filled
    return "█" * filled + "░" * empty


def _gap_reason(table: str) -> str:
    """Return known reason for a data gap."""
    reasons = {
        "cloud_fraction": "Earthdata auth (MODIS OPeNDAP)",
        "so2_column": "Earthdata auth (OMI OPeNDAP)",
        "nightlight": "Earthdata auth (VIIRS LAADS)",
        "iss_lis_lightning": "Earthdata auth (GHRC DAAC)",
        "lightning": "Blitzortung archive restricted",
        "insar_deformation": "LiCSAR: no Japan frames returned",
        "satellite_em": "CSES: registration required",
        "snet_pressure": "NIED: replaced by snet_waveform (Phase 18)",
        "snet_waveform": "NIED: S-net waveform features (Phase 18, backfilling)",
        "collector_status": "Analysis-derived (RPi5 only)",
    }
    return reasons.get(table, "Unknown")


def compare_with_previous(current: dict, prev_path: str) -> list[str]:
    """Compare current validation with previous run. Return list of regressions."""
    try:
        with open(prev_path) as f:
            prev = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

    prev_tables = prev.get("tables", {})
    curr_tables = current.get("tables", {})
    regressions = []

    for table, curr_entry in curr_tables.items():
        prev_entry = prev_tables.get(table)
        if not prev_entry:
            continue

        prev_status = prev_entry.get("status", "MISSING")
        curr_status = curr_entry.get("status", "MISSING")
        prev_rows = prev_entry.get("rows", 0)
        curr_rows = curr_entry.get("rows", 0)

        # Status regression (OK→EMPTY, OK→LOW, etc.)
        status_rank = {"OK": 3, "LOW": 2, "EMPTY": 1, "MISSING": 0}
        if status_rank.get(curr_status, 0) < status_rank.get(prev_status, 0):
            regressions.append(
                f"⚠️ `{table}`: {prev_status} ({prev_rows:,} rows) → "
                f"**{curr_status}** ({curr_rows:,} rows)"
            )
        # Row count drop >20% (within same status)
        elif (prev_rows > 0 and curr_rows > 0
              and curr_rows < prev_rows * 0.8
              and curr_status == prev_status):
            drop_pct = round((1 - curr_rows / prev_rows) * 100, 1)
            regressions.append(
                f"📉 `{table}`: {prev_rows:,} → {curr_rows:,} rows "
                f"(-{drop_pct}%, status still {curr_status})"
            )

    return regressions


if __name__ == "__main__":
    result = validate()

    # Compare with previous run if --previous flag provided
    prev_path = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--previous" and i < len(sys.argv) - 1:
            prev_path = sys.argv[i + 1]

    if prev_path and Path(prev_path).exists():
        regressions = compare_with_previous(result, prev_path)
        if regressions:
            print("\n" + "=" * 70)
            print(f"⚠️  DATA REGRESSIONS DETECTED ({len(regressions)} tables)")
            print("=" * 70)
            for r in regressions:
                print(f"  {r}")

            # Append to Job Summary
            github_summary = os.environ.get("GITHUB_STEP_SUMMARY")
            if github_summary:
                with open(github_summary, "a") as f:
                    f.write(f"\n### ⚠️ Data Regressions vs Previous Run ({len(regressions)})\n\n")
                    f.write("| Change |\n|---|\n")
                    for r in regressions:
                        f.write(f"| {r} |\n")
                    f.write("\n")
        else:
            print("\n✅ No data regressions vs previous run")

    # Always exit 0 — validation is informational, not blocking.
    # The report is saved to results/ for artifact upload.
    sys.exit(0)
