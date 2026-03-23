"""Diagnose data gaps in the geohazard database.

Produces a detailed report showing:
1. Per-table actual date coverage (distinct dates vs expected range)
2. Year-by-year date counts (heatmap-style)
3. Spatial data cell coverage analysis
4. ML analysis period gap detection (2011-2026)

Usage:
  python scripts/diagnose_data_gaps.py [--db PATH] [--json] [--ci]

  --db PATH   Path to geohazard.db (default: ./data/geohazard.db)
  --json      Output JSON report to results/data_gaps.json
  --ci        Write GitHub Actions Job Summary
"""

import json
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB_PATH = os.environ.get("GEOHAZARD_DB_PATH", "./data/geohazard.db")

# Analysis period for ML
ANALYSIS_START = datetime(2011, 1, 1)
ANALYSIS_END = datetime(2026, 3, 31)

# Tables with spatial data (cell_lat, cell_lon columns)
SPATIAL_TABLES = {
    "so2_column":      {"time_col": "observed_at", "lat_col": "cell_lat", "lon_col": "cell_lon",
                        "expected_range": (2004, 2026), "temporal": "daily"},
    "cloud_fraction":  {"time_col": "observed_at", "lat_col": "cell_lat", "lon_col": "cell_lon",
                        "expected_range": (2011, 2026), "temporal": "daily"},
    "nightlight":      {"time_col": "observed_at", "lat_col": "cell_lat", "lon_col": "cell_lon",
                        "expected_range": (2012, 2026), "temporal": "annual"},
    "gravity_mascon":  {"time_col": "observed_at", "lat_col": "cell_lat", "lon_col": "cell_lon",
                        "expected_range": (2002, 2026), "temporal": "monthly"},
    "soil_moisture":   {"time_col": "observed_at", "lat_col": "cell_lat", "lon_col": "cell_lon",
                        "expected_range": (2011, 2026), "temporal": "monthly"},
    "ocean_color":     {"time_col": "observed_at", "lat_col": "cell_lat", "lon_col": "cell_lon",
                        "expected_range": (2018, 2026), "temporal": "weekly"},
    "olr":             {"time_col": "observed_at", "lat_col": "cell_lat", "lon_col": "cell_lon",
                        "expected_range": (2011, 2026), "temporal": "daily"},
}

# Non-spatial time series tables
TIMESERIES_TABLES = {
    "earthquakes":     {"time_col": "time",        "expected_range": (2011, 2026)},
    "tec":             {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "geomag_kp":       {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "cosmic_ray":      {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "geomag_hourly":   {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "solar_wind":      {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "earth_rotation":  {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "goes_xray":       {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "goes_proton":     {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "tidal_stress":    {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "tide_gauge":      {"time_col": "observed_at", "expected_range": (2011, 2026)},
    "particle_flux":   {"time_col": "observed_at", "expected_range": None},
    "dart_pressure":   {"time_col": "observed_at", "expected_range": None},
    "ioc_sea_level":   {"time_col": "observed_at", "expected_range": None},
    "iss_lis_lightning": {"time_col": "observed_at", "expected_range": (2017, 2023)},
}

# Japan bounding box for cell_key analysis (matching 2° grid)
JAPAN_CELLS = []
for lat in range(24, 48, 2):
    for lon in range(122, 150, 2):
        JAPAN_CELLS.append((float(lat), float(lon)))


def diagnose(db_path: str) -> dict:
    """Run full data gap diagnosis."""
    if not Path(db_path).exists():
        print(f"FATAL: Database not found at {db_path}")
        return {"error": "Database not found"}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get existing tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {row[0] for row in cursor.fetchall()}

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "spatial": {},
        "timeseries": {},
        "summary": {},
    }

    # ---- 1. Spatial tables: deep gap analysis ----
    print("=" * 80)
    print("DATA GAP DIAGNOSIS REPORT")
    print("=" * 80)
    print()

    print("━" * 80)
    print("1. SPATIAL DATA COVERAGE (feature extraction will use these)")
    print("━" * 80)

    for table, spec in SPATIAL_TABLES.items():
        if table not in existing:
            print(f"\n  ✗ {table}: TABLE MISSING")
            report["spatial"][table] = {"status": "MISSING"}
            continue

        t_col = spec["time_col"]
        lat_col = spec["lat_col"]
        lon_col = spec["lon_col"]

        # Total rows
        cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
        total_rows = cursor.fetchone()[0]
        if total_rows == 0:
            print(f"\n  ✗ {table}: EMPTY")
            report["spatial"][table] = {"status": "EMPTY", "rows": 0}
            continue

        # Distinct dates
        cursor.execute(
            f"SELECT COUNT(DISTINCT DATE([{t_col}])) FROM [{table}]"
        )
        distinct_dates = cursor.fetchone()[0]

        # Date range
        cursor.execute(
            f"SELECT MIN(DATE([{t_col}])), MAX(DATE([{t_col}])) FROM [{table}]"
        )
        min_date_str, max_date_str = cursor.fetchone()

        # Year-by-year date counts
        cursor.execute(
            f"SELECT CAST(STRFTIME('%Y', [{t_col}]) AS INTEGER) AS yr, "
            f"COUNT(DISTINCT DATE([{t_col}])) AS n_dates, "
            f"COUNT(*) AS n_rows "
            f"FROM [{table}] "
            f"GROUP BY yr ORDER BY yr"
        )
        year_data = cursor.fetchall()

        # Distinct cells
        cursor.execute(
            f"SELECT COUNT(DISTINCT [{lat_col}] || ',' || [{lon_col}]) FROM [{table}]"
        )
        distinct_cells = cursor.fetchone()[0]

        # Cell coordinates
        cursor.execute(
            f"SELECT DISTINCT [{lat_col}], [{lon_col}] FROM [{table}]"
        )
        db_cells = set(cursor.fetchall())

        # Calculate expected date coverage (within analysis period)
        exp = spec["expected_range"]
        analysis_overlap_start = max(ANALYSIS_START, datetime(exp[0], 1, 1)) if exp else ANALYSIS_START
        analysis_overlap_end = min(ANALYSIS_END, datetime(exp[1], 12, 31)) if exp else ANALYSIS_END
        expected_span_days = (analysis_overlap_end - analysis_overlap_start).days + 1

        # Actual dates within analysis period
        cursor.execute(
            f"SELECT COUNT(DISTINCT DATE([{t_col}])) FROM [{table}] "
            f"WHERE DATE([{t_col}]) >= '2011-01-01' AND DATE([{t_col}]) <= '{ANALYSIS_END.strftime('%Y-%m-%d')}'"
        )
        dates_in_analysis = cursor.fetchone()[0]

        # Calculate real coverage
        if spec["temporal"] == "daily":
            real_coverage_pct = round(100 * dates_in_analysis / max(expected_span_days, 1), 1)
        elif spec["temporal"] == "weekly":
            expected_weeks = expected_span_days / 7
            real_coverage_pct = round(100 * dates_in_analysis / max(expected_weeks, 1), 1)
        elif spec["temporal"] == "monthly":
            expected_months = (analysis_overlap_end.year - analysis_overlap_start.year) * 12 + \
                              (analysis_overlap_end.month - analysis_overlap_start.month) + 1
            real_coverage_pct = round(100 * dates_in_analysis / max(expected_months, 1), 1)
        elif spec["temporal"] == "annual":
            expected_years = analysis_overlap_end.year - analysis_overlap_start.year + 1
            real_coverage_pct = round(100 * dates_in_analysis / max(expected_years, 1), 1)
        else:
            real_coverage_pct = None

        # Identify gap years (within analysis period)
        years_with_data = {row[0] for row in year_data}
        analysis_years = set(range(max(2011, exp[0] if exp else 2011),
                                   min(ANALYSIS_END.year + 1, (exp[1] if exp else 2026) + 1)))
        gap_years = sorted(analysis_years - years_with_data)

        # Print report
        print(f"\n  {'─' * 74}")
        print(f"  {table} ({spec['temporal']})")
        print(f"  {'─' * 74}")
        print(f"    Total: {total_rows:>10,} rows | {distinct_dates:>6,} distinct dates | {distinct_cells:>4} cells")
        print(f"    Range: {min_date_str} → {max_date_str}")
        print(f"    Analysis period coverage (2011-now): {dates_in_analysis} dates = {real_coverage_pct}%")

        if gap_years:
            # Group consecutive years for readability
            gap_ranges = _group_consecutive(gap_years)
            print(f"    ⚠️  GAP YEARS (no data): {_format_ranges(gap_ranges)}")

        # Year heatmap
        print(f"    Year breakdown:")
        for yr, n_dates, n_rows in year_data:
            bar = _bar(n_dates, 365 if spec["temporal"] == "daily" else
                       52 if spec["temporal"] == "weekly" else
                       12 if spec["temporal"] == "monthly" else 1)
            in_analysis = "✓" if 2011 <= yr <= ANALYSIS_END.year else " "
            print(f"      {in_analysis} {yr}: {n_dates:>4} dates ({n_rows:>8,} rows) {bar}")

        # Cell overlap with Japan grid
        japan_set = set(JAPAN_CELLS)
        overlap = db_cells & japan_set
        print(f"    Cell coverage: {len(db_cells)} unique cells, {len(overlap)} overlap with Japan 2° grid ({len(japan_set)} cells)")

        entry = {
            "status": "OK",
            "rows": total_rows,
            "distinct_dates": distinct_dates,
            "distinct_cells": distinct_cells,
            "date_range": [min_date_str, max_date_str],
            "analysis_period_dates": dates_in_analysis,
            "analysis_period_coverage_pct": real_coverage_pct,
            "gap_years": gap_years,
            "temporal": spec["temporal"],
            "year_breakdown": {str(yr): {"dates": nd, "rows": nr} for yr, nd, nr in year_data},
            "japan_grid_overlap": len(overlap),
        }
        report["spatial"][table] = entry

    # ---- 2. Time series tables ----
    print()
    print("━" * 80)
    print("2. TIME SERIES DATA COVERAGE")
    print("━" * 80)

    for table, spec in TIMESERIES_TABLES.items():
        if table not in existing:
            print(f"  ✗ {table}: MISSING")
            report["timeseries"][table] = {"status": "MISSING"}
            continue

        t_col = spec["time_col"]
        cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
        total_rows = cursor.fetchone()[0]
        if total_rows == 0:
            print(f"  ✗ {table}: EMPTY")
            report["timeseries"][table] = {"status": "EMPTY", "rows": 0}
            continue

        cursor.execute(
            f"SELECT COUNT(DISTINCT DATE([{t_col}])), "
            f"MIN(DATE([{t_col}])), MAX(DATE([{t_col}])) "
            f"FROM [{table}]"
        )
        distinct_dates, min_d, max_d = cursor.fetchone()

        # Year counts
        cursor.execute(
            f"SELECT CAST(STRFTIME('%Y', [{t_col}]) AS INTEGER) AS yr, "
            f"COUNT(DISTINCT DATE([{t_col}])) "
            f"FROM [{table}] GROUP BY yr ORDER BY yr"
        )
        year_data = cursor.fetchall()

        # Coverage within analysis period
        exp = spec.get("expected_range")
        if exp:
            cursor.execute(
                f"SELECT COUNT(DISTINCT DATE([{t_col}])) FROM [{table}] "
                f"WHERE DATE([{t_col}]) >= '{max(2011, exp[0])}-01-01' "
                f"AND DATE([{t_col}]) <= '{min(ANALYSIS_END.year, exp[1])}-12-31'"
            )
            dates_in_analysis = cursor.fetchone()[0]
            expected_days = (min(ANALYSIS_END, datetime(exp[1], 12, 31)) -
                             max(ANALYSIS_START, datetime(exp[0], 1, 1))).days + 1
            coverage_pct = round(100 * dates_in_analysis / max(expected_days, 1), 1)

            years_with_data = {row[0] for row in year_data}
            analysis_years = set(range(max(2011, exp[0]), min(ANALYSIS_END.year + 1, exp[1] + 1)))
            gap_years = sorted(analysis_years - years_with_data)
        else:
            dates_in_analysis = distinct_dates
            coverage_pct = None
            gap_years = []

        # Compact output for non-spatial tables
        gap_str = f" | GAPS: {_format_ranges(_group_consecutive(gap_years))}" if gap_years else ""
        cov_str = f" | analysis coverage: {coverage_pct}%" if coverage_pct is not None else ""
        print(f"  {'✓' if not gap_years else '⚠️'} {table}: {total_rows:>10,} rows | "
              f"{distinct_dates:>5} dates | {min_d} → {max_d}{cov_str}{gap_str}")

        report["timeseries"][table] = {
            "status": "OK",
            "rows": total_rows,
            "distinct_dates": distinct_dates,
            "date_range": [min_d, max_d],
            "analysis_period_coverage_pct": coverage_pct,
            "gap_years": gap_years,
        }

    # ---- 3. Summary: critical gaps for ML ----
    print()
    print("━" * 80)
    print("3. CRITICAL GAPS FOR ML (features that will be mostly zero)")
    print("━" * 80)

    critical_issues = []
    for table, entry in report["spatial"].items():
        if entry.get("status") in ("MISSING", "EMPTY"):
            critical_issues.append(f"  🔴 {table}: {entry['status']} — feature will be excluded")
            continue
        cov = entry.get("analysis_period_coverage_pct")
        gaps = entry.get("gap_years", [])
        if cov is not None and cov < 30:
            critical_issues.append(
                f"  🔴 {table}: only {cov}% analysis period coverage "
                f"({entry['analysis_period_dates']} dates) — feature will be mostly zero"
            )
        elif gaps:
            critical_issues.append(
                f"  🟡 {table}: missing years {_format_ranges(_group_consecutive(gaps))} "
                f"— {cov}% coverage"
            )
        overlap = entry.get("japan_grid_overlap", 0)
        if entry.get("status") == "OK" and overlap == 0:
            critical_issues.append(
                f"  🔴 {table}: 0 cells overlap with Japan 2° grid — coordinate mismatch?"
            )

    for table, entry in report["timeseries"].items():
        if entry.get("status") in ("MISSING", "EMPTY"):
            continue  # Not critical for individual features
        cov = entry.get("analysis_period_coverage_pct")
        if cov is not None and cov < 30:
            critical_issues.append(
                f"  🟡 {table}: only {cov}% analysis period coverage"
            )

    if critical_issues:
        for issue in critical_issues:
            print(issue)
    else:
        print("  ✅ No critical gaps detected")

    report["summary"]["critical_issues"] = critical_issues
    report["summary"]["n_spatial_tables"] = len(SPATIAL_TABLES)
    report["summary"]["n_timeseries_tables"] = len(TIMESERIES_TABLES)

    conn.close()
    print()
    print("=" * 80)

    return report


def _bar(value: int, max_val: int) -> str:
    """Visual bar with Unicode blocks."""
    pct = min(value / max(max_val, 1), 1.0)
    filled = int(pct * 20)
    return "█" * filled + "░" * (20 - filled) + f" {pct*100:.0f}%"


def _group_consecutive(years: list[int]) -> list[tuple[int, int]]:
    """Group consecutive years: [2014,2015,2016,2018] → [(2014,2016),(2018,2018)]"""
    if not years:
        return []
    ranges = []
    start = years[0]
    prev = years[0]
    for y in years[1:]:
        if y == prev + 1:
            prev = y
        else:
            ranges.append((start, prev))
            start = y
            prev = y
    ranges.append((start, prev))
    return ranges


def _format_ranges(ranges: list[tuple[int, int]]) -> str:
    """Format year ranges: [(2014,2016),(2018,2018)] → '2014-2016, 2018'"""
    parts = []
    for s, e in ranges:
        if s == e:
            parts.append(str(s))
        else:
            parts.append(f"{s}-{e}")
    return ", ".join(parts)


def write_ci_summary(report: dict):
    """Write GitHub Actions Job Summary."""
    github_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if not github_summary:
        return

    with open(github_summary, "a") as f:
        f.write("\n## Data Gap Diagnosis\n\n")

        # Critical issues
        issues = report.get("summary", {}).get("critical_issues", [])
        if issues:
            f.write("### Critical Gaps\n\n")
            for issue in issues:
                f.write(f"- {issue.strip()}\n")
            f.write("\n")

        # Spatial coverage table
        f.write("### Spatial Data Coverage\n\n")
        f.write("| Source | Dates | Cells | Coverage | Gap Years |\n")
        f.write("|---|---|---|---|---|\n")
        for table, entry in report.get("spatial", {}).items():
            if entry.get("status") in ("MISSING", "EMPTY"):
                f.write(f"| `{table}` | — | — | **{entry['status']}** | — |\n")
                continue
            dates = entry.get("analysis_period_dates", 0)
            cells = entry.get("distinct_cells", 0)
            cov = entry.get("analysis_period_coverage_pct", "?")
            gaps = entry.get("gap_years", [])
            gap_str = _format_ranges(_group_consecutive(gaps)) if gaps else "—"
            f.write(f"| `{table}` | {dates} | {cells} | {cov}% | {gap_str} |\n")
        f.write("\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Diagnose geohazard data gaps")
    parser.add_argument("--db", default=DB_PATH, help="Path to geohazard.db")
    parser.add_argument("--json", action="store_true", help="Output JSON report")
    parser.add_argument("--ci", action="store_true", help="Write GitHub Actions Job Summary")
    args = parser.parse_args()

    result = diagnose(args.db)

    if args.json:
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        out_path = results_dir / "data_gaps.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"JSON report saved to {out_path}")

    if args.ci:
        write_ci_summary(result)
