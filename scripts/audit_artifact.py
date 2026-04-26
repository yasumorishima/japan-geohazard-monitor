"""Per-table corruption probe for a backfill checkpoint SQLite DB.

Existing validate_data.py / diagnose_data_gaps.py assume a healthy DB.
This script survives corruption by probing each table independently and
recording which tables are readable, which are corrupt, and what the
row / date coverage is on readable tables.

Output:
  results/audit_{label}.json — machine-readable per-table report
  stdout — human summary
  $GITHUB_STEP_SUMMARY — markdown table (if set)

Usage:
  python scripts/audit_artifact.py --db PATH --label latest
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

# Ensure UTF-8 output on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Tables + their actual time column (verified against src/db.py and scripts/fetch_*.py
# CREATE TABLE statements on 2026-04-14). NOTE: validate_data.py and diagnose_data_gaps.py
# have inaccurate time_col entries for earthquakes/focal_mechanisms/tec/geomag_kp/
# modis_lst/snet_waveform/gnss_tec — do NOT copy from them blindly.
EXPECTED = {
    # Overrides vs observed_at default
    "earthquakes":       "occurred_at",
    "focal_mechanisms":  "occurred_at",
    "tec":               "epoch",
    "gnss_tec":          "epoch",
    "geomag_kp":         "time_tag",
    "modis_lst":         "observed_date",
    "snet_waveform":     "date_str",
    "fnet_waveform":     "date_str",
    # Default: observed_at
    "ulf_magnetic":      "observed_at",
    "cosmic_ray":        "observed_at",
    "iss_lis_lightning": "observed_at",
    "geomag_hourly":     "observed_at",
    "swarm_em":          "observed_at",
    "olr":               "observed_at",
    "earth_rotation":    "observed_at",
    "solar_wind":        "observed_at",
    "gravity_mascon":    "observed_at",
    "so2_column":        "observed_at",
    "soil_moisture":     "observed_at",
    "cloud_fraction":    "observed_at",
    "nightlight":        "observed_at",
    "tide_gauge":        "observed_at",
    "ocean_color":       "observed_at",
    "goes_xray":         "observed_at",
    "goes_proton":       "observed_at",
    "tidal_stress":      "observed_at",
    "particle_flux":     "observed_at",
    "dart_pressure":     "observed_at",
    "ioc_sea_level":     "observed_at",
}


def probe_db(db_path: str) -> dict:
    size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
    report: dict = {
        "db_path": db_path,
        "db_size_bytes": size,
        "db_size_mb": round(size / 1024 / 1024, 1),
        "integrity_check": None,
        "integrity_ok": False,
        "existing_tables": [],
        "tables": {},
    }
    if size == 0:
        report["error"] = "file missing or empty"
        return report

    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        report["error"] = f"open failed: {e}"
        return report

    try:
        return _probe_with_conn(conn, report)
    finally:
        conn.close()


def _probe_with_conn(conn: sqlite3.Connection, report: dict) -> dict:
    # Overall integrity (may return very long multi-line error; keep first 500 chars)
    try:
        r = conn.execute("PRAGMA integrity_check").fetchone()[0]
        report["integrity_check"] = r[:500]
        report["integrity_ok"] = r == "ok"
    except Exception as e:
        report["integrity_check"] = f"<raised: {e}>"

    # Enumerate tables that exist
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        report["existing_tables"] = [r[0] for r in rows]
    except Exception as e:
        report["enum_tables_error"] = str(e)

    # Probe each expected table independently
    for table, time_col in EXPECTED.items():
        entry: dict = {"exists": table in report["existing_tables"]}
        if not entry["exists"]:
            entry["status"] = "MISSING"
            report["tables"][table] = entry
            continue

        # COUNT(*) — survives most corruption unless table pages themselves broken
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            entry["row_count"] = n
        except sqlite3.DatabaseError as e:
            entry["status"] = "CORRUPT"
            entry["error"] = str(e)[:200]
            report["tables"][table] = entry
            continue
        except Exception as e:
            entry["status"] = "ERROR"
            entry["error"] = str(e)[:200]
            report["tables"][table] = entry
            continue

        if n == 0:
            entry["status"] = "EMPTY"
            report["tables"][table] = entry
            continue

        # Date range + distinct dates (if time_col readable)
        try:
            row = conn.execute(
                f"SELECT MIN({time_col}), MAX({time_col}), "
                f"COUNT(DISTINCT substr({time_col}, 1, 10)) "
                f"FROM {table}"
            ).fetchone()
            entry["min_time"] = row[0]
            entry["max_time"] = row[1]
            entry["distinct_days"] = row[2]
            entry["status"] = "OK"
        except sqlite3.DatabaseError as e:
            entry["status"] = "PARTIAL_CORRUPT"
            entry["error"] = str(e)[:200]
        except Exception as e:
            entry["status"] = "TIME_COL_ERROR"
            entry["error"] = str(e)[:200]

        report["tables"][table] = entry

    # Summary counts
    by_status: dict[str, int] = {}
    total_rows = 0
    for t in report["tables"].values():
        st = t.get("status", "?")
        by_status[st] = by_status.get(st, 0) + 1
        total_rows += t.get("row_count", 0) or 0
    report["summary"] = {
        "by_status": by_status,
        "total_rows": total_rows,
        "total_expected_tables": len(EXPECTED),
    }
    return report


def write_markdown(report: dict, label: str, out) -> None:
    out.write(f"## Audit: {label}\n\n")
    out.write(
        f"- DB size: **{report['db_size_mb']} MB**\n"
        f"- Integrity: **{'✅ OK' if report.get('integrity_ok') else '❌ FAILED'}**\n"
        f"- Tables present: {len(report.get('existing_tables', []))} / {report['summary']['total_expected_tables']} expected\n"
        f"- Total rows (readable): {report['summary']['total_rows']:,}\n"
        f"- Status breakdown: {report['summary']['by_status']}\n\n"
    )
    if not report.get("integrity_ok"):
        snippet = (report.get("integrity_check") or "")[:300]
        out.write(f"<details><summary>integrity_check output</summary>\n\n```\n{snippet}\n```\n\n</details>\n\n")

    out.write("### Per-table\n\n")
    out.write("| Table | Status | Rows | Days | Min | Max |\n")
    out.write("|---|---|---:|---:|---|---|\n")
    for table, entry in report["tables"].items():
        st = entry.get("status", "?")
        icon = {
            "OK": "✅",
            "EMPTY": "⚪",
            "MISSING": "⚫",
            "CORRUPT": "🔴",
            "PARTIAL_CORRUPT": "🟠",
            "ERROR": "🟡",
            "TIME_COL_ERROR": "🟡",
        }.get(st, "❓")
        rows = entry.get("row_count")
        days = entry.get("distinct_days")
        mn = entry.get("min_time", "")
        mx = entry.get("max_time", "")
        out.write(
            f"| `{table}` | {icon} {st} | "
            f"{rows if rows is not None else '—'} | "
            f"{days if days is not None else '—'} | "
            f"{mn or '—'} | {mx or '—'} |\n"
        )
    out.write("\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--label", default="audit")
    ap.add_argument("--out", default=None, help="JSON output path (default: results/audit_{label}.json)")
    args = ap.parse_args()

    report = probe_db(args.db)

    out_path = Path(args.out) if args.out else Path("results") / f"audit_{args.label}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"JSON report saved to {out_path}")

    # Human summary to stdout
    write_markdown(report, args.label, sys.stdout)

    # GitHub Actions job summary
    gh_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh_summary:
        with open(gh_summary, "a", encoding="utf-8") as f:
            write_markdown(report, args.label, f)

    return 0


if __name__ == "__main__":
    sys.exit(main())
