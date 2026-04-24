"""Run integrity check on a SQLite DB.

Modes:
    full  (default): PRAGMA integrity_check + per-table COUNT(*). ~107s on 9.9GB.
    quick:           PRAGMA quick_check only. ~15s on 9.9GB. Used by Restore step.

Exit 0 on ok, Exit 1 on any corruption or unreadable table.
Used by backfill.yml restore/init/snapshot steps.
"""

import argparse
import sqlite3
import sys


def check_quick(path: str) -> int:
    try:
        conn = sqlite3.connect(path)
    except Exception as e:
        print(f"  DB open failed: {e}")
        return 1
    try:
        r = conn.execute("PRAGMA quick_check").fetchone()[0]
        if r != "ok":
            print(f"  DB quick_check FAILED: {r[:200]}")
            return 1
        print("  DB quick_check OK")
        return 0
    finally:
        conn.close()


def check_full(path: str) -> int:
    try:
        conn = sqlite3.connect(path)
    except Exception as e:
        print(f"  DB open failed: {e}")
        return 1
    try:
        r = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if r != "ok":
            print(f"  DB integrity FAILED: {r[:200]}")
            return 1
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall():
            try:
                c = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                print(f"    {name}: {c:,} rows")
            except Exception as e:
                print(f"    {name}: unreadable ({e})")
                return 1
        print("  DB integrity OK")
        return 0
    finally:
        conn.close()


def main() -> int:
    p = argparse.ArgumentParser(description="SQLite DB integrity check")
    p.add_argument("path", nargs="?", default="data/geohazard.db")
    p.add_argument(
        "--mode",
        choices=["full", "quick"],
        default="full",
        help="quick=PRAGMA quick_check only (~15s), full=integrity_check + COUNT(*) (~107s)",
    )
    args = p.parse_args()
    if args.mode == "quick":
        return check_quick(args.path)
    return check_full(args.path)


if __name__ == "__main__":
    sys.exit(main())
