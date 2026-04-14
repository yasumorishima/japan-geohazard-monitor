"""Run PRAGMA integrity_check on a SQLite DB and print per-table row counts.

Exit 0 if integrity_check returns 'ok' AND all tables are readable.
Exit 1 on any corruption or unreadable table.
Used by backfill.yml restore/init steps.
"""

import sqlite3
import sys


def main(path: str) -> int:
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


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "data/geohazard.db"
    sys.exit(main(path))
