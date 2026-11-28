"""Run integrity check on a SQLite DB.

Modes:
    full  (default): PRAGMA integrity_check + per-table COUNT(*). ~107s on 9.9GB.
    quick:           PRAGMA quick_check only. ~15s on 9.9GB. Used by Restore step for
                     DBs up to 10 GB (own overlays).
    header:          structural check that finishes in seconds at any size: SQLite
                     header magic, page_count x page_size <= file size (catches a
                     truncated copy; extra trailing bytes only warn), schema readable, and every table's
                     first row and last rowid readable. Used by the Restore step for
                     the full checkpoint (43+ GB), where quick_check never finished
                     inside its 1200 s timeout and the copy was accepted unchecked.
                     It is NOT a page-level check: the full integrity_check still
                     gates every checkpoint when it is written (Snapshot DB, merge)
                     and weekly (weekly-integrity-audit.yml).

Exit 0 on ok, Exit 1 on any corruption or unreadable table.
Used by backfill.yml restore/init/snapshot steps.
"""

import argparse
import os
import sqlite3
import sys

SQLITE_MAGIC = b"SQLite format 3\x00"


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


def check_header(path: str) -> int:
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            head = f.read(100)
    except OSError as e:
        print(f"  DB header check: cannot read file: {e}")
        return 1
    if len(head) < 100 or head[:16] != SQLITE_MAGIC:
        print("  DB header check FAILED: not an SQLite 3 file (bad magic)")
        return 1
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except Exception as e:
        print(f"  DB open failed: {e}")
        return 1
    try:
        ps = conn.execute("PRAGMA page_size").fetchone()[0]
        pc = conn.execute("PRAGMA page_count").fetchone()[0]
        if pc * ps > size:
            print(f"  DB header check FAILED: truncated, page_count*page_size={pc * ps} > file size {size}")
            return 1
        if pc * ps < size:
            # SQLite ignores bytes past the in-header size; not corruption, but say so.
            print(f"  DB header check: warning, {size - pc * ps} bytes past page_count*page_size={pc * ps}")
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        if not tables:
            print("  DB header check FAILED: no tables in schema")
            return 1
        for name in tables:
            q = '"' + name.replace('"', '""') + '"'
            conn.execute(f"SELECT * FROM {q} LIMIT 1").fetchall()
            try:
                conn.execute(f"SELECT max(rowid) FROM {q}").fetchone()
            except sqlite3.OperationalError as e:
                if "rowid" not in str(e):   # WITHOUT ROWID tables have no rowid; anything else is real
                    raise
        print(f"  DB header check OK ({len(tables)} tables, {pc:,} pages x {ps} B)")
        return 0
    except sqlite3.DatabaseError as e:
        print(f"  DB header check FAILED: {e}")
        return 1
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
        choices=["full", "quick", "header"],
        default="full",
        help="quick=PRAGMA quick_check only (~15s), full=integrity_check + COUNT(*) (~107s), "
        "header=structural check in seconds at any size (see module docstring)",
    )
    args = p.parse_args()
    if args.mode == "quick":
        return check_quick(args.path)
    if args.mode == "header":
        return check_header(args.path)
    return check_full(args.path)


if __name__ == "__main__":
    sys.exit(main())
