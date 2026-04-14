"""Per-table salvage of a corrupt SQLite checkpoint.

Strategy: for each table in the corrupt source, (a) create its schema in a
fresh destination DB, (b) ATTACH the destination from the source connection,
(c) INSERT OR IGNORE INTO new.<table> SELECT * FROM main.<table>. Tables whose
B-tree contains the corrupt pages will fail; they are skipped and logged.

Rationale:
    Ubuntu runners ship sqlite3 without SQLITE_ENABLE_DBPAGE_VTAB, so the
    CLI `.recover` command fails immediately with
    "sql error: no such table: sqlite_dbpage". Debug workflow run
    24421917797 proved per-table ATTACH+INSERT recovers 35/37 tables
    from a fully corrupt 4GB artifact in ~20 seconds (integrity_check=ok).

Tables to skip (known corruption origin — let backfill re-fetch):
    so2_column, cloud_fraction

Usage:
    python3 scripts/salvage_db.py --src corrupt.db --dst salvaged.db

Exit code:
    0 if destination DB exists and passes integrity_check with >0 tables
    1 otherwise
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys

SKIP_TABLES = {"so2_column", "cloud_fraction"}


def salvage(src: str, dst: str) -> bool:
    if not os.path.exists(src):
        print(f"source DB missing: {src}")
        return False
    if os.path.exists(dst):
        os.remove(dst)

    # Step 1: read schemas from corrupt source. sqlite_master is usually
    # readable even when data B-trees are broken. We pull tables, indexes,
    # and triggers so the salvaged DB has the full DDL — otherwise indexes
    # would silently drop, since downstream init_db() only runs when
    # integrity fails (which the salvaged DB won't).
    src_meta = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    table_rows = src_meta.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    aux_rows = src_meta.execute(
        "SELECT name, sql, type FROM sqlite_master "
        "WHERE type IN ('index','trigger','view') "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    src_meta.close()

    if not table_rows:
        print("no tables in source DB")
        return False

    # Step 2: create fresh destination DB with table schemas applied.
    dst_conn = sqlite3.connect(dst)
    tables_to_copy: list[str] = []
    for name, schema in table_rows:
        if name in SKIP_TABLES:
            print(f"  {name}: SKIP (known corruption origin)")
            continue
        if not schema:
            print(f"  {name}: no schema — skip")
            continue
        try:
            dst_conn.execute(schema)
            tables_to_copy.append(name)
        except sqlite3.Error as e:
            print(f"  {name}: schema creation failed: {e}")
    dst_conn.commit()
    dst_conn.close()

    # Step 3: from source, ATTACH destination and per-table INSERT OR IGNORE.
    # Note: src opened read-write (default) because ATTACH for write-target
    # requires the main connection to allow writes. We never INSERT into
    # main — all writes go to attached "new".
    src_conn = sqlite3.connect(src)
    src_conn.execute("ATTACH DATABASE ? AS new", (dst,))

    ok_tables: list[tuple[str, int]] = []
    failed_tables: list[tuple[str, str]] = []

    for name in tables_to_copy:
        try:
            src_conn.execute(
                f'INSERT OR IGNORE INTO new."{name}" '
                f'SELECT * FROM main."{name}"'
            )
            src_conn.commit()
            n = src_conn.execute(
                f'SELECT COUNT(*) FROM new."{name}"'
            ).fetchone()[0]
            print(f"  {name}: {n:,} rows")
            ok_tables.append((name, n))
        except sqlite3.DatabaseError as e:
            print(f"  {name}: FAILED ({e})")
            failed_tables.append((name, str(e)))
            try:
                src_conn.rollback()
            except sqlite3.Error:
                pass

    src_conn.execute("DETACH DATABASE new")
    src_conn.close()

    # Step 4: recreate indexes, triggers, views on destination. Deferred
    # until after INSERTs so bulk copy doesn't pay index-maintenance cost
    # per row. All schemas are `CREATE ... IF NOT EXISTS`-style in the
    # project, so re-running against init_db() later is idempotent.
    dst_conn = sqlite3.connect(dst)
    aux_created = 0
    for name, schema, kind in aux_rows:
        if not schema:
            continue
        # Skip auxiliary objects that reference skipped tables.
        if any(t in schema for t in SKIP_TABLES):
            print(f"  {kind} {name}: skip (references SKIP_TABLES)")
            continue
        try:
            dst_conn.execute(schema)
            aux_created += 1
        except sqlite3.Error as e:
            print(f"  {kind} {name}: {e}")
    dst_conn.commit()
    dst_conn.close()
    if aux_created:
        print(f"  aux DDL recreated: {aux_created} objects")

    # Step 5: verify destination integrity.
    verify = sqlite3.connect(dst)
    try:
        integrity = verify.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        verify.close()

    size_mb = os.path.getsize(dst) / 1024 / 1024
    total_rows = sum(n for _, n in ok_tables)
    print()
    print("=== salvage summary ===")
    print(f"  dst size:        {size_mb:.1f} MB")
    print(f"  OK tables:       {len(ok_tables)} ({total_rows:,} rows)")
    print(f"  failed tables:   {len(failed_tables)}")
    for n, err in failed_tables:
        print(f"    - {n}: {err[:100]}")
    print(f"  integrity_check: {integrity}")

    return integrity == "ok" and len(ok_tables) > 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--src", required=True)
    p.add_argument("--dst", required=True)
    args = p.parse_args()
    return 0 if salvage(args.src, args.dst) else 1


if __name__ == "__main__":
    sys.exit(main())
