"""Extract specific tables from a SQLite source DB into a fresh, compact overlay DB.

Used by per-fetch GHA jobs in .github/workflows/backfill.yml to upload only the
table(s) each job owns, rather than the full ~1.87 GB geohazard.db. The merge
job's `scripts/merge_checkpoints.py --overlay PATH:TABLES` already only reads
the listed tables from each artifact, so the rest of the DB in the upload is
pure waste.

Behaviour:
    - Creates the destination DB fresh (overwrites if present).
    - For each table in --tables, copies CREATE TABLE schema verbatim, then
      copies all rows via ATTACH DATABASE + INSERT INTO ... SELECT.
    - Indexes are NOT copied: merge_checkpoints.py never queries the overlay
      with WHERE clauses that benefit from them, only `INSERT OR IGNORE INTO
      base SELECT * FROM overlay.<table>`. Skipping indexes shrinks artifact
      size with zero merge-step impact.
    - VACUUM at the end ensures freelist pages are reclaimed and file size
      reflects only used pages.

Why a fresh dst (not a cp + DROP):
    Copying then dropping leaves freelist pages until VACUUM, and on the way
    the 1.87 GB cp itself is the cost we are trying to eliminate. Building
    fresh from ATTACH + INSERT avoids both.

If a listed table is missing in src (e.g. fetcher skipped or first run),
the script logs a warning and proceeds — merge_checkpoints.py tolerates
overlays missing their listed tables (treated as "no overlay rows", base
rows for that table survive untouched).

Tested via `scripts/smoke_test_extract_overlay.py`.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys


def extract(src_path: str, dst_path: str, tables: list[str]) -> None:
    if not os.path.exists(src_path):
        print(f"ERROR: src not found: {src_path}", file=sys.stderr)
        sys.exit(1)

    if os.path.exists(dst_path):
        os.remove(dst_path)

    src_size_mb = os.path.getsize(src_path) / (1024 * 1024)
    print(f"src: {src_path} ({src_size_mb:.1f} MB)")
    print(f"dst: {dst_path}")
    print(f"tables: {tables}")

    dst = sqlite3.connect(dst_path)
    dst.execute("PRAGMA journal_mode = MEMORY")
    dst.execute(f"ATTACH DATABASE ? AS src", (src_path,))

    for table in tables:
        row = dst.execute(
            "SELECT sql FROM src.sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None or row[0] is None:
            print(f"WARN: table {table!r} not present in src (skipping)")
            continue
        create_sql = row[0]
        dst.execute(create_sql)
        n = dst.execute(
            f"SELECT COUNT(*) FROM src.{table}"
        ).fetchone()[0]
        if n > 0:
            dst.execute(f"INSERT INTO main.{table} SELECT * FROM src.{table}")
        dst.commit()
        print(f"  {table}: {n} rows copied")

    dst.execute("DETACH DATABASE src")
    dst.execute("VACUUM")
    dst.close()

    dst_size_mb = os.path.getsize(dst_path) / (1024 * 1024)
    ratio = src_size_mb / dst_size_mb if dst_size_mb > 0 else float("inf")
    print(f"done: {dst_size_mb:.1f} MB ({ratio:.1f}x smaller)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--src", required=True, help="source DB path (e.g. data/geohazard.db)")
    ap.add_argument("--dst", required=True, help="destination overlay DB path")
    ap.add_argument(
        "--tables",
        required=True,
        help="comma-separated list of table names to extract",
    )
    args = ap.parse_args()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not tables:
        print("ERROR: --tables produced empty list", file=sys.stderr)
        sys.exit(1)
    extract(args.src, args.dst, tables)


if __name__ == "__main__":
    main()
