"""Merge two SQLite checkpoint DBs from parallel heavy/light backfill jobs.

Strategy:
    1. Copy `--base` to `--dst` verbatim (light job DB, contains majority of
       tables and the previous-run snapshot of heavy-owned tables too).
    2. ATTACH `--overlay` (heavy job DB) and, for each table listed in
       `--overlay-tables`, DELETE the base rows then INSERT OR IGNORE from
       overlay. This guarantees heavy-owned tables reflect the heavy job's
       latest fetch and never carry stale base rows forward.
    3. Run integrity_check on `--dst` and refuse to declare success unless
       it returns "ok" with at least one table.

Why DELETE-then-INSERT instead of plain INSERT OR IGNORE for overlay tables:
    Both jobs restore the same prior checkpoint, so the base DB already has
    the previous-cron snapshot of heavy-owned tables. A plain INSERT OR
    IGNORE would leave those stale rows untouched and only append the
    incremental new rows from heavy. Wiping the base rows for overlay
    tables and re-inserting from overlay yields the same row set the
    heavy job would have produced if it had run alone.

Tolerant modes:
    - If `--base` is missing and `--require-base` is set, exit 1.
    - If `--base` is missing without --require-base, treat `--overlay` as base
      (no overlay step). NOTE: this loses every light-owned table, so
      WRITE_TRUNCATE BQ upload would erase BQ accumulation. CI must pass
      --require-base.
    - If `--overlay` is missing, copy `--base` to `--dst` and succeed.
    - If both are missing, exit 1.

Triggers:
    The geohazard schema defines no triggers (verified via repo-wide grep).
    DELETE on overlay tables therefore cannot fire trigger side effects.
    If a trigger is later added, revisit the `BEGIN; DELETE; INSERT; COMMIT;`
    transaction below — it currently relies on no implicit cascades.

WAL mode:
    backfill.yml uses `src.backup(dst)` for snapshot creation, which flushes
    the WAL into the main DB file. Both base and overlay arriving here are
    therefore standalone files with no -wal/-shm sidecars to worry about.

Usage:
    python3 scripts/merge_checkpoints.py \
        --base /tmp/light/geohazard.db \
        --overlay /tmp/heavy/geohazard.db \
        --overlay-tables modis_lst,so2_column,cloud_fraction,snet_waveform,snet_pressure \
        --dst data/geohazard.db \
        --require-base

Exit code:
    0 if dst exists and passes integrity_check with >0 tables
    1 otherwise
"""

from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import sys


def _verify(dst: str) -> bool:
    conn = sqlite3.connect(dst)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        n_tables = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchone()[0]
    finally:
        conn.close()
    size_mb = os.path.getsize(dst) / 1024 / 1024
    print(f"  dst size:        {size_mb:.1f} MB")
    print(f"  table count:     {n_tables}")
    print(f"  integrity_check: {integrity}")
    return integrity == "ok" and n_tables > 0


def merge(
    base: str,
    overlay: str,
    overlay_tables: list[str],
    dst: str,
    require_base: bool = False,
) -> bool:
    base_exists = bool(base) and os.path.exists(base)
    overlay_exists = bool(overlay) and os.path.exists(overlay)

    if not base_exists and not overlay_exists:
        print("both base and overlay are missing")
        return False

    if not base_exists and require_base:
        print(
            "base missing and --require-base set; refusing to promote overlay "
            "(would erase light-owned tables and zero them in BQ via "
            "WRITE_TRUNCATE)"
        )
        return False

    if os.path.exists(dst):
        os.remove(dst)
    os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)

    if not base_exists:
        # Heavy-only: promote overlay to dst as-is. No overlay step needed
        # because the only DB we have is already "the overlay". This path
        # is only reachable when require_base is False — caller has opted
        # in to losing light-owned tables.
        print(f"base missing, promoting overlay {overlay} to dst")
        shutil.copyfile(overlay, dst)
        print()
        print("=== merge summary (overlay-only) ===")
        return _verify(dst)

    print(f"copying base {base} -> {dst}")
    shutil.copyfile(base, dst)

    if not overlay_exists:
        print("overlay missing, dst is base verbatim")
        print()
        print("=== merge summary (base-only) ===")
        return _verify(dst)

    if not overlay_tables:
        print("--overlay-tables empty, dst is base verbatim")
        print()
        print("=== merge summary (no overlay tables) ===")
        return _verify(dst)

    # Overlay step: ATTACH overlay, DELETE then INSERT OR IGNORE per table.
    # IGNORE guards against UNIQUE conflicts that could appear if the same
    # row was somehow present in both (shouldn't happen post-DELETE, but
    # cheap insurance).
    #
    # isolation_level=None puts the connection in autocommit mode so we can
    # issue explicit BEGIN/COMMIT/ROLLBACK per table. This matters because
    # DELETE-then-INSERT must be atomic — if INSERT fails after DELETE
    # succeeds (e.g. schema mismatch), Python's implicit-transaction default
    # would leave the table empty after an automatic commit on the next
    # statement.
    conn = sqlite3.connect(dst, isolation_level=None)
    conn.execute("ATTACH DATABASE ? AS overlay", (overlay,))

    overlay_table_names = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM overlay.sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }
    base_table_names = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM main.sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }

    applied: list[tuple[str, int, int]] = []
    skipped: list[tuple[str, str]] = []

    for name in overlay_tables:
        # Defensive: reject identifiers containing a double-quote so that the
        # `"{name}"` interpolation below cannot escape into SQL injection.
        # Real callers pass a fixed list from CI, but a typo or future
        # refactor shouldn't open this path.
        if '"' in name:
            skipped.append((name, "table name contains double-quote"))
            continue
        if name not in overlay_table_names:
            skipped.append((name, "missing in overlay"))
            continue
        if name not in base_table_names:
            # Heavy-only table that base never created — copy schema first.
            schema = conn.execute(
                "SELECT sql FROM overlay.sqlite_master "
                "WHERE type='table' AND name=?",
                (name,),
            ).fetchone()
            if not schema or not schema[0]:
                skipped.append((name, "no schema in overlay"))
                continue
            try:
                conn.execute(schema[0])
            except sqlite3.Error as e:
                skipped.append((name, f"schema create failed: {e}"))
                continue

        try:
            before = conn.execute(
                f'SELECT COUNT(*) FROM main."{name}"'
            ).fetchone()[0]
            conn.execute("BEGIN")
            conn.execute(f'DELETE FROM main."{name}"')
            conn.execute(
                f'INSERT OR IGNORE INTO main."{name}" '
                f'SELECT * FROM overlay."{name}"'
            )
            conn.execute("COMMIT")
            after = conn.execute(
                f'SELECT COUNT(*) FROM main."{name}"'
            ).fetchone()[0]
            print(f"  {name}: {before:,} -> {after:,} rows (overlay applied)")
            applied.append((name, before, after))
        except sqlite3.DatabaseError as e:
            print(f"  {name}: FAILED ({e})")
            skipped.append((name, str(e)))
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass

    conn.execute("DETACH DATABASE overlay")
    conn.close()

    print()
    print("=== merge summary ===")
    print(f"  overlay tables applied: {len(applied)}")
    for name, before, after in applied:
        delta = after - before
        sign = "+" if delta >= 0 else ""
        print(f"    - {name}: {before:,} -> {after:,} ({sign}{delta:,})")
    if skipped:
        print(f"  overlay tables skipped: {len(skipped)}")
        for name, reason in skipped:
            print(f"    - {name}: {reason[:100]}")
    return _verify(dst)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True, help="base DB path (light job)")
    p.add_argument("--overlay", required=True, help="overlay DB path (heavy job)")
    p.add_argument(
        "--overlay-tables",
        required=True,
        help="comma-separated table names whose rows are taken from overlay",
    )
    p.add_argument("--dst", required=True, help="destination DB path")
    p.add_argument(
        "--require-base",
        action="store_true",
        help=(
            "fail if base is missing (recommended for CI; without this flag "
            "a missing base causes overlay to be promoted, erasing "
            "light-owned tables)"
        ),
    )
    args = p.parse_args()
    overlay_tables = [t.strip() for t in args.overlay_tables.split(",") if t.strip()]
    return 0 if merge(
        args.base, args.overlay, overlay_tables, args.dst, args.require_base
    ) else 1


if __name__ == "__main__":
    sys.exit(main())
