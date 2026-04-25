"""Merge N SQLite checkpoint DBs from parallel backfill fetch jobs.

Strategy:
    1. Copy `--base` to `--dst` verbatim (light job DB, contains majority of
       tables and the previous-run snapshot of every owned table from
       other jobs too, since every job restored the same prior checkpoint).
    2. For each `--overlay PATH:TABLES` arg, ATTACH the overlay DB and, per
       listed table, DELETE base rows then INSERT OR IGNORE from overlay.
       This guarantees overlay-owned tables reflect the job's latest fetch
       and never carry stale base rows forward.
    3. Missing overlay files are tolerated (their tables are left as-is in
       base — matches "fetch job skipped / cancelled" semantics). Their
       prior-checkpoint rows survive because base was restored from the
       same checkpoint.
    4. Run integrity_check on `--dst` and refuse to declare success unless
       it returns "ok" with at least one table.

Why DELETE-then-INSERT instead of plain INSERT OR IGNORE for overlay tables:
    Every job restores the same prior checkpoint, so the base DB already has
    the previous-cron snapshot of overlay-owned tables. A plain INSERT OR
    IGNORE would leave those stale rows untouched and only append the
    incremental new rows from the overlay. Wiping the base rows for overlay
    tables and re-inserting from overlay yields the same row set the overlay
    job would have produced if it had run alone.

Why missing overlay is safe:
    If a fetch job was skipped (target-not-this-job) or cancelled (5h
    timeout, runner failure), its overlay file is absent. Leaving base's
    checkpoint-derived rows in place means the merged DB matches the prior
    checkpoint for those tables — the same state BQ already has. No
    WRITE_TRUNCATE regression.

Tolerant modes:
    - If `--base` is missing and `--require-base` is set, exit 1.
    - If `--base` is missing without --require-base, the first overlay is
      promoted to base (no overlay step for that overlay's tables). Only
      reachable when caller accepts losing light-owned tables. CI must
      always pass --require-base.
    - If base is present but every overlay is missing, dst is base verbatim.

Triggers:
    The geohazard schema defines no triggers (verified via repo-wide grep).
    DELETE on overlay tables therefore cannot fire trigger side effects.
    If a trigger is later added, revisit the `BEGIN; DELETE; INSERT; COMMIT;`
    transaction below — it currently relies on no implicit cascades.

WAL mode:
    backfill.yml uses `src.backup(dst)` for snapshot creation, which flushes
    the WAL into the main DB file. Both base and overlays arriving here are
    therefore standalone files with no -wal/-shm sidecars to worry about.

Usage:
    python3 scripts/merge_checkpoints.py \
        --base /tmp/light/geohazard.db \
        --overlay /tmp/modis/geohazard.db:modis_lst \
        --overlay /tmp/so2/geohazard.db:so2_column \
        --overlay /tmp/cloud/geohazard.db:cloud_fraction \
        --overlay /tmp/snet/geohazard.db:snet_waveform \
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


def _apply_overlay(
    dst_conn: sqlite3.Connection,
    overlay_path: str,
    overlay_tables: list[str],
    base_table_names: set[str],
) -> tuple[list[tuple[str, int, int]], list[tuple[str, str]]]:
    """Apply one overlay to the already-opened dst connection.

    Returns (applied, skipped) per-table outcomes.
    """
    applied: list[tuple[str, int, int]] = []
    skipped: list[tuple[str, str]] = []

    dst_conn.execute("ATTACH DATABASE ? AS overlay", (overlay_path,))
    try:
        overlay_table_names = {
            row[0]
            for row in dst_conn.execute(
                "SELECT name FROM overlay.sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }

        for name in overlay_tables:
            # Reject identifiers with double-quote so `"{name}"` can't inject.
            if '"' in name:
                skipped.append((name, "table name contains double-quote"))
                continue
            if name not in overlay_table_names:
                skipped.append((name, "missing in overlay"))
                continue
            if name not in base_table_names:
                # Overlay-only table that base never created — copy schema.
                schema = dst_conn.execute(
                    "SELECT sql FROM overlay.sqlite_master "
                    "WHERE type='table' AND name=?",
                    (name,),
                ).fetchone()
                if not schema or not schema[0]:
                    skipped.append((name, "no schema in overlay"))
                    continue
                try:
                    dst_conn.execute(schema[0])
                    base_table_names.add(name)
                except sqlite3.Error as e:
                    skipped.append((name, f"schema create failed: {e}"))
                    continue

            try:
                before = dst_conn.execute(
                    f'SELECT COUNT(*) FROM main."{name}"'
                ).fetchone()[0]
                overlay_count = dst_conn.execute(
                    f'SELECT COUNT(*) FROM overlay."{name}"'
                ).fetchone()[0]

                # Refuse to wipe base rows when overlay has fewer rows than
                # base. All fetchers are append-only: each run restores the
                # prior checkpoint, then INSERT-OR-IGNOREs new rows. So a
                # successful run yields overlay_count >= before. Shrink
                # means restore failed (fetcher started from empty DB and
                # only wrote new fetches) or init_db reset the table.
                # Observed regressions:
                #   - cloud_fraction 523K -> 0 on 2026-04-18 (overlay empty)
                #   - modis_lst 488 -> 338 -> ... shrink chain on 2026-04-25
                # Keep base in either case; rely on the next successful run
                # to grow past base.
                if overlay_count < before:
                    deficit = before - overlay_count
                    if overlay_count == 0:
                        why = "overlay empty"
                    else:
                        why = f"overlay shrunk by {deficit:,} rows"
                    print(
                        f"  {name}: overlay has {overlay_count:,} rows but "
                        f"base has {before:,} rows ({why}) -- KEEPING base "
                        f"(append-only fetchers should never produce a "
                        f"smaller overlay; likely cause: checkpoint restore "
                        f"failure or fetcher init_db reset)"
                    )
                    skipped.append(
                        (name, f"{why}; base preserved ({before:,} rows)")
                    )
                    continue

                dst_conn.execute("BEGIN")
                dst_conn.execute(f'DELETE FROM main."{name}"')
                dst_conn.execute(
                    f'INSERT OR IGNORE INTO main."{name}" '
                    f'SELECT * FROM overlay."{name}"'
                )
                dst_conn.execute("COMMIT")
                after = dst_conn.execute(
                    f'SELECT COUNT(*) FROM main."{name}"'
                ).fetchone()[0]
                print(
                    f"  {name}: {before:,} -> {after:,} rows (overlay applied "
                    f"from {overlay_path})"
                )
                applied.append((name, before, after))
            except sqlite3.DatabaseError as e:
                print(f"  {name}: FAILED ({e})")
                skipped.append((name, str(e)))
                try:
                    dst_conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
    finally:
        dst_conn.execute("DETACH DATABASE overlay")
    return applied, skipped


def merge(
    base: str,
    overlays: list[tuple[str, list[str]]],
    dst: str,
    require_base: bool = False,
) -> bool:
    """Merge base + N overlays into dst.

    overlays: list of (path, [tables]) pairs. Missing files are tolerated.
    """
    base_exists = bool(base) and os.path.exists(base)
    present_overlays = [(p, t) for p, t in overlays if p and os.path.exists(p)]
    missing_overlays = [(p, t) for p, t in overlays if p and not os.path.exists(p)]

    for path, tables in missing_overlays:
        print(
            f"overlay missing: {path} (tables {tables} left as-is in base -- "
            "prior-checkpoint rows retained)"
        )

    if not base_exists and not present_overlays:
        print("base missing and no overlays present")
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

    # Seed dst from whichever DB we can.
    if base_exists:
        print(f"copying base {base} -> {dst}")
        shutil.copyfile(base, dst)
        seeded_from = "base"
    else:
        # Not reachable in CI (caller passes --require-base). Promote first
        # overlay so dst is at least a valid DB for its owned tables.
        first_path, _ = present_overlays[0]
        print(f"base missing, promoting first overlay {first_path} to dst")
        shutil.copyfile(first_path, dst)
        present_overlays = present_overlays[1:]
        seeded_from = f"overlay {first_path}"

    if not present_overlays:
        print(f"no overlays to apply, dst is {seeded_from} verbatim")
        print()
        print(f"=== merge summary ({seeded_from}-only) ===")
        return _verify(dst)

    # Overlay step: ATTACH overlay, DELETE then INSERT OR IGNORE per table.
    # isolation_level=None enables explicit BEGIN/COMMIT/ROLLBACK per table
    # — DELETE-then-INSERT must be atomic.
    conn = sqlite3.connect(dst, isolation_level=None)
    try:
        base_table_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM main.sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }

        all_applied: list[tuple[str, int, int]] = []
        all_skipped: list[tuple[str, str]] = []

        for path, tables in present_overlays:
            if not tables:
                print(f"skipping {path}: empty table list")
                continue
            print(f"applying overlay {path} for tables {tables}")
            applied, skipped = _apply_overlay(
                conn, path, tables, base_table_names
            )
            all_applied.extend(applied)
            all_skipped.extend(skipped)
    finally:
        conn.close()

    print()
    print("=== merge summary ===")
    print(f"  overlays present: {len(present_overlays)}")
    print(f"  overlays missing: {len(missing_overlays)}")
    print(f"  overlay tables applied: {len(all_applied)}")
    for name, before, after in all_applied:
        delta = after - before
        sign = "+" if delta >= 0 else ""
        print(f"    - {name}: {before:,} -> {after:,} ({sign}{delta:,})")
    if all_skipped:
        print(f"  overlay tables skipped: {len(all_skipped)}")
        for name, reason in all_skipped:
            print(f"    - {name}: {reason[:100]}")
    return _verify(dst)


def _parse_overlay(arg: str) -> tuple[str, list[str]]:
    """Parse `PATH:TABLE1,TABLE2` into (path, [tables]).

    Split on the last colon so Windows paths like `C:\\tmp\\x.db:t1,t2` work.
    """
    if ":" not in arg:
        raise argparse.ArgumentTypeError(
            f"--overlay arg must be PATH:TABLES, got {arg!r}"
        )
    path, _, tables_str = arg.rpartition(":")
    tables = [t.strip() for t in tables_str.split(",") if t.strip()]
    if not tables:
        raise argparse.ArgumentTypeError(
            f"--overlay arg has no tables after colon: {arg!r}"
        )
    return path, tables


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True, help="base DB path (light job)")
    p.add_argument(
        "--overlay",
        action="append",
        default=[],
        type=_parse_overlay,
        help=(
            "overlay as PATH:TABLE[,TABLE]. Repeat for each fetch job. "
            "Missing files are tolerated (their tables are left as base)."
        ),
    )
    p.add_argument("--dst", required=True, help="destination DB path")
    p.add_argument(
        "--require-base",
        action="store_true",
        help=(
            "fail if base is missing (recommended for CI; without this flag "
            "a missing base causes the first overlay to be promoted, erasing "
            "light-owned tables)"
        ),
    )
    args = p.parse_args()
    if not args.overlay:
        print("no --overlay given; dst will be base verbatim")
    return 0 if merge(args.base, args.overlay, args.dst, args.require_base) else 1


if __name__ == "__main__":
    sys.exit(main())
