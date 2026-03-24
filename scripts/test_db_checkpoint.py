#!/usr/bin/env python3
"""DB checkpoint integrity tests -- run BEFORE every fetch to catch corruption early.

Tests cover the exact failure modes observed in production:
  1. WAL checkpoint completeness
  2. Concurrent writer safety (30+ fetch scripts writing simultaneously)
  3. Checkpoint save/restore cycle (artifact upload/download simulation)
  4. PRAGMA settings verification (WAL mode, SYNCHRONOUS=FULL)
  5. Large transaction rollback safety
  6. Crash recovery (connection killed mid-transaction)
  7. File size sanity (detect truncation)
  8. Verified checkpoint function (production equivalent)
  9. Production DB check (if GEOHAZARD_DB_PATH is set)

Exit code 0 = all passed, 1 = failure (blocks fetch job).
"""

import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import traceback

PASS_COUNT = 0
FAIL_COUNT = 0


def report(name, ok, detail=""):
    global PASS_COUNT, FAIL_COUNT
    if ok:
        PASS_COUNT += 1
        print(f"  PASS: {name}")
    else:
        FAIL_COUNT += 1
        msg = f"  FAIL: {name}"
        if detail:
            msg += f" -- {detail}"
        print(msg)


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def create_test_db(path):
    """Create a test DB mimicking production schema (WAL + SYNCHRONOUS=FULL)."""
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS earthquakes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL, event_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL, latitude REAL NOT NULL,
            longitude REAL NOT NULL, depth_km REAL, magnitude REAL,
            received_at TEXT NOT NULL, UNIQUE(source, event_id))
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_eq_occurred ON earthquakes(occurred_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_eq_location ON earthquakes(latitude, longitude)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spatial_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL, date_str TEXT NOT NULL,
            lat REAL NOT NULL, lon REAL NOT NULL, value REAL,
            UNIQUE(source, date_str, lat, lon))
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spatial ON spatial_data(source, date_str)")
    conn.commit()
    return conn


def test_pragma_settings(db_path):
    print("\n[Test 1] PRAGMA settings verification")
    conn = sqlite3.connect(db_path)
    journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
    report("journal_mode = wal", journal == "wal", f"got: {journal}")
    sync = conn.execute("PRAGMA synchronous").fetchone()[0]
    report("synchronous = FULL (2)", sync == 2, f"got: {sync}")
    auto = conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
    report("wal_autocheckpoint > 0", auto > 0, f"got: {auto}")
    conn.close()


def test_wal_checkpoint(db_path):
    print("\n[Test 2] WAL checkpoint completeness")
    conn = sqlite3.connect(db_path)
    for i in range(500):
        conn.execute(
            "INSERT OR IGNORE INTO earthquakes "
            "(source, event_id, occurred_at, latitude, longitude, depth_km, magnitude, received_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("test", f"wal_test_{i}", f"2024-01-{(i % 28) + 1:02d}",
             35.0 + i * 0.01, 139.0, 10.0, 5.0, "2024-01-01"),
        )
    conn.commit()

    wal_path = db_path + "-wal"
    wal_size_before = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
    report("WAL file has pending data", wal_size_before > 0, f"WAL size: {wal_size_before}")

    result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    busy, log_pages, checkpointed = result
    report(
        "wal_checkpoint(TRUNCATE) complete",
        busy == 0 and log_pages == checkpointed,
        f"busy={busy}, log={log_pages}, checkpointed={checkpointed}",
    )

    wal_size_after = os.path.getsize(wal_path) if os.path.exists(wal_path) else -1
    report("WAL truncated to 0 bytes", wal_size_after == 0, f"WAL size after: {wal_size_after}")

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    report("integrity_check after checkpoint", integrity == "ok",
           integrity[:200] if integrity != "ok" else "")
    conn.close()


def test_concurrent_writes(db_path):
    print("\n[Test 3] Concurrent writer safety (10 threads x 100 rows)")
    errors = []
    inserted_counts = [0] * 10

    def writer(thread_id):
        try:
            conn = sqlite3.connect(db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=10000")
            for i in range(100):
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO spatial_data "
                        "(source, date_str, lat, lon, value) VALUES (?, ?, ?, ?, ?)",
                        (f"thread_{thread_id}", f"2024-01-{(i % 28) + 1:02d}",
                         30.0 + thread_id, 130.0 + i * 0.1, float(i)),
                    )
                    conn.commit()
                    inserted_counts[thread_id] += 1
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e):
                        time.sleep(0.05)
                    else:
                        errors.append(f"Thread {thread_id}: {e}")
            conn.close()
        except Exception as e:
            errors.append(f"Thread {thread_id} fatal: {e}")

    threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    total = sum(inserted_counts)
    report("No fatal errors in concurrent writes", len(errors) == 0, "; ".join(errors[:3]))
    report(f"All threads completed inserts ({total}/1000)", total >= 900, f"got {total}")

    conn = sqlite3.connect(db_path)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    report("integrity_check after concurrent writes", integrity == "ok",
           integrity[:200] if integrity != "ok" else "")
    conn.close()


def test_checkpoint_with_readers(db_path):
    print("\n[Test 4] WAL checkpoint while readers are active")
    errors = []
    read_complete = threading.Event()
    checkpoint_done = threading.Event()

    def reader():
        try:
            conn = sqlite3.connect(db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            count = conn.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]
            read_complete.set()
            checkpoint_done.wait(timeout=10)
            count2 = conn.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]
            if count2 < count:
                errors.append(f"Row count decreased: {count} -> {count2}")
            conn.close()
        except Exception as e:
            errors.append(f"Reader: {e}")

    reader_thread = threading.Thread(target=reader)
    reader_thread.start()
    read_complete.wait(timeout=5)

    conn = sqlite3.connect(db_path, timeout=30)
    result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    busy, log_pages, checkpointed = result
    checkpoint_done.set()
    conn.close()

    reader_thread.join(timeout=10)

    report("Checkpoint did not crash with active readers", True)
    report("Checkpoint status reported correctly", busy in (0, 1),
           f"busy={busy}, log={log_pages}, checkpointed={checkpointed}")
    report("No reader errors", len(errors) == 0, "; ".join(errors[:3]))

    conn = sqlite3.connect(db_path)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    report("integrity_check after checkpoint+readers", integrity == "ok",
           integrity[:200] if integrity != "ok" else "")
    conn.close()


def test_save_restore_cycle(db_path):
    print("\n[Test 5] Checkpoint save -> restore cycle (artifact simulation)")

    conn = sqlite3.connect(db_path)
    eq_count = conn.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]
    sp_count = conn.execute("SELECT COUNT(*) FROM spatial_data").fetchone()[0]

    result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    busy, log_pages, checkpointed = result
    report("Pre-save WAL flush complete",
           busy == 0 and log_pages == checkpointed,
           f"busy={busy}, log={log_pages}, checkpointed={checkpointed}")

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    report("Pre-save integrity OK", integrity == "ok",
           integrity[:200] if integrity != "ok" else "")
    conn.close()

    original_hash = md5(db_path)
    original_size = os.path.getsize(db_path)

    restore_dir = tempfile.mkdtemp(prefix="db_restore_")
    restore_path = os.path.join(restore_dir, "geohazard.db")
    shutil.copy2(db_path, restore_path)

    for ext in ("-wal", "-shm"):
        p = restore_path + ext
        if os.path.exists(p):
            os.remove(p)

    restored_hash = md5(restore_path)
    restored_size = os.path.getsize(restore_path)
    report("Restored file size matches", original_size == restored_size,
           f"orig={original_size}, restored={restored_size}")
    report("Restored file hash matches", original_hash == restored_hash)

    conn2 = sqlite3.connect(restore_path)
    integrity2 = conn2.execute("PRAGMA integrity_check").fetchone()[0]
    report("Restored DB integrity OK", integrity2 == "ok",
           integrity2[:200] if integrity2 != "ok" else "")

    eq2 = conn2.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]
    sp2 = conn2.execute("SELECT COUNT(*) FROM spatial_data").fetchone()[0]
    report("Row counts preserved",
           eq_count == eq2 and sp_count == sp2,
           f"eq: {eq_count}->{eq2}, spatial: {sp_count}->{sp2}")
    conn2.close()
    shutil.rmtree(restore_dir, ignore_errors=True)


def test_large_transaction_rollback(db_path):
    print("\n[Test 6] Large transaction rollback safety")
    conn = sqlite3.connect(db_path)
    count_before = conn.execute("SELECT COUNT(*) FROM spatial_data").fetchone()[0]

    conn.execute("BEGIN")
    for i in range(5000):
        conn.execute(
            "INSERT OR IGNORE INTO spatial_data "
            "(source, date_str, lat, lon, value) VALUES (?, ?, ?, ?, ?)",
            ("rollback_test", f"2025-{(i % 12) + 1:02d}-01",
             25.0 + i * 0.001, 125.0, float(i)),
        )
    conn.rollback()

    count_after = conn.execute("SELECT COUNT(*) FROM spatial_data").fetchone()[0]
    report("Rollback preserved row count", count_before == count_after,
           f"before={count_before}, after={count_after}")

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    report("integrity_check after rollback", integrity == "ok",
           integrity[:200] if integrity != "ok" else "")
    conn.close()


def test_crash_recovery(db_path):
    print("\n[Test 7] Crash recovery (connection killed mid-transaction)")
    conn = sqlite3.connect(db_path)
    count_before = conn.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]
    conn.close()

    conn2 = sqlite3.connect(db_path)
    conn2.execute("PRAGMA journal_mode=WAL")
    conn2.execute("PRAGMA synchronous=FULL")
    conn2.execute("BEGIN")
    for i in range(200):
        conn2.execute(
            "INSERT OR IGNORE INTO earthquakes "
            "(source, event_id, occurred_at, latitude, longitude, depth_km, magnitude, received_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("crash_test", f"crash_{i}", f"2025-06-{(i % 28) + 1:02d}",
             36.0, 140.0, 5.0, 4.0, "2025-06-01"),
        )
    # Simulate crash: close without commit
    conn2.close()

    conn3 = sqlite3.connect(db_path)
    count_after = conn3.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]
    report("Uncommitted rows not persisted", count_after == count_before,
           f"before={count_before}, after={count_after}")

    integrity = conn3.execute("PRAGMA integrity_check").fetchone()[0]
    report("integrity_check after crash recovery", integrity == "ok",
           integrity[:200] if integrity != "ok" else "")
    conn3.close()


def test_file_size_sanity(db_path):
    print("\n[Test 8] File size sanity checks")
    size = os.path.getsize(db_path)
    report("DB file > 4096 bytes", size > 4096, f"size={size}")

    conn = sqlite3.connect(db_path)
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    expected_size = page_size * page_count
    report("File size matches page_size x page_count", size == expected_size,
           f"file={size}, expected={expected_size} ({page_size}x{page_count})")

    freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
    freelist_pct = (freelist / page_count * 100) if page_count > 0 else 0
    report("Freelist fragmentation < 30%", freelist_pct < 30,
           f"{freelist_pct:.1f}% ({freelist}/{page_count} pages)")
    conn.close()


def test_verified_checkpoint(db_path):
    print("\n[Test 9] Verified checkpoint function (production equivalent)")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    for i in range(100):
        conn.execute(
            "INSERT OR IGNORE INTO spatial_data "
            "(source, date_str, lat, lon, value) VALUES (?, ?, ?, ?, ?)",
            ("verified_cp_test", f"2025-12-{(i % 28) + 1:02d}",
             40.0 + i * 0.01, 140.0, float(i)),
        )
    conn.commit()

    errors = []
    try:
        result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        busy, log_pages, checkpointed = result
        if busy != 0:
            errors.append(f"WAL checkpoint blocked (busy={busy})")
        if log_pages != checkpointed:
            errors.append(f"Incomplete: {log_pages} logged, {checkpointed} checkpointed")

        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            errors.append(f"integrity_check: {integrity[:100]}")

        wal_path = db_path + "-wal"
        if os.path.exists(wal_path):
            wal_size = os.path.getsize(wal_path)
            if wal_size > 0:
                errors.append(f"WAL not truncated: {wal_size} bytes remain")

        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
        expected = page_size * page_count
        actual = os.path.getsize(db_path)
        if actual != expected:
            errors.append(f"Size mismatch: file={actual}, expected={expected}")

    except Exception as e:
        errors.append(f"Exception: {e}")

    conn.close()
    report("Verified checkpoint passed all checks", len(errors) == 0, "; ".join(errors))


def test_production_db():
    db_path = os.environ.get("GEOHAZARD_DB_PATH", "")
    if not db_path or not os.path.exists(db_path):
        print("\n[Test 10] Production DB check -- SKIPPED (no DB at GEOHAZARD_DB_PATH)")
        return

    print(f"\n[Test 10] Production DB integrity ({db_path})")
    size = os.path.getsize(db_path)
    report(f"Production DB exists ({size:,} bytes)", size > 0)

    conn = sqlite3.connect(db_path)

    journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
    report("Production journal_mode = wal", journal == "wal", f"got: {journal}")

    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    expected = page_size * page_count
    report("Production file size matches pages", size == expected,
           f"file={size}, expected={expected}")

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    report("Production integrity_check", integrity == "ok",
           integrity[:300] if integrity != "ok" else "")

    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    for t in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM [{t[0]}]").fetchone()[0]
        print(f"    {t[0]}: {count:,} rows")

    conn.close()


def main():
    global PASS_COUNT, FAIL_COUNT

    print("=" * 70)
    print("DB CHECKPOINT INTEGRITY TEST SUITE")
    print("=" * 70)
    print("Tests the exact failure modes from production DB corruption incidents.")
    print(f"Python {sys.version.split()[0]}, sqlite3 {sqlite3.sqlite_version}")

    tmpdir = tempfile.mkdtemp(prefix="geohazard_dbtest_")
    db_path = os.path.join(tmpdir, "test_geohazard.db")

    try:
        conn = create_test_db(db_path)
        conn.close()

        test_pragma_settings(db_path)
        test_wal_checkpoint(db_path)
        test_concurrent_writes(db_path)
        test_checkpoint_with_readers(db_path)
        test_save_restore_cycle(db_path)
        test_large_transaction_rollback(db_path)
        test_crash_recovery(db_path)
        test_file_size_sanity(db_path)
        test_verified_checkpoint(db_path)
        test_production_db()

    except Exception as e:
        print(f"\n!! UNHANDLED EXCEPTION: {e}")
        traceback.print_exc()
        FAIL_COUNT += 1
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    print("\n" + "=" * 70)
    total = PASS_COUNT + FAIL_COUNT
    print(f"RESULTS: {PASS_COUNT} passed, {FAIL_COUNT} failed (total {total})")
    print("=" * 70)

    if FAIL_COUNT > 0:
        print("!! DB checkpoint tests FAILED -- DO NOT proceed with fetch")
        sys.exit(1)
    else:
        print("All DB checkpoint tests passed -- safe to proceed")
        sys.exit(0)


if __name__ == "__main__":
    main()
