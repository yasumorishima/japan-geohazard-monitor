"""Smoke test for scripts/extract_overlay.py — covers schema preservation,
row preservation, missing-table tolerance, and size-reduction behaviour.

Invoked via `python3 scripts/smoke_test_extract_overlay.py`. Exits non-zero
on any assertion failure. Self-contained: builds throwaway DBs in a temp dir
and removes them on success.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
EXTRACT = os.path.join(HERE, "extract_overlay.py")


def run_extract(src: str, dst: str, tables: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, EXTRACT, "--src", src, "--dst", dst, "--tables", tables],
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_basic_extract_preserves_schema_and_rows(tmp: str) -> None:
    src = os.path.join(tmp, "src.db")
    dst = os.path.join(tmp, "dst.db")
    conn = sqlite3.connect(src)
    conn.executescript(
        """
        CREATE TABLE wanted (id INTEGER PRIMARY KEY, val TEXT);
        CREATE TABLE other (a INTEGER, b TEXT);
        INSERT INTO wanted VALUES (1, 'a'), (2, 'b'), (3, 'c');
        INSERT INTO other VALUES (10, 'x'), (20, 'y');
        """
    )
    conn.commit()
    conn.close()

    res = run_extract(src, dst, "wanted")
    assert res.returncode == 0, f"extract failed: {res.stderr}"

    out = sqlite3.connect(dst)
    tables = [
        r[0] for r in out.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]
    assert tables == ["wanted"], f"expected only [wanted], got {tables}"
    rows = out.execute("SELECT id, val FROM wanted ORDER BY id").fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")], rows
    out.close()
    print("  PASS: basic_extract_preserves_schema_and_rows")


def test_multiple_tables(tmp: str) -> None:
    src = os.path.join(tmp, "src.db")
    dst = os.path.join(tmp, "dst.db")
    conn = sqlite3.connect(src)
    conn.executescript(
        """
        CREATE TABLE alpha (id INTEGER, name TEXT);
        CREATE TABLE beta (k TEXT PRIMARY KEY, v REAL);
        CREATE TABLE gamma (x INTEGER);
        INSERT INTO alpha VALUES (1, 'foo');
        INSERT INTO beta VALUES ('hi', 1.5);
        INSERT INTO gamma VALUES (99);
        """
    )
    conn.commit()
    conn.close()

    res = run_extract(src, dst, "alpha,beta")
    assert res.returncode == 0, f"extract failed: {res.stderr}"

    out = sqlite3.connect(dst)
    tables = sorted(
        r[0] for r in out.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    )
    assert tables == ["alpha", "beta"], tables
    assert out.execute("SELECT COUNT(*) FROM alpha").fetchone()[0] == 1
    assert out.execute("SELECT COUNT(*) FROM beta").fetchone()[0] == 1
    out.close()
    print("  PASS: multiple_tables")


def test_missing_table_tolerated(tmp: str) -> None:
    src = os.path.join(tmp, "src.db")
    dst = os.path.join(tmp, "dst.db")
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE present (x INTEGER)")
    conn.execute("INSERT INTO present VALUES (1)")
    conn.commit()
    conn.close()

    res = run_extract(src, dst, "present,absent")
    assert res.returncode == 0, f"extract failed: {res.stderr}"
    assert "WARN" in res.stdout and "absent" in res.stdout, res.stdout

    out = sqlite3.connect(dst)
    tables = [
        r[0] for r in out.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    ]
    assert tables == ["present"], tables
    out.close()
    print("  PASS: missing_table_tolerated")


def test_empty_table(tmp: str) -> None:
    src = os.path.join(tmp, "src.db")
    dst = os.path.join(tmp, "dst.db")
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE owned (id INTEGER PRIMARY KEY, payload BLOB)")
    conn.commit()
    conn.close()

    res = run_extract(src, dst, "owned")
    assert res.returncode == 0, f"extract failed: {res.stderr}"

    out = sqlite3.connect(dst)
    assert out.execute("SELECT COUNT(*) FROM owned").fetchone()[0] == 0
    out.close()
    print("  PASS: empty_table")


def test_size_reduction(tmp: str) -> None:
    """Ensure overlay DB is meaningfully smaller than the source when most
    tables are dropped. Loose threshold to account for SQLite page granularity.
    """
    src = os.path.join(tmp, "src.db")
    dst = os.path.join(tmp, "dst.db")
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE owned (id INTEGER, payload TEXT)")
    conn.execute("CREATE TABLE bulk (id INTEGER, payload TEXT)")
    conn.execute("INSERT INTO owned VALUES (1, 'small')")
    bulk_payload = "x" * 1024
    conn.executemany("INSERT INTO bulk VALUES (?, ?)", [(i, bulk_payload) for i in range(2000)])
    conn.commit()
    conn.close()

    res = run_extract(src, dst, "owned")
    assert res.returncode == 0, f"extract failed: {res.stderr}"

    src_sz = os.path.getsize(src)
    dst_sz = os.path.getsize(dst)
    assert dst_sz < src_sz / 4, f"overlay not meaningfully smaller: {dst_sz} vs {src_sz}"
    print(f"  PASS: size_reduction (src={src_sz} dst={dst_sz} ratio={src_sz/dst_sz:.1f}x)")


def test_missing_src_errors(tmp: str) -> None:
    dst = os.path.join(tmp, "dst.db")
    res = run_extract(os.path.join(tmp, "nope.db"), dst, "anything")
    assert res.returncode != 0, "expected non-zero on missing src"
    assert "src not found" in res.stderr, res.stderr
    print("  PASS: missing_src_errors")


def test_overwrites_existing_dst(tmp: str) -> None:
    """If dst already exists from a previous step, it must be replaced — not
    merged into. Prevents accidental accumulation.
    """
    src = os.path.join(tmp, "src.db")
    dst = os.path.join(tmp, "dst.db")
    open(dst, "wb").write(b"\x00" * 1024)  # garbage placeholder
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE owned (x INTEGER)")
    conn.execute("INSERT INTO owned VALUES (42)")
    conn.commit()
    conn.close()

    res = run_extract(src, dst, "owned")
    assert res.returncode == 0, f"extract failed: {res.stderr}"

    out = sqlite3.connect(dst)
    assert out.execute("SELECT x FROM owned").fetchone() == (42,)
    out.close()
    print("  PASS: overwrites_existing_dst")


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="extract_overlay_smoke_")
    try:
        for fn in [
            test_basic_extract_preserves_schema_and_rows,
            test_multiple_tables,
            test_missing_table_tolerated,
            test_empty_table,
            test_size_reduction,
            test_missing_src_errors,
            test_overwrites_existing_dst,
        ]:
            # each test gets a fresh subdir to avoid state bleed
            sub = tempfile.mkdtemp(dir=tmp, prefix=fn.__name__ + "_")
            fn(sub)
        print("\nALL TESTS PASS (7/7)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
