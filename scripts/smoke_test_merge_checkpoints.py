"""Smoke test for scripts/merge_checkpoints.py N-way merge.

Runs all paths locally with fake DBs, exits 0 if all pass.

Usage: python3 scripts/smoke_test_merge_checkpoints.py
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile


def make_db(path: str, data: dict[str, list[tuple]]):
    """Create a SQLite file at `path` with given tables.

    Each table has schema (id INTEGER PRIMARY KEY, val TEXT).
    """
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    for name, rows in data.items():
        conn.execute(
            f'CREATE TABLE "{name}" (id INTEGER PRIMARY KEY, val TEXT)'
        )
        conn.executemany(
            f'INSERT INTO "{name}"(id, val) VALUES (?, ?)', rows
        )
    conn.commit()
    conn.close()


def count_rows(path: str, table: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    finally:
        conn.close()


def fetch_vals(path: str, table: str) -> list[str]:
    conn = sqlite3.connect(path)
    try:
        return [
            r[0]
            for r in conn.execute(
                f'SELECT val FROM "{table}" ORDER BY id'
            ).fetchall()
        ]
    finally:
        conn.close()


def run_merge(args: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(
        [sys.executable, "scripts/merge_checkpoints.py", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )
    return proc.returncode, proc.stdout, proc.stderr


def test_one_overlay_replaces_stale_base(tmp: str) -> None:
    base = os.path.join(tmp, "base.db")
    modis = os.path.join(tmp, "modis.db")
    dst = os.path.join(tmp, "dst.db")

    # base has stale modis_lst (value="OLD") + fresh light table
    make_db(
        base,
        {
            "modis_lst": [(1, "OLD"), (2, "OLD")],
            "earthquakes": [(1, "quake-a"), (2, "quake-b")],
        },
    )
    # modis overlay has fresh modis_lst
    make_db(modis, {"modis_lst": [(1, "NEW"), (2, "NEW"), (3, "NEW")]})

    rc, out, err = run_merge(
        [
            "--base", base,
            "--overlay", f"{modis}:modis_lst",
            "--dst", dst,
            "--require-base",
        ]
    )
    assert rc == 0, f"rc={rc} out={out} err={err}"
    assert fetch_vals(dst, "modis_lst") == ["NEW", "NEW", "NEW"], (
        "base stale modis_lst rows not replaced: "
        f"{fetch_vals(dst, 'modis_lst')}"
    )
    assert fetch_vals(dst, "earthquakes") == ["quake-a", "quake-b"], (
        "light-owned table unexpectedly modified"
    )
    print("PASS: one-overlay replaces stale base")


def test_multiple_overlays(tmp: str) -> None:
    base = os.path.join(tmp, "base.db")
    modis = os.path.join(tmp, "modis.db")
    so2 = os.path.join(tmp, "so2.db")
    cloud = os.path.join(tmp, "cloud.db")
    snet = os.path.join(tmp, "snet.db")
    dst = os.path.join(tmp, "dst.db")

    make_db(
        base,
        {
            "earthquakes": [(1, "a")],
            "modis_lst": [(1, "old-modis")],
            "so2_column": [(1, "old-so2")],
            "cloud_fraction": [(1, "old-cloud")],
            "snet_waveform": [(1, "old-sw")],
            "snet_pressure": [(1, "old-sp")],
        },
    )
    make_db(modis, {"modis_lst": [(1, "new-modis"), (2, "new-modis2")]})
    make_db(so2, {"so2_column": [(1, "new-so2")]})
    make_db(cloud, {"cloud_fraction": [(1, "new-cloud")]})
    make_db(
        snet,
        {
            "snet_waveform": [(1, "new-sw")],
            "snet_pressure": [(1, "new-sp")],
        },
    )

    rc, out, _ = run_merge(
        [
            "--base", base,
            "--overlay", f"{modis}:modis_lst",
            "--overlay", f"{so2}:so2_column",
            "--overlay", f"{cloud}:cloud_fraction",
            "--overlay", f"{snet}:snet_waveform,snet_pressure",
            "--dst", dst,
            "--require-base",
        ]
    )
    assert rc == 0, out
    assert fetch_vals(dst, "modis_lst") == ["new-modis", "new-modis2"]
    assert fetch_vals(dst, "so2_column") == ["new-so2"]
    assert fetch_vals(dst, "cloud_fraction") == ["new-cloud"]
    assert fetch_vals(dst, "snet_waveform") == ["new-sw"]
    assert fetch_vals(dst, "snet_pressure") == ["new-sp"]
    assert fetch_vals(dst, "earthquakes") == ["a"]
    print("PASS: four overlays with mixed single/multi-table all applied")


def test_missing_overlay_keeps_base(tmp: str) -> None:
    base = os.path.join(tmp, "base.db")
    modis = os.path.join(tmp, "modis.db")
    dst = os.path.join(tmp, "dst.db")
    missing = os.path.join(tmp, "does-not-exist.db")

    make_db(
        base,
        {
            "modis_lst": [(1, "old-modis")],
            "so2_column": [(1, "prior-so2")],
        },
    )
    make_db(modis, {"modis_lst": [(1, "new-modis")]})

    rc, out, _ = run_merge(
        [
            "--base", base,
            "--overlay", f"{modis}:modis_lst",
            "--overlay", f"{missing}:so2_column",
            "--dst", dst,
            "--require-base",
        ]
    )
    assert rc == 0, out
    # modis overlay applied
    assert fetch_vals(dst, "modis_lst") == ["new-modis"]
    # so2 overlay missing → prior base row survives
    assert fetch_vals(dst, "so2_column") == ["prior-so2"], (
        f"missing overlay should leave base row intact, got "
        f"{fetch_vals(dst, 'so2_column')}"
    )
    assert "overlay missing" in out, f"expected 'overlay missing' in stdout:\n{out}"
    print("PASS: missing overlay leaves base rows intact")


def test_base_missing_with_require_base_fails(tmp: str) -> None:
    modis = os.path.join(tmp, "modis.db")
    dst = os.path.join(tmp, "dst.db")
    missing_base = os.path.join(tmp, "no-base.db")

    make_db(modis, {"modis_lst": [(1, "x")]})

    rc, out, _ = run_merge(
        [
            "--base", missing_base,
            "--overlay", f"{modis}:modis_lst",
            "--dst", dst,
            "--require-base",
        ]
    )
    assert rc == 1, f"expected exit 1, got {rc} out={out}"
    assert not os.path.exists(dst), "dst must not be created on require-base failure"
    print("PASS: missing base + --require-base refuses merge")


def test_overlay_table_missing_in_overlay_skips(tmp: str) -> None:
    base = os.path.join(tmp, "base.db")
    overlay = os.path.join(tmp, "ov.db")
    dst = os.path.join(tmp, "dst.db")

    make_db(base, {"modis_lst": [(1, "baseline")]})
    # overlay file exists but lacks modis_lst table
    make_db(overlay, {"cloud_fraction": [(1, "x")]})

    rc, out, _ = run_merge(
        [
            "--base", base,
            "--overlay", f"{overlay}:modis_lst",
            "--dst", dst,
            "--require-base",
        ]
    )
    assert rc == 0, out
    # Base's modis_lst should be untouched because overlay didn't have it.
    assert fetch_vals(dst, "modis_lst") == ["baseline"], (
        f"expected baseline preserved, got {fetch_vals(dst, 'modis_lst')}"
    )
    assert "missing in overlay" in out
    print("PASS: missing table in overlay → base row untouched")


def test_overlay_creates_table_new_in_base(tmp: str) -> None:
    base = os.path.join(tmp, "base.db")
    overlay = os.path.join(tmp, "ov.db")
    dst = os.path.join(tmp, "dst.db")

    make_db(base, {"earthquakes": [(1, "a")]})
    # overlay has a table that doesn't exist in base — merge should create
    # its schema from overlay and copy rows.
    make_db(overlay, {"modis_lst": [(1, "x"), (2, "y")]})

    rc, out, _ = run_merge(
        [
            "--base", base,
            "--overlay", f"{overlay}:modis_lst",
            "--dst", dst,
            "--require-base",
        ]
    )
    assert rc == 0, out
    assert fetch_vals(dst, "modis_lst") == ["x", "y"]
    print("PASS: overlay-only table created in dst with schema from overlay")


def test_injection_name_rejected(tmp: str) -> None:
    base = os.path.join(tmp, "base.db")
    overlay = os.path.join(tmp, "ov.db")
    dst = os.path.join(tmp, "dst.db")

    make_db(base, {"modis_lst": [(1, "a")]})
    make_db(overlay, {"modis_lst": [(1, "b")]})

    # Craft a malicious table name. Should be rejected (skipped with reason).
    rc, out, _ = run_merge(
        [
            "--base", base,
            "--overlay", f"{overlay}:modis_lst,evil\"name",
            "--dst", dst,
            "--require-base",
        ]
    )
    assert rc == 0, out
    # modis_lst still applied
    assert fetch_vals(dst, "modis_lst") == ["b"]
    assert "contains double-quote" in out
    print("PASS: table name with double-quote rejected (no SQL injection)")


def test_all_overlays_missing(tmp: str) -> None:
    base = os.path.join(tmp, "base.db")
    dst = os.path.join(tmp, "dst.db")

    make_db(
        base,
        {
            "earthquakes": [(1, "a")],
            "modis_lst": [(1, "prior-modis")],
        },
    )

    rc, out, _ = run_merge(
        [
            "--base", base,
            "--overlay", f"{os.path.join(tmp, 'nope1.db')}:modis_lst",
            "--overlay", f"{os.path.join(tmp, 'nope2.db')}:so2_column",
            "--dst", dst,
            "--require-base",
        ]
    )
    assert rc == 0, out
    assert fetch_vals(dst, "modis_lst") == ["prior-modis"]
    assert fetch_vals(dst, "earthquakes") == ["a"]
    print("PASS: all overlays missing → dst is base verbatim")


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        tests = [
            test_one_overlay_replaces_stale_base,
            test_multiple_overlays,
            test_missing_overlay_keeps_base,
            test_base_missing_with_require_base_fails,
            test_overlay_table_missing_in_overlay_skips,
            test_overlay_creates_table_new_in_base,
            test_injection_name_rejected,
            test_all_overlays_missing,
        ]
        for t in tests:
            sub = os.path.join(tmp, t.__name__)
            os.makedirs(sub, exist_ok=True)
            try:
                t(sub)
            except AssertionError as e:
                print(f"FAIL: {t.__name__}: {e}")
                return 1
            except Exception as e:
                print(f"ERROR: {t.__name__}: {type(e).__name__}: {e}")
                return 1
            finally:
                shutil.rmtree(sub, ignore_errors=True)
    print()
    print("ALL 8 SMOKE TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
