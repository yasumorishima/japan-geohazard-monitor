"""Upload the canonical geohazard.db to a Hugging Face dataset.

This replaces the retired RPi5 USB SSD primary tier. The Hugging Face dataset
yasumorishima/japan-geohazard is now the canonical durable copy of the merged
database; the GitHub artifact chain remains the per-run working state.

Safeguards:
    1. The source DB must pass PRAGMA integrity_check before any upload, so a
       corrupt/partial merge can never overwrite the canonical copy.
    2. Absolute floor: refuse a catastrophically tiny DB (--min-abs-gb),
       independent of any prior state -- a healthy DB is tens of GB.
    3. Row-count no-regression guard: backfill is append-only, so no table
       should LOSE rows. We compare per-table row counts against a small
       sidecar manifest (<path>.rowcounts.json) stored next to the DB on HF.
       If any table drops below --min-fraction of its previous count, refuse
       (degraded/partial merge). This replaces the old byte-size guard, which
       wrongly rejected a legitimate VACUUM/compaction: on 2026-07-05 a
       compaction shrank the canonical 44GB -> 18GB with IDENTICAL/greater row
       counts (ioc_sea_level 49M rows, current to yesterday), yet the byte
       guard blocked every upload for ~11 days. Rows, not bytes, are the real
       degradation signal. A first upload (no manifest yet) skips this check.
    4. --squash runs HfApi.super_squash_history AFTER a verified upload, folding
       the git-LFS history into a single commit. Each push is a large LFS
       commit, so without squashing the dataset's usedStorage grows unbounded.
       Squash is only reached once the upload has returned successfully.

HF_TOKEN is read from the environment by huggingface_hub automatically (same as
.github/workflows/hf-upload-checkpoint.yml).

Exit 0 on success, Exit 1 on integrity failure or upload error.
"""

import argparse
import json
import os
import sqlite3
import sys
import tempfile


def integrity_ok(path: str) -> bool:
    """Full PRAGMA integrity_check. Returns True only on 'ok'."""
    try:
        conn = sqlite3.connect(path)
    except Exception as e:  # noqa: BLE001 - report and fail closed
        print(f"  DB open failed: {e}")
        return False
    try:
        r = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if r != "ok":
            print(f"  integrity_check FAILED: {r[:200]}")
            return False
        print("  integrity_check OK")
        return True
    finally:
        conn.close()


def table_row_counts(path: str) -> dict:
    """Per-table row counts for all user tables. {} on failure."""
    counts = {}
    try:
        conn = sqlite3.connect(path)
    except Exception as e:  # noqa: BLE001
        print(f"  DB open failed for row counts: {e}")
        return counts
    try:
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        for t in tables:
            try:
                counts[t] = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            except Exception as e:  # noqa: BLE001 - skip unreadable table
                print(f"  row count failed for {t}: {e}")
    finally:
        conn.close()
    return counts


def remote_manifest(repo: str, manifest_path: str):
    """Fetch the previous row-count manifest from HF, or None if absent."""
    from huggingface_hub import hf_hub_download

    try:
        f = hf_hub_download(repo, manifest_path, repo_type="dataset")
    except Exception as e:  # noqa: BLE001 - absent/unknown -> first run
        print(f"  no remote row-count manifest ({e})")
        return None
    try:
        with open(f) as fh:
            return json.load(fh)
    except Exception as e:  # noqa: BLE001
        print(f"  manifest parse failed ({e})")
        return None


def main() -> int:
    p = argparse.ArgumentParser(description="Upload geohazard.db to Hugging Face")
    p.add_argument("--src", required=True, help="local DB path")
    p.add_argument("--repo", required=True, help="HF dataset repo id (owner/name)")
    p.add_argument("--path", default="geohazard.db", help="path within the repo")
    p.add_argument(
        "--message",
        default="Update geohazard.db (backfill checkpoint)",
        help="commit message",
    )
    p.add_argument(
        "--squash",
        action="store_true",
        help="fold LFS history into one commit after a verified upload",
    )
    p.add_argument(
        "--min-fraction",
        type=float,
        default=0.95,
        help="refuse upload if any table drops below this fraction of its "
        "previous row count (append-only no-regression guard). 0 disables.",
    )
    p.add_argument(
        "--min-abs-gb",
        type=float,
        default=5.0,
        help="absolute floor: refuse a DB smaller than this many GB (catches a "
        "catastrophically broken merge even on the first upload). 0 disables.",
    )
    args = p.parse_args()

    # (1) Fail closed: never push a DB that does not pass integrity_check.
    print(f"Verifying {args.src} before upload...")
    if not integrity_ok(args.src):
        print("::error::refusing to upload -- source DB failed integrity_check")
        return 1

    # (2) Absolute floor: a healthy DB is tens of GB; anything tiny is broken.
    if args.min_abs_gb > 0:
        local_bytes = os.path.getsize(args.src)
        floor_bytes = args.min_abs_gb * 1e9
        print(f"  local={local_bytes:,}B abs-floor={floor_bytes:,.0f}B")
        if local_bytes < floor_bytes:
            print(
                "::error::refusing to upload -- source DB "
                f"{local_bytes:,}B is below the absolute floor "
                f"{args.min_abs_gb}GB (catastrophic/partial merge)"
            )
            return 1

    # (3) Row-count no-regression guard (append-only). Rows, not bytes, so a
    #     legitimate VACUUM/compaction is not mistaken for a degraded merge.
    local_counts = table_row_counts(args.src)
    manifest_path = args.path + ".rowcounts.json"
    if args.min_fraction > 0:
        prev = remote_manifest(args.repo, manifest_path)
        if prev is None:
            print(
                "  no remote row-count manifest -- skipping regression guard "
                "(first run / re-baseline); manifest will be written after upload"
            )
        else:
            regressed = []
            for t, prev_n in prev.items():
                loc_n = local_counts.get(t, 0)
                if loc_n < prev_n * args.min_fraction:
                    regressed.append(
                        f"{t}: local={loc_n:,} < {args.min_fraction:.0%} of "
                        f"prev={prev_n:,}"
                    )
            if regressed:
                print(
                    "::error::refusing to upload -- row-count regression in "
                    f"{len(regressed)} table(s), likely a degraded/partial merge:"
                )
                for r in regressed[:12]:
                    print(f"    {r}")
                return 1
            print(
                f"  row-count guard OK ({len(prev)} tables checked, "
                f"min-fraction={args.min_fraction})"
            )

    from huggingface_hub import HfApi

    api = HfApi()
    print(f"Uploading {args.src} -> {args.repo}:{args.path} ...")
    api.upload_file(
        path_or_fileobj=args.src,
        path_in_repo=args.path,
        repo_id=args.repo,
        repo_type="dataset",
        commit_message=args.message,
    )
    print("Upload complete.")

    # Post-upload steps (manifest refresh + squash) are best-effort: the DB is
    # already durably committed above, so their failure must NOT be reported as
    # a job failure -- that would re-trigger the very error emails this guards
    # against. Warn and still exit 0.
    try:
        # Refresh the row-count manifest so the next run has a fresh baseline.
        mf = os.path.join(tempfile.gettempdir(), "geohazard_rowcounts.json")
        with open(mf, "w") as fh:
            json.dump(local_counts, fh, indent=0, sort_keys=True)
        api.upload_file(
            path_or_fileobj=mf,
            path_in_repo=manifest_path,
            repo_id=args.repo,
            repo_type="dataset",
            commit_message="Update row-count manifest",
        )
        print(f"Row-count manifest updated ({len(local_counts)} tables).")

        if args.squash:
            # Only reached after a successful upload above, so the canonical
            # copy is safely committed before history is collapsed (irreversible).
            print("Squashing LFS history to bound usedStorage...")
            api.super_squash_history(repo_id=args.repo, repo_type="dataset")
            print("History squashed.")
    except Exception as e:  # noqa: BLE001 - DB already uploaded; don't fail job
        print(f"::warning::post-upload step failed after a successful DB upload: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
