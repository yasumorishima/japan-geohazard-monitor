"""Upload the canonical geohazard.db to a Hugging Face dataset.

This replaces the retired RPi5 USB SSD primary tier. The Hugging Face dataset
yasumorishima/japan-geohazard is now the canonical durable copy of the merged
database; the GitHub artifact chain remains the per-run working state.

Safeguards:
    1. The source DB must pass PRAGMA integrity_check before any upload, so a
       corrupt/partial merge can never overwrite the canonical copy.
    2. No-shrink guard: backfill only ever adds rows, so the DB grows
       monotonically. If the source is smaller than --min-fraction of the
       current remote file, refuse the upload. This protects the canonical
       copy from a degraded/partial merge (e.g. a run that built on a salvaged
       checkpoint) silently overwriting -- and then squashing away -- the full
       history. A first upload (no remote file yet) skips this check.
    3. --squash runs HfApi.super_squash_history AFTER a verified upload, folding
       the git-LFS history into a single commit. Each push is a ~16.5GB LFS
       commit, so without squashing the dataset's usedStorage grows unbounded.
       Squash is only reached once the upload has returned successfully.

HF_TOKEN is read from the environment by huggingface_hub automatically (same as
.github/workflows/hf-upload-checkpoint.yml).

Exit 0 on success, Exit 1 on integrity failure or upload error.
"""

import argparse
import os
import sqlite3
import sys


def remote_size(repo: str, path: str):
    """Size in bytes of `path` in the HF dataset, or None if not present yet."""
    from huggingface_hub import HfApi

    try:
        info = HfApi().get_paths_info(repo, [path], repo_type="dataset")
    except Exception as e:  # noqa: BLE001 - treat as "unknown", caller decides
        print(f"  could not read remote size ({e})")
        return None
    if not info:
        return None
    obj = info[0]
    lfs = getattr(obj, "lfs", None)
    if lfs is not None and getattr(lfs, "size", None) is not None:
        return lfs.size
    return getattr(obj, "size", None)


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
        help="refuse upload if src is smaller than this fraction of the remote "
        "file (no-shrink guard; backfill grows monotonically). 0 disables.",
    )
    args = p.parse_args()

    # Fail closed: never push a DB that does not pass integrity_check.
    print(f"Verifying {args.src} before upload...")
    if not integrity_ok(args.src):
        print("::error::refusing to upload -- source DB failed integrity_check")
        return 1

    # No-shrink guard: a smaller-than-remote DB means a degraded/partial merge.
    if args.min_fraction > 0:
        local_bytes = os.path.getsize(args.src)
        remote_bytes = remote_size(args.repo, args.path)
        if remote_bytes is None:
            print("  no existing remote file (or size unknown) -- skipping shrink guard")
        else:
            floor = remote_bytes * args.min_fraction
            print(
                f"  local={local_bytes:,}B remote={remote_bytes:,}B "
                f"floor={floor:,.0f}B (min-fraction={args.min_fraction})"
            )
            if local_bytes < floor:
                print(
                    "::error::refusing to upload -- source DB is smaller than "
                    f"{args.min_fraction:.0%} of the remote canonical copy "
                    "(likely a degraded/partial merge)"
                )
                return 1

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

    if args.squash:
        # Only reached after a successful upload above, so the canonical copy
        # is safely committed before history is collapsed (irreversible).
        print("Squashing LFS history to bound usedStorage...")
        api.super_squash_history(repo_id=args.repo, repo_type="dataset")
        print("History squashed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
