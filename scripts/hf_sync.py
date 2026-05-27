"""Upload the canonical geohazard.db to a Hugging Face dataset.

This replaces the retired RPi5 USB SSD primary tier. The Hugging Face dataset
yasumorishima/japan-geohazard is now the canonical durable copy of the merged
database; the GitHub artifact chain remains the per-run working state.

Safeguards:
    1. The source DB must pass PRAGMA integrity_check before any upload, so a
       corrupt/partial merge can never overwrite the canonical copy.
    2. --squash runs HfApi.super_squash_history AFTER a verified upload, folding
       the git-LFS history into a single commit. Each push is a ~16.5GB LFS
       commit, so without squashing the dataset's usedStorage grows unbounded.
       Squash is only reached once the upload has returned successfully.

HF_TOKEN is read from the environment by huggingface_hub automatically (same as
.github/workflows/hf-upload-checkpoint.yml).

Exit 0 on success, Exit 1 on integrity failure or upload error.
"""

import argparse
import sqlite3
import sys


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
    args = p.parse_args()

    # Fail closed: never push a DB that does not pass integrity_check.
    print(f"Verifying {args.src} before upload...")
    if not integrity_ok(args.src):
        print("::error::refusing to upload -- source DB failed integrity_check")
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
