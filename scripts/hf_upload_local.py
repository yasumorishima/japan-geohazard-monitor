#!/usr/bin/env python3
"""Upload one local file to a Hugging Face dataset repo, then verify it landed.

Intended to run on the RPi5 self-hosted runner: the file lives on that box while
HF_TOKEN stays in GitHub Secrets, so no token is written to the box's disk.

Only derived artifacts belong here. NIED Hi-net / S-net raw waveforms must never
be redistributed, so raw/archive extensions are refused outright.

  python3 hf_upload_local.py --src /path/file.json \
      --repo owner/name --path derived/file.json --message "why"
"""
import argparse
import os
import sys

REFUSED_SUFFIXES = (
    ".mseed", ".sac", ".cnt", ".win32", ".sacpz",
    ".tar.gz", ".tgz", ".tar", ".zip",
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="local path on this host")
    ap.add_argument("--repo", required=True, help="HF dataset repo id (owner/name)")
    ap.add_argument("--path", required=True, help="destination path inside the repo")
    ap.add_argument("--message", default="upload derived artifact")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not os.path.isfile(args.src):
        print(f"not a file: {args.src}")
        return 1

    low = args.src.lower()
    if any(low.endswith(s) for s in REFUSED_SUFFIXES):
        print("refusing: raw/archive extension. Only derived artifacts may be uploaded.")
        print("  (NIED Hi-net / S-net raw redistribution is prohibited.)")
        return 1

    size = os.path.getsize(args.src)
    print(f"src   : {args.src} ({size} bytes)")
    print(f"dest  : {args.repo}:{args.path}")

    if args.dry_run:
        print("dry-run: nothing uploaded")
        return 0

    token = os.environ.get("HF_TOKEN")
    if not token:
        print("HF_TOKEN is not set in the environment")
        return 2

    from huggingface_hub import HfApi

    api = HfApi(token=token)
    api.upload_file(
        path_or_fileobj=args.src,
        path_in_repo=args.path,
        repo_id=args.repo,
        repo_type="dataset",
        commit_message=args.message,
    )
    print("upload call returned; verifying remote state")

    info = api.repo_info(args.repo, repo_type="dataset", files_metadata=True)
    hit = [s for s in info.siblings if s.rfilename == args.path]
    if not hit:
        print(f"NOT FOUND on remote: {args.path}")
        print("remote files:", [s.rfilename for s in info.siblings][:20])
        return 3

    remote_size = hit[0].size
    print(f"remote: {args.path} = {remote_size} bytes (local {size})")
    if remote_size != size:
        print("size mismatch -- treating as failure")
        return 4

    print("verified: sizes match")
    return 0


if __name__ == "__main__":
    sys.exit(main())
