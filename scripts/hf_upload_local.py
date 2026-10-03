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
    ".tar.gz", ".tgz", ".tar", ".zip", ".gz", ".xz", ".bz2", ".7z",
)


def ensure_repo(api, repo):
    """Create a missing dataset repo as PRIVATE (never public by default); an existing repo is left as it is."""
    if not api.repo_exists(repo, repo_type="dataset"):
        api.create_repo(repo, repo_type="dataset", private=True)
        print(f"created private dataset repo {repo}")


def upload_dir(args) -> int:
    """Upload exactly the checked regular files under a directory (no symlinks, raw/archive suffixes refused)
    in one commit, then verify every file's size on the remote."""
    from huggingface_hub import CommitOperationAdd, HfApi
    files = []
    for root, _, names in os.walk(args.src):
        for n in sorted(names):
            files.append(os.path.join(root, n))
    if not files:
        print("refusing: empty directory")
        return 1
    links = [f for f in files if os.path.islink(f) or not os.path.isfile(f)]
    if links:
        print("refusing: symlinks or non-regular files inside the directory:", links[:5])
        return 1
    bad = [f for f in files if any(f.lower().endswith(s) for s in REFUSED_SUFFIXES)]
    if bad:
        print("refusing: raw/archive extension inside the directory:", bad[:5])
        return 1
    base = args.path.strip("/")
    if not base:
        print("refusing: empty destination path")
        return 1
    local = {base + "/" + os.path.relpath(f, args.src).replace(os.sep, "/"): (f, os.path.getsize(f)) for f in files}
    print(f"src   : {args.src} ({len(local)} files, {sum(v[1] for v in local.values())} bytes)")
    print(f"dest  : {args.repo}:{base}/")
    if args.dry_run:
        print("dry-run: nothing uploaded")
        return 0
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("HF_TOKEN is not set in the environment")
        return 2
    api = HfApi(token=token)
    ensure_repo(api, args.repo)
    ops = [CommitOperationAdd(path_in_repo=k, path_or_fileobj=v[0]) for k, v in local.items()]
    api.create_commit(repo_id=args.repo, repo_type="dataset", operations=ops, commit_message=args.message)
    remote = {}
    for e in api.list_repo_tree(args.repo, path_in_repo=base, recursive=True, repo_type="dataset"):
        if getattr(e, "size", None) is not None and not hasattr(e, "tree_id"):
            remote[e.path] = e.size
    miss = [k for k, v in local.items() if remote.get(k) != v[1]]
    if miss:
        print(f"{len(miss)} files missing or size mismatch on remote, e.g. {miss[:5]}")
        return 4
    print(f"verified: all {len(local)} files present with matching sizes")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="local path on this host")
    ap.add_argument("--repo", required=True, help="HF dataset repo id (owner/name)")
    ap.add_argument("--path", required=True, help="destination path inside the repo")
    ap.add_argument("--message", default="upload derived artifact")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if os.path.isdir(args.src):
        return upload_dir(args)
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
    ensure_repo(api, args.repo)
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

    # Record a "published" marker so disk_watchdog.sh can reclaim the local copy
    # later WITHOUT needing network access or a token. The watchdog only deletes
    # a file whose current size still matches what we verified on HF.
    try:
        import hashlib
        import json
        import time

        h = hashlib.sha256()
        with open(args.src, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        marker_dir = "/home/yasu/geo-ml/.hf_published"
        os.makedirs(marker_dir, exist_ok=True)
        marker = os.path.join(marker_dir, os.path.basename(args.src) + ".json")
        with open(marker, "w") as fh:
            json.dump(
                {
                    "src": os.path.abspath(args.src),
                    "repo": args.repo,
                    "path_in_repo": args.path,
                    "size": size,
                    "sha256": h.hexdigest(),
                    "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
                fh,
                indent=1,
            )
        print(f"marker written: {marker}")
    except Exception as e:  # marker is a convenience, not the upload's success
        print(f"warning: could not write marker ({e.__class__.__name__}: {e})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
