#!/usr/bin/env python3
"""Publish a whole directory of derived artifacts to a Hugging Face dataset.

Same contract as hf_upload_local.py, but for a tree instead of one file, so a
1.2G cache does not need 300 workflow dispatches. Runs on the RPi5 self-hosted
runner: the files live on that box while HF_TOKEN stays in GitHub Secrets.

Every file is verified against the remote size after upload, and only then does
it get a marker under /home/yasu/geo-ml/.hf_published so disk_watchdog.sh can
reclaim the local copy later without network access or a token.

Refusals are deliberately broad, because the destination dataset is public and
NIED Hi-net / S-net redistribution is prohibited outright:
  - raw/archive file extensions
  - any path that mentions hinet, s-net/snet, obp or win32

  python3 hf_upload_dir.py --src /home/yasu/geo-ml/jma --repo owner/name \
      --prefix derived/jma --message "why" [--dry-run]
"""
import argparse
import fnmatch
import hashlib
import json
import os
import sys
import time

REFUSED_SUFFIXES = (
    ".mseed", ".sac", ".cnt", ".win32", ".sacpz",
    ".tar.gz", ".tgz", ".tar", ".zip",
)
REFUSED_PATH_WORDS = ("hinet", "snet", "s-net", "obp", "win32")
SKIP_DIR_NAMES = {"__pycache__", ".git", "venv", ".venv", "site-packages"}
MARKER_DIR = "/home/yasu/geo-ml/.hf_published"


def refused(path):
    low = path.lower()
    if any(low.endswith(s) for s in REFUSED_SUFFIXES):
        return "raw/archive extension"
    for w in REFUSED_PATH_WORDS:
        if w in low:
            return "path mentions " + w + " (redistribution prohibited)"
    return None


def collect(src, include, max_bytes):
    picked, skipped = [], []
    for root, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if d not in SKIP_DIR_NAMES]
        for name in sorted(files):
            full = os.path.join(root, name)
            if os.path.islink(full) or not os.path.isfile(full):
                continue
            if include and not fnmatch.fnmatch(name, include):
                continue
            why = refused(full)
            if why:
                skipped.append((full, why))
                continue
            size = os.path.getsize(full)
            if size == 0:
                skipped.append((full, "empty"))
                continue
            if max_bytes and size > max_bytes:
                skipped.append((full, "larger than --max-file-mb"))
                continue
            picked.append((full, size))
    return picked, skipped


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_marker(src, repo, path_in_repo, size):
    """Marker name must be unique per source path, not per basename: two dirs
    can both hold a features.npz and the second would silently overwrite the
    first, leaving disk_watchdog.sh able to reclaim only one of them."""
    os.makedirs(MARKER_DIR, exist_ok=True)
    tag = hashlib.sha1(os.path.abspath(src).encode("utf-8")).hexdigest()[:8]
    marker = os.path.join(MARKER_DIR, os.path.basename(src) + "-" + tag + ".json")
    with open(marker, "w") as fh:
        json.dump(
            {
                "src": os.path.abspath(src),
                "repo": repo,
                "path_in_repo": path_in_repo,
                "size": size,
                "sha256": sha256_of(src),
                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            fh,
            indent=1,
        )
    return marker


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="local directory on this host")
    ap.add_argument("--repo", required=True, help="HF dataset repo id (owner/name)")
    ap.add_argument("--prefix", required=True, help="destination prefix inside the repo")
    ap.add_argument("--include", default="", help="optional fnmatch on the file name")
    ap.add_argument("--max-file-mb", type=int, default=2048)
    ap.add_argument("--message", default="publish derived artifacts")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not os.path.isdir(args.src):
        print("not a directory: " + args.src)
        return 1

    max_bytes = args.max_file_mb * 1024 * 1024
    picked, skipped = collect(args.src, args.include, max_bytes)
    total = sum(s for _, s in picked)
    print("src    : " + args.src)
    print("dest   : " + args.repo + ":" + args.prefix)
    print("picked : " + str(len(picked)) + " files, " + str(total) + " bytes")
    for f, why in skipped:
        print("  SKIP " + f + "  (" + why + ")")
    if not picked:
        print("nothing to upload")
        return 0
    if args.dry_run:
        for f, s in picked:
            print("  DRY " + f + "  " + str(s))
        print("dry-run: nothing uploaded")
        return 0

    token = os.environ.get("HF_TOKEN")
    if not token:
        print("HF_TOKEN is not set in the environment")
        return 2

    from huggingface_hub import HfApi

    api = HfApi(token=token)
    dest_of = {}
    for full, _ in picked:
        rel = os.path.relpath(full, args.src).replace(os.sep, "/")
        dest_of[full] = args.prefix.rstrip("/") + "/" + rel

    for full, size in picked:
        api.upload_file(
            path_or_fileobj=full,
            path_in_repo=dest_of[full],
            repo_id=args.repo,
            repo_type="dataset",
            commit_message=args.message,
        )
        print("uploaded " + dest_of[full])

    print("verifying remote state")
    info = api.repo_info(args.repo, repo_type="dataset", files_metadata=True)
    remote = {s.rfilename: s.size for s in info.siblings}

    ok, bad = 0, []
    for full, size in picked:
        dest = dest_of[full]
        if dest not in remote:
            bad.append((dest, "missing on remote"))
            continue
        if remote[dest] != size:
            bad.append((dest, "size " + str(remote[dest]) + " != local " + str(size)))
            continue
        write_marker(full, args.repo, dest, size)
        ok += 1

    print("verified " + str(ok) + " of " + str(len(picked)) + " files")
    for dest, why in bad:
        print("  FAIL " + dest + "  (" + why + ")")
    if bad:
        return 3
    print("markers written under " + MARKER_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
