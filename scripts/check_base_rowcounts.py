"""Verify a restored base DB against the canonical row-count manifest.

The 2026-07-24 incident: the light job restored nothing, initialised an empty
DB and silently rebuilt the canonical history from scratch. The guard added in
PR #197 catches `restored=none`, but the restore loop has a second way to hand
back a thin base -- `<artifact>-salvaged`, a per-table copy out of a corrupt
checkpoint, which is table-incomplete by construction (the reseed workflow's
header describes an earlier "degraded ~587MB salvage" sitting at the head of
the chain).

hf_sync.py already publishes the counts we need: after every verified upload it
writes <path>.rowcounts.json next to the canonical DB on Hugging Face. The
dataset is public, so the manifest is readable over plain HTTPS with no token.
Backfill is append-only, so a restored base should never sit below the manifest.

Exit 1 when a table regressed; exit 0 otherwise, including when the manifest is
unreachable -- an outage at Hugging Face must not stop the pipeline.
"""
import argparse
import json
import sqlite3
import sys
import urllib.error
import urllib.request

MANIFEST_URL = "https://huggingface.co/datasets/{repo}/resolve/main/{path}.rowcounts.json"


def remote_manifest(repo: str, path: str, timeout: int):
    url = MANIFEST_URL.format(repo=repo, path=path)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.load(r)
    except (urllib.error.URLError, ValueError, TimeoutError) as e:
        print(f"::warning::row-count manifest unreachable ({e}) -- skipping base check")
        return None


def local_counts(src: str, tables):
    conn = sqlite3.connect(src)
    out = {}
    try:
        for t in tables:
            try:
                out[t] = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            except sqlite3.Error:
                out[t] = 0  # missing table reads as a total loss
    finally:
        conn.close()
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--src", required=True)
    p.add_argument("--repo", default="yasumorishima/japan-geohazard")
    p.add_argument("--path", default="geohazard.db")
    p.add_argument("--min-fraction", type=float, default=0.95,
                   help="matches hf_sync.py's guard")
    p.add_argument("--timeout", type=int, default=60)
    a = p.parse_args()

    prev = remote_manifest(a.repo, a.path, a.timeout)
    if not prev:
        return 0

    prev = {t: n for t, n in prev.items() if n > 0}
    loc = local_counts(a.src, prev.keys())

    regressed = [
        f"{t}: base={loc[t]:,} < {a.min_fraction:.0%} of canonical {prev[t]:,}"
        for t in sorted(prev)
        if loc[t] < prev[t] * a.min_fraction
    ]
    print(f"checked {len(prev)} tables against {a.repo}/{a.path}.rowcounts.json")
    if not regressed:
        print("base row counts OK (no table below the canonical floor)")
        return 0

    for line in regressed:
        print(f"  {line}")
    print(
        f"::error::restored base regressed in {len(regressed)} table(s) -- refusing "
        "to fetch onto a degraded base. Run reseed-checkpoint-from-hf.yml to "
        "restore the chain head from the Hugging Face canonical copy."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
