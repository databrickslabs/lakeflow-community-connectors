#!/usr/bin/env python3
"""Live smoke test for the GitHub read-only ingestion story.

Standalone (uses only ``requests``, no pyspark), so it runs on a laptop with a
real fine-grained PAT. It exercises the exact endpoints the ``repository_files``
table uses, then attempts a write to prove the token cannot mutate the repo.

This is the evidence a security team wants: reads succeed, writes are refused.

Usage:
    GITHUB_TOKEN=<fine-grained-read-only-pat> \
        python tools/scripts/smoke_repository_files.py <owner> <repo> [ref]

Exit code 0 means: reads worked AND the write attempt was correctly rejected.
"""

import base64
import os
import sys

import requests

API = os.environ.get("GITHUB_API", "https://api.github.com").rstrip("/")


def _session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    )
    return s


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("ERROR: set GITHUB_TOKEN to a fine-grained read-only PAT")
        return 2

    owner, repo = sys.argv[1], sys.argv[2]
    ref = sys.argv[3] if len(sys.argv) > 3 else None
    s = _session(token)

    # 1. Default branch (GET /repos/{owner}/{repo}) -- Metadata: Read
    r = s.get(f"{API}/repos/{owner}/{repo}", timeout=30)
    r.raise_for_status()
    ref = ref or (r.json().get("default_branch") or "main")
    print(f"[read] repo ok, ref = {ref}")

    # 2. Resolve ref -> tree (GET /commits/{ref}) -- Contents: Read
    r = s.get(f"{API}/repos/{owner}/{repo}/commits/{ref}", timeout=30)
    r.raise_for_status()
    commit = r.json()
    tree_sha = commit["commit"]["tree"]["sha"]
    print(f"[read] commit {commit['sha'][:10]} tree {tree_sha[:10]}")

    # 3. List tree (GET /git/trees/{sha}?recursive=1) -- Contents: Read
    r = s.get(
        f"{API}/repos/{owner}/{repo}/git/trees/{tree_sha}",
        params={"recursive": "1"},
        timeout=30,
    )
    r.raise_for_status()
    tree = r.json()
    blobs = [t for t in tree.get("tree", []) if t.get("type") == "blob"]
    print(f"[read] tree listed: {len(blobs)} files (truncated={tree.get('truncated')})")

    # 4. Fetch up to 3 blobs (GET /git/blobs/{sha}) -- Contents: Read
    for blob in blobs[:3]:
        rb = s.get(f"{API}/repos/{owner}/{repo}/git/blobs/{blob['sha']}", timeout=30)
        rb.raise_for_status()
        data = rb.json()
        raw = base64.b64decode(data.get("content") or "")
        kind = "text" if _is_text(raw) else "binary"
        print(f"[read] blob {blob['path']} ({len(raw)} bytes, {kind})")

    # 5. Attempt a WRITE -- must be refused for a read-only token.
    w = s.post(
        f"{API}/repos/{owner}/{repo}/issues",
        json={"title": "smoke-test-should-be-rejected"},
        timeout=30,
    )
    if 200 <= w.status_code < 300:
        print(f"[write] FAIL: write succeeded (HTTP {w.status_code}) -- token is NOT read-only")
        return 1
    print(f"[write] refused as expected: HTTP {w.status_code}")

    print("\nSMOKE PASS: reads work, writes are refused. Token is read-only.")
    return 0


def _is_text(raw: bytes) -> bool:
    try:
        raw.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False


if __name__ == "__main__":
    raise SystemExit(main())
