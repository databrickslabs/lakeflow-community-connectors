#!/usr/bin/env python3
"""GitHub example: discover repositories and emit a CSV for the generic ingestion DAB generator.

This is a *discovery adapter* that outputs the standard CSV schema consumed by:
  tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py

What it does:
- Lists repos for an org or user using the GitHub REST API
- Emits one CSV row per repo for a chosen repository-scoped table (default: issues)
- Uses a proxy weight (default: open_issues_count + size) for load balancing

Notes:
- The GitHub community connector requires a PAT at connection level, so this script
  also requires a token to discover repos reliably.
- This script does NOT call the connector; it calls the GitHub API directly.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import requests


@dataclass
class Repo:
    owner: str
    name: str
    open_issues_count: int
    size: int  # GitHub repo size (KB)


def _headers(token: str) -> Dict[str, str]:
    h = {"Accept": "application/vnd.github+json"}
    token = (token or "").strip()
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _list_repos(base_url: str, token: str, *, org: str = "", user: str = "", per_page: int = 100, max_pages: int = 50) -> List[Repo]:
    if bool(org) == bool(user):
        raise ValueError("Provide exactly one of --org or --user")

    base_url = base_url.rstrip("/")
    if org:
        url = f"{base_url}/orgs/{org}/repos"
        owner = org
    else:
        url = f"{base_url}/users/{user}/repos"
        owner = user

    sess = requests.Session()
    sess.headers.update(_headers(token))

    out: List[Repo] = []
    page = 1
    while page <= max_pages:
        r = sess.get(url, params={"per_page": per_page, "page": page, "sort": "updated"}, timeout=60)
        r.raise_for_status()
        items = r.json() or []
        if not items:
            break
        for it in items:
            name = it.get("name")
            if not name:
                continue
            out.append(
                Repo(
                    owner=owner,
                    name=name,
                    open_issues_count=int(it.get("open_issues_count") or 0),
                    size=int(it.get("size") or 0),
                )
            )
        if len(items) < per_page:
            break
        page += 1

    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Discover GitHub repos and emit ingestion-generator CSV.")
    p.add_argument("--token", required=True, help="GitHub PAT used to call the GitHub API.")
    p.add_argument("--base-url", default="https://api.github.com", help="GitHub API base URL (GHE: https://<host>/api/v3).")

    p.add_argument("--org", default="", help="GitHub org to discover repos for.")
    p.add_argument("--user", default="", help="GitHub user to discover repos for.")

    p.add_argument("--table", default="issues", help="Repository-scoped connector table to generate rows for (e.g., issues, pull_requests, comments, commits).")
    p.add_argument("--start-date", default="", help="Optional ISO start_date for CDC tables (e.g., 2024-01-01T00:00:00Z).")
    p.add_argument("--state", default="all", help="Optional state for issues/pull_requests/comments where applicable.")

    p.add_argument("--output-csv", required=True, help="Output CSV path.")
    p.add_argument("--connection-name", default="", help="Optional: set connection_name column value.")
    p.add_argument("--dest-catalog", default="main", help="Destination catalog.")
    p.add_argument("--dest-schema", default="bronze", help="Destination schema.")

    p.add_argument("--per-page", type=int, default=100, help="GitHub pagination page size.")
    p.add_argument("--max-pages", type=int, default=20, help="Max pages to fetch for repo listing.")

    p.add_argument("--schedule", default="", help="Optional schedule (5-field cron or Quartz cron).")

    args = p.parse_args()

    repos = _list_repos(
        args.base_url,
        args.token,
        org=args.org,
        user=args.user,
        per_page=int(args.per_page),
        max_pages=int(args.max_pages),
    )

    out_path = Path(args.output_csv).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Weight proxy: open issues + size (KB). You can swap this for stars, forks, etc.
    def weight_for(r: Repo) -> int:
        w = (r.open_issues_count or 0) + max(1, int(r.size or 0) // 100)  # size scaled down
        return max(1, int(w))

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "connection_name",
                "source_table",
                "destination_catalog",
                "destination_schema",
                "destination_table",
                "pipeline_group",
                "schedule",
                "table_options_json",
                "weight",
            ],
        )
        writer.writeheader()

        for r in repos:
            table_opts: Dict[str, object] = {"owner": r.owner, "repo": r.name}
            if args.state:
                table_opts["state"] = args.state
            if args.start_date:
                table_opts["start_date"] = args.start_date

            # One destination table per repo per table.
            dest_table = f"github_{args.table}_{r.owner}_{r.name}".lower().replace("-", "_")

            writer.writerow(
                {
                    "connection_name": (args.connection_name or "").strip(),
                    "source_table": args.table,
                    "destination_catalog": args.dest_catalog,
                    "destination_schema": args.dest_schema,
                    "destination_table": dest_table,
                    "pipeline_group": "",  # leave empty -> use --num-pipelines for auto-balance
                    "schedule": args.schedule,
                    "table_options_json": json.dumps(table_opts, separators=(",", ":")),
                    "weight": str(weight_for(r)),
                }
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
