#!/usr/bin/env python3
"""HubSpot example: emit a CSV for the generic ingestion DAB generator.

HubSpot connector tables are mostly known ahead of time, and custom objects may be discovered.
This discovery adapter emits the standard CSV schema with one row per HubSpot object/table.

This script does NOT call the connector; it calls HubSpot APIs directly to optionally discover custom objects.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List

import requests


STANDARD_TABLES = [
    "contacts",
    "companies",
    "deals",
    "tickets",
    "calls",
    "emails",
    "meetings",
    "tasks",
    "notes",
]

# Proxy weights: tune based on expected volumes.
DEFAULT_WEIGHTS: Dict[str, int] = {
    "contacts": 120,
    "companies": 40,
    "deals": 60,
    "tickets": 80,
    "calls": 30,
    "emails": 30,
    "meetings": 20,
    "tasks": 20,
    "notes": 15,
}


def _discover_custom_objects(access_token: str, base_url: str = "https://api.hubapi.com") -> List[str]:
    """Return custom object names (lowercased)."""
    url = base_url.rstrip("/") + "/crm/v3/schemas"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=60)
    if r.status_code != 200:
        return []
    data = r.json() or {}
    out: List[str] = []

    # HubSpot standard objectTypeIds (common): contacts 0-1, companies 0-2, deals 0-3, tickets 0-5.
    standard_ids = {"0-1", "0-2", "0-3", "0-5"}

    for schema in data.get("results", []) or []:
        object_type = (schema.get("objectTypeId") or "").strip()
        name = (schema.get("name") or "").strip()
        if not name:
            continue
        if object_type and object_type not in standard_ids:
            out.append(name.lower())

    return sorted(set(out))


def main() -> int:
    p = argparse.ArgumentParser(description="Emit ingestion-generator CSV for HubSpot connector tables.")
    p.add_argument("--output-csv", required=True, help="Output CSV path.")
    p.add_argument("--connection-name", default="", help="Optional: set connection_name column value.")
    p.add_argument("--dest-catalog", default="main", help="Destination catalog.")
    p.add_argument("--dest-schema", default="bronze", help="Destination schema.")
    p.add_argument("--schedule", default="", help="Optional schedule (5-field cron or Quartz cron).")

    p.add_argument("--include-custom", action="store_true", help="Discover and include custom objects (requires access token).")
    p.add_argument("--access-token", default="", help="HubSpot private app token (only needed for --include-custom).")
    p.add_argument("--base-url", default="https://api.hubapi.com", help="HubSpot API base URL.")

    args = p.parse_args()

    tables = list(STANDARD_TABLES)
    if args.include_custom:
        token = (args.access_token or "").strip()
        if not token:
            raise SystemExit("--include-custom requires --access-token")
        tables.extend(_discover_custom_objects(token, base_url=args.base_url))

    out_path = Path(args.output_csv).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

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

        for t in tables:
            writer.writerow(
                {
                    "connection_name": (args.connection_name or "").strip(),
                    "source_table": t,
                    "destination_catalog": args.dest_catalog,
                    "destination_schema": args.dest_schema,
                    "destination_table": f"hubspot_{t}",
                    "pipeline_group": "",  # leave empty -> use --num-pipelines for auto-balance
                    "schedule": args.schedule,
                    "table_options_json": json.dumps({}, separators=(",", ":")),
                    "weight": str(int(DEFAULT_WEIGHTS.get(t, 5))),
                }
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
