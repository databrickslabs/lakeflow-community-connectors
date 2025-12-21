#!/usr/bin/env python3
"""Zendesk example: emit a CSV for the generic ingestion DAB generator.

Zendesk connector tables are static and typically do not require per-table options.
This discovery adapter simply outputs a standard CSV with one row per Zendesk table.

You can assign custom weights (proxy) to influence load balancing.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict


DEFAULT_TABLES = [
    "tickets",
    "ticket_comments",
    "users",
    "organizations",
    "articles",
    "groups",
    "brands",
    "topics",
]

# Proxy weights: tune based on expected volume in your Zendesk instance.
DEFAULT_WEIGHTS: Dict[str, int] = {
    "tickets": 100,
    "ticket_comments": 120,
    "users": 20,
    "organizations": 10,
    "articles": 10,
    "groups": 5,
    "brands": 5,
    "topics": 5,
}


def main() -> int:
    p = argparse.ArgumentParser(description="Emit ingestion-generator CSV for Zendesk connector tables.")
    p.add_argument("--output-csv", required=True, help="Output CSV path.")
    p.add_argument("--connection-name", default="", help="Optional: set connection_name column value.")
    p.add_argument("--dest-catalog", default="main", help="Destination catalog.")
    p.add_argument("--dest-schema", default="bronze", help="Destination schema.")
    p.add_argument("--schedule", default="", help="Optional schedule (5-field cron or Quartz cron).")
    p.add_argument("--tables", default=",".join(DEFAULT_TABLES), help="Comma-separated list of tables to include.")

    args = p.parse_args()

    tables = [t.strip() for t in (args.tables or "").split(",") if t.strip()]
    if not tables:
        raise SystemExit("No tables provided")

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
                    "destination_table": f"zendesk_{t}",
                    "pipeline_group": "",  # leave empty -> use --num-pipelines for auto-balance
                    "schedule": args.schedule,
                    "table_options_json": json.dumps({}, separators=(",", ":")),
                    "weight": str(int(DEFAULT_WEIGHTS.get(t, 1))),
                }
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
