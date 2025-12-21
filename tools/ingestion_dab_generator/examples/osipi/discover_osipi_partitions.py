#!/usr/bin/env python3
"""
OSIPI example: discover plant tag counts and emit a CSV suitable for the generic ingestion DAB generator.

This script targets PI Web API-compatible endpoints, including hosted test endpoints.

It generates rows that split ingestion by plant using `nameFilter`:
  - source_table: pi_timeseries
  - destination_table: pi_timeseries_<plant_lower>
  - table_options_json: {"nameFilter": "<Plant>_*", "lookback_minutes": 60, ...}

The per-plant tag count is written as `weight` for load-balancing.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests


@dataclass
class PlantInfo:
    plant: str
    tag_count: int


def _safe_table_suffix(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in s).strip("_")


def _get_dataserver_webid(pi_base_url: str, headers: Dict[str, str], verify_ssl: bool) -> str:
    r = requests.get(f"{pi_base_url}/piwebapi/dataservers", headers=headers, timeout=60, verify=verify_ssl)
    r.raise_for_status()
    items = (r.json() or {}).get("Items") or []
    if not items or not items[0].get("WebId"):
        raise RuntimeError("No dataservers returned from PI Web API.")
    return items[0]["WebId"]


def _count_points(
    *,
    pi_base_url: str,
    dataserver_webid: str,
    name_filter: str,
    headers: Dict[str, str],
    page_size: int,
    max_total: int,
    verify_ssl: bool,
) -> int:
    start_index = 0
    total = 0
    while start_index < max_total:
        params = {"nameFilter": name_filter, "maxCount": str(page_size), "startIndex": str(start_index)}
        r = requests.get(
            f"{pi_base_url}/piwebapi/dataservers/{dataserver_webid}/points",
            headers=headers,
            params=params,
            timeout=60,
            verify=verify_ssl,
        )
        r.raise_for_status()
        items = (r.json() or {}).get("Items") or []
        n = len(items)
        total += n
        if n < page_size:
            break
        start_index += page_size
    return total


def main() -> int:
    p = argparse.ArgumentParser(description="Discover OSIPI plant partitions and emit generator CSV.")
    p.add_argument("--pi-base-url", required=True, help="PI base URL (no trailing slash).")
    p.add_argument("--access-token", default="", help="Bearer token (without 'Bearer ').")
    p.add_argument("--plants", required=True, help="Comma-separated plant prefixes, e.g. Sydney,Melbourne,Brisbane")
    p.add_argument("--output-csv", required=True, help="Output CSV path.")
    p.add_argument("--dest-catalog", default="main", help="Destination catalog.")
    p.add_argument("--dest-schema", default="bronze", help="Destination schema.")
    p.add_argument("--connection-name", default="", help="Optional connection_name column value.")

    p.add_argument("--lookback-minutes", type=int, default=60, help="Default lookback for pi_timeseries.")
    p.add_argument("--maxCount", type=int, default=1000, help="Default maxCount for pi_timeseries reads.")
    p.add_argument("--window-seconds", type=int, default=0, help="Optional window_seconds for time chunking.")

    p.add_argument("--verify-ssl", action="store_true", help="Verify TLS certs (default).")
    p.add_argument("--no-verify-ssl", action="store_true", help="Disable TLS verification (development/testing only).")

    p.add_argument("--page-size", type=int, default=10000, help="Points pagination page size.")
    p.add_argument("--max-total", type=int, default=200000, help="Max points to count per plant.")

    args = p.parse_args()

    pi_base_url = args.pi_base_url.rstrip("/")
    verify_ssl = True
    if args.no_verify_ssl:
        verify_ssl = False
    elif args.verify_ssl:
        verify_ssl = True

    headers: Dict[str, str] = {"Accept": "application/json"}
    token = (args.access_token or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    plants = [x.strip() for x in (args.plants or "").split(",") if x.strip()]
    if not plants:
        raise SystemExit("No plants provided.")

    dataserver_webid = _get_dataserver_webid(pi_base_url, headers, verify_ssl)

    plant_infos: List[PlantInfo] = []
    for plant in plants:
        name_filter = f"{plant}_*"
        n = _count_points(
            pi_base_url=pi_base_url,
            dataserver_webid=dataserver_webid,
            name_filter=name_filter,
            headers=headers,
            page_size=int(args.page_size),
            max_total=int(args.max_total),
            verify_ssl=verify_ssl,
        )
        plant_infos.append(PlantInfo(plant=plant, tag_count=n))

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

        for info in plant_infos:
            plant = info.plant
            suffix = _safe_table_suffix(plant)
            table_options = {
                "nameFilter": f"{plant}_*",
                "lookback_minutes": int(args.lookback_minutes),
                "maxCount": int(args.maxCount),
            }
            if int(args.window_seconds) > 0:
                table_options["window_seconds"] = int(args.window_seconds)

            writer.writerow(
                {
                    "connection_name": (args.connection_name or "").strip(),
                    "source_table": "pi_timeseries",
                    "destination_catalog": args.dest_catalog,
                    "destination_schema": args.dest_schema,
                    "destination_table": f"pi_timeseries_{suffix}",
                    # leave blank to let the generator auto-assign with --num-pipelines
                    "pipeline_group": "",
                    "schedule": "",
                    "table_options_json": json.dumps(table_options),
                    "weight": str(info.tag_count or 1),
                }
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


