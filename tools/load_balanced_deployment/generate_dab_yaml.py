#!/usr/bin/env python3
"""
Generate Databricks Asset Bundle YAML for load-balanced multi-pipeline deployment.

This tool generates DAB YAML that references Python ingest files (not notebooks)
created by generate_ingest_files.py. Each pipeline group gets its own DLT pipeline.

Key differences from notebook-based approach:
- Uses Python files (.py) with UC Connection injection (no dbutils.secrets!)
- Works with GENERIC_LAKEFLOW_CONNECT connection type
- Generic for all community connectors (osipi, hubspot, github, etc.)

Usage:
    python generate_dab_yaml.py \
        --connector-name osipi \
        --input-csv metadata.csv \
        --output-yaml databricks.yml \
        --ingest-files-path /Workspace/Users/user@company.com/osipi_ingest \
        --connection-name osipi_connection_lakeflow \
        --catalog osipi \
        --schema bronze
"""

import argparse
import csv
import yaml
from pathlib import Path
from typing import Dict, List, Any
from collections import defaultdict


def convert_cron_to_quartz(cron_expr: str) -> str:
    """Convert 5-field cron to 6-field Quartz cron."""
    parts = cron_expr.strip().split()

    if len(parts) == 6:
        return cron_expr  # Already Quartz format

    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {cron_expr}")

    minute, hour, dom, mon, dow = parts

    # Quartz requires '?' for either dom or dow
    if dow == "*":
        dow = "?"
    else:
        dom = "?"

    return f"0 {minute} {hour} {dom} {mon} {dow}"


def generate_dab_yaml(
    connector_name: str,
    csv_path: Path,
    ingest_files_path: str,
    connection_name: str,
    catalog: str,
    schema: str,
    emit_jobs: bool = True,
    pause_jobs: bool = True,
    cluster_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Generate DAB YAML for file-based (not notebook!) load-balanced pipelines."""

    # Read CSV and group by pipeline_group
    groups = defaultdict(list)
    schedules = {}

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_table = row.get('source_table', '').strip()
            if not source_table:
                continue

            group = (row.get('pipeline_group') or row.get('category') or '').strip() or 'default'
            groups[group].append(row)

            # Save schedule for this group (first non-empty)
            if row.get('schedule', '').strip() and group not in schedules:
                schedules[group] = row['schedule'].strip()

    if not groups:
        raise ValueError("No tables found in CSV")

    # Default cluster config
    if cluster_config is None:
        cluster_config = {"num_workers": 1}

    # Build YAML structure
    dab = {
        "bundle": {
            "name": f"{connector_name}_load_balanced"
        },
        "variables": {
            "catalog": {"default": catalog},
            "schema": {"default": schema},
            "connection_name": {"default": connection_name},
            "ingest_files_path": {"default": ingest_files_path}
        },
        "resources": {
            "pipelines": {},
            "jobs": {}
        }
    }

    # Generate pipeline for each group
    for group in sorted(groups.keys()):
        table_count = len(groups[group])
        pipeline_key = f"{connector_name}_{group}"
        ingest_file = f"${{var.ingest_files_path}}/ingest_{group}.py"

        # Pipeline definition
        # CRITICAL: Community connectors REQUIRE channel: PREVIEW and serverless: true
        pipeline_def = {
            "name": f"{connector_name.upper()} Load Balanced - {group.replace('_', ' ').title()} ({table_count} tables)",
            "catalog": "${var.catalog}",
            "target": "${var.schema}",
            "channel": "PREVIEW",       # Required for PySpark Data Source API support
            "serverless": True,         # Required for community connectors
            "development": True,
            "continuous": False,
            "libraries": [
                {
                    "file": {
                        "path": ingest_file  # Python file, NOT notebook!
                    }
                }
            ]
            # NO clusters configuration when using serverless
        }

        dab["resources"]["pipelines"][pipeline_key] = pipeline_def

        # Generate job if schedule exists
        if emit_jobs and group in schedules:
            job_key = f"{connector_name}_{group}_scheduler"

            job_def = {
                "name": f"{connector_name.upper()} Load Balanced Scheduler - {group.replace('_', ' ').title()}",
                "schedule": {
                    "quartz_cron_expression": convert_cron_to_quartz(schedules[group]),
                    "timezone_id": "UTC",
                    "pause_status": "PAUSED" if pause_jobs else "UNPAUSED"
                },
                "tasks": [
                    {
                        "task_key": f"ingest_{group}",
                        "pipeline_task": {
                            "pipeline_id": f"${{resources.pipelines.{pipeline_key}.id}}"
                        }
                    }
                ]
            }

            dab["resources"]["jobs"][job_key] = job_def

    # Remove jobs section if empty
    if not dab["resources"]["jobs"]:
        del dab["resources"]["jobs"]

    return dab


def main():
    parser = argparse.ArgumentParser(
        description="Generate DAB YAML for load-balanced multi-pipeline deployment (file-based, no notebooks!)"
    )
    parser.add_argument("--connector-name", required=True, help="Connector name (e.g., osipi, hubspot, github)")
    parser.add_argument("--input-csv", required=True, help="CSV with table metadata and pipeline_group assignments")
    parser.add_argument("--output-yaml", required=True, help="Output path for generated databricks.yml")
    parser.add_argument("--ingest-files-path", required=True, help="Workspace path where ingest_*.py files are stored")
    parser.add_argument("--connection-name", required=True, help="UC Connection name (GENERIC_LAKEFLOW_CONNECT type)")
    parser.add_argument("--catalog", required=True, help="Destination catalog")
    parser.add_argument("--schema", required=True, help="Destination schema")
    parser.add_argument("--emit-jobs", action="store_true", help="Generate scheduled jobs for pipelines with schedules")
    parser.add_argument("--pause-jobs", action="store_true", default=True, help="Create jobs in PAUSED state (default: True)")
    parser.add_argument("--num-workers", type=int, default=1, help="Number of workers for pipeline clusters (default: 1)")

    args = parser.parse_args()

    csv_path = Path(args.input_csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    cluster_config = {"num_workers": args.num_workers}

    dab = generate_dab_yaml(
        connector_name=args.connector_name,
        csv_path=csv_path,
        ingest_files_path=args.ingest_files_path,
        connection_name=args.connection_name,
        catalog=args.catalog,
        schema=args.schema,
        emit_jobs=args.emit_jobs,
        pause_jobs=args.pause_jobs,
        cluster_config=cluster_config
    )

    output_path = Path(args.output_yaml)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        yaml.dump(dab, f, default_flow_style=False, sort_keys=False, indent=2)

    pipeline_count = len(dab["resources"]["pipelines"])
    job_count = len(dab["resources"].get("jobs", {}))

    print(f"✓ Generated DAB YAML: {output_path}")
    print(f"  - {pipeline_count} pipelines")
    print(f"  - {job_count} scheduled jobs")
    print(f"\nNext steps:")
    print(f"  1. Review and customize {output_path}")
    print(f"  2. Deploy: databricks bundle deploy")
    print(f"  3. Run pipelines: databricks bundle run <pipeline_key>")


if __name__ == "__main__":
    main()
