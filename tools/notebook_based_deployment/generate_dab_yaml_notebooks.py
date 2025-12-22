#!/usr/bin/env python3
"""
Generate DAB YAML for notebook-based DLT pipelines.

Creates pipeline and job definitions that reference DLT notebooks
(for connectors where ingestion_definition doesn't auto-inject secrets).

Usage:
    python generate_dab_yaml_notebooks.py \
        --connector-name osipi \
        --input-csv metadata.csv \
        --output-yaml osipi_pipelines.yml \
        --notebook-base-path /Workspace/Users/user@company.com/dlt_pipelines \
        --dest-catalog osipi \
        --dest-schema bronze
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
    notebook_base_path: str,
    dest_catalog: str,
    dest_schema: str,
    emit_jobs: bool = True,
    pause_jobs: bool = True,
    cluster_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Generate DAB YAML for notebook-based pipelines."""
    
    # Read CSV and group by pipeline_group
    groups = defaultdict(list)
    schedules = {}
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_table = row.get('source_table', '').strip()
            if not source_table:
                continue
            
            group = row.get('pipeline_group', '').strip() or 'default'
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
        "variables": {
            "dest_catalog": {"default": dest_catalog},
            "dest_schema": {"default": dest_schema},
        },
        "resources": {
            "pipelines": {},
            "jobs": {}
        }
    }
    
    # Generate pipeline for each group
    for group in sorted(groups.keys()):
        table_count = len(groups[group])
        pipeline_key = f"pipeline_{connector_name}_{group}"
        
        # Pipeline definition
        pipeline_def = {
            "name": f"{connector_name.upper()} - {group.replace('_', ' ').title()}",
            "catalog": "${var.dest_catalog}",
            "target": "${var.dest_schema}",
            "development": True,
            "continuous": False,
            "libraries": [
                {
                    "notebook": {
                        "path": f"{notebook_base_path}/{connector_name}_{group}"
                    }
                }
            ],
            "clusters": [
                {
                    "label": "default",
                    **cluster_config
                }
            ]
        }
        
        dab["resources"]["pipelines"][pipeline_key] = pipeline_def
        
        # Generate job if schedule exists
        if emit_jobs and group in schedules:
            job_key = f"job_{connector_name}_{group}"
            
            job_def = {
                "name": f"{connector_name.upper()} Scheduler - {group.replace('_', ' ').title()}",
                "schedule": {
                    "quartz_cron_expression": convert_cron_to_quartz(schedules[group]),
                    "timezone_id": "UTC",
                    "pause_status": "PAUSED" if pause_jobs else "UNPAUSED"
                },
                "tasks": [
                    {
                        "task_key": f"run_{connector_name}_{group}",
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
        description="Generate DAB YAML for notebook-based DLT pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  # OSI PI - Generate YAML for notebook-based pipelines
  python generate_dab_yaml_notebooks.py \\
    --connector-name osipi \\
    --input-csv examples/osipi/osipi_tables_metadata.csv \\
    --output-yaml dab_template/resources/osipi_pipelines.yml \\
    --notebook-base-path /Workspace/Users/pravin.varma@databricks.com/osipi_dlt_pipelines \\
    --dest-catalog osipi \\
    --dest-schema bronze \\
    --emit-jobs

  # HubSpot
  python generate_dab_yaml_notebooks.py \\
    --connector-name hubspot \\
    --input-csv examples/hubspot/tables.csv \\
    --output-yaml dab_template/resources/hubspot_pipelines.yml \\
    --notebook-base-path /Workspace/Users/user@company.com/hubspot_pipelines \\
    --dest-catalog marketing \\
    --dest-schema bronze

  # With custom cluster config
  python generate_dab_yaml_notebooks.py \\
    ... \\
    --cluster-workers 4 \\
    --cluster-instance-pool-id pool-xyz
        """
    )
    
    parser.add_argument("--connector-name", required=True, help="Connector name")
    parser.add_argument("--input-csv", required=True, help="Metadata CSV")
    parser.add_argument("--output-yaml", required=True, help="Output YAML path")
    parser.add_argument("--notebook-base-path", required=True, 
                       help="Workspace path where notebooks are uploaded")
    parser.add_argument("--dest-catalog", default="main", help="Destination catalog")
    parser.add_argument("--dest-schema", default="bronze", help="Destination schema")
    parser.add_argument("--emit-jobs", action="store_true", help="Generate scheduled jobs")
    parser.add_argument("--unpause-jobs", action="store_true", help="Create jobs as UNPAUSED")
    parser.add_argument("--cluster-workers", type=int, default=1, help="Number of workers per cluster")
    parser.add_argument("--cluster-instance-pool-id", help="Instance pool ID for clusters")
    
    args = parser.parse_args()
    
    # Build cluster config
    cluster_config = {"num_workers": args.cluster_workers}
    if args.cluster_instance_pool_id:
        cluster_config["instance_pool_id"] = args.cluster_instance_pool_id
    
    # Generate YAML
    dab = generate_dab_yaml(
        args.connector_name,
        Path(args.input_csv),
        args.notebook_base_path,
        args.dest_catalog,
        args.dest_schema,
        emit_jobs=args.emit_jobs,
        pause_jobs=not args.unpause_jobs,
        cluster_config=cluster_config
    )
    
    # Write YAML
    output_path = Path(args.output_yaml)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        yaml.safe_dump(dab, f, sort_keys=False, default_flow_style=False)
    
    print(f"\n✓ Generated DAB YAML: {output_path}")
    print(f"  Pipelines: {len(dab['resources']['pipelines'])}")
    if 'jobs' in dab['resources']:
        print(f"  Jobs: {len(dab['resources']['jobs'])}")


if __name__ == "__main__":
    main()
