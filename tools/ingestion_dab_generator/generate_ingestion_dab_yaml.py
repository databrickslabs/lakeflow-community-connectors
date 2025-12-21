#!/usr/bin/env python3
"""
Generate Databricks Asset Bundle (DAB) YAML for Lakeflow Connect ingestion pipelines.

This tool is connector-agnostic: it takes a CSV describing "what to ingest" and emits a
single YAML file containing:
  - resources.pipelines.* entries using ingestion_definition (Lakeflow Connect ingestion)
  - (optional) resources.jobs.* entries to schedule those pipelines

It also supports auto load-balancing: if pipeline_group is missing/empty, assign rows
to N pipelines using First-Fit Decreasing (FFD) bin packing on a weight column
("row_count" / "size" / "weight", etc.).

CSV columns (minimum):
  - source_table                (required)  Lakeflow connector tableName, e.g. "pi_timeseries"
  - destination_table           (optional)  defaults to source_table
  - pipeline_group              (optional)  if absent, auto-balanced when --num-pipelines is set

Recommended / common columns:
  - connection_name             (optional)  must be the same across rows if provided
  - destination_catalog         (optional)  if absent, uses --dest-catalog
  - destination_schema          (optional)  if absent, uses --dest-schema
  - schedule                    (optional)  5-field cron or Quartz cron; creates jobs when present
  - table_options_json          (optional)  JSON object merged into table_configuration
  - weight                      (optional)  numeric weight for load balancing (default 1)

Example row (OSIPI):
  pi_timeseries,pi_timeseries_sydney,osipi,bronze,,Sydney_*,{"lookback_minutes":60,"maxCount":1000},1200
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _convert_cron_to_quartz(expr: str) -> str:
    """
    Convert standard 5-field cron (min hour dom mon dow) to Quartz 6-field cron:
      second min hour dom mon dow

    If expr already has 6+ fields, return as-is.
    """
    parts = (expr or "").strip().split()
    if len(parts) != 5:
        return expr.strip()

    minute, hour, dom, mon, dow = parts

    # Quartz requires '?' for either dom or dow when the other is specified.
    if dow == "*":
        dow = "?"
    else:
        dom = "?"

    return f"0 {minute} {hour} {dom} {mon} {dow}"


def _safe_name(s: str) -> str:
    out = []
    for ch in (s or "").strip():
        if ch.isalnum():
            out.append(ch.lower())
        else:
            out.append("_")
    name = "".join(out).strip("_")
    while "__" in name:
        name = name.replace("__", "_")
    return name or "group"


def _coerce_table_config(value: Any) -> str:
    # ingestion pipeline spec normalizes table_configuration to str->str
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def _maybe_parse_json_obj(s: str) -> Dict[str, Any]:
    s = (s or "").strip()
    if not s:
        return {}
    obj = json.loads(s)
    if not isinstance(obj, dict):
        raise ValueError("table_options_json must be a JSON object (dictionary)")
    return obj


@dataclass
class Row:
    source_table: str
    destination_catalog: Optional[str]
    destination_schema: Optional[str]
    destination_table: str
    pipeline_group: Optional[str]
    schedule: Optional[str]
    table_config: Dict[str, str]
    weight: int


def _read_rows(
    csv_path: Path,
    *,
    dest_catalog_default: str,
    dest_schema_default: str,
    connection_name_override: Optional[str],
    weight_column: str,
) -> Tuple[str, List[Row]]:
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")

        fieldnames = {c.strip() for c in reader.fieldnames if c}
        if "source_table" not in fieldnames:
            raise ValueError("CSV must include required column: source_table")

        connection_names = set()
        rows: List[Row] = []
        for raw in reader:
            source_table = (raw.get("source_table") or "").strip()
            if not source_table:
                continue

            connection_name = (raw.get("connection_name") or "").strip()
            if connection_name:
                connection_names.add(connection_name)

            dest_catalog = (raw.get("destination_catalog") or "").strip() or None
            dest_schema = (raw.get("destination_schema") or "").strip() or None
            dest_table = (raw.get("destination_table") or "").strip() or source_table

            pipeline_group = (raw.get("pipeline_group") or "").strip() or None

            # Optional: SFDC-style grouping support.
            # If pipeline_group is not provided, derive it from prefix+priority when present.
            prefix = (raw.get("prefix") or "").strip() or None
            priority = (raw.get("priority") or "").strip() or None
            if pipeline_group is None and prefix is not None:
                pipeline_group = f"{prefix}_{priority}" if priority else prefix
            schedule = (raw.get("schedule") or "").strip() or None

            table_options_json = (raw.get("table_options_json") or "").strip()
            table_options = _maybe_parse_json_obj(table_options_json)
            # allow also "table_configuration" alias (some people prefer it)
            if not table_options:
                table_options = _maybe_parse_json_obj((raw.get("table_configuration_json") or "").strip())

            # Normalize to str->str for SDP SpecParser
            table_config: Dict[str, str] = {str(k): _coerce_table_config(v) for k, v in table_options.items()}

            w_raw = (raw.get(weight_column) or "").strip()
            if not w_raw:
                w = 1
            else:
                try:
                    w = int(math.ceil(float(w_raw)))
                except Exception:
                    w = 1
            w = max(1, w)

            rows.append(
                Row(
                    source_table=source_table,
                    destination_catalog=dest_catalog or dest_catalog_default,
                    destination_schema=dest_schema or dest_schema_default,
                    destination_table=dest_table,
                    pipeline_group=pipeline_group,
                    schedule=schedule,
                    table_config=table_config,
                    weight=w,
                )
            )

    # Determine effective connection name
    if connection_name_override:
        effective_connection_name = connection_name_override
    else:
        if len(connection_names) == 0:
            raise ValueError(
                "Missing connection_name. Provide it as a CSV column or pass --connection-name."
            )
        if len(connection_names) > 1:
            raise ValueError(f"CSV contains multiple connection_name values: {sorted(connection_names)}")
        effective_connection_name = next(iter(connection_names))

    if not rows:
        raise ValueError("No usable rows found in CSV (check source_table values).")

    return effective_connection_name, rows


def _ffd_binpack(
    items: List[Tuple[int, int]],  # (index, weight)
    *,
    num_bins: int,
    max_items_per_bin: Optional[int] = None,
) -> List[int]:
    """
    First-Fit Decreasing bin packing:
      - sort by weight desc
      - place each item in the bin with the smallest current load that has capacity
    Returns: bin index for each item index (0..num_bins-1)
    """
    if num_bins <= 0:
        raise ValueError("--num-pipelines must be >= 1")

    sorted_items = sorted(items, key=lambda t: t[1], reverse=True)
    loads = [0 for _ in range(num_bins)]
    counts = [0 for _ in range(num_bins)]
    assignment = [-1 for _ in range(max((i for i, _w in items), default=-1) + 1)]

    for idx, w in sorted_items:
        candidates = []
        for b in range(num_bins):
            if max_items_per_bin is not None and counts[b] >= max_items_per_bin:
                continue
            candidates.append(b)
        if not candidates:
            raise ValueError("No bins available (max-items-per-pipeline too small).")

        b = min(candidates, key=lambda bi: loads[bi])
        loads[b] += w
        counts[b] += 1
        assignment[idx] = b

    return assignment


def _group_id_from_bin(bin_idx: int) -> str:
    # human-friendly 1-based group labels
    return str(bin_idx + 1)


def _build_yaml(
    *,
    connector_name: str,
    connection_name: str,
    dest_catalog: str,
    dest_schema: str,
    rows: List[Row],
    emit_jobs: bool,
    pause_jobs: bool,
    use_variables: bool,
) -> Dict[str, Any]:
    # Group rows by pipeline_group
    grouped: Dict[str, List[Row]] = {}
    for r in rows:
        if not r.pipeline_group:
            raise ValueError("Internal error: pipeline_group is required on all rows before YAML build.")
        grouped.setdefault(r.pipeline_group, []).append(r)

    var_connection = "connection_name"
    var_dest_catalog = "dest_catalog"
    var_dest_schema = "dest_schema"

    def vref(name: str) -> str:
        return f"${{var.{name}}}"

    root: Dict[str, Any] = {}
    if use_variables:
        root["variables"] = {
            var_dest_catalog: {"default": dest_catalog},
            var_dest_schema: {"default": dest_schema},
            var_connection: {"default": connection_name},
        }

    resources: Dict[str, Any] = {"pipelines": {}, "jobs": {}}

    for group in sorted(grouped.keys(), key=lambda x: (int(x) if x.isdigit() else 10**9, x)):
        group_rows = grouped[group]
        pipeline_key = f"pipeline_{_safe_name(connector_name)}_{_safe_name(group)}"
        pipeline_display = f"{connector_name} ingestion - group {group}"

        pipeline_def: Dict[str, Any] = {
            "name": pipeline_display,
            "catalog": vref(var_dest_catalog) if use_variables else dest_catalog,
            "ingestion_definition": {
                "connection_name": vref(var_connection) if use_variables else connection_name,
                "objects": [],
            },
        }

        for r in group_rows:
            table_entry: Dict[str, Any] = {
                "table": {
                    "source_table": r.source_table,
                    "destination_catalog": vref(var_dest_catalog) if use_variables else r.destination_catalog,
                    "destination_schema": vref(var_dest_schema) if use_variables else r.destination_schema,
                    "destination_table": r.destination_table,
                }
            }
            if r.table_config:
                table_entry["table"]["table_configuration"] = dict(r.table_config)
            pipeline_def["ingestion_definition"]["objects"].append(table_entry)

        resources["pipelines"][pipeline_key] = pipeline_def

        if emit_jobs:
            # Use the first non-empty schedule in group, warn via docs rather than stdout.
            schedule = None
            for r in group_rows:
                if r.schedule:
                    schedule = r.schedule
                    break
            if schedule:
                job_key = f"job_{_safe_name(connector_name)}_{_safe_name(group)}"
                job_def: Dict[str, Any] = {
                    "name": f"{connector_name} scheduler - group {group}",
                    "schedule": {
                        "quartz_cron_expression": _convert_cron_to_quartz(schedule),
                        "timezone_id": "UTC",
                        "pause_status": "PAUSED" if pause_jobs else "UNPAUSED",
                    },
                    "tasks": [
                        {
                            "task_key": f"run_{_safe_name(connector_name)}_{_safe_name(group)}",
                            "pipeline_task": {"pipeline_id": f"${{resources.pipelines.{pipeline_key}.id}}"},
                        }
                    ],
                }
                resources["jobs"][job_key] = job_def

    # Drop empty jobs if none created
    if not resources["jobs"]:
        resources.pop("jobs", None)

    root["resources"] = resources
    return root


def main() -> int:
    p = argparse.ArgumentParser(description="Generate DAB YAML for Lakeflow Connect ingestion pipelines.")
    p.add_argument("--input-csv", required=True, help="Path to input CSV metadata.")
    p.add_argument("--output-yaml", required=True, help="Path to output YAML file.")
    p.add_argument("--connector-name", required=True, help="Connector name (e.g., osipi, hubspot). Used for naming.")

    p.add_argument("--connection-name", default=None, help="Databricks connection name (override CSV).")
    p.add_argument("--dest-catalog", default="main", help="Default destination catalog.")
    p.add_argument("--dest-schema", default="bronze", help="Default destination schema.")

    p.add_argument("--weight-column", default="weight", help="CSV column to use as weight for auto balancing.")
    p.add_argument("--num-pipelines", type=int, default=0, help="If >0 and pipeline_group is missing, auto-assign.")
    p.add_argument("--max-items-per-pipeline", type=int, default=0, help="Optional cap for auto balancing.")

    p.add_argument("--emit-jobs", action="store_true", help="Also emit Databricks jobs (schedules) for each pipeline.")
    p.add_argument("--unpause-jobs", action="store_true", help="Emit jobs as UNPAUSED (default is PAUSED).")
    p.add_argument("--no-variables", action="store_true", help="Inline values instead of ${var.*} variables.")

    args = p.parse_args()

    csv_path = Path(args.input_csv).expanduser().resolve()
    out_path = Path(args.output_yaml).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    connection_name, rows = _read_rows(
        csv_path,
        dest_catalog_default=args.dest_catalog,
        dest_schema_default=args.dest_schema,
        connection_name_override=args.connection_name,
        weight_column=args.weight_column,
    )

    # If any row already has pipeline_group, we keep it (and require all to have it).
    has_any_group = any(r.pipeline_group for r in rows)
    has_missing_group = any(not r.pipeline_group for r in rows)

    if has_any_group and has_missing_group:
        raise ValueError("Some rows have pipeline_group and others do not. Either fill all or use auto-balance for all.")

    if (not has_any_group) and args.num_pipelines <= 0:
        raise ValueError("pipeline_group is missing. Provide pipeline_group in CSV or set --num-pipelines for auto-balance.")

    if not has_any_group:
        num_bins = int(args.num_pipelines)
        max_items = int(args.max_items_per_pipeline) if args.max_items_per_pipeline and args.max_items_per_pipeline > 0 else None
        assignment = _ffd_binpack([(i, r.weight) for i, r in enumerate(rows)], num_bins=num_bins, max_items_per_bin=max_items)
        for i, r in enumerate(rows):
            r.pipeline_group = _group_id_from_bin(assignment[i])

    # Determine canonical dest catalog/schema for variable defaults:
    # use provided args (explicitly), not per-row values.
    yaml_obj = _build_yaml(
        connector_name=args.connector_name,
        connection_name=connection_name,
        dest_catalog=args.dest_catalog,
        dest_schema=args.dest_schema,
        rows=rows,
        emit_jobs=bool(args.emit_jobs),
        pause_jobs=not bool(args.unpause_jobs),
        use_variables=not bool(args.no_variables),
    )

    try:
        import yaml  # type: ignore
    except Exception:
        raise SystemExit(
            "Missing dependency: pyyaml. Install with: pip install pyyaml\n"
            f"Then rerun. Output path would have been: {out_path}"
        )

    out_path.write_text(yaml.safe_dump(yaml_obj, sort_keys=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


