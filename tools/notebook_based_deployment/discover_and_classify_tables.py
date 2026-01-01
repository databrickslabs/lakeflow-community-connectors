#!/usr/bin/env python3
"""Discover connector tables and generate a multi-pipeline CSV based on characteristics.

Goal
----
Turn a connector implementation into an ingestion CSV that can be fed into:
  tools/notebook_based_deployment/generate_dab_yaml_notebooks.py

This script is connector-agnostic. It uses the connector's own:
- list_tables()
- read_table_metadata(table_name, table_options)

…and derives:
- pipeline_group (based on ingestion_type by default)
- schedule (optional, based on ingestion_type)
- weight (optional, based on ingestion_type)

No fake fallbacks
-----------------
By default this script will FAIL if metadata can't be retrieved for a table.
Use --best-effort to continue (tables with missing metadata will be labeled
as ingestion_type=unknown).

Typical usage
-------------
1) Discover/classify -> CSV
2) CSV -> DAB YAML (optionally auto-balance further)

Example:
  python3 tools/notebook_based_deployment/discover_and_classify_tables.py \
    --connector-name osipi \
    --output-csv /tmp/osipi.csv \
    --dest-catalog main \
    --dest-schema bronze \
    --connection-name osipi_connection \
    --group-by ingestion_type \
    --schedule-snapshot "0 0 * * *" \
    --schedule-append "*/15 * * * *"

Then:
  python3 tools/notebook_based_deployment/generate_dab_yaml_notebooks.py \
    --input-csv /tmp/osipi.csv \
    --output-yaml /tmp/osipi_pipelines.yml \
    --connector-name osipi \
    --connection-name osipi_connection \
    --dest-catalog main \
    --dest-schema bronze
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _find_repo_root(start: Path) -> Optional[Path]:
    for p in [start] + list(start.parents):
        if (p / "sources").is_dir():
            return p
    return None


def _load_connector_class(*, connector_name: str, connector_python_file: Optional[str]) -> type:
    if connector_python_file:
        src_path = Path(connector_python_file).expanduser().resolve()
    else:
        repo_root = _find_repo_root(Path(__file__).resolve())
        if repo_root is None:
            raise ValueError("Unable to locate repo root (missing `sources/`).")
        src_path = repo_root / "sources" / connector_name / f"{connector_name}.py"

    if not src_path.exists():
        raise ValueError(f"Connector source file not found: {src_path}")

    module_name = f"_lakeflow_cc_{connector_name}_connector_discovery"
    spec = importlib.util.spec_from_file_location(module_name, str(src_path))
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load module spec from: {src_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]

    cls = getattr(mod, "LakeflowConnect", None)
    if cls is None:
        raise ValueError(f"No LakeflowConnect class found in: {src_path}")
    return cls


def _coerce_table_config(value: Any) -> str:
    # ingestion pipeline spec normalizes table_configuration to str->str
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def _default_schedule_for_type(ingestion_type: str, *, snapshot: str, append: str, cdc: str, unknown: str) -> str:
    it = (ingestion_type or "").strip().lower()
    if it == "snapshot":
        return snapshot
    if it == "append":
        return append
    if it == "cdc":
        return cdc
    return unknown


def _default_weight_for_type(ingestion_type: str, *, snapshot: int, append: int, cdc: int, unknown: int) -> int:
    it = (ingestion_type or "").strip().lower()
    if it == "snapshot":
        return snapshot
    if it == "append":
        return append
    if it == "cdc":
        return cdc
    return unknown


def _table_category_from_tables_lists(inst: Any, table_name: str) -> Optional[str]:
    """Best-effort table category based on connector TABLES_* lists.

    Many connectors (including OSIPI) define class attributes like TABLES_TIME_SERIES.
    If present, we use them to produce stable categories without guessing from names.
    """

    t = (table_name or "").strip()
    if not t:
        return None

    # Prefer class attributes to avoid triggering expensive network calls.
    cls = inst.__class__
    for attr in dir(cls):
        if not attr.startswith("TABLES_"):
            continue
        v = getattr(cls, attr, None)
        if isinstance(v, list) and t in v:
            return attr[len("TABLES_"):].lower()
    return None


def _table_category_fallback_by_name(table_name: str) -> str:
    """Fallback categorization when TABLES_* lists don't exist.

    This is intentionally generic: group by the first token before '_' (or 'unknown').
    Connector-specific categorization should be provided via connector TABLES_* lists
    or via an external rules/map file in tools examples.
    """

    t = (table_name or "").strip().lower()
    if not t:
        return "unknown"
    if "_" in t:
        return t.split("_", 1)[0]
    return "unknown"




def _table_category(inst: Any, table_name: str, *, prefix_map: Dict[str, str] | None = None) -> str:
    c = _table_category_from_tables_lists(inst, table_name)
    if c:
        return c
    pm = prefix_map or {}
    t = (table_name or "").strip().lower()
    for k, v in pm.items():
        kk = (k or "").strip().lower()
        if kk and t.startswith(kk):
            return v
    return _table_category_fallback_by_name(table_name)



def main() -> int:
    p = argparse.ArgumentParser(description="Discover connector tables and generate a classified ingestion CSV.")
    p.add_argument("--connector-name", required=True, help="Connector name (e.g., osipi, hubspot).")
    p.add_argument("--connector-python-file", default=None, help="Override path to sources/<connector>/<connector>.py")
    p.add_argument("--init-options-json", default=None, help="JSON object passed to LakeflowConnect(...) for discovery.")

    p.add_argument("--output-csv", required=True, help="Where to write the ingestion CSV.")
    p.add_argument("--connection-name", required=True, help="Unity Catalog connection name for the generated CSV.")
    p.add_argument("--dest-catalog", default="main", help="Default destination catalog.")
    p.add_argument("--dest-schema", default="bronze", help="Default destination schema.")

    p.add_argument(
        "--group-by",
        default="ingestion_type",
        choices=["ingestion_type", "category", "category_and_ingestion_type", "none"],
        help="How to derive pipeline_group (default: ingestion_type).",
    )

    p.add_argument(
        "--category-prefix-map-json",
        default=None,
        help=(
            "Optional JSON object mapping table-name prefixes to category names. "
            "Example: {'pi_streamset':'time_series','pi_af':'asset_framework','pi_event':'event_frames'}. "
            "Only used when --group-by uses category and TABLES_* lists are unavailable."
        ),
    )

    p.add_argument("--schedule-snapshot", default="", help="Schedule (cron) for snapshot tables. Empty = no jobs.")
    p.add_argument("--schedule-append", default="", help="Schedule (cron) for append tables. Empty = no jobs.")
    p.add_argument("--schedule-cdc", default="", help="Schedule (cron) for cdc tables. Empty = no jobs.")
    p.add_argument("--schedule-unknown", default="", help="Schedule (cron) for unknown tables. Empty = no jobs.")

    p.add_argument("--weight-snapshot", type=int, default=1, help="Default weight for snapshot tables.")
    p.add_argument("--weight-append", type=int, default=10, help="Default weight for append tables.")
    p.add_argument("--weight-cdc", type=int, default=10, help="Default weight for cdc tables.")
    p.add_argument("--weight-unknown", type=int, default=1, help="Default weight for unknown tables.")

    p.add_argument(
        "--best-effort",
        action="store_true",
        help="Continue if read_table_metadata fails (labels ingestion_type=unknown).",
    )

    args = p.parse_args()

    init_options: Dict[str, Any] = {}
    if args.init_options_json:
        init_options = json.loads(args.init_options_json)
        if not isinstance(init_options, dict):
            raise ValueError("--init-options-json must be a JSON object")
    category_prefix_map: Dict[str, str] = {}
    if args.category_prefix_map_json:
        category_prefix_map = json.loads(args.category_prefix_map_json)
        if (
            not isinstance(category_prefix_map, dict)
            or not all(isinstance(k, str) and isinstance(v, str) for k, v in category_prefix_map.items())
        ):
            raise ValueError("--category-prefix-map-json must be a JSON object of string->string")


    cls = _load_connector_class(connector_name=args.connector_name, connector_python_file=args.connector_python_file)
    inst = cls(init_options)

    tables = inst.list_tables()
    if not isinstance(tables, list) or not all(isinstance(t, str) for t in tables):
        raise ValueError("list_tables() must return list[str]")

    # Deterministic ordering
    tables = [t.strip() for t in tables if t and t.strip()]
    tables = sorted(set(tables))

    out_path = Path(args.output_csv).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "connection_name",
        "source_table",
        "destination_catalog",
        "destination_schema",
        "destination_table",
        "pipeline_group",
        "schedule",
        "table_options_json",
        "weight",
        "ingestion_type",
        "primary_keys",
        "cursor_field",
        "category",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for table in tables:
            try:
                meta = inst.read_table_metadata(table, {})
                if not isinstance(meta, dict):
                    raise ValueError("read_table_metadata must return dict")
            except Exception as e:
                if not args.best_effort:
                    raise
                meta = {"ingestion_type": "unknown"}

            ingestion_type = str(meta.get("ingestion_type") or "unknown")
            cursor_field = meta.get("cursor_field")
            primary_keys = meta.get("primary_keys")

            category = _table_category(inst, table, prefix_map=category_prefix_map)

            if args.group_by == "ingestion_type":
                pipeline_group = ingestion_type
            elif args.group_by == "category":
                pipeline_group = category
            elif args.group_by == "category_and_ingestion_type":
                pipeline_group = f"{category}_{ingestion_type}"
            else:
                pipeline_group = "all"

            schedule = _default_schedule_for_type(
                ingestion_type,
                snapshot=args.schedule_snapshot,
                append=args.schedule_append,
                cdc=args.schedule_cdc,
                unknown=args.schedule_unknown,
            )

            weight = _default_weight_for_type(
                ingestion_type,
                snapshot=int(args.weight_snapshot),
                append=int(args.weight_append),
                cdc=int(args.weight_cdc),
                unknown=int(args.weight_unknown),
            )

            row = {
                "connection_name": args.connection_name,
                "source_table": table,
                "destination_catalog": args.dest_catalog,
                "destination_schema": args.dest_schema,
                "destination_table": table,
                "pipeline_group": pipeline_group,
                "schedule": schedule,
                "table_options_json": "{}",
                "weight": str(max(1, int(weight))),
                "ingestion_type": ingestion_type,
                "primary_keys": _coerce_table_config(primary_keys),
                "cursor_field": _coerce_table_config(cursor_field),
                "category": category,
            }
            w.writerow(row)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
