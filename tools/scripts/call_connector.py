#!/usr/bin/env python3
"""
Generic script to call LakeflowConnect methods.

Supports all connector interface methods:
  - list_tables
  - get_table_schema
  - read_table_metadata
  - read_table
  - read_table_deletes

Usage:
    python tools/scripts/call_connector.py <source> <method> [table] [options]

Examples:
    # List all tables
    python tools/scripts/call_connector.py aha list_tables

    # Get schema for a table
    python tools/scripts/call_connector.py aha get_table_schema ideas

    # Get metadata for a table
    python tools/scripts/call_connector.py aha read_table_metadata ideas

    # Read table data
    python tools/scripts/call_connector.py aha read_table ideas

    # Read with table options
    python tools/scripts/call_connector.py aha read_table ideas --opt max_ideas=50 --opt status=Open

    # Read with offset (for incremental sync)
    python tools/scripts/call_connector.py aha read_table ideas \\
        --offset '{"updated_since": "2024-01-01T00:00:00Z"}'

    # Read with config file (table options + offset)
    python tools/scripts/call_connector.py aha read_table ideas --config config.json

    # Read table deletes
    python tools/scripts/call_connector.py aha read_table_deletes ideas

    # JSON output for programmatic use
    python tools/scripts/call_connector.py aha read_table ideas --json
"""

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional


METHODS = [
    "list_tables",
    "get_table_schema",
    "read_table_metadata",
    "read_table",
    "read_table_deletes",
]


def discover_source(repo_path: Path, source_name: str) -> Optional[Path]:
    """Discover the source module in the given repository."""
    source_dir = repo_path / "sources" / source_name
    source_file = source_dir / f"{source_name}.py"
    
    if source_file.exists():
        return source_file
    return None


def load_connector(repo_path: Path, source_name: str):
    """Dynamically load the LakeflowConnect class from the source module."""
    if str(repo_path) not in sys.path:
        sys.path.insert(0, str(repo_path))
    
    module = importlib.import_module(f"sources.{source_name}.{source_name}")
    return module.LakeflowConnect


def load_configs(repo_path: Path, source_name: str) -> tuple:
    """Load dev_config.json and dev_table_config.json for the source."""
    config_dir = repo_path / "sources" / source_name / "configs"
    
    config_path = config_dir / "dev_config.json"
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    
    with open(config_path, "r") as f:
        config = json.load(f)
    
    table_config_path = config_dir / "dev_table_config.json"
    table_configs = {}
    if table_config_path.exists():
        with open(table_config_path, "r") as f:
            table_configs = json.load(f)
    
    return config, table_configs


def parse_key_value_pair(pair: str) -> tuple:
    """Parse a key=value string into a tuple."""
    if "=" not in pair:
        raise ValueError(f"Invalid key-value pair: {pair}. Expected format: key=value")
    key, value = pair.split("=", 1)
    return key.strip(), value.strip()


def format_schema(schema) -> str:
    """Format a PySpark schema for display."""
    try:
        # Try to get the JSON representation
        if hasattr(schema, "json"):
            return json.dumps(json.loads(schema.json()), indent=2)
        elif hasattr(schema, "simpleString"):
            return schema.simpleString()
        else:
            return str(schema)
    except Exception:
        return str(schema)


def call_list_tables(connector, args) -> Dict[str, Any]:
    """Call list_tables method."""
    tables = connector.list_tables()
    return {
        "method": "list_tables",
        "tables": tables,
        "count": len(tables),
    }


def call_get_table_schema(connector, args, table_options) -> Dict[str, Any]:
    """Call get_table_schema method."""
    if not args.table:
        raise ValueError("Table name required for get_table_schema")
    
    # Try with table_options first, fall back to without if not supported
    try:
        schema = connector.get_table_schema(args.table, table_options)
    except TypeError:
        schema = connector.get_table_schema(args.table)
    
    return {
        "method": "get_table_schema",
        "table": args.table,
        "table_options": table_options,
        "schema": format_schema(schema),
        "schema_raw": str(schema),
    }


def call_read_table_metadata(connector, args, table_options) -> Dict[str, Any]:
    """Call read_table_metadata method."""
    if not args.table:
        raise ValueError("Table name required for read_table_metadata")
    
    metadata = connector.read_table_metadata(args.table, table_options)
    return {
        "method": "read_table_metadata",
        "table": args.table,
        "table_options": table_options,
        "metadata": metadata,
    }


def call_read_table(connector, args, table_options, offset) -> Dict[str, Any]:
    """Call read_table method."""
    if not args.table:
        raise ValueError("Table name required for read_table")
    
    records_iter, returned_offset = connector.read_table(args.table, offset, table_options)
    records = list(records_iter)
    
    result = {
        "method": "read_table",
        "table": args.table,
        "table_options": table_options,
        "input_offset": offset,
        "returned_offset": returned_offset,
        "record_count": len(records),
    }
    
    if args.limit:
        result["records"] = records[:args.limit]
        result["records_limited"] = args.limit < len(records)
    else:
        result["records"] = records
    
    return result


def call_read_table_deletes(connector, args, table_options, offset) -> Dict[str, Any]:
    """Call read_table_deletes method."""
    if not args.table:
        raise ValueError("Table name required for read_table_deletes")
    
    if not hasattr(connector, "read_table_deletes"):
        return {
            "method": "read_table_deletes",
            "table": args.table,
            "error": "Connector does not implement read_table_deletes",
        }
    
    records_iter, returned_offset = connector.read_table_deletes(args.table, offset, table_options)
    records = list(records_iter)
    
    result = {
        "method": "read_table_deletes",
        "table": args.table,
        "table_options": table_options,
        "input_offset": offset,
        "returned_offset": returned_offset,
        "record_count": len(records),
    }
    
    if args.limit:
        result["records"] = records[:args.limit]
        result["records_limited"] = args.limit < len(records)
    else:
        result["records"] = records
    
    return result


def print_result(result: Dict[str, Any], json_output: bool = False) -> None:
    """Print the result in human-readable or JSON format."""
    if json_output:
        print(json.dumps(result, indent=2, default=str))
        return
    
    method = result.get("method", "unknown")
    print(f"\n{'='*60}")
    print(f"Method: {method}")
    print(f"{'='*60}")
    
    if "error" in result:
        print(f"\n❌ Error: {result['error']}")
        return
    
    if method == "list_tables":
        print(f"\nTables ({result['count']}):")
        for table in result["tables"]:
            print(f"  - {table}")
    
    elif method == "get_table_schema":
        print(f"\nTable: {result['table']}")
        print(f"\nSchema:\n{result['schema']}")
    
    elif method == "read_table_metadata":
        print(f"\nTable: {result['table']}")
        if result.get("table_options"):
            print(f"Table Options: {json.dumps(result['table_options'])}")
        print(f"\nMetadata:")
        for key, value in result["metadata"].items():
            print(f"  {key}: {value}")
    
    elif method in ("read_table", "read_table_deletes"):
        print(f"\nTable: {result['table']}")
        if result.get("table_options"):
            print(f"Table Options: {json.dumps(result['table_options'])}")
        print(f"Input Offset: {json.dumps(result['input_offset'])}")
        print(f"Returned Offset: {json.dumps(result['returned_offset'], default=str)}")
        print(f"Record Count: {result['record_count']}")
        
        if result.get("records_limited"):
            print(f"\n(Showing first {len(result['records'])} of {result['record_count']} records)")
        
        print(f"\n{'-'*60}")
        print("Records:")
        print(f"{'-'*60}")
        
        for i, record in enumerate(result.get("records", [])):
            print(f"\n[{i+1}] {json.dumps(record, indent=2, default=str)}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Call LakeflowConnect methods for any source connector.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Methods:
  list_tables          List all available tables
  get_table_schema     Get the PySpark schema for a table
  read_table_metadata  Get metadata (primary keys, cursor field, ingestion type)
  read_table           Read records from a table
  read_table_deletes   Read deleted records (if supported)

Examples:
  python tools/scripts/call_connector.py aha read_table ideas --config config.json

Config file format (--config):
  {"table_name": {"option1": "value1", "offset": {"cursor_key": "value"}}}
""",
    )

    parser.add_argument(
        "source",
        help="Name of the source connector (e.g., aha, github, zendesk)",
    )

    parser.add_argument(
        "method",
        choices=METHODS,
        help="Method to call on the connector",
    )

    parser.add_argument(
        "table",
        nargs="?",
        help="Table name (required for all methods except list_tables)",
    )

    parser.add_argument(
        "--offset",
        type=str,
        default="{}",
        help="Starting offset as JSON string (for read_table/read_table_deletes)",
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to JSON config file with table options and optional 'offset' key per table",
    )

    parser.add_argument(
        "--opt",
        action="append",
        dest="options",
        metavar="KEY=VALUE",
        help="Table option as key=value pair (can be specified multiple times)",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of records to display (for read_table/read_table_deletes)",
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )

    args = parser.parse_args()

    # Validate table argument for methods that require it
    if args.method != "list_tables" and not args.table:
        parser.error(f"Table name is required for {args.method}")

    # Use current working directory as repo path
    repo_path = Path.cwd()

    # Verify source exists
    source_file = discover_source(repo_path, args.source)
    if not source_file:
        print(f"Error: Source '{args.source}' not found in {repo_path}/sources/", file=sys.stderr)
        sys.exit(1)

    # Load config file if provided
    config_file_options = {}
    if args.config:
        if not args.config.exists():
            print(f"Error: Config file not found: {args.config}", file=sys.stderr)
            sys.exit(1)
        try:
            with open(args.config, "r") as f:
                config_file_options = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in config file: {e}", file=sys.stderr)
            sys.exit(1)

    # Parse CLI offset
    try:
        cli_offset = json.loads(args.offset)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in offset: {e}", file=sys.stderr)
        sys.exit(1)

    # Parse table options from CLI
    cli_options = {}
    if args.options:
        for opt in args.options:
            try:
                key, value = parse_key_value_pair(opt)
                cli_options[key] = value
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

    # Load connector and configs
    if not args.json:
        print(f"Loading connector: {args.source}", file=sys.stderr)
        print(f"Repository: {repo_path}", file=sys.stderr)

    try:
        LakeflowConnect = load_connector(repo_path, args.source)
        config, table_configs = load_configs(repo_path, args.source)
        connector = LakeflowConnect(config)
    except Exception as e:
        print(f"Error loading connector: {e}", file=sys.stderr)
        sys.exit(1)

    # Build table options and offset
    # Priority: CLI > config file > dev_table_config.json
    table_options = {}
    offset = cli_offset
    
    if args.table:
        # Start with dev_table_config.json
        table_options = {**table_configs.get(args.table, {})}
        
        # Merge config file options (if provided)
        if args.table in config_file_options:
            file_opts = config_file_options[args.table].copy()
            # Extract offset from config file if present
            if "offset" in file_opts:
                if not cli_offset:  # Only use file offset if CLI offset not provided
                    offset = file_opts.pop("offset")
                else:
                    file_opts.pop("offset")  # Remove offset, use CLI version
            table_options.update(file_opts)
        
        # CLI options take highest precedence
        table_options.update(cli_options)

    # Call the appropriate method
    try:
        if args.method == "list_tables":
            result = call_list_tables(connector, args)
        elif args.method == "get_table_schema":
            result = call_get_table_schema(connector, args, table_options)
        elif args.method == "read_table_metadata":
            result = call_read_table_metadata(connector, args, table_options)
        elif args.method == "read_table":
            result = call_read_table(connector, args, table_options, offset)
        elif args.method == "read_table_deletes":
            result = call_read_table_deletes(connector, args, table_options, offset)
        else:
            print(f"Error: Unknown method {args.method}", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        result = {
            "method": args.method,
            "table": args.table,
            "error": str(e),
        }
        print_result(result, args.json)
        sys.exit(1)

    print_result(result, args.json)


if __name__ == "__main__":
    main()
