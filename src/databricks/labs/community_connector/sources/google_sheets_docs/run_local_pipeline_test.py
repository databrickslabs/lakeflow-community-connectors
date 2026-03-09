#!/usr/bin/env python3
"""
Validate and inspect the local pipeline spec (no Spark required).
Run from repo root:
  python src/databricks/labs/community_connector/sources/google_sheets_docs/run_local_pipeline_test.py

Or with PYTHONPATH=src:
  python -m databricks.labs.community_connector.sources.google_sheets_docs.run_local_pipeline_test
"""
import json
import sys
from pathlib import Path

# Add repo src so we can import spec_parser
_repo_root = Path(__file__).resolve().parents[5]
_src = _repo_root / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from databricks.labs.community_connector.libs.spec_parser import SpecParser


def main():
    spec_path = Path(__file__).parent / "pipeline_spec_local_test.json"
    with open(spec_path) as f:
        spec = json.load(f)

    print("Validating pipeline spec...")
    try:
        parser = SpecParser(spec)
    except ValueError as e:
        print(f"ERROR: Invalid spec: {e}")
        sys.exit(1)

    print(f"Connection: {parser.connection_name()}")
    print(f"Tables: {parser.get_table_list()}")

    for table in parser.get_table_list():
        config = parser.get_table_configuration(table)
        pk = parser.get_primary_keys(table)
        scd = parser.get_scd_type(table)
        print(f"\n  {table}:")
        print(f"    table_configuration (passed to connector): {config}")
        print(f"    primary_keys (from spec): {pk}")
        print(f"    scd_type: {scd}")

    print("\nSpec is valid. For sheet_values the connector will receive:")
    print("  spreadsheet_id, sheet_name, range, use_first_row_as_header")
    print("  and will return schema with row_index + header columns (e.g. ID) when use_first_row_as_header is true.")
    print("\nTo run the full pipeline you need Spark and:")
    print("  from databricks.labs.community_connector.pipeline.ingestion_pipeline import ingest")
    print("  ingest(spark, pipeline_spec)")


if __name__ == "__main__":
    main()
