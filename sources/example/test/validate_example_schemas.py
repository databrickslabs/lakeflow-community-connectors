#!/usr/bin/env python3
"""
Schema validation script for example connector.
Compares actual API responses with documented schemas.

This is a template - copy to your connector and customize:
    cp sources/example/test/validate_example_schemas.py sources/{source_name}/test/validate_{source_name}_schemas.py

Usage:
    python sources/example/test/validate_example_schemas.py
"""

import sys
from pathlib import Path

from tests.schema_validator import SchemaValidator
from tests.test_utils import load_config
from sources.example.example import LakeflowConnect


def main():
    # Load config
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)

    # Load table configs if file exists
    table_config = {}
    if table_config_path.exists():
        table_config = load_config(table_config_path)

    # Initialize connector
    print(f"Initializing {LakeflowConnect.__name__}...")
    connector = LakeflowConnect(config)

    # Create validator and run validation
    validator = SchemaValidator(connector)
    results = validator.validate_all_tables(table_config)

    # Print summary
    discrepancies = validator.print_summary(results)

    # Save detailed results
    output_file = parent_dir / "schema_validation_results.json"
    validator.save_results(results, output_file)

    # Exit with appropriate code
    sys.exit(0 if discrepancies == 0 else 1)


if __name__ == "__main__":
    main()
