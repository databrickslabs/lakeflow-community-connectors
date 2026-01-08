#!/usr/bin/env python3
"""
Schema validation script for Qualtrics connector.
Compares actual API responses with documented schemas.

Usage:
    python sources/qualtrics/test/validate_qualtrics_schemas.py
"""

import sys
from pathlib import Path

from tests.schema_validator import SchemaValidator
from tests.test_utils import load_config
from sources.qualtrics.qualtrics import LakeflowConnect


def main():
    """Run schema validation for Qualtrics connector."""
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
