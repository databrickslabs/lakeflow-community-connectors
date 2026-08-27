#!/usr/bin/env python3
"""
Schema validation script for Employment Hero connector.
Compares actual API responses with documented schemas.

Usage:
    python -m tests.unit.sources.employmenthero.validate_employmenthero_schemas
"""

import sys
from pathlib import Path

from tests.schema_validator import SchemaValidator
from tests.unit.sources.test_utils import load_config
from databricks.labs.community_connector.sources.employmenthero.employmenthero import (
    EmploymentHeroLakeflowConnect,
)


def main():
    """Run schema validation for employmenthero connector."""
    # Load config
    config_dir = Path(__file__).parent / "configs"
    config_path = config_dir / "dev_config.json"
    table_config_path = config_dir / "dev_table_config.json"

    config = load_config(config_path)

    # Load table configs if file exists
    table_config = {}
    if table_config_path.exists():
        table_config = load_config(table_config_path)

    # Initialize connector
    print(f"Initializing {EmploymentHeroLakeflowConnect.__name__}...")
    connector = EmploymentHeroLakeflowConnect(config)

    # Create validator and run validation
    validator = SchemaValidator(connector)
    results = validator.validate_all_tables(table_config)

    # Print summary
    discrepancies = validator.print_summary(results)

    # Save detailed results
    output_file = Path(__file__).parent / "schema_validation_results.json"
    validator.save_results(results, output_file)

    # Exit with appropriate code
    sys.exit(0 if discrepancies == 0 else 1)


if __name__ == "__main__":
    main()
