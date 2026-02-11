"""
Group-based test runner for SAP SuccessFactors connector.

Extends LakeflowConnectTester to support filtering tests by table groups.
"""

import json
import os
import sys
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

# Add project root to path
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
sys.path.insert(0, project_root)

from pyspark.sql.types import StructType, IntegerType

from libs.utils import parse_value
from sources.sap_successfactors.sap_successfactors import LakeflowConnect
from sources.sap_successfactors.test.table_groups import (
    TABLE_GROUPS,
    GROUP_DESCRIPTIONS,
    get_group_tables,
    get_all_groups,
)


@dataclass
class TableTestResult:
    """Result of testing a single table."""
    table_name: str
    test_name: str
    passed: bool
    error_message: str = ""
    traceback_str: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GroupTestReport:
    """Test report for a group."""
    group_name: str
    description: str
    total_tables: int
    passed_tables: int
    failed_tables: int
    error_tables: int
    table_results: List[TableTestResult]
    timestamp: str

    def to_dict(self) -> dict:
        return {
            "group": self.group_name,
            "description": self.description,
            "total_tables": self.total_tables,
            "passed": self.passed_tables,
            "failed": self.failed_tables,
            "errors": self.error_tables,
            "success_rate": f"{(self.passed_tables / self.total_tables * 100):.1f}%" if self.total_tables > 0 else "0%",
            "timestamp": self.timestamp,
            "results": [
                {
                    "table": r.table_name,
                    "test": r.test_name,
                    "passed": r.passed,
                    "error": r.error_message,
                    "traceback": r.traceback_str if not r.passed else "",
                    "details": r.details,
                }
                for r in self.table_results
            ],
            "failures": [
                {
                    "table": r.table_name,
                    "test": r.test_name,
                    "error": r.error_message,
                    "traceback": r.traceback_str,
                    "details": r.details,
                }
                for r in self.table_results if not r.passed
            ]
        }


class GroupTestRunner:
    """Test runner that supports filtering by table groups."""

    def __init__(self, init_options: dict):
        """Initialize with connection options."""
        self.init_options = init_options
        self.connector: Optional[LakeflowConnect] = None
        self.results: List[TableTestResult] = []

    def initialize_connector(self) -> bool:
        """Initialize the connector."""
        try:
            self.connector = LakeflowConnect(self.init_options)
            return True
        except Exception as e:
            print(f"ERROR: Failed to initialize connector: {e}")
            traceback.print_exc()
            return False

    def run_group_tests(self, group_name: str, verbose: bool = True) -> GroupTestReport:
        """
        Run tests for a specific group.

        Args:
            group_name: Name of the group to test
            verbose: Whether to print progress

        Returns:
            GroupTestReport with results
        """
        tables = get_group_tables(group_name)
        description = GROUP_DESCRIPTIONS.get(group_name, "")

        if verbose:
            print(f"\n{'='*60}")
            print(f"Testing Group: {group_name}")
            print(f"Description: {description}")
            print(f"Tables: {len(tables)}")
            print(f"{'='*60}\n")

        if not self.connector:
            if not self.initialize_connector():
                return GroupTestReport(
                    group_name=group_name,
                    description=description,
                    total_tables=len(tables),
                    passed_tables=0,
                    failed_tables=0,
                    error_tables=len(tables),
                    table_results=[
                        TableTestResult(
                            table_name=t,
                            test_name="initialization",
                            passed=False,
                            error_message="Connector initialization failed",
                        )
                        for t in tables
                    ],
                    timestamp=datetime.now().isoformat(),
                )

        self.results = []
        passed = 0
        failed = 0
        errors = 0

        for i, table_name in enumerate(tables):
            if verbose:
                print(f"[{i+1}/{len(tables)}] Testing {table_name}...")

            # Run tests for this table
            table_passed = True

            # Test 1: get_table_schema
            schema_result = self._test_get_table_schema(table_name)
            self.results.append(schema_result)
            if not schema_result.passed:
                table_passed = False
                if verbose:
                    print(f"  ❌ get_table_schema: {schema_result.error_message}")

            # Test 2: read_table_metadata
            metadata_result = self._test_read_table_metadata(table_name)
            self.results.append(metadata_result)
            if not metadata_result.passed:
                table_passed = False
                if verbose:
                    print(f"  ❌ read_table_metadata: {metadata_result.error_message}")

            # Test 3: read_table (only if previous tests passed)
            if schema_result.passed and metadata_result.passed:
                read_result = self._test_read_table(table_name)
                self.results.append(read_result)
                if not read_result.passed:
                    table_passed = False
                    if verbose:
                        print(f"  ❌ read_table: {read_result.error_message}")

            if table_passed:
                passed += 1
                if verbose:
                    print(f"  ✅ All tests passed")
            else:
                # Count as error if exception, failed otherwise
                if any(r.traceback_str and r.table_name == table_name for r in self.results):
                    errors += 1
                else:
                    failed += 1

        report = GroupTestReport(
            group_name=group_name,
            description=description,
            total_tables=len(tables),
            passed_tables=passed,
            failed_tables=failed,
            error_tables=errors,
            table_results=self.results,
            timestamp=datetime.now().isoformat(),
        )

        if verbose:
            self._print_report_summary(report)

        return report

    def _test_get_table_schema(self, table_name: str) -> TableTestResult:
        """Test get_table_schema for a single table."""
        try:
            schema = self.connector.get_table_schema(table_name, {})

            # Validate schema
            if not isinstance(schema, StructType):
                return TableTestResult(
                    table_name=table_name,
                    test_name="get_table_schema",
                    passed=False,
                    error_message=f"Schema should be StructType, got {type(schema).__name__}",
                )

            # Check for IntegerType fields (should use LongType)
            for field in schema:
                if isinstance(field.dataType, IntegerType):
                    return TableTestResult(
                        table_name=table_name,
                        test_name="get_table_schema",
                        passed=False,
                        error_message=f"Field {field.name} uses IntegerType, should use LongType",
                        details={"field": field.name},
                    )

            return TableTestResult(
                table_name=table_name,
                test_name="get_table_schema",
                passed=True,
                details={"field_count": len(schema.fields)},
            )

        except Exception as e:
            return TableTestResult(
                table_name=table_name,
                test_name="get_table_schema",
                passed=False,
                error_message=str(e),
                traceback_str=traceback.format_exc(),
            )

    def _test_read_table_metadata(self, table_name: str) -> TableTestResult:
        """Test read_table_metadata for a single table."""
        try:
            metadata = self.connector.read_table_metadata(table_name, {})

            # Validate metadata
            if not isinstance(metadata, dict):
                return TableTestResult(
                    table_name=table_name,
                    test_name="read_table_metadata",
                    passed=False,
                    error_message=f"Metadata should be dict, got {type(metadata).__name__}",
                )

            ingestion_type = metadata.get("ingestion_type")

            # Check required keys based on ingestion type
            if ingestion_type != "append":
                if "primary_keys" not in metadata:
                    return TableTestResult(
                        table_name=table_name,
                        test_name="read_table_metadata",
                        passed=False,
                        error_message="Missing required key: primary_keys",
                        details={"keys": list(metadata.keys())},
                    )

            if ingestion_type not in ("snapshot", "append"):
                if "cursor_field" not in metadata:
                    return TableTestResult(
                        table_name=table_name,
                        test_name="read_table_metadata",
                        passed=False,
                        error_message="Missing required key: cursor_field for CDC table",
                        details={"keys": list(metadata.keys())},
                    )

            # Validate primary keys exist in schema
            schema = self.connector.get_table_schema(table_name, {})
            schema_fields = schema.fieldNames()

            if "primary_keys" in metadata:
                for pk in metadata["primary_keys"]:
                    # Handle nested primary keys (e.g., "properties.id")
                    top_level = pk.split(".")[0]
                    if top_level not in schema_fields:
                        return TableTestResult(
                            table_name=table_name,
                            test_name="read_table_metadata",
                            passed=False,
                            error_message=f"Primary key '{pk}' not found in schema",
                            details={"primary_keys": metadata["primary_keys"], "schema_fields": schema_fields},
                        )

            # Validate cursor field exists in schema (for CDC)
            if metadata.get("cursor_field"):
                cursor_field = metadata["cursor_field"]
                top_level = cursor_field.split(".")[0]
                if top_level not in schema_fields:
                    return TableTestResult(
                        table_name=table_name,
                        test_name="read_table_metadata",
                        passed=False,
                        error_message=f"Cursor field '{cursor_field}' not found in schema",
                        details={"cursor_field": cursor_field, "schema_fields": schema_fields},
                    )

            return TableTestResult(
                table_name=table_name,
                test_name="read_table_metadata",
                passed=True,
                details={
                    "ingestion_type": ingestion_type,
                    "primary_keys": metadata.get("primary_keys", []),
                    "cursor_field": metadata.get("cursor_field"),
                },
            )

        except Exception as e:
            return TableTestResult(
                table_name=table_name,
                test_name="read_table_metadata",
                passed=False,
                error_message=str(e),
                traceback_str=traceback.format_exc(),
            )

    def _test_read_table(self, table_name: str) -> TableTestResult:
        """Test read_table for a single table."""
        try:
            result = self.connector.read_table(table_name, {}, {})

            # Validate return type
            if not isinstance(result, tuple) or len(result) != 2:
                return TableTestResult(
                    table_name=table_name,
                    test_name="read_table",
                    passed=False,
                    error_message=f"Expected tuple of length 2, got {type(result).__name__}",
                )

            iterator, offset = result

            # Validate iterator
            if not hasattr(iterator, "__iter__"):
                return TableTestResult(
                    table_name=table_name,
                    test_name="read_table",
                    passed=False,
                    error_message=f"First element should be iterator, got {type(iterator).__name__}",
                )

            # Validate offset is dict
            if not isinstance(offset, dict):
                return TableTestResult(
                    table_name=table_name,
                    test_name="read_table",
                    passed=False,
                    error_message=f"Offset should be dict, got {type(offset).__name__}",
                )

            # Try to read first 3 records
            records = []
            record_count = 0
            sample_count = 3

            try:
                for record in iterator:
                    if record_count >= sample_count:
                        break
                    if not isinstance(record, dict):
                        return TableTestResult(
                            table_name=table_name,
                            test_name="read_table",
                            passed=False,
                            error_message=f"Record {record_count} is not dict: {type(record).__name__}",
                        )
                    records.append(record)
                    record_count += 1
            except Exception as e:
                return TableTestResult(
                    table_name=table_name,
                    test_name="read_table",
                    passed=False,
                    error_message=f"Iterator failed: {str(e)}",
                    traceback_str=traceback.format_exc(),
                )

            # Validate records can be parsed with schema
            if records:
                try:
                    schema = self.connector.get_table_schema(table_name, {})
                    for record in records:
                        parse_value(record, schema)
                except Exception as e:
                    return TableTestResult(
                        table_name=table_name,
                        test_name="read_table",
                        passed=False,
                        error_message=f"Failed to parse record with schema: {str(e)}",
                        traceback_str=traceback.format_exc(),
                        details={"sample_record": records[0] if records else None},
                    )

            return TableTestResult(
                table_name=table_name,
                test_name="read_table",
                passed=True,
                details={
                    "records_sampled": record_count,
                    "offset_keys": list(offset.keys()),
                    "sample_record_keys": list(records[0].keys()) if records else [],
                },
            )

        except Exception as e:
            return TableTestResult(
                table_name=table_name,
                test_name="read_table",
                passed=False,
                error_message=str(e),
                traceback_str=traceback.format_exc(),
            )

    def _print_report_summary(self, report: GroupTestReport):
        """Print a summary of the test report."""
        print(f"\n{'='*60}")
        print(f"TEST REPORT: {report.group_name}")
        print(f"{'='*60}")
        print(f"Total Tables: {report.total_tables}")
        print(f"Passed: {report.passed_tables}")
        print(f"Failed: {report.failed_tables}")
        print(f"Errors: {report.error_tables}")
        print(f"Success Rate: {report.passed_tables / report.total_tables * 100:.1f}%" if report.total_tables > 0 else "0%")

        if report.failed_tables > 0 or report.error_tables > 0:
            print(f"\n{'='*40}")
            print("FAILURES:")
            print(f"{'='*40}")
            for result in report.table_results:
                if not result.passed:
                    print(f"\n{result.table_name} - {result.test_name}:")
                    print(f"  Error: {result.error_message}")
                    if result.details:
                        print(f"  Details: {result.details}")

    def save_report(self, report: GroupTestReport, output_dir: str):
        """Save report to JSON file."""
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{report.group_name.lower().replace('-', '_')}_test_results.json"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, "w") as f:
            json.dump(report.to_dict(), f, indent=2)

        print(f"\nReport saved to: {filepath}")
        return filepath


def run_group(group_name: str, config_path: str = None, output_dir: str = None, verbose: bool = True):
    """Run tests for a single group."""
    # Load config
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "configs",
            "dev_config.json",
        )

    with open(config_path, "r") as f:
        config = json.load(f)

    # Create runner and run tests
    runner = GroupTestRunner(config)
    report = runner.run_group_tests(group_name, verbose=verbose)

    # Save report if output dir specified
    if output_dir:
        runner.save_report(report, output_dir)

    return report


def run_all_groups(config_path: str = None, output_dir: str = None, verbose: bool = True):
    """Run tests for all groups."""
    groups = get_all_groups()
    all_reports = []

    for group_name in groups:
        report = run_group(group_name, config_path, output_dir, verbose)
        all_reports.append(report)

    # Print overall summary
    total_tables = sum(r.total_tables for r in all_reports)
    total_passed = sum(r.passed_tables for r in all_reports)
    total_failed = sum(r.failed_tables for r in all_reports)
    total_errors = sum(r.error_tables for r in all_reports)

    print(f"\n{'='*60}")
    print("OVERALL SUMMARY")
    print(f"{'='*60}")
    print(f"Groups Tested: {len(all_reports)}")
    print(f"Total Tables: {total_tables}")
    print(f"Passed: {total_passed}")
    print(f"Failed: {total_failed}")
    print(f"Errors: {total_errors}")
    print(f"Overall Success Rate: {total_passed / total_tables * 100:.1f}%" if total_tables > 0 else "0%")

    return all_reports


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SAP SuccessFactors connector tests by group")
    parser.add_argument("--group", "-g", help="Group name to test (e.g., EC-Core)")
    parser.add_argument("--all", "-a", action="store_true", help="Run all groups")
    parser.add_argument("--list", "-l", action="store_true", help="List available groups")
    parser.add_argument("--config", "-c", help="Path to config file")
    parser.add_argument("--output", "-o", help="Output directory for reports")
    parser.add_argument("--quiet", "-q", action="store_true", help="Quiet mode (less output)")

    args = parser.parse_args()

    if args.list:
        print("\nAvailable groups:")
        for group in get_all_groups():
            tables = get_group_tables(group)
            desc = GROUP_DESCRIPTIONS.get(group, "")
            print(f"  {group}: {len(tables)} tables - {desc}")
        sys.exit(0)

    if args.group:
        run_group(args.group, args.config, args.output, not args.quiet)
    elif args.all:
        run_all_groups(args.config, args.output, not args.quiet)
    else:
        print("Please specify --group <name> or --all")
        print("Use --list to see available groups")
        sys.exit(1)
