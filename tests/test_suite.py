# pylint: disable=too-many-lines
"""Test suite for LakeflowConnect implementations."""

import json
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Tuple, Iterator, Optional, Callable

from pyspark.sql.types import *  # pylint: disable=wildcard-import,unused-wildcard-import

from libs.utils import parse_value


class TestStatus(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    ERROR = "ERROR"


@dataclass
class TestResult:
    """Represents the result of a single test"""

    test_name: str
    status: TestStatus
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    exception: Optional[Exception] = None
    traceback_str: Optional[str] = None


@dataclass
class TestReport:
    """Complete test report"""

    connector_class_name: str
    test_results: List[TestResult]
    total_tests: int
    passed_tests: int
    failed_tests: int
    error_tests: int
    timestamp: str

    def success_rate(self) -> float:
        if self.total_tests == 0:
            return 0.0
        return (self.passed_tests / self.total_tests) * 100


class TestFailedException(Exception):
    """Exception raised when tests fail or have errors"""

    def __init__(self, message: str, report: TestReport):
        super().__init__(message)
        self.report = report


class LakeflowConnectTester:
    def __init__(self, init_options: dict, table_configs: Dict[str, Dict[str, Any]] = {}):
        self._init_options = init_options
        # Per-table configuration passed as table_options into connector methods.
        # Keys are table names, values are dicts of options for that table.
        self._table_configs: Dict[str, Dict[str, Any]] = table_configs
        self.test_results: List[TestResult] = []

    def run_all_tests(self) -> TestReport:
        """Run all available tests and return a comprehensive report"""
        # Reset results
        self.test_results = []

        # Test each function separately
        self.test_initialization()

        # Only run other tests if initialization succeeds
        if self.connector is not None:
            self.test_list_tables()
            self.test_get_table_schema()
            self.test_read_table_metadata()
            self.test_read_table()
            self.test_read_table_deletes()

            # Test write functionality if connector_test_utils is available
            if hasattr(self, "connector_test_utils") and self.connector_test_utils is not None:
                self.test_list_insertable_tables()
                self.test_write_to_source()
                self.test_incremental_after_write()

        return self._generate_report()

    def test_initialization(self):
        """Test connector initialization"""
        try:
            self.connector = LakeflowConnect(self._init_options)  # pylint: disable=undefined-variable

            # Try to initialize test utils - may fail if not implemented for this connector
            try:
                self.connector_test_utils = LakeflowConnectTestUtils(self._init_options)  # pylint: disable=undefined-variable
            except Exception:
                self.connector_test_utils = None

            if self.connector is None:
                self._add_result(
                    TestResult(
                        test_name="test_initialization",
                        status=TestStatus.FAILED,
                        message="Connector initialization returned None",
                    )
                )
            else:
                self._add_result(
                    TestResult(
                        test_name="test_initialization",
                        status=TestStatus.PASSED,
                        message="Connector initialized successfully",
                    )
                )

        except Exception as e:
            self._add_result(
                TestResult(
                    test_name="test_initialization",
                    status=TestStatus.ERROR,
                    message=f"Initialization failed: {str(e)}",
                    exception=e,
                    traceback_str=traceback.format_exc(),
                )
            )

    def test_list_tables(self):
        """Test list_tables method"""
        try:
            tables = self.connector.list_tables()

            # Validate return type
            if not isinstance(tables, list):
                self._add_result(
                    TestResult(
                        test_name="test_list_tables",
                        status=TestStatus.FAILED,
                        message=f"Expected list, got {type(tables).__name__}",
                        details={
                            "returned_type": str(type(tables)),
                            "returned_value": str(tables),
                        },
                    )
                )
                return

            # Validate list contents
            for i, table in enumerate(tables):
                if not isinstance(table, str):
                    self._add_result(
                        TestResult(
                            test_name="test_list_tables",
                            status=TestStatus.FAILED,
                            message=f"Table name at index {i} is not a string: "
                            f"{type(table).__name__}",
                            details={
                                "table_index": i,
                                "table_type": str(type(table)),
                                "table_value": str(table),
                            },
                        )
                    )
                    return

            self._add_result(
                TestResult(
                    test_name="test_list_tables",
                    status=TestStatus.PASSED,
                    message=f"Successfully retrieved {len(tables)} tables",
                    details={"table_count": len(tables), "tables": tables},
                )
            )

        except Exception as e:
            self._add_result(
                TestResult(
                    test_name="test_list_tables",
                    status=TestStatus.ERROR,
                    message=f"list_tables failed: {str(e)}",
                    exception=e,
                    traceback_str=traceback.format_exc(),
                )
            )

    def test_get_table_schema(self):
        """Test get_table_schema method on all available tables"""
        # First get all tables to test with
        try:
            tables = self.connector.list_tables()
            if not tables:
                self._add_result(
                    TestResult(
                        test_name="test_get_table_schema",
                        status=TestStatus.FAILED,
                        message="No tables available to test "
                        "because list_tables returned an empty list",
                    )
                )
                return

        except Exception:
            self._add_result(
                TestResult(
                    test_name="test_get_table_schema",
                    status=TestStatus.FAILED,
                    message="Could not get tables for testing get_table_schema",
                )
            )
            return

        # Test each table
        passed_tables = []
        failed_tables = []
        error_tables = []

        for table_name in tables:
            try:
                schema = self.connector.get_table_schema(
                    table_name, self._get_table_options(table_name)
                )

                # Validate schema
                if not isinstance(schema, StructType):
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Schema should be StructType, got {type(schema).__name__}",
                        }
                    )
                    continue

                # Check for IntegerType fields (should use LongType instead)
                for field in schema:
                    if isinstance(field.dataType, IntegerType):
                        failed_tables.append(
                            {
                                "table": table_name,
                                "reason": f"Schema field {field.name} is IntegerType, "
                                "please always use LongType instead.",
                            }
                        )
                        break
                else:
                    passed_tables.append(
                        {
                            "table": table_name,
                            "schema_fields": len(schema.fields),
                        }
                    )

            except Exception as e:
                error_tables.append(
                    {
                        "table": table_name,
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )

        # Generate overall result
        total_tables = len(tables)
        passed_count = len(passed_tables)
        failed_count = len(failed_tables)
        error_count = len(error_tables)

        if error_count > 0:
            # If any tables had errors, mark as ERROR
            self._add_result(
                TestResult(
                    test_name="test_get_table_schema",
                    status=TestStatus.ERROR,
                    message=f"Tested {total_tables} tables: {passed_count} passed, "
                    f"{failed_count} failed, {error_count} errors",
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )
        elif failed_count > 0:
            # If any tables failed validation, mark as FAILED
            self._add_result(
                TestResult(
                    test_name="test_get_table_schema",
                    status=TestStatus.FAILED,
                    message=f"Tested {total_tables} tables: {passed_count} passed, "
                    f"{failed_count} failed",
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )
        else:
            # All tables passed
            self._add_result(
                TestResult(
                    test_name="test_get_table_schema",
                    status=TestStatus.PASSED,
                    message=f"Successfully retrieved table schema for all {total_tables} tables",
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )

    def test_read_table_metadata(self):  # pylint: disable=too-many-branches
        """Test read_table_metadata method on all available tables"""
        # First get all tables to test with
        try:
            tables = self.connector.list_tables()
            if not tables:
                self._add_result(
                    TestResult(
                        test_name="test_read_table_metadata",
                        status=TestStatus.FAILED,
                        message="No tables available to test "
                        "because list_tables returned an empty list",
                    )
                )
                return

        except Exception:
            self._add_result(
                TestResult(
                    test_name="test_read_table_metadata",
                    status=TestStatus.FAILED,
                    message="Could not get tables for testing read_table_metadata",
                )
            )
            return

        # Test each table
        passed_tables = []
        failed_tables = []
        error_tables = []

        for table_name in tables:
            try:
                metadata = self.connector.read_table_metadata(
                    table_name, self._get_table_options(table_name)
                )

                # Validate metadata
                if not isinstance(metadata, dict):
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Metadata should be dict, got {type(metadata).__name__}",
                        }
                    )
                    continue

                expected_keys = []
                # Only require primary_keys if not an append table
                if self._should_validate_primary_key(metadata):
                    expected_keys.append("primary_keys")
                # Only require cursor_field if not a snapshot table
                if self._should_validate_cursor_field(metadata):
                    expected_keys.append("cursor_field")

                missing_keys = [key for key in expected_keys if key not in metadata]

                if missing_keys:
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Missing expected metadata keys: {missing_keys}",
                            "metadata_keys": list(metadata.keys()),
                        }
                    )
                    continue

                # Validate primary_keys and cursor_field exist in schema if required
                schema = self.connector.get_table_schema(
                    table_name, self._get_table_options(table_name)
                )

                if self._should_validate_primary_key(metadata) and not self._validate_primary_keys(
                    metadata["primary_keys"], schema
                ):
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Primary key columns "
                            f"{metadata['primary_keys']} not found in schema",
                            "primary_keys": metadata["primary_keys"],
                        }
                    )
                elif self._should_validate_cursor_field(
                    metadata
                ) and not self._field_exists_in_schema(metadata["cursor_field"], schema):
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Cursor field {metadata['cursor_field']} "
                            "not found in schema",
                            "cursor_field": metadata["cursor_field"],
                        }
                    )
                elif (
                    metadata.get("ingestion_type") == "cdc_with_deletes"
                    and not hasattr(self.connector, "read_table_deletes")
                ):
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": "ingestion_type is 'cdc_with_deletes' but "
                            "connector does not implement read_table_deletes()",
                            "ingestion_type": metadata.get("ingestion_type"),
                        }
                    )
                else:
                    passed_tables.append(
                        {
                            "table": table_name,
                            "metadata_keys": list(metadata.keys()),
                        }
                    )

            except Exception as e:
                error_tables.append(
                    {
                        "table": table_name,
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )

        # Generate overall result
        total_tables = len(tables)
        passed_count = len(passed_tables)
        failed_count = len(failed_tables)
        error_count = len(error_tables)

        if error_count > 0:
            # If any tables had errors, mark as ERROR
            self._add_result(
                TestResult(
                    test_name="test_read_table_metadata",
                    status=TestStatus.ERROR,
                    message=f"Tested {total_tables} tables: {passed_count} passed, "
                    f"{failed_count} failed, {error_count} errors",
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )
        elif failed_count > 0:
            # If any tables failed validation, mark as FAILED
            self._add_result(
                TestResult(
                    test_name="test_read_table_metadata",
                    status=TestStatus.FAILED,
                    message=f"Tested {total_tables} tables: {passed_count} passed, "
                    f"{failed_count} failed",
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )
        else:
            # All tables passed
            self._add_result(
                TestResult(
                    test_name="test_read_table_metadata",
                    status=TestStatus.PASSED,
                    message=f"Successfully retrieved table metadata for all {total_tables} tables",
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )

    def _test_read_method(  # pylint: disable=too-many-locals,too-many-branches
        self,
        test_name: str,
        read_fn: Callable,
        tables: List[str],
        success_message: str,
    ):
        """
        Shared helper to test read_table or read_table_deletes methods.

        Args:
            test_name: Name of the test for reporting
            read_fn: The read function to call (e.g., connector.read_table)
            tables: List of table names to test
            success_message: Message to show on success
        """
        passed_tables = []
        failed_tables = []
        error_tables = []

        for table_name in tables:
            try:
                result = read_fn(table_name, {}, self._get_table_options(table_name))

                # Validate return type is tuple
                if not isinstance(result, tuple) or len(result) != 2:
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Expected tuple of length 2, got {type(result).__name__}",
                        }
                    )
                    continue

                iterator, offset = result

                # Validate iterator
                if not hasattr(iterator, "__iter__"):
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"First element should be an iterator, "
                            f"got {type(iterator).__name__}",
                        }
                    )
                    continue

                # Validate offset is dict
                if not isinstance(offset, dict):
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Offset should be dict, got {type(offset).__name__}",
                        }
                    )
                    continue

                # Try to read a few records to validate iterator works
                record_count = 0
                sample_count = 3
                sample_records = []
                try:
                    for i, record in enumerate(iterator):
                        if i >= sample_count:
                            break
                        if not isinstance(record, dict):
                            failed_tables.append(
                                {
                                    "table": table_name,
                                    "reason": f"Record {i} is not a dict: {type(record).__name__}",
                                }
                            )
                            break
                        record_count += 1
                        sample_records.append(record)

                    # Add to passed_tables if we didn't fail validation
                    if not any(f["table"] == table_name for f in failed_tables):
                        passed_tables.append(
                            {
                                "table": table_name,
                                "records_sampled": record_count,
                                "offset_keys": list(offset.keys()),
                                "sample_records": sample_records[:2],
                            }
                        )
                except Exception as iter_e:
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Iterator failed during iteration: {str(iter_e)}",
                        }
                    )
                    continue

                # Validate records can be parsed with schema
                try:
                    schema = self.connector.get_table_schema(
                        table_name, self._get_table_options(table_name)
                    )
                    for record in sample_records:
                        parse_value(record, schema)
                except Exception as parse_e:
                    failed_tables.append(
                        {
                            "table": table_name,
                            "reason": f"Failed to parse record with the schema "
                            f"for table: {str(parse_e)}",
                        }
                    )
                    continue

            except Exception as e:
                error_tables.append(
                    {
                        "table": table_name,
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )

        # Generate overall result
        total_tables = len(tables)
        passed_count = len(passed_tables)
        failed_count = len(failed_tables)
        error_count = len(error_tables)

        if error_count > 0:
            self._add_result(
                TestResult(
                    test_name=test_name,
                    status=TestStatus.ERROR,
                    message=f"Tested {total_tables} tables: {passed_count} passed, "
                    f"{failed_count} failed, {error_count} errors",
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )
        elif failed_count > 0:
            self._add_result(
                TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message=f"Tested {total_tables} tables: {passed_count} passed, "
                    f"{failed_count} failed",
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )
        else:
            self._add_result(
                TestResult(
                    test_name=test_name,
                    status=TestStatus.PASSED,
                    message=success_message.format(total_tables=total_tables),
                    details={
                        "total_tables": total_tables,
                        "passed_tables": passed_tables,
                        "failed_tables": failed_tables,
                        "error_tables": error_tables,
                    },
                )
            )

    def test_read_table(self):
        """Test read_table method on all available tables"""
        try:
            tables = self.connector.list_tables()
            if not tables:
                self._add_result(
                    TestResult(
                        test_name="test_read_table",
                        status=TestStatus.FAILED,
                        message="No tables available to test read_table",
                    )
                )
                return
        except Exception:
            self._add_result(
                TestResult(
                    test_name="test_read_table",
                    status=TestStatus.FAILED,
                    message="Could not get tables for testing read_table",
                )
            )
            return

        self._test_read_method(
            test_name="test_read_table",
            read_fn=self.connector.read_table,
            tables=tables,
            success_message="Successfully read all {total_tables} tables",
        )

    def test_read_table_deletes(self):
        """Test read_table_deletes method on tables with ingestion_type 'cdc_with_deletes'"""
        # Skip if connector doesn't implement read_table_deletes
        if not hasattr(self.connector, "read_table_deletes"):
            self._add_result(
                TestResult(
                    test_name="test_read_table_deletes",
                    status=TestStatus.PASSED,
                    message="Skipped: connector does not implement read_table_deletes",
                )
            )
            return

        # Get tables with ingestion_type 'cdc_with_deletes'
        try:
            all_tables = self.connector.list_tables()
            if not all_tables:
                self._add_result(
                    TestResult(
                        test_name="test_read_table_deletes",
                        status=TestStatus.PASSED,
                        message="Skipped: No tables available",
                    )
                )
                return
        except Exception:
            self._add_result(
                TestResult(
                    test_name="test_read_table_deletes",
                    status=TestStatus.FAILED,
                    message="Could not get tables for testing read_table_deletes",
                )
            )
            return

        # Filter to only tables with ingestion_type 'cdc_with_deletes'
        tables_with_deletes = []
        for table_name in all_tables:
            try:
                metadata = self.connector.read_table_metadata(
                    table_name, self._get_table_options(table_name)
                )
                if metadata.get("ingestion_type") == "cdc_with_deletes":
                    tables_with_deletes.append(table_name)
            except Exception:
                pass  # Skip tables we can't get metadata for

        if not tables_with_deletes:
            self._add_result(
                TestResult(
                    test_name="test_read_table_deletes",
                    status=TestStatus.PASSED,
                    message="Skipped: No tables with ingestion_type 'cdc_with_deletes'",
                )
            )
            return

        self._test_read_method(
            test_name="test_read_table_deletes",
            read_fn=self.connector.read_table_deletes,  # pylint: disable=no-member
            tables=tables_with_deletes,
            success_message="Successfully tested read_table_deletes on {total_tables} tables",
        )

    def _field_exists_in_schema(self, field_path: str, schema) -> bool:
        """
        Check if a field path exists in the schema (supports nested paths).
        Supports both top-level fields and nested fields (e.g., "properties.time").
        """
        # Handle simple field names without dots
        if "." not in field_path:
            return field_path in schema.fieldNames()

        # Handle nested paths
        parts = field_path.split(".", 1)
        field_name = parts[0]
        remaining_path = parts[1]

        if field_name not in schema.fieldNames():
            return False

        # Get the field type
        field = schema[field_name]
        field_type = field.dataType

        if isinstance(field_type, StructType):
            return self._field_exists_in_schema(remaining_path, field_type)

        # Field exists but can't traverse further (not a struct)
        return False

    def _validate_primary_keys(self, primary_keys: list, schema) -> bool:
        """
        Validate that all primary key columns exist in the schema.
        Supports both top-level fields and nested fields (e.g., "properties.time").
        """
        return all(self._field_exists_in_schema(field, schema) for field in primary_keys)

    def _should_validate_cursor_field(self, metadata: dict) -> bool:
        """
        Determine if cursor_field should be validated based on ingestion_type.
        """
        ingestion_type = metadata.get("ingestion_type")

        if ingestion_type in ("snapshot", "append"):
            return False

        return True

    def _should_validate_primary_key(self, metadata: dict) -> bool:
        """
        Determine if primary_keys should be validated based on ingestion_type.
        """
        ingestion_type = metadata.get("ingestion_type")

        if ingestion_type == "append":
            return False

        return True

    def test_list_insertable_tables(self):
        """Test that list_insertable_tables returns a subset of list_tables"""
        try:
            insertable_tables = self.connector_test_utils.list_insertable_tables()
            all_tables = self.connector.list_tables()

            if not isinstance(insertable_tables, list):
                self._add_result(
                    TestResult(
                        test_name="test_list_insertable_tables",
                        status=TestStatus.FAILED,
                        message=f"Expected list, got {type(insertable_tables).__name__}",
                    )
                )
                return

            # Check that insertable tables is a subset of all tables
            insertable_set = set(insertable_tables)
            all_tables_set = set(all_tables)

            if not insertable_set.issubset(all_tables_set):
                invalid_tables = insertable_set - all_tables_set
                self._add_result(
                    TestResult(
                        test_name="test_list_insertable_tables",
                        status=TestStatus.FAILED,
                        message=f"Insertable tables not subset of all tables: {invalid_tables}",
                    )
                )
                return

            self._add_result(
                TestResult(
                    test_name="test_list_insertable_tables",
                    status=TestStatus.PASSED,
                    message=f"Insertable tables ({len(insertable_tables)}) "
                    f"is subset of all tables ({len(all_tables)})",
                )
            )

        except Exception as e:
            self._add_result(
                TestResult(
                    test_name="test_list_insertable_tables",
                    status=TestStatus.ERROR,
                    message=f"list_insertable_tables failed: {str(e)}",
                    exception=e,
                    traceback_str=traceback.format_exc(),
                )
            )

    def test_write_to_source(self):  # pylint: disable=too-many-locals,too-many-branches
        """Test WriteToSource generate_rows_and_write method"""
        try:
            insertable_tables = self.connector_test_utils.list_insertable_tables()
            if not insertable_tables:
                self._add_result(
                    TestResult(
                        test_name="test_write_to_source",
                        status=TestStatus.FAILED,
                        message="No insertable tables available to test write functionality",
                    )
                )
                return

        except Exception:
            self._add_result(
                TestResult(
                    test_name="test_write_to_source",
                    status=TestStatus.FAILED,
                    message="Could not get insertable tables for testing write functionality",
                )
            )
            return

        # Test all insertable tables
        test_row_count = 1
        passed_tables = []
        failed_tables = []
        error_tables = []

        for test_table in insertable_tables:
            try:
                result = self.connector_test_utils.generate_rows_and_write(
                    test_table, test_row_count
                )

                # Validate return type is tuple with 3 elements
                if not isinstance(result, tuple) or len(result) != 3:
                    failed_tables.append(
                        {
                            "table": test_table,
                            "reason": f"Expected tuple of length 3, got {type(result).__name__}",
                        }
                    )
                    continue

                success, rows, column_names = result

                # Validate types of all return elements
                if (
                    not isinstance(success, bool)
                    or not isinstance(rows, list)
                    or not isinstance(column_names, dict)
                ):
                    failed_tables.append(
                        {
                            "table": test_table,
                            "reason": f"Invalid return types: "
                            f"success={type(success).__name__}, "
                            f"rows={type(rows).__name__}, "
                            f"column_mapping={type(column_names).__name__}",
                        }
                    )
                    continue

                # Validate consistency between success, rows, and column names
                if success:
                    if len(rows) != test_row_count:
                        failed_tables.append(
                            {
                                "table": test_table,
                                "reason": f"Expected {test_row_count} rows "
                                f"when successful, got {len(rows)}",
                            }
                        )
                        continue

                    if not column_names:
                        failed_tables.append(
                            {
                                "table": test_table,
                                "reason": "Expected non-empty column_mapping when successful",
                            }
                        )
                        continue

                    # Validate each row is a dict and has the expected columns
                    row_validation_failed = False
                    for i, row in enumerate(rows):
                        if not isinstance(row, dict):
                            failed_tables.append(
                                {
                                    "table": test_table,
                                    "reason": f"Row {i} is not a dict: {type(row).__name__}",
                                }
                            )
                            row_validation_failed = True
                            break

                    if row_validation_failed:
                        continue

                    passed_tables.append(
                        {
                            "table": test_table,
                            "rows": len(rows),
                            "column_mappings": len(column_names),
                        }
                    )
                else:
                    failed_tables.append(
                        {"table": test_table, "reason": "Write unsuccessful"}
                    )

            except Exception as e:
                error_tables.append({"table": test_table, "error": str(e)})

        # Generate overall result
        total_tables = len(insertable_tables)
        error_count = len(error_tables)
        failed_count = len(failed_tables)

        status = (
            TestStatus.ERROR
            if error_count > 0
            else TestStatus.FAILED
            if failed_count > 0
            else TestStatus.PASSED
        )
        message = (
            f"Tested {total_tables} insertable tables: {len(passed_tables)} passed, "
            f"{failed_count} failed, {error_count} errors"
            if error_count > 0 or failed_count > 0
            else f"Successfully tested write functionality on all {total_tables} insertable tables"
        )

        details = {"passed_tables": passed_tables}
        if failed_count > 0:
            details["failed_tables"] = failed_tables
        if error_count > 0:
            details["error_tables"] = error_tables

        self._add_result(
            TestResult(
                test_name="test_write_to_source",
                status=status,
                message=message,
                details=details,
            )
        )

    def test_incremental_after_write(self):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """Test incremental ingestion after writing a row.

        Should return 1 row if ingestion_type is incremental.
        """
        try:
            insertable_tables = self.connector_test_utils.list_insertable_tables()
            if not insertable_tables:
                self._add_result(
                    TestResult(
                        test_name="test_incremental_after_write",
                        status=TestStatus.FAILED,
                        message="No insertable tables available to test incremental ingestion",
                    )
                )
                return

        except Exception:
            self._add_result(
                TestResult(
                    test_name="test_incremental_after_write",
                    status=TestStatus.FAILED,
                    message="Could not get insertable tables for testing incremental ingestion",
                )
            )
            return

        # Test all insertable tables
        passed_tables = []
        failed_tables = []
        error_tables = []

        for test_table in insertable_tables:
            try:
                # First, check if the table supports incremental ingestion
                metadata = self.connector.read_table_metadata(
                    test_table, self._get_table_options(test_table)
                )

                # Get initial state for incremental read
                initial_result = self.connector.read_table(
                    test_table, {}, self._get_table_options(test_table)
                )
                if not isinstance(initial_result, tuple) or len(initial_result) != 2:
                    failed_tables.append(
                        {
                            "table": test_table,
                            "reason": "Failed to get initial table state",
                        }
                    )
                    continue

                initial_iterator, initial_offset = initial_result

                # Count initial records for snapshot comparison
                initial_record_count = 0
                if metadata.get("ingestion_type") == "snapshot":
                    for record in initial_iterator:
                        initial_record_count += 1

                # Write 1 row to the table
                write_result = self.connector_test_utils.generate_rows_and_write(
                    test_table, 1
                )
                if not isinstance(write_result, tuple) or len(write_result) != 3:
                    failed_tables.append(
                        {
                            "table": test_table,
                            "reason": "Write operation failed - invalid return format",
                        }
                    )
                    continue

                write_success, written_rows, column_mapping = write_result
                if not write_success:
                    failed_tables.append(
                        {
                            "table": test_table,
                            "reason": "Write operation was not successful",
                        }
                    )
                    continue

                # Perform read after write
                after_write_result = self.connector.read_table(
                    test_table, initial_offset, self._get_table_options(test_table)
                )
                if (
                    not isinstance(after_write_result, tuple)
                    or len(after_write_result) != 2
                ):
                    failed_tables.append(
                        {
                            "table": test_table,
                            "reason": "Read after write failed - invalid return format",
                        }
                    )
                    continue

                after_write_iterator, new_offset = after_write_result

                # Count records returned after write
                after_write_records = []
                for record in after_write_iterator:
                    after_write_records.append(record)

                # Validate based on ingestion type
                ingestion_type = metadata.get("ingestion_type", "cdc")
                actual_count = len(after_write_records)

                if ingestion_type in ["cdc", "cdc_with_deletes", "append"]:
                    # CDC/Append: should return at least 1 record (allows for concurrent writes)
                    if actual_count < 1:
                        failed_tables.append(
                            {
                                "table": test_table,
                                "reason": f"Expected at least 1 record "
                                f"for {ingestion_type}, got {actual_count}",
                                "ingestion_type": ingestion_type,
                                "expected_count": "≥ 1",
                                "actual_count": actual_count,
                            }
                        )
                        continue
                else:
                    # Snapshot: should return exactly initial + 1 records
                    expected_count = initial_record_count + 1
                    if actual_count != expected_count:
                        failed_tables.append(
                            {
                                "table": test_table,
                                "reason": f"Expected exactly {expected_count} records "
                                f"for {ingestion_type}, got {actual_count}",
                                "ingestion_type": ingestion_type,
                                "expected_count": expected_count,
                                "actual_count": actual_count,
                            }
                        )
                        continue

                # Verify written rows are present in returned results
                if not self._verify_written_rows_present(
                    written_rows, after_write_records, column_mapping
                ):
                    failed_tables.append(
                        {
                            "table": test_table,
                            "reason": "Written rows not found in returned results",
                            "ingestion_type": ingestion_type,
                            "written_rows_count": len(written_rows),
                            "returned_records_count": actual_count,
                        }
                    )
                    continue

                expected_display = (
                    "≥ 1"
                    if ingestion_type in ["cdc", "cdc_with_deletes", "append"]
                    else str(initial_record_count + 1)
                )
                passed_tables.append(
                    {
                        "table": test_table,
                        "ingestion_type": ingestion_type,
                        "expected_count": expected_display,
                        "actual_count": actual_count,
                    }
                )

            except Exception as e:
                error_tables.append({"table": test_table, "error": str(e)})

        # Generate overall result
        total_tables = len(insertable_tables)
        error_count = len(error_tables)
        failed_count = len(failed_tables)

        status = (
            TestStatus.ERROR
            if error_count > 0
            else TestStatus.FAILED
            if failed_count > 0
            else TestStatus.PASSED
        )
        message = (
            f"Tested {total_tables} insertable tables: {len(passed_tables)} passed, "
            f"{failed_count} failed, {error_count} errors"
            if error_count > 0 or failed_count > 0
            else f"Successfully verified incremental ingestion "
            f"on all {total_tables} insertable tables"
        )

        details = {"passed_tables": passed_tables}
        if failed_count > 0:
            details["failed_tables"] = failed_tables
        if error_count > 0:
            details["error_tables"] = error_tables

        self._add_result(
            TestResult(
                test_name="test_incremental_after_write",
                status=status,
                message=message,
                details=details,
            )
        )

    def _verify_written_rows_present(
        self,
        written_rows: List[Dict],
        returned_records: List[Dict],
        column_mapping: Dict[str, str],
    ) -> bool:
        """
        Verify that written rows are present in the returned results.
        Compares mapped column values. Supports nested column paths using
        dot notation (e.g., 'properties.email').
        """
        if not written_rows or not column_mapping:
            return True

        # Extract comparison data from written rows
        written_signatures = []
        for row in written_rows:
            signature = {
                written_col: row.get(written_col)
                for written_col in column_mapping.keys()
                if written_col in row
            }
            print(f"\nwritten row: {signature}\n")
            written_signatures.append(signature)

        # Extract comparison data from returned records using column mapping
        returned_signatures = []
        for record in returned_records:
            # Handle JSON string records
            if isinstance(record, str):
                try:
                    record = json.loads(record)
                except:
                    continue

            if isinstance(record, dict):
                signature = {}
                for written_col, returned_col in column_mapping.items():
                    # Handle nested paths (e.g., 'properties.email')
                    value = self._get_nested_value(record, returned_col)
                    if value is not None:
                        signature[written_col] = value

                print(f"\nreturned row: {signature}\n")
                if signature:
                    returned_signatures.append(signature)

        # Check if all written signatures are present in returned signatures
        for written_sig in written_signatures:
            if written_sig not in returned_signatures:
                return False

        return True

    def _get_nested_value(self, record: Dict, path: str) -> Any:
        """
        Get a value from a nested dictionary using dot notation path.
        E.g., _get_nested_value({"properties": {"email": "x"}}, "properties.email") -> "x"
        """
        parts = path.split('.')
        current = record
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def _add_result(self, result: TestResult):
        """Add a test result to the collection"""
        self.test_results.append(result)

    def _get_table_options(self, table_name: str) -> Dict[str, Any]:
        """
        Helper to fetch table_options for a given table.
        Returns an empty dict if no specific config is provided.
        """
        return self._table_configs.get(table_name, {})

    def _generate_report(self) -> TestReport:
        """Generate a comprehensive test report"""
        passed = len([r for r in self.test_results if r.status == TestStatus.PASSED])
        failed = len([r for r in self.test_results if r.status == TestStatus.FAILED])
        errors = len([r for r in self.test_results if r.status == TestStatus.ERROR])

        return TestReport(
            connector_class_name="LakeflowConnect",
            test_results=self.test_results,
            total_tests=len(self.test_results),
            passed_tests=passed,
            failed_tests=failed,
            error_tests=errors,
            timestamp=datetime.now().isoformat(),  # pylint: disable=no-member
        )

    def print_report(self, report: TestReport, show_details: bool = True):
        print(f"\n{'=' * 50}")
        print(f"LAKEFLOW CONNECT TEST REPORT")
        print(f"{'=' * 50}")
        print(f"Connector Class: {report.connector_class_name}")
        print(f"Timestamp: {report.timestamp}")
        print(f"\nSUMMARY:")
        print(f"  Total Tests: {report.total_tests}")
        print(f"  Passed: {report.passed_tests}")
        print(f"  Failed: {report.failed_tests}")
        print(f"  Errors: {report.error_tests}")
        print(f"  Success Rate: {report.success_rate():.1f}%")

        if show_details:
            print(f"\nTEST RESULTS:")
            print(f"{'-' * 50}")

            for result in report.test_results:
                status_symbol = {
                    TestStatus.PASSED: "✅",
                    TestStatus.FAILED: "❌",
                    TestStatus.ERROR: "❗",
                }

                print(f"{status_symbol[result.status]} {result.test_name}")
                print(f"  Status: {result.status.value}")
                print(f"  Message: {result.message}")

                if result.details:
                    print(
                        f"  Details: {json.dumps(result.details, indent=4, default=str)}"
                    )

                if result.traceback_str and result.status in [
                    TestStatus.ERROR,
                    TestStatus.FAILED,
                ]:
                    print(f"  Traceback:\n{result.traceback_str}")

                print()

        if report.failed_tests > 0 or report.error_tests > 0:
            failed_tests = [
                r.test_name
                for r in report.test_results
                if r.status == TestStatus.FAILED
            ]
            error_tests = [
                r.test_name for r in report.test_results if r.status == TestStatus.ERROR
            ]

            error_parts = []
            if failed_tests:
                error_parts.append(f"Failed tests: {', '.join(failed_tests)}")
            if error_tests:
                error_parts.append(f"Error tests: {', '.join(error_tests)}")
            error_message = (
                f"Test suite failed with {report.failed_tests} failures "
                f"and {report.error_tests} errors. {' | '.join(error_parts)}"
            )
            raise TestFailedException(error_message, report)
