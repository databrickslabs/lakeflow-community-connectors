# pylint: disable=too-many-lines
"""Test suite for LakeflowConnect implementations.

Design goals
------------
1. **Per-table diagnostics** – every table emits its own TestResult so an LLM
   agent (or human) can see exactly which table failed which check.
2. **Actionable fix hints** – every failure includes a ``fix_hint`` that tells
   the agent what code to change.
3. **Pagination-contract test** – validates the offset protocol the framework
   relies on (stop when ``end_offset == start_offset``).
4. **Machine-readable output** – ``TestReport.to_json()`` and
   ``TestReport.failure_summary()`` for programmatic consumption.
"""

import json
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from pyspark.sql.types import *  # pylint: disable=wildcard-import,unused-wildcard-import

from databricks.labs.community_connector.libs.utils import parse_value

# Valid values for read_table_metadata()["ingestion_type"]
VALID_INGESTION_TYPES = {"snapshot", "cdc", "cdc_with_deletes", "append"}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

class TestStatus(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    ERROR = "ERROR"


@dataclass
class TestResult:
    """Represents the result of a single test."""

    test_name: str
    status: TestStatus
    message: str = ""
    fix_hint: Optional[str] = None
    table_name: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    exception: Optional[Exception] = None
    traceback_str: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "test_name": self.test_name,
            "status": self.status.value,
            "message": self.message,
        }
        if self.fix_hint:
            d["fix_hint"] = self.fix_hint
        if self.table_name:
            d["table_name"] = self.table_name
        if self.details:
            d["details"] = self.details
        if self.traceback_str:
            d["traceback"] = self.traceback_str
        return d


@dataclass
class TestReport:
    """Complete test report."""

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

    # -- Machine-readable helpers ------------------------------------------

    def to_json(self, indent: int = 2) -> str:
        """Return the full report as a JSON string."""
        return json.dumps(
            {
                "connector_class_name": self.connector_class_name,
                "timestamp": self.timestamp,
                "summary": {
                    "total": self.total_tests,
                    "passed": self.passed_tests,
                    "failed": self.failed_tests,
                    "errors": self.error_tests,
                    "success_rate": round(self.success_rate(), 1),
                },
                "results": [r.to_dict() for r in self.test_results],
            },
            indent=indent,
            default=str,
        )

    def failure_summary(self) -> str:
        """Return a compact, LLM-friendly summary of failures only.

        Each line contains the test name, a one-sentence reason, and the fix
        hint (if available).  Passed tests are omitted so the agent can focus
        on what to fix.

        Ends with a ``pytest -k`` command to re-run only the failing tests.
        """
        lines: List[str] = []
        failed_top_level_names: set = set()
        for r in self.test_results:
            if r.status == TestStatus.PASSED:
                continue
            parts = [f"[{r.status.value}] {r.test_name}: {r.message}"]
            if r.fix_hint:
                parts.append(f"  -> Fix: {r.fix_hint}")
            if r.traceback_str:
                # Only keep the last line of the traceback (the actual error).
                last_line = r.traceback_str.strip().rsplit("\n", 1)[-1]
                parts.append(f"  -> Exception: {last_line}")
            lines.append("\n".join(parts))
            # Extract top-level name: "test_read_table[orders]" -> "test_read_table"
            top_name = r.test_name.split("[")[0]
            failed_top_level_names.add(top_name)
        if not lines:
            return "All tests passed."
        rerun_expr = " or ".join(sorted(failed_top_level_names))
        lines.append(
            f"To re-run only the failed tests:\n"
            f"  pytest -k \"{rerun_expr}\" <test_file>"
        )
        return "\n\n".join(lines)


class TestFailedException(Exception):
    """Exception raised when tests fail or have errors."""

    def __init__(self, message: str, report: TestReport):
        super().__init__(message)
        self.report = report


# ---------------------------------------------------------------------------
# Tester
# ---------------------------------------------------------------------------

class LakeflowConnectTester:
    def __init__(
        self,
        init_options: dict,
        table_configs: Dict[str, Dict[str, Any]] = {},
        sample_records: int = 10,
    ):
        self._init_options = init_options
        # Per-table configuration passed as table_options into connector methods.
        self._table_configs: Dict[str, Dict[str, Any]] = table_configs
        # Number of records to sample from iterators during read validation.
        self._sample_records: int = sample_records
        self.test_results: List[TestResult] = []

    # ------------------------------------------------------------------
    # Test registry & entry point
    # ------------------------------------------------------------------

    # Ordered list of all test method names.  The order matters because some
    # tests depend on earlier ones (e.g. everything after initialization
    # requires a working connector instance).
    ALL_TESTS: List[str] = [
        "test_initialization",
        "test_list_tables",
        "test_get_table_schema",
        "test_read_table_metadata",
        "test_read_table",
        "test_micro_batch_offset_contract",
        "test_read_table_deletes",
    ]

    # Write-back tests (only run when connector_test_utils is available).
    WRITE_BACK_TESTS: List[str] = [
        "test_list_insertable_tables",
        "test_list_deletable_tables",
        "test_write_to_source",
        "test_incremental_after_write",
        "test_delete_and_read_deletes",
    ]

    def run_all_tests(self) -> TestReport:
        """Run all available tests and return a comprehensive report."""
        self.test_results = []

        self.test_initialization()

        if self.connector is None:
            return self._generate_report()

        for name in self.ALL_TESTS:
            if name == "test_initialization":
                continue
            getattr(self, name)()

        has_write_utils = (
            hasattr(self, "connector_test_utils")
            and self.connector_test_utils is not None
        )
        if has_write_utils:
            for name in self.WRITE_BACK_TESTS:
                getattr(self, name)()

        return self._generate_report()

    # ------------------------------------------------------------------
    # test_initialization
    # ------------------------------------------------------------------

    def test_initialization(self):
        """Test connector initialization."""
        try:
            self.connector = LakeflowConnect(self._init_options)  # pylint: disable=undefined-variable

            # Try to initialize test utils – not all connectors have them.
            try:
                self.connector_test_utils = LakeflowConnectTestUtils(self._init_options)  # pylint: disable=undefined-variable
            except Exception:
                self.connector_test_utils = None

            if self.connector is None:
                self._add_result(TestResult(
                    test_name="test_initialization",
                    status=TestStatus.FAILED,
                    message="Connector initialization returned None",
                    fix_hint="__init__ must not return None. Ensure it stores options and returns normally.",
                ))
            else:
                self._add_result(TestResult(
                    test_name="test_initialization",
                    status=TestStatus.PASSED,
                    message="Connector initialized successfully",
                ))

        except Exception as e:
            self._add_result(TestResult(
                test_name="test_initialization",
                status=TestStatus.ERROR,
                message=f"Initialization failed: {e}",
                fix_hint=(
                    "The connector's __init__(options) raised an exception. "
                    "Check that all required keys are present in options and that "
                    "no network calls happen during __init__."
                ),
                exception=e,
                traceback_str=traceback.format_exc(),
            ))
            self.connector = None

    # ------------------------------------------------------------------
    # test_list_tables
    # ------------------------------------------------------------------

    def test_list_tables(self):
        """Test list_tables returns a non-empty list of unique strings."""
        try:
            tables = self.connector.list_tables()

            if not isinstance(tables, list):
                self._add_result(TestResult(
                    test_name="test_list_tables",
                    status=TestStatus.FAILED,
                    message=f"Expected list, got {type(tables).__name__}",
                    fix_hint="list_tables() must return a list[str].",
                    details={"returned_type": str(type(tables)), "returned_value": str(tables)},
                ))
                return

            if not tables:
                self._add_result(TestResult(
                    test_name="test_list_tables",
                    status=TestStatus.FAILED,
                    message="list_tables() returned an empty list",
                    fix_hint="list_tables() must return at least one table name.",
                ))
                return

            for i, table in enumerate(tables):
                if not isinstance(table, str):
                    self._add_result(TestResult(
                        test_name="test_list_tables",
                        status=TestStatus.FAILED,
                        message=f"Table name at index {i} is not a string: {type(table).__name__}",
                        fix_hint="Every element of list_tables() must be a str.",
                        details={"table_index": i, "table_value": str(table)},
                    ))
                    return

            # Check uniqueness
            duplicates = [t for t in tables if tables.count(t) > 1]
            if duplicates:
                self._add_result(TestResult(
                    test_name="test_list_tables",
                    status=TestStatus.FAILED,
                    message=f"Duplicate table names: {sorted(set(duplicates))}",
                    fix_hint="list_tables() must return unique table names. Remove duplicates.",
                ))
                return

            self._add_result(TestResult(
                test_name="test_list_tables",
                status=TestStatus.PASSED,
                message=f"Returned {len(tables)} tables: {tables}",
                details={"tables": tables, "count": len(tables)},
            ))

        except Exception as e:
            self._add_result(TestResult(
                test_name="test_list_tables",
                status=TestStatus.ERROR,
                message=f"list_tables() raised: {e}",
                fix_hint="list_tables() must not raise exceptions.",
                exception=e,
                traceback_str=traceback.format_exc(),
            ))

    # ------------------------------------------------------------------
    # test_get_table_schema  (per-table)
    # ------------------------------------------------------------------

    def test_get_table_schema(self):
        """Test get_table_schema for every table."""
        tables = self._get_tables_safe("test_get_table_schema")
        if tables is None:
            return

        for table_name in tables:
            test_name = f"test_get_table_schema[{table_name}]"
            try:
                schema = self.connector.get_table_schema(
                    table_name, self._get_table_options(table_name)
                )

                if not isinstance(schema, StructType):
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Expected StructType, got {type(schema).__name__}",
                        fix_hint="get_table_schema() must return a pyspark.sql.types.StructType.",
                        table_name=table_name,
                    ))
                    continue

                if not schema.fields:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Schema has no fields",
                        fix_hint="get_table_schema() returned an empty StructType. Add StructField entries.",
                        table_name=table_name,
                    ))
                    continue

                # Check for duplicate field names
                field_names = [f.name for f in schema.fields]
                dupes = [n for n in field_names if field_names.count(n) > 1]
                if dupes:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Duplicate field names in schema: {sorted(set(dupes))}",
                        fix_hint="Schema field names must be unique. Remove or rename duplicates.",
                        table_name=table_name,
                    ))
                    continue

                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.PASSED,
                    message=f"Schema has {len(schema.fields)} fields",
                    table_name=table_name,
                    details={
                        "field_count": len(schema.fields),
                        "fields": [
                            {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
                            for f in schema.fields
                        ],
                    },
                ))

            except Exception as e:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.ERROR,
                    message=f"get_table_schema raised: {e}",
                    fix_hint=f"get_table_schema('{table_name}', ...) must not raise. Check the table name mapping.",
                    table_name=table_name,
                    exception=e,
                    traceback_str=traceback.format_exc(),
                ))

    # ------------------------------------------------------------------
    # test_read_table_metadata  (per-table)
    # ------------------------------------------------------------------

    def test_read_table_metadata(self):  # pylint: disable=too-many-branches
        """Test read_table_metadata for every table."""
        tables = self._get_tables_safe("test_read_table_metadata")
        if tables is None:
            return

        for table_name in tables:
            test_name = f"test_read_table_metadata[{table_name}]"
            try:
                metadata = self.connector.read_table_metadata(
                    table_name, self._get_table_options(table_name)
                )

                if not isinstance(metadata, dict):
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Expected dict, got {type(metadata).__name__}",
                        fix_hint="read_table_metadata() must return a dict.",
                        table_name=table_name,
                    ))
                    continue

                # ---- ingestion_type ----------------------------------
                ingestion_type = metadata.get("ingestion_type")
                if ingestion_type is None:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Missing required key 'ingestion_type'",
                        fix_hint=(
                            "read_table_metadata() must return a dict with 'ingestion_type'. "
                            f"Valid values: {sorted(VALID_INGESTION_TYPES)}"
                        ),
                        table_name=table_name,
                    ))
                    continue

                if ingestion_type not in VALID_INGESTION_TYPES:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Invalid ingestion_type: '{ingestion_type}'",
                        fix_hint=f"ingestion_type must be one of {sorted(VALID_INGESTION_TYPES)}.",
                        table_name=table_name,
                        details={"ingestion_type": ingestion_type},
                    ))
                    continue

                # ---- primary_keys ------------------------------------
                primary_keys = metadata.get("primary_keys")
                pk_ok = True
                if self._should_validate_primary_key(metadata):
                    if primary_keys is None:
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message="Missing required key 'primary_keys'",
                            fix_hint=(
                                "read_table_metadata() must include 'primary_keys' (list[str]) "
                                f"for ingestion_type='{ingestion_type}'."
                            ),
                            table_name=table_name,
                        ))
                        continue
                    if not isinstance(primary_keys, list):
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message=f"primary_keys should be list, got {type(primary_keys).__name__}",
                            fix_hint="primary_keys must be a list of column name strings.",
                            table_name=table_name,
                        ))
                        continue
                    if not primary_keys:
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message="primary_keys is empty",
                            fix_hint=(
                                f"For ingestion_type='{ingestion_type}', primary_keys must "
                                "contain at least one column name."
                            ),
                            table_name=table_name,
                        ))
                        continue

                    # Validate PKs exist in schema
                    try:
                        schema = self.connector.get_table_schema(
                            table_name, self._get_table_options(table_name)
                        )
                        if not self._validate_primary_keys(primary_keys, schema):
                            missing = [
                                pk for pk in primary_keys
                                if not self._field_exists_in_schema(pk, schema)
                            ]
                            self._add_result(TestResult(
                                test_name=test_name,
                                status=TestStatus.FAILED,
                                message=f"primary_keys {missing} not found in schema",
                                fix_hint=(
                                    f"These primary key columns are missing from get_table_schema('{table_name}'). "
                                    "Either add them to the schema or fix the primary_keys list."
                                ),
                                table_name=table_name,
                                details={"primary_keys": primary_keys, "schema_fields": schema.fieldNames()},
                            ))
                            pk_ok = False
                    except Exception:
                        pass  # Schema test already covers schema errors.

                # ---- cursor_field ------------------------------------
                if self._should_validate_cursor_field(metadata):
                    cursor_field = metadata.get("cursor_field")
                    if cursor_field is None:
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message="Missing required key 'cursor_field'",
                            fix_hint=(
                                f"read_table_metadata() must include 'cursor_field' (str) "
                                f"for ingestion_type='{ingestion_type}'."
                            ),
                            table_name=table_name,
                        ))
                        continue
                    if not isinstance(cursor_field, str):
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message=f"cursor_field should be str, got {type(cursor_field).__name__}",
                            fix_hint="cursor_field must be a single column name string, not a list or other type.",
                            table_name=table_name,
                        ))
                        continue

                    # Validate cursor field exists in schema
                    try:
                        schema = self.connector.get_table_schema(
                            table_name, self._get_table_options(table_name)
                        )
                        if not self._field_exists_in_schema(cursor_field, schema):
                            self._add_result(TestResult(
                                test_name=test_name,
                                status=TestStatus.FAILED,
                                message=f"cursor_field '{cursor_field}' not found in schema",
                                fix_hint=(
                                    f"The cursor_field '{cursor_field}' must exist in get_table_schema('{table_name}'). "
                                    "Either add it to the schema or choose a different cursor_field."
                                ),
                                table_name=table_name,
                                details={"cursor_field": cursor_field, "schema_fields": schema.fieldNames()},
                            ))
                            continue
                    except Exception:
                        pass

                # ---- cdc_with_deletes requires read_table_deletes ----
                if (
                    ingestion_type == "cdc_with_deletes"
                    and not hasattr(self.connector, "read_table_deletes")
                ):
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="ingestion_type is 'cdc_with_deletes' but connector does not implement read_table_deletes()",
                        fix_hint=(
                            "Either implement read_table_deletes() or change ingestion_type to 'cdc'."
                        ),
                        table_name=table_name,
                    ))
                    continue

                if not pk_ok:
                    continue

                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.PASSED,
                    message=f"Metadata valid (ingestion_type={ingestion_type})",
                    table_name=table_name,
                    details={"metadata": metadata},
                ))

            except Exception as e:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.ERROR,
                    message=f"read_table_metadata raised: {e}",
                    fix_hint=f"read_table_metadata('{table_name}', ...) must not raise.",
                    table_name=table_name,
                    exception=e,
                    traceback_str=traceback.format_exc(),
                ))

    # ------------------------------------------------------------------
    # test_read_table  (per-table, delegates to shared helper)
    # ------------------------------------------------------------------

    def test_read_table(self):
        """Test read_table for every table."""
        tables = self._get_tables_safe("test_read_table")
        if tables is None:
            return

        for table_name in tables:
            self._test_read_for_table(
                test_name_prefix="test_read_table",
                read_fn=self.connector.read_table,
                table_name=table_name,
                is_read_table=True,
            )

    # ------------------------------------------------------------------
    # test_read_table_deletes  (per-table for cdc_with_deletes tables)
    # ------------------------------------------------------------------

    def test_read_table_deletes(self):
        """Test read_table_deletes for tables with ingestion_type 'cdc_with_deletes'."""
        if not hasattr(self.connector, "read_table_deletes"):
            self._add_result(TestResult(
                test_name="test_read_table_deletes",
                status=TestStatus.PASSED,
                message="Skipped: connector does not implement read_table_deletes",
            ))
            return

        tables = self._get_tables_safe("test_read_table_deletes")
        if tables is None:
            return

        tables_with_deletes = self._filter_tables_by_ingestion_type(tables, "cdc_with_deletes")
        if not tables_with_deletes:
            self._add_result(TestResult(
                test_name="test_read_table_deletes",
                status=TestStatus.PASSED,
                message="Skipped: no tables with ingestion_type 'cdc_with_deletes'",
            ))
            return

        for table_name in tables_with_deletes:
            self._test_read_for_table(
                test_name_prefix="test_read_table_deletes",
                read_fn=self.connector.read_table_deletes,  # pylint: disable=no-member
                table_name=table_name,
                is_read_table=False,
            )

    # ------------------------------------------------------------------
    # test_pagination_contract  (per-table)
    # ------------------------------------------------------------------

    def test_micro_batch_offset_contract(self):
        """Test that the read_table micro-batch offset protocol works.

        The framework repeatedly calls ``read_table(table, prev_offset, opts)``
        in successive micro-batches and stops when ``new_offset == prev_offset``.
        This test makes **two** calls per table to verify the contract without
        excessive API usage.
        """
        tables = self._get_tables_safe("test_micro_batch_offset_contract")
        if tables is None:
            return

        for table_name in tables:
            test_name = f"test_micro_batch_offset_contract[{table_name}]"
            try:
                # --- Call 1: initial read (empty offset) ---------------
                result1 = self.connector.read_table(
                    table_name, {}, self._get_table_options(table_name)
                )

                if not isinstance(result1, tuple) or len(result1) != 2:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"First read_table call returned {type(result1).__name__}, expected 2-tuple",
                        fix_hint="read_table() must always return (iterator, offset_dict).",
                        table_name=table_name,
                    ))
                    continue

                iter1, offset1 = result1
                page1 = self._consume_iterator(iter1)

                if offset1 is not None and not isinstance(offset1, dict):
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Offset must be dict or None, got {type(offset1).__name__}",
                        fix_hint="read_table() must return a dict as the second element (the offset).",
                        table_name=table_name,
                    ))
                    continue

                # None offset → table reads everything in one batch
                if offset1 is None:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.PASSED,
                        message=f"Table returns all data in one batch (offset=None, {len(page1)} records)",
                        table_name=table_name,
                    ))
                    continue

                # Offset must be JSON-serializable
                try:
                    json.dumps(offset1)
                except (TypeError, ValueError) as je:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Offset is not JSON-serializable: {je}",
                        fix_hint=(
                            "The offset dict must be JSON-serializable because the framework "
                            "checkpoints it. Avoid datetime objects or custom classes as values."
                        ),
                        table_name=table_name,
                        details={"offset": str(offset1)},
                    ))
                    continue

                # --- Call 2: read from offset1 -------------------------
                result2 = self.connector.read_table(
                    table_name, offset1, self._get_table_options(table_name)
                )

                if not isinstance(result2, tuple) or len(result2) != 2:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Second read_table call (with offset) returned invalid format",
                        fix_hint=(
                            "read_table() must handle receiving its own previously-returned "
                            "offset as start_offset. Ensure the offset dict is correctly parsed."
                        ),
                        table_name=table_name,
                    ))
                    continue

                iter2, offset2 = result2
                page2 = self._consume_iterator(iter2)

                if offset2 is not None and not isinstance(offset2, dict):
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Second offset must be dict or None, got {type(offset2).__name__}",
                        table_name=table_name,
                    ))
                    continue

                terminated = (offset2 == offset1) or (offset2 is None)
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.PASSED,
                    message=(
                        f"Pagination OK: page1={len(page1)} records, page2={len(page2)} records"
                        f"{' (terminated)' if terminated else ' (more data available)'}"
                    ),
                    table_name=table_name,
                    details={
                        "offset_after_page1": offset1,
                        "offset_after_page2": offset2,
                        "terminated": terminated,
                    },
                ))

            except Exception as e:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.ERROR,
                    message=f"Pagination test error: {e}",
                    fix_hint=(
                        "read_table() raised an exception when called with its own offset. "
                        "Make sure the connector can round-trip its offsets."
                    ),
                    table_name=table_name,
                    exception=e,
                    traceback_str=traceback.format_exc(),
                ))

    # ------------------------------------------------------------------
    # Shared read-method helper (per-table)
    # ------------------------------------------------------------------

    def _test_read_for_table(  # pylint: disable=too-many-return-statements,too-many-branches
        self,
        test_name_prefix: str,
        read_fn: Callable,
        table_name: str,
        is_read_table: bool = True,
    ):
        """Validate a single table's read method and emit one TestResult.

        Args:
            test_name_prefix: e.g. "test_read_table" or "test_read_table_deletes"
            read_fn: The bound method to call (connector.read_table or connector.read_table_deletes)
            table_name: Table to test
            is_read_table: True for read_table, False for read_table_deletes
        """
        test_name = f"{test_name_prefix}[{table_name}]"
        method_name = "read_table" if is_read_table else "read_table_deletes"

        try:
            result = read_fn(table_name, {}, self._get_table_options(table_name))

            # ---- Return type ----
            if not isinstance(result, tuple) or len(result) != 2:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message=f"Expected 2-tuple (Iterator, dict), got {type(result).__name__}",
                    fix_hint=f"{method_name}() must return (records_iterator, offset_dict).",
                    table_name=table_name,
                ))
                return

            iterator, offset = result

            # ---- Iterator ----
            if not hasattr(iterator, "__iter__"):
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message=f"First element is not iterable: {type(iterator).__name__}",
                    fix_hint=f"{method_name}() must return an iterator/generator as the first element.",
                    table_name=table_name,
                ))
                return

            # ---- Offset type ----
            if offset is not None and not isinstance(offset, dict):
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message=f"Offset must be dict or None, got {type(offset).__name__}",
                    fix_hint=f"{method_name}() must return a dict (or None) as the second element.",
                    table_name=table_name,
                ))
                return

            # ---- Offset JSON-serializable ----
            if isinstance(offset, dict):
                try:
                    json.dumps(offset)
                except (TypeError, ValueError) as je:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Offset is not JSON-serializable: {je}",
                        fix_hint="Offset values must be JSON-serializable (strings, numbers, booleans, None).",
                        table_name=table_name,
                        details={"offset": str(offset)},
                    ))
                    return

            # ---- Consume iterator ----
            sample_records: List[dict] = []
            try:
                for record in iterator:
                    if not isinstance(record, dict):
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message=f"Record is {type(record).__name__}, expected dict",
                            fix_hint=f"{method_name}() must yield dicts. Got: {str(record)[:200]}",
                            table_name=table_name,
                        ))
                        return
                    sample_records.append(record)
                    if len(sample_records) >= self._sample_records:
                        break
            except Exception as iter_e:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message=f"Iterator raised during iteration: {iter_e}",
                    fix_hint=f"The iterator returned by {method_name}() raised an exception while yielding records.",
                    table_name=table_name,
                    exception=iter_e,
                    traceback_str=traceback.format_exc(),
                ))
                return

            # ---- Parse records with schema ----
            try:
                schema = self.connector.get_table_schema(
                    table_name, self._get_table_options(table_name)
                )
            except Exception as schema_e:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.ERROR,
                    message=f"Could not fetch schema for record validation: {schema_e}",
                    fix_hint="get_table_schema() is needed to validate records. Fix it first.",
                    table_name=table_name,
                ))
                return

            for i, record in enumerate(sample_records):
                try:
                    parse_value(record, schema)
                except Exception as parse_e:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Record {i} failed schema parsing: {parse_e}",
                        fix_hint=(
                            f"The record from {method_name}() cannot be parsed with the schema from "
                            f"get_table_schema('{table_name}'). Either fix the record format or "
                            "adjust the schema. The framework handles type conversion — return raw values."
                        ),
                        table_name=table_name,
                        details={"record": record, "schema_fields": schema.fieldNames()},
                    ))
                    return

            # ---- Field-level validations ----
            if sample_records:
                for record in sample_records:
                    # Non-nullable check (read_table only)
                    if is_read_table:
                        violations = self._check_non_nullable_fields(record, schema)
                        if violations:
                            self._add_result(TestResult(
                                test_name=test_name,
                                status=TestStatus.FAILED,
                                message=f"Non-nullable field(s) are None: {violations}",
                                fix_hint=(
                                    "Either make these fields nullable=True in get_table_schema() "
                                    f"or ensure {method_name}() always populates them."
                                ),
                                table_name=table_name,
                                details={"violations": violations, "record": record},
                            ))
                            return

                    # All-columns-null check
                    if self._all_columns_null(record, schema):
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message="All columns are null in a record",
                            fix_hint=(
                                f"{method_name}() yielded a record where every field is None. "
                                "This usually means the API response is not being parsed correctly."
                            ),
                            table_name=table_name,
                            details={"record": record},
                        ))
                        return

                    # Primary-key presence check (for read_table_deletes)
                    if not is_read_table:
                        try:
                            metadata = self.connector.read_table_metadata(
                                table_name, self._get_table_options(table_name)
                            )
                            pk_fields = metadata.get("primary_keys", [])
                            missing_pks = [
                                pk for pk in pk_fields
                                if self._get_nested_value(record, pk) is None
                            ]
                            if missing_pks:
                                self._add_result(TestResult(
                                    test_name=test_name,
                                    status=TestStatus.FAILED,
                                    message=f"Deleted record missing primary key(s): {missing_pks}",
                                    fix_hint=(
                                        "read_table_deletes() must include primary key fields in every record "
                                        "so the framework can identify which rows were deleted."
                                    ),
                                    table_name=table_name,
                                    details={"record": record, "primary_keys": pk_fields},
                                ))
                                return
                        except Exception:
                            pass  # Metadata errors covered elsewhere.

            # ---- PASSED ----
            self._add_result(TestResult(
                test_name=test_name,
                status=TestStatus.PASSED,
                message=f"Read {len(sample_records)} records successfully",
                table_name=table_name,
                details={
                    "records_sampled": len(sample_records),
                    "offset_keys": list(offset.keys()) if isinstance(offset, dict) else None,
                    "sample_records": sample_records[:2],
                },
            ))

        except Exception as e:
            self._add_result(TestResult(
                test_name=test_name,
                status=TestStatus.ERROR,
                message=f"{method_name} raised: {e}",
                fix_hint=f"{method_name}('{table_name}', {{}}, ...) must not raise.",
                table_name=table_name,
                exception=e,
                traceback_str=traceback.format_exc(),
            ))

    # ------------------------------------------------------------------
    # Write-back tests
    # ------------------------------------------------------------------

    def test_list_insertable_tables(self):
        """Test that list_insertable_tables returns a subset of list_tables."""
        try:
            insertable_tables = self.connector_test_utils.list_insertable_tables()
            all_tables = self.connector.list_tables()

            if not isinstance(insertable_tables, list):
                self._add_result(TestResult(
                    test_name="test_list_insertable_tables",
                    status=TestStatus.FAILED,
                    message=f"Expected list, got {type(insertable_tables).__name__}",
                    fix_hint="list_insertable_tables() must return a list[str].",
                ))
                return

            insertable_set = set(insertable_tables)
            all_tables_set = set(all_tables)

            if not insertable_set.issubset(all_tables_set):
                invalid = insertable_set - all_tables_set
                self._add_result(TestResult(
                    test_name="test_list_insertable_tables",
                    status=TestStatus.FAILED,
                    message=f"Insertable tables not subset of all tables: {invalid}",
                    fix_hint="Every table in list_insertable_tables() must also appear in list_tables().",
                ))
                return

            self._add_result(TestResult(
                test_name="test_list_insertable_tables",
                status=TestStatus.PASSED,
                message=f"Insertable tables ({len(insertable_tables)}) is subset of all tables ({len(all_tables)})",
            ))

        except Exception as e:
            self._add_result(TestResult(
                test_name="test_list_insertable_tables",
                status=TestStatus.ERROR,
                message=f"list_insertable_tables failed: {e}",
                exception=e,
                traceback_str=traceback.format_exc(),
            ))

    def test_list_deletable_tables(self):
        """Test that list_deletable_tables returns valid tables with cdc_with_deletes ingestion type."""
        if not hasattr(self.connector_test_utils, "list_deletable_tables"):
            self._add_result(TestResult(
                test_name="test_list_deletable_tables",
                status=TestStatus.PASSED,
                message="Skipped: test utils does not implement list_deletable_tables",
            ))
            return

        try:
            deletable_tables = self.connector_test_utils.list_deletable_tables()
            all_tables = self.connector.list_tables()

            if not isinstance(deletable_tables, list):
                self._add_result(TestResult(
                    test_name="test_list_deletable_tables",
                    status=TestStatus.FAILED,
                    message=f"Expected list, got {type(deletable_tables).__name__}",
                    fix_hint="list_deletable_tables() must return a list[str].",
                ))
                return

            if not deletable_tables:
                self._add_result(TestResult(
                    test_name="test_list_deletable_tables",
                    status=TestStatus.PASSED,
                    message="No deletable tables configured",
                ))
                return

            deletable_set = set(deletable_tables)
            all_tables_set = set(all_tables)

            if not deletable_set.issubset(all_tables_set):
                invalid = deletable_set - all_tables_set
                self._add_result(TestResult(
                    test_name="test_list_deletable_tables",
                    status=TestStatus.FAILED,
                    message=f"Deletable tables not subset of all tables: {invalid}",
                    fix_hint="Every table in list_deletable_tables() must also appear in list_tables().",
                ))
                return

            # Verify ingestion type
            invalid_tables = []
            for tbl in deletable_tables:
                try:
                    meta = self.connector.read_table_metadata(
                        tbl, self._get_table_options(tbl)
                    )
                    it = meta.get("ingestion_type")
                    if it != "cdc_with_deletes":
                        invalid_tables.append({"table": tbl, "ingestion_type": it})
                except Exception as e:
                    invalid_tables.append({"table": tbl, "error": str(e)})

            if invalid_tables:
                self._add_result(TestResult(
                    test_name="test_list_deletable_tables",
                    status=TestStatus.FAILED,
                    message="Deletable tables must have ingestion_type 'cdc_with_deletes'",
                    fix_hint="Change ingestion_type to 'cdc_with_deletes' for deletable tables or remove them from list_deletable_tables().",
                    details={"invalid_tables": invalid_tables},
                ))
                return

            self._add_result(TestResult(
                test_name="test_list_deletable_tables",
                status=TestStatus.PASSED,
                message=f"Deletable tables ({len(deletable_tables)}) validated successfully",
                details={"deletable_tables": deletable_tables},
            ))

        except Exception as e:
            self._add_result(TestResult(
                test_name="test_list_deletable_tables",
                status=TestStatus.ERROR,
                message=f"list_deletable_tables failed: {e}",
                exception=e,
                traceback_str=traceback.format_exc(),
            ))

    def test_write_to_source(self):  # pylint: disable=too-many-locals,too-many-branches
        """Test generate_rows_and_write on each insertable table."""
        try:
            insertable_tables = self.connector_test_utils.list_insertable_tables()
            if not insertable_tables:
                self._add_result(TestResult(
                    test_name="test_write_to_source",
                    status=TestStatus.FAILED,
                    message="No insertable tables available",
                    fix_hint="list_insertable_tables() returned empty. Add tables or skip write tests.",
                ))
                return
        except Exception:
            self._add_result(TestResult(
                test_name="test_write_to_source",
                status=TestStatus.FAILED,
                message="Could not get insertable tables",
            ))
            return

        test_row_count = 1

        for test_table in insertable_tables:
            test_name = f"test_write_to_source[{test_table}]"
            try:
                result = self.connector_test_utils.generate_rows_and_write(
                    test_table, test_row_count
                )

                if not isinstance(result, tuple) or len(result) != 3:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Expected 3-tuple, got {type(result).__name__}",
                        fix_hint="generate_rows_and_write() must return (bool, list[dict], dict).",
                        table_name=test_table,
                    ))
                    continue

                success, rows, column_names = result

                if not isinstance(success, bool) or not isinstance(rows, list) or not isinstance(column_names, dict):
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=(
                            f"Invalid return types: success={type(success).__name__}, "
                            f"rows={type(rows).__name__}, column_mapping={type(column_names).__name__}"
                        ),
                        fix_hint="generate_rows_and_write() must return (bool, list[dict], dict[str,str]).",
                        table_name=test_table,
                    ))
                    continue

                if not success:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Write was not successful (returned False)",
                        fix_hint="generate_rows_and_write() returned success=False. Check the write implementation.",
                        table_name=test_table,
                    ))
                    continue

                if len(rows) != test_row_count:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message=f"Expected {test_row_count} rows, got {len(rows)}",
                        fix_hint="generate_rows_and_write() should return exactly the number of rows requested.",
                        table_name=test_table,
                    ))
                    continue

                if not column_names:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Expected non-empty column_mapping when successful",
                        fix_hint="generate_rows_and_write() must return a non-empty column mapping dict.",
                        table_name=test_table,
                    ))
                    continue

                # Validate rows are dicts
                bad_row = False
                for i, row in enumerate(rows):
                    if not isinstance(row, dict):
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message=f"Row {i} is {type(row).__name__}, expected dict",
                            table_name=test_table,
                        ))
                        bad_row = True
                        break
                if bad_row:
                    continue

                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.PASSED,
                    message=f"Wrote {len(rows)} row(s) successfully",
                    table_name=test_table,
                    details={"rows": len(rows), "column_mappings": len(column_names)},
                ))

            except Exception as e:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.ERROR,
                    message=f"Write test error: {e}",
                    table_name=test_table,
                    exception=e,
                    traceback_str=traceback.format_exc(),
                ))

    def test_incremental_after_write(self):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """Test incremental ingestion after writing a row."""
        try:
            insertable_tables = self.connector_test_utils.list_insertable_tables()
            if not insertable_tables:
                self._add_result(TestResult(
                    test_name="test_incremental_after_write",
                    status=TestStatus.FAILED,
                    message="No insertable tables available",
                ))
                return
        except Exception:
            self._add_result(TestResult(
                test_name="test_incremental_after_write",
                status=TestStatus.FAILED,
                message="Could not get insertable tables",
            ))
            return

        for test_table in insertable_tables:
            test_name = f"test_incremental_after_write[{test_table}]"
            try:
                metadata = self.connector.read_table_metadata(
                    test_table, self._get_table_options(test_table)
                )

                # Get initial state
                initial_result = self.connector.read_table(
                    test_table, {}, self._get_table_options(test_table)
                )
                if not isinstance(initial_result, tuple) or len(initial_result) != 2:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Failed to get initial table state",
                        table_name=test_table,
                    ))
                    continue

                initial_iterator, initial_offset = initial_result
                initial_record_count = 0
                if metadata.get("ingestion_type") == "snapshot":
                    for _ in initial_iterator:
                        initial_record_count += 1

                # Write 1 row
                write_result = self.connector_test_utils.generate_rows_and_write(
                    test_table, 1
                )
                if not isinstance(write_result, tuple) or len(write_result) != 3:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Write failed: invalid return format",
                        table_name=test_table,
                    ))
                    continue

                write_success, written_rows, column_mapping = write_result
                if not write_success:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Write was not successful",
                        fix_hint="generate_rows_and_write() returned False. Fix write logic first.",
                        table_name=test_table,
                    ))
                    continue

                # Read after write
                after_result = self.connector.read_table(
                    test_table, initial_offset, self._get_table_options(test_table)
                )
                if not isinstance(after_result, tuple) or len(after_result) != 2:
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Read after write returned invalid format",
                        table_name=test_table,
                    ))
                    continue

                after_iterator, _ = after_result
                after_records = list(after_iterator)
                actual_count = len(after_records)
                ingestion_type = metadata.get("ingestion_type", "cdc")

                if ingestion_type in ("cdc", "cdc_with_deletes", "append"):
                    if actual_count < 1:
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message=f"Expected >= 1 record for {ingestion_type}, got {actual_count}",
                            fix_hint=(
                                "After writing 1 row, incremental read should return at least 1 new record. "
                                "Check that the offset logic correctly tracks new data."
                            ),
                            table_name=test_table,
                            details={"ingestion_type": ingestion_type, "actual_count": actual_count},
                        ))
                        continue
                else:
                    expected = initial_record_count + 1
                    if actual_count != expected:
                        self._add_result(TestResult(
                            test_name=test_name,
                            status=TestStatus.FAILED,
                            message=f"Expected {expected} records for snapshot, got {actual_count}",
                            fix_hint="For snapshot tables, full re-read should include the newly written row.",
                            table_name=test_table,
                            details={"initial_count": initial_record_count, "actual_count": actual_count},
                        ))
                        continue

                # Verify written row is present
                if not self._verify_written_rows_present(
                    written_rows, after_records, column_mapping
                ):
                    self._add_result(TestResult(
                        test_name=test_name,
                        status=TestStatus.FAILED,
                        message="Written row not found in returned results",
                        fix_hint=(
                            "The row written via generate_rows_and_write() was not found in the "
                            "subsequent read_table() results. Check column_mapping and offset logic."
                        ),
                        table_name=test_table,
                        details={
                            "written_rows": written_rows,
                            "returned_count": actual_count,
                            "column_mapping": column_mapping,
                        },
                    ))
                    continue

                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.PASSED,
                    message=f"Incremental read after write OK ({actual_count} records, {ingestion_type})",
                    table_name=test_table,
                ))

            except Exception as e:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.ERROR,
                    message=f"Incremental test error: {e}",
                    table_name=test_table,
                    exception=e,
                    traceback_str=traceback.format_exc(),
                ))

    def test_delete_and_read_deletes(self):  # pylint: disable=too-many-return-statements
        """Test delete functionality and verify deleted rows appear in read_table_deletes."""
        if not hasattr(self.connector_test_utils, "delete_rows"):
            self._add_result(TestResult(
                test_name="test_delete_and_read_deletes",
                status=TestStatus.PASSED,
                message="Skipped: test utils does not implement delete_rows",
            ))
            return

        if not hasattr(self.connector, "read_table_deletes"):
            self._add_result(TestResult(
                test_name="test_delete_and_read_deletes",
                status=TestStatus.PASSED,
                message="Skipped: connector does not implement read_table_deletes",
            ))
            return

        try:
            deletable_tables = self.connector_test_utils.list_deletable_tables()
            if not deletable_tables:
                self._add_result(TestResult(
                    test_name="test_delete_and_read_deletes",
                    status=TestStatus.PASSED,
                    message="Skipped: no deletable tables configured",
                ))
                return
        except Exception:
            self._add_result(TestResult(
                test_name="test_delete_and_read_deletes",
                status=TestStatus.FAILED,
                message="Could not get deletable tables",
            ))
            return

        test_table = deletable_tables[0]
        self._run_delete_test_for_table(test_table)

    def _run_delete_test_for_table(self, test_table: str):
        """Run delete test for a single table."""
        test_name = f"test_delete_and_read_deletes[{test_table}]"
        try:
            delete_result = self.connector_test_utils.delete_rows(test_table, 1)

            if not isinstance(delete_result, tuple) or len(delete_result) != 3:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message=f"delete_rows returned invalid format: {type(delete_result)}",
                    fix_hint="delete_rows() must return (bool, list[dict], dict[str,str]).",
                    table_name=test_table,
                ))
                return

            success, deleted_rows, column_mapping = delete_result

            if not success or not deleted_rows:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message="delete_rows failed or returned empty deleted_rows",
                    fix_hint="delete_rows() must return success=True and at least one deleted row dict.",
                    table_name=test_table,
                ))
                return

            read_result = self.connector.read_table_deletes(  # pylint: disable=no-member
                test_table, {}, self._get_table_options(test_table)
            )

            if not isinstance(read_result, tuple) or len(read_result) != 2:
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message=f"read_table_deletes returned invalid format: {type(read_result)}",
                    fix_hint="read_table_deletes() must return (iterator, offset_dict).",
                    table_name=test_table,
                ))
                return

            iterator, _ = read_result
            deleted_records = list(iterator)

            if not self._verify_written_rows_present(deleted_rows, deleted_records, column_mapping):
                self._add_result(TestResult(
                    test_name=test_name,
                    status=TestStatus.FAILED,
                    message="Deleted row not found in read_table_deletes results",
                    fix_hint=(
                        "The row deleted via delete_rows() was not found in read_table_deletes(). "
                        "Check column_mapping and that the source API surfaces deletes."
                    ),
                    table_name=test_table,
                    details={
                        "deleted_rows": deleted_rows,
                        "deleted_records_count": len(deleted_records),
                    },
                ))
                return

            self._add_result(TestResult(
                test_name=test_name,
                status=TestStatus.PASSED,
                message=f"Delete flow verified on '{test_table}'",
                table_name=test_table,
                details={
                    "deleted_rows": deleted_rows,
                    "deleted_records_count": len(deleted_records),
                },
            ))

        except Exception as e:
            self._add_result(TestResult(
                test_name=test_name,
                status=TestStatus.ERROR,
                message=f"Delete test error: {e}",
                table_name=test_table,
                exception=e,
                traceback_str=traceback.format_exc(),
            ))

    # ------------------------------------------------------------------
    # Helper: safely get tables (used by many tests)
    # ------------------------------------------------------------------

    def _get_tables_safe(self, caller_test_name: str) -> Optional[List[str]]:
        """Return the table list or emit a FAILED result and return None."""
        try:
            tables = self.connector.list_tables()
            if not tables:
                self._add_result(TestResult(
                    test_name=caller_test_name,
                    status=TestStatus.FAILED,
                    message="No tables available (list_tables returned empty)",
                    fix_hint="Fix list_tables() first — it must return at least one table name.",
                ))
                return None
            return tables
        except Exception:
            self._add_result(TestResult(
                test_name=caller_test_name,
                status=TestStatus.FAILED,
                message="Could not get tables (list_tables raised)",
                fix_hint="Fix list_tables() first.",
            ))
            return None

    def _filter_tables_by_ingestion_type(
        self, tables: List[str], ingestion_type: str
    ) -> List[str]:
        """Return only tables whose metadata has the given ingestion_type."""
        result = []
        for tbl in tables:
            try:
                meta = self.connector.read_table_metadata(
                    tbl, self._get_table_options(tbl)
                )
                if meta.get("ingestion_type") == ingestion_type:
                    result.append(tbl)
            except Exception:
                pass
        return result

    def _consume_iterator(self, iterator, max_records: int = None) -> List[dict]:
        """Consume up to ``max_records`` items from an iterator."""
        if max_records is None:
            max_records = self._sample_records
        records: List[dict] = []
        for record in iterator:
            records.append(record)
            if len(records) >= max_records:
                break
        return records

    # ------------------------------------------------------------------
    # Schema / record helpers
    # ------------------------------------------------------------------

    def _field_exists_in_schema(self, field_path: str, schema) -> bool:
        """Check if a field path exists in the schema (supports nested dot notation)."""
        if "." not in field_path:
            return field_path in schema.fieldNames()

        parts = field_path.split(".", 1)
        field_name, remaining = parts[0], parts[1]

        if field_name not in schema.fieldNames():
            return False

        field_type = schema[field_name].dataType
        if isinstance(field_type, StructType):
            return self._field_exists_in_schema(remaining, field_type)
        return False

    def _validate_primary_keys(self, primary_keys: list, schema) -> bool:
        return all(self._field_exists_in_schema(f, schema) for f in primary_keys)

    def _should_validate_cursor_field(self, metadata: dict) -> bool:
        ingestion_type = metadata.get("ingestion_type")
        return ingestion_type not in ("snapshot", "append")

    def _should_validate_primary_key(self, metadata: dict) -> bool:
        return metadata.get("ingestion_type") != "append"

    def _check_non_nullable_fields(
        self, record: dict, schema: StructType, prefix: str = ""
    ) -> List[str]:
        """Return field paths that are non-nullable but None in the record."""
        violations = []
        for f in schema.fields:
            path = f"{prefix}.{f.name}" if prefix else f.name
            value = record.get(f.name) if isinstance(record, dict) else None
            if not f.nullable and value is None:
                violations.append(path)
            if isinstance(f.dataType, StructType) and isinstance(value, dict):
                violations.extend(
                    self._check_non_nullable_fields(value, f.dataType, path)
                )
        return violations

    def _all_columns_null(
        self, record: dict, schema: StructType, prefix: str = ""
    ) -> bool:
        """Return True if every schema field is None in the record."""
        for f in schema.fields:
            value = record.get(f.name) if isinstance(record, dict) else None
            if value is not None and not isinstance(value, dict):
                return False
            if isinstance(f.dataType, StructType) and isinstance(value, dict):
                path = f"{prefix}.{f.name}" if prefix else f.name
                if not self._all_columns_null(value, f.dataType, path):
                    return False
        return True

    def _get_nested_value(self, record: Dict, path: str) -> Any:
        """Get a value from a nested dict using dot notation."""
        current = record
        for part in path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def _verify_written_rows_present(
        self,
        written_rows: List[Dict],
        returned_records: List[Dict],
        column_mapping: Dict[str, str],
    ) -> bool:
        """Verify that written rows appear in returned results using column_mapping."""
        if not written_rows or not column_mapping:
            return True

        written_signatures = []
        for row in written_rows:
            sig = {
                col: row.get(col)
                for col in column_mapping.keys()
                if col in row
            }
            print(f"\nwritten row: {sig}\n")
            written_signatures.append(sig)

        returned_signatures = []
        for record in returned_records:
            if isinstance(record, str):
                try:
                    record = json.loads(record)
                except Exception:
                    continue

            if isinstance(record, dict):
                sig = {}
                for written_col, returned_col in column_mapping.items():
                    value = self._get_nested_value(record, returned_col)
                    if value is not None:
                        sig[written_col] = value
                print(f"\nreturned row: {sig}\n")
                if sig:
                    returned_signatures.append(sig)

        for ws in written_signatures:
            if ws not in returned_signatures:
                return False
        return True

    # ------------------------------------------------------------------
    # Result management & report generation
    # ------------------------------------------------------------------

    def _add_result(self, result: TestResult):
        self.test_results.append(result)

    def _get_table_options(self, table_name: str) -> Dict[str, Any]:
        return self._table_configs.get(table_name, {})

    def _generate_report(self) -> TestReport:
        passed = sum(1 for r in self.test_results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in self.test_results if r.status == TestStatus.FAILED)
        errors = sum(1 for r in self.test_results if r.status == TestStatus.ERROR)

        return TestReport(
            connector_class_name="LakeflowConnect",
            test_results=self.test_results,
            total_tests=len(self.test_results),
            passed_tests=passed,
            failed_tests=failed,
            error_tests=errors,
            timestamp=datetime.now().isoformat(),  # pylint: disable=no-member
        )

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def print_report(self, report: TestReport, show_details: bool = True):
        print(f"\n{'=' * 60}")
        print("LAKEFLOW CONNECT TEST REPORT")
        print(f"{'=' * 60}")
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
            print(f"{'-' * 60}")

            status_symbol = {
                TestStatus.PASSED: "PASS",
                TestStatus.FAILED: "FAIL",
                TestStatus.ERROR: "ERR!",
            }

            for result in report.test_results:
                symbol = status_symbol[result.status]
                print(f"[{symbol}] {result.test_name}")
                print(f"       {result.message}")

                if result.fix_hint and result.status != TestStatus.PASSED:
                    print(f"       Fix: {result.fix_hint}")

                if result.details and result.status != TestStatus.PASSED:
                    print(
                        f"       Details: {json.dumps(result.details, indent=8, default=str)}"
                    )

                if result.traceback_str and result.status in (
                    TestStatus.ERROR,
                    TestStatus.FAILED,
                ):
                    print(f"       Traceback:\n{result.traceback_str}")

                print()

        # Print compact failure summary for LLM agents
        if report.failed_tests > 0 or report.error_tests > 0:
            print(f"{'=' * 60}")
            print("FAILURE SUMMARY (for automated agents)")
            print(f"{'=' * 60}")
            print(report.failure_summary())
            print()

            failed_tests = [
                r.test_name for r in report.test_results if r.status == TestStatus.FAILED
            ]
            error_tests = [
                r.test_name for r in report.test_results if r.status == TestStatus.ERROR
            ]
            parts = []
            if failed_tests:
                parts.append(f"Failed tests: {', '.join(failed_tests)}")
            if error_tests:
                parts.append(f"Error tests: {', '.join(error_tests)}")
            error_message = (
                f"Test suite failed with {report.failed_tests} failures "
                f"and {report.error_tests} errors. {' | '.join(parts)}"
            )
            raise TestFailedException(error_message, report)


# ---------------------------------------------------------------------------
# Pytest integration
# ---------------------------------------------------------------------------

def create_test_class(
    connector_class,
    config: dict,
    table_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    sample_records: int = 10,
    test_utils_class=None,
    setup_fn: Optional[Callable] = None,
) -> type:
    """Create a pytest test class with one test method per tester check.

    Each ``test_*`` method on ``LakeflowConnectTester`` becomes a separate
    pytest item, so ``pytest -k "test_read_table"`` re-runs only that check.

    Usage in a connector test file::

        # tests/unit/sources/my_source/test_my_source_lakeflow_connect.py
        from tests.unit.sources.test_suite import create_test_class
        from my_source import MyLakeflowConnect

        TestMySource = create_test_class(
            connector_class=MyLakeflowConnect,
            config=load_config(...),
        )

    Then run::

        pytest tests/unit/sources/my_source/ -v
        pytest tests/unit/sources/my_source/ -k "test_read_table"
        pytest tests/unit/sources/my_source/ -k "test_read_table or test_get_table_schema"

    Args:
        connector_class: The LakeflowConnect subclass to test.
        config: Init options dict (same as ``LakeflowConnectTester``).
        table_configs: Per-table options (default empty).
        sample_records: Max records to sample per table.
        test_utils_class: Optional ``LakeflowConnectTestUtils`` subclass for
            write-back tests.
        setup_fn: Optional callable invoked once before all tests (e.g. to
            reset a simulated API).
    """
    import pytest  # Local import so test_suite.py can be used outside pytest.

    class _TestClass:
        @classmethod
        def setup_class(cls):
            if setup_fn:
                setup_fn()

            # Inject into module namespace (backward compat with run_all_tests).
            import tests.unit.sources.test_suite as ts
            ts.LakeflowConnect = connector_class
            if test_utils_class:
                ts.LakeflowConnectTestUtils = test_utils_class

            cls._tester = LakeflowConnectTester(
                config,
                table_configs or {},
                sample_records,
            )
            # Initialize the connector once — every other test needs it.
            cls._tester.test_initialization()

        def _run_and_assert(self, method_name: str):
            """Run one tester method, then assert that no new failures appeared."""
            tester = self.__class__._tester
            if tester.connector is None:
                pytest.fail("Connector initialization failed — fix test_initialization first.")

            start_idx = len(tester.test_results)
            getattr(tester, method_name)()
            new_results = tester.test_results[start_idx:]

            failures = [r for r in new_results if r.status != TestStatus.PASSED]
            if failures:
                lines = []
                for f in failures:
                    line = f"[{f.status.value}] {f.test_name}: {f.message}"
                    if f.fix_hint:
                        line += f"\n  Fix: {f.fix_hint}"
                    lines.append(line)
                pytest.fail("\n\n".join(lines))

        # test_initialization is validated via the setup_class result.
        def test_initialization(self):
            results = [
                r for r in self.__class__._tester.test_results
                if r.test_name == "test_initialization"
            ]
            failures = [r for r in results if r.status != TestStatus.PASSED]
            if failures:
                msg = failures[0].message
                if failures[0].fix_hint:
                    msg += f"\n  Fix: {failures[0].fix_hint}"
                pytest.fail(msg)

    # Dynamically add a test method for each tester check (except init).
    for method_name in LakeflowConnectTester.ALL_TESTS:
        if method_name == "test_initialization":
            continue

        def _make_test(name):
            def test_method(self):
                self._run_and_assert(name)
            test_method.__name__ = name
            test_method.__qualname__ = f"_TestClass.{name}"
            return test_method

        setattr(_TestClass, method_name, _make_test(method_name))

    # Write-back tests — skip gracefully when no test utils are available.
    for method_name in LakeflowConnectTester.WRITE_BACK_TESTS:
        def _make_write_test(name):
            def test_method(self):
                tester = self.__class__._tester
                has_utils = (
                    hasattr(tester, "connector_test_utils")
                    and tester.connector_test_utils is not None
                )
                if not has_utils:
                    pytest.skip("No connector_test_utils available")
                self._run_and_assert(name)
            test_method.__name__ = name
            test_method.__qualname__ = f"_TestClass.{name}"
            return test_method

        setattr(_TestClass, method_name, _make_write_test(method_name))

    return _TestClass
