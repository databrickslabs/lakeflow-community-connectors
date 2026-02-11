"""
Reusable schema validation utility for Lakeflow Community Connectors.

This module provides intelligent schema validation that understands:
- Nested MapType fields (dynamic keys)
- Nested StructType fields (fixed structure)
- Distinguishes between truly missing fields and expected nested fields

Usage:
    from tests.schema_validator import SchemaValidator

    validator = SchemaValidator(connector)
    results = validator.validate_all_tables(table_configs)
    validator.print_report(results)
"""

import json
import traceback
from typing import Dict, Any, List, Set, Tuple
from pyspark.sql.types import StructType, MapType, ArrayType


def get_field_names(record: dict, prefix: str = "") -> Set[str]:
    """Recursively get all field names from a record."""
    fields = set()
    for key, value in record.items():
        field_path = f"{prefix}.{key}" if prefix else key
        fields.add(field_path)
        if isinstance(value, dict):
            fields.update(get_field_names(value, field_path))
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            fields.update(get_field_names(value[0], field_path))
    return fields


def is_field_covered_by_schema(field_path: str, schema: StructType) -> bool:
    """
    Check if a nested field path is covered by MapType or StructType in schema.

    Examples:
    - "values.QID3.choiceId" is covered by MapType(StringType, StructType([choiceId, ...]))
    - "stats.sent" is covered by StructType([StructField("sent", ...)])
    """
    parts = field_path.split(".")
    return _check_field_parts(parts, schema, 0)


# pylint: disable=too-many-return-statements
def _check_field_parts(parts: List[str], current_schema, index: int) -> bool:
    """Helper to recursively check field parts against schema."""
    if index >= len(parts):
        return True

    part = parts[index]
    is_last = index == len(parts) - 1

    if isinstance(current_schema, StructType):
        field = next((f for f in current_schema.fields if f.name == part), None)
        if not field:
            return False

        if isinstance(field.dataType, MapType) and not is_last:
            # MapType with more parts - check remaining against value type
            remaining_parts = parts[index + 2:]  # Skip dynamic key
            if remaining_parts:
                return check_nested_fields(remaining_parts, field.dataType.valueType)
            return True

        if is_last:
            return True

        return _check_field_parts(parts, field.dataType, index + 1)

    if isinstance(current_schema, MapType):
        # Dynamic key in map - check remaining parts against value type
        if not is_last:
            remaining_parts = parts[index + 1:]
            return check_nested_fields(remaining_parts, current_schema.valueType)
        return True

    if isinstance(current_schema, ArrayType):
        return _check_field_parts(parts, current_schema.elementType, index)

    return False


def check_nested_fields(parts: List[str], schema) -> bool:
    """Helper to check if nested parts exist in a schema type."""
    if not parts:
        return True

    if isinstance(schema, StructType):
        field = next((f for f in schema.fields if f.name == parts[0]), None)
        if field:
            if len(parts) == 1:
                return True
            return check_nested_fields(parts[1:], field.dataType)
    elif isinstance(schema, MapType):
        # Any key is valid in a map, check value type for remaining parts
        if len(parts) > 1:
            return check_nested_fields(parts[1:], schema.valueType)
        return True

    return False


def filter_covered_fields(field_paths: Set[str], schema: StructType) -> Set[str]:
    """
    Filter out field paths that are expected nested fields covered by MapType/StructType.
    Returns only the truly missing fields.
    """
    truly_missing = set()
    for field_path in field_paths:
        if not is_field_covered_by_schema(field_path, schema):
            truly_missing.add(field_path)
    return truly_missing


class SchemaValidator:
    """Validates connector schemas against actual API responses."""

    def __init__(self, connector):
        """
        Initialize validator with a connector instance.

        Args:
            connector: Instance of LakeflowConnect implementation
        """
        self.connector = connector

    def validate_table(
        self, table_name: str, table_options: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Validate a single table's schema against actual API response.

        Args:
            table_name: Name of the table to validate
            table_options: Optional table-specific options

        Returns:
            Dictionary with validation results
        """
        if table_options is None:
            table_options = {}

        print(f"\n{'=' * 60}")
        print(f"VALIDATING TABLE: {table_name}")
        print(f"{'=' * 60}")

        # Get documented schema and fields
        documented_schema, documented_fields = self._get_documented_schema(
            table_name, table_options
        )

        # Fetch actual data
        result = self._fetch_and_analyze_data(
            table_name, table_options, documented_schema, documented_fields
        )
        return result

    def _get_documented_schema(
        self, table_name: str, table_options: Dict[str, Any]
    ) -> Tuple[StructType, Set[str]]:
        """Get documented schema and extract field names."""
        documented_schema = self.connector.get_table_schema(table_name, table_options)
        documented_fields = {field.name for field in documented_schema.fields}

        print(f"\nDocumented fields ({len(documented_fields)}):")
        for field_name in sorted(documented_fields):
            field = next(f for f in documented_schema.fields if f.name == field_name)
            print(f"  - {field_name} ({field.dataType.simpleString()})")

        return documented_schema, documented_fields

    def _fetch_and_analyze_data(
        self, table_name: str, table_options: Dict[str, Any],
        documented_schema: StructType, documented_fields: Set[str]
    ) -> Dict[str, Any]:
        """Fetch data from API and analyze fields."""
        print(f"\nFetching actual data from API...")
        try:
            data_iter, _ = self.connector.read_table(table_name, {}, table_options)
            records = list(data_iter)
            print(f"Retrieved {len(records)} records")

            if not records:
                return self._no_data_result(table_name)

            # Analyze fields
            all_actual_fields = self._collect_actual_fields(records)

            # Compare schemas
            comparison = self._compare_schemas(
                documented_fields, all_actual_fields, documented_schema
            )

            # Print comparison
            self._print_comparison(comparison, records)

            return self._build_success_result(
                table_name, documented_fields, all_actual_fields,
                comparison, records
            )

        except Exception as e:
            return self._error_result(table_name, e)

    def _collect_actual_fields(self, records: List[Dict]) -> Set[str]:
        """Collect all actual field names from records."""
        all_actual_fields = set()
        for record in records[:5]:  # Check first 5 records
            record_fields = get_field_names(record)
            all_actual_fields.update(record_fields)

        print(f"\nActual fields from API ({len(all_actual_fields)}):")
        for field_name in sorted(all_actual_fields):
            print(f"  - {field_name}")

        return all_actual_fields

    def _compare_schemas(
        self, documented_fields: Set[str], all_actual_fields: Set[str],
        documented_schema: StructType
    ) -> Dict[str, Any]:
        """Compare documented and actual schemas."""
        raw_missing_in_documented = all_actual_fields - documented_fields
        truly_missing_in_documented = filter_covered_fields(
            raw_missing_in_documented, documented_schema
        )

        missing_in_actual = documented_fields - all_actual_fields
        matching_fields = documented_fields & all_actual_fields
        covered_nested_fields = raw_missing_in_documented - truly_missing_in_documented

        return {
            "matching_fields": matching_fields,
            "covered_nested_fields": covered_nested_fields,
            "truly_missing_in_documented": truly_missing_in_documented,
            "missing_in_actual": missing_in_actual,
        }

    def _print_comparison(self, comparison: Dict[str, Any], records: List[Dict]):
        """Print schema comparison results."""
        print(f"\n{'=' * 60}")
        print("SCHEMA COMPARISON")
        print(f"{'=' * 60}")

        matching = comparison["matching_fields"]
        covered = comparison["covered_nested_fields"]
        truly_missing = comparison["truly_missing_in_documented"]
        missing_actual = comparison["missing_in_actual"]

        print(f"\n✅ Matching fields ({len(matching)}):")
        for field in sorted(matching):
            print(f"  - {field}")

        if covered:
            self._print_covered_nested_fields(covered)

        if truly_missing:
            self._print_missing_in_documented(truly_missing, records)

        if missing_actual:
            print(
                f"\n⚠️  Fields in documented schema but NOT in "
                f"API response ({len(missing_actual)}):"
            )
            for field in sorted(missing_actual):
                print(f"  - {field}")

        # Show sample record
        print(f"\n{'=' * 60}")
        print("SAMPLE RECORD (first record)")
        print(f"{'=' * 60}")
        print(json.dumps(records[0], indent=2, default=str))

    def _print_covered_nested_fields(self, covered_nested_fields: Set[str]):
        """Print covered nested fields."""
        print(f"\n✅ Nested fields covered by MapType/StructType ({len(covered_nested_fields)}):")
        print(f"   (These are expected - dynamic map keys or nested struct fields)")
        examples = sorted(covered_nested_fields)[:5]
        for field in examples:
            print(f"  - {field}")
        if len(covered_nested_fields) > 5:
            print(f"  ... and {len(covered_nested_fields) - 5} more")

    def _print_missing_in_documented(self, truly_missing: Set[str], records: List[Dict]):
        """Print fields missing in documented schema."""
        print(f"\n⚠️  Fields in API but NOT in documented schema ({len(truly_missing)}):")
        for field in sorted(truly_missing):
            sample_value = self._get_sample_value(field, records)
            print(f"  - {field} (sample: {repr(sample_value)})")

    def _get_sample_value(self, field: str, records: List[Dict]) -> Any:
        """Get sample value for a field from records."""
        for record in records[:3]:
            try:
                parts = field.split(".")
                val = record
                for part in parts:
                    val = val.get(part) if isinstance(val, dict) else None
                    if val is None:
                        break
                if val is not None:
                    return val
            except (AttributeError, TypeError, KeyError):
                pass
        return None

    @staticmethod
    def _no_data_result(table_name: str) -> Dict[str, Any]:
        """Return result for tables with no data."""
        print("⚠️  WARNING: No records returned, cannot validate schema")
        return {
            "table": table_name,
            "status": "no_data",
            "message": "No records available for validation",
        }

    @staticmethod
    def _error_result(table_name: str, error: Exception) -> Dict[str, Any]:
        """Return result for validation errors."""
        print(f"\n❌ ERROR: {error}")
        traceback.print_exc()
        return {
            "table": table_name,
            "status": "error",
            "message": str(error),
        }

    @staticmethod
    def _build_success_result(
        table_name: str, documented_fields: Set[str], all_actual_fields: Set[str],
        comparison: Dict[str, Any], records: List[Dict]
    ) -> Dict[str, Any]:
        """Build success result dictionary."""
        return {
            "table": table_name,
            "status": "success",
            "documented_fields": list(documented_fields),
            "actual_fields": list(all_actual_fields),
            "matching_fields": list(comparison["matching_fields"]),
            "covered_nested_fields": list(comparison["covered_nested_fields"]),
            "truly_missing_in_documented": list(comparison["truly_missing_in_documented"]),
            "missing_in_actual": list(comparison["missing_in_actual"]),
            "sample_record": records[0],
        }

    def validate_all_tables(
        self, table_configs: Dict[str, Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Validate all tables in the connector.

        Args:
            table_configs: Dictionary mapping table names to their options

        Returns:
            Dictionary with validation results for all tables
        """
        if table_configs is None:
            table_configs = {}

        print("\n" + "=" * 80)
        print("STARTING SCHEMA VALIDATION")
        print("=" * 80)

        results = {}
        tables = self.connector.list_tables()

        for table_name in tables:
            table_options = table_configs.get(table_name, {})
            results[table_name] = self.validate_table(table_name, table_options)

        return results

    def print_summary(self, results: Dict[str, Any]) -> int:
        """
        Print a summary of validation results.

        Returns:
            Total number of discrepancies found
        """
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)

        total_discrepancies = 0
        total_covered_nested = 0

        for table_name, result in results.items():
            if result.get("status") != "success":
                self._print_non_success_summary(table_name, result)
                continue

            discrepancies, covered_nested = self._print_table_summary(table_name, result)
            total_discrepancies += discrepancies
            total_covered_nested += covered_nested

        self._print_final_summary(total_discrepancies, total_covered_nested)
        return total_discrepancies

    @staticmethod
    def _print_non_success_summary(table_name: str, result: Dict[str, Any]):
        """Print summary for non-success validation results."""
        print(f"\n{table_name}: ⚠️  {result.get('status', 'unknown').upper()}")
        if "message" in result:
            print(f"  {result['message']}")

    @staticmethod
    def _print_table_summary(table_name: str, result: Dict[str, Any]) -> Tuple[int, int]:
        """Print summary for a single table."""
        truly_missing = len(result.get("truly_missing_in_documented", []))
        missing_in_actual = len(result.get("missing_in_actual", []))
        covered_nested = len(result.get("covered_nested_fields", []))

        discrepancies = truly_missing + missing_in_actual
        status = "✅ PERFECT MATCH" if discrepancies == 0 else f"⚠️  {discrepancies} DISCREPANCIES"

        print(f"\n{table_name}: {status}")
        if truly_missing:
            print(f"  - {truly_missing} fields in API but not documented")
        if missing_in_actual:
            print(f"  - {missing_in_actual} documented fields not in API")
        if covered_nested:
            print(f"  ✅ {covered_nested} nested fields correctly covered by MapType/StructType")

        return discrepancies, covered_nested

    @staticmethod
    def _print_final_summary(total_discrepancies: int, total_covered_nested: int):
        """Print final validation summary."""
        print(f"\n{'=' * 80}")
        if total_discrepancies == 0:
            print("✅ ALL SCHEMAS MATCH PERFECTLY!")
            if total_covered_nested > 0:
                print(
                    f"   {total_covered_nested} nested fields correctly "
                    "covered by MapType/StructType schemas"
                )
        else:
            print(f"⚠️  TOTAL DISCREPANCIES: {total_discrepancies}")
            if total_covered_nested > 0:
                print(
                    f"✅ {total_covered_nested} nested fields correctly "
                    "covered by MapType/StructType"
                )
            print("Please review the detailed output above and update schemas accordingly.")
        print(f"{'=' * 80}\n")

    def save_results(self, results: Dict[str, Any], output_file: str):
        """Save validation results to JSON file."""
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Detailed results saved to: {output_file}")

    @staticmethod
    def has_no_discrepancies(results: Dict[str, Any]) -> bool:
        """Check if validation found any discrepancies."""
        for result in results.values():
            if result.get("status") != "success":
                return False
            if result.get("truly_missing_in_documented"):
                return False
            if result.get("missing_in_actual"):
                return False
        return True
