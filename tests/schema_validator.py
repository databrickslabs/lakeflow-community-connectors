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
from typing import Dict, Any, List, Set
from pyspark.sql.types import StructType, MapType, ArrayType, StructField


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
    current_schema = schema

    for i, part in enumerate(parts):
        # Find the field in current schema level
        if isinstance(current_schema, StructType):
            field = next((f for f in current_schema.fields if f.name == part), None)
            if not field:
                return False
            current_schema = field.dataType

            # If this is a MapType, any subsequent parts are dynamic keys
            if isinstance(current_schema, MapType):
                if i < len(parts) - 1:  # There are more parts
                    # This is a dynamic map key, check if subsequent parts match value type
                    remaining_parts = parts[i + 2 :]  # Skip the dynamic key
                    if remaining_parts:
                        return check_nested_fields(remaining_parts, current_schema.valueType)
                    return True
                return True

            # If this is the last part and we found it, it's covered
            if i == len(parts) - 1:
                return True

        elif isinstance(current_schema, MapType):
            # Dynamic key in a map
            if i < len(parts) - 1:
                # Check if remaining parts match the value type
                remaining_parts = parts[i + 1 :]
                return check_nested_fields(remaining_parts, current_schema.valueType)
            return True

        elif isinstance(current_schema, ArrayType):
            # Arrays - check element type
            current_schema = current_schema.elementType
            continue

        else:
            return False

    return True


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

        # Get documented schema
        documented_schema = self.connector.get_table_schema(table_name, table_options)
        documented_fields = {field.name for field in documented_schema.fields}

        print(f"\nDocumented fields ({len(documented_fields)}):")
        for field_name in sorted(documented_fields):
            field = next(f for f in documented_schema.fields if f.name == field_name)
            print(f"  - {field_name} ({field.dataType.simpleString()})")

        # Get actual data
        print(f"\nFetching actual data from API...")
        try:
            data_iter, offset = self.connector.read_table(
                table_name, {}, table_options
            )
            records = list(data_iter)
            print(f"Retrieved {len(records)} records")

            if not records:
                print("⚠️  WARNING: No records returned, cannot validate schema")
                return {
                    "table": table_name,
                    "status": "no_data",
                    "message": "No records available for validation",
                }

            # Analyze actual fields from first few records
            all_actual_fields = set()
            for record in records[:5]:  # Check first 5 records
                record_fields = get_field_names(record)
                all_actual_fields.update(record_fields)

            print(f"\nActual fields from API ({len(all_actual_fields)}):")
            for field_name in sorted(all_actual_fields):
                print(f"  - {field_name}")

            # Compare schemas
            print(f"\n{'=' * 60}")
            print("SCHEMA COMPARISON")
            print(f"{'=' * 60}")

            # Filter out nested fields that are covered by MapType/StructType
            raw_missing_in_documented = all_actual_fields - documented_fields
            truly_missing_in_documented = filter_covered_fields(
                raw_missing_in_documented, documented_schema
            )

            missing_in_actual = documented_fields - all_actual_fields
            matching_fields = documented_fields & all_actual_fields

            # Count nested fields that are covered
            covered_nested_fields = (
                raw_missing_in_documented - truly_missing_in_documented
            )

            print(f"\n✅ Matching fields ({len(matching_fields)}):")
            for field in sorted(matching_fields):
                print(f"  - {field}")

            if covered_nested_fields:
                print(
                    f"\n✅ Nested fields covered by MapType/StructType ({len(covered_nested_fields)}):"
                )
                print(f"   (These are expected - dynamic map keys or nested struct fields)")
                # Show a few examples
                examples = sorted(covered_nested_fields)[:5]
                for field in examples:
                    print(f"  - {field}")
                if len(covered_nested_fields) > 5:
                    print(f"  ... and {len(covered_nested_fields) - 5} more")

            if truly_missing_in_documented:
                print(
                    f"\n⚠️  Fields in API but NOT in documented schema ({len(truly_missing_in_documented)}):"
                )
                for field in sorted(truly_missing_in_documented):
                    # Show sample value
                    sample_value = None
                    for record in records[:3]:
                        # Navigate nested path
                        try:
                            parts = field.split(".")
                            val = record
                            for part in parts:
                                val = val.get(part) if isinstance(val, dict) else None
                                if val is None:
                                    break
                            sample_value = val
                        except:
                            pass
                    print(f"  - {field} (sample: {repr(sample_value)})")

            if missing_in_actual:
                print(
                    f"\n⚠️  Fields in documented schema but NOT in API response ({len(missing_in_actual)}):"
                )
                for field in sorted(missing_in_actual):
                    print(f"  - {field}")

            # Show sample record
            print(f"\n{'=' * 60}")
            print("SAMPLE RECORD (first record)")
            print(f"{'=' * 60}")
            print(json.dumps(records[0], indent=2, default=str))

            return {
                "table": table_name,
                "status": "success",
                "documented_fields": list(documented_fields),
                "actual_fields": list(all_actual_fields),
                "matching_fields": list(matching_fields),
                "covered_nested_fields": list(covered_nested_fields),
                "truly_missing_in_documented": list(truly_missing_in_documented),
                "missing_in_actual": list(missing_in_actual),
                "sample_record": records[0],
            }

        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback

            traceback.print_exc()
            return {
                "table": table_name,
                "status": "error",
                "message": str(e),
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

    def print_summary(self, results: Dict[str, Any]):
        """Print a summary of validation results."""
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)

        total_discrepancies = 0
        total_covered_nested = 0

        for table_name, result in results.items():
            if result.get("status") != "success":
                print(f"\n{table_name}: ⚠️  {result.get('status', 'unknown').upper()}")
                if "message" in result:
                    print(f"  {result['message']}")
                continue

            truly_missing = len(result.get("truly_missing_in_documented", []))
            missing_in_actual = len(result.get("missing_in_actual", []))
            covered_nested = len(result.get("covered_nested_fields", []))

            discrepancies = truly_missing + missing_in_actual
            total_discrepancies += discrepancies
            total_covered_nested += covered_nested

            if discrepancies == 0:
                status = "✅ PERFECT MATCH"
            else:
                status = f"⚠️  {discrepancies} DISCREPANCIES"

            print(f"\n{table_name}: {status}")
            if truly_missing:
                print(f"  - {truly_missing} fields in API but not documented")
            if missing_in_actual:
                print(f"  - {missing_in_actual} documented fields not in API")
            if covered_nested:
                print(
                    f"  ✅ {covered_nested} nested fields correctly covered by MapType/StructType"
                )

        print(f"\n{'=' * 80}")
        if total_discrepancies == 0:
            print("✅ ALL SCHEMAS MATCH PERFECTLY!")
            if total_covered_nested > 0:
                print(
                    f"   {total_covered_nested} nested fields correctly covered by MapType/StructType schemas"
                )
        else:
            print(f"⚠️  TOTAL DISCREPANCIES: {total_discrepancies}")
            if total_covered_nested > 0:
                print(
                    f"✅ {total_covered_nested} nested fields correctly covered by MapType/StructType"
                )
            print("Please review the detailed output above and update schemas accordingly.")
        print(f"{'=' * 80}\n")

        return total_discrepancies

    def save_results(self, results: Dict[str, Any], output_file):
        """Save validation results to JSON file."""
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Detailed results saved to: {output_file}")

    def has_no_discrepancies(self, results: Dict[str, Any]) -> bool:
        """Check if validation found any discrepancies."""
        for result in results.values():
            if result.get("status") != "success":
                return False
            if result.get("truly_missing_in_documented"):
                return False
            if result.get("missing_in_actual"):
                return False
        return True
