"""
Tests for the parse_value function in libs/utils.py

This module tests the conversion of JSON/dict values to PySpark-compatible data types.
"""

from datetime import datetime, date
from decimal import Decimal

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    MapType,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    BooleanType,
    DateType,
    TimestampType,
    BinaryType,
    DataType,
)

from libs.utils import parse_value


# =============================================================================
# Tests for None values
# =============================================================================
class TestNoneValue:  # pylint: disable=too-few-public-methods
    """Test handling of None values for various field types."""

    @pytest.mark.parametrize(
        "field_type",
        [
            StringType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            DecimalType(10, 2),
            BooleanType(),
            DateType(),
            TimestampType(),
        ],
    )
    def test_none_returns_none(self, field_type):
        """None value should return None for any field type."""
        assert parse_value(None, field_type) is None


# =============================================================================
# Tests for StringType
# =============================================================================
class TestStringType:
    """Test StringType conversion."""

    def test_string_to_string(self):
        assert parse_value("hello", StringType()) == "hello"

    def test_int_to_string(self):
        assert parse_value(123, StringType()) == "123"

    def test_float_to_string(self):
        assert parse_value(3.14, StringType()) == "3.14"

    def test_bool_to_string(self):
        assert parse_value(True, StringType()) == "True"

    def test_empty_string(self):
        assert parse_value("", StringType()) == ""


# =============================================================================
# Tests for IntegerType and LongType
# =============================================================================
class TestIntegerTypes:
    """Test IntegerType and LongType conversion."""

    @pytest.mark.parametrize("field_type", [IntegerType(), LongType()])
    def test_int_value(self, field_type):
        assert parse_value(42, field_type) == 42

    @pytest.mark.parametrize("field_type", [IntegerType(), LongType()])
    def test_string_to_int(self, field_type):
        assert parse_value("123", field_type) == 123

    @pytest.mark.parametrize("field_type", [IntegerType(), LongType()])
    def test_string_with_decimal_to_int(self, field_type):
        # "3.7" should become 3 (truncated)
        assert parse_value("3.7", field_type) == 3

    @pytest.mark.parametrize("field_type", [IntegerType(), LongType()])
    def test_float_to_int(self, field_type):
        assert parse_value(3.9, field_type) == 3

    @pytest.mark.parametrize("field_type", [IntegerType(), LongType()])
    def test_negative_int(self, field_type):
        assert parse_value(-100, field_type) == -100

    @pytest.mark.parametrize("field_type", [IntegerType(), LongType()])
    def test_invalid_string_raises_error(self, field_type):
        with pytest.raises(ValueError):
            parse_value("not_a_number", field_type)

    @pytest.mark.parametrize("field_type", [IntegerType(), LongType()])
    def test_empty_string_raises_error(self, field_type):
        with pytest.raises(ValueError):
            parse_value("", field_type)


# =============================================================================
# Tests for FloatType and DoubleType
# =============================================================================
class TestFloatingPointTypes:
    """Test FloatType and DoubleType conversion."""

    @pytest.mark.parametrize("field_type", [FloatType(), DoubleType()])
    def test_float_value(self, field_type):
        assert parse_value(3.14, field_type) == 3.14

    @pytest.mark.parametrize("field_type", [FloatType(), DoubleType()])
    def test_int_to_float(self, field_type):
        assert parse_value(42, field_type) == 42.0

    @pytest.mark.parametrize("field_type", [FloatType(), DoubleType()])
    def test_string_to_float(self, field_type):
        assert parse_value("3.14159", field_type) == 3.14159

    @pytest.mark.parametrize("field_type", [FloatType(), DoubleType()])
    def test_negative_float(self, field_type):
        assert parse_value(-2.5, field_type) == -2.5

    @pytest.mark.parametrize("field_type", [FloatType(), DoubleType()])
    def test_scientific_notation_string(self, field_type):
        assert parse_value("1.5e-3", field_type) == 0.0015


# =============================================================================
# Tests for DecimalType
# =============================================================================
class TestDecimalType:
    """Test DecimalType conversion."""

    def test_decimal_from_string(self):
        result = parse_value("123.45", DecimalType(10, 2))
        assert result == Decimal("123.45")

    def test_decimal_from_int(self):
        result = parse_value(100, DecimalType(10, 2))
        assert result == Decimal("100")

    def test_decimal_from_float(self):
        result = parse_value(3.14, DecimalType(10, 2))
        # Float to string conversion may introduce precision issues
        assert isinstance(result, Decimal)

    def test_decimal_negative(self):
        result = parse_value("-99.99", DecimalType(10, 2))
        assert result == Decimal("-99.99")


# =============================================================================
# Tests for BooleanType
# =============================================================================
class TestBooleanType:
    """Test BooleanType conversion."""

    @pytest.mark.parametrize(
        "value", [True, "true", "True", "TRUE", "t", "T", "yes", "Yes", "y", "Y", "1"]
    )
    def test_truthy_values(self, value):
        assert parse_value(value, BooleanType()) is True

    @pytest.mark.parametrize(
        "value", [False, "false", "False", "FALSE", "f", "F", "no", "No", "n", "N", "0"]
    )
    def test_falsy_values(self, value):
        assert parse_value(value, BooleanType()) is False

    def test_non_empty_string_is_truthy(self):
        # Non-standard strings are converted using bool()
        assert parse_value("random_string", BooleanType()) is True

    def test_empty_string_is_falsy(self):
        # Empty string bool() returns False
        assert parse_value("", BooleanType()) is False

    def test_zero_is_falsy(self):
        assert parse_value(0, BooleanType()) is False

    def test_nonzero_is_truthy(self):
        assert parse_value(1, BooleanType()) is True
        assert parse_value(42, BooleanType()) is True


# =============================================================================
# Tests for DateType
# =============================================================================
class TestDateType:
    """Test DateType conversion."""

    def test_iso_format(self):
        result = parse_value("2024-01-15", DateType())
        assert result == date(2024, 1, 15)

    def test_us_format(self):
        result = parse_value("01/15/2024", DateType())
        assert result == date(2024, 1, 15)

    def test_european_format(self):
        result = parse_value("15-01-2024", DateType())
        assert result == date(2024, 1, 15)

    def test_slash_format(self):
        result = parse_value("2024/01/15", DateType())
        assert result == date(2024, 1, 15)

    def test_datetime_to_date(self):
        dt = datetime(2024, 6, 20, 10, 30, 0)
        result = parse_value(dt, DateType())
        assert result == date(2024, 6, 20)

    def test_invalid_date_raises_error(self):
        with pytest.raises(ValueError):
            parse_value("not-a-date", DateType())


# =============================================================================
# Tests for TimestampType
# =============================================================================
class TestTimestampType:
    """Test TimestampType conversion."""

    def test_iso_format(self):
        result = parse_value("2024-01-15T10:30:00", TimestampType())
        assert result == datetime(2024, 1, 15, 10, 30, 0)

    def test_iso_format_with_z(self):
        result = parse_value("2024-01-15T10:30:00Z", TimestampType())
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_iso_format_with_timezone(self):
        result = parse_value("2024-01-15T10:30:00+05:00", TimestampType())
        assert result.year == 2024

    def test_space_separated_format(self):
        result = parse_value("2024-01-15 10:30:00", TimestampType())
        assert result == datetime(2024, 1, 15, 10, 30, 0)

    def test_unix_timestamp_int(self):
        # Unix timestamp for 2024-01-15 10:30:00 UTC (approximately)
        result = parse_value(1705315800, TimestampType())
        assert isinstance(result, datetime)

    def test_unix_timestamp_float(self):
        result = parse_value(1705315800.5, TimestampType())
        assert isinstance(result, datetime)

    def test_datetime_passthrough(self):
        dt = datetime(2024, 6, 20, 15, 45, 30)
        result = parse_value(dt, TimestampType())
        assert result == dt

    def test_invalid_timestamp_raises_error(self):
        with pytest.raises(ValueError):
            parse_value("not-a-timestamp", TimestampType())


# =============================================================================
# Tests for BinaryType
# =============================================================================
class TestBinaryType:
    """Test BinaryType conversion."""

    def test_bytes_passthrough(self):
        data = b"\x00\x01\x02\x03"
        result = parse_value(data, BinaryType())
        assert result == data

    def test_bytearray_to_bytes(self):
        data = bytearray([0, 1, 2, 3])
        result = parse_value(data, BinaryType())
        assert result == b"\x00\x01\x02\x03"
        assert isinstance(result, bytes)

    def test_base64_string_decoding(self):
        # "SGVsbG8gV29ybGQ=" is base64 for "Hello World"
        result = parse_value("SGVsbG8gV29ybGQ=", BinaryType())
        assert result == b"Hello World"

    def test_hex_string_decoding(self):
        # "48656c6c6f" is hex for "Hello"
        result = parse_value("48656c6c6f", BinaryType())
        assert result == b"Hello"

    def test_utf8_fallback_for_plain_string(self):
        # A string that's not valid base64 or hex should be UTF-8 encoded
        result = parse_value("plain text!", BinaryType())
        assert result == b"plain text!"

    def test_list_of_integers_to_bytes(self):
        data = [72, 101, 108, 108, 111]  # ASCII for "Hello"
        result = parse_value(data, BinaryType())
        assert result == b"Hello"

    def test_empty_bytes(self):
        result = parse_value(b"", BinaryType())
        assert result == b""

    def test_empty_list(self):
        result = parse_value([], BinaryType())
        assert result == b""

    def test_other_type_encoded_as_utf8(self):
        # Numbers and other types should be converted to string then UTF-8 encoded
        result = parse_value(12345, BinaryType())
        assert result == b"12345"

    def test_none_returns_none(self):
        result = parse_value(None, BinaryType())
        assert result is None


# =============================================================================
# Tests for ArrayType
# =============================================================================
class TestArrayType:
    """Test ArrayType conversion."""

    def test_list_of_strings(self):
        result = parse_value(["a", "b", "c"], ArrayType(StringType()))
        assert result == ["a", "b", "c"]

    def test_list_of_integers(self):
        result = parse_value([1, 2, 3], ArrayType(IntegerType()))
        assert result == [1, 2, 3]

    def test_list_with_string_conversion(self):
        result = parse_value(["1", "2", "3"], ArrayType(IntegerType()))
        assert result == [1, 2, 3]

    def test_empty_list(self):
        result = parse_value([], ArrayType(StringType()))
        assert result == []

    def test_single_value_to_array_with_contains_null(self):
        # When containsNull=True, a single value can be converted to array
        result = parse_value("single", ArrayType(StringType(), containsNull=True))
        assert result == ["single"]

    def test_single_value_to_array_without_contains_null_raises(self):
        with pytest.raises(ValueError):
            parse_value("single", ArrayType(StringType(), containsNull=False))

    def test_nested_array(self):
        result = parse_value([[1, 2], [3, 4]], ArrayType(ArrayType(IntegerType())))
        assert result == [[1, 2], [3, 4]]

    def test_array_with_null_elements(self):
        result = parse_value(["a", None, "c"], ArrayType(StringType(), containsNull=True))
        assert result == ["a", None, "c"]


# =============================================================================
# Tests for MapType
# =============================================================================
class TestMapType:
    """Test MapType conversion."""

    def test_string_to_string_map(self):
        result = parse_value(
            {"key1": "value1", "key2": "value2"}, MapType(StringType(), StringType())
        )
        assert result == {"key1": "value1", "key2": "value2"}

    def test_string_to_int_map(self):
        result = parse_value({"a": 1, "b": 2}, MapType(StringType(), IntegerType()))
        assert result == {"a": 1, "b": 2}

    def test_empty_map(self):
        result = parse_value({}, MapType(StringType(), StringType()))
        assert result == {}

    def test_map_with_type_conversion(self):
        result = parse_value({"count": "42"}, MapType(StringType(), IntegerType()))
        assert result == {"count": 42}

    def test_map_with_null_values(self):
        result = parse_value({"a": "value", "b": None}, MapType(StringType(), StringType()))
        assert result == {"a": "value", "b": None}

    def test_invalid_map_type_raises_error(self):
        with pytest.raises(ValueError):
            parse_value("not_a_dict", MapType(StringType(), StringType()))


# =============================================================================
# Tests for StructType
# =============================================================================
class TestStructType:
    """Test StructType conversion."""

    def test_simple_struct(self):
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        result = parse_value({"name": "Alice", "age": 30}, schema)
        assert isinstance(result, Row)
        assert result.name == "Alice"
        assert result.age == 30

    def test_struct_with_type_conversion(self):
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("active", BooleanType(), True),
            ]
        )
        result = parse_value({"id": "123", "active": "yes"}, schema)
        assert result.id == 123
        assert result.active is True

    def test_nested_struct(self):
        schema = StructType(
            [
                StructField(
                    "person",
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("age", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
                StructField("city", StringType(), True),
            ]
        )
        result = parse_value({"person": {"name": "Bob", "age": 25}, "city": "NYC"}, schema)
        assert result.person.name == "Bob"
        assert result.person.age == 25
        assert result.city == "NYC"

    def test_struct_with_array_field(self):
        schema = StructType(
            [
                StructField("tags", ArrayType(StringType()), True),
                StructField("total", IntegerType(), True),
            ]
        )
        result = parse_value({"tags": ["a", "b", "c"], "total": 3}, schema)
        assert result.tags == ["a", "b", "c"]
        assert result.total == 3

    def test_struct_with_map_field(self):
        schema = StructType(
            [
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )
        result = parse_value({"metadata": {"key": "value"}}, schema)
        assert result.metadata == {"key": "value"}

    def test_invalid_struct_type_raises_error(self):
        schema = StructType([StructField("name", StringType(), True)])
        with pytest.raises(ValueError, match="Expected a dictionary"):
            parse_value("not_a_dict", schema)

    def test_empty_dict_raises_error(self):
        schema = StructType([StructField("name", StringType(), True)])
        with pytest.raises(ValueError, match="cannot be an empty dict"):
            parse_value({}, schema)


# =============================================================================
# Tests for Missing Fields in StructType (Nullable behavior)
# =============================================================================
class TestMissingNullableFields:
    """Test that missing nullable fields are set to None."""

    def test_missing_nullable_field_is_none(self):
        schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField("age", IntegerType(), nullable=True),
                StructField("email", StringType(), nullable=True),
            ]
        )
        # Only provide 'name', missing 'age' and 'email'
        result = parse_value({"name": "Alice"}, schema)
        assert result.name == "Alice"
        assert result.age is None
        assert result.email is None

    def test_all_fields_missing_but_nullable(self):
        schema = StructType(
            [
                StructField("field1", StringType(), nullable=True),
                StructField("field2", IntegerType(), nullable=True),
                StructField("field3", BooleanType(), nullable=True),
            ]
        )
        # Provide at least one field to avoid empty dict error
        # The function requires at least one field to be present
        result = parse_value({"field1": "value"}, schema)
        assert result.field1 == "value"
        assert result.field2 is None
        assert result.field3 is None

    def test_missing_non_nullable_field_raises_error(self):
        schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField("id", IntegerType(), nullable=False),  # Not nullable!
            ]
        )
        with pytest.raises(ValueError, match="not nullable but not found"):
            parse_value({"name": "Alice"}, schema)

    def test_partial_fields_in_nested_struct(self):
        schema = StructType(
            [
                StructField(
                    "outer",
                    StructType(
                        [
                            StructField("inner1", StringType(), nullable=True),
                            StructField("inner2", IntegerType(), nullable=True),
                            StructField("inner3", BooleanType(), nullable=True),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )
        # Provide only inner1
        result = parse_value({"outer": {"inner1": "hello"}}, schema)
        assert result.outer.inner1 == "hello"
        assert result.outer.inner2 is None
        assert result.outer.inner3 is None

    def test_null_nested_struct(self):
        schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField(
                    "address",
                    StructType(
                        [
                            StructField("city", StringType(), nullable=True),
                            StructField("zip", StringType(), nullable=True),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )
        # address is None
        result = parse_value({"name": "Bob", "address": None}, schema)
        assert result.name == "Bob"
        assert result.address is None

    def test_missing_nested_struct_field_is_none(self):
        schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField(
                    "address",
                    StructType(
                        [
                            StructField("city", StringType(), nullable=True),
                            StructField("zip", StringType(), nullable=True),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )
        # address is not provided
        result = parse_value({"name": "Charlie"}, schema)
        assert result.name == "Charlie"
        assert result.address is None

    def test_mixed_nullable_and_non_nullable_fields(self):
        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("email", StringType(), nullable=True),
                StructField("created_at", TimestampType(), nullable=True),
            ]
        )
        # Only provide required field 'id'
        result = parse_value({"id": 42}, schema)
        assert result.id == 42
        assert result.name is None
        assert result.email is None
        assert result.created_at is None

    def test_array_of_structs_with_missing_fields(self):
        inner_schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField("value", IntegerType(), nullable=True),
            ]
        )
        schema = ArrayType(inner_schema)

        data = [
            {"name": "first"},  # missing 'value'
            {"value": 100},  # missing 'name'
            {"name": "third", "value": 300},  # complete
        ]
        result = parse_value(data, schema)

        assert len(result) == 3
        assert result[0].name == "first"
        assert result[0].value is None
        assert result[1].name is None
        assert result[1].value == 100
        assert result[2].name == "third"
        assert result[2].value == 300


# =============================================================================
# Tests for Complex Nested Structures
# =============================================================================
class TestComplexNestedStructures:
    """Test complex combinations of nested types."""

    def test_deeply_nested_structure(self):
        schema = StructType(
            [
                StructField(
                    "level1",
                    StructType(
                        [
                            StructField(
                                "level2",
                                StructType(
                                    [
                                        StructField("level3", StringType(), nullable=True),
                                    ]
                                ),
                                nullable=True,
                            ),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )
        result = parse_value({"level1": {"level2": {"level3": "deep"}}}, schema)
        assert result.level1.level2.level3 == "deep"

    def test_array_of_maps(self):
        schema = ArrayType(MapType(StringType(), IntegerType()))
        data = [{"a": 1, "b": 2}, {"c": 3}]
        result = parse_value(data, schema)
        assert result == [{"a": 1, "b": 2}, {"c": 3}]

    def test_map_of_arrays(self):
        schema = MapType(StringType(), ArrayType(IntegerType()))
        data = {"nums": [1, 2, 3], "more_nums": [4, 5]}
        result = parse_value(data, schema)
        assert result == {"nums": [1, 2, 3], "more_nums": [4, 5]}

    def test_struct_with_all_types(self):
        schema = StructType(
            [
                StructField("string_field", StringType(), nullable=True),
                StructField("int_field", IntegerType(), nullable=True),
                StructField("long_field", LongType(), nullable=True),
                StructField("float_field", FloatType(), nullable=True),
                StructField("double_field", DoubleType(), nullable=True),
                StructField("decimal_field", DecimalType(10, 2), nullable=True),
                StructField("bool_field", BooleanType(), nullable=True),
                StructField("date_field", DateType(), nullable=True),
                StructField("timestamp_field", TimestampType(), nullable=True),
                StructField("array_field", ArrayType(StringType()), nullable=True),
                StructField("map_field", MapType(StringType(), IntegerType()), nullable=True),
            ]
        )
        data = {
            "string_field": "hello",
            "int_field": 42,
            "long_field": 9999999999,
            "float_field": 3.14,
            "double_field": 2.718281828,
            "decimal_field": "123.45",
            "bool_field": True,
            "date_field": "2024-01-15",
            "timestamp_field": "2024-01-15T10:30:00",
            "array_field": ["a", "b", "c"],
            "map_field": {"x": 1, "y": 2},
        }
        result = parse_value(data, schema)

        assert result.string_field == "hello"
        assert result.int_field == 42
        assert result.long_field == 9999999999
        assert result.float_field == 3.14
        assert result.double_field == 2.718281828
        assert result.decimal_field == Decimal("123.45")
        assert result.bool_field is True
        assert result.date_field == date(2024, 1, 15)
        assert result.timestamp_field == datetime(2024, 1, 15, 10, 30, 0)
        assert result.array_field == ["a", "b", "c"]
        assert result.map_field == {"x": 1, "y": 2}

    def test_struct_with_all_types_missing_optional_fields(self):
        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("string_field", StringType(), nullable=True),
                StructField("int_field", IntegerType(), nullable=True),
                StructField("array_field", ArrayType(StringType()), nullable=True),
                StructField("map_field", MapType(StringType(), IntegerType()), nullable=True),
                StructField(
                    "nested",
                    StructType(
                        [
                            StructField("inner", StringType(), nullable=True),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )
        # Only provide required field
        result = parse_value({"id": 1}, schema)

        assert result.id == 1
        assert result.string_field is None
        assert result.int_field is None
        assert result.array_field is None
        assert result.map_field is None
        assert result.nested is None


# =============================================================================
# Tests for Error Cases
# =============================================================================
class TestErrorCases:
    """Test error handling for invalid inputs."""

    def test_unsupported_field_type(self):
        # Create a custom DataType that's not supported
        class CustomType(DataType):
            pass

        with pytest.raises((ValueError, TypeError)):
            parse_value("value", CustomType())

    def test_struct_with_non_dict_value(self):
        schema = StructType([StructField("name", StringType(), True)])
        with pytest.raises(ValueError, match="Expected a dictionary"):
            parse_value([1, 2, 3], schema)

    def test_map_with_non_dict_value(self):
        schema = MapType(StringType(), StringType())
        with pytest.raises(ValueError, match="Expected a dictionary"):
            parse_value([1, 2, 3], schema)

    def test_invalid_integer_conversion(self):
        with pytest.raises(ValueError):
            parse_value("abc", IntegerType())

    def test_invalid_date_format(self):
        with pytest.raises(ValueError):
            parse_value("invalid-date", DateType())

    def test_invalid_timestamp_format(self):
        with pytest.raises(ValueError):
            parse_value("invalid-timestamp", TimestampType())
