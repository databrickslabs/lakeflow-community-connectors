from pyspark.sql import Row
from pyspark.sql.types import *
from decimal import Decimal
from datetime import datetime
from typing import Any


def parse_value(value: Any, field_type: DataType) -> Any:
    """
    Converts a JSON value into a PySpark-compatible data type based on the provided field type.
    """
    if value is None:
        return None
    # Handle complex types
    if isinstance(field_type, StructType):
        # Validate input for StructType
        if not isinstance(value, dict):
            raise ValueError(f"Expected a dictionary for StructType, got {type(value)}")
        # Spark Python -> Arrow conversion require missing StructType fields to be assigned None.
        if value == {}:
            raise ValueError(
                f"field in StructType cannot be an empty dict. Please assign None as the default value instead."
            )
        # For StructType, recursively parse fields into a Row
        field_dict = {}
        for field in field_type.fields:
            # When a field does not exist in the input:
            # 1. set it to None when schema marks it as nullable
            # 2. Otherwise, raise an error.
            if field.name in value:
                field_dict[field.name] = parse_value(
                    value.get(field.name), field.dataType
                )
            elif field.nullable:
                field_dict[field.name] = None
            else:
                raise ValueError(
                    f"Field {field.name} is not nullable but not found in the input"
                )

        return Row(**field_dict)
    elif isinstance(field_type, ArrayType):
        # For ArrayType, parse each element in the array
        if not isinstance(value, list):
            # Handle edge case: single value that should be an array
            if field_type.containsNull:
                # Try to convert to a single-element array if nulls are allowed
                return [parse_value(value, field_type.elementType)]
            else:
                raise ValueError(f"Expected a list for ArrayType, got {type(value)}")
        return [parse_value(v, field_type.elementType) for v in value]
    elif isinstance(field_type, MapType):
        # Handle MapType - new support
        if not isinstance(value, dict):
            raise ValueError(f"Expected a dictionary for MapType, got {type(value)}")
        return {
            parse_value(k, field_type.keyType): parse_value(v, field_type.valueType)
            for k, v in value.items()
        }
    # Handle primitive types with more robust error handling and type conversion
    try:
        if isinstance(field_type, StringType):
            # Don't convert None to "None" string
            return str(value) if value is not None else None
        elif isinstance(field_type, (IntegerType, LongType)):
            # Convert numeric strings and floats to integers
            if isinstance(value, str) and value.strip():
                # Handle numeric strings
                if "." in value:
                    return int(float(value))
                return int(value)
            elif isinstance(value, (int, float)):
                return int(value)
            raise ValueError(f"Cannot convert {value} to integer")
        elif isinstance(field_type, FloatType) or isinstance(field_type, DoubleType):
            # New support for floating point types
            if isinstance(value, str) and value.strip():
                return float(value)
            return float(value)
        elif isinstance(field_type, DecimalType):
            # New support for Decimal type

            if isinstance(value, str) and value.strip():
                return Decimal(value)
            return Decimal(str(value))
        elif isinstance(field_type, BooleanType):
            # Enhanced boolean conversion
            if isinstance(value, str):
                lowered = value.lower()
                if lowered in ("true", "t", "yes", "y", "1"):
                    return True
                elif lowered in ("false", "f", "no", "n", "0"):
                    return False
            return bool(value)
        elif isinstance(field_type, DateType):
            # New support for DateType
            if isinstance(value, str):
                # Try multiple date formats
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                # ISO format as fallback
                return datetime.fromisoformat(value).date()
            elif isinstance(value, datetime):
                return value.date()
            raise ValueError(f"Cannot convert {value} to date")
        elif isinstance(field_type, TimestampType):
            # Enhanced timestamp handling
            if isinstance(value, str):
                # Handle multiple timestamp formats including Z and timezone offsets
                if value.endswith("Z"):
                    value = value.replace("Z", "+00:00")
                try:
                    return datetime.fromisoformat(value)
                except ValueError:
                    # Try additional formats if ISO format fails
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
            elif isinstance(value, (int, float)):
                # Handle Unix timestamps
                return datetime.fromtimestamp(value)
            elif isinstance(value, datetime):
                return value
            raise ValueError(f"Cannot convert {value} to timestamp")
        elif isinstance(field_type, NullType):
            # If schema says NullType, the only safe value is None.
            return None
        else:
            # NOTE: Do NOT call `field_type.fromJson(value)` here.
            # PySpark's `fromJson` is for parsing *type definitions* from JSON, not decoding row values.
            raise TypeError(f"Unsupported field type: {field_type}")
    except (ValueError, TypeError) as e:
        # Add context to the error
        raise ValueError(
            f"Error converting '{value}' ({type(value)}) to {field_type}: {str(e)}"
        )
