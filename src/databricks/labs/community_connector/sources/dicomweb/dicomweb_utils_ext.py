# pylint: disable=undefined-variable
"""DICOMweb-specific extension to parse_value that adds VariantType support.

This module is merged AFTER utils.py by merge_python_source.py, so the
redefined parse_value shadows the original and adds handling for VariantType.
The helper functions (_parse_struct, _parse_array, _parse_map, _PRIMITIVE_PARSERS)
defined in the utils.py section are still in scope.
"""

from typing import Any

from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    StructType,
    VariantType,
    VariantVal,
)


def parse_value(value: Any, field_type: DataType) -> Any:
    """Extended parse_value with VariantType support.

    Shadows the parse_value from utils.py to add handling for VariantType.
    When field_type is VariantType and value is a JSON string, the string is
    converted to a VariantVal using VariantVal.parseJson(). If value is already
    a VariantVal it is returned unchanged. All other types fall through to the
    same logic as utils.parse_value.
    """
    if value is None:
        return None

    if isinstance(field_type, StructType):
        result = _parse_struct(value, field_type)
    elif isinstance(field_type, ArrayType):
        result = _parse_array(value, field_type)
    elif isinstance(field_type, MapType):
        result = _parse_map(value, field_type)
    elif isinstance(field_type, VariantType):
        result = VariantVal.parseJson(value) if isinstance(value, str) else value
    elif type(field_type) in _PRIMITIVE_PARSERS:
        result = _PRIMITIVE_PARSERS[type(field_type)](value)
    elif hasattr(field_type, "fromJson"):
        try:
            result = field_type.fromJson(value)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Error converting '{value}' ({type(value)}) to {field_type}: {str(e)}"
            ) from e
    else:
        raise TypeError(f"Unsupported field type: {field_type}")
    return result
