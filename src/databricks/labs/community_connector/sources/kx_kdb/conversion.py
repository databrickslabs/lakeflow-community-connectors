"""Normalize PyKX column slices into Spark-compatible Python records."""

from __future__ import annotations

import math
from typing import Iterator, List


def normalize_partition_frame(
    frame,
    date_partition: str,
    column_names: List[str],
    column_defs: List[dict],
):
    """Align a column slice with the inferred schema and coerce value types."""
    import pandas as pd

    spark_types = {column["name"]: column.get("spark_type", "") for column in column_defs}
    aligned = pd.DataFrame(index=frame.index)
    for column_name in column_names:
        if column_name in frame.columns:
            aligned[column_name] = _coerce_to_expected_type(
                frame[column_name], spark_types.get(column_name, "")
            )
        elif column_name == "date":
            aligned[column_name] = date_partition
        else:
            aligned[column_name] = None
    return aligned


def iter_records(frame) -> Iterator[dict]:
    """Yield one ``{column: value}`` dict per row, as the framework parser expects."""
    columns = list(frame.columns)
    for row in frame.itertuples(index=False, name=None):
        yield {columns[col_index]: _to_python(value) for col_index, value in enumerate(row)}


def _coerce_to_expected_type(series, spark_type: str):
    import pandas as pd

    normalized_type = str(spark_type or "").lower()
    if "string" in normalized_type:
        return series.map(_to_text).astype(object)
    if "timestamp" in normalized_type:
        return pd.to_datetime(series, errors="coerce").dt.floor("us")
    if "boolean" in normalized_type:
        return series.map(_to_bool).astype(object)
    if any(type_name in normalized_type for type_name in ("short", "integer", "long")):
        return pd.Series(
            pd.array([_to_int(value) for value in series], dtype="Int64"),
            index=series.index,
        )
    if any(type_name in normalized_type for type_name in ("float", "double")):
        return pd.to_numeric(series, errors="coerce").astype("float64")
    return series


def _is_null(value) -> bool:
    import pandas as pd

    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _to_text(value):
    if _is_null(value):
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def _to_int(value):
    if _is_null(value):
        return None
    if isinstance(value, float):
        return int(value) if math.isfinite(value) and value.is_integer() else None
    return int(value)


def _to_bool(value):
    import numpy as np

    if _is_null(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, float, np.number)):
        return value != 0
    return str(value).strip().lower() in {"1", "1.0", "true", "t", "yes", "y"}


def _to_python(value):
    if _is_null(value):
        return None
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value
