"""Normalize PyKX column slices into Spark-compatible Python records."""

from __future__ import annotations

from typing import Iterator, List


def normalize_partition_frame(
    frame,
    date_partition: str,
    column_names: List[str],
    column_defs: List[dict],
):
    """Align a column slice with the inferred schema and coerce value types."""
    import pandas as pd

    frame.columns = [str(column) for column in frame.columns]
    if frame.columns.duplicated().any():
        frame = frame.loc[:, ~frame.columns.duplicated()]

    if "date" in frame.columns:
        date_series = frame["date"]
        if isinstance(date_series, pd.DataFrame):
            date_series = date_series.iloc[:, 0]
        parsed = pd.to_datetime(date_series, errors="coerce")
        fallback = date_series.map(lambda value: str(value) if pd.notna(value) else None)
        frame["date"] = parsed.dt.strftime("%Y.%m.%d").where(parsed.notna(), fallback)
    elif "date" in column_names:
        frame["date"] = date_partition

    for index in range(len(frame.columns)):
        series = frame.iloc[:, index]
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
        if not isinstance(series, pd.Series):
            series = pd.Series(series, index=frame.index)

        dtype_string = str(series.dtype)
        try:
            if series.dtype == object:
                frame.iloc[:, index] = series.map(
                    lambda value: str(value) if pd.notna(value) else None
                )
            elif "timedelta" in dtype_string:
                frame.iloc[:, index] = (
                    series.dt.total_seconds() * 1e9
                ).round().astype("Int64")
            elif "datetime64" in dtype_string:
                frame.iloc[:, index] = pd.to_datetime(series, errors="coerce").dt.floor("us")
            else:
                frame.iloc[:, index] = series
        except Exception:
            frame.iloc[:, index] = series.map(
                lambda value: str(value) if pd.notna(value) else None
            )

    aligned = pd.DataFrame(index=frame.index)
    spark_types = {column["name"]: column.get("spark_type", "") for column in column_defs}
    for column_name in column_names:
        if column_name in frame.columns:
            selected = frame[column_name]
            if isinstance(selected, pd.DataFrame):
                selected = selected.iloc[:, 0]
            aligned[column_name] = _coerce_to_expected_type(
                selected, spark_types.get(column_name, "")
            )
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
        return series.map(lambda value: str(value) if pd.notna(value) else None)
    if "timestamp" in normalized_type:
        return pd.to_datetime(series, errors="coerce").dt.floor("us")
    if "boolean" in normalized_type:
        return series.map(_to_bool)
    if any(type_name in normalized_type for type_name in ("short", "integer", "long")):
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if any(type_name in normalized_type for type_name in ("float", "double")):
        return pd.to_numeric(series, errors="coerce").astype("float64")
    return series


def _to_bool(value):
    import numpy as np
    import pandas as pd

    if pd.isna(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, float, np.number)):
        return value != 0
    return str(value).strip().lower() in {"1", "1.0", "true", "t", "yes", "y"}


def _to_python(value):
    import pandas as pd

    if value is None:
        return None
    if value is pd.NaT:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value
