"""Shared PyKX result conversion helpers for KX HDB reads."""

from __future__ import annotations

from typing import Iterator, List, Union

from databricks.labs.community_connector.sources.kx_kdb.conversion_metrics import (
    ConversionMode,
    record_conversion_path,
)

DEFAULT_PARTITION_CONVERSION_MODE = "pandas"
PARTITION_CONVERSION_MODES = frozenset(
    {"pandas", "arrow_pandas", "arrow_direct"}
)
Record = Union[dict, tuple]


def normalize_conversion_mode(conversion_mode: str) -> ConversionMode:
    normalized = str(conversion_mode or DEFAULT_PARTITION_CONVERSION_MODE).strip().lower()
    if normalized not in PARTITION_CONVERSION_MODES:
        raise ValueError(
            f"Unsupported partition_conversion_mode {conversion_mode!r}. "
            f"Expected one of {sorted(PARTITION_CONVERSION_MODES)}."
        )
    return normalized  # type: ignore[return-value]


def normalize_partition_frame(
    frame,
    date_partition: str,
    column_names: List[str],
    column_defs: List[dict],
    include_row_id: bool,
    row_id_start: int = 0,
):
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

    if include_row_id:
        aligned["__row_id"] = [
            f"{date_partition}:{row_id_start + row_index}"
            for row_index in range(len(aligned.index))
        ]

    return aligned


def iter_records(frame, *, conversion_mode: ConversionMode) -> Iterator[Record]:
    if conversion_mode == "arrow_direct":
        yield from _iter_tuples(frame)
        return
    yield from _iter_dict_records(frame)


def result_to_frame(result, *, conversion_mode: ConversionMode):
    import pandas as pd

    if conversion_mode == "arrow_pandas":
        frame, path = _result_to_pandas_via_arrow(result, allow_pd_fallback=False)
    else:
        frame, path = _result_to_pandas_via_arrow(result, allow_pd_fallback=True)
    record_conversion_path(path)
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    return frame


def result_len(result) -> int:
    if result is None:
        return 0
    try:
        return int(len(result))
    except Exception:
        return 0


def _coerce_to_expected_type(series, spark_type: str):
    import pandas as pd

    normalized_type = str(spark_type or "").lower()
    if "string" in normalized_type:
        return series.map(lambda value: str(value) if pd.notna(value) else None)
    if "timestamp" in normalized_type:
        return pd.to_datetime(series, errors="coerce").dt.floor("us")
    if "boolean" in normalized_type:
        return series.map(
            lambda value: None
            if pd.isna(value)
            else str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
        )
    if any(type_name in normalized_type for type_name in ("short", "integer", "long")):
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if any(type_name in normalized_type for type_name in ("float", "double")):
        return pd.to_numeric(series, errors="coerce").astype("float64")
    return series


def _iter_dict_records(frame) -> Iterator[dict]:
    columns = list(frame.columns)
    for row in frame.itertuples(index=False, name=None):
        yield {columns[col_index]: _to_python(value) for col_index, value in enumerate(row)}


def _iter_tuples(frame) -> Iterator[tuple]:
    for row in frame.itertuples(index=False, name=None):
        yield tuple(_to_python(value) for value in row)


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


def _result_to_pandas_via_arrow(result, *, allow_pd_fallback: bool):
    try:
        return result.pa(raw=True).to_pandas(timestamp_as_object=False), "pa_raw"
    except Exception:
        try:
            return result.pa().to_pandas(timestamp_as_object=False), "pa"
        except Exception:
            if not allow_pd_fallback:
                raise
            return result.pd(), "pd_fallback"
