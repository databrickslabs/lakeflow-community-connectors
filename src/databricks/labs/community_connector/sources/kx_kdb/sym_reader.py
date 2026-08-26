"""Read one KDB date+symbol slice via FUSE HDB roots."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Iterator, List

from databricks.labs.community_connector.sources.kx_kdb.conversion import (
    DEFAULT_PARTITION_CONVERSION_MODE,
    Record,
    iter_records,
    normalize_conversion_mode,
    normalize_partition_frame,
)
from databricks.labs.community_connector.sources.kx_kdb.filesystem import (
    hdb_child_path,
)
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    PyKxRuntimeConfig,
    prepare_pykx,
)

logger = logging.getLogger(__name__)

# Keep each date×sym slice chunk small enough for serverless Python memory limits.
_ROW_CHUNK_SIZE = 10_000


def _partition_path(hdb_root_path: str, date_partition: str, kdb_table_name: str) -> str:
    return hdb_child_path(hdb_root_path, date_partition, kdb_table_name)


def _sym_file_path(hdb_root_path: str) -> str:
    return hdb_child_path(hdb_root_path, "sym")


def _load_sym_into_q(kx, sym_file: str) -> None:
    """Load the root sym enumeration into the q session."""
    try:
        kx.q("{[p] `sym set get hsym p}", sym_file)
    except Exception:
        kx.q("sym: get `:", sym_file)


def _ensure_sym_loaded(kx, hdb_root_path: str) -> str:
    sym_file = _sym_file_path(hdb_root_path)
    _load_sym_into_q(kx, sym_file)
    return sym_file


def load_sym_enumeration(hdb_root_path: str, runtime_config: PyKxRuntimeConfig) -> list[str]:
    """Load the HDB sym enumeration once on the driver."""
    symbols, _ = load_sym_enumeration_with_indices(hdb_root_path, runtime_config)
    return symbols


def load_sym_enumeration_with_indices(
    hdb_root_path: str, runtime_config: PyKxRuntimeConfig
) -> tuple[list[str], dict[str, int]]:
    """Return sym strings and their KDB enumeration indices."""
    kx = prepare_pykx(runtime_config)
    sym_file = _ensure_sym_loaded(kx, hdb_root_path)
    try:
        sym_values = kx.q("{[p] get hsym p}", sym_file)
    except Exception:
        sym_values = kx.q("get `:", sym_file)
    symbols = _symbols_to_strings(sym_values)
    indices = {symbol: index for index, symbol in enumerate(symbols)}
    return symbols, indices


def _symbols_to_strings(value) -> list[str]:
    if hasattr(value, "py"):
        try:
            value = value.py()
        except Exception:
            pass
    if hasattr(value, "tolist"):
        try:
            value = value.tolist()
        except Exception:
            pass
    if isinstance(value, (str, bytes)):
        values: Iterable = [value]
    else:
        try:
            values = list(value)
        except TypeError:
            values = [value]

    symbols = []
    for item in values:
        text = _symbol_text(item)
        if text:
            symbols.append(text)
    return symbols


def _symbol_text(value) -> str:
    text = value.decode("utf-8") if isinstance(value, bytes) else str(value)
    text = text.strip().strip("`")
    while text.endswith("/"):
        text = text[:-1]
    if "/" in text:
        text = text.rsplit("/", 1)[-1]
    return text


def read_kdb_date_sym_records(
    *,
    hdb_root_path: str,
    kdb_table_name: str,
    date_partition: str,
    symbol: str,
    sym_index: int,
    runtime_config: PyKxRuntimeConfig,
    column_defs: List[dict],
    sym_column: str = "sym",
    conversion_mode: str = DEFAULT_PARTITION_CONVERSION_MODE,
) -> Iterator[Record]:
    """Read one HDB date partition filtered to a single symbol."""
    import gc

    mode = normalize_conversion_mode(conversion_mode)
    kx = prepare_pykx(runtime_config)

    column_names = [column["name"] for column in column_defs]
    sym_column = sym_column.strip()
    if not sym_column:
        raise ValueError("sym_column must be a non-empty KDB partition column name")

    try:
        if sym_index < 0:
            return

        partition_path = _partition_path(hdb_root_path, date_partition, kdb_table_name)
        existing_columns = _existing_partition_columns(partition_path, column_names)
        if sym_column.lower() not in {column.lower() for column in existing_columns}:
            logger.warning(
                "Skipping %s because required sym column %r is missing.",
                partition_path,
                sym_column,
            )
            return

        column_paths = [f"{partition_path}/{column_name}" for column_name in existing_columns]
        sym_path = f"{partition_path}/{sym_column}"
        _prepare_symbol_row_indices(kx, sym_path, sym_index)
        row_count = _symbol_row_count(kx)
        if row_count <= 0:
            return

        for offset in range(0, row_count, _ROW_CHUNK_SIZE):
            frame = _read_symbol_chunk(
                kx,
                column_paths,
                existing_columns,
                offset,
                _ROW_CHUNK_SIZE,
            )
            if frame is None or len(frame.index) == 0:
                continue

            import pandas as pd

            if sym_column in frame.columns:
                key_series = frame[sym_column]
                if isinstance(key_series, pd.DataFrame):
                    key_series = key_series.iloc[:, 0]
                frame[sym_column] = key_series.astype(str)
            elif sym_column in column_names:
                frame[sym_column] = symbol

            normalized = normalize_partition_frame(
                frame=frame,
                date_partition=date_partition,
                column_names=column_names,
                column_defs=column_defs,
                include_row_id=False,
                row_id_start=offset,
            )
            yield from iter_records(normalized, conversion_mode=mode)

            del frame, normalized
            gc.collect()
    finally:
        gc.collect()


def _existing_partition_columns(partition_path: str, column_names: list[str]) -> list[str]:
    """Return requested physical column files that exist in a splayed partition."""
    base = Path(partition_path)
    existing = []
    for column_name in column_names:
        if column_name == "date":
            continue
        if (base / column_name).is_file():
            existing.append(column_name)
        else:
            logger.debug("Missing KDB column file: %s", base / column_name)
    return existing


def _prepare_symbol_row_indices(kx, sym_path: str, sym_index: int) -> None:
    """Materialize row indices for one sym value in the q session."""
    kx.q(
        "{[sympath; symi] `chunk_idx set where (get hsym sympath)=symi}",
        sym_path,
        int(sym_index),
    )


def _symbol_row_count(kx) -> int:
    return _scalar_int(kx.q("count chunk_idx"))


def _scalar_int(value) -> int:
    if hasattr(value, "py"):
        try:
            value = value.py()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return int(value)


def _read_symbol_chunk(
    kx,
    column_paths: list[str],
    columns: list[str],
    row_off: int,
    nrows: int,
):
    """Read one row slice for a symbol without materializing all indices in Python."""
    row_indices = kx.q(
        "{[row_off; nrows] "
        "m: nrows & (count chunk_idx - row_off); "
        "$[m<=0; `long$(); chunk_idx[row_off+til m]]}",
        int(row_off),
        int(nrows),
    )
    if _is_empty_collection(row_indices):
        return None
    return _read_columns_for_indices(kx, column_paths, columns, row_indices)


def _symbol_row_indices(kx, sym_path: str, sym_index: int):
    """Return row indices for one sym enumeration value within a partition sym column."""
    return kx.q(
        "{[sympath; symi] where (get hsym sympath)=symi}",
        sym_path,
        int(sym_index),
    )


def _index_values(indices) -> list[int]:
    if indices is None:
        return []

    raw = indices
    if hasattr(raw, "py"):
        try:
            raw = raw.py()
        except Exception:
            pass
    if hasattr(raw, "tolist"):
        try:
            raw = raw.tolist()
        except Exception:
            pass

    if isinstance(raw, (str, bytes)):
        return [int(raw)]

    try:
        values = list(raw)
    except TypeError:
        return [_scalar_int(raw)]

    flattened: list[int] = []
    for value in values:
        if isinstance(value, (list, tuple)):
            flattened.extend(_index_values(value))
        else:
            flattened.append(_scalar_int(value))
    return flattened


def _is_empty_collection(value) -> bool:
    if value is None:
        return True
    try:
        return len(value) == 0
    except TypeError:
        return False


def _read_columns_for_indices(kx, column_paths: list[str], columns: list[str], indices):
    """Read selected rows from existing splayed column files."""
    if _is_empty_collection(indices):
        return None
    vectors = kx.q(
        "{[column_paths; idx] {[p; idx] (get hsym p) idx} each column_paths}",
        column_paths,
        indices,
    )
    return _column_vectors_to_frame(columns, vectors)


def _read_existing_columns_for_symbol(
    kx,
    partition_path: str,
    columns: list[str],
    sym_index: int,
):
    """Read all rows for one sym index from existing splayed column files."""
    column_paths = [f"{partition_path}/{column_name}" for column_name in columns]
    sym_path = f"{partition_path}/sym"
    _prepare_symbol_row_indices(kx, sym_path, sym_index)
    return _read_symbol_chunk(kx, column_paths, columns, 0, _symbol_row_count(kx))


def _column_vectors_to_frame(columns: list[str], vectors):
    import pandas as pd

    if vectors is None:
        return pd.DataFrame()

    raw_vectors = vectors
    if hasattr(raw_vectors, "py"):
        try:
            raw_vectors = raw_vectors.py()
        except Exception:
            pass

    try:
        items = list(raw_vectors)
    except TypeError:
        items = [raw_vectors]

    if not items:
        return pd.DataFrame()

    series_by_name = {}
    for column_name, vector in zip(columns, items):
        series_by_name[column_name] = _vector_to_values(vector)

    return pd.DataFrame(series_by_name)


def _vector_to_values(vector):
    import pandas as pd

    if hasattr(vector, "py"):
        try:
            vector = vector.py()
        except Exception:
            pass
    if hasattr(vector, "pd"):
        try:
            vector = vector.pd()
        except Exception:
            pass
    if hasattr(vector, "tolist"):
        try:
            vector = vector.tolist()
        except Exception:
            pass

    if isinstance(vector, pd.Series):
        return [_plain_python_value(value) for value in vector.tolist()]
    if isinstance(vector, pd.Index):
        return [_plain_python_value(value) for value in vector.tolist()]
    if isinstance(vector, (str, bytes)):
        return [vector]
    try:
        return [_plain_python_value(value) for value in list(vector)]
    except TypeError:
        return [_plain_python_value(vector)]


def _plain_python_value(value):
    from datetime import date, datetime, time
    from decimal import Decimal

    import pandas as pd

    if hasattr(value, "py"):
        try:
            converted = value.py()
            if converted is not value:
                value = converted
        except Exception:
            pass
    if hasattr(value, "tolist"):
        try:
            converted = value.tolist()
            if converted is not value:
                value = converted
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            converted = value.item()
            if converted is not value:
                value = converted
        except Exception:
            pass
    if isinstance(value, list):
        return [_plain_python_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_plain_python_value(item) for item in value)
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, (str, bytes, bool, int, float, Decimal, date, datetime, time)):
        return value
    # Do not let pandas see PyKX/foreign wrapper objects. Pandas may call
    # their __array__ implementation and recurse indefinitely.
    return str(value)
