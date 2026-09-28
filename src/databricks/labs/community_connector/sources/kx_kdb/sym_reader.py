"""Read one KDB date+symbol slice from a FUSE-mounted HDB root."""

from __future__ import annotations

import gc
import logging
import os
import stat
from typing import Iterable, Iterator, List

from databricks.labs.community_connector.sources.kx_kdb.conversion import (
    iter_records,
    normalize_partition_frame,
)
from databricks.labs.community_connector.sources.kx_kdb.filesystem import (
    hdb_child_path,
    validate_hdb_name,
)
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    PyKxRuntimeConfig,
    _PicklableLock,
    _ProcessLocalDict,
    prepare_pykx,
)

logger = logging.getLogger(__name__)

# Keep each date×sym slice chunk small enough for serverless Python memory limits.
_ROW_CHUNK_SIZE = 10_000

_LOAD_SYM_DOMAIN_Q = "{[p] `sym set get hsym p}"
_READ_SYM_FILE_Q = "{[p] get hsym p}"
# Enumerations do not compare with longs once the sym domain is loaded.
_SYMBOL_ROWS_Q = "{[sympath; symi] `chunk_idx set where symi=`long$get hsym sympath}"
_SYMBOL_ROW_COUNT_Q = "count chunk_idx"
_CHUNK_INDICES_Q = "{[row_off; m] chunk_idx[row_off+til m]}"
# q lambdas do not capture outer locals, so ``idx`` is projected explicitly.
# Timespans (16h) become nanosecond longs because Python timedelta drops
# sub-microsecond precision. Month, date, minute, second, and time columns
# (13h, 14h, 17h-19h) become q-formatted strings with nulls as generic null.
_READ_COLUMNS_Q = (
    "{[column_paths; idx] {[idx; p] v: (get hsym p) idx; "
    "$[16h=type v; `long$v; (type v) in 13 14 17 18 19h; "
    "{$[null x; (::); string x]} each v; v]}[idx] each column_paths}"
)

_SYM_DOMAIN_LOCK = _PicklableLock()
_LOADED_SYM_DOMAIN: dict[str, tuple[int, int]] = _ProcessLocalDict()


def _sym_file_path(hdb_root_path: str) -> str:
    return hdb_child_path(hdb_root_path, "sym")


def _sym_file_signature(sym_file: str) -> tuple[int, int]:
    try:
        info = os.lstat(sym_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"HDB sym file does not exist: {sym_file}") from None
    if stat.S_ISLNK(info.st_mode):
        raise ValueError(
            f"HDB sym file {sym_file} is a symbolic link; symlinked HDB content is not "
            "supported."
        )
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"HDB sym file {sym_file} is not a regular file.")
    return info.st_size, info.st_mtime_ns


def _ensure_sym_domain(kx, hdb_root_path: str) -> str:
    """Load the root ``sym`` domain so enumerated columns decode to symbols."""
    sym_file = _sym_file_path(hdb_root_path)
    signature = _sym_file_signature(sym_file)
    with _SYM_DOMAIN_LOCK:
        if _LOADED_SYM_DOMAIN.get(sym_file) != signature:
            kx.q(_LOAD_SYM_DOMAIN_Q, sym_file)
            _LOADED_SYM_DOMAIN.clear()
            _LOADED_SYM_DOMAIN[sym_file] = signature
    return sym_file


def load_sym_enumeration(hdb_root_path: str, runtime_config: PyKxRuntimeConfig) -> list[str]:
    """Return the HDB sym enumeration; list positions are KDB enum indices."""
    kx = prepare_pykx(runtime_config)
    sym_file = _ensure_sym_domain(kx, hdb_root_path)
    return _symbols_to_strings(kx.q(_READ_SYM_FILE_Q, sym_file))


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
    return [_symbol_text(item) for item in values]


def _symbol_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _chunk_slices(row_count: int, chunk_size: int) -> Iterator[tuple[int, int]]:
    """Yield ``(offset, size)`` pairs that cover exactly ``row_count`` rows."""
    for offset in range(0, max(int(row_count), 0), chunk_size):
        yield offset, min(chunk_size, row_count - offset)


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
) -> Iterator[dict]:
    """Read one HDB date partition filtered to a single symbol."""
    sym_column = validate_hdb_name(sym_column, "sym_column")
    if sym_index < 0:
        return

    column_names = [column["name"] for column in column_defs]
    existing_columns = _existing_partition_columns(
        hdb_root_path, date_partition, kdb_table_name, column_names
    )
    physical_sym_column = next(
        (column for column in existing_columns if column.lower() == sym_column.lower()),
        None,
    )
    partition_path = hdb_child_path(hdb_root_path, date_partition, kdb_table_name)
    if physical_sym_column is None:
        logger.warning(
            "Skipping %s because required sym column %r is missing.",
            partition_path,
            sym_column,
        )
        return

    kx = prepare_pykx(runtime_config)
    _ensure_sym_domain(kx, hdb_root_path)
    column_paths = [hdb_child_path(partition_path, column) for column in existing_columns]
    sym_path = hdb_child_path(partition_path, physical_sym_column)
    _prepare_symbol_row_indices(kx, sym_path, sym_index)
    row_count = _symbol_row_count(kx)

    try:
        for offset, size in _chunk_slices(row_count, _ROW_CHUNK_SIZE):
            frame = _read_symbol_chunk(kx, column_paths, existing_columns, offset, size)
            if frame is None or len(frame.index) == 0:
                continue

            if physical_sym_column in frame.columns:
                frame[physical_sym_column] = frame[physical_sym_column].map(_symbol_text)
            elif sym_column in column_names:
                frame[sym_column] = symbol

            normalized = normalize_partition_frame(
                frame=frame,
                date_partition=date_partition,
                column_names=column_names,
                column_defs=column_defs,
            )
            yield from iter_records(normalized)

            del frame, normalized
            gc.collect()
    finally:
        gc.collect()


def _existing_partition_columns(
    hdb_root_path: str,
    date_partition: str,
    kdb_table_name: str,
    column_names: list[str],
) -> list[str]:
    """Return requested column files that are regular files inside the partition."""
    date_path = hdb_child_path(hdb_root_path, date_partition)
    table_entry = _find_entry(date_path, kdb_table_name)
    if table_entry is None:
        return []
    if table_entry.is_symlink():
        raise ValueError(
            f"HDB table directory {table_entry.path} is a symbolic link; "
            "symlinked HDB content is not supported."
        )
    if not table_entry.is_dir(follow_symlinks=False):
        return []

    with os.scandir(table_entry.path) as scanned:
        entries = {entry.name: entry for entry in scanned}
    existing = []
    for column_name in column_names:
        if column_name == "date":
            continue
        entry = entries.get(column_name)
        if entry is None:
            logger.debug("Missing KDB column file: %s/%s", table_entry.path, column_name)
            continue
        if entry.is_symlink():
            raise ValueError(
                f"HDB column file {entry.path} is a symbolic link; "
                "symlinked HDB content is not supported."
            )
        if entry.is_file(follow_symlinks=False):
            existing.append(column_name)
    return existing


def _find_entry(parent_path: str, name: str):
    try:
        with os.scandir(parent_path) as scanned:
            for entry in scanned:
                if entry.name == name:
                    return entry
    except (FileNotFoundError, NotADirectoryError):
        return None
    return None


def _prepare_symbol_row_indices(kx, sym_path: str, sym_index: int) -> None:
    """Materialize row indices for one sym value in the q session."""
    kx.q(_SYMBOL_ROWS_Q, sym_path, int(sym_index))


def _symbol_row_count(kx) -> int:
    return _scalar_int(kx.q(_SYMBOL_ROW_COUNT_Q))


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
    kx, column_paths: list[str], columns: list[str], row_off: int, size: int
):
    """Read exactly ``size`` rows of the current symbol starting at ``row_off``."""
    row_indices = kx.q(_CHUNK_INDICES_Q, int(row_off), int(size))
    if _is_empty_collection(row_indices):
        return None
    vectors = kx.q(_READ_COLUMNS_Q, column_paths, row_indices)
    return _column_vectors_to_frame(columns, vectors)


def _is_empty_collection(value) -> bool:
    if value is None:
        return True
    try:
        return len(value) == 0
    except TypeError:
        return False


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
    if len(items) != len(columns):
        raise RuntimeError(
            f"KDB returned {len(items)} column vectors for {len(columns)} requested columns."
        )

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
    if isinstance(vector, (bytes, bytearray)):
        # A q char vector (type c) converts to one bytes object; each byte is a row.
        return list(bytes(vector).decode("latin-1"))
    if isinstance(vector, str):
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
    if isinstance(value, (bytes, bytearray)):
        # q strings (nested char columns) arrive as bytes; the schema maps them to strings.
        return bytes(value).decode("utf-8", "replace")
    if isinstance(value, (str, bool, int, float, Decimal, date, datetime, time)):
        return value
    if (type(value).__module__ or "").startswith("pykx"):
        raise TypeError(
            f"KDB column data contains an unconverted PyKX {type(value).__name__} value."
        )
    # Do not let pandas see foreign wrapper objects. Pandas may call their
    # __array__ implementation and recurse indefinitely.
    return str(value)
