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
    scan_partition_table,
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
_SYM_DOMAIN_Q = "sym"
# Enumerations do not compare with longs once the sym domain is loaded.
_SYMBOL_ROWS_Q = "{[sympath; symi] `chunk_idx set where symi=`long$get hsym sympath}"
# Rows whose enumeration index is null or beyond the root sym file.
_UNMATCHED_ROWS_Q = (
    "{[sympath; n] `chunk_idx set where not (`long$get hsym sympath) within (0; n-1)}"
)
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
    _ensure_sym_domain(kx, hdb_root_path)
    return _symbols_to_strings(kx.q(_SYM_DOMAIN_Q))


def sym_file_signature(hdb_root_path: str) -> tuple[int, int]:
    """Return ``(size, mtime_ns)`` of the root sym file."""
    return _sym_file_signature(_sym_file_path(hdb_root_path))


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
    sym_index: int | None,
    runtime_config: PyKxRuntimeConfig,
    column_defs: List[dict],
    sym_column: str = "sym",
    sym_count: int | None = None,
) -> Iterator[dict]:
    """Read one HDB date partition filtered to one symbol enumeration index.

    ``sym_index=None`` selects the rows whose index is null or not below
    ``sym_count``, the number of symbols in the root sym file.
    """
    sym_column = validate_hdb_name(sym_column, "sym_column")
    if sym_index is None and sym_count is None:
        raise ValueError("sym_count is required to read unmatched symbol rows")
    if sym_index is not None and sym_index < 0:
        return

    entries = scan_partition_table(hdb_root_path, date_partition, kdb_table_name)
    partition_path = hdb_child_path(hdb_root_path, date_partition, kdb_table_name)
    if entries is None:
        logger.debug("Table partition %s is absent.", partition_path)
        return

    column_names = [column["name"] for column in column_defs]
    existing_columns = [
        name
        for name in column_names
        if name != "date" and name in entries and entries[name].is_file(follow_symlinks=False)
    ]
    physical_sym_column = next(
        (column for column in existing_columns if column.lower() == sym_column.lower()),
        None,
    )
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
    if sym_index is None:
        kx.q(_UNMATCHED_ROWS_Q, sym_path, int(sym_count))
    else:
        _prepare_symbol_row_indices(kx, sym_path, sym_index)
    row_count = _symbol_row_count(kx)

    try:
        for offset, size in _chunk_slices(row_count, _ROW_CHUNK_SIZE):
            frame = _read_symbol_chunk(kx, column_paths, existing_columns, offset, size)
            if frame is None or len(frame.index) == 0:
                continue
            frame[physical_sym_column] = frame[physical_sym_column].map(_symbol_text)
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

    # Iterate the q list itself so each column vector keeps its q type for
    # raw integer conversion; converting the whole list first loses it.
    try:
        items = list(vectors)
    except TypeError:
        raw_vectors = vectors.py() if hasattr(vectors, "py") else vectors
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

    # Object dtype stops pandas from turning ints with nulls into float64,
    # which would round longs above 2**53; conversion applies schema types.
    series_by_name = {
        column_name: pd.Series(_vector_to_values(vector), dtype=object)
        for column_name, vector in zip(columns, items)
    }
    return pd.DataFrame(series_by_name)


_Q_INTEGER_NULLS = {5: -(2**15), 6: -(2**31), 7: -(2**63)}


def _raw_integer_values(vector):
    """Return q byte/short/int/long values as Python ints with q nulls as None.

    Raw conversion keeps q infinities (0W) as their integer values instead of
    floats and avoids pandas NA handling.
    """
    q_type = getattr(vector, "t", None)
    if q_type not in (4, 5, 6, 7):
        return None
    try:
        values = vector.py(raw=True)
    except TypeError:
        return None
    if isinstance(values, (bytes, bytearray)):
        return list(values)
    null = _Q_INTEGER_NULLS.get(q_type)
    return [None if value == null else int(value) for value in values]


def _vector_to_values(vector):
    import pandas as pd

    raw_integers = _raw_integer_values(vector)
    if raw_integers is not None:
        return raw_integers
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
    if value is None or value is pd.NaT or value is pd.NA:
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
