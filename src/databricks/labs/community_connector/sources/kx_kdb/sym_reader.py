"""Read KDB partition shapes and row ranges from a FUSE-mounted HDB root."""

from __future__ import annotations

import gc
import logging
import os
import stat
import time
from dataclasses import dataclass
from typing import Iterable, Iterator, List

from databricks.labs.community_connector.sources.kx_kdb.conversion import (
    iter_records,
    normalize_partition_frame,
    to_python,
)
from databricks.labs.community_connector.sources.kx_kdb.filesystem import (
    hdb_child_path,
    partition_last_modified,
    scan_partition_table,
    validate_hdb_name,
)
from databricks.labs.community_connector.sources.kx_kdb.process_local import (
    ProcessLocalDict,
    ProcessLocalLock,
)
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    PyKxRuntimeConfig,
    prepare_pykx,
)

logger = logging.getLogger(__name__)

# Keep each row-range chunk small enough for serverless Python memory limits.
_ROW_CHUNK_SIZE = 10_000

_LOAD_SYM_DOMAIN_Q = "{[p] `sym set get hsym p}"
_READ_DOT_D_Q = "{[p] get hsym p}"
_COLUMN_COUNTS_Q = "{[paths] {count get hsym x} each paths}"
# q lambdas do not capture outer locals, so ``idx`` is projected explicitly.
# Timespans (16h) become nanosecond longs because Python timedelta drops
# sub-microsecond precision. Month, date, minute, second, and time columns
# (13h, 14h, 17h-19h) become q-formatted strings with nulls as generic null.
_READ_ROWS_Q = (
    "{[column_paths; off; m] idx: off + til m; {[idx; p] v: (get hsym p) idx; "
    "$[16h=type v; `long$v; (type v) in 13 14 17 18 19h; "
    "{$[null x; (::); string x]} each v; v]}[idx] each column_paths}"
)

_SYM_DOMAIN_LOCK = ProcessLocalLock()
_LOADED_SYM_DOMAIN: dict[str, tuple[int, int]] = ProcessLocalDict()


@dataclass(frozen=True)
class PartitionShape:
    """What one splayed table partition looks like on disk.

    ``present`` is False when the date has no directory for the table, which is
    how a sparse table looks. ``complete`` is False while the partition cannot
    be read safely; ``reason`` then says why, and ``recent`` is True when the
    only problem is that a file changed within the configured minimum age.
    """

    present: bool
    complete: bool
    row_count: int = 0
    reason: str = ""
    recent: bool = False


def _sym_file_path(hdb_root_path: str) -> str:
    return hdb_child_path(hdb_root_path, "sym")


def _sym_file_signature(sym_file: str) -> tuple[int, int]:
    try:
        info = os.lstat(sym_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"HDB sym file does not exist: {sym_file}") from None
    if stat.S_ISLNK(info.st_mode):
        raise ValueError(
            f"HDB sym file {sym_file} is a symbolic link; symlinked HDB content is not supported."
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


def _text_list(value) -> list[str]:
    value = to_python(value)
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


def _int_list(value) -> list[int]:
    value = to_python(value)
    try:
        return [int(to_python(item)) for item in value]
    except TypeError:
        return [int(value)]


def _is_regular_file(entries: dict[str, os.DirEntry], name: str) -> bool:
    entry = entries.get(name)
    return entry is not None and entry.is_file(follow_symlinks=False)


def read_partition_shape(
    *,
    hdb_root_path: str,
    kdb_table_name: str,
    date_partition: str,
    runtime_config: PyKxRuntimeConfig,
    check_all_columns: bool,
    min_age_seconds: float = 0.0,
) -> PartitionShape:
    """Check that one table partition is fully written and return its row count.

    A partition is complete when ``.d`` exists, every column it lists has a
    file, and the counted columns have the same length. ``check_all_columns``
    counts every column; otherwise only the first column of ``.d`` is counted.
    ``min_age_seconds`` also requires the newest file to be at least that old.
    """
    entries = scan_partition_table(hdb_root_path, date_partition, kdb_table_name)
    if entries is None:
        return PartitionShape(present=False, complete=True)
    partition_path = hdb_child_path(hdb_root_path, date_partition, kdb_table_name)

    def incomplete(reason: str) -> PartitionShape:
        return PartitionShape(present=True, complete=False, reason=f"{partition_path} {reason}")

    if not _is_regular_file(entries, ".d"):
        return incomplete("has no .d file")
    if min_age_seconds > 0:
        age = time.time() - partition_last_modified(entries)
        if age < min_age_seconds:
            return PartitionShape(
                present=True,
                complete=False,
                recent=True,
                reason=(
                    f"{partition_path} was modified {age:.0f}s ago, less than the "
                    f"configured minimum age of {min_age_seconds:.0f}s"
                ),
            )

    kx = prepare_pykx(runtime_config)
    _ensure_sym_domain(kx, hdb_root_path)
    try:
        columns = _text_list(kx.q(_READ_DOT_D_Q, hdb_child_path(partition_path, ".d")))
    except Exception as exc:
        return incomplete(f"has a .d file that q cannot read ({type(exc).__name__})")
    for column in columns:
        validate_hdb_name(column, "column name")

    missing = [column for column in columns if not _is_regular_file(entries, column)]
    if missing:
        return incomplete(f"is missing column files {missing}")
    if not columns:
        return PartitionShape(present=True, complete=True, row_count=0)

    counted = columns if check_all_columns else columns[:1]
    try:
        counts = _int_list(
            kx.q(
                _COLUMN_COUNTS_Q,
                [hdb_child_path(partition_path, column) for column in counted],
            )
        )
    except Exception as exc:
        return incomplete(f"has column files that q cannot read ({type(exc).__name__})")
    if len(counts) != len(counted):
        return incomplete(f"returned {len(counts)} counts for {len(counted)} columns")
    if len(set(counts)) > 1:
        lengths = dict(zip(counted, counts))
        return incomplete(f"has columns of different lengths {lengths}")
    return PartitionShape(present=True, complete=True, row_count=counts[0])


def _chunk_slices(row_count: int, chunk_size: int) -> Iterator[tuple[int, int]]:
    """Yield ``(offset, size)`` pairs that cover exactly ``row_count`` rows."""
    for offset in range(0, max(int(row_count), 0), chunk_size):
        yield offset, min(chunk_size, row_count - offset)


def read_kdb_row_range(
    *,
    hdb_root_path: str,
    kdb_table_name: str,
    date_partition: str,
    row_offset: int,
    row_count: int,
    total_rows: int,
    runtime_config: PyKxRuntimeConfig,
    column_defs: List[dict],
) -> Iterator[dict]:
    """Read rows ``[row_offset, row_offset + row_count)`` of one table partition.

    ``total_rows`` is the partition row count seen at planning time. The read
    fails if any schema column no longer has that many rows (q would return
    nulls for indexes past the end of a short column) or is missing.
    """
    if row_offset < 0 or row_count < 0 or row_offset + row_count > total_rows:
        raise ValueError(
            f"Invalid row range offset={row_offset} count={row_count} for a "
            f"{total_rows}-row partition."
        )
    partition_path = hdb_child_path(hdb_root_path, date_partition, kdb_table_name)
    entries = scan_partition_table(hdb_root_path, date_partition, kdb_table_name)
    if entries is None:
        raise RuntimeError(
            f"Table partition {partition_path} is missing; it was planned with {total_rows} rows."
        )

    column_names = [column["name"] for column in column_defs]
    physical_columns = [name for name in column_names if name != "date"]
    missing = [name for name in physical_columns if not _is_regular_file(entries, name)]
    if missing:
        raise RuntimeError(
            f"Table partition {partition_path} is missing column files {missing}. The "
            "partition is incomplete or its columns differ from the inferred schema."
        )
    if row_count == 0 or not physical_columns:
        return

    kx = prepare_pykx(runtime_config)
    _ensure_sym_domain(kx, hdb_root_path)
    column_paths = [hdb_child_path(partition_path, column) for column in physical_columns]
    counts = dict(zip(physical_columns, _int_list(kx.q(_COLUMN_COUNTS_Q, column_paths))))
    if len(counts) != len(physical_columns) or any(
        count != total_rows for count in counts.values()
    ):
        raise RuntimeError(
            f"Table partition {partition_path} has column lengths {counts} but was "
            f"planned with {total_rows} rows; it changed after the batch was planned."
        )
    symbol_columns = [
        column["name"]
        for column in column_defs
        if column.get("q_type") == "s" and column["name"] in physical_columns
    ]

    try:
        for offset, size in _chunk_slices(row_count, _ROW_CHUNK_SIZE):
            vectors = kx.q(_READ_ROWS_Q, column_paths, int(row_offset + offset), int(size))
            frame = _column_vectors_to_frame(physical_columns, vectors, expected_rows=size)
            for column in symbol_columns:
                frame[column] = frame[column].map(_symbol_text)
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


def _column_vectors_to_frame(columns: list[str], vectors, expected_rows: int | None = None):
    import pandas as pd

    if vectors is None:
        items = []
    else:
        # Iterate the q list itself so each column vector keeps its q type for
        # raw integer conversion; converting the whole list first loses it.
        try:
            items = list(vectors)
        except TypeError:
            raw_vectors = to_python(vectors)
            try:
                items = list(raw_vectors)
            except TypeError:
                items = [raw_vectors]

    if len(items) != len(columns):
        raise RuntimeError(
            f"KDB returned {len(items)} column vectors for {len(columns)} requested columns."
        )
    if not items:
        return pd.DataFrame()

    values_by_name = {
        column_name: _vector_to_values(vector) for column_name, vector in zip(columns, items)
    }
    if expected_rows is not None:
        lengths = {name: len(values) for name, values in values_by_name.items()}
        if any(length != expected_rows for length in lengths.values()):
            raise RuntimeError(
                f"KDB returned column lengths {lengths} for a {expected_rows}-row range."
            )
    # Object dtype stops pandas from turning ints with nulls into float64,
    # which would round longs above 2**53; conversion applies schema types.
    return pd.DataFrame(
        {name: pd.Series(values, dtype=object) for name, values in values_by_name.items()}
    )


_Q_INTEGER_NULLS = {5: -(2**15), 6: -(2**31), 7: -(2**63)}


def _raw_integer_values(vector):
    """Return q byte/short/int/long values as Python ints with q nulls as None.

    Raw conversion keeps q infinities (0W) as their integer values instead of
    floats and avoids pandas NA handling.
    """
    q_type = getattr(vector, "t", None)
    if q_type not in (4, 5, 6, 7):
        return None
    values = to_python(vector, raw=True)
    if values is vector:
        return None
    if isinstance(values, (bytes, bytearray)):
        return list(values)
    null = _Q_INTEGER_NULLS.get(q_type)
    return [None if value == null else int(value) for value in values]


def _vector_to_values(vector):
    raw_integers = _raw_integer_values(vector)
    if raw_integers is not None:
        return raw_integers
    vector = to_python(vector)
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
    from datetime import date, datetime
    from datetime import time as time_of_day
    from decimal import Decimal

    import pandas as pd

    value = to_python(value)
    if isinstance(value, list):
        return [_plain_python_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_plain_python_value(item) for item in value)
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, (bytes, bytearray)):
        # q strings (nested char columns) arrive as bytes; the schema maps them to strings.
        return bytes(value).decode("utf-8", "replace")
    if isinstance(value, (str, bool, int, float, Decimal, date, datetime, time_of_day)):
        return value
    if (type(value).__module__ or "").startswith("pykx"):
        raise TypeError(
            f"KDB column data contains an unconverted PyKX {type(value).__name__} value."
        )
    # Do not let pandas see foreign wrapper objects. Pandas may call their
    # __array__ implementation and recurse indefinitely.
    return str(value)
