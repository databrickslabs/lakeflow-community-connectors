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
_MAP_COLUMNS_Q = "{[paths] {get hsym x} each paths}"
_MAPPED_COUNTS_Q = "{[columns] count each columns}"
# q lambdas do not capture outer locals, so ``idx`` is projected explicitly.
# Timespans (16h) become nanosecond longs because Python timedelta drops
# sub-microsecond precision. Month, date, minute, second, and time columns
# (13h, 14h, 17h-19h) become q-formatted strings with nulls as generic null.
_READ_ROWS_Q = (
    "{[columns; off; m] idx: off + til m; {[idx; c] v: c idx; "
    "$[16h=type v; `long$v; (type v) in 13 14 17 18 19h; "
    "{$[null x; (::); string x]} each v; v]}[idx] each columns}"
)

# Layout that q writes for a symbol list such as .d: an 8-byte header, then
# null-terminated names.
_SYMBOL_LIST_MAGIC = b"\xff\x01\x0b\x00"
_SYMBOL_LIST_HEADER_BYTES = 8
_MAX_SYMBOL_LIST_BYTES = 1 << 20
# Layout that q writes for an uncompressed, unattributed fixed-width vector: a
# 16-byte header (magic, type, then zeros) followed by the elements.
_SIMPLE_VECTOR_MAGIC = b"\xfe\x20"
_SIMPLE_VECTOR_HEADER_BYTES = 16
_Q_TYPE_WIDTHS = {
    1: 1,
    2: 16,
    4: 1,
    5: 2,
    6: 4,
    7: 8,
    8: 4,
    9: 8,
    10: 1,
    12: 8,
    13: 4,
    14: 4,
    15: 8,
    16: 8,
    17: 4,
    18: 4,
    19: 4,
}

_SYM_DOMAIN_LOCK = ProcessLocalLock()
_LOADED_SYM_DOMAIN: dict[str, tuple[int, int]] = ProcessLocalDict()


@dataclass(frozen=True)
class PartitionShape:
    """What one splayed table partition looks like on disk.

    ``present`` is False when the date has no directory for the table, which is
    how a sparse table looks. ``complete`` is False while the partition cannot
    be read safely; ``reason`` then says why, and ``recent`` is True when the
    only problem is that a file changed within the configured minimum age.
    ``missing_columns`` lists ``.d`` columns without a file that were allowed
    to be read as nulls.
    """

    present: bool
    complete: bool
    row_count: int = 0
    reason: str = ""
    recent: bool = False
    missing_columns: tuple[str, ...] = ()


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
    allow_missing_columns: bool = False,
) -> PartitionShape:
    """Check that one table partition is fully written and return its row count.

    A partition is complete when ``.d`` exists, every column it lists has a
    file, and the counted columns have the same length. ``check_all_columns``
    counts every column; otherwise only the first column of ``.d`` that has a
    file is counted. ``min_age_seconds`` also requires the newest file to be at
    least that old. ``allow_missing_columns`` accepts ``.d`` columns without a
    file, as long as at least one column has one.
    """
    entries, partition_path, shape = _shape_prelude(
        hdb_root_path, kdb_table_name, date_partition, min_age_seconds
    )
    if shape is not None:
        return shape

    kx = prepare_pykx(runtime_config)
    _ensure_sym_domain(kx, hdb_root_path)
    try:
        columns = _text_list(kx.q(_READ_DOT_D_Q, hdb_child_path(partition_path, ".d")))
    except Exception as exc:
        return _incomplete(
            partition_path, f"has a .d file that q cannot read ({type(exc).__name__})"
        )
    missing, readable, shape = _check_columns(
        entries, partition_path, columns, allow_missing_columns
    )
    if shape is not None:
        return shape

    counted = readable if check_all_columns else readable[:1]
    try:
        counts = _int_list(
            kx.q(
                _COLUMN_COUNTS_Q,
                [hdb_child_path(partition_path, column) for column in counted],
            )
        )
    except Exception as exc:
        return _incomplete(
            partition_path, f"has column files that q cannot read ({type(exc).__name__})"
        )
    if len(counts) != len(counted):
        return _incomplete(
            partition_path, f"returned {len(counts)} counts for {len(counted)} columns"
        )
    if len(set(counts)) > 1:
        lengths = dict(zip(counted, counts))
        return _incomplete(partition_path, f"has columns of different lengths {lengths}")
    return PartitionShape(
        present=True, complete=True, row_count=counts[0], missing_columns=tuple(missing)
    )


def probe_partition_shape(
    *,
    hdb_root_path: str,
    kdb_table_name: str,
    date_partition: str,
    min_age_seconds: float = 0.0,
    allow_missing_columns: bool = False,
) -> PartitionShape | None:
    """Check one table partition from file headers only, without q.

    Applies the same checks as ``read_partition_shape`` with
    ``check_all_columns=False`` and is safe to call from several threads. The
    row count comes from the first column stored as an uncompressed,
    unattributed fixed-width vector. Returns None when ``.d`` or no column is
    in that layout (for example compressed files), so the caller can ask q.
    """
    entries, partition_path, shape = _shape_prelude(
        hdb_root_path, kdb_table_name, date_partition, min_age_seconds
    )
    if shape is not None:
        return shape
    columns = _read_symbol_list_file(hdb_child_path(partition_path, ".d"))
    if columns is None:
        return None
    missing, readable, shape = _check_columns(
        entries, partition_path, columns, allow_missing_columns
    )
    if shape is not None:
        return shape
    for column in readable:
        count = _simple_vector_length(hdb_child_path(partition_path, column), entries[column])
        if count is not None:
            return PartitionShape(
                present=True, complete=True, row_count=count, missing_columns=tuple(missing)
            )
    return None


def _incomplete(partition_path: str, reason: str) -> PartitionShape:
    return PartitionShape(present=True, complete=False, reason=f"{partition_path} {reason}")


def _shape_prelude(
    hdb_root_path: str, kdb_table_name: str, date_partition: str, min_age_seconds: float
) -> tuple[dict[str, os.DirEntry] | None, str, PartitionShape | None]:
    """Run the checks that need only a directory listing.

    Returns ``(entries, partition_path, shape)``; ``shape`` is set when these
    checks already decide the partition.
    """
    entries = scan_partition_table(hdb_root_path, date_partition, kdb_table_name)
    if entries is None:
        return None, "", PartitionShape(present=False, complete=True)
    partition_path = hdb_child_path(hdb_root_path, date_partition, kdb_table_name)
    if not _is_regular_file(entries, ".d"):
        return entries, partition_path, _incomplete(partition_path, "has no .d file")
    if min_age_seconds > 0:
        age = time.time() - partition_last_modified(entries)
        if age < min_age_seconds:
            return (
                entries,
                partition_path,
                PartitionShape(
                    present=True,
                    complete=False,
                    recent=True,
                    reason=(
                        f"{partition_path} was modified {age:.0f}s ago, less than the "
                        f"configured minimum age of {min_age_seconds:.0f}s"
                    ),
                ),
            )
    return entries, partition_path, None


def _check_columns(
    entries: dict[str, os.DirEntry],
    partition_path: str,
    columns: list[str],
    allow_missing_columns: bool,
) -> tuple[list[str], list[str], PartitionShape | None]:
    """Validate the ``.d`` columns against the files on disk.

    Returns ``(missing, readable, shape)``; ``shape`` is set when the columns
    already decide the partition.
    """
    for column in columns:
        validate_hdb_name(column, "column name")
    missing = [column for column in columns if not _is_regular_file(entries, column)]
    if missing and not allow_missing_columns:
        return missing, [], _incomplete(partition_path, f"is missing column files {missing}")
    if not columns:
        return [], [], PartitionShape(present=True, complete=True, row_count=0)
    readable = [column for column in columns if column not in missing]
    if not readable:
        return (
            missing,
            [],
            _incomplete(partition_path, f"has no file for any column listed in .d {columns}"),
        )
    return missing, readable, None


def _read_symbol_list_file(path: str) -> list[str] | None:
    """Return the names in a q symbol-list file such as ``.d``, or None if unrecognized."""
    with open(path, "rb") as handle:
        raw = handle.read(_MAX_SYMBOL_LIST_BYTES + 1)
    if len(raw) > _MAX_SYMBOL_LIST_BYTES or not raw.startswith(_SYMBOL_LIST_MAGIC):
        return None
    body = raw[_SYMBOL_LIST_HEADER_BYTES:]
    if not body:
        return []
    if not body.endswith(b"\0"):
        return None
    try:
        return [name.decode("utf-8") for name in body[:-1].split(b"\0")]
    except UnicodeDecodeError:
        return None


def _simple_vector_length(path: str, entry: os.DirEntry) -> int | None:
    """Return the length of an unattributed fixed-width q vector file, or None."""
    try:
        with open(path, "rb") as handle:
            header = handle.read(_SIMPLE_VECTOR_HEADER_BYTES)
        size = entry.stat(follow_symlinks=False).st_size
    except OSError:
        return None
    if (
        len(header) < _SIMPLE_VECTOR_HEADER_BYTES
        or not header.startswith(_SIMPLE_VECTOR_MAGIC)
        or any(header[3:])
    ):
        return None
    width = _Q_TYPE_WIDTHS.get(header[2])
    payload = size - _SIMPLE_VECTOR_HEADER_BYTES
    if width is None or payload < 0 or payload % width:
        return None
    return payload // width


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
    allow_missing_columns: bool = False,
) -> Iterator[dict]:
    """Read rows ``[row_offset, row_offset + row_count)`` of one table partition.

    ``total_rows`` is the partition row count seen at planning time. The read
    fails if any schema column no longer has that many rows (q would return
    nulls for indexes past the end of a short column). A schema column without
    a file fails the read, or is read as nulls with ``allow_missing_columns``.
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
    schema_columns = [name for name in column_names if name != "date"]
    missing = [name for name in schema_columns if not _is_regular_file(entries, name)]
    if missing and not allow_missing_columns:
        raise RuntimeError(
            f"Table partition {partition_path} is missing column files {missing}. The "
            "partition is incomplete or its columns differ from the inferred schema."
        )
    physical_columns = [name for name in schema_columns if name not in missing]
    if missing:
        logger.warning(
            "Table partition %s has no files for columns %s; reading them as nulls.",
            partition_path,
            missing,
        )
    if row_count == 0 or not schema_columns:
        return
    if not physical_columns:
        raise RuntimeError(
            f"Table partition {partition_path} has no file for any schema column; it "
            f"was planned with {total_rows} rows."
        )

    kx = prepare_pykx(runtime_config)
    _ensure_sym_domain(kx, hdb_root_path)
    # Map every column once; each chunk then indexes the mapped vectors instead
    # of opening the files again over FUSE.
    columns = kx.q(
        _MAP_COLUMNS_Q,
        [hdb_child_path(partition_path, column) for column in physical_columns],
    )
    counts = dict(zip(physical_columns, _int_list(kx.q(_MAPPED_COUNTS_Q, columns))))
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
            vectors = kx.q(_READ_ROWS_Q, columns, int(row_offset + offset), int(size))
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
        # Unmaps the column files before the next task in this worker.
        del columns
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
