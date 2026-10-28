"""Tests for KDB partition-shape checks and row-range reads."""

from __future__ import annotations

import os
import time

import pytest

from databricks.labs.community_connector.sources.kx_kdb import sym_reader
from databricks.labs.community_connector.sources.kx_kdb.runtime import PyKxRuntimeConfig
from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
    read_kdb_row_range,
    read_partition_shape,
)

_CONFIG = PyKxRuntimeConfig(license_directory="/tmp/lic")


@pytest.fixture(autouse=True)
def _reset_sym_domain():
    sym_reader._LOADED_SYM_DOMAIN.clear()
    yield
    sym_reader._LOADED_SYM_DOMAIN.clear()


class _FakeKx:
    """Fake q session over in-memory columns of one partition."""

    def __init__(self, columns: dict[str, list], dot_d=None, fail_on=()):
        self.columns = columns
        self.dot_d = list(columns) if dot_d is None else dot_d
        self.fail_on = set(fail_on)
        self.calls: list[tuple[str, tuple]] = []

    def q(self, query, *args):
        self.calls.append((query, args))
        if query in self.fail_on:
            raise RuntimeError("q error")
        if query == sym_reader._LOAD_SYM_DOMAIN_Q:
            return None
        if query == sym_reader._READ_DOT_D_Q:
            assert args[0].endswith("/.d")
            return self.dot_d
        if query == sym_reader._COLUMN_COUNTS_Q:
            return [len(self.columns[path.rsplit("/", 1)[-1]]) for path in args[0]]
        if query == sym_reader._READ_ROWS_Q:
            paths, offset, size = args
            return [self.columns[path.rsplit("/", 1)[-1]][offset : offset + size] for path in paths]
        raise AssertionError(f"unexpected q query: {query!r}")


def _partition(tmp_path, files=("sym", "time", "price"), dot_d=True):
    (tmp_path / "sym").write_bytes(b"stub")
    partition = tmp_path / "2024.01.01" / "TRADES"
    partition.mkdir(parents=True)
    for name in files:
        (partition / name).write_bytes(b"stub")
    if dot_d:
        (partition / ".d").write_bytes(b"stub")
    return partition


def _use(monkeypatch, fake_kx):
    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: fake_kx)
    return fake_kx


def _shape(tmp_path, **overrides):
    kwargs = {
        "hdb_root_path": str(tmp_path),
        "kdb_table_name": "TRADES",
        "date_partition": "2024.01.01",
        "runtime_config": _CONFIG,
        "check_all_columns": True,
    }
    kwargs.update(overrides)
    return read_partition_shape(**kwargs)


_COLUMN_DEFS = [
    {"name": "date", "spark_type": "StringType"},
    {"name": "sym", "spark_type": "StringType", "q_type": "s"},
    {"name": "time", "spark_type": "StringType", "q_type": "t"},
    {"name": "price", "spark_type": "DoubleType", "q_type": "f"},
]


def _rows(tmp_path, **overrides):
    kwargs = {
        "hdb_root_path": str(tmp_path),
        "kdb_table_name": "TRADES",
        "date_partition": "2024.01.01",
        "row_offset": 0,
        "row_count": 3,
        "total_rows": 3,
        "runtime_config": _CONFIG,
        "column_defs": _COLUMN_DEFS,
    }
    kwargs.update(overrides)
    return list(read_kdb_row_range(**kwargs))


def _three_rows():
    return {
        "sym": ["AAPL", "AAPL", "MSFT"],
        "time": ["09:30:00.000", "09:30:01.000", "09:30:02.000"],
        "price": [1.0, 2.0, 3.0],
    }


# ---------------------------------------------------------------------------
# read_partition_shape
# ---------------------------------------------------------------------------


def test_complete_partition_reports_its_row_count(monkeypatch, tmp_path):
    _partition(tmp_path)
    _use(monkeypatch, _FakeKx(_three_rows()))

    shape = _shape(tmp_path)

    assert (shape.present, shape.complete, shape.row_count) == (True, True, 3)


def test_absent_table_directory_is_an_empty_sparse_date(monkeypatch, tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")
    (tmp_path / "2024.01.01").mkdir()
    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: pytest.fail("no q for absent table"))

    shape = _shape(tmp_path)

    assert (shape.present, shape.complete, shape.row_count) == (False, True, 0)


def test_partition_without_dot_d_is_incomplete(monkeypatch, tmp_path):
    _partition(tmp_path, dot_d=False)
    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: pytest.fail("no q without .d"))

    shape = _shape(tmp_path)

    assert not shape.complete
    assert "no .d file" in shape.reason


def test_partition_missing_a_dot_d_column_file_is_incomplete(monkeypatch, tmp_path):
    _partition(tmp_path, files=("sym", "time"))
    _use(monkeypatch, _FakeKx(_three_rows()))

    shape = _shape(tmp_path)

    assert not shape.complete
    assert "['price']" in shape.reason


def test_unequal_column_lengths_make_the_newest_date_incomplete(monkeypatch, tmp_path):
    _partition(tmp_path)
    columns = _three_rows()
    columns["price"] = [1.0]
    _use(monkeypatch, _FakeKx(columns))

    shape = _shape(tmp_path)

    assert not shape.complete
    assert "different lengths" in shape.reason


def test_older_dates_count_only_the_first_column(monkeypatch, tmp_path):
    _partition(tmp_path)
    columns = _three_rows()
    columns["price"] = [1.0]
    fake_kx = _use(monkeypatch, _FakeKx(columns))

    shape = _shape(tmp_path, check_all_columns=False)

    assert (shape.complete, shape.row_count) == (True, 3)
    counted = [args[0] for query, args in fake_kx.calls if query == sym_reader._COLUMN_COUNTS_Q]
    assert counted == [[f"{tmp_path}/2024.01.01/TRADES/sym"]]


def test_unreadable_dot_d_is_incomplete(monkeypatch, tmp_path):
    _partition(tmp_path)
    _use(monkeypatch, _FakeKx(_three_rows(), fail_on={sym_reader._READ_DOT_D_Q}))

    shape = _shape(tmp_path)

    assert not shape.complete
    assert "cannot read" in shape.reason


def test_partially_written_column_is_incomplete(monkeypatch, tmp_path):
    _partition(tmp_path)
    _use(monkeypatch, _FakeKx(_three_rows(), fail_on={sym_reader._COLUMN_COUNTS_Q}))

    assert not _shape(tmp_path).complete


def test_recently_modified_partition_is_incomplete_under_min_age(monkeypatch, tmp_path):
    partition = _partition(tmp_path)
    _use(monkeypatch, _FakeKx(_three_rows()))

    assert not _shape(tmp_path, min_age_seconds=600).complete

    old = time.time() - 3600
    for path in partition.iterdir():
        os.utime(path, (old, old))
    assert _shape(tmp_path, min_age_seconds=600).complete


def test_unsafe_column_name_in_dot_d_is_rejected(monkeypatch, tmp_path):
    _partition(tmp_path)
    _use(monkeypatch, _FakeKx(_three_rows(), dot_d=["sym", "../escape"]))

    with pytest.raises(ValueError, match="column name"):
        _shape(tmp_path)


# ---------------------------------------------------------------------------
# read_kdb_row_range
# ---------------------------------------------------------------------------


def test_row_range_reads_only_its_rows_in_chunks(monkeypatch, tmp_path):
    _partition(tmp_path)
    monkeypatch.setattr(sym_reader, "_ROW_CHUNK_SIZE", 2)
    columns = {
        "sym": ["A", "B", "C", "D", "E", "F"],
        "time": ["t0", "t1", "t2", "t3", "t4", "t5"],
        "price": [0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
    }
    fake_kx = _use(monkeypatch, _FakeKx(columns))

    records = _rows(tmp_path, row_offset=1, row_count=3, total_rows=6)

    reads = [args[1:] for query, args in fake_kx.calls if query == sym_reader._READ_ROWS_Q]
    assert reads == [(1, 2), (3, 1)]
    assert records == [
        {"date": "2024.01.01", "sym": "B", "time": "t1", "price": 1.0},
        {"date": "2024.01.01", "sym": "C", "time": "t2", "price": 2.0},
        {"date": "2024.01.01", "sym": "D", "time": "t3", "price": 3.0},
    ]


def test_row_range_loads_the_sym_domain_before_reading(monkeypatch, tmp_path):
    _partition(tmp_path)
    fake_kx = _use(monkeypatch, _FakeKx(_three_rows()))

    _rows(tmp_path)

    assert fake_kx.calls[0] == (sym_reader._LOAD_SYM_DOMAIN_Q, (f"{tmp_path}/sym",))


def test_row_range_fails_when_the_partition_changed_after_planning(monkeypatch, tmp_path):
    _partition(tmp_path)
    _use(monkeypatch, _FakeKx(_three_rows()))

    with pytest.raises(RuntimeError, match="changed after the batch was planned"):
        _rows(tmp_path, row_count=2, total_rows=2)


def test_row_range_fails_when_any_column_is_shorter_than_planned(monkeypatch, tmp_path):
    # q returns nulls for indexes past the end of a short column, so every
    # column must be counted before reading.
    _partition(tmp_path)
    columns = _three_rows()
    columns["price"] = [1.0, 2.0]
    fake_kx = _use(monkeypatch, _FakeKx(columns))

    with pytest.raises(RuntimeError, match=r"'price': 2"):
        _rows(tmp_path)
    assert not any(query == sym_reader._READ_ROWS_Q for query, _ in fake_kx.calls)


def test_row_range_fails_on_a_missing_schema_column(monkeypatch, tmp_path):
    _partition(tmp_path, files=("sym", "time"))
    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: pytest.fail("no q without columns"))

    with pytest.raises(RuntimeError, match=r"missing column files \['price'\]"):
        _rows(tmp_path)


def test_row_range_fails_when_the_partition_disappeared(tmp_path):
    (tmp_path / "sym").write_bytes(b"stub")
    (tmp_path / "2024.01.01").mkdir()

    with pytest.raises(RuntimeError, match="planned with 3 rows"):
        _rows(tmp_path)


@pytest.mark.parametrize(("offset", "count", "total"), [(-1, 1, 3), (0, -1, 3), (2, 2, 3)])
def test_invalid_row_ranges_are_rejected(tmp_path, offset, count, total):
    with pytest.raises(ValueError, match="Invalid row range"):
        _rows(tmp_path, row_offset=offset, row_count=count, total_rows=total)


def test_null_symbols_become_empty_strings(monkeypatch, tmp_path):
    _partition(tmp_path)
    columns = _three_rows()
    columns["sym"] = [None, b"BRK/B", "AAPL"]
    _use(monkeypatch, _FakeKx(columns))

    assert [record["sym"] for record in _rows(tmp_path)] == ["", "BRK/B", "AAPL"]


def test_only_the_minimum_age_check_reports_a_recent_partition(monkeypatch, tmp_path):
    partition = _partition(tmp_path)
    _use(monkeypatch, _FakeKx(_three_rows()))

    recent = _shape(tmp_path, min_age_seconds=600)
    assert (recent.complete, recent.recent) == (False, True)

    (partition / ".d").unlink()
    missing = _shape(tmp_path, min_age_seconds=600)
    assert (missing.complete, missing.recent) == (False, False)


# ---------------------------------------------------------------------------
# Column vector conversion
# ---------------------------------------------------------------------------


def test_column_vectors_to_frame_wraps_single_row_scalars():
    frame = sym_reader._column_vectors_to_frame(
        ["sym", "time", "type", "price"],
        ["AAPL", "09:30:00.000", "T", 1.23],
    )

    assert frame.to_dict(orient="list") == {
        "sym": ["AAPL"],
        "time": ["09:30:00.000"],
        "type": ["T"],
        "price": [1.23],
    }


def test_column_vectors_to_frame_unwraps_pykx_atom_wrappers():
    class _PyKxAtom:
        def __init__(self, value):
            self._value = value

        def py(self):
            return self._value

    frame = sym_reader._column_vectors_to_frame(
        ["sym", "type"],
        [[_PyKxAtom("AAPL")], [_PyKxAtom("T")]],
    )

    assert frame.to_dict(orient="list") == {"sym": ["AAPL"], "type": ["T"]}


def test_column_vectors_to_frame_stringifies_unconverted_foreign_wrappers():
    class _ForeignWrapper:
        def py(self):
            return self

        def __array__(self, *_):
            raise RecursionError("simulated recursive __array__")

        def __str__(self):
            return "wrapped"

    frame = sym_reader._column_vectors_to_frame(["type"], [[_ForeignWrapper()]])

    assert frame.to_dict(orient="list") == {"type": ["wrapped"]}


def test_column_vectors_to_frame_rejects_a_column_count_mismatch():
    with pytest.raises(RuntimeError, match="1 column vectors for 2 requested columns"):
        sym_reader._column_vectors_to_frame(["sym", "price"], [["AAPL"]])
