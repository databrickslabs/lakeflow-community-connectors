"""Tests for date_sym KDB reads."""

from __future__ import annotations

from databricks.labs.community_connector.sources.kx_kdb import sym_reader
from databricks.labs.community_connector.sources.kx_kdb.runtime import PyKxRuntimeConfig
from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
    read_kdb_date_sym_records,
)


class _ArrowFrame:
    def __init__(self, frame):
        self._frame = frame

    def to_pandas(self, **_):
        return self._frame.copy()


class _Result:
    def __init__(self, frame):
        self._frame = frame

    def __len__(self):
        return len(self._frame.index)

    def pa(self, **_):
        return _ArrowFrame(self._frame)


class _FakeKx:
    def __init__(self):
        self.calls = []

    def SymbolAtom(self, value):
        return value

    def q(self, query, *args):
        self.calls.append((query, args))
        if "`chunk_idx set where (get hsym sympath)=symi" in query:
            return None
        if query == "count chunk_idx":
            return 1
        if "chunk_idx[row_off+til m]" in query:
            return [0]
        if "each column_paths" in query:
            paths = args[0]
            assert [p.rsplit("/", 1)[-1] for p in paths] == ["sym", "time", "price"]
            return [
                ["AAPL"],
                ["09:30:00.000"],
                [1.23],
            ]
        return None


def test_date_sym_reader_ignores_missing_optional_column_files(monkeypatch, tmp_path):
    partition = tmp_path / "2024.01.01" / "TRADES"
    partition.mkdir(parents=True)
    for column_name in ("sym", "time", "price"):
        (partition / column_name).write_text("stub")

    fake_kx = _FakeKx()
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.sym_reader.prepare_pykx",
        lambda _: fake_kx,
    )

    records = list(
        read_kdb_date_sym_records(
            hdb_root_path=str(tmp_path),
            kdb_table_name="TRADES",
            date_partition="2024.01.01",
            symbol="AAPL",
            sym_index=0,
            runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
            column_defs=[
                {"name": "date", "spark_type": "StringType"},
                {"name": "sym", "spark_type": "StringType"},
                {"name": "time", "spark_type": "StringType"},
                {"name": "price", "spark_type": "DoubleType"},
                {"name": "size", "spark_type": "LongType"},
            ],
        )
    )

    assert records == [
        {
            "date": "2024.01.01",
            "sym": "AAPL",
            "time": "09:30:00.000",
            "price": 1.23,
            "size": None,
        }
    ]


def test_date_sym_reader_uses_custom_sym_column(monkeypatch, tmp_path):
    partition = tmp_path / "2024.01.01" / "TRADES"
    partition.mkdir(parents=True)
    for column_name in ("optionId", "time", "price"):
        (partition / column_name).write_text("stub")

    class _FakeKx:
        def q(self, query, *args):
            if "`chunk_idx set where (get hsym sympath)=symi" in query:
                assert args[0].endswith("/optionId")
                return None
            if query == "count chunk_idx":
                return 1
            if "chunk_idx[row_off+til m]" in query:
                return [0]
            if "each column_paths" in query:
                assert [p.rsplit("/", 1)[-1] for p in args[0]] == [
                    "optionId",
                    "time",
                    "price",
                ]
                return [["OPT1"], ["09:30:00.000"], [1.23]]
            return None

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.sym_reader.prepare_pykx",
        lambda _: _FakeKx(),
    )

    records = list(
        read_kdb_date_sym_records(
            hdb_root_path=str(tmp_path),
            kdb_table_name="TRADES",
            date_partition="2024.01.01",
            symbol="OPT1",
            sym_index=0,
            sym_column="optionId",
            runtime_config=PyKxRuntimeConfig(license_directory="/tmp/lic"),
            column_defs=[
                {"name": "date", "spark_type": "StringType"},
                {"name": "optionId", "spark_type": "StringType"},
                {"name": "time", "spark_type": "StringType"},
                {"name": "price", "spark_type": "DoubleType"},
            ],
        )
    )

    assert records == [
        {
            "date": "2024.01.01",
            "optionId": "OPT1",
            "time": "09:30:00.000",
            "price": 1.23,
        }
    ]
def test_load_sym_enumeration_with_indices_supports_fuse_roots(monkeypatch):
    calls = []

    class _FakeKx:
        def q(self, query, *args):
            calls.append((query, args))
            if query == "{[p] `sym set get hsym p}":
                return None
            if query == "{[p] get hsym p}":
                assert args == ("/Volumes/main/rkh/rbc_kx/kdb_format/sym",)
                return ["a", "b"]
            raise AssertionError(f"unexpected query: {query!r}")

    monkeypatch.setattr(sym_reader, "prepare_pykx", lambda _: _FakeKx())

    symbols, indices = sym_reader.load_sym_enumeration_with_indices(
        "/Volumes/main/rkh/rbc_kx/kdb_format",
        PyKxRuntimeConfig(license_directory="/tmp/lic"),
    )

    assert symbols == ["a", "b"]
    assert indices == {"a": 0, "b": 1}
    assert calls[0] == (
        "{[p] `sym set get hsym p}",
        ("/Volumes/main/rkh/rbc_kx/kdb_format/sym",),
    )


def test_read_existing_columns_builds_frame_in_python_for_reserved_names():
    """Column vectors are assembled in Python, not via q table ops."""
    from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
        _read_existing_columns_for_symbol,
    )

    class _FakeKx:
        def q(self, query, *args):
            if "`chunk_idx set where (get hsym sympath)=symi" in query:
                return None
            if query == "count chunk_idx":
                return 1
            if "chunk_idx[row_off+til m]" in query:
                return [0]
            if "each column_paths" in query:
                assert args[0] == [
                    "/tmp/hdb/2024.01.01/TRADES/sym",
                    "/tmp/hdb/2024.01.01/TRADES/type",
                ]
                return [["AAPL"], ["T"]]
            raise AssertionError(f"unexpected query: {query!r}")

    frame = _read_existing_columns_for_symbol(
        _FakeKx(),
        "/tmp/hdb/2024.01.01/TRADES",
        ["sym", "type"],
        0,
    )

    assert frame.to_dict(orient="list") == {"sym": ["AAPL"], "type": ["T"]}


def test_index_values_flattens_nested_lists():
    from databricks.labs.community_connector.sources.kx_kdb.sym_reader import _index_values

    assert _index_values([[1, 2], 3]) == [1, 2, 3]


def test_column_vectors_to_frame_wraps_single_row_scalars():
    from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
        _column_vectors_to_frame,
    )

    frame = _column_vectors_to_frame(
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
    from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
        _column_vectors_to_frame,
    )

    class _PyKxAtom:
        def __init__(self, value):
            self._value = value

        def py(self):
            return self._value

    frame = _column_vectors_to_frame(
        ["sym", "type"],
        [[_PyKxAtom("AAPL")], [_PyKxAtom("T")]],
    )

    assert frame.to_dict(orient="list") == {"sym": ["AAPL"], "type": ["T"]}


def test_column_vectors_to_frame_stringifies_unconverted_foreign_wrappers():
    from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
        _column_vectors_to_frame,
    )

    class _ForeignWrapper:
        def py(self):
            return self

        def __array__(self, *_):
            raise RecursionError("simulated recursive __array__")

        def __str__(self):
            return "wrapped"

    frame = _column_vectors_to_frame(["type"], [[_ForeignWrapper()]])

    assert frame.to_dict(orient="list") == {"type": ["wrapped"]}
