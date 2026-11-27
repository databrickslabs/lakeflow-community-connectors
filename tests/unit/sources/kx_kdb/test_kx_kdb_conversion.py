"""Tests for shared KX KDB conversion helpers."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from databricks.labs.community_connector.sources.kx_kdb.conversion import (
    iter_records,
    normalize_partition_frame,
    to_python,
)


def _columns(**types):
    return [{"name": name, "spark_type": spark_type} for name, spark_type in types.items()]


def test_iter_records_yields_dicts_in_schema_order():
    frame = pd.DataFrame({"sym": ["a", "b"], "price": [1.0, 2.0]})
    normalized = normalize_partition_frame(
        frame=frame,
        date_partition="2024.01.01",
        column_names=["sym", "price"],
        column_defs=_columns(sym="StringType", price="DoubleType"),
    )

    records = list(iter_records(normalized))

    assert records == [{"sym": "a", "price": 1.0}, {"sym": "b", "price": 2.0}]


def test_normalize_adds_partition_date_and_missing_columns():
    frame = pd.DataFrame({"sym": ["a"]})

    normalized = normalize_partition_frame(
        frame=frame,
        date_partition="2024.01.02",
        column_names=["date", "sym", "size"],
        column_defs=_columns(date="StringType", sym="StringType", size="LongType"),
    )

    assert list(iter_records(normalized)) == [{"date": "2024.01.02", "sym": "a", "size": None}]


def test_normalize_coerces_types_and_nulls():
    frame = pd.DataFrame(
        {
            "size": ["10", None],
            "ts": [datetime(2024, 1, 1, 9, 30, 0, 123456), None],
            "flag": [1, None],
        }
    )

    records = list(
        iter_records(
            normalize_partition_frame(
                frame=frame,
                date_partition="2024.01.01",
                column_names=["size", "ts", "flag"],
                column_defs=_columns(size="LongType", ts="TimestampType", flag="BooleanType"),
            )
        )
    )

    assert records[0] == {
        "size": 10,
        "ts": datetime(2024, 1, 1, 9, 30, 0, 123456),
        "flag": True,
    }
    assert records[1] == {"size": None, "ts": None, "flag": None}


class _PyKxLike:
    def __init__(self, value=None, *, py_fails=False, pd_value=None):
        self._value = value
        self._py_fails = py_fails
        self._pd_value = pd_value
        self.raw_calls = []

    def py(self, raw=False):
        self.raw_calls.append(raw)
        if self._py_fails:
            raise ValueError("unsupported conversion")
        return self._value

    def pd(self):
        return self._pd_value


def test_to_python_unwraps_pykx_numpy_and_pandas_values():
    assert to_python(_PyKxLike([1, 2])) == [1, 2]
    assert to_python(np.array([1, 2])) == [1, 2]
    assert to_python(np.int64(7)) == 7 and type(to_python(np.int64(7))) is int
    assert to_python(pd.Series(["a"])) == ["a"]
    assert to_python("plain") == "plain"


def test_to_python_falls_back_to_pandas_when_py_fails():
    assert to_python(_PyKxLike(py_fails=True, pd_value=pd.Series([1.5]))) == [1.5]


def test_to_python_returns_an_unconvertible_value_unchanged():
    class _Unconvertible:
        def py(self):
            raise ValueError("unsupported conversion")

    value = _Unconvertible()

    assert to_python(value) is value


def test_to_python_raw_mode_only_uses_raw_py():
    vector = _PyKxLike([1, -(2**63)], pd_value=pd.Series([1.0]))

    assert to_python(vector, raw=True) == [1, -(2**63)]
    assert vector.raw_calls == [True]

    failing = _PyKxLike(py_fails=True, pd_value=pd.Series([1.0]))
    assert to_python(failing, raw=True) is failing
