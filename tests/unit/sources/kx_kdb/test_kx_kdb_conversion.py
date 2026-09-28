"""Tests for shared KX KDB conversion helpers."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from databricks.labs.community_connector.sources.kx_kdb.conversion import (
    iter_records,
    normalize_partition_frame,
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

    assert list(iter_records(normalized)) == [
        {"date": "2024.01.02", "sym": "a", "size": None}
    ]


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
