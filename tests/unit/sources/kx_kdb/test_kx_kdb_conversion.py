"""Tests for shared KX KDB conversion helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from databricks.labs.community_connector.sources.kx_kdb.conversion import (
    iter_records,
    normalize_conversion_mode,
    normalize_partition_frame,
    result_to_frame,
)
from databricks.labs.community_connector.sources.kx_kdb.conversion_metrics import (
    get_conversion_path_counts,
    reset_conversion_path_counts,
)


class _ArrowFrame:
    def __init__(self, frame):
        self._frame = frame

    def to_pandas(self, **_):
        return self._frame.copy()


class _Result:
    def __init__(self, frame, *, fail_raw=False, fail_pa=False):
        self._frame = frame
        self.fail_raw = fail_raw
        self.fail_pa = fail_pa
        self.pa_raw_calls = 0
        self.pa_plain_calls = 0
        self.pd_calls = 0

    def pa(self, **kwargs):
        if kwargs.get("raw") is True:
            self.pa_raw_calls += 1
            if self.fail_raw:
                raise RuntimeError("raw conversion failed")
        else:
            self.pa_plain_calls += 1
            if self.fail_pa:
                raise RuntimeError("plain conversion failed")
        return _ArrowFrame(self._frame)

    def pd(self):
        self.pd_calls += 1
        return self._frame.copy()


def _columns(*names):
    return [{"name": name, "spark_type": "StringType"} for name in names]


def test_result_to_frame_falls_back_from_raw_pa_to_plain_pa():
    page = _Result(pd.DataFrame({"sym": ["a"]}), fail_raw=True)
    reset_conversion_path_counts()

    frame = result_to_frame(page, conversion_mode="pandas")

    assert frame.to_dict("records") == [{"sym": "a"}]
    assert page.pa_raw_calls == 1
    assert page.pa_plain_calls == 1
    assert page.pd_calls == 0


def test_result_to_frame_falls_back_from_pa_to_pd():
    page = _Result(pd.DataFrame({"sym": ["a"]}), fail_raw=True, fail_pa=True)
    reset_conversion_path_counts()

    frame = result_to_frame(page, conversion_mode="pandas")

    assert frame.to_dict("records") == [{"sym": "a"}]
    assert page.pa_raw_calls == 1
    assert page.pa_plain_calls == 1
    assert page.pd_calls == 1


def test_result_to_frame_tracks_pa_raw_conversion_path():
    page = _Result(pd.DataFrame({"sym": ["a"]}))
    reset_conversion_path_counts()

    result_to_frame(page, conversion_mode="pandas")

    assert get_conversion_path_counts() == {
        "pa_raw": 1,
        "pa": 0,
        "pd_fallback": 0,
    }


def test_arrow_direct_yields_column_order_tuples():
    frame = pd.DataFrame({"sym": ["a", "b"], "price": [1.0, 2.0]})
    normalized = normalize_partition_frame(
        frame=frame,
        date_partition="2024.01.01",
        column_names=["sym", "price"],
        column_defs=_columns("sym", "price"),
        include_row_id=False,
    )

    records = list(iter_records(normalized, conversion_mode="arrow_direct"))

    assert records == [("a", "1.0"), ("b", "2.0")]
    assert all(isinstance(record, tuple) for record in records)


def test_arrow_pandas_rejects_pd_fallback():
    page = _Result(pd.DataFrame({"sym": ["a"]}), fail_raw=True, fail_pa=True)

    with pytest.raises(Exception):
        result_to_frame(page, conversion_mode="arrow_pandas")


def test_normalize_conversion_mode_rejects_unknown_conversion_mode():
    with pytest.raises(ValueError, match="partition_conversion_mode"):
        normalize_conversion_mode("invalid")
