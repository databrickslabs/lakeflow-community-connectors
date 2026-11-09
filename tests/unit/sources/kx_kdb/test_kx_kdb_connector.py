"""Tests for the KX KDB Lakeflow connector class."""

from pathlib import Path

import pytest
from pyspark import cloudpickle

from databricks.labs.community_connector.sources.kx_kdb import filesystem
from databricks.labs.community_connector.sources.kx_kdb._generated_kx_kdb_python_source import (
    register_lakeflow_source,
)
from databricks.labs.community_connector.sources.kx_kdb.kx_kdb import (
    DEFAULT_MAX_ROWS_PER_TASK,
    KxKdbLakeflowConnect,
)
from databricks.labs.community_connector.sources.kx_kdb.sym_reader import PartitionShape

_MODULE = "databricks.labs.community_connector.sources.kx_kdb.kx_kdb"
_DATES = ("2024.01.01", "2024.01.02", "2024.01.03")


@pytest.fixture(autouse=True)
def _reset_discovery_cache():
    filesystem.clear_discovery_cache()
    yield
    filesystem.clear_discovery_cache()


def _build_hdb(tmp_path, dates=_DATES):
    (tmp_path / "sym").write_bytes(b"stub")
    for date in dates:
        (tmp_path / date / "TRADES").mkdir(parents=True)
    return tmp_path


def _connector(tmp_path):
    return KxKdbLakeflowConnect(
        {
            "hdb_root_path": str(tmp_path),
            "license_volume_path": "/Volumes/main/default/keys",
        }
    )


class _Shapes:
    """Fake partition shapes keyed by date; unlisted dates are complete with 3 rows.

    Serves both the q check (``read_partition_shape``) and the header reader
    (``probe_partition_shape``); ``undecided`` dates make the header reader
    return None so the connector falls back to q.
    """

    def __init__(self, monkeypatch, undecided=(), **by_date):
        self.by_date = {date.replace("_", "."): shape for date, shape in by_date.items()}
        self.undecided = set(undecided)
        self.calls: list[dict] = []
        monkeypatch.setattr(f"{_MODULE}.read_partition_shape", self)
        monkeypatch.setattr(f"{_MODULE}.probe_partition_shape", self.probe)

    def __call__(self, via="q", **kwargs):
        self.calls.append({"via": via, **kwargs})
        return self.by_date.get(
            kwargs["date_partition"], PartitionShape(present=True, complete=True, row_count=3)
        )

    def probe(self, **kwargs):
        if kwargs["date_partition"] in self.undecided:
            self.calls.append({"via": "probe-undecided", "check_all_columns": False, **kwargs})
            return None
        return self(via="probe", check_all_columns=False, **kwargs)


def _complete(rows):
    return PartitionShape(present=True, complete=True, row_count=rows)


def _incomplete(reason="has no .d file"):
    return PartitionShape(present=True, complete=False, reason=reason)


def _ranges(date, total, size):
    return [
        {
            "date_partition": date,
            "row_offset": offset,
            "row_count": min(size, total - offset),
            "total_rows": total,
        }
        for offset in range(0, total, size)
    ]


def test_generated_data_source_class_is_cloudpickle_serializable():
    registered = []

    class _Registry:
        @staticmethod
        def register(data_source):
            registered.append(data_source)

    class _Spark:
        dataSource = _Registry()

    register_lakeflow_source(_Spark())

    assert len(registered) == 1
    cloudpickle.dumps(registered[0])


def test_generated_source_does_not_rebind_module_globals():
    generated = (
        Path(__file__).parents[4]
        / "src"
        / "databricks"
        / "labs"
        / "community_connector"
        / "sources"
        / "kx_kdb"
        / "_generated_kx_kdb_python_source.py"
    ).read_text(encoding="utf-8")

    assert "global _RUNTIME_HOMES" not in generated
    assert "global _PREPARED_RUNTIME_KEYS" not in generated


def test_connector_rejects_relative_hdb_path():
    with pytest.raises(ValueError, match="absolute filesystem path"):
        KxKdbLakeflowConnect(
            {
                "hdb_root_path": "relative/hdb",
                "license_volume_path": "/Volumes/catalog/schema/volume/keys",
            }
        )


def test_connector_normalizes_license_file_path(tmp_path):
    connector = KxKdbLakeflowConnect(
        {
            "hdb_root_path": str(_build_hdb(tmp_path)),
            "license_volume_path": "/Volumes/main/kx/files/keys/kc.lic",
        }
    )
    assert connector.runtime_config.license_directory == "/Volumes/main/kx/files/keys"


# ---------------------------------------------------------------------------
# Admission: latest_offset
# ---------------------------------------------------------------------------


def test_latest_offset_returns_the_newest_complete_date(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch)

    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.03"}


def test_incomplete_newest_date_is_not_admitted(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_03": _incomplete()})

    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.02"}
    start = {"date_partition": "2024.01.02"}
    assert connector.latest_offset("trades", {}, start) == start


def test_newest_date_is_admitted_once_it_is_complete(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch, **{"2024_01_03": _incomplete()})
    start = connector.latest_offset("trades", {})

    shapes.by_date["2024.01.03"] = _complete(5)

    assert connector.latest_offset("trades", {}, start) == {"date_partition": "2024.01.03"}
    assert connector.get_partitions("trades", {}, start, {"date_partition": "2024.01.03"}) == (
        _ranges("2024.01.03", 5, DEFAULT_MAX_ROWS_PER_TASK)
    )


def test_incomplete_older_date_is_never_skipped(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_02": _incomplete("is missing column files ['price']")})

    # The dates before it are committed first, then the stream fails on it.
    first = connector.latest_offset("trades", {})
    assert first == {"date_partition": "2024.01.01"}
    with pytest.raises(RuntimeError, match=r"2024\.01\.02 is incomplete.*price") as excinfo:
        connector.latest_offset("trades", {}, first)
    assert "missing_columns" in str(excinfo.value)
    assert "start_date" not in str(excinfo.value)


def test_batch_read_fails_on_an_incomplete_older_date(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_02": _incomplete()})

    with pytest.raises(RuntimeError, match="incomplete"):
        connector.get_partitions("trades", {})


def test_end_date_makes_every_selected_date_an_older_date(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_02": _incomplete()})
    options = {"end_date": "2024.01.02"}

    first = connector.latest_offset("trades", options)
    assert first == {"date_partition": "2024.01.01"}
    with pytest.raises(RuntimeError, match="incomplete"):
        connector.latest_offset("trades", options, first)


def test_missing_columns_null_admits_older_dates_but_not_the_newest(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch)
    options = {"missing_columns": "null"}

    connector.latest_offset("trades", options)

    allowed = {call["date_partition"]: call["allow_missing_columns"] for call in shapes.calls}
    assert allowed == {"2024.01.01": True, "2024.01.02": True, "2024.01.03": False}


def test_missing_columns_defaults_to_fail(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch)

    connector.latest_offset("trades", {})

    assert {call["allow_missing_columns"] for call in shapes.calls} == {False}


def test_unknown_missing_columns_policy_is_rejected(tmp_path):
    connector = _connector(_build_hdb(tmp_path))

    with pytest.raises(ValueError, match="missing_columns"):
        connector.read_table_metadata("trades", {"missing_columns": "skip"})


def test_only_the_newest_date_counts_every_column_but_every_date_checks_age(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch)

    connector.latest_offset("trades", {"min_partition_age_minutes": "2"})

    by_date = {call["date_partition"]: call for call in shapes.calls}
    assert by_date["2024.01.01"]["check_all_columns"] is False
    assert by_date["2024.01.03"]["check_all_columns"] is True
    assert {call["min_age_seconds"] for call in shapes.calls} == {120}


def test_a_recently_modified_older_date_waits_instead_of_failing(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(
        monkeypatch,
        **{"2024_01_02": PartitionShape(present=True, complete=False, recent=True, reason="new")},
    )

    options = {"min_partition_age_minutes": "30"}
    assert connector.latest_offset("trades", options) == {"date_partition": "2024.01.01"}


def test_minimum_age_is_validated_even_when_the_newest_date_is_excluded(tmp_path):
    connector = _connector(_build_hdb(tmp_path))

    with pytest.raises(ValueError, match="min_partition_age_minutes"):
        connector.latest_offset(
            "trades", {"end_date": "2024.01.01", "min_partition_age_minutes": "soon"}
        )


def test_older_dates_use_the_header_reader_and_the_newest_uses_q(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch)

    connector.latest_offset("trades", {})

    via = {call["date_partition"]: call["via"] for call in shapes.calls}
    assert via == {"2024.01.01": "probe", "2024.01.02": "probe", "2024.01.03": "q"}


def test_dates_the_header_reader_cannot_decide_fall_back_to_q(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch, undecided={"2024.01.02"})

    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.03"}

    calls = [(call["date_partition"], call["via"]) for call in shapes.calls]
    assert ("2024.01.02", "probe-undecided") in calls
    assert ("2024.01.02", "q") in calls


def test_only_dates_within_the_batch_cap_are_probed(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch)

    connector.latest_offset("trades", {"max_dates_per_batch": "1"})

    assert [call["date_partition"] for call in shapes.calls] == ["2024.01.01"]


def test_complete_older_dates_are_probed_once(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch)

    connector.latest_offset("trades", {})
    connector.latest_offset("trades", {})

    probed = [call["date_partition"] for call in shapes.calls]
    assert probed.count("2024.01.01") == 1
    assert probed.count("2024.01.03") == 2


def test_max_dates_per_batch_caps_admission(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch)
    options = {"max_dates_per_batch": "2"}

    first = connector.latest_offset("trades", options)
    second = connector.latest_offset("trades", options, first)
    third = connector.latest_offset("trades", options, second)

    assert first == {"date_partition": "2024.01.02"}
    assert second == {"date_partition": "2024.01.03"}
    assert third == second


def test_max_tasks_per_batch_caps_admission(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_01": _complete(4), "2024_01_02": _complete(4)})
    options = {"max_rows_per_task": "2", "max_tasks_per_batch": "3"}

    assert connector.latest_offset("trades", options) == {"date_partition": "2024.01.01"}


def test_a_date_larger_than_the_task_cap_is_still_admitted(monkeypatch, tmp_path, caplog):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_01": _complete(10)})
    options = {"max_rows_per_task": "2", "max_tasks_per_batch": "3"}

    with caplog.at_level("WARNING"):
        assert connector.latest_offset("trades", options) == {"date_partition": "2024.01.01"}
    assert "max_tasks_per_batch" in caplog.text


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("max_dates_per_batch", "0"),
        ("max_tasks_per_batch", "-1"),
        ("max_rows_per_task", "many"),
        ("min_partition_age_minutes", "-5"),
        ("min_partition_age_minutes", "nan"),
    ],
)
def test_invalid_batch_options_are_rejected(monkeypatch, tmp_path, key, value):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch)

    with pytest.raises(ValueError, match=key):
        connector.latest_offset("trades", {key: value})


def test_sparse_interior_date_is_admitted_without_tasks(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_02": PartitionShape(present=False, complete=True)})

    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.03"}
    assert [p["date_partition"] for p in connector.get_partitions("trades", {})] == [
        "2024.01.01",
        "2024.01.03",
    ]


def test_a_sparse_date_is_probed_again_in_case_its_table_appears(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch, **{"2024_01_02": PartitionShape(present=False, complete=True)})
    start = {"date_partition": "2024.01.01"}
    end = connector.latest_offset("trades", {"max_dates_per_batch": "1"}, start)
    assert end == {"date_partition": "2024.01.02"}

    shapes.by_date["2024.01.02"] = _complete(4)

    assert connector.get_partitions("trades", {}, start, end) == _ranges(
        "2024.01.02", 4, DEFAULT_MAX_ROWS_PER_TASK
    )


# ---------------------------------------------------------------------------
# Planning: get_partitions
# ---------------------------------------------------------------------------


def test_get_partitions_plans_row_ranges_for_the_offset_range(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_02": _complete(5), "2024_01_03": _complete(2)})

    partitions = connector.get_partitions(
        "trades",
        {"max_rows_per_task": "2"},
        {"date_partition": "2024.01.01"},
        {"date_partition": "2024.01.03"},
    )

    assert partitions == _ranges("2024.01.02", 5, 2) + _ranges("2024.01.03", 2, 2)


def test_get_partitions_skips_empty_dates(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_02": _complete(0)})

    partitions = connector.get_partitions(
        "trades", {}, {"date_partition": "2024.01.01"}, {"date_partition": "2024.01.03"}
    )

    assert partitions == _ranges("2024.01.03", 3, DEFAULT_MAX_ROWS_PER_TASK)


def test_get_partitions_returns_nothing_for_equal_offsets(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch)
    offset = {"date_partition": "2024.01.03"}

    assert connector.get_partitions("trades", {}, offset, offset) == []


def test_batch_read_excludes_an_incomplete_newest_date(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch, **{"2024_01_03": _incomplete()})

    dates = {p["date_partition"] for p in connector.get_partitions("trades", {})}

    assert dates == {"2024.01.01", "2024.01.02"}


def test_batch_read_is_not_capped(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch)

    partitions = connector.get_partitions("trades", {"max_dates_per_batch": "1"})

    assert {p["date_partition"] for p in partitions} == set(_DATES)


def test_planned_date_that_became_incomplete_fails_the_batch(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    shapes = _Shapes(monkeypatch)
    end = connector.latest_offset("trades", {})
    shapes.by_date["2024.01.03"] = _incomplete()

    with pytest.raises(RuntimeError, match="no longer complete"):
        connector.get_partitions("trades", {}, {}, end)


def test_read_table_returns_one_capped_batch(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch)
    read = []
    monkeypatch.setattr(
        f"{_MODULE}.infer_schema_from_partition",
        lambda **_: [{"name": "date", "spark_type": "StringType"}],
    )
    monkeypatch.setattr(
        f"{_MODULE}.read_kdb_row_range",
        lambda **kwargs: read.append(kwargs["date_partition"]) or iter([{"date": "x"}]),
    )
    options = {"max_dates_per_batch": "2"}

    records, offset = connector.read_table("trades", {}, options)
    assert len(list(records)) == 2
    assert offset == {"date_partition": "2024.01.02"}
    assert read == ["2024.01.01", "2024.01.02"]

    records, offset = connector.read_table("trades", offset, options)
    assert len(list(records)) == 1
    records, final = connector.read_table("trades", offset, options)
    assert list(records) == []
    assert final == offset


def test_read_table_without_an_offset_reads_every_date(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _Shapes(monkeypatch)
    monkeypatch.setattr(
        f"{_MODULE}.infer_schema_from_partition",
        lambda **_: [{"name": "date", "spark_type": "StringType"}],
    )
    monkeypatch.setattr(
        f"{_MODULE}.read_kdb_row_range",
        lambda **kwargs: iter([{"date": kwargs["date_partition"]}]),
    )

    records, offset = connector.read_table("trades", None, {"max_dates_per_batch": "1"})

    assert [record["date"] for record in records] == list(_DATES)
    assert offset == {"date_partition": "2024.01.03"}


# ---------------------------------------------------------------------------
# Table options and reads
# ---------------------------------------------------------------------------


def test_read_table_metadata_defaults_to_append(tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    assert connector.read_table_metadata("trades", {}) == {
        "primary_keys": None,
        "cursor_field": None,
        "ingestion_type": "append",
    }


def test_read_table_metadata_rejects_snapshot(tmp_path):
    connector = _connector(_build_hdb(tmp_path))

    with pytest.raises(ValueError, match="append only"):
        connector.read_table_metadata("trades", {"ingestion_mode": "snapshot"})


@pytest.mark.parametrize("option", ["partition_strategy", "sym_column"])
def test_removed_sym_planning_options_are_rejected(tmp_path, option):
    connector = _connector(_build_hdb(tmp_path))

    with pytest.raises(ValueError, match="no longer supported"):
        connector.read_table_metadata("trades", {option: "date_sym"})


def test_get_table_schema_rejects_snapshot(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))

    monkeypatch.setattr(
        f"{_MODULE}.infer_schema_from_partition",
        lambda **_: [
            {"name": "date", "spark_type": "StringType"},
            {"name": "price", "spark_type": "DoubleType"},
        ],
    )

    with pytest.raises(ValueError, match="append only"):
        connector.get_table_schema("trades", {"ingestion_mode": "snapshot"})


def test_read_partition_rejects_descriptor_without_row_range(tmp_path):
    connector = _connector(_build_hdb(tmp_path))

    with pytest.raises(ValueError, match="row_offset"):
        connector.read_partition("trades", {"date_partition": "2024.01.01"}, {})


def test_read_partition_routes_row_range_descriptor(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    captured = {}

    monkeypatch.setattr(
        f"{_MODULE}.infer_schema_from_partition",
        lambda **_: [
            {"name": "date", "spark_type": "StringType"},
            {"name": "sym", "spark_type": "StringType", "q_type": "s"},
            {"name": "price", "spark_type": "DoubleType", "q_type": "f"},
        ],
    )

    def fake_reader(**kwargs):
        captured.update(kwargs)
        return iter([{"date": "2024.01.01", "sym": "a", "price": 1.23}])

    monkeypatch.setattr(f"{_MODULE}.read_kdb_row_range", fake_reader)

    records = list(
        connector.read_partition(
            "trades",
            {"date_partition": "2024.01.01", "row_offset": 4, "row_count": 2, "total_rows": 6},
            {},
        )
    )

    assert records == [{"date": "2024.01.01", "sym": "a", "price": 1.23}]
    assert captured["kdb_table_name"] == "TRADES"
    assert captured["date_partition"] == "2024.01.01"
    assert (captured["row_offset"], captured["row_count"], captured["total_rows"]) == (4, 2, 6)
    assert captured["allow_missing_columns"] is False

    list(
        connector.read_partition(
            "trades",
            {"date_partition": "2024.01.01", "row_offset": 0, "row_count": 1, "total_rows": 6},
            {"missing_columns": "null"},
        )
    )
    assert captured["allow_missing_columns"] is True
