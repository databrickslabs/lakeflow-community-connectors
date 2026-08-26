"""Tests for the KX KDB Lakeflow connector class."""

import json
from pathlib import Path

import pytest
from pyspark import cloudpickle

from databricks.labs.community_connector.sources.kx_kdb._generated_kx_kdb_python_source import (
    register_lakeflow_source,
)
from databricks.labs.community_connector.sources.kx_kdb.kx_kdb import KxKdbLakeflowConnect


def _build_hdb(tmp_path):
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    (tmp_path / "2024.01.02" / "TRADES").mkdir(parents=True)
    (tmp_path / "2024.01.03" / "TRADES").mkdir(parents=True)
    return tmp_path


def _connector(tmp_path):
    return KxKdbLakeflowConnect(
        {
            "hdb_root_path": str(tmp_path),
            "license_volume_path": "/Volumes/main/default/keys",
        }
    )


def _patch_symbols(monkeypatch, symbols=None):
    symbols = symbols or ["a", "b"]
    indices = {symbol: index for index, symbol in enumerate(symbols)}
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.load_sym_enumeration_with_indices",
        lambda *_: (symbols, indices),
    )


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

    assert "global _RUNTIME_HOME_CACHE" not in generated
    assert "global _LOCAL_BUNDLE_DIR_CACHE" not in generated


def test_connector_rejects_relative_hdb_path():
    with pytest.raises(ValueError, match="absolute filesystem path"):
        KxKdbLakeflowConnect(
            {
                "hdb_root_path": "relative/hdb",
                "license_volume_path": "/Volumes/catalog/schema/volume/keys",
            }
        )


def test_latest_offset_returns_last_partition(tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    assert connector.latest_offset("trades", {}) == {"date_partition": "2024.01.03"}


def test_get_partitions_defaults_to_date_sym_incremental_range(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _patch_symbols(monkeypatch)

    partitions = connector.get_partitions(
        "trades",
        {},
        {"date_partition": "2024.01.01"},
        {"date_partition": "2024.01.03"},
    )

    assert partitions == [
        {"date_partition": "2024.01.02", "sym": "a", "sym_index": 0},
        {"date_partition": "2024.01.02", "sym": "b", "sym_index": 1},
        {"date_partition": "2024.01.03", "sym": "a", "sym_index": 0},
        {"date_partition": "2024.01.03", "sym": "b", "sym_index": 1},
    ]


def test_get_partitions_date_sym_expands_dates_and_symbols(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    _patch_symbols(monkeypatch)

    partitions = connector.get_partitions(
        "trades",
        {"partition_strategy": "date_sym"},
        {"date_partition": "2024.01.01"},
        {"date_partition": "2024.01.02"},
    )

    assert partitions == [
        {"date_partition": "2024.01.02", "sym": "a", "sym_index": 0},
        {"date_partition": "2024.01.02", "sym": "b", "sym_index": 1},
    ]


def test_get_partitions_rejects_date_strategy(tmp_path):
    connector = _connector(_build_hdb(tmp_path))

    with pytest.raises(ValueError, match="date_sym"):
        connector.get_partitions("trades", {"partition_strategy": "date"})


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


def test_get_table_schema_rejects_snapshot(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.infer_schema_from_partition",
        lambda **_: [
            {"name": "date", "spark_type": "StringType"},
            {"name": "price", "spark_type": "DoubleType"},
        ],
    )

    with pytest.raises(ValueError, match="append only"):
        connector.get_table_schema("trades", {"ingestion_mode": "snapshot"})


def test_read_partition_rejects_date_only_descriptor(tmp_path):
    connector = _connector(_build_hdb(tmp_path))

    with pytest.raises(ValueError, match="date-only"):
        connector.read_partition("trades", {"date_partition": "2024.01.01"}, {})


def test_read_partition_routes_date_sym_descriptor(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    captured = {}

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.infer_schema_from_partition",
        lambda **_: [
            {"name": "date", "spark_type": "StringType"},
            {"name": "sym", "spark_type": "StringType"},
            {"name": "price", "spark_type": "DoubleType"},
        ],
    )

    def fake_reader(**kwargs):
        captured.update(kwargs)
        return iter([{"date": "2024.01.01", "sym": "a", "price": 1.23}])

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.read_kdb_date_sym_records",
        fake_reader,
    )

    records = list(
        connector.read_partition(
            "trades",
            {"date_partition": "2024.01.01", "sym": "a", "sym_index": 0},
            {"partition_strategy": "date_sym", "partition_conversion_mode": "arrow_direct"},
        )
    )

    assert records == [{"date": "2024.01.01", "sym": "a", "price": 1.23}]
    assert captured["kdb_table_name"] == "TRADES"
    assert captured["date_partition"] == "2024.01.01"
    assert captured["symbol"] == "a"
    assert captured["sym_index"] == 0
    assert captured["conversion_mode"] == "arrow_direct"
    assert captured["sym_column"] == "sym"


def test_read_partition_passes_custom_sym_column(monkeypatch, tmp_path):
    connector = _connector(_build_hdb(tmp_path))
    captured = {}

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.infer_schema_from_partition",
        lambda **_: [
            {"name": "date", "spark_type": "StringType"},
            {"name": "optionId", "spark_type": "StringType"},
            {"name": "price", "spark_type": "DoubleType"},
        ],
    )

    def fake_reader(**kwargs):
        captured.update(kwargs)
        return iter([{"date": "2024.01.01", "optionId": "a", "price": 1.23}])

    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.kx_kdb.kx_kdb.read_kdb_date_sym_records",
        fake_reader,
    )

    list(
        connector.read_partition(
            "trades",
            {"date_partition": "2024.01.01", "sym": "a", "sym_index": 0},
            {"sym_column": "optionId"},
        )
    )

    assert captured["sym_column"] == "optionId"


def test_connector_normalizes_license_file_path(tmp_path):
    connector = KxKdbLakeflowConnect(
        {
            "hdb_root_path": str(_build_hdb(tmp_path)),
            "license_volume_path": "/Volumes/main/rkh/rbc_kx/key/kc.lic",
        }
    )
    assert connector.license_path == "/Volumes/main/rkh/rbc_kx/key"


def test_connector_uses_table_configs_for_metadata_reader_required_options(tmp_path):
    hdb_root = _build_hdb(tmp_path)
    table_configs = {
        "trades": {
            "hdb_root_path": str(hdb_root),
            "license_volume_path": "/Volumes/main/default/keys",
            "start_date": "2024.01.01",
        }
    }

    connector = KxKdbLakeflowConnect(
        {
            "tableName": "_lakeflow_metadata",
            "tableNameList": "trades",
            "tableConfigs": json.dumps(table_configs),
            "hdb_root_path": "/stale/hdb",
            "license_volume_path": "/stale/license",
        }
    )

    assert connector.hdb_root_path == str(hdb_root)
    assert connector.license_path == "/Volumes/main/default/keys"
