"""Tests for filesystem discovery in the KX KDB Lakeflow connector."""

from databricks.labs.community_connector.sources.kx_kdb.filesystem import (
    discover_table_partitions,
    discover_tables,
    normalize_partition_date,
    resolve_table_directory_name,
)


def _build_hdb(tmp_path):
    (tmp_path / "sym").write_text("")
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    (tmp_path / "2024.01.01" / "QUOTES").mkdir(parents=True)
    (tmp_path / "2024.01.02" / "TRADES").mkdir(parents=True)
    (tmp_path / "2024.01.03" / "TRADES").mkdir(parents=True)
    return tmp_path


def test_normalize_partition_date_supports_multiple_formats():
    assert normalize_partition_date("2024.01.01") == "2024.01.01"
    assert normalize_partition_date("2024-01-01") == "2024.01.01"
    assert normalize_partition_date("2024/01/01") == "2024.01.01"


def test_discover_tables_returns_logical_lowercase_names(tmp_path):
    hdb_root = _build_hdb(tmp_path)
    assert discover_tables(str(hdb_root)) == ["quotes", "trades"]


def test_resolve_table_directory_name_is_case_insensitive(tmp_path):
    hdb_root = _build_hdb(tmp_path)
    assert resolve_table_directory_name(str(hdb_root), "trades") == "TRADES"
    assert resolve_table_directory_name(str(hdb_root), "TRADES") == "TRADES"


def test_discover_table_partitions_respects_date_filters(tmp_path):
    hdb_root = _build_hdb(tmp_path)
    assert discover_table_partitions(str(hdb_root), "trades") == [
        "2024.01.01",
        "2024.01.02",
        "2024.01.03",
    ]
    assert discover_table_partitions(
        str(hdb_root), "trades", start_date="2024-01-02", end_date="2024.01.03"
    ) == ["2024.01.02", "2024.01.03"]
