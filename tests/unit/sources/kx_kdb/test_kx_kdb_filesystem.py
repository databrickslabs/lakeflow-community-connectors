"""Tests for filesystem discovery in the KX KDB Lakeflow connector."""

import os
import pickle

import pytest

from databricks.labs.community_connector.sources.kx_kdb import filesystem
from databricks.labs.community_connector.sources.kx_kdb.filesystem import (
    clear_discovery_cache,
    discover_table_partitions,
    discover_tables,
    normalize_partition_date,
    resolve_table_directory_name,
)


@pytest.fixture(autouse=True)
def _reset_discovery_cache():
    clear_discovery_cache()
    yield
    clear_discovery_cache()


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


def test_discovery_uses_strict_date_names_and_listing_entry_types(tmp_path):
    hdb_root = _build_hdb(tmp_path)
    (hdb_root / "2024.01.04").write_text("date-like marker")
    (hdb_root / "2024.13.45").mkdir()
    (hdb_root / "not-a-date" / "TRADES").mkdir(parents=True)

    assert filesystem._scan_date_partitions(str(hdb_root)) == [
        "2024.01.01",
        "2024.01.02",
        "2024.01.03",
    ]


def test_discovery_includes_sparse_interior_dates_but_bounds_edges(tmp_path):
    (tmp_path / "sym").write_text("")
    (tmp_path / "2024.01.01" / "TRADES").mkdir(parents=True)
    (tmp_path / "2024.01.02").mkdir()
    (tmp_path / "2024.01.03" / "TRADES").mkdir(parents=True)
    (tmp_path / "2024.01.04").mkdir()

    assert discover_table_partitions(str(tmp_path), "trades") == [
        "2024.01.01",
        "2024.01.02",
        "2024.01.03",
    ]


def test_discovery_probes_only_until_first_and_latest_table(monkeypatch, tmp_path):
    (tmp_path / "sym").write_text("")
    for day in range(1, 7):
        (tmp_path / f"2024.01.{day:02d}").mkdir()
    (tmp_path / "2024.01.02" / "TRADES").mkdir()
    (tmp_path / "2024.01.05" / "TRADES").mkdir()
    probes = []
    original_exists = filesystem._table_directory_exists

    def tracked_exists(root_path, date_partition, actual_name):
        probes.append(date_partition)
        return original_exists(root_path, date_partition, actual_name)

    monkeypatch.setattr(filesystem, "_table_directory_exists", tracked_exists)

    assert discover_table_partitions(str(tmp_path), "trades") == [
        "2024.01.02",
        "2024.01.03",
        "2024.01.04",
        "2024.01.05",
    ]
    assert probes == ["2024.01.06", "2024.01.05"]


def test_discovery_snapshot_is_shared_across_functions(monkeypatch, tmp_path):
    hdb_root = _build_hdb(tmp_path)
    calls = {"dates": 0, "table": 0}
    original_dates = filesystem._scan_date_partitions
    original_table = filesystem._find_table_boundaries

    def tracked_dates(root_path):
        calls["dates"] += 1
        return original_dates(root_path)

    def tracked_table(root_path, date_partitions, table_name):
        calls["table"] += 1
        return original_table(root_path, date_partitions, table_name)

    monkeypatch.setattr(filesystem, "_scan_date_partitions", tracked_dates)
    monkeypatch.setattr(filesystem, "_find_table_boundaries", tracked_table)

    assert resolve_table_directory_name(str(hdb_root), "trades") == "TRADES"
    assert discover_table_partitions(str(hdb_root), "TRADES") == [
        "2024.01.01",
        "2024.01.02",
        "2024.01.03",
    ]
    assert resolve_table_directory_name(str(hdb_root), "TRADES") == "TRADES"
    assert calls == {"dates": 1, "table": 1}


def test_discovery_cache_serializes_without_driver_metadata(tmp_path):
    hdb_root = _build_hdb(tmp_path)
    discover_table_partitions(str(hdb_root), "trades")

    restored = pickle.loads(pickle.dumps(filesystem._DISCOVERY_CACHE))

    assert restored._roots == {}
    assert restored._tables == {}
    assert restored._table_lists == {}
    with restored._lock:
        pass


def test_expired_table_entry_is_rebuilt_from_a_fresh_root_listing(monkeypatch, tmp_path):
    hdb_root = _build_hdb(tmp_path)
    scans = []
    original_dates = filesystem._scan_date_partitions
    monkeypatch.setattr(
        filesystem,
        "_scan_date_partitions",
        lambda root_path: scans.append(root_path) or original_dates(root_path),
    )
    assert discover_table_partitions(str(hdb_root), "trades")[-1] == "2024.01.03"

    (hdb_root / "2024.01.04" / "TRADES").mkdir(parents=True)
    with filesystem._DISCOVERY_CACHE._lock:
        table_key = (str(hdb_root), "trades")
        entry = filesystem._DISCOVERY_CACHE._tables[table_key]
        expired = entry.created_at - filesystem._DISCOVERY_CACHE_TTL_SECONDS - 1
        filesystem._DISCOVERY_CACHE._tables[table_key] = filesystem._TableDiscovery(
            created_at=expired,
            actual_name=entry.actual_name,
            first_partition=entry.first_partition,
            latest_partition=entry.latest_partition,
            partitions=entry.partitions,
        )

    assert discover_table_partitions(str(hdb_root), "trades")[-1] == "2024.01.04"
    assert len(scans) == 2


def test_latest_table_partition_ignores_later_dates_without_the_table(tmp_path):
    hdb_root = _build_hdb(tmp_path)
    (hdb_root / "2024.01.04" / "QUOTES").mkdir(parents=True)

    assert filesystem.latest_table_partition(str(hdb_root), "trades") == "2024.01.03"
    assert filesystem.latest_table_partition(str(hdb_root), "quotes") == "2024.01.04"


def test_partition_last_modified_is_the_newest_file_time(tmp_path):
    partition = tmp_path / "2024.01.01" / "TRADES"
    partition.mkdir(parents=True)
    for name, mtime in (("sym", 1_000), ("price", 2_000), (".d", 1_500)):
        (partition / name).write_bytes(b"stub")
        os.utime(partition / name, (mtime, mtime))

    entries = filesystem.scan_partition_table(str(tmp_path), "2024.01.01", "TRADES")

    assert filesystem.partition_last_modified(entries) == 2_000
    assert filesystem.partition_last_modified({}) == 0.0
