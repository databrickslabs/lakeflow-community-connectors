"""Filesystem helpers for KX KDB HDB discovery."""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path

DATE_PARTITION_RE = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
_DISCOVERY_CACHE_TTL_SECONDS = 3600

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _RootDiscovery:
    created_at: float
    root_path: str
    date_partitions: tuple[str, ...]


@dataclass(frozen=True)
class _TableDiscovery:
    created_at: float
    actual_name: str
    first_partition: str
    latest_partition: str
    partitions: tuple[str, ...]


@dataclass(frozen=True)
class _TablesDiscovery:
    created_at: float
    tables: tuple[str, ...]


class _DiscoveryCache:
    """Process-shared FUSE metadata cache that serializes as empty."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._roots: dict[str, _RootDiscovery] = {}
        self._tables: dict[tuple[str, str], _TableDiscovery] = {}
        self._table_lists: dict[tuple[str, int], _TablesDiscovery] = {}

    def __getstate__(self) -> dict:
        return {}

    def __setstate__(self, _state: dict) -> None:
        self.__init__()

    def clear(self) -> None:
        with self._lock:
            self._roots.clear()
            self._tables.clear()
            self._table_lists.clear()

    @staticmethod
    def _is_fresh(created_at: float) -> bool:
        return time.monotonic() - created_at < _DISCOVERY_CACHE_TTL_SECONDS

    def root(self, root_path: str) -> _RootDiscovery:
        normalized_root = str(root_path).rstrip("/")
        with self._lock:
            cached = self._roots.get(normalized_root)
            if cached is not None and self._is_fresh(cached.created_at):
                logger.warning(
                    "KX_DISCOVERY event=root_dates_cache_hit dates=%s",
                    len(cached.date_partitions),
                )
                return cached

            started = time.perf_counter()
            date_partitions = tuple(_scan_date_partitions(normalized_root))
            self._roots[normalized_root] = _RootDiscovery(
                created_at=time.monotonic(),
                root_path=normalized_root,
                date_partitions=date_partitions,
            )
            logger.warning(
                "KX_DISCOVERY event=root_dates_build duration_ms=%s dates=%s",
                round((time.perf_counter() - started) * 1000),
                len(date_partitions),
            )
            return self._roots[normalized_root]

    def date_partitions(self, root_path: str) -> tuple[str, ...]:
        return self.root(root_path).date_partitions

    def table(self, root_path: str, table_name: str) -> _TableDiscovery:
        normalized_root = str(root_path).rstrip("/")
        normalized_table = str(table_name).strip().lower()
        key = (normalized_root, normalized_table)
        with self._lock:
            cached = self._tables.get(key)
            if cached is not None and self._is_fresh(cached.created_at):
                logger.warning(
                    "KX_DISCOVERY event=table_cache_hit table=%s partitions=%s",
                    normalized_table,
                    len(cached.partitions),
                )
                return cached

            started = time.perf_counter()
            root = self.root(normalized_root)
            actual_name, first_partition, latest_partition = _find_table_boundaries(
                root.root_path,
                root.date_partitions,
                normalized_table,
            )
            partitions = _candidate_table_partitions(
                root.date_partitions,
                first_partition,
                latest_partition,
            )
            discovered = _TableDiscovery(
                created_at=time.monotonic(),
                actual_name=actual_name,
                first_partition=first_partition,
                latest_partition=latest_partition,
                partitions=partitions,
            )
            self._tables[key] = discovered
            logger.warning(
                "KX_DISCOVERY event=table_build duration_ms=%s table=%s "
                "partitions=%s first=%s latest=%s",
                round((time.perf_counter() - started) * 1000),
                normalized_table,
                len(partitions),
                first_partition,
                latest_partition,
            )
            return discovered

    def list_tables(self, root_path: str, sample_dates: int) -> tuple[str, ...]:
        normalized_root = str(root_path).rstrip("/")
        normalized_sample = max(int(sample_dates or 0), 0)
        key = (normalized_root, normalized_sample)
        with self._lock:
            cached = self._table_lists.get(key)
            if cached is not None and self._is_fresh(cached.created_at):
                logger.warning(
                    "KX_DISCOVERY event=table_list_cache_hit tables=%s",
                    len(cached.tables),
                )
                return cached.tables

            started = time.perf_counter()
            root = self.root(normalized_root)
            date_partitions = root.date_partitions
            if normalized_sample:
                date_partitions = date_partitions[:normalized_sample]
            table_names: set[str] = set()
            for date_partition in date_partitions:
                for child_name in _scan_child_directories(
                    hdb_child_path(root.root_path, date_partition)
                ):
                    table_names.add(child_name.lower())
            tables = tuple(sorted(table_names))
            self._table_lists[key] = _TablesDiscovery(
                created_at=time.monotonic(),
                tables=tables,
            )
            logger.warning(
                "KX_DISCOVERY event=table_list_build duration_ms=%s tables=%s",
                round((time.perf_counter() - started) * 1000),
                len(tables),
            )
            return tables


_DISCOVERY_CACHE = _DiscoveryCache()


def hdb_child_path(root_path: str, *parts: str) -> str:
    """Build a child path below a FUSE-mounted HDB root."""
    result = str(root_path or "").strip().rstrip("/")
    for part in parts:
        value = str(part or "").strip().strip("/")
        if value:
            result = f"{result}/{value}" if result else value
    return result


def normalize_partition_date(raw_value: str | None) -> str | None:
    """Normalize supported date formats to KDB's YYYY.MM.DD partition format."""
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    if DATE_PARTITION_RE.match(value):
        return value
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return value.replace("-", ".")
    if re.match(r"^\d{4}/\d{2}/\d{2}$", value):
        return value.replace("/", ".")
    return value


def _scan_date_partitions(root_path: str) -> list[str]:
    if "://" in str(root_path):
        raise ValueError(
            f"HDB root path must be a FUSE/local directory, not a URI: {root_path}"
        )

    try:
        date_partitions = [
            entry.name
            for entry in os.scandir(root_path)
            if DATE_PARTITION_RE.match(entry.name)
        ]
    except FileNotFoundError:
        raise FileNotFoundError(f"HDB root path does not exist: {root_path}") from None
    except NotADirectoryError:
        raise ValueError(f"HDB root path must be a directory: {root_path}") from None
    return sorted(date_partitions)


def _scan_child_directories(parent_path: str) -> list[str]:
    try:
        return [
            entry.name
            for entry in os.scandir(parent_path)
            if entry.is_dir(follow_symlinks=False)
        ]
    except FileNotFoundError:
        return []
    except NotADirectoryError:
        return []


def _table_directory_exists(root_path: str, date_partition: str, actual_name: str) -> bool:
    return actual_name in _scan_child_directories(hdb_child_path(root_path, date_partition))


def _find_table_boundaries(
    root_path: str,
    date_partitions: tuple[str, ...],
    table_name: str,
) -> tuple[str, str, str]:
    actual_name = ""
    first_partition = ""
    for date_partition in date_partitions:
        for child_name in _scan_child_directories(hdb_child_path(root_path, date_partition)):
            if child_name.lower() == table_name:
                actual_name = child_name
                first_partition = date_partition
                break
        if actual_name:
            break

    if not actual_name:
        raise ValueError(f"Table {table_name!r} was not found under the HDB root")

    latest_partition = first_partition
    for date_partition in reversed(date_partitions):
        if date_partition < first_partition:
            break
        if _table_directory_exists(root_path, date_partition, actual_name):
            latest_partition = date_partition
            break
    return actual_name, first_partition, latest_partition


def _candidate_table_partitions(
    date_partitions: tuple[str, ...],
    first_partition: str,
    latest_partition: str,
) -> tuple[str, ...]:
    return tuple(
        partition
        for partition in date_partitions
        if first_partition <= partition <= latest_partition
    )


def _find_actual_table_name(
    root_path: str,
    date_partitions: tuple[str, ...],
    table_name: str,
) -> str:
    actual_name, _, _ = _find_table_boundaries(root_path, date_partitions, table_name)
    return actual_name


def _scan_date_directories(root_path: str) -> list[Path]:
    return [
        Path(hdb_child_path(root_path, partition))
        for partition in _scan_date_partitions(root_path)
    ]


def _date_partitions(root_path: str) -> list[str]:
    return list(_DISCOVERY_CACHE.date_partitions(root_path))


def _date_directories(root_path: str) -> list[Path]:
    return [
        Path(hdb_child_path(root_path, partition))
        for partition in _date_partitions(root_path)
    ]


def clear_discovery_cache() -> None:
    """Clear process-shared discovery metadata, primarily for tests."""
    _DISCOVERY_CACHE.clear()


def list_date_partitions(root_path: str) -> list[str]:
    """List all date partitions found under an HDB root path."""
    return _date_partitions(root_path)


def discover_tables(root_path: str, sample_dates: int = 0) -> list[str]:
    """Discover logical table names by scanning date-partition directories."""
    return list(_DISCOVERY_CACHE.list_tables(root_path, sample_dates))


def resolve_table_directory_name(
    root_path: str,
    table_name: str,
    sample_dates: int = 0,
) -> str:
    """Resolve a logical table name to the actual directory name on disk."""
    target = str(table_name).strip().lower()
    if not target:
        raise ValueError("table_name must be non-empty")

    del sample_dates
    return _DISCOVERY_CACHE.table(root_path, target).actual_name


def discover_table_partitions(
    root_path: str,
    table_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    """Return all partitions that contain the requested table directory."""
    normalized_start = normalize_partition_date(start_date)
    normalized_end = normalize_partition_date(end_date)
    discovery = _DISCOVERY_CACHE.table(root_path, table_name)
    return [
        partition
        for partition in discovery.partitions
        if (normalized_start is None or partition >= normalized_start)
        and (normalized_end is None or partition <= normalized_end)
    ]
