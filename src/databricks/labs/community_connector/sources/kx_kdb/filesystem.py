"""Filesystem helpers for KX KDB HDB discovery."""

from __future__ import annotations

import re
from pathlib import Path

DATE_PARTITION_RE = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")


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


def _date_directories(root_path: str) -> list[Path]:
    if "://" in str(root_path):
        raise ValueError(
            f"HDB root path must be a FUSE/local directory, not a URI: {root_path}"
        )
    root = Path(root_path)
    if not root.exists():
        raise FileNotFoundError(f"HDB root path does not exist: {root_path}")
    if not root.is_dir():
        raise ValueError(f"HDB root path must be a directory: {root_path}")

    date_dirs = [
        child
        for child in root.iterdir()
        if child.is_dir() and DATE_PARTITION_RE.match(child.name)
    ]
    return sorted(date_dirs, key=lambda item: item.name)


def list_date_partitions(root_path: str) -> list[str]:
    """List all date partitions found under an HDB root path."""
    return [path.name for path in _date_directories(root_path)]


def discover_tables(root_path: str, sample_dates: int = 0) -> list[str]:
    """Discover logical table names by scanning date-partition directories."""
    date_dirs = _date_directories(root_path)
    if sample_dates and sample_dates > 0:
        date_dirs = date_dirs[:sample_dates]

    table_names: set[str] = set()
    for date_dir in date_dirs:
        for child in date_dir.iterdir():
            if child.is_dir():
                table_names.add(child.name.lower())
    return sorted(table_names)


def resolve_table_directory_name(
    root_path: str,
    table_name: str,
    sample_dates: int = 0,
) -> str:
    """Resolve a logical table name to the actual directory name on disk."""
    target = str(table_name).strip().lower()
    if not target:
        raise ValueError("table_name must be non-empty")

    date_dirs = _date_directories(root_path)
    if sample_dates and sample_dates > 0:
        date_dirs = date_dirs[:sample_dates]

    for date_dir in date_dirs:
        for child in date_dir.iterdir():
            if child.is_dir() and child.name.lower() == target:
                return child.name

    raise ValueError(f"Table {table_name!r} was not found under {root_path}")


def discover_table_partitions(
    root_path: str,
    table_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    """Return all partitions that contain the requested table directory."""
    normalized_start = normalize_partition_date(start_date)
    normalized_end = normalize_partition_date(end_date)
    actual_table_name = resolve_table_directory_name(root_path, table_name)

    partitions = []
    for date_dir in _date_directories(root_path):
        partition_name = date_dir.name
        if normalized_start and partition_name < normalized_start:
            continue
        if normalized_end and partition_name > normalized_end:
            continue
        if (date_dir / actual_table_name).is_dir():
            partitions.append(partition_name)

    return partitions
