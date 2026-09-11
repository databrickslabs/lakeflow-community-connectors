"""Utility functions for the GitHub connector.

This module contains helper functions for pagination, link header parsing,
and common option parsing used across the GitHub connector.
"""

import base64
import binascii
import fnmatch
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class PaginationOptions:
    """Configuration options for GitHub API pagination."""

    per_page: int
    lookback_seconds: int
    max_records_per_batch: int | None


def parse_pagination_options(
    table_options: dict[str, str],
    default_per_page: int = 100,
    default_lookback: int = 300,
) -> PaginationOptions:
    """
    Parse common pagination options from table_options.

    Args:
        table_options: Dictionary of table-level configuration options.
        default_per_page: Default page size (max 100 for GitHub API).
        default_lookback: Default lookback window in seconds.

    Returns:
        PaginationOptions with parsed values.
    """
    try:
        per_page = int(table_options.get("per_page", default_per_page))
    except (TypeError, ValueError):
        per_page = default_per_page
    per_page = max(1, min(per_page, 100))

    try:
        lookback_seconds = int(table_options.get("lookback_seconds", default_lookback))
    except (TypeError, ValueError):
        lookback_seconds = default_lookback

    max_records_per_batch: int | None = None
    raw_max_records = table_options.get("max_records_per_batch")
    if raw_max_records is not None:
        try:
            max_records_per_batch = int(raw_max_records)
        except (TypeError, ValueError):
            pass

    return PaginationOptions(
        per_page=per_page,
        lookback_seconds=lookback_seconds,
        max_records_per_batch=max_records_per_batch,
    )


def extract_next_link(link_header: str | None) -> str | None:
    """
    Parse the GitHub Link header to extract the URL with rel="next".

    The GitHub API uses Link headers for pagination following RFC 5988.
    Format: <url>; rel="next", <url>; rel="last", ...

    Args:
        link_header: The value of the Link header from a GitHub API response.

    Returns:
        The URL for the next page, or None if not found.
    """
    if not link_header:
        return None

    parts = link_header.split(",")
    for part in parts:
        section = part.strip()
        if 'rel="next"' in section:
            # Format: <url>; rel="next"
            start = section.find("<")
            end = section.find(">", start + 1)
            if start != -1 and end != -1:
                return section[start + 1 : end]
    return None


def compute_next_cursor(
    max_timestamp: str | None,
    current_cursor: str | None,
) -> str | None:
    """
    Return the next cursor value to checkpoint.

    The offset stores the raw max observed timestamp so that progress is never
    lost. Lookback is applied separately at read time via ``apply_lookback``.

    Args:
        max_timestamp: The maximum observed timestamp in ISO 8601 format.
        current_cursor: The current cursor value (fallback when no data found).

    Returns:
        max_timestamp if available, otherwise current_cursor.
    """
    return max_timestamp if max_timestamp else current_cursor


def apply_lookback(
    cursor: str | None,
    lookback_seconds: int,
    timestamp_format: str = "%Y-%m-%dT%H:%M:%SZ",
) -> str | None:
    """
    Subtract a lookback window from a cursor timestamp.

    Used at read time to widen the ``since`` filter so that records updated
    concurrently during the previous batch are not missed.

    Args:
        cursor: ISO 8601 timestamp string to adjust.
        lookback_seconds: Seconds to subtract from cursor.
        timestamp_format: The format of the timestamp string.

    Returns:
        The adjusted timestamp, or the original cursor if parsing fails or
        cursor is None.
    """
    if not cursor or lookback_seconds <= 0:
        return cursor

    try:
        dt = datetime.strptime(cursor, timestamp_format)
        return (dt - timedelta(seconds=lookback_seconds)).strftime(timestamp_format)
    except (ValueError, TypeError):
        return cursor


def get_cursor_from_offset(
    start_offset: dict | None, table_options: dict[str, str]
) -> str | None:
    """
    Extract the cursor value from start_offset or fall back to table_options.

    Args:
        start_offset: The offset dictionary from a previous read.
        table_options: Table-level configuration options.

    Returns:
        The cursor value, or None if not found.
    """
    cursor = None
    if start_offset and isinstance(start_offset, dict):
        cursor = start_offset.get("cursor")
    if not cursor:
        cursor = table_options.get("start_date")
    return cursor


def _split_csv(value: str | None) -> list[str]:
    """Split a comma-separated option string into a list of trimmed tokens.

    Returns an empty list for ``None`` or blank input.
    """
    if not value:
        return []
    return [token.strip() for token in value.split(",") if token.strip()]


@dataclass
class FileFilter:
    """Parsed include/exclude rules for the ``repository_files`` table."""

    extensions: list[str]
    include_globs: list[str]
    exclude_globs: list[str]


def parse_file_filter(table_options: dict[str, str]) -> FileFilter:
    """Parse file-selection options for the ``repository_files`` table.

    Options (all optional, comma-separated):
        - include_extensions: e.g. ``"py,java,ts"`` (leading dot optional).
        - include_globs: e.g. ``"src/**,lib/**"`` (fnmatch semantics).
        - exclude_globs: e.g. ``"**/vendor/**,*.min.js"``.
    """
    extensions = []
    for ext in _split_csv(table_options.get("include_extensions")):
        extensions.append(ext if ext.startswith(".") else f".{ext}")

    return FileFilter(
        extensions=extensions,
        include_globs=_split_csv(table_options.get("include_globs")),
        exclude_globs=_split_csv(table_options.get("exclude_globs")),
    )


def should_include_file(path: str, file_filter: FileFilter) -> bool:
    """Decide whether a file path passes the include/exclude filter.

    Rules, in order:
        1. Any matching ``exclude_globs`` pattern rejects the file.
        2. If neither ``extensions`` nor ``include_globs`` is set, the file is
           included (subject only to excludes).
        3. Otherwise the file is included when it matches an allowed extension
           OR an ``include_globs`` pattern.

    Uses ``fnmatch`` semantics, where ``*`` also matches ``/``.
    """
    for pattern in file_filter.exclude_globs:
        if fnmatch.fnmatch(path, pattern):
            return False

    if not file_filter.extensions and not file_filter.include_globs:
        return True

    if any(path.endswith(ext) for ext in file_filter.extensions):
        return True
    return any(fnmatch.fnmatch(path, pattern) for pattern in file_filter.include_globs)


def decode_blob_content(
    raw_content: str | None, encoding: str | None, include_binary: bool
) -> tuple[str | None, str | None, str | None, bool]:
    """Decode a GitHub blob payload into text or (optionally) base64 bytes.

    GitHub returns blob content base64-encoded. This decodes it and detects
    binary files by attempting a strict UTF-8 decode.

    Returns a tuple of ``(content, content_base64, encoding, is_binary)``:
        - Text file: ``(text, None, "utf-8", False)``.
        - Binary file with ``include_binary=True``: ``(None, base64, "base64", True)``.
        - Binary file with ``include_binary=False``: ``(None, None, "base64", True)``.
    """
    if encoding != "base64" or raw_content is None:
        # Unexpected shape; surface the raw value as text without guessing.
        return raw_content, None, encoding, False

    try:
        raw_bytes = base64.b64decode(raw_content)
    except (binascii.Error, ValueError):
        return None, None, "base64", True

    try:
        return raw_bytes.decode("utf-8"), None, "utf-8", False
    except UnicodeDecodeError:
        if include_binary:
            return None, base64.b64encode(raw_bytes).decode("ascii"), "base64", True
        return None, None, "base64", True


def require_owner_repo(
    table_options: dict[str, str], table_name: str
) -> tuple[str, str]:
    """
    Validate and extract owner and repo from table_options.

    Args:
        table_options: Table-level configuration options.
        table_name: Name of the table (for error message).

    Returns:
        Tuple of (owner, repo).

    Raises:
        ValueError: If owner or repo is missing or empty.
    """
    owner = table_options.get("owner")
    repo = table_options.get("repo")
    if not owner or not repo:
        raise ValueError(
            f"table_configuration for '{table_name}' must include "
            f"non-empty 'owner' and 'repo'"
        )
    return owner, repo
