"""Utility functions for the GitHub connector.

This module contains helper functions for pagination, link header parsing,
and common option parsing used across the GitHub connector.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class PaginationOptions:
    """Configuration options for GitHub API pagination."""

    per_page: int
    max_pages_per_batch: int
    lookback_seconds: int


def parse_pagination_options(
    table_options: dict[str, str],
    default_per_page: int = 100,
    default_max_pages: int = 50,
    default_lookback: int = 300,
) -> PaginationOptions:
    """
    Parse common pagination options from table_options.

    Args:
        table_options: Dictionary of table-level configuration options.
        default_per_page: Default page size (max 100 for GitHub API).
        default_max_pages: Default maximum pages per batch.
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
        max_pages_per_batch = int(
            table_options.get("max_pages_per_batch", default_max_pages)
        )
    except (TypeError, ValueError):
        max_pages_per_batch = default_max_pages

    try:
        lookback_seconds = int(table_options.get("lookback_seconds", default_lookback))
    except (TypeError, ValueError):
        lookback_seconds = default_lookback

    return PaginationOptions(
        per_page=per_page,
        max_pages_per_batch=max_pages_per_batch,
        lookback_seconds=lookback_seconds,
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
