"""Utility functions for the OSI PI connector.

This module contains helper functions for time parsing, type conversion,
batch request handling, and common utilities used across the OSI PI connector.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple


def utcnow() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def isoformat_z(dt: datetime) -> str:
    """Format a datetime as an ISO 8601 string with 'Z' suffix for UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value: str) -> datetime:
    """Parse an ISO 8601 timestamp string to a timezone-aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_pi_time(value: Optional[str], now: Optional[datetime] = None) -> datetime:
    """
    Parse PI Web API time expressions commonly used in query params.

    Supports:
    - "*" (now)
    - "*-10m", "*-2h", "*-7d" (relative to now)
    - ISO timestamps with or without Z suffix

    Args:
        value: The time expression to parse.
        now: Reference time for relative expressions (defaults to current UTC time).

    Returns:
        A timezone-aware datetime.
    """
    now_dt = now or utcnow()
    if value is None or value == "" or value == "*":
        return now_dt

    v = str(value).strip()
    if v.startswith("*-") and len(v) >= 4:
        num = v[2:-1]
        unit = v[-1]
        try:
            n = int(num)
            if unit == "m":
                return now_dt - timedelta(minutes=n)
            if unit == "h":
                return now_dt - timedelta(hours=n)
            if unit == "d":
                return now_dt - timedelta(days=n)
        except Exception:
            pass

    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return now_dt


def chunks(items: List[str], n: int) -> List[List[str]]:
    """Split a list into chunks of size n.

    Args:
        items: List to split.
        n: Chunk size. If <= 0, returns the original list as a single chunk.

    Returns:
        List of chunks.
    """
    if n <= 0:
        return [items]
    return [items[i : i + n] for i in range(0, len(items), n)]


def as_bool(v: Any, default: bool = False) -> bool:
    """Convert a value to boolean with flexible parsing.

    Accepts:
    - bool: returns as-is
    - int/float: returns bool(v)
    - str: parses "true", "yes", "1" as True; "false", "no", "0" as False

    Args:
        v: Value to convert.
        default: Default value if conversion fails.

    Returns:
        Boolean value.
    """
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return default


def try_float(v: Any) -> Optional[float]:
    """Try to convert a value to float.

    Args:
        v: Value to convert.

    Returns:
        Float value or None if conversion fails.
    """
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def batch_request_dict(requests_list: List[dict]) -> dict:
    """Convert a list of batch requests to the PI Web API batch request format.

    PI Web API docs define the batch request body as a dictionary keyed by ids.

    Args:
        requests_list: List of request dictionaries.

    Returns:
        Dictionary keyed by string ids (1, 2, 3, ...).
    """
    return {str(i + 1): req for i, req in enumerate(requests_list)}


def batch_response_items(resp_json: dict) -> List[Tuple[str, dict]]:
    """Normalize PI Web API batch response into a list of (request_id, response_obj).

    Handles both:
    - Official PI Web API format: dictionary keyed by request ids
    - Some mocks: {"Responses": [...]}

    Args:
        resp_json: The JSON response from a batch request.

    Returns:
        List of (request_id, response_dict) tuples, sorted by numeric id.
    """
    if not isinstance(resp_json, dict):
        return []

    # Some mocks use {"Responses": [...]}
    if "Responses" in resp_json and isinstance(resp_json.get("Responses"), list):
        return [(str(i + 1), r) for i, r in enumerate(resp_json.get("Responses") or [])]

    # Otherwise treat top-level keys as request ids
    out = []
    for k, v in resp_json.items():
        if isinstance(v, dict) and ("Status" in v or "Content" in v or "Headers" in v):
            out.append((str(k), v))

    # Preserve numeric ordering when possible
    def keyfn(x):
        try:
            return int(x[0])
        except Exception:
            return 10**18

    return sorted(out, key=keyfn)
