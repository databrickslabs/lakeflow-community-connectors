"""Value, timestamp, and response-shape helpers shared by the Egnyte readers.

Pure functions only — nothing here touches the network or connector state, so
they are equally usable from the driver and from an executor.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Sequence

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"
DATE_FMT = "%Y-%m-%d"
EPOCH_ISO = "1970-01-01T00:00:00Z"
# A resumed cursor at or below this is "never read anything yet", which the
# range readers serve with one open-ended partition instead of ~55 years of
# empty windows.
FIRST_RUN_THRESHOLD_ISO = "1971-01-01T00:00:00Z"


def records_from(body: dict, candidate_keys: tuple[str, ...]) -> list:
    """Pull the record array out of a response whose wrapper key varies.

    The v2 links list's pagination-metadata field names are UNVERIFIED in the
    API doc, so the wrapper key is probed rather than assumed. A bare JSON
    array is also accepted (``EgnyteClient`` parks one under ``_list``).
    """
    for key in candidate_keys:
        value = body.get(key)
        if isinstance(value, list):
            return value
    value = body.get("_list")
    return value if isinstance(value, list) else []


def csv_option(raw: str | None, default: Sequence[str]) -> list[str]:
    """Split a comma-separated table option into a list, or fall back."""
    if raw is None or not raw.strip():
        return list(default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def parse_bool(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def parse_int(raw: Any, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value >= minimum else default


def parse_float(raw: Any, default: float) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


def parse_optional_int(raw: Any) -> int | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 timestamp into an aware UTC datetime.

    Tolerates the ``Z`` suffix, an explicit offset, a naive value (assumed
    UTC), and a bare date — the doc warns that timestamp spellings differ
    between Egnyte endpoints.
    """
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Cannot parse {value!r} as an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_optional_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return parse_iso(value)
    except ValueError:
        return None


def format_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(ISO_FMT)


def api_timestamp(value: datetime) -> str:
    """Render a timestamp for Egnyte's v2 query params.

    Uses the explicit ``+00:00`` offset form the links API documents. Passing
    it through requests' ``params`` percent-encodes the ``+`` as ``%2B``,
    which is exactly the encoding the doc calls out as a gotcha.
    """
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def parse_date(value: str) -> date:
    return datetime.strptime(str(value).strip()[:10], DATE_FMT).date()


def format_date(value: date) -> str:
    return value.strftime(DATE_FMT)


def parent_path_of(path: str) -> str:
    """Derive a folder's parent path from its own canonical path."""
    trimmed = (path or "/").rstrip("/")
    if "/" not in trimmed[1:]:
        return "/"
    return trimmed.rsplit("/", 1)[0] or "/"
