"""HTTP client, pagination and record-normalisation helpers for Pipedrive."""

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional

import requests

DEFAULT_BASE_URL = "https://api.pipedrive.com"
DEFAULT_START_DATE = "2010-01-01T00:00:00Z"
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 6
INITIAL_BACKOFF_SECONDS = 2.0
MAX_BACKOFF_SECONDS = 60.0
RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# Pipedrive's burst limit is a rolling 2-second window per token.  The lowest
# plan (Lite, API token) allows 20 requests / 2 s; the plan is not knowable
# from config, so default to that floor.
BURST_WINDOW_SECONDS = 2.0
DEFAULT_BURST_LIMIT = 20

# Status codes that mean "this company has no access to the feature" for the
# optional Projects add-on tables (projects, tasks).
OPTIONAL_TABLE_SKIP_CODES = frozenset({402, 403, 415})

# Status codes for a deal that vanished / became hidden between the parent
# deals read and the per-deal flow read.
CHILD_SKIP_CODES = frozenset({403, 404, 410})

_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_CANONICAL_FMT = "%Y-%m-%dT%H:%M:%SZ"


class PipedriveAPIError(RuntimeError):
    """Raised for non-retriable / exhausted Pipedrive API errors."""

    def __init__(self, message: str, status_code: Optional[int] = None, is_json: bool = True):
        super().__init__(message)
        self.status_code = status_code
        self.is_json = is_json


# --------------------------------------------------------------------------- #
# Timestamps
# --------------------------------------------------------------------------- #


def parse_ts(value: Any) -> Optional[datetime]:
    """Parse any Pipedrive timestamp format into an aware UTC datetime.

    Handles RFC 3339 (``2025-01-01T10:20:00Z``, with or without fractional
    seconds / offsets) and v1's ``YYYY-MM-DD HH:MM:SS`` (UTC).  Returns
    ``None`` for empty or unparseable values.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            try:
                dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_ts(dt: datetime) -> str:
    """Render a datetime in the canonical cursor form ``YYYY-MM-DDTHH:MM:SSZ``."""
    return dt.astimezone(timezone.utc).strftime(_CANONICAL_FMT)


def normalize_ts(value: Any) -> Optional[str]:
    """Canonicalise a timestamp string (second precision, UTC, ``Z`` suffix)."""
    dt = parse_ts(value)
    return format_ts(dt) if dt else None


def shift_ts(value: str, seconds: int) -> str:
    """Shift a canonical timestamp by ``seconds`` (negative = earlier)."""
    dt = parse_ts(value)
    if dt is None:
        raise ValueError(f"Invalid timestamp: {value!r}")
    return format_ts(dt + timedelta(seconds=seconds))


def now_utc() -> str:
    return format_ts(datetime.now(timezone.utc))


def record_cursor(record: dict, cursor_field: str = "update_time") -> Optional[str]:
    """Canonical cursor of a record, falling back to ``add_time`` when null."""
    return normalize_ts(record.get(cursor_field)) or normalize_ts(record.get("add_time"))


# --------------------------------------------------------------------------- #
# Record normalisation
# --------------------------------------------------------------------------- #


def _to_json_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def normalize_record(
    record: dict,
    *,
    json_fields: tuple = (),
    json_array_fields: tuple = (),
    lead_custom_fields: bool = False,
    field_table: bool = False,
    deal_flow: bool = False,
    fill_update_time: bool = False,
) -> dict:
    """Adjust a raw Pipedrive record so it fits the static schema.

    Only shape-level fixes are applied (mixed-type / free-form values are
    JSON-encoded, aliases are mapped); value types are otherwise left to the
    framework's schema coercion.
    """
    rec = dict(record)

    if lead_custom_fields:
        # Leads inherit deal custom fields as root-level 40-hex keys that are
        # only present when set.  Gather them into one JSON column.
        custom = {k: rec.pop(k) for k in list(rec) if _HEX40_RE.match(str(k))}
        existing = rec.get("custom_fields")
        if isinstance(existing, dict):
            custom = {**existing, **custom}
        rec["custom_fields"] = custom or None

    if "ims" in rec and rec.get("im") is None:
        # Persons: the spec schema says ``im``; the migration guide says ``ims``.
        rec["im"] = rec.pop("ims")

    if field_table:
        flag = rec.get("mandatory_flag")
        if isinstance(flag, bool):
            rec["mandatory_flag"] = "true" if flag else "false"
        elif flag is not None:
            rec["mandatory_flag"] = _to_json_string(flag)

    if deal_flow:
        flag = rec.get("is_bulk_update_flag")
        if flag is not None and not isinstance(flag, bool):
            rec["is_bulk_update_flag"] = str(flag).lower() in ("1", "true", "yes")

    for name in json_fields:
        if name in rec:
            value = rec[name]
            # An empty ``custom_fields`` object carries no information.
            if name == "custom_fields" and value in ({}, []):
                value = None
            rec[name] = _to_json_string(value)

    for name in json_array_fields:
        value = rec.get(name)
        if isinstance(value, list):
            rec[name] = [_to_json_string(v) for v in value]
        elif value is not None:
            rec[name] = [_to_json_string(value)]

    if fill_update_time and not rec.get("update_time") and rec.get("add_time"):
        # Records never updated have a null update_time; fall back to add_time
        # so the CDC sequence column is always populated.
        rec["update_time"] = rec["add_time"]

    return rec


# --------------------------------------------------------------------------- #
# HTTP client
# --------------------------------------------------------------------------- #


class PipedriveClient:
    """Thin ``requests`` wrapper: auth header, throttling, retries, pagination."""

    def __init__(self, api_token: str, base_url: str, burst_limit: int = DEFAULT_BURST_LIMIT):
        self._api_token = api_token
        self._base_url = base_url.rstrip("/")
        self._burst_limit = max(1, int(burst_limit))
        self._session: Optional[requests.Session] = None
        self._recent_calls: list[float] = []

    # Sessions are created lazily so the connector stays picklable.
    def __getstate__(self):
        state = dict(self.__dict__)
        state["_session"] = None
        state["_recent_calls"] = []
        return state

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            sess = requests.Session()
            sess.headers.update(
                {
                    "x-api-token": self._api_token,
                    "Accept": "application/json",
                    "User-Agent": "lakeflow-community-connector-pipedrive",
                }
            )
            self._session = sess
        return self._session

    def _throttle(self) -> None:
        now = time.monotonic()
        window_start = now - BURST_WINDOW_SECONDS
        self._recent_calls = [t for t in self._recent_calls if t > window_start]
        if len(self._recent_calls) >= self._burst_limit:
            sleep_for = self._recent_calls[0] - window_start
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._recent_calls.append(time.monotonic())

    @staticmethod
    def _retry_delay(resp: requests.Response, backoff: float) -> float:
        reset = resp.headers.get("x-ratelimit-reset") or resp.headers.get("Retry-After")
        if reset:
            try:
                # x-ratelimit-reset is *relative* seconds until window reset.
                return min(MAX_BACKOFF_SECONDS, max(0.5, float(reset)))
            except ValueError:
                pass
        return backoff

    def get(self, path: str, params: Optional[dict] = None) -> requests.Response:
        """GET with throttling and retry on 429 / 5xx.  Returns the final response."""
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF_SECONDS
        resp = None
        for attempt in range(MAX_RETRIES):
            self._throttle()
            try:
                resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            except (requests.ConnectionError, requests.Timeout):
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(backoff)
                backoff = min(MAX_BACKOFF_SECONDS, backoff * 2)
                continue
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                time.sleep(self._retry_delay(resp, backoff))
                backoff = min(MAX_BACKOFF_SECONDS, backoff * 2)
        return resp

    def get_json(self, path: str, params: Optional[dict] = None) -> dict:
        """GET and decode the JSON body; raise ``PipedriveAPIError`` on errors."""
        resp = self.get(path, params)
        content_type = resp.headers.get("Content-Type", "")
        is_json = "json" in content_type.lower()
        if resp.status_code != 200:
            snippet = resp.text[:300] if is_json else f"<{content_type or 'non-JSON'} body>"
            raise PipedriveAPIError(
                f"Pipedrive API GET {path} failed with HTTP {resp.status_code}: {snippet}",
                status_code=resp.status_code,
                is_json=is_json,
            )
        try:
            body = resp.json()
        except ValueError as exc:
            raise PipedriveAPIError(
                f"Pipedrive API GET {path} returned a non-JSON body ({content_type})",
                status_code=resp.status_code,
                is_json=False,
            ) from exc
        if isinstance(body, dict) and body.get("success") is False:
            raise PipedriveAPIError(
                f"Pipedrive API GET {path} returned success=false: "
                f"{body.get('error') or body.get('error_info') or ''}",
                status_code=resp.status_code,
            )
        return body if isinstance(body, dict) else {"data": body}

    # ------------------------------------------------------------------ #
    # Pagination
    # ------------------------------------------------------------------ #

    def iter_v2_pages(self, path: str, params: dict) -> Iterator[list]:
        """Yield each page's ``data`` list from a cursor-paginated v2 endpoint."""
        query = dict(params)
        while True:
            body = self.get_json(path, query)
            data = body.get("data") or []
            yield data if isinstance(data, list) else [data]
            next_cursor = (body.get("additional_data") or {}).get("next_cursor")
            if not next_cursor or not data:
                return
            if next_cursor == query.get("cursor"):
                # A repeated cursor would replay the same page forever.
                raise PipedriveAPIError(
                    f"Pipedrive API GET {path} returned a next_cursor that did not advance"
                )
            query["cursor"] = next_cursor

    def iter_v1_pages(self, path: str, params: dict) -> Iterator[list]:
        """Yield each page's ``data`` list from an offset-paginated v1 endpoint.

        Endpoints without a ``pagination`` block are treated as a single page.
        """
        query = dict(params)
        while True:
            body = self.get_json(path, query)
            data = body.get("data") or []
            yield data if isinstance(data, list) else [data]
            pagination = (body.get("additional_data") or {}).get("pagination") or {}
            if not pagination.get("more_items_in_collection") or not data:
                return
            current_start = int(query.get("start", 0))
            next_start = pagination.get("next_start")
            if next_start is None:
                next_start = current_start + len(data)
            if int(next_start) <= current_start:
                return  # defensive: never loop on a non-advancing offset
            query["start"] = next_start
