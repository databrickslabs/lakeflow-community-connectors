"""Lakeflow community connector for Visma e-conomic (OpenAPI family).

Reads from the e-conomic "resource APIs" at ``https://apis.e-conomic.com/``:

* ``projectsapi``  -> ``employees`` (/Employees), ``projects`` (/Projects),
  ``project_time_entries`` (/TimeEntries), ``activities`` (/Activities),
  ``activity_groups`` (/ActivityGroups), ``employee_groups``
  (/EmployeeGroups), ``project_groups`` (/ProjectGroups),
  ``project_statuses`` (/ProjectStatuses), ``mileages`` (/Mileages),
  ``project_employees`` (/project-employees), ``project_customers``
  (/project-customers), ``time_entry_prices`` (/TimeEntryPrices),
  ``cost_types`` (/CostTypes)
* ``customersapi`` -> ``customers`` (/Customers), ``customer_contacts``
  (/Contacts), ``customer_delivery_locations`` (/DeliveryLocations)

Every table is a flat, top-level cursor list (no parent fan-out); the
table -> endpoint routing lives in ``TABLE_ENDPOINTS``.

Authentication is two static headers (``X-AppSecretToken`` and
``X-AgreementGrantToken``). All list reads use the cursor endpoints, which
return ``{"items": [...], "cursor": "<next>"}`` with ``cursor`` absent on the
last page. Items are returned in identifier (primary key) order and the
cursor endpoints do not support ``sort``.

Ingestion strategy
------------------
* cdc on ``lastUpdated``: ``projects``, ``project_time_entries``,
  ``cost_types``, ``customer_contacts``, ``customer_delivery_locations``.
* snapshot (no filterable change-tracking field): every other table.
  ``time_entry_prices`` is snapshot even though it mirrors time entries 1:1,
  because its prices are calculated and can change without the entry's
  ``lastUpdated`` moving.

Because results are ordered by primary key rather than ``lastUpdated``, the
cdc read is a *resumable PK-ordered scan*: each trigger scans every record
with ``lastUpdated >= <last high-water mark - lookback>`` and the high-water
mark is only advanced once the whole scan has finished. When
``max_records_per_batch`` cuts a scan short, the offset records the last
emitted primary key and the next call resumes the same scan with an extra
``number$gt:<pk>`` filter clause. A completed scan stamps the offset with the
trigger's init time (``synced_at``), so the next call in the same trigger
returns the same offset and Trigger.AvailableNow terminates.

Why not ``SupportsPartitionedStream``: the cursor endpoints cannot sort, so
there is no cheap high-water-mark query for ``latest_offset``; the API is
rate limited by a per-agreement token bucket; and accounting datasets are
small, so a single-driver sequential read is sufficient.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.economic.economic_schemas import (
    CURSOR_FORMAT,
    CUSTOMERS_API,
    DEFAULT_BASE_URL,
    DEFAULT_CUSTOMERS_API_VERSION,
    DEFAULT_LOOKBACK_SECONDS,
    DEFAULT_MAX_RECORDS_PER_BATCH,
    DEFAULT_PROJECTS_API_VERSION,
    INITIAL_BACKOFF_SECONDS,
    MAX_BACKOFF_SECONDS,
    MAX_PAGES_PER_READ,
    MAX_RETRIES,
    PROJECTS_API,
    REQUEST_TIMEOUT_SECONDS,
    RETRIABLE_STATUS_CODES,
    SUPPORTED_TABLES,
    TABLE_ENDPOINTS,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

logger = logging.getLogger(__name__)

_FRACTION_RE = re.compile(r"\.\d+")

# Offset ``phase`` values.
_PHASE_SCANNING = "scanning"
_PHASE_SYNCED = "synced"


class EconomicLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Visma e-conomic."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._app_secret_token = options.get("app_secret_token")
        self._agreement_grant_token = options.get("agreement_grant_token")
        if not self._app_secret_token or not self._agreement_grant_token:
            raise ValueError(
                "e-conomic connector requires both 'app_secret_token' and "
                "'agreement_grant_token' options (use 'demo' for both to read "
                "the public demo agreement)."
            )

        self._base_url = (options.get("base_url") or DEFAULT_BASE_URL).rstrip("/")
        self._api_versions = {
            PROJECTS_API: _normalize_version(
                options.get("projects_api_version") or DEFAULT_PROJECTS_API_VERSION
            ),
            CUSTOMERS_API: _normalize_version(
                options.get("customers_api_version") or DEFAULT_CUSTOMERS_API_VERSION
            ),
        }

        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-AppSecretToken": self._app_secret_token,
                "X-AgreementGrantToken": self._agreement_grant_token,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Cap cursors at init time so a trigger never chases new data; the
        # next trigger creates a fresh instance with a newer cap. Stored in
        # the same naive ISO format the API uses for ``lastUpdated``.
        init_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        self._init_ts = init_dt.strftime(CURSOR_FORMAT)
        # Higher-resolution marker identifying this trigger; stamped on the
        # offset when a scan completes so the next call in the same trigger
        # short-circuits while a later trigger (new instance) scans again.
        self._sync_marker = init_dt.isoformat(timespec="microseconds")

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        table_options = table_options or {}
        if TABLE_METADATA[table_name]["ingestion_type"] == "snapshot":
            return self._read_snapshot(table_name), {}
        return self._read_cdc(table_name, start_offset, table_options)

    # ------------------------------------------------------------------
    # Snapshot reads
    # ------------------------------------------------------------------

    def _read_snapshot(self, table_name: str) -> Iterator[dict]:
        """Full cursor traversal; pages are fetched lazily as Spark iterates."""
        api, resource, _pk = TABLE_ENDPOINTS[table_name]
        for page in self._iter_pages(api, resource, filter_expr=None):
            yield from page

    # ------------------------------------------------------------------
    # CDC reads (resumable PK-ordered scan)
    # ------------------------------------------------------------------

    def _read_cdc(
        self, table_name: str, start_offset: Optional[dict], table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        api, resource, pk = TABLE_ENDPOINTS[table_name]
        cursor_field = TABLE_METADATA[table_name]["cursor_field"]
        start = dict(start_offset) if start_offset else {}
        max_records = _positive_int(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH
        )

        committed_cursor = start.get("cursor") or ""

        if start.get("phase") == _PHASE_SCANNING:
            # Resume a scan that a previous call cut short.
            since = start.get("scan_since") or None
            after_pk = start.get("after_pk")
            scan_max = start.get("scan_max") or ""
        else:
            # A scan already completed within this trigger -> caught up.
            synced_at = start.get("synced_at")
            if synced_at and synced_at >= self._sync_marker:
                return iter([]), start_offset
            since = self._scan_lower_bound(committed_cursor, table_options)
            after_pk = None
            scan_max = committed_cursor

        filter_parts = []
        if since:
            filter_parts.append(f"{cursor_field}$gte:{since}")
        if after_pk is not None:
            filter_parts.append(f"{pk}$gt:{int(after_pk)}")
        filter_expr = "$and:".join(filter_parts) or None

        records: list[dict] = []
        truncated = False
        for page in self._iter_pages(api, resource, filter_expr=filter_expr):
            for record in page:
                records.append(record)
                value = _normalize_ts(record.get(cursor_field))
                if value and value > scan_max:
                    scan_max = value
                if len(records) >= max_records:
                    truncated = True
                    break
            if truncated:
                break

        if truncated:
            end_offset = {
                "phase": _PHASE_SCANNING,
                "cursor": committed_cursor,
                "scan_since": since or "",
                "after_pk": int(records[-1][pk]),
                "scan_max": scan_max,
            }
        else:
            # Scan finished: commit the high-water mark, capped at init time
            # so clock skew / future-dated rows can never push the cursor
            # past data that has not been observed yet.
            new_cursor = min(scan_max, self._init_ts) if scan_max else committed_cursor
            end_offset = {
                "phase": _PHASE_SYNCED,
                "cursor": new_cursor,
                "synced_at": self._sync_marker,
            }

        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _scan_lower_bound(
        self, committed_cursor: str, table_options: dict[str, str]
    ) -> Optional[str]:
        """Resolve the ``lastUpdated$gte`` bound for a new scan.

        The lookback is applied once per scan (here) and never stored in
        the offset, so the checkpointed cursor does not drift.
        """
        if committed_cursor:
            lookback = _non_negative_int(
                table_options, "lookback_seconds", DEFAULT_LOOKBACK_SECONDS
            )
            bound = datetime.strptime(committed_cursor, CURSOR_FORMAT) - timedelta(
                seconds=lookback
            )
            return bound.strftime(CURSOR_FORMAT)

        start_ts = table_options.get("start_timestamp")
        if start_ts:
            normalized = _normalize_ts(start_ts)
            if not normalized:
                raise ValueError(
                    f"Invalid 'start_timestamp' {start_ts!r}; expected ISO-8601 "
                    "such as '2024-01-01T00:00:00'."
                )
            return normalized
        # First run with no explicit start: full scan (no filter).
        return None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _resource_url(self, api: str, resource: str) -> str:
        return f"{self._base_url}/{api}/{self._api_versions[api]}/{resource}"

    def _iter_pages(
        self, api: str, resource: str, filter_expr: Optional[str]
    ) -> Iterator[list[dict]]:
        """Yield each page's ``items`` list, following ``cursor`` until absent.

        The same ``filter`` is re-sent with every cursor request, as the
        cursor endpoint expects.
        """
        url = self._resource_url(api, resource)
        cursor: Optional[str] = None
        seen_cursors: set[str] = set()
        for _ in range(MAX_PAGES_PER_READ):
            params: dict[str, str] = {}
            if filter_expr:
                params["filter"] = filter_expr
            if cursor:
                params["cursor"] = cursor

            body = self._get_json(url, params)
            items = body.get("items") if isinstance(body, dict) else None
            if items is None:
                raise RuntimeError(
                    f"Unexpected e-conomic response from {url}: missing 'items' key"
                )
            yield items

            next_cursor = body.get("cursor")
            if not next_cursor or not items:
                return
            if next_cursor in seen_cursors:
                raise RuntimeError(
                    f"e-conomic returned a repeated cursor {next_cursor!r} for {url}; "
                    "aborting to avoid an infinite pagination loop."
                )
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise RuntimeError(
            f"Exceeded {MAX_PAGES_PER_READ} pages while reading {url}; aborting."
        )

    def _get_json(self, url: str, params: dict[str, str]) -> Any:
        """GET with retries on 429 (honouring Retry-After) and 5xx."""
        backoff = INITIAL_BACKOFF_SECONDS
        resp: Optional[requests.Response] = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                logger.warning("e-conomic request to %s failed (%s); retrying", url, exc)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
                continue

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code not in RETRIABLE_STATUS_CODES or attempt == MAX_RETRIES - 1:
                break

            wait = _retry_after_seconds(resp)
            if wait is None:
                wait = backoff
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            logger.warning(
                "e-conomic returned %s for %s; retrying in %.1fs", resp.status_code, url, wait
            )
            time.sleep(wait)

        raise RuntimeError(_describe_error(resp, url))

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_table(table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {SUPPORTED_TABLES}"
            )


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------


def _normalize_version(version: str) -> str:
    """Accept ``1.1.0`` or ``v1.1.0`` and return the URL form ``v1.1.0``."""
    version = str(version).strip().strip("/")
    return version if version.lower().startswith("v") else f"v{version}"


def _normalize_ts(value: Any) -> Optional[str]:
    """Normalize an ISO-8601 timestamp to naive ``YYYY-MM-DDTHH:MM:SS``.

    The API emits naive timestamps (``2024-12-07T23:14:00``). Values with
    an offset or ``Z`` are converted to UTC before dropping the zone;
    fractional seconds are truncated. Returns None for empty / unparsable
    input.
    """
    if not value or not isinstance(value, str):
        return None
    text = _FRACTION_RE.sub("", value.strip()).replace("Z", "+00:00").replace("z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.strftime(CURSOR_FORMAT)


def _positive_int(options: dict[str, str], key: str, default: int) -> int:
    raw = options.get(key)
    if raw in (None, ""):
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"Table option '{key}' must be a positive integer, got {raw!r}")
    return value


def _non_negative_int(options: dict[str, str], key: str, default: int) -> int:
    raw = options.get(key)
    if raw in (None, ""):
        return default
    value = int(raw)
    if value < 0:
        raise ValueError(f"Table option '{key}' must be >= 0, got {raw!r}")
    return value


def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return min(max(float(raw), 0.0), MAX_BACKOFF_SECONDS)
    except ValueError:
        return None


def _describe_error(resp: Optional[requests.Response], url: str) -> str:
    if resp is None:
        return f"e-conomic request to {url} failed with no response"
    detail = ""
    try:
        body = resp.json()
        if isinstance(body, dict):
            parts = [str(body.get(k)) for k in ("errorCode", "title", "detail") if body.get(k)]
            detail = " - ".join(parts)
    except ValueError:
        detail = (resp.text or "")[:500]
    hint = ""
    if resp.status_code == 401:
        hint = " (check app_secret_token / agreement_grant_token)"
    elif resp.status_code == 403:
        hint = " (the agreement grant lacks the required role for this resource)"
    return f"e-conomic API error {resp.status_code} for {url}{hint}: {detail}"
