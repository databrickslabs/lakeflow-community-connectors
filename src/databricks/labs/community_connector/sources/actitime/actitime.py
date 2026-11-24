"""actiTIME source connector for Lakeflow Community Connectors.

Implements the :class:`LakeflowConnect` interface against the actiTIME REST API
v1 (https://www.actitime.com/api-documentation). Sixteen tables are exposed:

    Core business: customers, projects, tasks, timetrack, leavetime
    User/org:      users, departments, userGroups, userRates
    Reference:     typesOfWork, leaveTypes, workflowStatuses, timeZoneGroups,
                   settings, holidays
    Approval:      approvalStatus

Authentication is HTTP Basic. The connector stores the actiTIME base URL, the
service-account username and the password; there is no OAuth flow.

Two API pagination styles are used:

* Offset/limit  — list resources (customers, projects, tasks, users, …).
* ``stopAfter`` — time-windowed resources (``/timetrack``, ``/leavetime``).
  The connector also slices the requested ``dateFrom``/``dateTo`` window so
  the server-side cap is exercised deterministically.

CDC tables (``customers``, ``projects``, ``tasks``) do **not** have a
``modifiedAt`` field on the API, so a strict "since last sync" filter is not
possible. We instead perform a full-list refresh on every trigger and rely on
upsert-on-primary-key semantics downstream. The cursor advances to a sentinel
("done" → trigger termination per Trigger.AvailableNow contract).

``users`` is treated as a ``snapshot`` table because the actiTIME API does
not expose any timestamp field on a user (no ``created``, ``modifiedAt``,
or equivalent) — see the live response in Phase 2. A full-refresh + upsert
remains correct and cheap for the typically small user roster.

Some endpoints are tenant-feature-gated and return HTTP 404 on accounts
that don't have them enabled (commonly ``/userGroups``, ``/settings``,
``/holidays``, ``/approvalStatus``). The connector treats a 404 on a
list/snapshot endpoint as "table absent on this tenant" — read returns
zero records with a ``{"done": True}`` sentinel so the trigger terminates
cleanly. Add or remove a feature on the tenant to see the table populate.

All list endpoints (``/customers``, ``/projects``, ``/tasks``, ``/users``,
``/departments``, ``/typesOfWork``, ``/leaveTypes``, ``/workflowStatuses``,
``/timeZoneGroups``, ``/userGroups``, ``/holidays``) return an envelope of
the form ``{"offset": N, "limit": M, "items": [...]}``. ``/timetrack`` and
``/leavetime`` return ``{"dateFrom": ..., "dateTo": ..., "data": [...]}``.
The connector unwraps these consistently in :meth:`_unwrap_records`.

Append tables (``timetrack``, ``leavetime``, ``approvalStatus``) use the
sliding time-window strategy. The ``date`` field (or ``week_start_date`` for
``approvalStatus``) is the cursor. A configurable ``window_days`` table option
controls how many days each microbatch covers. Lookback of ``lookback_days``
days is applied at read time to catch late-arriving edits within the open
editable window. The cursor is short-circuited at ``_init_ts`` so the trigger
always terminates.
"""

import base64
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterator
from urllib.parse import urljoin

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

logger = logging.getLogger(__name__)


# HTTP retry knobs — used by the lightweight retry helper.
INITIAL_BACKOFF = 1.0
MAX_RETRIES = 5
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_TIMEOUT = 30


# Ingestion-type metadata — primary keys and cursor fields per table.
# Kept as a module-level constant so it stays in sync with TABLE_SCHEMAS below.
TABLE_METADATA: dict[str, dict] = {
    # CDC tables. ``customers``, ``projects`` and ``tasks`` expose
    # a ``created`` epoch-ms timestamp on the API; we keep it as a proxy
    # cursor but still perform a full-list refresh + upsert on id since
    # there is no ``modifiedAt`` filter.
    "customers": {"primary_keys": ["id"], "cursor_field": "created", "ingestion_type": "cdc"},
    "projects": {"primary_keys": ["id"], "cursor_field": "created", "ingestion_type": "cdc"},
    "tasks": {"primary_keys": ["id"], "cursor_field": "created", "ingestion_type": "cdc"},
    # ``users`` has no timestamp field on the actiTIME API (verified
    # against live response — see class docstring). Treat as a snapshot.
    "users": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    # Append tables — cursor on the date field, sliding-window strategy.
    # timetrack records are nested per (user, day); after flattening the
    # natural composite key is (user_id, date, task_id). leavetime is one
    # row per (user, day, leaveType) — composite key (user_id, date,
    # leave_type_id). Neither endpoint exposes a record-level ``id``.
    "timetrack": {
        "primary_keys": ["user_id", "date", "task_id"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
    "leavetime": {
        "primary_keys": ["user_id", "date", "leave_type_id"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
    "approvalStatus": {
        "primary_keys": ["user_id", "week_start_date"],
        "cursor_field": "week_start_date",
        "ingestion_type": "append",
    },
    # Snapshot tables.
    "departments": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "userGroups": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "userRates": {
        "primary_keys": ["user_id", "date_from"],
        "ingestion_type": "snapshot",
    },
    "typesOfWork": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "leaveTypes": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "workflowStatuses": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "timeZoneGroups": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "settings": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "holidays": {
        "primary_keys": ["date", "time_zone_group_id"],
        "ingestion_type": "snapshot",
    },
}


def _build_schemas() -> dict[str, StructType]:
    """Return the static schema dictionary.

    Wrapped in a function to keep the module body short and make schema
    additions easy to grep.
    """

    return {
        "customers": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("archived", BooleanType()),
                StructField("created", TimestampType()),
                StructField("url", StringType()),
                StructField("allowed_actions_can_modify", BooleanType()),
                StructField("allowed_actions_can_delete", BooleanType()),
            ]
        ),
        "projects": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("customer_id", LongType()),
                StructField("archived", BooleanType()),
                StructField("created", TimestampType()),
                StructField("workflow_enabled", BooleanType()),
                StructField("default_type_of_work_id", LongType()),
                StructField("url", StringType()),
                StructField("allowed_actions_can_modify", BooleanType()),
                StructField("allowed_actions_can_delete", BooleanType()),
            ]
        ),
        "tasks": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("project_id", LongType()),
                StructField("customer_id", LongType()),
                StructField("archived", BooleanType()),
                StructField("created", TimestampType()),
                StructField("type_of_work_id", LongType()),
                StructField("workflow_status_id", LongType()),
                StructField("deadline", DateType()),
                StructField("estimated_time", LongType()),
                StructField("url", StringType()),
                StructField("allowed_actions_can_modify", BooleanType()),
                StructField("allowed_actions_can_delete", BooleanType()),
                StructField("allowed_actions_can_complete", BooleanType()),
            ]
        ),
        # NOTE: actiTIME's ``/users`` response has no ``created`` (nor any
        # other) timestamp, so the column is omitted. ``user_groups`` and
        # ``user_roles`` are likewise absent from the live response on
        # this tenant; we leave them off the schema to avoid carrying
        # always-null columns.
        "users": StructType(
            [
                StructField("id", LongType()),
                StructField("first_name", StringType()),
                StructField("middle_name", StringType()),
                StructField("last_name", StringType()),
                StructField("full_name", StringType()),
                StructField("username", StringType()),
                StructField("email", StringType()),
                StructField("department_id", LongType()),
                StructField("active", BooleanType()),
                StructField("time_zone_group_id", LongType()),
                StructField("hired", DateType()),
                StructField("release_date", DateType()),
                StructField("allowed_actions_can_submit_timetrack", BooleanType()),
            ]
        ),
        # timetrack: nested envelope flattened to one row per record. The
        # inner ``records`` array only carries ``taskId`` and ``time`` —
        # there is no record-level id/comment/approved/locked on this
        # tenant's API. The wire field ``dayOffset`` is intentionally
        # dropped: empirically (PR #176 review, comment 3228778244) it is
        # ``(entry.date - request.dateFrom).days``, which flickers across
        # overlapping fetches caused by ``lookback_days > 0`` and is not
        # a stable record property. ``date`` is the canonical day.
        "timetrack": StructType(
            [
                StructField("user_id", LongType()),
                StructField("date", DateType()),
                StructField("task_id", LongType()),
                StructField("time", LongType()),
            ]
        ),
        # leavetime: flat per-user-per-day-per-leaveType entries; no
        # nested ``records`` array. The minute total lives under
        # ``leaveTime`` on the wire. ``dayOffset`` is dropped for the
        # same reason documented on the timetrack schema above.
        "leavetime": StructType(
            [
                StructField("user_id", LongType()),
                StructField("date", DateType()),
                StructField("leave_type_id", LongType()),
                StructField("time", LongType()),
            ]
        ),
        "departments": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("parent_id", LongType()),
                StructField("manager_id", LongType()),
            ]
        ),
        "userGroups": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
            ]
        ),
        # /userRates/{userId} response: ``leaveRates`` is an array of
        # ``{leaveTypeId, rate}`` objects, not a map keyed by leaveType.
        # Model as ARRAY<STRUCT<leave_type_id, rate>> for fidelity.
        "userRates": StructType(
            [
                StructField("user_id", LongType()),
                StructField("date_from", DateType()),
                StructField("regular_rate", DecimalType(18, 4)),
                StructField("overtime_rate", DecimalType(18, 4)),
                StructField(
                    "leave_rates",
                    ArrayType(
                        StructType(
                            [
                                StructField("leave_type_id", LongType()),
                                StructField("rate", DecimalType(18, 4)),
                            ]
                        )
                    ),
                ),
            ]
        ),
        "typesOfWork": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("archived", BooleanType()),
                StructField("billable", BooleanType()),
            ]
        ),
        "leaveTypes": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("archived", BooleanType()),
                StructField("paid_leave", BooleanType()),
                StructField("auto_accrual", BooleanType()),
            ]
        ),
        "workflowStatuses": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("type", StringType()),
                StructField("order", LongType()),
            ]
        ),
        "timeZoneGroups": StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("time_zone", StringType()),
            ]
        ),
        "settings": StructType(
            [
                StructField("id", LongType()),
                StructField("workday_duration", LongType()),
                StructField("week_start_day", LongType()),
                StructField("date_format", StringType()),
                StructField("time_format", StringType()),
                StructField("currency_code", StringType()),
                StructField("decimal_separator", StringType()),
                StructField("thousands_separator", StringType()),
            ]
        ),
        "holidays": StructType(
            [
                StructField("date", DateType()),
                StructField("name", StringType()),
                StructField("time_zone_group_id", LongType()),
            ]
        ),
        "approvalStatus": StructType(
            [
                StructField("user_id", LongType()),
                StructField("week_start_date", DateType()),
                StructField("status", StringType()),
                StructField("submitted_at", TimestampType()),
                StructField("approved_at", TimestampType()),
                StructField("approver_id", LongType()),
            ]
        ),
    }


TABLE_SCHEMAS = _build_schemas()


# Tables that paginate via offset/limit on the actiTIME server side.
_OFFSET_LIMIT_TABLES = {"customers", "projects", "tasks", "users"}

# Tables that paginate via stopAfter + dateFrom/dateTo window.
_TIME_WINDOW_TABLES = {"timetrack", "leavetime"}


class ActitimeLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for actiTIME REST API v1."""

    # ------------------------------------------------------------------
    # Construction & helpers
    # ------------------------------------------------------------------

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        # Required connection parameters. ``base_url`` should look like
        # ``https://online.actitime.com/<tenant>`` or
        # ``https://<self-hosted-host>``. The connector appends ``/api/v1``.
        base_url = options.get("base_url")
        if not base_url:
            raise ValueError(
                "actiTIME connector requires 'base_url' option "
                "(e.g. 'https://online.actitime.com/<tenant>')."
            )
        username = options.get("username")
        password = options.get("password")
        if not username or not password:
            raise ValueError("actiTIME connector requires 'username' and 'password' options.")

        # Normalise base URL — keep a trailing /api/v1 root so urljoin works.
        self._root = base_url.rstrip("/") + "/api/v1/"

        auth_bytes = f"{username}:{password}".encode("utf-8")
        self._auth_header = {
            "Authorization": "Basic " + base64.b64encode(auth_bytes).decode("ascii"),
            "Accept": "application/json; charset=UTF-8",
        }

        # Cap cursors at init time so trigger.availableNow always terminates.
        # Stored as an ISO date string because cursor fields on append tables
        # are calendar dates (YYYY-MM-DD), not timestamps.
        self._init_date = datetime.now(timezone.utc).date()
        self._init_ts_iso = self._init_date.isoformat()

    def _url(self, path: str) -> str:
        """Join *path* (no leading slash) onto the API root."""
        return urljoin(self._root, path.lstrip("/"))

    def _request(self, path: str, params: dict | None = None) -> requests.Response:
        """GET *path* with retry on 429/5xx; honour ``Retry-After``.

        Every request carries an explicit timeout so the connector cannot hang
        on a stuck socket. ``requests`` raises ``RequestException`` subclasses
        for transport-level errors; we let those propagate so the framework
        retries the microbatch.
        """
        backoff = INITIAL_BACKOFF
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            resp = requests.get(
                self._url(path),
                headers=self._auth_header,
                params=params or {},
                timeout=DEFAULT_TIMEOUT,
            )
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            # Honour Retry-After when present; fall back to exponential backoff.
            retry_after = resp.headers.get("Retry-After")
            sleep_s = backoff
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except ValueError:
                    pass
            logger.warning(
                "actiTIME %s returned %s; retrying in %.1fs (attempt %d/%d)",
                path,
                resp.status_code,
                sleep_s,
                attempt + 1,
                MAX_RETRIES,
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(sleep_s)
                backoff *= 2

        assert resp is not None  # for type-checkers; always set inside the loop
        return resp

    def _get_json(self, path: str, params: dict | None = None):
        """Issue a GET, raise on non-2xx, return the decoded JSON body."""
        resp = self._request(path, params=params)
        if resp.status_code // 100 != 2:
            raise RuntimeError(
                f"actiTIME API GET {path} failed with HTTP {resp.status_code}: {resp.text[:500]}"
            )
        return resp.json()

    def _get_json_or_none(self, path: str, params: dict | None = None):
        """GET ``path`` and return the JSON body, or ``None`` on HTTP 404.

        Some endpoints are gated by tenant feature flags (``/userGroups``,
        ``/settings``, ``/holidays``, ``/approvalStatus``). Treat 404 as
        "table not enabled on this tenant" and let the caller emit an
        empty result so the trigger terminates cleanly.
        """
        resp = self._request(path, params=params)
        if resp.status_code == 404:
            logger.info(
                "actiTIME API GET %s returned 404 — treating as absent table on this tenant.",
                path,
            )
            return None
        if resp.status_code // 100 != 2:
            raise RuntimeError(
                f"actiTIME API GET {path} failed with HTTP {resp.status_code}: {resp.text[:500]}"
            )
        return resp.json()

    @staticmethod
    def _unwrap_records(body, key: str = "items") -> list:
        """Return the records list from an actiTIME response body.

        actiTIME wraps list responses in an envelope:
            { "items": [...], "offset": N, "limit": M }       — most lists
            { "data": [...], "dateFrom": ..., "dateTo": ... } — timetrack/leavetime

        Bare arrays still occur on some endpoints (e.g. ``/userRates/{id}``).
        Accept all three shapes — return ``[]`` for anything else (None, an
        error dict, an unexpected schema) so callers can iterate without
        guarding.
        """
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            inner = body.get(key)
            if isinstance(inner, list):
                return inner
            # Fallback: tolerate the alternate well-known wrapper key in
            # case the API ever changes between ``items``/``data``.
            for alt in ("items", "data", "records"):
                if alt == key:
                    continue
                inner = body.get(alt)
                if isinstance(inner, list):
                    return inner
        return []

    # ------------------------------------------------------------------
    # LakeflowConnect API
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        # actiTIME has no discovery endpoint; tables are statically known.
        return list(TABLE_SCHEMAS.keys())

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        # Return a fresh copy so callers cannot mutate the module-level dict.
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        metadata = TABLE_METADATA[table_name]
        ingestion_type = metadata["ingestion_type"]

        if ingestion_type == "snapshot":
            return self._read_snapshot(table_name, start_offset, table_options)
        if table_name in _TIME_WINDOW_TABLES:
            return self._read_time_window(table_name, start_offset, table_options)
        if table_name == "approvalStatus":
            return self._read_approval_status(start_offset, table_options)
        # CDC tables fall through to the full-list refresh path.
        return self._read_full_list_cdc(table_name, start_offset, table_options)

    # ------------------------------------------------------------------
    # Snapshot reads
    # ------------------------------------------------------------------

    def _read_snapshot(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Full-refresh read for snapshot tables.

        Returns ``({"done": True})`` after the first call so subsequent calls
        within the same Trigger.AvailableNow trigger short-circuit (per the
        ``end_offset == start_offset`` termination contract).
        """
        if start_offset and start_offset.get("done"):
            return iter([]), start_offset

        if table_name == "settings":
            records = list(self._read_settings())
        elif table_name == "userRates":
            records = list(self._read_user_rates())
        elif table_name == "holidays":
            records = list(self._read_holidays(table_options))
        else:
            records = list(self._read_offset_limit(table_name, table_options, params={}))

        return iter(records), {"done": True}

    def _read_settings(self) -> Iterator[dict]:
        """/settings is a singleton: returns a JSON object, not an array.

        On tenants that don't expose tenant-wide settings this endpoint
        returns 404; we yield no rows in that case (see class docstring).
        An empty body (``{}``) is treated as "settings unavailable" too,
        rather than emitting a row with only a synthetic id.
        """
        body = self._get_json_or_none("settings")
        if body is None:
            return
        if isinstance(body, list):
            # Defensive: if the API ever changes to a list we still cope.
            if not body:
                return
            body = body[0]
        if not isinstance(body, dict) or not body:
            return
        # Be conservative: only emit a row if at least one known settings
        # field is present. Avoids materialising rows from an error
        # envelope that happened to have a 200 status.
        known = {
            "workdayDuration",
            "weekStartDay",
            "dateFormat",
            "timeFormat",
            "currencyCode",
            "decimalSeparator",
            "thousandsSeparator",
        }
        if not known.intersection(body.keys()):
            return
        record = dict(body)
        # Synthesise a constant primary key so the row is addressable.
        record.setdefault("id", 1)
        yield self._map_record("settings", record)

    def _read_user_rates(self) -> Iterator[dict]:
        """/userRates is per-user: fan out one request per user from /users.

        Takes no ``table_options``: the inner ``/users`` walk is transport
        plumbing for the fan-out, not an emission of the users table, and
        ``/userRates/{id}`` itself accepts no filters. Helper defaults
        handle any tenant size (cap is gated on ``_raw=True`` in
        :meth:`_read_offset_limit`).
        """
        users = list(self._read_offset_limit("users", {}, params={}, _raw=True))
        for user in users:
            uid = user.get("id")
            if uid is None:
                continue
            rates = self._get_json_or_none(f"userRates/{uid}")
            if rates is None:
                # Tenant feature disabled for this user — skip.
                continue
            for r in self._unwrap_records(rates, key="items"):
                rec = dict(r)
                rec["userId"] = uid  # inject path param into the body
                yield self._map_record("userRates", rec)

    def _read_holidays(self, table_options: dict[str, str]) -> Iterator[dict]:
        """/holidays: optionally bounded by dateFrom/dateTo from table_options.

        Returns no rows on a 404 (feature absent for this tenant).
        """
        params: dict[str, str] = {}
        if "dateFrom" in table_options:
            params["dateFrom"] = table_options["dateFrom"]
        if "dateTo" in table_options:
            params["dateTo"] = table_options["dateTo"]
        body = self._get_json_or_none("holidays", params=params)
        if body is None:
            return
        for raw in self._unwrap_records(body, key="items"):
            yield self._map_record("holidays", raw)

    # ------------------------------------------------------------------
    # CDC reads (full-list + upsert)
    # ------------------------------------------------------------------

    def _read_full_list_cdc(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Full-list refresh for CDC tables without a modifiedAt field.

        actiTIME does not expose a "modified since" filter on customers /
        projects / tasks / users, so the only correct way to capture
        updates and archive flips is to re-list the collection and upsert
        on ``id``. Trigger.AvailableNow termination is signalled with the
        ``{"done": True}`` sentinel — the same pattern used for snapshots.
        """
        if start_offset and start_offset.get("done"):
            return iter([]), start_offset

        records = list(self._read_offset_limit(table_name, table_options, params={}))
        return iter(records), {"done": True}

    # ------------------------------------------------------------------
    # Time-window reads (timetrack / leavetime)
    # ------------------------------------------------------------------

    def _read_time_window(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Sliding date-window read for timetrack and leavetime.

        actiTIME's ``/timetrack`` and ``/leavetime`` endpoints accept
        ``dateFrom`` / ``dateTo`` (inclusive) and a ``userIds`` filter,
        but expose no offset/limit pagination — only ``stopAfter`` as a
        server-side result cap. Capping by record count silently drops
        overflow once a tenant has more activity than ``stopAfter`` (the
        cursor advances to the window end regardless).

        To make the per-call response size predictable independent of
        tenant scale, the connector enumerates users (via the same
        ``/users`` walk used by :meth:`_read_user_rates`) and fans out
        one request per batch of ``user_batch_size`` user IDs (default
        ``100``). ``stopAfter`` is forwarded only when the caller
        explicitly sets it — for the default flow the user batch is the
        natural size bound. Records older than ``lookback_days`` are
        pulled again on each run to capture late edits within the open
        editable week.
        """
        # Resolve the starting cursor in priority order:
        # 1. The previous run's checkpointed offset.
        # 2. A user-supplied ``start_date`` table option.
        # 3. 30 days before init time — keeps the very first run bounded.
        cursor = start_offset.get("cursor") if start_offset else None
        if not cursor:
            cursor = table_options.get("start_date")
        if not cursor:
            cursor = (self._init_date - timedelta(days=30)).isoformat()

        # The effective upper bound is _init_date for /timetrack (logged
        # hours don't exist in the future) but extends forward for
        # /leavetime, where planned leave is a real use case. Operators
        # control the lookahead via ``forward_days`` (default 365).
        if table_name == "leavetime":
            forward_days = max(0, int(table_options.get("forward_days", "365")))
            upper_bound = self._init_date + timedelta(days=forward_days)
        else:
            upper_bound = self._init_date

        cursor_date = _parse_iso_date(cursor)
        if cursor_date >= upper_bound:
            # Caught up to the upper bound — short-circuit so the trigger terminates.
            return iter([]), start_offset or {"cursor": upper_bound.isoformat()}

        # Apply lookback at read-time only (do NOT modify the stored offset).
        lookback_days = int(table_options.get("lookback_days", "7"))
        read_from = cursor_date - timedelta(days=lookback_days)

        window_days = max(1, int(table_options.get("window_days", "7")))
        user_batch_size = max(1, int(table_options.get("user_batch_size", "100")))

        window_end_date = min(
            cursor_date + timedelta(days=window_days), upper_bound
        )
        if window_end_date == upper_bound:
            # Final window of the trigger: include ``upper_bound`` itself.
            # There is no successor window in this trigger and the next call
            # short-circuits on ``cursor_date >= upper_bound``, so no
            # double-fetch is possible. The ``-1 day`` clamp below would
            # otherwise exclude the boundary day, causing same-day rows to
            # lag one trigger.
            window_end_inclusive = window_end_date
        else:
            # Intermediate window. The actiTIME range is inclusive of both
            # endpoints; request [read_from, window_end_date - 1 day] so
            # the boundary day isn't double-fetched when the next window
            # starts at ``dateFrom = window_end_date``.
            window_end_inclusive = max(window_end_date - timedelta(days=1), read_from)

        # Enumerate users for this microbatch. Same call shape as
        # :meth:`_read_user_rates`, paginated by ``_read_offset_limit``.
        # Pass {} so options scoped to the calling table (timetrack /
        # leavetime) don't leak into the users discovery walk — see
        # _read_user_rates for the same pattern.
        user_ids = [
            u.get("id")
            for u in self._read_offset_limit(
                "users", {}, params={}, _raw=True
            )
            if u.get("id") is not None
        ]
        if not user_ids:
            # Empty tenant — still advance the cursor so the framework
            # makes progress instead of looping on an empty window.
            return iter([]), {"cursor": window_end_date.isoformat()}

        base_params = {
            "dateFrom": read_from.isoformat(),
            "dateTo": window_end_inclusive.isoformat(),
        }
        if "stopAfter" in table_options:
            # Caller has explicitly opted into a server-side cap; forward
            # it per batch. Otherwise leave it off — the user batch is the
            # size bound.
            base_params["stopAfter"] = str(table_options["stopAfter"])

        flat: list[dict] = []
        for batch_start in range(0, len(user_ids), user_batch_size):
            batch = user_ids[batch_start : batch_start + user_batch_size]
            params = dict(base_params)
            params["userIds"] = ",".join(str(u) for u in batch)
            body = self._get_json_or_none(table_name, params=params)
            if body is None:
                # 404 — feature absent on this tenant. Terminate the walk
                # by advancing to the upper bound so the next trigger short-
                # circuits cleanly instead of re-issuing the same 404.
                return iter([]), {"cursor": upper_bound.isoformat()}
            flat.extend(self._flatten_time_window_batch(table_name, body))

        # Advance the stored cursor to the end of the window we just read.
        next_offset = {"cursor": window_end_date.isoformat()}
        return iter(flat), next_offset

    def _flatten_time_window_batch(self, table_name: str, body) -> list[dict]:
        """Flatten one ``/timetrack`` or ``/leavetime`` response batch.

        The wire envelope is ``{"dateFrom": ..., "dateTo": ..., "data":
        [...]}``. Inside ``data``:
          * timetrack — each entry is ``{userId, date, records:[{taskId,
            time}, ...]}``. Flatten one row per record, copying
            ``userId`` and ``date`` down. ``dayOffset`` is on the wire
            but intentionally not propagated — see schema docstring.
          * leavetime — each entry is already flat:
            ``{userId, date, leaveTypeId, leaveTime}``. Emit as-is via
            ``_map_record``.
        """
        data = self._unwrap_records(body, key="data")
        out: list[dict] = []
        for env in data:
            if table_name == "timetrack":
                uid = env.get("userId")
                edate = env.get("date")
                for r in env.get("records", []) or []:
                    merged = {
                        **r,
                        "userId": uid,
                        "date": edate,
                    }
                    out.append(self._map_record(table_name, merged))
            else:  # leavetime — flat per-entry.
                out.append(self._map_record(table_name, env))
        return out

    def _read_approval_status(
        self,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Sliding date-window read for /approvalStatus.

        The cursor is ``week_start_date``. Like the time-window tables we
        slice the requested range into windows; this endpoint does not
        expose ``stopAfter``, so we rely on bounded windows alone.
        """
        cursor = start_offset.get("cursor") if start_offset else None
        if not cursor:
            cursor = table_options.get("start_date")
        if not cursor:
            cursor = (self._init_date - timedelta(days=30)).isoformat()

        # /approvalStatus supports approving future-week leave; allow the
        # operator to extend the upper bound past _init_date.
        forward_days = max(0, int(table_options.get("forward_days", "365")))
        upper_bound = self._init_date + timedelta(days=forward_days)

        cursor_date = _parse_iso_date(cursor)
        if cursor_date >= upper_bound:
            return iter([]), start_offset or {"cursor": upper_bound.isoformat()}

        lookback_days = int(table_options.get("lookback_days", "14"))
        read_from = cursor_date - timedelta(days=lookback_days)
        window_days = max(1, int(table_options.get("window_days", "28")))
        window_end_date = min(
            cursor_date + timedelta(days=window_days), upper_bound
        )
        if window_end_date == upper_bound:
            # Final window: include the upper bound itself. See
            # :meth:`_read_time_window` for the boundary-day reasoning.
            window_end_inclusive = window_end_date
        else:
            window_end_inclusive = max(window_end_date - timedelta(days=1), read_from)

        params = {
            "dateFrom": read_from.isoformat(),
            "dateTo": window_end_inclusive.isoformat(),
        }
        body = self._get_json_or_none("approvalStatus", params=params)
        if body is None:
            # 404 — feature absent on this tenant. Advance past the upper
            # bound so the next trigger short-circuits cleanly.
            return iter([]), {"cursor": upper_bound.isoformat()}
        raw_records = self._unwrap_records(body, key="items")
        records = [self._map_record("approvalStatus", r) for r in raw_records]
        next_offset = {"cursor": window_end_date.isoformat()}
        return iter(records), next_offset

    # ------------------------------------------------------------------
    # Offset/limit pagination helper
    # ------------------------------------------------------------------

    def _read_offset_limit(
        self,
        table_name: str,
        table_options: dict[str, str],
        params: dict,
        _raw: bool = False,
    ) -> Iterator[dict]:
        """Paginate an offset/limit list endpoint until the server is drained.

        actiTIME wraps every list response in ``{"items": [...], "offset":
        N, "limit": M}``. We unwrap with :meth:`_unwrap_records` so the
        connector tolerates both the envelope and the legacy bare-array
        shape some endpoints might still produce.

        When ``_raw`` is true the raw API record is yielded (used internally
        for /userRates fan-out); otherwise we run the record through
        :meth:`_map_record` so the output already matches ``TABLE_SCHEMAS``.

        A 404 from a list endpoint is treated as "feature absent on this
        tenant" — iteration ends without raising. See class docstring.
        """
        page_size = int(table_options.get("limit", "1000"))
        # Opt-in emission cap. When unset we drain the endpoint fully: the
        # _read_full_list_cdc / _read_snapshot cursor model is {"done": True}
        # after one call, so any partial drain would silently lose records
        # 100k+ permanently. See PR #176 review thread for follow-up issue.
        # Also gated on ``not _raw`` below so internal lookups never cap.
        max_records = int(table_options.get("max_records_per_batch", str(sys.maxsize)))
        # Surface archived rows too unless the caller has overridden it.
        params = dict(params)
        params.setdefault("archived", "all")

        offset = 0
        emitted = 0
        while True:
            params["offset"] = str(offset)
            params["limit"] = str(page_size)
            body = self._get_json_or_none(table_name, params=params)
            if body is None:
                # 404 — treat as empty.
                return
            records = self._unwrap_records(body, key="items")
            if not records:
                break
            # actiTIME may cap a page below the requested ``limit``; trust the
            # envelope's ``limit`` field so a server cap doesn't break out of
            # the loop after the first page.
            applied_page_size = page_size
            if isinstance(body, dict):
                server_limit = body.get("limit")
                if isinstance(server_limit, int) and server_limit > 0:
                    applied_page_size = server_limit
            for raw in records:
                yield raw if _raw else self._map_record(table_name, raw)
                emitted += 1
                # The emission cap only applies on the primary path. Internal
                # lookups (_raw=True) must drain the endpoint regardless of
                # the calling table's max_records_per_batch — otherwise the
                # inner /users walk for userRates / timetrack / leavetime
                # fan-out silently truncates discovery.
                if not _raw and emitted >= max_records:
                    return
            if len(records) < applied_page_size:
                break
            offset += len(records)

    # ------------------------------------------------------------------
    # Validation & field mapping
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {sorted(TABLE_SCHEMAS)}"
            )

    def _map_record(self, table_name: str, raw: dict) -> dict:
        """Translate an actiTIME JSON record into the connector's row shape.

        camelCase → snake_case, ``allowedActions.*`` flattening, and a few
        per-table tweaks. Type coercion is left to the framework; we only
        rename keys and pull nested values out where the schema expects flat
        columns.
        """
        if table_name == "customers":
            actions = raw.get("allowedActions") or {}
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "description": raw.get("description"),
                "archived": raw.get("archived"),
                "created": _epoch_ms_to_iso(raw.get("created")),
                "url": raw.get("url"),
                "allowed_actions_can_modify": actions.get("canModify"),
                "allowed_actions_can_delete": actions.get("canDelete"),
            }
        if table_name == "projects":
            actions = raw.get("allowedActions") or {}
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "description": raw.get("description"),
                "customer_id": raw.get("customerId"),
                "archived": raw.get("archived"),
                "created": _epoch_ms_to_iso(raw.get("created")),
                "workflow_enabled": raw.get("workflowEnabled"),
                "default_type_of_work_id": raw.get("defaultTypeOfWorkId"),
                "url": raw.get("url"),
                "allowed_actions_can_modify": actions.get("canModify"),
                "allowed_actions_can_delete": actions.get("canDelete"),
            }
        if table_name == "tasks":
            actions = raw.get("allowedActions") or {}
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "description": raw.get("description"),
                "project_id": raw.get("projectId"),
                "customer_id": raw.get("customerId"),
                "archived": raw.get("archived"),
                "created": _epoch_ms_to_iso(raw.get("created")),
                "type_of_work_id": raw.get("typeOfWorkId"),
                "workflow_status_id": raw.get("workflowStatusId"),
                "deadline": raw.get("deadline"),
                "estimated_time": raw.get("estimatedTime"),
                "url": raw.get("url"),
                "allowed_actions_can_modify": actions.get("canModify"),
                "allowed_actions_can_delete": actions.get("canDelete"),
                "allowed_actions_can_complete": actions.get("canComplete"),
            }
        if table_name == "users":
            actions = raw.get("allowedActions") or {}
            return {
                "id": raw.get("id"),
                "first_name": raw.get("firstName"),
                "middle_name": raw.get("middleName"),
                "last_name": raw.get("lastName"),
                "full_name": raw.get("fullName"),
                "username": raw.get("username"),
                "email": raw.get("email"),
                "department_id": raw.get("departmentId"),
                "active": raw.get("active"),
                "time_zone_group_id": raw.get("timeZoneGroupId"),
                "hired": raw.get("hired"),
                "release_date": raw.get("releaseDate"),
                "allowed_actions_can_submit_timetrack": actions.get("canSubmitTimetrack"),
            }
        if table_name == "timetrack":
            return {
                "user_id": raw.get("userId"),
                "date": raw.get("date"),
                "task_id": raw.get("taskId"),
                "time": raw.get("time"),
            }
        if table_name == "leavetime":
            # Live API returns minutes under ``leaveTime`` (not ``time``).
            return {
                "user_id": raw.get("userId"),
                "date": raw.get("date"),
                "leave_type_id": raw.get("leaveTypeId"),
                "time": raw.get("leaveTime"),
            }
        if table_name == "departments":
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "description": raw.get("description"),
                "parent_id": raw.get("parentId"),
                "manager_id": raw.get("managerId"),
            }
        if table_name == "userGroups":
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "description": raw.get("description"),
            }
        if table_name == "userRates":
            raw_rates = raw.get("leaveRates") or []
            leave_rates = [
                {
                    "leave_type_id": item.get("leaveTypeId"),
                    "rate": item.get("rate"),
                }
                for item in raw_rates
                if isinstance(item, dict)
            ]
            return {
                "user_id": raw.get("userId"),
                "date_from": raw.get("dateFrom"),
                "regular_rate": raw.get("regularRate"),
                "overtime_rate": raw.get("overtimeRate"),
                "leave_rates": leave_rates,
            }
        if table_name == "typesOfWork":
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "description": raw.get("description"),
                "archived": raw.get("archived"),
                "billable": raw.get("billable"),
            }
        if table_name == "leaveTypes":
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "description": raw.get("description"),
                "archived": raw.get("archived"),
                "paid_leave": raw.get("paidLeave"),
                "auto_accrual": raw.get("autoAccrual"),
            }
        if table_name == "workflowStatuses":
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "type": raw.get("type"),
                "order": raw.get("order"),
            }
        if table_name == "timeZoneGroups":
            # Wire field is ``timeZoneId`` (e.g. "America/New_York"), not
            # ``timeZone`` — verified against the live API response across
            # all three time-zone groups on the test tenant. Accept the
            # legacy ``timeZone`` key too just in case the API ever varies.
            return {
                "id": raw.get("id"),
                "name": raw.get("name"),
                "time_zone": raw.get("timeZoneId") or raw.get("timeZone"),
            }
        if table_name == "settings":
            return {
                "id": raw.get("id", 1),
                "workday_duration": raw.get("workdayDuration"),
                "week_start_day": raw.get("weekStartDay"),
                "date_format": raw.get("dateFormat"),
                "time_format": raw.get("timeFormat"),
                "currency_code": raw.get("currencyCode"),
                "decimal_separator": raw.get("decimalSeparator"),
                "thousands_separator": raw.get("thousandsSeparator"),
            }
        if table_name == "holidays":
            return {
                "date": raw.get("date"),
                "name": raw.get("name"),
                "time_zone_group_id": raw.get("timeZoneGroupId"),
            }
        if table_name == "approvalStatus":
            return {
                "user_id": raw.get("userId"),
                "week_start_date": raw.get("weekStartDate"),
                "status": raw.get("status"),
                "submitted_at": _epoch_ms_to_iso(raw.get("submittedAt")),
                "approved_at": _epoch_ms_to_iso(raw.get("approvedAt")),
                "approver_id": raw.get("approverId"),
            }
        # Should be unreachable thanks to _validate_table.
        raise ValueError(f"Unsupported table for mapping: {table_name}")


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _epoch_ms_to_iso(value):
    """Convert a Unix-epoch-milliseconds long to an ISO-8601 UTC timestamp.

    Returns ``None`` if *value* is falsy or not numeric so that the framework
    casts it to a SQL NULL.
    """
    if value is None:
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def _parse_iso_date(value: str) -> date:
    """Parse an ISO date or timestamp string back into a ``date`` object."""
    if not value:
        raise ValueError("empty date string")
    # Accept both bare dates ("2026-01-01") and timestamps ("2026-01-01T00:00:00+00:00").
    if "T" in value:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    return date.fromisoformat(value)
