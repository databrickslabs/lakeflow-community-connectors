"""HiBob (Bob) HR platform connector.

Tables
------
* ``employees`` — ``POST /people/search`` (snapshot).
* ``employee_{work,employment,lifecycle}_history`` —
  ``GET /bulk/people/{work|employment|lifecycle}`` with cursor
  pagination, flattened from ``results[].values[]`` (snapshot).
* ``time_off_request_changes`` — ``GET /timeoff/requests/changes`` with a
  ``since``/``to`` range (append, partitioned by time window).
* ``named_lists``, ``employee_fields``, ``custom_tables_metadata`` — small
  reference data (snapshot).
* ``company_report`` — ``GET /company/reports/{reportId}/download?format=csv``
  for the report named by the required ``report_id`` table option
  (snapshot). The schema is dynamic: one nullable string column per CSV
  header label, normalised to snake_case.

Only ``time_off_request_changes`` exposes a range filter, so it is the only
table on the partitioned-stream path; everything else falls back to
``read_table`` via ``is_partitioned() == False``.
"""

from __future__ import annotations

import copy
import csv
import io
import json
import logging
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence
from urllib.parse import quote

import requests
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.hibob.hibob_schemas import (
    BULK_DEFAULT_PAGE_SIZE,
    BULK_JSON_FIELDS,
    BULK_MAX_EMPLOYEE_IDS,
    BULK_MAX_PAGE_SIZE,
    BULK_TABLE_ENDPOINTS,
    COMPANY_REPORT_TABLE,
    DEFAULT_EMPLOYEE_FIELDS,
    DYNAMIC_SCHEMA_TABLES,
    HUMAN_READABLE_MODES,
    INITIAL_BACKOFF_SECONDS,
    MAX_BACKOFF_SECONDS,
    MAX_EMPLOYEE_FIELDS,
    MAX_RETRIES,
    NAMED_LIST_MAX_DEPTH,
    PARTITIONED_TABLES,
    PRODUCTION_BASE_URL,
    REPORT_DOWNLOAD_TIMEOUT_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    RETRIABLE_STATUS_CODES,
    SANDBOX_BASE_URL,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TIME_OFF_DEFAULT_MAX_LOOKBACK_DAYS,
    TIME_OFF_DEFAULT_MAX_RECORDS_PER_BATCH,
    TIME_OFF_DEFAULT_WINDOW_DAYS,
    TIME_OFF_REQUEST_CHANGES_SCHEMA,
)

logger = logging.getLogger(__name__)

_TRUE_VALUES = {"true", "1", "yes", "y", "t"}
_TIME_OFF_KNOWN_FIELDS = {f.name for f in TIME_OFF_REQUEST_CHANGES_SCHEMA.fields} - {
    "additional_fields"
}


class HibobLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for HiBob.

    Connection options:
        service_user_id     Service User ID (Basic auth username). Required.
        service_user_token  Service User token (Basic auth password). Required.
        use_sandbox         "true" to target api.sandbox.hibob.com. Optional.
        base_url            Explicit API base URL override (incl. ``/v1``). Optional.

    Table options:
        employees:
            fields             Comma-separated extra field IDs to request
                               (e.g. custom fields); appended to the defaults.
            show_inactive      Include non-employed people (default "true").
            human_readable     "", "APPEND" (default) or "REPLACE".
        employee_*_history:
            include_archived   Include archived entries (default "false").
            employee_ids       Comma-separated employee IDs to restrict to.
            page_size          Bulk page size, 1-200 (default 200).
        named_lists:
            include_archived   Include archived items (default "false").
        time_off_request_changes:
            start_date         ISO-8601 start for the initial load. Clamped to
                               ``max_lookback_days`` (the API rejects older).
            window_days        Days per partition / window (default 7).
            max_lookback_days  Max history the API allows (default 180).
            include_pending    Include pending requests (default "false").
            max_records_per_batch
                               Best-effort cap for the non-partitioned
                               ``read_table`` path (default 1000). Windows are
                               always fully drained (append-only table).
        company_report:
            report_id          ID of the saved HiBob report to download.
                               Required.
            primary_keys       Comma-separated (or JSON list of) normalised
                               column names to report as primary keys.
                               Optional; when omitted no primary key is
                               returned and the pipeline spec must supply one.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._user_id = options.get("service_user_id")
        self._token = options.get("service_user_token")
        if not self._user_id or not self._token:
            raise ValueError(
                "HiBob connector requires 'service_user_id' and 'service_user_token'."
            )

        base_url = options.get("base_url")
        if not base_url:
            use_sandbox = _as_bool(options.get("use_sandbox"), default=False)
            base_url = SANDBOX_BASE_URL if use_sandbox else PRODUCTION_BASE_URL
        self._base_url = base_url.rstrip("/")

        # Offset cap: a trigger never chases data created after it started.
        self._init_time = _format_ts(datetime.now(timezone.utc))
        self._session_obj: requests.Session | None = None
        # report_id -> (header labels, data rows); avoids downloading the same
        # report twice for schema + metadata + read on one instance.
        self._report_cache: dict[str, tuple[list[str], list[list[str]]]] = {}

    # ------------------------------------------------------------------
    # Pickling: drop the live HTTP session (re-created lazily on executors).
    # ------------------------------------------------------------------

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        state["_session_obj"] = None
        # Do not ship (potentially multi-MB) downloaded reports to executors.
        state["_report_cache"] = {}
        return state

    @property
    def _session(self) -> requests.Session:
        if self._session_obj is None:
            session = requests.Session()
            session.auth = (self._user_id, self._token)
            session.headers.update({"Accept": "application/json"})
            self._session_obj = session
        return self._session_obj

    # ------------------------------------------------------------------
    # LakeflowConnect: schema / metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        if table_name == COMPANY_REPORT_TABLE:
            columns = self._report_columns(table_options)
            return StructType([StructField(c, StringType(), True) for c in columns])
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        if table_name == COMPANY_REPORT_TABLE:
            return self._company_report_metadata(table_options)
        return dict(TABLE_METADATA[table_name])

    # ------------------------------------------------------------------
    # LakeflowConnect: read_table (single-driver path)
    # ------------------------------------------------------------------

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        if table_name == "time_off_request_changes":
            return self._read_time_off_incremental(start_offset, table_options)
        return iter(self._read_snapshot(table_name, table_options)), {}

    def _read_snapshot(self, table_name: str, table_options: dict[str, str]) -> list[dict]:
        if table_name == COMPANY_REPORT_TABLE:
            return self._read_company_report(table_options)
        if table_name == "employees":
            records = self._read_employees(table_options)
        elif table_name in BULK_TABLE_ENDPOINTS:
            records = self._read_bulk(table_name, table_options)
        elif table_name == "named_lists":
            records = self._read_named_lists(table_options)
        elif table_name == "employee_fields":
            records = self._read_employee_fields()
        elif table_name == "custom_tables_metadata":
            records = self._read_custom_tables_metadata()
        else:  # pragma: no cover - guarded by _validate_table
            raise ValueError(f"Unsupported table: {table_name}")
        schema = TABLE_SCHEMAS[table_name]
        return [_empty_structs_to_none(r, schema) for r in records]

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        return table_name in PARTITIONED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """No cheap high-water-mark endpoint exists; the init time is the cap.

        Returning the constant init time (or the committed offset if it is
        already past it) makes consecutive calls stable, which terminates
        ``Trigger.AvailableNow``.
        """
        self._validate_table(table_name)
        if table_name not in PARTITIONED_TABLES:
            # Snapshot tables are not read via the partitioned stream path.
            return {}
        current = (start_offset or {}).get("cursor")
        if current and _parse_ts(current) >= _parse_ts(self._init_time):
            return {"cursor": current}
        return {"cursor": self._init_time}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        self._validate_table(table_name)

        if table_name not in PARTITIONED_TABLES:
            # Snapshot tables: one partition that performs the full read.
            if start_offset is None and end_offset is None:
                return [{"snapshot": True}]
            if start_offset == end_offset:
                return []
            return [{"snapshot": True}]

        if start_offset is not None and start_offset == end_offset:
            return []

        start_cursor = (start_offset or {}).get("cursor")
        end_cursor = (end_offset or {}).get("cursor") or self._init_time
        start_dt = self._resolve_time_off_start(start_cursor, table_options)
        end_dt = _parse_ts(end_cursor)
        if start_dt >= end_dt:
            return []

        window = timedelta(days=_as_int(table_options.get("window_days"),
                                        TIME_OFF_DEFAULT_WINDOW_DAYS, minimum=1))
        partitions: list[dict] = []
        cursor = start_dt
        while cursor < end_dt:
            nxt = min(cursor + window, end_dt)
            partitions.append({"since": _format_ts(cursor), "to": _format_ts(nxt)})
            cursor = nxt
        return partitions

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        self._validate_table(table_name)
        if table_name not in PARTITIONED_TABLES:
            yield from self._read_snapshot(table_name, table_options)
            return
        yield from self._fetch_time_off_window(
            partition["since"], partition["to"], table_options
        )

    # ------------------------------------------------------------------
    # time_off_request_changes
    # ------------------------------------------------------------------

    def _resolve_time_off_start(
        self, cursor: str | None, table_options: dict[str, str]
    ) -> datetime:
        """Start of the (exclusive) range: offset > start_date > max lookback.

        Always clamped to ``init_time - max_lookback_days`` because the API
        errors when ``since`` is older than ~6 months.
        """
        max_lookback = _as_int(table_options.get("max_lookback_days"),
                               TIME_OFF_DEFAULT_MAX_LOOKBACK_DAYS, minimum=1)
        floor = _parse_ts(self._init_time) - timedelta(days=max_lookback)
        raw = cursor or table_options.get("start_date")
        if not raw:
            return floor
        start = _parse_ts(raw)
        if start < floor:
            if cursor:
                logger.warning(
                    "HiBob time_off_request_changes: checkpoint %s is older than the "
                    "API's %d-day lookback limit; changes between %s and %s cannot be "
                    "fetched.", cursor, max_lookback, cursor, _format_ts(floor),
                )
            return floor
        return start

    def _read_time_off_incremental(
        self, start_offset: dict | None, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sliding-window read for the single-driver path.

        Consecutive windows are drained until ``max_records_per_batch`` is
        reached (best-effort: windows are never truncated since the table is
        append-only) or the init-time cap is hit.
        """
        cursor = (start_offset or {}).get("cursor")
        if cursor and _parse_ts(cursor) >= _parse_ts(self._init_time):
            return iter([]), start_offset

        since_dt = self._resolve_time_off_start(cursor, table_options)
        init_dt = _parse_ts(self._init_time)
        if since_dt >= init_dt:
            end_offset = {"cursor": self._init_time}
            return iter([]), start_offset if start_offset else end_offset

        window = timedelta(days=_as_int(table_options.get("window_days"),
                                        TIME_OFF_DEFAULT_WINDOW_DAYS, minimum=1))
        max_records = _as_int(table_options.get("max_records_per_batch"),
                              TIME_OFF_DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1)

        records: list[dict] = []
        window_start = since_dt
        while window_start < init_dt and len(records) < max_records:
            window_end = min(window_start + window, init_dt)
            records.extend(self._fetch_time_off_window(
                _format_ts(window_start), _format_ts(window_end), table_options
            ))
            window_start = window_end

        end_offset = {"cursor": _format_ts(window_start)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _fetch_time_off_window(
        self, since: str, to: str, table_options: dict[str, str]
    ) -> list[dict]:
        """Fetch changes with ``since < createdOn <= to``.

        The server-side filter bounds the scan; the client-side check makes
        adjacent windows disjoint regardless of the API's boundary semantics.
        """
        params = {
            "since": since,
            "to": to,
            "includePending": "true" if _as_bool(
                table_options.get("include_pending"), default=False) else "false",
        }
        body = self._get_json("/timeoff/requests/changes", params=params)
        since_dt, to_dt = _parse_ts(since), _parse_ts(to)
        out: list[dict] = []
        for change in body.get("changes") or []:
            created = change.get("createdOn")
            if created:
                try:
                    created_dt = _parse_ts(created)
                except ValueError:
                    created_dt = None
                if created_dt is not None and not since_dt < created_dt <= to_dt:
                    continue
            out.append(_normalize_time_off_change(change))
        return out

    # ------------------------------------------------------------------
    # employees
    # ------------------------------------------------------------------

    def _read_employees(self, table_options: dict[str, str]) -> list[dict]:
        fields = list(DEFAULT_EMPLOYEE_FIELDS)
        for extra in (table_options.get("fields") or "").split(","):
            extra = extra.strip()
            if extra and extra not in fields:
                fields.append(extra)
        if len(fields) > MAX_EMPLOYEE_FIELDS:
            raise ValueError(
                f"HiBob people search accepts at most {MAX_EMPLOYEE_FIELDS} fields; "
                f"got {len(fields)}."
            )
        human_readable = table_options.get("human_readable", "APPEND")
        human_readable = (human_readable or "").upper()
        if human_readable not in HUMAN_READABLE_MODES:
            raise ValueError(
                f"human_readable must be one of {sorted(HUMAN_READABLE_MODES)}; "
                f"got {human_readable!r}"
            )
        # Never send ``filters`` (an empty list is rejected with 400).
        payload = {
            "fields": fields,
            "showInactive": _as_bool(table_options.get("show_inactive"), default=True),
            "humanReadable": human_readable,
        }
        body = self._request_json("POST", "/people/search", json_body=payload)
        return [_normalize_employee(e) for e in body.get("employees") or []]

    # ------------------------------------------------------------------
    # Bulk history tables
    # ------------------------------------------------------------------

    def _read_bulk(self, table_name: str, table_options: dict[str, str]) -> list[dict]:
        path = f"/bulk/people/{BULK_TABLE_ENDPOINTS[table_name]}"
        page_size = _as_int(table_options.get("page_size"), BULK_DEFAULT_PAGE_SIZE,
                            minimum=1, maximum=BULK_MAX_PAGE_SIZE)
        base_params: dict[str, str] = {
            "limit": str(page_size),
            "includeArchived": "true" if _as_bool(
                table_options.get("include_archived"), default=False) else "false",
        }
        employee_ids = [
            e.strip() for e in (table_options.get("employee_ids") or "").split(",") if e.strip()
        ]
        id_chunks: list[list[str] | None] = (
            [employee_ids[i:i + BULK_MAX_EMPLOYEE_IDS]
             for i in range(0, len(employee_ids), BULK_MAX_EMPLOYEE_IDS)]
            if employee_ids else [None]
        )

        rows: list[dict] = []
        for chunk in id_chunks:
            params = dict(base_params)
            if chunk:
                params["employeeIds"] = ",".join(chunk)
            seen_cursors: set[str] = set()
            while True:
                body = self._get_json(path, params=params)
                for result in body.get("results") or []:
                    emp_id = result.get("employeeId")
                    for entry in result.get("values") or []:
                        rows.append(_normalize_bulk_entry(entry, emp_id))
                _log_bulk_errors(table_name, body.get("errors"))
                next_cursor = (body.get("response_metadata") or {}).get("next_cursor")
                if not next_cursor or next_cursor in seen_cursors:
                    break
                seen_cursors.add(next_cursor)
                # requests URL-encodes query params.
                params["cursor"] = next_cursor
        return rows

    # ------------------------------------------------------------------
    # Reference tables
    # ------------------------------------------------------------------

    def _read_named_lists(self, table_options: dict[str, str]) -> list[dict]:
        params = {
            "includeArchived": "true" if _as_bool(
                table_options.get("include_archived"), default=False) else "false",
        }
        body = self._get_json("/company/named-lists", params=params)
        rows: list[dict] = []
        for named_list in _iter_named_lists(body):
            list_name = named_list.get("name")
            _flatten_list_items(list_name, named_list.get("items") or [], None, rows, 0)
        return rows

    def _read_employee_fields(self) -> list[dict]:
        body = self._get_json("/company/people/fields")
        fields = body.get("fields") if isinstance(body, dict) else body
        out = []
        for field in fields or []:
            rec = dict(field)
            rec["typeData"] = _to_json(rec.get("typeData"))
            out.append(rec)
        return out

    def _read_custom_tables_metadata(self) -> list[dict]:
        body = self._get_json("/people/custom-tables/metadata")
        out = []
        for table in body.get("tables") or []:
            rec = dict(table)
            columns = []
            for col in rec.get("columns") or []:
                col = dict(col)
                col["typeData"] = _to_json(col.get("typeData"))
                columns.append(col)
            rec["columns"] = columns
            out.append(rec)
        return out

    # ------------------------------------------------------------------
    # company_report
    # ------------------------------------------------------------------

    def _company_report_metadata(self, table_options: dict[str, str]) -> dict:
        metadata: dict[str, Any] = {"ingestion_type": "snapshot"}
        raw_pks = table_options.get("primary_keys")
        pks = _parse_name_list(raw_pks)
        if raw_pks not in (None, "") and not pks:
            raise ValueError(
                f"company_report: 'primary_keys' option {raw_pks!r} contains no column names."
            )
        report_id = str(table_options.get("report_id") or "").strip()
        if not report_id:
            # Managed ingestion may call metadata without the connector options;
            # the pipeline spec's primary_keys take over, and report_id is
            # enforced on the schema/read paths that actually download the report.
            if pks:
                metadata["primary_keys"] = pks
            return metadata
        if pks:
            columns = self._report_columns(table_options)
            missing = [pk for pk in pks if pk not in columns]
            if missing:
                raise ValueError(
                    f"company_report: primary_keys {missing} are not columns of report "
                    f"{report_id}. Use normalised column names; available columns: {columns}"
                )
            metadata["primary_keys"] = pks
        return metadata

    def _report_columns(self, table_options: dict[str, str]) -> list[str]:
        header, _ = self._download_report(_require_report_id(table_options))
        return normalize_report_columns(header)

    def _read_company_report(self, table_options: dict[str, str]) -> list[dict]:
        report_id = _require_report_id(table_options)
        header, rows = self._download_report(report_id)
        columns = normalize_report_columns(header)
        width = len(columns)
        out: list[dict] = []
        overflow = 0
        for row in rows:
            if len(row) > width:
                overflow += 1
            values = list(row[:width]) + [""] * (width - len(row))
            out.append({c: (v if v != "" else None) for c, v in zip(columns, values)})
        if overflow:
            logger.warning(
                "HiBob company_report %s: %d row(s) had more cells than the header; "
                "extra cells were dropped.", report_id, overflow,
            )
        return out

    def _download_report(self, report_id: str) -> tuple[list[str], list[list[str]]]:
        """Download a saved report as CSV; returns ``(header, data_rows)``.

        Cached per ``report_id`` on this instance. The body is UTF-8 with an
        optional BOM; fully blank lines are skipped.
        """
        cached = self._report_cache.get(report_id)
        if cached is not None:
            return cached
        path = f"/company/reports/{quote(report_id, safe='')}/download"
        resp = self._request_with_retry(
            "GET",
            path,
            params={"format": "csv"},
            headers={"Accept": "text/csv, */*"},
            timeout=REPORT_DOWNLOAD_TIMEOUT_SECONDS,
        )
        if resp.status_code >= 400:
            hint = {
                401: "invalid service user credentials",
                403: "the service user lacks permission to this report or its fields",
                404: ("report not found, or not shared with the service user; it must "
                      "be visible in GET /company/reports"),
                429: "rate limit exceeded after retries",
            }.get(resp.status_code, "request failed")
            raise RuntimeError(
                f"HiBob API GET {path} returned {resp.status_code} ({hint}): "
                f"{resp.text[:500]}"
            )
        text = resp.content.decode("utf-8-sig")
        reader = csv.reader(io.StringIO(text, newline=""))
        rows = [r for r in reader if r and any(cell != "" for cell in r)]
        if not rows:
            raise RuntimeError(
                f"HiBob report {report_id} download returned no CSV header row."
            )
        result = (rows[0], rows[1:])
        self._report_cache[report_id] = result
        return result

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get_json(self, path: str, params: dict | None = None) -> dict:
        return self._request_json("GET", path, params=params)

    def _request_json(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> dict:
        resp = self._request_with_retry(method, path, params=params, json_body=json_body)
        if resp.status_code >= 400:
            hint = {
                400: "bad request (invalid field, filter or parameter)",
                401: "invalid service user credentials",
                403: "service user lacks the required category permission or audience",
                404: "endpoint not found",
                429: "rate limit exceeded after retries",
            }.get(resp.status_code, "request failed")
            raise RuntimeError(
                f"HiBob API {method} {path} returned {resp.status_code} ({hint}): "
                f"{resp.text[:500]}"
            )
        if not resp.content:
            return {}
        return resp.json()

    def _request_with_retry(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: dict | None = None,
        headers: dict | None = None,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> requests.Response:
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF_SECONDS
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=timeout,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
                continue

            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                time.sleep(_retry_wait_seconds(resp, backoff))
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
        return resp  # type: ignore[return-value]

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS and table_name not in DYNAMIC_SCHEMA_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {SUPPORTED_TABLES}"
            )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_report_column(label: str) -> str:
    """Normalise one report header label to a snake_case, Spark-safe name.

    Accents are stripped (NFKD → ASCII), the text is lower-cased, every run
    of characters outside ``[a-z0-9]`` becomes a single ``_`` and leading /
    trailing underscores are removed. E.g. ``"Employee ID (bob)"`` →
    ``"employee_id_bob"``, ``"Manager's ID"`` → ``"manager_s_id"``. Returns
    ``""`` when nothing alphanumeric remains.
    """
    ascii_label = (
        unicodedata.normalize("NFKD", label or "").encode("ascii", "ignore").decode("ascii")
    )
    return _NON_ALNUM_RE.sub("_", ascii_label.lower()).strip("_")


def normalize_report_columns(labels: Sequence[str]) -> list[str]:
    """Normalise header labels and make them unique.

    Labels that normalise to an empty string become ``column_<n>`` (1-based
    position). Collisions get a numeric suffix in header order: the first
    occurrence keeps the bare name, later ones become ``<name>_2``,
    ``<name>_3``, ... (skipping any suffix that is already taken).
    """
    names: list[str] = []
    used: set[str] = set()
    for position, label in enumerate(labels, start=1):
        base = normalize_report_column(label) or f"column_{position}"
        name = base
        suffix = 2
        while name in used:
            name = f"{base}_{suffix}"
            suffix += 1
        used.add(name)
        names.append(name)
    return names


def _require_report_id(table_options: dict[str, str]) -> str:
    report_id = str(table_options.get("report_id") or "").strip()
    if not report_id:
        raise ValueError(
            "Table 'company_report' requires the 'report_id' table option (the ID of a "
            "saved HiBob report shared with the service user; list them with "
            f"GET /company/reports). Options received: {sorted(table_options)}"
        )
    return report_id


def _parse_name_list(raw: Any) -> list[str]:
    """Parse ``"a,b"`` / ``'["a","b"]'`` / a list into stripped, non-empty names."""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        text = str(raw).strip()
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except ValueError as exc:
                raise ValueError(f"Invalid JSON list for primary_keys: {text!r}") from exc
            items = parsed if isinstance(parsed, list) else [parsed]
        else:
            items = text.split(",")
    out: list[str] = []
    for item in items:
        name = str(item).strip()
        if name and name not in out:
            out.append(name)
    return out


def _retry_wait_seconds(resp: requests.Response, backoff: float) -> float:
    """Honour ``X-RateLimit-Reset`` (Unix ts) then ``Retry-After``, else backoff."""
    reset = resp.headers.get("X-RateLimit-Reset")
    if reset:
        try:
            wait = float(reset) - time.time()
            if wait > 0:
                return min(wait + 0.5, MAX_BACKOFF_SECONDS)
        except (TypeError, ValueError):
            pass
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), MAX_BACKOFF_SECONDS)
        except (TypeError, ValueError):
            pass
    return backoff


def _as_bool(value: Any, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _TRUE_VALUES


def _as_int(value: Any, default: int, minimum: int | None = None,
            maximum: int | None = None) -> int:
    try:
        result = int(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        result = default
    if minimum is not None:
        result = max(minimum, result)
    if maximum is not None:
        result = min(maximum, result)
    return result


def _parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 date / datetime; naive values are treated as UTC."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)) and not value:
        return None
    return json.dumps(value, sort_keys=True, default=str)


def _as_employee_ref(value: Any) -> dict | None:
    if value is None or value == "" or value == {}:
        return None
    if isinstance(value, dict):
        return value
    return {"id": str(value)}


def _empty_structs_to_none(value: Any, data_type: Any) -> Any:
    """Recursively replace ``{}`` with ``None`` for StructType-typed fields."""
    if isinstance(data_type, StructType):
        if not isinstance(value, dict):
            return value
        if not value:
            return None
        out = dict(value)
        for field in data_type.fields:
            if field.name in out:
                out[field.name] = _empty_structs_to_none(out[field.name], field.dataType)
        return out
    if isinstance(data_type, ArrayType) and isinstance(value, list):
        return [_empty_structs_to_none(v, data_type.elementType) for v in value]
    return value


def _normalize_employee(raw: dict) -> dict:
    """Merge flat slash keys (``"/work/title": {"value": ...}``) into nested form.

    ``/root/<x>`` maps to the top level; ``/<category>/<x>`` to
    ``record[category][x]``. Existing nested values take precedence.
    """
    record: dict = copy.deepcopy({k: v for k, v in raw.items() if not k.startswith("/")})
    for key, val in raw.items():
        if not key.startswith("/"):
            continue
        parts = [p for p in key.strip("/").split("/") if p]
        if not parts:
            continue
        if isinstance(val, dict) and "value" in val:
            val = val["value"]
        if parts[0] == "root":
            parts = parts[1:]
            if not parts:
                continue
        cur = record
        for part in parts[:-1]:
            nxt = cur.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                cur[part] = nxt
            cur = nxt
        cur.setdefault(parts[-1], val)

    work = record.get("work")
    if isinstance(work, dict) and "reportsTo" in work:
        work = dict(work)
        work["reportsTo"] = _as_employee_ref(work.get("reportsTo"))
        record["work"] = work
    if record.get("id") is not None:
        record["id"] = str(record["id"])
    record["raw_json"] = json.dumps(raw, sort_keys=True, default=str)
    record["humanReadable"] = _to_json(record.get("humanReadable"))
    return record


def _normalize_bulk_entry(entry: dict, employee_id: Any) -> dict:
    row = dict(entry)
    if employee_id is not None:
        row["employeeId"] = str(employee_id)
    for key in BULK_JSON_FIELDS:
        if key in row:
            row[key] = _to_json(row[key])
    if "reportsTo" in row:
        row["reportsTo"] = _as_employee_ref(row.get("reportsTo"))
    if row.get("change") == {}:
        row["change"] = None
    return row


def _normalize_time_off_change(change: dict) -> dict:
    row = {k: v for k, v in change.items() if k in _TIME_OFF_KNOWN_FIELDS}
    extras = {k: v for k, v in change.items() if k not in _TIME_OFF_KNOWN_FIELDS}
    if "additional_fields" in extras and len(extras) == 1:
        # Already-normalised record (e.g. corpus replay) — keep as-is.
        row["additional_fields"] = _to_json(extras["additional_fields"])
    else:
        row["additional_fields"] = _to_json(extras) if extras else None
    return row


def _iter_named_lists(body: Any) -> Iterator[dict]:
    """Yield list dicts from either ``{"lists": [...]}`` or a name-keyed map."""
    if not isinstance(body, dict):
        return
    lists = body.get("lists", body)
    if isinstance(lists, list):
        for item in lists:
            if isinstance(item, dict):
                yield item
    elif isinstance(lists, dict):
        for name, item in lists.items():
            if isinstance(item, dict):
                if not item.get("name"):
                    item = {**item, "name": name}
                yield item


def _flatten_list_items(
    list_name: Any, items: list, parent_id: str | None, out: list[dict], depth: int
) -> None:
    if depth > NAMED_LIST_MAX_DEPTH:
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        item_id = str(item_id) if item_id is not None else None
        out.append({
            "list_name": list_name,
            "item_id": item_id,
            "value": item.get("value"),
            "name": item.get("name"),
            "archived": item.get("archived"),
            "parent_id": parent_id,
        })
        children = item.get("children") or []
        if children:
            _flatten_list_items(list_name, children, item_id, out, depth + 1)


def _log_bulk_errors(table_name: str, errors: Any) -> None:
    if not errors:
        return
    count = len(errors) if isinstance(errors, list) else 1
    sample = errors[:3] if isinstance(errors, list) else errors
    logger.warning(
        "HiBob %s: %d employee(s) skipped due to per-employee errors "
        "(e.g. MISSING_PERMISSION). Sample: %s", table_name, count, sample,
    )
