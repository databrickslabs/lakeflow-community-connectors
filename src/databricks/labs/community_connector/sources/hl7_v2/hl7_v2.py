"""HL7 v2 community connector — ingests HL7 messages from multiple source types.

Supported source modes (configured via ``source_type`` connection option):

* ``gcp`` (default) — fetches messages from a Google Cloud Healthcare API
  HL7v2 store via REST.
* ``delta`` — reads pre-loaded messages from a Bronze Delta table.  The table
  must contain columns ``data`` (raw HL7 pipe-delimited text), ``createTime``
  (RFC3339 string), and optionally ``name`` (source identifier).

Each HL7 segment type becomes its own table (msh, pid, pv1, obr, obx, …).

Schemas follow the HL7 v2.9 specification (the latest version, which is a
superset of all prior versions).

Incremental cursor: ``createTime`` (RFC3339 timestamp).
The connector uses a sliding time-window strategy to bound each micro-batch.
"""

from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import service_account as google_sa
from pyspark.sql.types import StructType

from databricks.sdk import WorkspaceClient
from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_extractors import (
    _EXTRACTORS,
    _SINGLE_SEGMENT_TABLES,
    _extract_al1,
    _extract_dg1,
    _extract_evn,
    _extract_ft1,
    _extract_generic,
    _extract_gt1,
    _extract_iam,
    _extract_in1,
    _extract_msh,
    _extract_mrg,
    _extract_nk1,
    _extract_nte,
    _extract_obr,
    _extract_obx,
    _extract_orc,
    _extract_pd1,
    _extract_pid,
    _extract_pr1,
    _extract_pv1,
    _extract_pv2,
    _extract_rxa,
    _extract_sch,
    _extract_spm,
    _extract_txa,
    _metadata,
    _split_messages,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    HL7Message,
    HL7Segment,
    parse_message,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_schemas import (
    SEGMENT_SCHEMAS,
    SEGMENT_TABLES,
    get_schema,
)

# Re-exports — preserve the historical import surface so tests and
# downstream callers can still do ``from .hl7_v2 import _extract_pid``,
# ``_split_messages``, etc.  Don't remove without auditing test imports.
__all__ = [
    "HL7V2LakeflowConnect",
    "_EXTRACTORS",
    "_SINGLE_SEGMENT_TABLES",
    "_extract_al1",
    "_extract_dg1",
    "_extract_evn",
    "_extract_ft1",
    "_extract_generic",
    "_extract_gt1",
    "_extract_iam",
    "_extract_in1",
    "_extract_msh",
    "_extract_mrg",
    "_extract_nk1",
    "_extract_nte",
    "_extract_obr",
    "_extract_obx",
    "_extract_orc",
    "_extract_pd1",
    "_extract_pid",
    "_extract_pr1",
    "_extract_pv1",
    "_extract_pv2",
    "_extract_rxa",
    "_extract_sch",
    "_extract_spm",
    "_extract_txa",
    "_split_messages",
]


_DEFAULT_WINDOW_SECONDS = 86_400
_RETRIABLE_STATUS_CODES = (429, 500, 503)
_MAX_RETRIES = 3
_INITIAL_BACKOFF = 1
_REQUEST_TIMEOUT = 30
_MAX_PAGE_SIZE = 1000


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class HL7V2LakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for HL7 v2 messages.

    Supports two source modes controlled by the ``source_type`` option:

    * ``gcp`` (default) — fetches from a Google Cloud Healthcare API HL7v2 store.
    * ``delta`` — reads from a Bronze Delta table containing pre-loaded HL7
      messages with columns ``data`` (raw text), ``createTime``, and optionally ``name``.

    Each HL7 segment type is a separate table.  Incremental loading is driven
    by ``createTime`` using a sliding time-window.

    GCP mode connection options:
        project_id, location, dataset_id, hl7v2_store_id, service_account_json

    Delta mode connection options:
        delta_table_name (fully-qualified catalog.schema.table)
        delta_query_mode (str): ``"preload"`` (default) loads the entire table
            into memory at init — fast for small tables.  ``"per_window"``
            issues a live SQL query per micro-batch window — scales to
            arbitrarily large tables with no memory overhead.

    Table options (both modes):
        segment_type (str): Override segment type for custom/Z-segments.
        window_seconds (str): Duration of the sliding time-window in seconds
            (default 86400).  Smaller values produce smaller batches.
        start_timestamp (str): RFC3339 timestamp to start reading from when no
            prior offset exists and auto-discovery is not possible.
        max_records_per_batch (str): Hard upper bound on rows yielded by a
            single ``read_table`` call.  Once this many output rows are
            produced, the iterator stops early and the cursor advances only
            up to the last source ``createTime`` actually consumed, so the
            next batch resumes from there.  Use this to bound memory when
            ``window_seconds`` is large or messages are dense.
    """

    _GCP_REQUIRED_KEYS = (
        "project_id",
        "location",
        "dataset_id",
        "hl7v2_store_id",
        "service_account_json",
    )

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._source_type = options.get("source_type", "gcp").lower()
        self._init_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._oldest_create_time: str | None = None

        if self._source_type == "delta":
            self._init_delta(options)
        elif self._source_type == "gcp":
            self._init_gcp(options)
        else:
            raise ValueError(
                f"Unsupported source_type '{self._source_type}'. "
                "Must be 'gcp' or 'delta'."
            )

    def _init_gcp(self, options: dict[str, str]) -> None:
        for key in self._GCP_REQUIRED_KEYS:
            if key not in options:
                raise ValueError(f"'{key}' is required in connector options for source_type 'gcp'.")

        self._base_url = (
            f"https://healthcare.googleapis.com/v1"
            f"/projects/{options['project_id']}"
            f"/locations/{options['location']}"
            f"/datasets/{options['dataset_id']}"
            f"/hl7V2Stores/{options['hl7v2_store_id']}/messages"
        )

        raw_sa = options["service_account_json"]
        sa_info = self._parse_service_account_json(raw_sa)
        self._creds = google_sa.Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self._google_request = google_auth_requests.Request()
        self._session = requests.Session()
        self._creds.refresh(self._google_request)

    _DELTA_REQUIRED_KEYS = (
        "delta_table_name",
        "databricks_host",
        "databricks_token",
        "sql_warehouse_id",
    )

    def _init_delta(self, options: dict[str, str]) -> None:
        for key in self._DELTA_REQUIRED_KEYS:
            if key not in options:
                raise ValueError(
                    f"'{key}' is required in connector options for source_type 'delta'."
                )
        self._delta_table = options["delta_table_name"]
        self._dbx_host = options["databricks_host"]
        self._dbx_token = options["databricks_token"]
        self._sql_warehouse_id = options["sql_warehouse_id"]
        self._delta_query_mode = options.get("delta_query_mode", "preload").lower()

        if self._delta_query_mode == "per_window":
            self._delta_cache = None
            self._delta_preload_error = None
            self._ws_client = self._create_workspace_client()
        elif self._delta_query_mode == "preload":
            self._ws_client = None
            self._delta_cache: list[dict] | None = None
            self._delta_preload_error: str | None = None
            self._preload_delta()
        else:
            raise ValueError(
                f"Unsupported delta_query_mode '{self._delta_query_mode}'. "
                "Must be 'preload' (default) or 'per_window'."
            )

    @staticmethod
    def _parse_service_account_json(raw: str | dict) -> dict:
        """Parse a service-account JSON value that may arrive in several forms.

        UC connection options can deliver the value as:
        * A ``dict`` (already parsed by the framework)
        * A well-formed JSON string
        * A double-serialised JSON string (``'"{\\"type\\":…}"'``)
        * A base64-encoded JSON string (some UI flows encode binary-like values)
        """
        if isinstance(raw, dict):
            return raw

        raw = raw.strip()

        # Attempt 1: direct parse
        try:
            parsed = json.loads(raw, strict=False)
            if isinstance(parsed, dict):
                return parsed
            # Double-serialised — json.loads returned a string, parse again
            if isinstance(parsed, str):
                inner = json.loads(parsed, strict=False)
                if isinstance(inner, dict):
                    return inner
        except (json.JSONDecodeError, TypeError):
            pass

        # Attempt 2: base64-decode then parse
        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            parsed = json.loads(decoded, strict=False)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

        # Attempt 3: find a JSON object embedded in the string
        brace_start = raw.find("{")
        brace_end = raw.rfind("}")
        if brace_start != -1 and brace_end > brace_start:
            try:
                parsed = json.loads(raw[brace_start:brace_end + 1], strict=False)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass

        preview = raw[:120] + ("…" if len(raw) > 120 else "")
        raise ValueError(
            f"Could not parse 'service_account_json' as a JSON object. "
            f"Received value (first 120 chars): {preview!r}. "
            f"Ensure the entire contents of the GCP service account JSON key "
            f"file are pasted into the connection parameter — it should start "
            f"with '{{' and end with '}}'."
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get_headers(self) -> dict[str, str]:
        if not self._creds.valid:
            self._creds.refresh(self._google_request)
        return {"Authorization": f"Bearer {self._creds.token}"}

    def _api_get(self, params: dict[str, str]) -> dict:
        """GET the messages endpoint with retry on transient errors."""
        backoff = _INITIAL_BACKOFF
        last_resp = None
        for attempt in range(_MAX_RETRIES):
            resp = self._session.get(
                self._base_url,
                headers=self._get_headers(),
                params=params,
                timeout=_REQUEST_TIMEOUT,
            )
            last_resp = resp
            if resp.status_code not in _RETRIABLE_STATUS_CODES:
                resp.raise_for_status()
                return resp.json()
            if attempt < _MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        last_resp.raise_for_status()
        return last_resp.json()

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SEGMENT_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name)
        return get_schema(segment_type)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).lower()
        is_known = segment_type in SEGMENT_SCHEMAS
        is_single = segment_type in _SINGLE_SEGMENT_TABLES
        if is_known and not is_single:
            pks = ["message_id", "set_id"]
        else:
            pks = ["message_id"]
        return {
            "ingestion_type": "append",
            "cursor_field": "create_time",
            "primary_keys": pks,
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sliding time-window incremental read.

        Fetches all messages whose ``createTime`` falls in
        ``(since, since + window_seconds]``, parses them, and returns rows
        for the requested segment type.  The cursor advances to the window
        end regardless of whether data was found, ensuring forward progress.

        Works identically for both GCP and Delta source modes — only the
        fetch method differs.
        """
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).upper()

        since = start_offset.get("cursor") if start_offset is not None else None
        if not since:
            since = table_options.get("start_timestamp")
        if not since:
            since = self._peek_oldest_create_time()
        if not since:
            print(
                f"[HL7v2] read_table({table_name}): no start cursor resolved "
                f"(start_offset={start_offset}, source_type={self._source_type}). "
                f"Returning empty."
            )
            return iter([]), start_offset or {}

        if since >= self._init_ts:
            return iter([]), start_offset or {}

        window_seconds = int(table_options.get("window_seconds", str(_DEFAULT_WINDOW_SECONDS)))
        max_records_raw = table_options.get("max_records_per_batch")
        max_records = int(max_records_raw) if max_records_raw else None

        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        window_end_dt = since_dt + timedelta(seconds=window_seconds)
        window_end = min(
            window_end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            self._init_ts,
        )

        if self._source_type == "delta":
            api_messages = self._fetch_messages_from_delta(since, window_end)
        else:
            api_messages = self._fetch_messages_in_window(since, window_end)

        records = self._parse_api_messages(
            api_messages, segment_type, decode_base64=(self._source_type != "delta")
        )

        # Admission control: cap rows yielded per batch.  Cuts only at
        # message boundaries (one HL7 message can produce many rows when
        # the requested segment repeats — e.g. several OBX per ORU), so
        # rows from the same source message stay together.  The cursor
        # rewinds to the createTime of the last fully-consumed message;
        # the next batch resumes from there because the GCP / Delta
        # filter is strict ``createTime > since``.
        if max_records is not None and len(records) > max_records:
            cut = max_records
            # Walk forward until create_time changes — keep all rows
            # belonging to the message that straddles ``max_records``.
            straddle_ct = records[cut - 1].get("create_time")
            while cut < len(records) and records[cut].get("create_time") == straddle_ct:
                cut += 1
            records = records[:cut]
            last_ct = records[-1].get("create_time") if records else None
            if last_ct and last_ct < window_end:
                end_offset = {"cursor": last_ct}
                if start_offset is not None and start_offset == end_offset:
                    return iter([]), start_offset
                return iter(records), end_offset

        end_offset = {"cursor": window_end}
        if start_offset is not None and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str, table_options: dict) -> None:
        if table_name not in SEGMENT_TABLES and "segment_type" not in table_options:
            raise ValueError(
                f"Unknown table '{table_name}'. "
                f"Supported tables: {SEGMENT_TABLES}. "
                "For custom/Z-segments, provide 'segment_type' in table_options."
            )

    def _peek_oldest_create_time(self) -> str | None:
        """Auto-discover the earliest createTime by fetching the first message.

        The result is cached for the lifetime of this connector instance since
        the oldest message never changes once discovered.
        """
        if self._oldest_create_time is not None:
            return self._oldest_create_time

        if self._source_type == "delta":
            self._oldest_create_time = self._peek_oldest_create_time_delta()
            return self._oldest_create_time

        body = self._api_get({
            "view": "FULL",
            "pageSize": "1",
            "orderBy": "sendTime asc",
        })
        messages = body.get("hl7V2Messages", [])
        if messages:
            ts = messages[0].get("createTime")
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                dt -= timedelta(seconds=1)
                ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            self._oldest_create_time = ts
        return self._oldest_create_time

    def _fetch_messages_in_window(self, since: str, until: str) -> list[dict]:
        """Fetch all API messages with createTime in (since, until]."""
        filter_str = f'createTime > "{since}" AND createTime <= "{until}"'
        messages: list[dict] = []
        page_token: str | None = None

        while True:
            params: dict[str, str] = {
                "view": "FULL",
                "pageSize": str(_MAX_PAGE_SIZE),
                "filter": filter_str,
                "orderBy": "sendTime asc",
            }
            if page_token:
                params["pageToken"] = page_token

            body = self._api_get(params)
            batch = body.get("hl7V2Messages", [])
            messages.extend(batch)

            page_token = body.get("nextPageToken")
            if not page_token:
                break

        return messages

    def _create_workspace_client(self):
        return WorkspaceClient(host=self._dbx_host, token=self._dbx_token)

    def _execute_delta_sql(self, stmt: str) -> list[list[str]]:
        """Execute a SQL statement against the Delta table via the Statement Execution API.

        Returns the raw ``data_array`` (list of rows, each row a list of strings).
        """
        w = self._ws_client or self._create_workspace_client()
        result = w.statement_execution.execute_statement(
            warehouse_id=self._sql_warehouse_id,
            statement=stmt,
            wait_timeout="50s",
        )
        while result.status and result.status.state.value in ("PENDING", "RUNNING"):
            time.sleep(1)
            result = w.statement_execution.get_statement(result.statement_id)

        state = getattr(getattr(result, "status", None), "state", None)
        if state and state.value not in ("SUCCEEDED",):
            error_msg = getattr(getattr(result, "status", None), "error", None)
            raise RuntimeError(
                f"Delta SQL statement ended in state '{state.value}'. "
                f"Error: {error_msg}. Statement: {stmt[:200]}"
            )
        if result.result is None:
            raise RuntimeError(
                f"Delta SQL statement returned no result object. "
                f"State: {state}. Statement: {stmt[:200]}"
            )
        return result.result.data_array or []

    def _preload_delta(self) -> None:
        """Pre-load Delta table data via the Databricks SQL Statement Execution API.

        SparkSession is unavailable in both ``DataSource.__init__`` and the
        streaming reader subprocess, so we use the Databricks SDK with explicit
        credentials (``databricks_host``, ``databricks_token``) provided as
        connection parameters — the same pattern used by the GCP mode with
        ``service_account_json``.
        """
        try:
            stmt = (
                f"SELECT data, "
                f"date_format(createTime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS createTime, "
                f"name "
                f"FROM {self._delta_table} "
                f"ORDER BY createTime"
            )
            data_array = self._execute_delta_sql(stmt)
            rows = []
            for row in data_array:
                rows.append({
                    "data": row[0] or "",
                    "createTime": row[1] or "",
                    "name": row[2] if len(row) > 2 else "",
                })
            self._delta_cache = rows
            print(
                f"[HL7v2 Delta] Preloaded {len(rows)} message(s) from "
                f"{self._delta_table}"
            )
        except Exception as exc:
            self._delta_cache = None
            self._delta_preload_error = f"{type(exc).__name__}: {exc}"
            print(
                f"[HL7v2 Delta] ERROR — _preload_delta failed for "
                f"table={self._delta_table}, "
                f"host={self._dbx_host}, "
                f"warehouse={self._sql_warehouse_id}: "
                f"{self._delta_preload_error}"
            )
            raise RuntimeError(
                f"Failed to preload Delta table '{self._delta_table}'. "
                f"Verify that 'databricks_host', 'databricks_token', and "
                f"'sql_warehouse_id' in the connection are valid and the token "
                f"has not expired. Error: {self._delta_preload_error}"
            ) from exc

    def _fetch_messages_from_delta(self, since: str, until: str) -> list[dict]:
        """Fetch messages with createTime in (since, until].

        In ``preload`` mode, filters the in-memory cache.
        In ``per_window`` mode, issues a live SQL query scoped to the window.

        Returns a list of dicts with the same shape as the GCP API response
        (keys: ``data``, ``createTime``, ``name``) so that ``_parse_api_messages``
        works unchanged.
        """
        if self._delta_query_mode == "per_window":
            return self._fetch_messages_from_delta_live(since, until)

        if self._delta_cache is None:
            raise RuntimeError(
                f"Delta cache was not populated. "
                f"Preload error: {self._delta_preload_error}. "
                f"Table: {self._delta_table}"
            )

        return [
            row for row in self._delta_cache
            if since < str(row.get("createTime", "")) <= until
        ]

    def _fetch_messages_from_delta_live(self, since: str, until: str) -> list[dict]:
        """Issue a live SQL query for messages in (since, until]."""
        stmt = (
            f"SELECT data, "
            f"date_format(createTime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS createTime, "
            f"name "
            f"FROM {self._delta_table} "
            f"WHERE date_format(createTime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") > '{since}' "
            f"  AND date_format(createTime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") <= '{until}' "
            f"ORDER BY createTime"
        )
        data_array = self._execute_delta_sql(stmt)
        return [
            {
                "data": row[0] or "",
                "createTime": row[1] or "",
                "name": row[2] if len(row) > 2 else "",
            }
            for row in data_array
        ]

    def _peek_oldest_create_time_delta(self) -> str | None:
        """Return the earliest createTime from the Delta table.

        In ``preload`` mode, reads from the in-memory cache.
        In ``per_window`` mode, issues a ``SELECT MIN(createTime)`` query.
        """
        if self._delta_query_mode == "per_window":
            return self._peek_oldest_create_time_delta_live()

        if self._delta_cache is None:
            print(
                f"[HL7v2 Delta] _peek_oldest_create_time_delta: cache is None. "
                f"Preload error: {self._delta_preload_error}"
            )
            return None
        if len(self._delta_cache) == 0:
            print(
                f"[HL7v2 Delta] _peek_oldest_create_time_delta: cache is empty "
                f"(0 rows returned from {self._delta_table})"
            )
            return None

        first_ts = str(self._delta_cache[0].get("createTime", ""))
        if not first_ts:
            return None

        dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
        dt -= timedelta(seconds=1)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _peek_oldest_create_time_delta_live(self) -> str | None:
        """Discover the earliest createTime via a live SQL query."""
        stmt = (
            f"SELECT date_format(MIN(createTime), \"yyyy-MM-dd'T'HH:mm:ss'Z'\") "
            f"FROM {self._delta_table}"
        )
        data_array = self._execute_delta_sql(stmt)
        if not data_array or not data_array[0] or not data_array[0][0]:
            return None

        first_ts = data_array[0][0]
        dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
        dt -= timedelta(seconds=1)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _parse_api_messages(
        self, api_messages: list[dict], segment_type: str, *, decode_base64: bool = True
    ) -> list[dict]:
        """Decode, parse, and extract rows from message payloads.

        Args:
            decode_base64: When True (GCP mode), the ``data`` field is
                base64-decoded.  When False (Delta mode), ``data`` is
                treated as raw HL7 text.
        """
        records: list[dict] = []

        for msg_data in api_messages:
            send_time = msg_data.get("sendTime", "")
            create_time = msg_data.get("createTime", "") or send_time
            raw_data = msg_data.get("data", "")
            if not raw_data:
                continue
            if decode_base64:
                raw_hl7 = base64.b64decode(raw_data).decode("utf-8", errors="replace")
            else:
                raw_hl7 = raw_data
            source_name = msg_data.get("name", "")

            for msg_text in _split_messages(raw_hl7):
                msg: HL7Message | None = parse_message(msg_text)
                if msg is None:
                    continue
                msh = msg.get_segment("MSH")
                meta = _metadata(msh, source_name, send_time, create_time)

                if segment_type == "MSH":
                    if msh is not None:
                        records.append(meta | _extract_msh(msh) | {"raw_segment": msh.raw_line})
                else:
                    extractor = _EXTRACTORS.get(segment_type.lower(), _extract_generic)
                    is_multi_segment = segment_type.lower() not in _SINGLE_SEGMENT_TABLES
                    for idx, seg in enumerate(msg.get_segments(segment_type), start=1):
                        row = meta | extractor(seg) | {"raw_segment": seg.raw_line}
                        if is_multi_segment and "set_id" not in row:
                            row["set_id"] = idx
                        records.append(row)

        return records
