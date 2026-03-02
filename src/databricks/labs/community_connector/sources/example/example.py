"""Example connector for the simulated source API."""

import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.libs.simulated_source.api import get_api
from databricks.labs.community_connector.sources.example.example_schemas import (
    INGESTION_TYPE_OVERRIDES,
    INITIAL_BACKOFF,
    MAX_RETRIES,
    METRICS_METADATA,
    METRICS_SCHEMA,
    RETRIABLE_STATUS_CODES,
    build_spark_type,
)


class ExampleLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the simulated Example source."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        username = options.get("username", "default_user")
        password = options.get("password", "default_pass")
        self._api = get_api(username, password)

        # Cap cursors at init time so a trigger never chases new data.
        # The next trigger creates a fresh instance and picks up from here.
        self._init_ts = datetime.now(timezone.utc).isoformat()

    def _request_with_retry(self, method: str, path: str, **kwargs):
        """Issue an API request, retrying on 429/500/503 with exponential backoff."""
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            if method == "GET":
                resp = self._api.get(path, **kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2

        return resp

    def list_tables(self) -> list[str]:
        """Discoverable tables from the API plus the hidden ``metrics`` table."""
        resp = self._request_with_retry("GET", "/tables")
        tables = resp.json()["tables"]
        tables.append("metrics")  # static table that cannot be discovered via the API
        return tables

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Return the Spark schema.  Hard-coded for ``metrics``; fetched from the API otherwise."""
        self._validate_table(table_name)
        if table_name == "metrics":
            return METRICS_SCHEMA

        resp = self._request_with_retry("GET", f"/tables/{table_name}/schema")
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to get schema for '{table_name}': {resp.json()}")
        fields = resp.json()["schema"]
        return StructType([build_spark_type(f) for f in fields])

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return metadata with ingestion type resolved via hard-coded overrides
        then the cursor_field presence fallback."""
        self._validate_table(table_name)
        if table_name == "metrics":
            metadata = dict(METRICS_METADATA)
        else:
            resp = self._request_with_retry("GET", f"/tables/{table_name}/metadata")
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to get metadata for '{table_name}': {resp.json()}")
            metadata = dict(resp.json()["metadata"])

        if table_name in INGESTION_TYPE_OVERRIDES:
            metadata["ingestion_type"] = INGESTION_TYPE_OVERRIDES[table_name]
        elif metadata.get("cursor_field"):
            metadata["ingestion_type"] = "cdc"
        else:
            metadata["ingestion_type"] = "snapshot"

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Route to snapshot or incremental read based on ingestion type."""
        self._validate_table(table_name)
        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata["ingestion_type"]

        if ingestion_type == "snapshot":
            return self._read_snapshot(table_name, table_options)

        cursor_field = metadata.get("cursor_field")
        if table_name == "events":
            return self._read_incremental_by_limit(
                table_name, start_offset, table_options, cursor_field
            )
        if table_name == "metrics":
            return self._read_incremental_by_window(
                table_name, start_offset, table_options, cursor_field
            )
        return self._read_incremental(table_name, start_offset, table_options, cursor_field)

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch deleted-record tombstones.  Only ``orders`` supports this endpoint."""
        self._validate_table(table_name)
        if table_name != "orders":
            raise ValueError(f"Table '{table_name}' does not support deleted records")

        since = start_offset.get("cursor") if start_offset else None
        # Already caught up to init time — skip the API call entirely.
        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = int(table_options.get("max_records_per_batch", "200"))

        records = []
        page = 1
        while len(records) < max_records:
            params = {"page": str(page)}
            if since:
                params["since"] = since

            resp = self._request_with_retry("GET", "/tables/orders/deleted_records", params=params)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to read deleted records for 'orders': {resp.json()}")

            body = resp.json()
            batch = body["records"]
            if not batch:
                break

            records.extend(batch)

            if body["next_page"] is None:
                break
            page = body["next_page"]

        if not records:
            return iter([]), start_offset or {}

        # Records are sorted by cursor — last record has the max.
        last_cursor = records[-1]["updated_at"]
        end_offset = {"cursor": last_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _validate_table(self, table_name: str) -> None:
        supported = self.list_tables()
        if table_name not in supported:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {supported}"
            )

    def _read_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Full-refresh read.  Paginates through all pages and returns offset=None."""
        records = []
        page = 1
        params = {}

        if table_name == "products" and "category" in table_options:
            params["category"] = table_options["category"]

        while True:
            params["page"] = str(page)
            resp = self._request_with_retry("GET", f"/tables/{table_name}/records", params=params)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to read records from '{table_name}': {resp.json()}")

            body = resp.json()
            records.extend(body["records"])

            if body["next_page"] is None:
                break
            page = body["next_page"]

        return iter(records), {}  # no offset for snapshot reads

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Record-count–based incremental read for cdc and cdc_with_deletes tables.

        Respects ``max_records_per_batch`` from *table_options* to limit the
        microbatch size.  Safe for CDC tables where primary-key–based upsert
        semantics tolerate duplicate deliveries.
        """
        since = start_offset.get("cursor") if start_offset else None
        # Already caught up to init time — skip the API call entirely.
        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = int(table_options.get("max_records_per_batch", "200"))

        params = {}
        if since:
            params["since"] = since

        if table_name == "orders":
            for opt_key in ("user_id", "status"):
                if opt_key in table_options:
                    params[opt_key] = table_options[opt_key]

        records = []
        page = 1
        while len(records) < max_records:
            params["page"] = str(page)
            resp = self._request_with_retry("GET", f"/tables/{table_name}/records", params=params)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to read records from '{table_name}': {resp.json()}")

            body = resp.json()
            batch = body["records"]
            if not batch:
                break

            records.extend(batch)

            if body["next_page"] is None:
                break
            page = body["next_page"]

        if not records:
            return iter([]), start_offset or {}

        # Records are sorted by cursor — last record has the max.
        last_cursor = records[-1][cursor_field]
        end_offset = {"cursor": last_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_incremental_by_limit(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Limit-before-fetch incremental read for append_only tables.

        Each API call receives a small ``limit`` so the server controls
        the batch boundary — no client-side truncation.  Calls repeat
        until ``max_records_per_batch`` is reached or the last record
        reaches ``_init_ts``.  The actual total may be approximate since
        we never cut within a server response.
        """
        since = start_offset.get("cursor") if start_offset else None
        # Already caught up to init time — skip the API call entirely.
        if since and since >= self._init_ts:
            return iter([]), start_offset

        limit = int(table_options.get("limit", "50"))
        max_records = int(table_options.get("max_records_per_batch", "200"))

        params = {"limit": str(limit)}
        if since:
            params["since"] = since

        records = []
        page = 1
        while len(records) < max_records:
            params["page"] = str(page)
            resp = self._request_with_retry("GET", f"/tables/{table_name}/records", params=params)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to read records from '{table_name}': {resp.json()}")

            body = resp.json()
            batch = body["records"]
            if not batch:
                break

            records.extend(batch)

            # Stop when the last record reached init time — stream caught up.
            if batch[-1].get(cursor_field, "") >= self._init_ts:
                break

            if body["next_page"] is None:
                break
            page = body["next_page"]

        if not records:
            return iter([]), start_offset or {}

        # Records are sorted by cursor — last record has the max.
        last_cursor = records[-1][cursor_field]
        end_offset = {"cursor": last_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_incremental_by_window(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Sliding time-window incremental read for large-volume tables.

        Queries data in fixed-size windows using ``since``/``until``
        parameters, paginates within each window, then advances the
        cursor to the window end.  The window size is controlled by
        ``window_seconds`` in *table_options* (default 3600).
        ``max_records_per_batch`` caps the records per call as a safety
        bound in case the window contains more data than expected.
        """
        since = start_offset.get("cursor") if start_offset else None
        # Already caught up to init time — skip the API call entirely.
        if since and since >= self._init_ts:
            return iter([]), start_offset

        window_seconds = int(table_options.get("window_seconds", "3600"))
        max_records = int(table_options.get("max_records_per_batch", "200"))

        if since:
            window_end_dt = datetime.fromisoformat(since) + timedelta(seconds=window_seconds)
        else:
            window_end_dt = datetime.now(timezone.utc)

        # Cap window end at init time.
        window_end = min(window_end_dt.isoformat(), self._init_ts)

        params = {"until": window_end}
        if since:
            params["since"] = since

        records = []
        page = 1
        while len(records) < max_records:
            params["page"] = str(page)
            resp = self._request_with_retry("GET", f"/tables/{table_name}/records", params=params)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to read records from '{table_name}': {resp.json()}")

            body = resp.json()
            batch = body["records"]
            if not batch:
                break

            records.extend(batch)

            if body["next_page"] is None:
                break
            page = body["next_page"]

        if not records:
            # No records in this window — advance cursor to window end
            # so the next call slides forward.
            end_offset = {"cursor": window_end}
            if start_offset and start_offset == end_offset:
                return iter([]), start_offset
            return iter([]), end_offset

        # Records are sorted by cursor — last record has the max.
        last_cursor = records[-1][cursor_field]
        end_offset = {"cursor": last_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset
