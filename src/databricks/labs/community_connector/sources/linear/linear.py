"""LakeflowConnect connector for Linear (GraphQL API).

Linear is a single-endpoint GraphQL API. Both supported tables (``issues``
and ``projects``) are ingested incrementally (``cdc``) using the ``updatedAt``
cursor: each read filters ``updatedAt: { gte: <cursor> }``, orders by
``updatedAt`` ascending, and paginates with Relay-style cursors
(``first`` / ``after`` + ``pageInfo.endCursor``).

Linear does not expose a hard-delete event stream, so deletes are not
synchronized. Soft state (``trashed``, ``archivedAt``) is captured as fields,
and ``includeArchived: true`` is always passed so archived records are read.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.linear.linear_schemas import (
    CONNECTION_FIELDS,
    DEFAULT_LOOKBACK_SECONDS,
    DEFAULT_MAX_RECORDS_PER_BATCH,
    DEFAULT_PAGE_SIZE,
    GRAPHQL_URL,
    INITIAL_BACKOFF,
    MAX_PAGE_SIZE,
    MAX_RETRIES,
    OAUTH_TOKEN_URL,
    REQUEST_TIMEOUT_SECONDS,
    RETRIABLE_STATUS_CODES,
    ROOT_FIELD,
    TABLE_METADATA,
    TABLE_QUERIES,
    TABLE_SCHEMAS,
)


class LinearLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Linear GraphQL API."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        # Auth: prefer a personal API key (sent verbatim, no Bearer prefix).
        # Fall back to OAuth refresh-token exchange when no key is supplied.
        self._api_key = options.get("api_key") or options.get("personal_api_key")
        self._client_id = options.get("client_id")
        self._client_secret = options.get("client_secret")
        self._refresh_token = options.get("refresh_token")
        self._oauth_access_token: Optional[str] = None

        if not self._api_key and not (
            self._client_id and self._client_secret and self._refresh_token
        ):
            raise ValueError(
                "Linear connector requires either 'api_key' or the OAuth trio "
                "'client_id' + 'client_secret' + 'refresh_token' in options."
            )

        # Cap reads at construction time so a trigger never chases data that
        # arrives after the run started. The next trigger builds a fresh
        # instance with a newer snapshot. See read_table for how this is used.
        self._init_ts = datetime.now(timezone.utc).isoformat()

        # Tracks tables whose lookback was already applied for this trigger
        # (this instance). Lookback is applied only on the first read of a
        # table per trigger, not on every pagination microbatch.
        self._lookback_done: set[str] = set()

    # --- LakeflowConnect interface ----------------------------------------

    def list_tables(self) -> list[str]:
        """Static list — Linear's object set is fixed by the GraphQL schema."""
        return list(TABLE_SCHEMAS.keys())

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Incremental Relay-cursor read filtered by ``updatedAt``.

        Paginates within a single call until ``max_records_per_batch`` is
        reached or the source has no more pages, then checkpoints the maximum
        ``updatedAt`` seen. Returns ``end_offset == start_offset`` when there
        is nothing new, which signals Trigger.AvailableNow to terminate.
        """
        self._validate_table(table_name)

        since = start_offset.get("cursor") if start_offset else None

        # Short-circuit: already caught up to the init-time snapshot. Returning
        # the unchanged offset terminates the trigger.
        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = int(
            table_options.get("max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH)
        )
        page_size = min(
            int(table_options.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE
        )

        query_since = self._resolve_query_since(table_name, since, table_options)
        graphql_filter = (
            {"updatedAt": {"gte": query_since}} if query_since else None
        )

        query = TABLE_QUERIES[table_name]
        root = ROOT_FIELD[table_name]

        records: list[dict] = []
        after: Optional[str] = None
        while len(records) < max_records:
            variables: dict = {"first": page_size, "after": after}
            if graphql_filter is not None:
                variables["filter"] = graphql_filter

            data = self._graphql(query, variables)
            connection = data.get(root) or {}
            nodes = connection.get("nodes") or []
            records.extend(self._flatten(table_name, n) for n in nodes)

            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            after = page_info.get("endCursor")
            if not after:
                break

        if not records:
            return iter([]), start_offset or {}

        # CDC tables tolerate client-side truncation: the next trigger resumes
        # from the checkpointed cursor and the primary-key upsert dedups any
        # overlap. Records are sorted ascending by updatedAt, so truncating
        # keeps the oldest and checkpoints their max.
        if len(records) > max_records:
            records = records[:max_records]

        last_cursor = max(r["updatedAt"] for r in records if r.get("updatedAt"))
        end_offset = {"cursor": last_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    # --- internals --------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {list(TABLE_SCHEMAS.keys())}"
            )

    def _resolve_query_since(
        self, table_name: str, since: Optional[str], table_options: dict[str, str]
    ) -> Optional[str]:
        """Compute the ``gte`` bound, applying lookback once per trigger.

        The stored offset is always the raw maximum ``updatedAt``. The lookback
        is subtracted only when building the query, and only on the first read
        of a table within this trigger, to avoid re-widening the window on
        every pagination microbatch.
        """
        if not since:
            return None
        if table_name in self._lookback_done:
            return since
        self._lookback_done.add(table_name)

        lookback = int(table_options.get("lookback_seconds", DEFAULT_LOOKBACK_SECONDS))
        if lookback <= 0:
            return since
        return self._subtract_seconds(since, lookback)

    @staticmethod
    def _subtract_seconds(iso_ts: str, seconds: int) -> str:
        """Subtract ``seconds`` from an ISO-8601 timestamp string."""
        try:
            dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        except ValueError:
            return iso_ts
        shifted = dt - timedelta(seconds=seconds)
        return shifted.isoformat().replace("+00:00", "Z")

    @staticmethod
    def _flatten(table_name: str, record: dict) -> dict:
        """Unwrap GraphQL connection envelopes (``{ "nodes": [...] }``) to arrays."""
        for field in CONNECTION_FIELDS.get(table_name, []):
            value = record.get(field)
            if isinstance(value, dict):
                record[field] = value.get("nodes") or []
        return record

    def _auth_header(self) -> dict[str, str]:
        if self._api_key:
            # Personal API keys are sent verbatim, with no Bearer prefix.
            return {"Authorization": self._api_key}
        return {"Authorization": f"Bearer {self._get_oauth_token()}"}

    def _get_oauth_token(self) -> str:
        if self._oauth_access_token:
            return self._oauth_access_token
        resp = self._request_with_retry(
            "POST",
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
            },
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Linear OAuth token exchange failed: {resp.status_code} {resp.text}"
            )
        self._oauth_access_token = resp.json().get("access_token")
        if not self._oauth_access_token:
            raise RuntimeError("Linear OAuth response did not include an access_token")
        return self._oauth_access_token

    def _graphql(self, query: str, variables: dict) -> dict:
        """Execute a GraphQL query and return the ``data`` object.

        Raises on transport errors and on GraphQL-level ``errors``.
        """
        headers = {"Content-Type": "application/json", **self._auth_header()}
        resp = self._request_with_retry(
            "POST",
            GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=headers,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Linear GraphQL request failed: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        if body.get("errors"):
            raise RuntimeError(f"Linear GraphQL returned errors: {body['errors']}")
        return body.get("data") or {}

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Issue an HTTP request, retrying on 429/5xx with exponential backoff."""
        kwargs.setdefault("timeout", REQUEST_TIMEOUT_SECONDS)
        backoff = INITIAL_BACKOFF
        resp = None
        for attempt in range(MAX_RETRIES):
            resp = requests.request(method, url, **kwargs)
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        return resp
