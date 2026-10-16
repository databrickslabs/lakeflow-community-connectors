"""Linear connector — GraphQL ingestion for issues and projects.

Linear exposes a single GraphQL endpoint (``POST https://api.linear.app/graphql``)
with Relay-style cursor pagination. Both supported tables (``issues`` and
``projects``) are CDC: incremental upsert keyed on ``id`` with ``updatedAt``
as the cursor.

Why single-driver (standard ``LakeflowConnect``) rather than a partitioned
stream: although Linear's filter syntax supports ``updatedAt`` range queries,
every query hits the *same* GraphQL endpoint, which is protected by a strict
30 req/min per-endpoint burst limit (plus an hourly request + complexity
budget). Partitioning the time range across Spark executors would have all
partitions hammer that one endpoint in parallel and trip the burst limit, so
sequential reads with a sliding watermark are the right fit here.

Auth: a personal API key passed verbatim in the ``Authorization`` header
(no ``Bearer`` prefix). See ``linear_api_doc.md`` for the full reference.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.linear.linear_schemas import (
    DEFAULT_PAGE_SIZE,
    GRAPHQL_CONNECTION,
    GRAPHQL_URL,
    INITIAL_BACKOFF,
    MAX_PAGE_SIZE,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    build_query,
)

_LOG = logging.getLogger(__name__)

_REQUEST_TIMEOUT = 30  # seconds


class LinearLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Linear GraphQL API."""

    def __init__(self, options: dict[str, str]) -> None:
        """Initialize the Linear connector.

        Expected options (one auth mode):
            - api_key: Linear personal API key (preferred). Sent raw in the
              ``Authorization`` header, no ``Bearer`` prefix.
            - access_token: OAuth 2.0 access token (alternative). Sent as
              ``Bearer <token>``.
        """
        super().__init__(options)

        api_key = options.get("api_key")
        access_token = options.get("access_token")
        if not api_key and not access_token:
            raise ValueError(
                "Linear connector requires 'api_key' (personal API key) "
                "or 'access_token' (OAuth)."
            )

        # Personal API keys are sent verbatim; OAuth tokens use Bearer.
        auth_value = api_key if api_key else f"Bearer {access_token}"

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": auth_value,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        # AvailableNow termination guard: cap incremental cursors at the
        # connector's init time so a single trigger only drains data that
        # existed when it started. Records updated after this are picked up
        # by the next trigger, which builds a fresh instance with a newer
        # ``_init_time``. Stored as an ISO-8601 Zulu string so it compares
        # lexicographically against the API's ``updatedAt`` values.
        self._init_time = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

    # ------------------------------------------------------------------ #
    # Interface methods
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Incremental CDC read over ``updatedAt``.

        Within a single ``read_table`` call we walk the Relay ``after``
        cursor across pages until either ``max_records_per_batch`` is
        reached or the source reports ``hasNextPage == false``. The
        checkpointed offset is the max ``updatedAt`` observed; the next
        call resumes from there (inclusive ``gte``), and CDC upsert on the
        ``id`` primary key dedupes any boundary overlap.
        """
        self._validate_table(table_name)
        start_offset = start_offset or {}
        since: str | None = start_offset.get("cursor")

        # Already drained up to init time — return a stable offset so the
        # framework terminates this microbatch (end_offset == start_offset).
        if since and since >= self._init_time:
            return iter([]), start_offset

        max_records = self._parse_max_records(table_options)
        page_size = self._parse_page_size(table_options)

        records: list[dict[str, Any]] = []
        max_seen: str | None = since
        after: str | None = None
        cap_hit = False

        while True:
            connection = self._fetch_page(
                table_name,
                since=since,
                until=self._init_time,
                first=page_size,
                after=after,
            )
            nodes = connection.get("nodes") or []
            for node in nodes:
                records.append(node)
                updated = node.get("updatedAt")
                if updated and (max_seen is None or updated > max_seen):
                    max_seen = updated

            if max_records is not None and len(records) >= max_records:
                cap_hit = True
                break

            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            after = page_info.get("endCursor")
            if not after:
                break

        if cap_hit:
            # Resume from the last-seen watermark on the next microbatch.
            return iter(records), {"cursor": max_seen}

        # Window fully drained for this trigger. Advance the offset to the
        # init-time cap so a follow-up call hits the early-exit above and
        # the trigger converges. Without this, a drained-but-empty window
        # would keep the offset pinned at ``since`` and AvailableNow would
        # never see forward progress.
        end_cursor = (
            max_seen
            if max_seen and max_seen >= self._init_time
            else self._init_time
        )
        return iter(records), {"cursor": end_cursor}

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )

    @staticmethod
    def _parse_max_records(table_options: dict[str, str]) -> int | None:
        raw = table_options.get("max_records_per_batch")
        if raw is None:
            return None
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return None
        return value if value > 0 else None

    @staticmethod
    def _parse_page_size(table_options: dict[str, str]) -> int:
        raw = table_options.get("page_size")
        try:
            value = int(raw) if raw is not None else DEFAULT_PAGE_SIZE
        except (TypeError, ValueError):
            value = DEFAULT_PAGE_SIZE
        return max(1, min(value, MAX_PAGE_SIZE))

    def _fetch_page(
        self,
        table_name: str,
        *,
        since: str | None,
        until: str,
        first: int,
        after: str | None,
    ) -> dict:
        """POST one GraphQL page and return the root connection dict.

        The connection dict has the shape
        ``{"nodes": [...], "pageInfo": {"hasNextPage": bool, "endCursor": str}}``.
        """
        variables: dict[str, Any] = {
            "first": first,
            "after": after,
            "since": since,
            "until": until,
        }
        payload = {"query": build_query(table_name), "variables": variables}

        body = self._post_with_retry(payload)

        errors = body.get("errors")
        if errors:
            raise RuntimeError(
                f"Linear GraphQL returned errors for '{table_name}': {errors}"
            )

        data = body.get("data") or {}
        connection = data.get(GRAPHQL_CONNECTION[table_name])
        if connection is None:
            raise RuntimeError(
                f"Linear GraphQL response missing "
                f"'{GRAPHQL_CONNECTION[table_name]}' for '{table_name}': {body}"
            )
        return connection

    def _post_with_retry(self, payload: dict) -> dict:
        """POST to the GraphQL endpoint, retrying transient failures.

        Retries on HTTP 429/5xx and on GraphQL ``RATELIMITED`` errors
        (which Linear surfaces as an HTTP 400 with a typed extension) using
        exponential backoff.
        """
        backoff = INITIAL_BACKOFF
        last_exc: Exception | None = None

        for attempt in range(MAX_RETRIES):
            resp = self._session.post(
                GRAPHQL_URL, json=payload, timeout=_REQUEST_TIMEOUT
            )

            if resp.status_code in RETRIABLE_STATUS_CODES:
                last_exc = RuntimeError(
                    f"Linear request failed with HTTP {resp.status_code}"
                )
                self._sleep_backoff(attempt, backoff)
                backoff *= 2
                continue

            try:
                body = resp.json()
            except ValueError as exc:
                last_exc = exc
                self._sleep_backoff(attempt, backoff)
                backoff *= 2
                continue

            if self._is_rate_limited(body):
                last_exc = RuntimeError("Linear GraphQL rate limited")
                self._sleep_backoff(attempt, backoff)
                backoff *= 2
                continue

            if resp.status_code >= 400 and not body.get("data"):
                raise RuntimeError(
                    f"Linear request failed with HTTP {resp.status_code}: {body}"
                )

            return body

        raise RuntimeError(
            f"Linear request failed after {MAX_RETRIES} attempts: {last_exc}"
        )

    @staticmethod
    def _is_rate_limited(body: dict) -> bool:
        for err in body.get("errors") or []:
            ext = err.get("extensions") or {}
            if ext.get("type") == "RATELIMITED" or ext.get("code") == "RATELIMITED":
                return True
        return False

    @staticmethod
    def _sleep_backoff(attempt: int, backoff: float) -> None:
        if attempt < MAX_RETRIES - 1:
            time.sleep(backoff)
