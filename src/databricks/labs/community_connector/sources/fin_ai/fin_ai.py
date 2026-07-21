"""Lakeflow community connector for Fin.ai (Intercom's Fin AI Agent).

``fin_ai`` is an Intercom REST API connector. Fin is not a standalone data
platform — it is Intercom's AI customer-service agent, and all the data a
Lakeflow connector cares about (conversations Fin participated in, its
resolution outcome and CSAT, plus contacts, companies, tickets, tags,
segments, admins, teams and custom-attribute metadata) is exposed through
the standard Intercom REST API (``api.intercom.io``). Fin-specific analytics
live on the ``conversations`` object via the nested ``ai_agent`` sub-object
and the ``ai_agent_participated`` boolean.

Per-stream sync strategy (from the API research doc):

| Table             | API                              | Ingestion | Cursor       |
|-------------------|----------------------------------|-----------|--------------|
| ``conversations`` | POST /conversations/search       | cdc       | updated_at   |
| ``contacts``      | POST /contacts/search            | cdc       | updated_at   |
| ``tickets``       | POST /tickets/search             | cdc       | updated_at   |
| ``companies``     | GET /companies/scroll            | snapshot  | —            |
| ``admins``        | GET /admins                      | snapshot  | —            |
| ``tags``          | GET /tags                        | snapshot  | —            |
| ``segments``      | GET /segments                    | snapshot  | —            |
| ``data_attributes`` | GET /data_attributes           | snapshot  | —            |
| ``teams``         | GET /teams                       | snapshot  | —            |

Partitioned streaming: the three Search-API tables support ``updated_at``
range queries, which fits ``SupportsPartitionedStream`` naturally —
``latest_offset`` returns an init-time snapshot cursor and ``get_partitions``
splits the ``(start, end]`` range into independent time windows that
executors query in parallel. The six snapshot tables opt out of partitioning
(``is_partitioned`` returns ``False``) and run on the single-driver
``read_table`` path.

Termination: ``latest_offset`` is capped at the connector's init time
(a fixed value across a trigger run), so once the stream catches up to that
snapshot ``latest_offset`` stops advancing and ``Trigger.AvailableNow``
terminates. A later trigger creates a fresh instance with a newer cap and
picks up anything modified since.
"""

import time
from datetime import datetime, timezone
from typing import Iterator, Sequence

import requests
from requests.exceptions import RequestException
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.fin_ai.fin_ai_schemas import (
    BASE_URLS,
    DEFAULT_PER_PAGE,
    DEFAULT_TIMEOUT,
    DEFAULT_WINDOW_DAYS,
    INITIAL_BACKOFF,
    INTERCOM_VERSION,
    MAX_PER_PAGE,
    MAX_RETRIES,
    MAX_SCROLL_PAGES,
    PARTITIONED_TABLES,
    RETRIABLE_STATUS_CODES,
    SEARCH_ENDPOINTS,
    SNAPSHOT_ENDPOINTS,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

_SECONDS_PER_DAY = 86_400


class FinAiLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Fin.ai (Intercom REST API)."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        self._access_token = options.get("access_token")
        if not self._access_token:
            raise ValueError(
                "Fin.ai connector requires 'access_token' (an Intercom private "
                "app Access Token) in options"
            )

        region = (options.get("region") or "us").lower()
        if region not in BASE_URLS:
            raise ValueError(
                f"Unsupported region {region!r}; expected one of {sorted(BASE_URLS)}"
            )
        self._base_url = BASE_URLS[region]

        # Optional connection-level lower bound for incremental backfills.
        self._start_epoch = self._parse_start_date(options.get("start_date"))

        # Re-read the last N days of each window every sync to catch late
        # ``updated_at`` mutations (mirrors Airbyte's lookback_window option).
        self._lookback_seconds = (
            self._non_negative_int(options.get("lookback_window"), 0) * _SECONDS_PER_DAY
        )

        # Self-throttle budget (requests/minute). Retained for parity with the
        # documented option; 429s are handled via Retry-After/X-RateLimit-Reset.
        self._api_rate_limit = self._non_negative_int(
            options.get("api_rate_limit"), 9500
        )

        # Freeze the upper bound at init time so incremental offsets stabilise
        # across microbatches within one Trigger.AvailableNow run.
        self._init_epoch = int(datetime.now(timezone.utc).timestamp())

    # ------------------------------------------------------------------
    # LakeflowConnect: schema / metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Static object list — Intercom has no single discovery endpoint."""
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

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        """Only the Search-API tables support range-query partitioning."""
        return table_name in PARTITIONED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the most recent offset available, capped at init time.

        Returning a fixed value (the init-time snapshot) across a trigger run
        is what lets Trigger.AvailableNow terminate: once the committed offset
        reaches this cap, subsequent calls return the same value and
        ``get_partitions`` yields no work.
        """
        self._validate_table(table_name)
        return {"cursor": self._init_epoch}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split the ``(start, end]`` ``updated_at`` range into time windows.

        Batch reads (``start_offset``/``end_offset`` both ``None``) partition
        the whole table from the configured floor up to the init-time cap.
        """
        self._validate_table(table_name)

        window_days = self._positive_int(
            table_options.get("window_days"), DEFAULT_WINDOW_DAYS
        )
        window_seconds = window_days * _SECONDS_PER_DAY

        lower = self._resolve_lower(start_offset)
        upper = self._resolve_upper(end_offset)
        if lower >= upper:
            return []

        # First-run optimisation: with no floor at all (no prior offset and no
        # start_date), a single open-ended partition avoids splitting all of
        # history back to the epoch into thousands of windows.
        if lower == 0:
            return [{"since": 0, "until": upper}]

        # Apply the lookback at partition-build time (not in the stored offset)
        # so the checkpointed cursor doesn't drift while still re-checking
        # recently-updated records.
        if self._lookback_seconds:
            lower = max(0, lower - self._lookback_seconds)
            if lower >= upper:
                return []

        partitions: list[dict] = []
        cursor = lower
        while cursor < upper:
            nxt = min(cursor + window_seconds, upper)
            partitions.append({"since": cursor, "until": nxt})
            cursor = nxt
        return partitions

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one ``(since, until]`` window of records on an executor."""
        self._validate_table(table_name)
        path, records_key = SEARCH_ENDPOINTS[table_name]
        since = int(partition["since"])
        until = int(partition["until"])
        yield from self._search_paginated(
            path, records_key, since, until, table_options
        )

    # ------------------------------------------------------------------
    # LakeflowConnect: read_table (single-driver path)
    # ------------------------------------------------------------------

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver read.

        Used by ``simpleStreamReader`` for the non-partitioned snapshot tables,
        and as a fallback for the Search-API tables when partitioning is not
        used.
        """
        self._validate_table(table_name)
        start_offset = start_offset or {}

        if table_name in PARTITIONED_TABLES:
            return self._read_search_fallback(table_name, start_offset, table_options)
        if table_name == "companies":
            return self._read_companies(start_offset, table_options)
        return self._read_snapshot(table_name, start_offset, table_options)

    # ------------------------------------------------------------------
    # Search-API reads
    # ------------------------------------------------------------------

    def _read_search_fallback(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fallback single-driver incremental read for a Search-API table.

        Reads everything from the prior cursor up to the init-time cap in one
        batch, then advances the offset to the cap. The next call sees the
        cursor already at/past the cap and returns an empty batch with the same
        offset, terminating the trigger.
        """
        since = self._resolve_lower(start_offset)
        if since >= self._init_epoch:
            return iter([]), {"cursor": self._init_epoch}

        path, records_key = SEARCH_ENDPOINTS[table_name]
        query_since = max(0, since - self._lookback_seconds) if since > 0 else 0
        records = list(
            self._search_paginated(
                path, records_key, query_since, self._init_epoch, table_options
            )
        )
        return iter(records), {"cursor": self._init_epoch}

    def _search_paginated(
        self,
        path: str,
        records_key: str,
        since: int,
        until: int,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Yield records from a POST search endpoint across cursor pages.

        The query bounds ``updated_at`` to ``(since, until]`` — an exclusive
        lower bound so back-to-back windows are disjoint, and an inclusive upper
        bound at the window end. ``since == 0`` means "beginning of time" and
        only the upper bound is applied.
        """
        per_page = self._positive_int(
            table_options.get("per_page"), DEFAULT_PER_PAGE
        )
        per_page = min(per_page, MAX_PER_PAGE)

        starting_after: str | None = None
        while True:
            filters = [{"field": "updated_at", "operator": "<=", "value": until}]
            if since > 0:
                filters.append(
                    {"field": "updated_at", "operator": ">", "value": since}
                )
            body: dict = {
                "query": {"operator": "AND", "value": filters},
                # ``sort`` is undocumented but relied on in production (Airbyte)
                # to make paginated incremental reads deterministic.
                "sort": {"field": "updated_at", "order": "ascending"},
                "pagination": {"per_page": per_page},
            }
            if starting_after:
                body["pagination"]["starting_after"] = starting_after

            resp = self._request_with_retry("POST", path, json_body=body)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Intercom search failed for {path}: "
                    f"{resp.status_code} {resp.text[:500]}"
                )

            data = resp.json()
            for record in self._extract_records(data, records_key):
                yield record

            starting_after = self._next_starting_after(data)
            if not starting_after:
                return

    # ------------------------------------------------------------------
    # Snapshot reads
    # ------------------------------------------------------------------

    def _read_companies(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Full-refresh read of companies via the Scroll API.

        The Scroll API iterates the entire company list with an opaque
        ``scroll_param``; there is no server-side time filter. All pages are
        drained in one ``read_table`` call and a ``{"done": True}`` sentinel is
        returned so a follow-up call in the same trigger short-circuits and the
        trigger terminates.
        """
        if start_offset.get("done"):
            return iter([]), start_offset

        records: list[dict] = []
        scroll_param: str | None = None
        for _ in range(MAX_SCROLL_PAGES):
            params: dict[str, str] = {}
            if scroll_param:
                params["scroll_param"] = scroll_param

            resp = self._request_with_retry("GET", "/companies/scroll", params=params)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Intercom companies/scroll failed: "
                    f"{resp.status_code} {resp.text[:500]}"
                )

            body = resp.json()
            batch = self._extract_records(body, "data")
            if not batch:
                break
            records.extend(batch)

            scroll_param = body.get("scroll_param")
            if not scroll_param:
                break

        return iter(records), {"done": True}

    def _read_snapshot(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Full-refresh read of a simple single-call GET snapshot table."""
        if start_offset.get("done"):
            return iter([]), start_offset

        path, records_key = SNAPSHOT_ENDPOINTS[table_name]
        params: dict[str, str] = {}
        if table_name == "data_attributes":
            # Omitting ``model`` returns attributes for all models in one call;
            # each record carries its own ``model`` field.
            params["include_archived"] = table_options.get("include_archived", "false")
        elif table_name == "segments" and "include_count" in table_options:
            params["include_count"] = table_options["include_count"]
        elif table_name == "admins":
            params["display_avatar"] = table_options.get("display_avatar", "false")

        resp = self._request_with_retry("GET", path, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Intercom {path} failed: {resp.status_code} {resp.text[:500]}"
            )
        records = self._extract_records(resp.json(), records_key)
        return iter(records), {"done": True}

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Intercom-Version": INTERCOM_VERSION,
        }

    def _request_with_retry(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> requests.Response:
        """Issue an HTTP request, retrying transient failures with backoff.

        Fresh ``requests`` calls are used per request (no cached ``Session``)
        so the connector instance stays picklable when shipped to executors.
        Rate limiting (429) honours ``Retry-After`` and falls back to
        ``X-RateLimit-Reset`` when present.
        """
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF
        resp: requests.Response | None = None
        last_exc: RequestException | None = None

        for attempt in range(MAX_RETRIES):
            try:
                if method == "GET":
                    resp = requests.get(
                        url,
                        headers=self._headers(),
                        params=params,
                        timeout=DEFAULT_TIMEOUT,
                    )
                elif method == "POST":
                    resp = requests.post(
                        url,
                        headers=self._headers(),
                        params=params,
                        json=json_body,
                        timeout=DEFAULT_TIMEOUT,
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
            except RequestException as exc:
                last_exc = exc
                resp = None
            else:
                if resp.status_code not in RETRIABLE_STATUS_CODES:
                    return resp

            if attempt < MAX_RETRIES - 1:
                time.sleep(self._retry_delay(resp, backoff))
                backoff *= 2

        if resp is None and last_exc is not None:
            raise RuntimeError(
                f"Intercom request failed after {MAX_RETRIES} attempts for "
                f"{method} {path}: {last_exc}"
            ) from last_exc
        return resp  # type: ignore[return-value]

    @staticmethod
    def _retry_delay(resp: requests.Response | None, backoff: float) -> float:
        """Pick a retry delay, preferring server throttling hints."""
        if resp is None:
            return backoff
        retry_after = resp.headers.get("Retry-After", "").strip()
        if retry_after:
            try:
                return max(backoff, float(retry_after))
            except ValueError:
                pass
        # 429 with a reset timestamp: wait until the window resets.
        if resp.status_code == 429:
            reset = resp.headers.get("X-RateLimit-Reset", "").strip()
            if reset:
                try:
                    wait = float(reset) - time.time()
                    if wait > 0:
                        return max(backoff, min(wait, 60.0))
                except ValueError:
                    pass
        return backoff

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )

    def _resolve_lower(self, start_offset: dict | None) -> int:
        """Resolve the exclusive lower cursor bound (Unix seconds)."""
        if start_offset:
            cursor = start_offset.get("cursor")
            if cursor is not None:
                return int(cursor)
        if self._start_epoch is not None:
            return self._start_epoch
        return 0

    def _resolve_upper(self, end_offset: dict | None) -> int:
        """Resolve the inclusive upper cursor bound (Unix seconds)."""
        if end_offset:
            cursor = end_offset.get("cursor")
            if cursor is not None:
                return int(cursor)
        return self._init_epoch

    @staticmethod
    def _extract_records(body, records_key: str) -> list[dict]:
        """Extract a record list from a wrapped or bare-list response.

        Intercom's list/search envelopes vary by resource (``conversations`` /
        ``tickets`` / ``data`` / ``segments`` / ...), so try the expected key
        first, then the common fallbacks, then any top-level list value.
        """
        if isinstance(body, list):
            return body
        if not isinstance(body, dict):
            return []
        value = body.get(records_key)
        if isinstance(value, list):
            return value
        for fallback in ("data", "conversations", "tickets", "contacts"):
            value = body.get(fallback)
            if isinstance(value, list):
                return value
        return []

    @staticmethod
    def _next_starting_after(body) -> str | None:
        """Read the next cursor from ``pages.next`` (dict or URL string)."""
        if not isinstance(body, dict):
            return None
        pages = body.get("pages")
        if not isinstance(pages, dict):
            return None
        nxt = pages.get("next")
        if isinstance(nxt, dict):
            return nxt.get("starting_after")
        if isinstance(nxt, str) and nxt:
            return nxt
        return None

    @staticmethod
    def _parse_start_date(value: str | None) -> int | None:
        """Parse an ISO-8601 ``start_date`` into Unix seconds, or ``None``."""
        if not value:
            return None
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.astimezone(timezone.utc).timestamp())

    @staticmethod
    def _positive_int(raw: str | None, default_value: int) -> int:
        try:
            value = int(raw) if raw is not None else default_value
        except (TypeError, ValueError):
            return default_value
        return value if value > 0 else default_value

    @staticmethod
    def _non_negative_int(raw: str | None, default_value: int) -> int:
        try:
            value = int(raw) if raw is not None else default_value
        except (TypeError, ValueError):
            return default_value
        return value if value >= 0 else default_value
