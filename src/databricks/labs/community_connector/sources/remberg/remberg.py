"""remberg source connector for Lakeflow Community Connectors.

Implements the :class:`LakeflowConnect` interface against the remberg public
REST API (https://developers.remberg.de). remberg is an asset-centric
maintenance / field-service platform; nine tables are exposed:

    CDC (incremental): assets, work_orders, tickets, work_requests,
                       organizations, parts
    Snapshot:          contacts, users, forms

Authentication is a static API key sent in an HTTP header literally named
``authorization`` (no ``Bearer`` prefix — this is what the official OpenAPI
security scheme declares: ``type: apiKey, in: header, name: authorization``).
Keys are created under Settings > Data > API in the remberg web app and
expire after one year.

Every list endpoint paginates with 1-indexed ``page`` + ``limit`` (default
20, max 1000) query parameters; the last page is detected by receiving fewer
than ``limit`` records. The response envelope key varies per resource
(``data`` for most, ``tickets``/``organizations``/``contacts`` for those
resources) — see ``TABLE_ENDPOINTS``.

Incremental strategy (the six CDC tables): the endpoints accept inclusive
``updatedAtFrom`` / ``updatedAtUntil`` ISO-8601 filters on the record's
``updatedAt``. Each trigger reads the bounded range
``[cursor - lookback_seconds, _init_ts]`` page by page; when a page comes
back short the range is drained and the cursor advances to the range's upper
bound. Server-side ``updatedAt`` sorting is not available on all of these
endpoints, but draining the bounded range makes result order irrelevant. The
upper bound is pinned at ``_init_ts`` (captured in ``__init__``) so
Trigger.AvailableNow always terminates, and pinned inside the offset while a
range is being paginated so a ``max_records_per_batch`` split cannot shift
pages between microbatches. Lookback is applied at read time only — never
stored — to re-capture records whose ``updatedAt`` moved past the range's
upper bound while it was being paged.

Snapshot tables (``contacts``, ``users``, ``forms``) carry no usable cursor
field on their list records (contacts/users have no ``updatedAt`` at all;
forms has one but the endpoint cannot filter on it), so they are re-listed
in full each trigger and upserted on ``id``. Termination uses the
``{"done": True}`` sentinel per the ``end_offset == start_offset`` contract.

Rate limits are strict and shape the connector: 10 requests/1 s (burst) and
25 requests/5 s (base), enforced simultaneously per user AND per endpoint,
and 429 responses count against the limit. The connector therefore spaces
requests to the same endpoint at least ``MIN_REQUEST_INTERVAL`` apart and
honours the ``Retry-After-Burst`` / ``Retry-After-Base`` headers on 429.
These limits are also why this is a standard (non-partitioned) connector —
parallel readers against a 25-requests-per-5-seconds budget only
manufacture 429s.

remberg exposes no deleted-records feed, so deletes do not propagate
(``cdc``, not ``cdc_with_deletes``).

Known OpenAPI documentation bugs worked around here (see
``remberg_api_doc.md`` for details): ``tickets.createdAt/updatedAt``,
``assets.installationDate`` and ``work_orders.dueDate`` are declared as
free-form objects in the official spec but are ISO date-time strings on the
wire; they are typed as timestamps.
"""

import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from urllib.parse import urljoin

import requests
from pyspark.sql.types import (
    ArrayType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

logger = logging.getLogger(__name__)


DEFAULT_BASE_URL = "https://api.remberg.de"

# HTTP retry knobs — used by the lightweight retry helper.
INITIAL_BACKOFF = 1.0
MAX_RETRIES = 5
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_TIMEOUT = 30

# Client-side throttle. remberg enforces 10 req/1s (burst) and 25 req/5s
# (base) per endpoint; official guidance is ≤ 4-5 req/s. 0.25 s spacing
# stays under both windows with headroom.
MIN_REQUEST_INTERVAL = 0.25

# ``limit`` query param: remberg default is 20, server max is 1000.
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 1000

# Read-time lookback for CDC ranges. Records updated while a bounded range
# is being paginated fall out of the range's filter (their new ``updatedAt``
# exceeds the pinned upper bound) and can shift pagination; re-reading a
# small overlap on the next trigger re-captures them. Upserts make the
# overlap harmless.
DEFAULT_LOOKBACK_SECONDS = 300


# Table → (endpoint path, response envelope key holding the records array).
# The envelope key is inconsistent across remberg resources: most wrap in
# ``data`` but tickets/organizations/contacts use a resource-named key.
TABLE_ENDPOINTS: dict[str, tuple[str, str]] = {
    "assets": ("v2/assets", "data"),
    "work_orders": ("v2/work-orders", "data"),
    "tickets": ("v2/tickets", "tickets"),
    "work_requests": ("v1/work-requests", "data"),
    "organizations": ("v1/organizations", "organizations"),
    "parts": ("v2/parts", "data"),
    "contacts": ("v1/contacts", "contacts"),
    "users": ("v1/users", "data"),
    "forms": ("v1/forms", "data"),
}


# Ingestion-type metadata — primary keys and cursor fields per table.
# Kept as a module-level constant so it stays in sync with TABLE_SCHEMAS.
TABLE_METADATA: dict[str, dict] = {
    # CDC tables: server-side updatedAtFrom/updatedAtUntil filter plus an
    # ``updatedAt`` field on every record.
    "assets": {"primary_keys": ["id"], "cursor_field": "updatedAt", "ingestion_type": "cdc"},
    "work_orders": {"primary_keys": ["id"], "cursor_field": "updatedAt", "ingestion_type": "cdc"},
    "tickets": {"primary_keys": ["id"], "cursor_field": "updatedAt", "ingestion_type": "cdc"},
    "work_requests": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "organizations": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "parts": {"primary_keys": ["id"], "cursor_field": "updatedAt", "ingestion_type": "cdc"},
    # Snapshot tables. ``contacts`` list records carry no ``updatedAt``
    # (even though the endpoint accepts the filter), ``users`` records have
    # no timestamps at all, and ``/v1/forms`` cannot filter on ``updatedAt``
    # (only ``finalizedAt*``) — so none of them has a usable cursor.
    "contacts": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "users": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "forms": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
}


def _build_schemas() -> dict[str, StructType]:
    """Return the static schema dictionary.

    Field names are kept exactly as the API returns them (camelCase) so
    rows map 1:1 to the official remberg API documentation. Schemas come
    from the response DTOs in the official OpenAPI files — see
    ``remberg_api_doc.md`` for the extraction notes and known doc bugs.
    """

    address = StructType(
        [
            StructField("company", StringType()),
            StructField("street", StringType()),
            StructField("streetNumber", StringType()),
            StructField("zipPostCode", StringType()),
            StructField("city", StringType()),
            StructField("countryProvince", StringType()),
            StructField("country", StringType()),
            StructField("other", StringType()),
        ]
    )
    phone_number = StructType(
        [
            StructField("number", StringType()),
            StructField("countryPrefix", StringType()),
        ]
    )
    person_ref = StructType(
        [
            StructField("id", StringType()),
            StructField("firstName", StringType()),
            StructField("lastName", StringType()),
            StructField("email", StringType()),
        ]
    )

    return {
        "assets": StructType(
            [
                StructField("id", StringType()),
                StructField("assetNumber", StringType()),
                StructField("assetType", StringType()),
                StructField("assetCategory", StringType()),
                StructField("assetTypeId", StringType()),
                StructField("name", StringType()),
                StructField("createdAt", TimestampType()),
                StructField("updatedAt", TimestampType()),
                StructField("location", address),
                StructField("criticality", StringType()),
                StructField("status", StringType()),
                # Declared as a free-form object in the OpenAPI spec (doc
                # bug); an ISO date-time string on the wire.
                StructField("installationDate", TimestampType()),
            ]
        ),
        "work_orders": StructType(
            [
                StructField("id", StringType()),
                StructField("createdAt", TimestampType()),
                StructField("createdByType", StringType()),
                StructField("updatedAt", TimestampType()),
                StructField("counter", StringType()),
                StructField("subject", StringType()),
                StructField("parentWorkOrderId", StringType()),
                StructField("relatedOrganizationId", StringType()),
                StructField("externalReference", StringType()),
                StructField("statusReference", StringType()),
                StructField("typeReference", StringType()),
                # Free-form object in the OpenAPI spec (doc bug) — ISO
                # date-time string on the wire.
                StructField("dueDate", TimestampType()),
            ]
        ),
        "tickets": StructType(
            [
                StructField("id", StringType()),
                StructField("subject", StringType()),
                # Both declared as free-form objects in the OpenAPI spec
                # (doc bug) — ISO date-time strings on the wire, and
                # ``updatedAt`` is the CDC cursor.
                StructField("createdAt", TimestampType()),
                StructField("updatedAt", TimestampType()),
                StructField("status", StringType()),
                StructField("ticketID", StringType()),
                StructField("priority", StringType()),
                StructField("assignedPersonId", StringType()),
                StructField("assignedPerson", person_ref),
                StructField("summary", StringType()),
                StructField("solution", StringType()),
                StructField("resolutionTime", DoubleType()),
                StructField("relatedOrganizationIds", ArrayType(StringType())),
                StructField(
                    "relatedOrganizations",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("organizationName", StringType()),
                                StructField("organizationNumber", StringType()),
                            ]
                        )
                    ),
                ),
                StructField("relatedContactIds", ArrayType(StringType())),
                StructField("relatedContacts", ArrayType(person_ref)),
                StructField("relatedAssetIds", ArrayType(StringType())),
                StructField(
                    "relatedAssets",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("assetNumber", StringType()),
                                StructField("assetType", StringType()),
                            ]
                        )
                    ),
                ),
                StructField(
                    "relatedParts",
                    ArrayType(
                        StructType(
                            [
                                StructField("partId", StringType()),
                                StructField("quantity", DoubleType()),
                            ]
                        )
                    ),
                ),
                StructField("sourceType", StringType()),
                StructField("supportEmailAddress", StringType()),
                StructField("categoryId", StringType()),
                # Custom-property values are user-defined and untyped on
                # the API; ``value`` / ``associationValue`` entries are
                # JSON-serialized to strings in ``_map_record``.
                StructField(
                    "customPropertyValues",
                    ArrayType(
                        StructType(
                            [
                                StructField("reference", StringType()),
                                StructField("value", StringType()),
                                StructField("associationValue", ArrayType(StringType())),
                            ]
                        )
                    ),
                ),
            ]
        ),
        "work_requests": StructType(
            [
                StructField("id", StringType()),
                StructField("counter", StringType()),
                StructField("status", StringType()),
                StructField("relatedAssetId", StringType()),
                StructField("assetStatus", StringType()),
                StructField("description", StringType()),
                StructField("declineReason", StringType()),
                StructField("externalReference", StringType()),
                StructField("relatedWorkOrderId", StringType()),
                StructField("failureTypeIds", ArrayType(StringType())),
                StructField(
                    "failureTypes",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("reference", StringType()),
                            ]
                        )
                    ),
                ),
                StructField("createdAt", TimestampType()),
                StructField("updatedAt", TimestampType()),
                StructField("approvedAt", TimestampType()),
                StructField("completedAt", TimestampType()),
            ]
        ),
        "organizations": StructType(
            [
                StructField("id", StringType()),
                StructField("createdAt", TimestampType()),
                StructField("updatedAt", TimestampType()),
                StructField("name", StringType()),
                StructField("organizationNumber", StringType()),
                StructField("phoneNumber", phone_number),
                StructField("email", StringType()),
                StructField("shippingAddress", address),
                StructField("website", StringType()),
                StructField("lang", StringType()),
                StructField("tz", StringType()),
            ]
        ),
        "parts": StructType(
            [
                StructField("id", StringType()),
                StructField("partNumber", StringType()),
                StructField("externalReference", StringType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("createdAt", TimestampType()),
                StructField("updatedAt", TimestampType()),
                StructField("price", DoubleType()),
                StructField("minimumStock", DoubleType()),
                StructField("availableStock", DoubleType()),
            ]
        ),
        "contacts": StructType(
            [
                StructField("id", StringType()),
                StructField("firstName", StringType()),
                StructField("lastName", StringType()),
                StructField("rembergUserEmail", StringType()),
                StructField("jobPosition", StringType()),
                StructField("phoneNumber", phone_number),
                StructField("organizationNumber", StringType()),
                StructField(
                    "hourlyRates",
                    ArrayType(
                        StructType(
                            [
                                StructField("value", DoubleType()),
                                StructField("validFrom", StringType()),
                            ]
                        )
                    ),
                ),
            ]
        ),
        "users": StructType(
            [
                StructField("id", StringType()),
                StructField("email", StringType()),
                StructField("firstName", StringType()),
                StructField("lastName", StringType()),
                StructField("fullName", StringType()),
                StructField("status", StringType()),
            ]
        ),
        "forms": StructType(
            [
                StructField("id", StringType()),
                StructField("formTemplateId", StringType()),
                StructField("counter", LongType()),
                StructField("relatedWorkOrderId", StringType()),
                StructField("name", StringType()),
                StructField("status", StringType()),
                StructField("createdAt", TimestampType()),
                StructField("updatedAt", TimestampType()),
                StructField("finalizedAt", TimestampType()),
            ]
        ),
    }


TABLE_SCHEMAS = _build_schemas()


class RembergLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the remberg public REST API."""

    # ------------------------------------------------------------------
    # Construction & helpers
    # ------------------------------------------------------------------

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        api_key = options.get("api_key")
        if not api_key:
            raise ValueError(
                "remberg connector requires 'api_key' option (created under "
                "Settings > Data > API in the remberg web app)."
            )

        base_url = options.get("base_url") or DEFAULT_BASE_URL
        self._root = base_url.rstrip("/") + "/"

        # The OpenAPI security scheme is ``type: apiKey, in: header,
        # name: authorization`` — the raw key, no ``Bearer`` prefix.
        self._headers = {
            "authorization": api_key,
            "Accept": "application/json",
        }

        # Cap cursors at init time so Trigger.AvailableNow always terminates.
        # Parse the formatted string back so ``_init_dt`` carries the same
        # millisecond precision as stored cursors (which come from
        # ``_format_ts``); otherwise the sub-ms microseconds would make the
        # caught-up fast-path in ``_read_incremental`` never compare equal.
        self._init_ts_iso = _format_ts(datetime.now(timezone.utc))
        self._init_dt = _parse_ts(self._init_ts_iso)

        # Client-side throttle state: monotonic timestamp of the last
        # request per endpoint path (remberg limits are per endpoint).
        self._last_request_at: dict[str, float] = {}

    def _url(self, path: str) -> str:
        """Join *path* (no leading slash) onto the API root."""
        return urljoin(self._root, path.lstrip("/"))

    def _throttle(self, path: str) -> None:
        """Space requests to the same endpoint ≥ MIN_REQUEST_INTERVAL apart."""
        last = self._last_request_at.get(path)
        if last is not None:
            elapsed = time.monotonic() - last
            if elapsed < MIN_REQUEST_INTERVAL:
                time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_at[path] = time.monotonic()

    def _request(self, path: str, params: dict | None = None) -> requests.Response:
        """GET *path* with retry on 429/5xx; honour ``Retry-After-*``.

        remberg 429s carry ``Retry-After-Burst`` or ``Retry-After-Base``
        (seconds) depending on which throttler tripped — and the 429 itself
        counts against the limit, so waiting the advertised time (not just
        our own backoff) matters. Every request carries an explicit timeout
        so the connector cannot hang on a stuck socket; transport-level
        ``RequestException`` errors propagate so the framework retries the
        microbatch.
        """
        backoff = INITIAL_BACKOFF
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            self._throttle(path)
            resp = requests.get(
                self._url(path),
                headers=self._headers,
                params=params or {},
                timeout=DEFAULT_TIMEOUT,
            )
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            sleep_s = backoff
            retry_after = _retry_after_seconds(resp)
            if retry_after is not None:
                sleep_s = max(sleep_s, retry_after)
            logger.warning(
                "remberg %s returned %s; retrying in %.1fs (attempt %d/%d)",
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
                f"remberg API GET {path} failed with HTTP {resp.status_code}: {resp.text[:500]}"
            )
        return resp.json()

    @staticmethod
    def _unwrap_records(body, key: str) -> list:
        """Return the records list from a remberg response body.

        remberg wraps list responses in an envelope whose key varies per
        resource (``data`` / ``tickets`` / ``organizations`` / ``contacts``
        — see ``TABLE_ENDPOINTS``). Tolerate a bare array and fall back to
        ``data`` in case an envelope key ever changes; return ``[]`` for
        anything else so callers can iterate without guarding.
        """
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            inner = body.get(key)
            if isinstance(inner, list):
                return inner
            if key != "data":
                inner = body.get("data")
                if isinstance(inner, list):
                    return inner
        return []

    # ------------------------------------------------------------------
    # LakeflowConnect API
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        # remberg has no discovery endpoint; tables are statically known.
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
        if TABLE_METADATA[table_name]["ingestion_type"] == "snapshot":
            return self._read_snapshot(table_name, start_offset, table_options)
        return self._read_incremental(table_name, start_offset, table_options)

    # ------------------------------------------------------------------
    # Snapshot reads (contacts / users / forms)
    # ------------------------------------------------------------------

    def _read_snapshot(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Full-refresh read for snapshot tables.

        Returns ``{"done": True}`` after the first call so subsequent calls
        within the same Trigger.AvailableNow trigger short-circuit (per the
        ``end_offset == start_offset`` termination contract).
        """
        if start_offset and start_offset.get("done"):
            return iter([]), start_offset

        path, records_key = TABLE_ENDPOINTS[table_name]
        limit = _page_size(table_options)

        def generate() -> Iterator[dict]:
            page = 1
            while True:
                body = self._get_json(path, params={"page": str(page), "limit": str(limit)})
                records = self._unwrap_records(body, records_key)
                for raw in records:
                    yield self._map_record(table_name, raw)
                if len(records) < limit:
                    return
                page += 1

        return generate(), {"done": True}

    # ------------------------------------------------------------------
    # Incremental reads (the six CDC tables)
    # ------------------------------------------------------------------

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Bounded ``updatedAt``-range read with page continuation.

        Offset shapes:
          ``{"cursor": <iso>}``                     — caught-up steady state.
          ``{"since": ..., "until": ..., "page": N}`` — mid-range, produced
              only when ``max_records_per_batch`` split a range across
              microbatches. ``since``/``until`` stay pinned so page numbers
              remain stable for the rest of the range.

        A new range spans ``[cursor - lookback_seconds, _init_ts]``; when a
        page returns fewer than ``limit`` records the range is drained and
        the cursor advances to the range's upper bound. The very first sync
        has no lower bound (full backfill) unless ``start_timestamp`` is
        supplied.
        """
        offset = dict(start_offset or {})
        limit = _page_size(table_options)
        max_records = int(table_options.get("max_records_per_batch", str(sys.maxsize)))

        if offset.get("until"):
            # Resume a partially-drained range with its bounds pinned.
            since = offset.get("since")
            until = offset["until"]
            page = int(offset.get("page", 1))
        else:
            cursor = offset.get("cursor")
            if cursor and _parse_ts(cursor) >= self._init_dt:
                # Caught up to init time — short-circuit so the trigger
                # terminates (end_offset == start_offset contract).
                return iter([]), start_offset
            until = self._init_ts_iso
            page = 1
            if cursor:
                lookback = max(
                    0,
                    int(table_options.get("lookback_seconds", str(DEFAULT_LOOKBACK_SECONDS))),
                )
                since = _format_ts(_parse_ts(cursor) - timedelta(seconds=lookback))
            else:
                since = table_options.get("start_timestamp")

        path, records_key = TABLE_ENDPOINTS[table_name]
        params: dict[str, str] = {"updatedAtUntil": until, "limit": str(limit)}
        if since:
            params["updatedAtFrom"] = since

        if max_records >= sys.maxsize:
            # Unbounded (the default): stream pages lazily so driver memory
            # tracks a single page, not the whole range. A short page drains
            # the range; the cursor then advances to its upper bound. Because
            # the range always fully drains here, the end offset is known up
            # front (like the snapshot path), so no accumulation is needed.
            def generate() -> Iterator[dict]:
                p = page
                while True:
                    page_records = self._unwrap_records(
                        self._get_json(path, params={**params, "page": str(p)}),
                        records_key,
                    )
                    yield from (
                        self._map_record(table_name, raw) for raw in page_records
                    )
                    if len(page_records) < limit:
                        return
                    p += 1

            return generate(), {"cursor": until}

        # Bounded by ``max_records_per_batch``: accumulate up to the cap
        # (memory stays bounded by the cap) so the split offset can be
        # computed before the tuple is returned.
        records: list[dict] = []
        while True:
            params["page"] = str(page)
            body = self._get_json(path, params=params)
            page_records = self._unwrap_records(body, records_key)
            records.extend(self._map_record(table_name, raw) for raw in page_records)
            if len(page_records) < limit:
                # Range drained — advance the cursor to its upper bound.
                return iter(records), {"cursor": until}
            page += 1
            if len(records) >= max_records:
                # Split the range across microbatches; the cap applies at
                # page granularity so resuming at ``page`` never skips the
                # tail of a partially-emitted page.
                next_offset = {"until": until, "page": page}
                if since:
                    next_offset["since"] = since
                return iter(records), next_offset

    # ------------------------------------------------------------------
    # Validation & field mapping
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {sorted(TABLE_SCHEMAS)}"
            )

    def _map_record(self, table_name: str, raw: dict) -> dict:
        """Project a raw API record onto the table schema.

        Field names are kept as the API returns them, so mapping is a
        schema-driven projection: every schema column is present in the
        output (absent wire fields become ``None``), nested structs and
        arrays are projected recursively, and unknown wire fields are
        dropped. Type coercion is left to the framework — ISO date-time
        strings pass through for timestamp columns.
        """
        if table_name == "tickets":
            raw = _normalize_ticket(raw)
        projected = _project(raw, TABLE_SCHEMAS[table_name])
        return projected if projected is not None else {}


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _retry_after_seconds(resp: requests.Response) -> float | None:
    """Return the longest advertised wait among remberg's Retry-After headers.

    remberg 429s carry ``Retry-After-Burst`` or ``Retry-After-Base``
    depending on which throttler tripped; when both limits are exhausted
    waiting only the shorter one would burn another request (429s count
    against the limit), so take the max. The plain ``Retry-After`` is
    accepted too for proxy/5xx responses.
    """
    values = []
    for header in ("Retry-After-Burst", "Retry-After-Base", "Retry-After"):
        raw = resp.headers.get(header)
        if raw:
            try:
                values.append(float(raw))
            except ValueError:
                pass
    return max(values) if values else None


def _page_size(table_options: dict[str, str]) -> int:
    """Resolve the ``limit`` page-size option, clamped to the server max."""
    return max(1, min(int(table_options.get("limit", str(DEFAULT_PAGE_SIZE))), MAX_PAGE_SIZE))


def _project(value: Any, data_type: DataType) -> Any:
    """Recursively project a JSON value onto a Spark DataType."""
    if value is None:
        return None
    if isinstance(data_type, StructType):
        if not isinstance(value, dict):
            return None
        return {f.name: _project(value.get(f.name), f.dataType) for f in data_type.fields}
    if isinstance(data_type, ArrayType):
        if not isinstance(value, list):
            return None
        return [_project(item, data_type.elementType) for item in value]
    return value


def _normalize_ticket(raw: dict) -> dict:
    """JSON-serialize the untyped custom-property values on a ticket.

    ``customPropertyValues[].value`` (and ``associationValue`` entries) are
    user-defined and untyped on the API — strings, numbers, booleans,
    objects, arrays. Serialize non-string values to JSON strings so the
    column has a stable type.
    """
    cpvs = raw.get("customPropertyValues")
    if not isinstance(cpvs, list):
        return raw
    normalized = []
    for cpv in cpvs:
        if not isinstance(cpv, dict):
            continue
        assoc = cpv.get("associationValue")
        normalized.append(
            {
                **cpv,
                "value": _json_str(cpv.get("value")),
                "associationValue": (
                    [_json_str(item) for item in assoc] if isinstance(assoc, list) else assoc
                ),
            }
        )
    out = dict(raw)
    out["customPropertyValues"] = normalized
    return out


def _json_str(value: Any) -> str | None:
    """Return *value* as a string: pass strings through, JSON-encode the rest."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _format_ts(dt: datetime) -> str:
    """Format a datetime as the ISO-8601 UTC shape remberg uses (ms + Z)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp string (Z or offset) into an aware datetime."""
    if not value:
        raise ValueError("empty timestamp string")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
