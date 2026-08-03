"""remberg source connector for Lakeflow Community Connectors.

Implements the :class:`LakeflowConnect` interface against the remberg public
REST API (https://developers.remberg.de). remberg is an asset-centric
maintenance / field-service platform; fifteen tables are exposed:

    CDC (incremental): assets, work_orders, tickets, work_requests,
                       organizations, parts, asset_status_signals
    Snapshot:          contacts, users, forms
    CDC via fan-out:   work_order_times, work_order_stock_changes,
                       part_inventories, part_stock_changes,
                       ticket_conversations

The last group are per-parent sub-resources: remberg serves them only under
a parent record's id (``/v2/work-orders/{id}/times``) with no list-all
variant, so the connector lists the parent over a bounded ``updatedAt``
range and issues one request per parent in that range. See
``ChildEndpoint`` and ``_read_child``. Their cost tracks parent churn
rather than table size, which is what keeps them affordable under the rate
limits described below; ``full_parent_scan`` trades that back for
completeness when needed.

``asset_status_signals`` is NOT a fan-out table: ``/v2/assets/status-signals``
lists every signal across assets with embedded asset info. It is a plain CDC
read, but keyed on ``createdAt`` because that endpoint offers no
``updatedAt`` bound — see the caveat in ``remberg_api_doc.md``.

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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from urllib.parse import quote, urljoin

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
    "asset_status_signals": ("v2/assets/status-signals", "data"),
}


# CDC range-filter query-param names per table. Almost every remberg list
# endpoint filters on ``updatedAt`` via ``updatedAtFrom`` / ``updatedAtUntil``;
# ``/v2/assets/status-signals`` only offers ``createdAt`` bounds (plus
# ``resolvedAt`` bounds, which are not a change cursor).
DEFAULT_RANGE_PARAMS = ("updatedAtFrom", "updatedAtUntil")
RANGE_PARAMS: dict[str, tuple[str, str]] = {
    "asset_status_signals": ("createdAtFrom", "createdAtUntil"),
}


@dataclass(frozen=True)
class ChildEndpoint:
    """A per-parent sub-resource served only under a parent record's ID.

    remberg exposes these as ``/<parent>/{id}/<sub-resource>`` with no
    list-all variant, so the connector lists the parent table over a bounded
    ``updatedAt`` range and issues one request per parent in that range.

    ``route`` is the path *template*: it doubles as the client-side throttle
    key, because remberg rate-limits per endpoint route rather than per
    concrete URL (see ``_throttle``).

    Child records carry no independent change cursor — several have no ``id``
    at all — so the parent's ``updatedAt`` is injected onto every row as
    ``parent_cursor_field`` and used as the table's ``cursor_field``. That is
    the value the connector actually advances on, which keeps the CDC
    contract honest.
    """

    parent: str  # parent table name in TABLE_ENDPOINTS
    route: str  # path template, ``{id}`` substituted per parent
    records_key: str  # envelope key holding the child array
    parent_id_field: str  # column the parent's id is injected into
    parent_cursor_field: str  # column the parent's updatedAt is injected into
    paginated: bool  # child endpoint accepts page/limit


# Child (fan-out) tables. Note ``/v2/work-orders/{id}/checklist`` is POST-only
# in the remberg API — there is no GET, so work-order checklists cannot be
# ingested. Asset status signals are NOT here: ``/v2/assets/status-signals``
# lists them all with embedded asset info, so that table is a plain CDC read.
CHILD_ENDPOINTS: dict[str, ChildEndpoint] = {
    "work_order_times": ChildEndpoint(
        parent="work_orders",
        route="v2/work-orders/{id}/times",
        records_key="timeEntries",
        parent_id_field="workOrderId",
        parent_cursor_field="workOrderUpdatedAt",
        paginated=False,
    ),
    "work_order_stock_changes": ChildEndpoint(
        parent="work_orders",
        route="v2/work-orders/{id}/stock-changes",
        records_key="data",
        parent_id_field="workOrderId",
        parent_cursor_field="workOrderUpdatedAt",
        paginated=True,
    ),
    "part_inventories": ChildEndpoint(
        parent="parts",
        route="v2/parts/{id}/inventories",
        records_key="data",
        parent_id_field="partTypeId",
        parent_cursor_field="partUpdatedAt",
        paginated=True,
    ),
    "part_stock_changes": ChildEndpoint(
        parent="parts",
        route="v2/parts/{id}/stock-changes",
        records_key="data",
        parent_id_field="partTypeId",
        parent_cursor_field="partUpdatedAt",
        paginated=True,
    ),
    "ticket_conversations": ChildEndpoint(
        parent="tickets",
        route="v2/tickets/{id}/conversations",
        records_key="activities",
        parent_id_field="ticketId",
        parent_cursor_field="ticketUpdatedAt",
        paginated=False,
    ),
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
    # Flat CDC read over /v2/assets/status-signals — cursor is ``createdAt``
    # because the endpoint offers no ``updatedAt`` bound.
    "asset_status_signals": {
        "primary_keys": ["id"],
        "cursor_field": "createdAt",
        "ingestion_type": "cdc",
    },
    # Child (fan-out) tables. The cursor is always the injected parent
    # ``updatedAt`` — see ``ChildEndpoint``. Primary keys are composite
    # because remberg gives several of these sub-resources no ``id``.
    "work_order_times": {
        # No ``id`` on a time entry; a work order cannot hold two entries for
        # the same person starting at the same instant.
        "primary_keys": ["workOrderId", "performingPersonId", "startTime"],
        "cursor_field": "workOrderUpdatedAt",
        "ingestion_type": "cdc",
    },
    "work_order_stock_changes": {
        "primary_keys": ["id"],
        "cursor_field": "workOrderUpdatedAt",
        "ingestion_type": "cdc",
    },
    "part_inventories": {
        "primary_keys": ["id"],
        "cursor_field": "partUpdatedAt",
        "ingestion_type": "cdc",
    },
    "part_stock_changes": {
        "primary_keys": ["id"],
        "cursor_field": "partUpdatedAt",
        "ingestion_type": "cdc",
    },
    "ticket_conversations": {
        # No ``id`` on a conversation activity; ``createdAt`` + ``kind``
        # disambiguates the (rare) same-instant note and email.
        "primary_keys": ["ticketId", "createdAt", "kind"],
        "cursor_field": "ticketUpdatedAt",
        "ingestion_type": "cdc",
    },
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
    # AssetInfoCfaResponseDto / PartTypeInfoCfaResponseDto — the embedded
    # reference objects the sub-resource DTOs share.
    asset_info = StructType(
        [
            StructField("id", StringType()),
            StructField("assetNumber", StringType()),
            StructField("assetType", StringType()),
        ]
    )
    part_type_info = StructType(
        [
            StructField("id", StringType()),
            StructField("partNumber", StringType()),
            StructField("externalReference", StringType()),
            StructField("name", StringType()),
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
        # ---- flat sub-resource table ----
        # AssetStatusSignalWithAssetInfoCfaResponseDto
        "asset_status_signals": StructType(
            [
                StructField("id", StringType()),
                StructField("createdAt", TimestampType()),
                StructField("reportedAt", TimestampType()),
                StructField("resolvedAt", TimestampType()),
                StructField("status", StringType()),
                StructField("asset", asset_info),
            ]
        ),
        # ---- child (fan-out) tables ----
        # The first two columns of each are injected by ``_map_child``: the
        # parent's id and the parent's ``updatedAt`` (the CDC cursor).
        # WorkOrderTimeEntryCfaResponseDto. The response's root-level
        # ``totalDurationInSeconds`` is deliberately dropped — it is exactly
        # SUM(durationInSeconds) per work order, so carrying it on every row
        # would denormalize an aggregate that SQL derives trivially.
        "work_order_times": StructType(
            [
                StructField("workOrderId", StringType()),
                StructField("workOrderUpdatedAt", TimestampType()),
                StructField("performingPersonId", StringType()),
                StructField("startTime", TimestampType()),
                StructField("endTime", TimestampType()),
                StructField("durationInSeconds", DoubleType()),
            ]
        ),
        # WorkOrderCfaPartStockChangeResponseDto
        "work_order_stock_changes": StructType(
            [
                StructField("workOrderId", StringType()),
                StructField("workOrderUpdatedAt", TimestampType()),
                StructField("id", StringType()),
                StructField("createdAt", TimestampType()),
                StructField("updatedAt", TimestampType()),
                StructField("change", DoubleType()),
                StructField("action", StringType()),
                StructField("partType", part_type_info),
                StructField("storageAsset", asset_info),
            ]
        ),
        # PartInventoryWithStorageCfaResponseDto. ``partTypeId`` is already on
        # the wire record; the injection just guarantees it is populated.
        "part_inventories": StructType(
            [
                StructField("partTypeId", StringType()),
                StructField("partUpdatedAt", TimestampType()),
                StructField("id", StringType()),
                StructField("storageAsset", asset_info),
                StructField("availableStock", DoubleType()),
                StructField("totalStock", DoubleType()),
                StructField("createdAt", TimestampType()),
            ]
        ),
        # PartStockChangeCfaResponseDto. ``createdAt`` / ``updatedAt`` are
        # declared as free-form objects in the OpenAPI spec (the same doc bug
        # as tickets/assets/work_orders) but are ISO date-time strings on the
        # wire.
        "part_stock_changes": StructType(
            [
                StructField("partTypeId", StringType()),
                StructField("partUpdatedAt", TimestampType()),
                StructField("id", StringType()),
                StructField("createdAt", TimestampType()),
                StructField("updatedAt", TimestampType()),
                StructField("change", DoubleType()),
                StructField("comment", StringType()),
                StructField("action", StringType()),
                StructField("storageAsset", asset_info),
            ]
        ),
        # TicketCfaConversationActivityResponseDto. ``creator`` is an untyped
        # object in the OpenAPI spec (no declared properties), so it is
        # JSON-serialized to a string in ``_map_record`` rather than guessed
        # at as a struct.
        "ticket_conversations": StructType(
            [
                StructField("ticketId", StringType()),
                StructField("ticketUpdatedAt", TimestampType()),
                StructField("createdAt", TimestampType()),
                StructField("kind", StringType()),
                StructField("creator", StringType()),
                StructField("subject", StringType()),
                StructField("plainText", StringType()),
                StructField(
                    "header",
                    StructType(
                        [
                            StructField("from", StringType()),
                            StructField("to", ArrayType(StringType())),
                            StructField("cc", ArrayType(StringType())),
                            StructField("bcc", ArrayType(StringType())),
                        ]
                    ),
                ),
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

    def _throttle(self, route: str) -> None:
        """Space requests to the same endpoint ≥ MIN_REQUEST_INTERVAL apart.

        *route* must be the endpoint **template**, not the concrete path.
        remberg throttles per endpoint route, so the fan-out reads — which hit
        ``v2/work-orders/{id}/times`` once per work order — have to share a
        single bucket. Keying on the concrete path would give every parent id
        its own bucket, space nothing, and 429-storm on the first fan-out.
        """
        last = self._last_request_at.get(route)
        if last is not None:
            elapsed = time.monotonic() - last
            if elapsed < MIN_REQUEST_INTERVAL:
                time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_at[route] = time.monotonic()

    def _request(
        self, path: str, params: dict | None = None, route: str | None = None
    ) -> requests.Response:
        """GET *path* with retry on 429/5xx; honour ``Retry-After-*``.

        *route* is the throttle key — pass the endpoint template when *path*
        embeds a record id. Defaults to *path* for the flat list endpoints,
        where the two are the same string.

        remberg 429s carry ``Retry-After-Burst`` or ``Retry-After-Base``
        (seconds) depending on which throttler tripped — and the 429 itself
        counts against the limit, so waiting the advertised time (not just
        our own backoff) matters. Every request carries an explicit timeout
        so the connector cannot hang on a stuck socket; transport-level
        ``RequestException`` errors propagate so the framework retries the
        microbatch.
        """
        throttle_key = route or path
        backoff = INITIAL_BACKOFF
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            self._throttle(throttle_key)
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
                throttle_key,
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

    def _get_json(
        self,
        path: str,
        params: dict | None = None,
        route: str | None = None,
        missing_ok: bool = False,
    ):
        """Issue a GET, raise on non-2xx, return the decoded JSON body.

        ``missing_ok`` returns ``None`` on 404 instead of raising — used by
        the fan-out reads, where a parent listed at the start of a range can
        be deleted before its sub-resource is fetched. Failing the whole
        microbatch over one vanished parent would stall the table.
        """
        resp = self._request(path, params=params, route=route)
        if missing_ok and resp.status_code == 404:
            logger.debug("remberg GET %s returned 404; treating as empty", path)
            return None
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
        if table_name in CHILD_ENDPOINTS:
            return self._read_child(table_name, start_offset, table_options)
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

        resolved = self._resolve_range(offset, table_options)
        if resolved is None:
            # Caught up to init time — short-circuit so the trigger
            # terminates (end_offset == start_offset contract).
            return iter([]), start_offset
        since, until, page, _ = resolved

        path, records_key = TABLE_ENDPOINTS[table_name]
        params = self._range_params(table_name, since, until, limit)

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
    # Bounded-range plumbing (shared by flat and fan-out incremental reads)
    # ------------------------------------------------------------------

    def _resolve_range(
        self, offset: dict, table_options: dict[str, str]
    ) -> tuple[str | None, str, int, int] | None:
        """Resolve the bounded range to read, or ``None`` when caught up.

        Returns ``(since, until, page, parent_index)``. ``parent_index`` is
        only meaningful for fan-out reads; flat reads ignore it.

        ``full_parent_scan`` widens a fan-out read to every parent regardless
        of the stored cursor — for backfills, or if a source's child edits
        turn out not to bump the parent's ``updatedAt``. It deliberately does
        NOT bypass the caught-up check: without that the trigger could never
        satisfy the ``end_offset == start_offset`` termination contract.
        """
        if offset.get("until"):
            # Resume a partially-drained range with its bounds pinned.
            return (
                offset.get("since"),
                offset["until"],
                int(offset.get("page", 1)),
                int(offset.get("parent_index", 0)),
            )

        cursor = offset.get("cursor")
        if cursor and _parse_ts(cursor) >= self._init_dt:
            return None

        if _is_true(table_options.get("full_parent_scan")):
            since = None
        elif cursor:
            lookback = max(
                0,
                int(table_options.get("lookback_seconds", str(DEFAULT_LOOKBACK_SECONDS))),
            )
            since = _format_ts(_parse_ts(cursor) - timedelta(seconds=lookback))
        else:
            since = table_options.get("start_timestamp")
        return since, self._init_ts_iso, 1, 0

    @staticmethod
    def _range_params(
        table_name: str, since: str | None, until: str, limit: int
    ) -> dict[str, str]:
        """Build the list-endpoint query params for a bounded range."""
        from_param, until_param = RANGE_PARAMS.get(table_name, DEFAULT_RANGE_PARAMS)
        params = {until_param: until, "limit": str(limit)}
        if since:
            params[from_param] = since
        return params

    # ------------------------------------------------------------------
    # Fan-out reads (per-parent sub-resources)
    # ------------------------------------------------------------------

    def _read_child(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read a per-parent sub-resource by fanning out over its parent.

        The parent table is listed over the same bounded ``updatedAt`` range
        the flat CDC reads use, and each parent in that range costs one
        request (or one page-loop) against the child endpoint. Steady-state
        cost is therefore proportional to parent churn, not to table size —
        which is what makes these endpoints affordable under remberg's
        10 req/s ceiling.

        Offset shapes mirror ``_read_incremental``, with one addition:
          ``{"cursor": <iso>}``  — caught-up steady state.
          ``{"since", "until", "page", "parent_index"}`` — mid-range, emitted
              only when ``max_records_per_batch`` split the range.
              ``parent_index`` is the offset *within* the pinned parent page,
              so a split never re-reads or skips a parent's children.
        """
        spec = CHILD_ENDPOINTS[table_name]
        offset = dict(start_offset or {})
        limit = _page_size(table_options)
        max_records = int(table_options.get("max_records_per_batch", str(sys.maxsize)))

        resolved = self._resolve_range(offset, table_options)
        if resolved is None:
            return iter([]), start_offset
        since, until, page, parent_index = resolved

        parent_path, parent_key = TABLE_ENDPOINTS[spec.parent]
        parent_params = self._range_params(spec.parent, since, until, limit)

        if max_records >= sys.maxsize:
            # Unbounded (the default): stream lazily so driver memory tracks
            # one parent page plus one parent's children, not the whole range.
            # The range always fully drains here, so the end offset is known
            # up front.
            def generate() -> Iterator[dict]:
                p = page
                while True:
                    parents = self._unwrap_records(
                        self._get_json(parent_path, params={**parent_params, "page": str(p)}),
                        parent_key,
                    )
                    for parent in parents:
                        yield from self._fetch_children(table_name, spec, parent, limit)
                    if len(parents) < limit:
                        return
                    p += 1

            return generate(), {"cursor": until}

        # Bounded by ``max_records_per_batch``: accumulate until the cap is
        # reached, then split. The cap is applied at *parent* granularity —
        # a parent's children are always emitted together, so resuming at
        # ``parent_index`` cannot tear one parent's child set across batches.
        records: list[dict] = []
        while True:
            parents = self._unwrap_records(
                self._get_json(parent_path, params={**parent_params, "page": str(page)}),
                parent_key,
            )
            for idx in range(parent_index, len(parents)):
                records.extend(
                    self._fetch_children(table_name, spec, parents[idx], limit)
                )
                if len(records) < max_records:
                    continue
                # Cap hit. If this was the page's last parent, roll the split
                # forward to the next page rather than re-fetching a page we
                # have fully consumed.
                if idx + 1 >= len(parents):
                    if len(parents) < limit:
                        return iter(records), {"cursor": until}
                    next_offset = {"until": until, "page": page + 1, "parent_index": 0}
                else:
                    next_offset = {"until": until, "page": page, "parent_index": idx + 1}
                if since:
                    next_offset["since"] = since
                return iter(records), next_offset

            parent_index = 0
            if len(parents) < limit:
                # Parent range drained — advance the cursor to its upper bound.
                return iter(records), {"cursor": until}
            page += 1

    def _fetch_children(
        self,
        table_name: str,
        spec: ChildEndpoint,
        parent: dict,
        limit: int,
    ) -> list[dict]:
        """Fetch and map one parent's child records.

        Returns ``[]`` for a parent with no id, or one that 404s because it
        was deleted between the parent listing and this request.
        """
        parent_id = parent.get("id")
        if not parent_id:
            return []
        # Parent ids are opaque strings from the API, but quote them anyway so
        # an unexpected value can never alter the request's path structure.
        path = spec.route.format(id=quote(str(parent_id), safe=""))
        parent_cursor = parent.get("updatedAt")

        def mapped(raw: dict) -> dict:
            return self._map_child(table_name, spec, parent_id, parent_cursor, raw)

        if not spec.paginated:
            body = self._get_json(path, route=spec.route, missing_ok=True)
            return [mapped(raw) for raw in self._unwrap_records(body, spec.records_key)]

        out: list[dict] = []
        p = 1
        while True:
            body = self._get_json(
                path,
                params={"page": str(p), "limit": str(limit)},
                route=spec.route,
                missing_ok=True,
            )
            page_records = self._unwrap_records(body, spec.records_key)
            out.extend(mapped(raw) for raw in page_records)
            if len(page_records) < limit:
                return out
            p += 1

    def _map_child(
        self,
        table_name: str,
        spec: ChildEndpoint,
        parent_id: str,
        parent_cursor: Any,
        raw: dict,
    ) -> dict:
        """Inject the parent id + cursor onto a child record, then project."""
        enriched = {
            **raw,
            spec.parent_id_field: parent_id,
            spec.parent_cursor_field: parent_cursor,
        }
        return self._map_record(table_name, enriched)

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
        elif table_name == "ticket_conversations":
            raw = {**raw, "creator": _json_str(raw.get("creator"))}
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


def _is_true(value: str | None) -> bool:
    """Parse a boolean table option; table options arrive as strings."""
    return str(value).strip().lower() in ("1", "true", "yes", "on")


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
