"""Microsoft Purview connector (read-only).

Ingests governed business metadata from the Microsoft Purview **Unified
Catalog Data Governance** REST API (data plane,
``api-version=2026-03-20-preview``, public preview) into Unity Catalog.
Implements the standard ``LakeflowConnect`` interface (single-driver reads).

Why not ``SupportsPartitionedStream``?
    The partitioned-stream pattern exists to parallelise **server-side
    time-range** queries (``since``/``until``) across executors. The Unified
    Catalog list endpoints this connector uses
    (``/datagovernance/catalog/businessdomains``, ``.../dataProducts``,
    ``.../terms``) accept only ``skip`` / ``top`` / ``orderBy`` / ``$skipToken``
    — there is **no server-side ``lastModifiedAt`` range filter**. Time-window
    partitioning would force every executor to full-scan the whole collection
    and filter client-side, multiplying total work by the number of partitions
    for no benefit. This is the same situation as the Collibra connector; the
    standard single-driver path with client-side incremental filtering is the
    correct fit here. (The older Atlas Data Map ``/search/query`` endpoint does
    support a server-side ``updateTime`` filter, but this connector targets the
    newer Unified Catalog governance surface per the connector design.)

Auth (m2m OAuth, offloaded to Unity Catalog):
    Purview data-plane APIs use Azure Entra ID (formerly Azure AD) OAuth 2.0
    client-credentials against ``https://login.microsoftonline.com/{tenant_id}``
    with scope ``https://purview.azure.net/.default``. The UC COMMUNITY
    connection runs the token exchange and refresh server-side and injects a
    fresh bearer token as ``access_token`` at query time; the connector simply
    sends ``Authorization: Bearer {access_token}`` and never holds the client
    secret or runs the token flow itself. A ``token`` fallback is accepted for
    ad-hoc / personal-token use.

Incremental model:
    ``data_products`` and ``terms`` are ``cdc`` on the
    ``systemData.lastModifiedAt`` cursor (ISO-8601 UTC date-time string, which
    is lexicographically ordered). The Unified Catalog list endpoints do not
    accept a server-side modified-since filter and their ``orderBy`` support
    for the nested ``systemData/lastModifiedAt`` path is unverified against a
    live tenant, so the cursor is applied CLIENT-SIDE and the connector drains
    the full collection per run rather than truncating by count on an
    unordered slice (needs-live-testing: confirm ``orderBy`` on the cursor
    path to enable safe count-based batching). An init-time upper bound
    (``self._init_ts``) caps the returned cursor so a Trigger.AvailableNow
    microbatch terminates. ``business_domains`` is a ``snapshot`` (low-volume
    governance taxonomy).
"""

import logging
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_schemas import (  # pylint: disable=line-too-long
    API_VERSION,
    DEFAULT_ENDPOINT,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_utils import (  # pylint: disable=line-too-long
    next_link_paginate,
    normalize_contacts,
)

_LOG = logging.getLogger(__name__)

DEFAULT_MAX_RECORDS_PER_BATCH = 5000

# Catalog path prefixes (relative to the data-plane endpoint).
_CATALOG_BASE = "/datagovernance/catalog"


class MicrosoftPurviewLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Purview Unified Catalog API."""

    def __init__(self, options: dict[str, str]) -> None:
        """Initialize the Microsoft Purview connector.

        Expected options:
            - tenant_id: Azure Entra (AAD) tenant ID/GUID. Used for row
              disambiguation and (server-side, by UC) as the token-endpoint
              interpolation value. Required.
            - endpoint: Unified Catalog data-plane base URL. Defaults to the
              shared ``https://api.purview-service.microsoft.com``.
            - access_token: OAuth 2.0 bearer token, minted and injected by the
              UC COMMUNITY connection (m2m / client-credentials).
            - token: personal/API token fallback used the same way as
              ``access_token`` (``Authorization: Bearer``) for ad-hoc use.
            - page_size: ``top`` page size for skip/top-paginated endpoints.
        """
        super().__init__(options)

        self.tenant_id = options.get("tenant_id") or options.get("account")
        if not self.tenant_id:
            raise ValueError(
                "Microsoft Purview connector requires 'tenant_id' (Azure "
                "Entra tenant ID)"
            )

        endpoint = options.get("endpoint") or DEFAULT_ENDPOINT
        self.endpoint = endpoint.rstrip("/")

        try:
            self._page_size = max(
                1,
                min(
                    MAX_PAGE_SIZE,
                    int(options.get("page_size") or DEFAULT_PAGE_SIZE),
                ),
            )
        except (TypeError, ValueError):
            self._page_size = DEFAULT_PAGE_SIZE

        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        self._configure_auth(options)

        # Cap incremental cursors at init time so a single Trigger.AvailableNow
        # run only drains data that existed when the connector started; data
        # modified after this point is picked up by the next trigger with a
        # fresh _init_ts. This is the termination guard. ISO-8601 UTC string to
        # match the systemData.lastModifiedAt cursor unit (ISO strings compare
        # lexicographically).
        self._init_ts = datetime.now(timezone.utc).isoformat()

    def _configure_auth(self, options: dict[str, str]) -> None:
        """Set the Bearer auth header from the injected token.

        ``access_token`` (OAuth m2m, UC-injected) takes precedence; ``token``
        is an equivalent personal-token fallback. The connector never runs the
        OAuth client-credentials exchange itself — that is offloaded to Unity
        Catalog, mirroring the Azure DevOps ``service_principal`` model.
        """
        access_token = options.get("access_token") or options.get("token")
        if not access_token:
            raise ValueError(
                "Microsoft Purview connector requires 'access_token' (OAuth "
                "m2m bearer token, injected by the UC connection) or 'token'"
            )
        self._session.headers["Authorization"] = f"Bearer {access_token}"

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
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        if table_name == "business_domains":
            return self._read_business_domains(table_options)
        if table_name == "data_products":
            return self._read_data_products(start_offset, table_options)
        if table_name == "terms":
            return self._read_terms(start_offset, table_options)
        raise ValueError(f"Unsupported table: {table_name!r}")

    # ------------------------------------------------------------------ #
    # Table readers
    # ------------------------------------------------------------------ #

    def _read_business_domains(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read business (governance) domains as a snapshot.

        The businessdomains endpoint paginates via a ``$skipToken`` embedded in
        ``nextLink``; ``next_link_paginate`` follows it transparently.
        """
        params = {"api-version": API_VERSION}
        if table_options.get("write_only"):
            params["writeOnly"] = table_options["write_only"]

        url = f"{self.endpoint}{_CATALOG_BASE}/businessdomains"
        records: list[dict[str, Any]] = []
        for raw in next_link_paginate(self._session, url, params, "business_domains"):
            records.append(self._shape_business_domain(raw))
        return iter(records), {}

    def _read_data_products(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read data products incrementally on ``systemData.lastModifiedAt``.

        Paginated by ``skip`` / ``top`` with a ``nextLink`` follow. Optional
        ``domain_id`` scopes to a single governance domain.
        """
        params = {"api-version": API_VERSION, "top": str(self._page_size)}
        if table_options.get("domain_id"):
            params["domainId"] = table_options["domain_id"]
        if table_options.get("order_by"):
            params["orderBy"] = table_options["order_by"]

        url = f"{self.endpoint}{_CATALOG_BASE}/dataProducts"
        record_iter = next_link_paginate(self._session, url, params, "data_products")
        return self._incremental_from_iter(
            record_iter, start_offset, transform=self._shape_data_product
        )

    def _read_terms(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read glossary terms incrementally on ``systemData.lastModifiedAt``.

        Paginated by ``skip`` / ``top`` with a ``nextLink`` follow. Optional
        ``domain_id`` / ``parent_id`` / ``keyword`` narrow the extract.
        """
        params = {"api-version": API_VERSION, "top": str(self._page_size)}
        if table_options.get("domain_id"):
            params["domainId"] = table_options["domain_id"]
        if table_options.get("parent_id"):
            params["parentId"] = table_options["parent_id"]
        if table_options.get("keyword"):
            params["keyword"] = table_options["keyword"]
        if table_options.get("order_by"):
            params["orderBy"] = table_options["order_by"]

        url = f"{self.endpoint}{_CATALOG_BASE}/terms"
        record_iter = next_link_paginate(self._session, url, params, "terms")
        return self._incremental_from_iter(
            record_iter, start_offset, transform=self._shape_term
        )

    # ------------------------------------------------------------------ #
    # Incremental engine
    # ------------------------------------------------------------------ #

    def _incremental_from_iter(
        self,
        record_iter: Iterator[dict[str, Any]],
        start_offset: dict,
        transform,
    ) -> tuple[Iterator[dict], dict]:
        """Apply the client-side incremental cursor over a record iterator.

        Applies, per record:
          * strict ``> since`` filtering (exclusive lower bound), so a resumed
            boundary record is not re-emitted, and
          * an init-time upper cap (skip records modified after
            ``self._init_ts``) so Trigger.AvailableNow terminates.

        The Unified Catalog list endpoints do not accept a server-side
        modified-since filter, and ``orderBy`` on the nested
        ``systemData/lastModifiedAt`` path is unverified against a live tenant,
        so records are treated as **not** cursor-sorted: we drain the full
        collection per run (bounded only by the ``self._init_ts`` cap) rather
        than truncating by count on an unordered slice. Count-based truncation
        of an unordered slice would advance the watermark to an arbitrary max
        and silently drop not-yet-seen records at or below it. CDC upsert on
        the primary key tolerates the resulting re-reads. (needs-live-testing:
        once ``orderBy=systemData/lastModifiedAt asc`` is confirmed, this can
        switch to a soft, tie-group-aware count cap like Collibra's sorted
        path.)

        Returns ``(records, end_offset)``. When nothing new is emitted, the
        offset is returned unchanged so ``end_offset == start_offset`` and the
        trigger converges.
        """
        start_offset = start_offset or {}
        since = start_offset.get("cursor")

        # Already caught up to init time — nothing new can be emitted.
        if since is not None and since >= self._init_ts:
            return iter([]), start_offset

        records: list[dict[str, Any]] = []
        max_seen = since
        for raw in record_iter:
            cursor = self._record_cursor(raw)
            # Strict `>` so a resumed inclusive `since` doesn't re-emit boundary.
            if since is not None and cursor is not None and cursor <= since:
                continue
            # Init-time cap: skip records modified after the connector started.
            if cursor is not None and cursor > self._init_ts:
                continue

            records.append(transform(raw))
            if cursor is not None and (max_seen is None or cursor > max_seen):
                max_seen = cursor

        if not records or max_seen is None or max_seen == since:
            # No forward progress — return the offset unchanged so the
            # framework sees end_offset == start_offset and terminates.
            return iter(records), start_offset

        return iter(records), {"cursor": max_seen}

    @staticmethod
    def _record_cursor(raw: dict[str, Any]) -> str | None:
        """Extract the ``systemData.lastModifiedAt`` cursor value from a record.

        Returns the ISO-8601 string, or ``None`` when absent so it is treated
        as uncomparable and never breaks ordering.
        """
        system_data = raw.get("systemData")
        if isinstance(system_data, dict):
            value = system_data.get("lastModifiedAt")
            if isinstance(value, str) and value:
                return value
        return None

    # ------------------------------------------------------------------ #
    # Record shaping
    # ------------------------------------------------------------------ #

    def _shape_business_domain(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["purview_tenant_id"] = self.tenant_id
        return rec

    def _shape_data_product(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["contacts"] = normalize_contacts(raw.get("contacts"))
        rec["purview_tenant_id"] = self.tenant_id
        return rec

    def _shape_term(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["contacts"] = normalize_contacts(raw.get("contacts"))
        rec["purview_tenant_id"] = self.tenant_id
        return rec

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table {table_name!r}; "
                f"supported: {SUPPORTED_TABLES}"
            )
