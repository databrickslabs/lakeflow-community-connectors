"""Collibra Data Intelligence Platform connector (read-only).

Implements the standard ``LakeflowConnect`` interface (single-driver reads).

Why not ``SupportsPartitionedStream``?
    The partitioned-stream pattern exists to parallelise **server-side
    time-range** queries (``since``/``until``) across executors. Collibra's
    Core REST API v2 has **no server-side ``lastModifiedAfter`` filter** on
    any of the endpoints this connector uses (see ``collibra_api_doc.md``
    gotcha #9), and it only offers cursor pagination (assets / attributes /
    domains) or offset pagination (responsibilities). Time-window
    partitioning would force every executor to full-scan the whole entity
    collection and filter client-side — multiplying total work by the number
    of partitions for no benefit. The standard single-driver path with
    client-side incremental filtering is the correct fit here.

Auth (m2m, mirrors the Azure DevOps connector):
    Collibra uses OAuth 2.0 client-credentials. The UC COMMUNITY connection
    runs the token exchange and injects a refreshed bearer token as
    ``access_token`` at query time; the connector simply sends
    ``Authorization: Bearer {access_token}`` and never holds the client
    secret or runs the token flow itself. A ``token`` fallback is accepted
    for ad-hoc / personal-token use.

Incremental model:
    ``assets``, ``attributes``, and ``responsibilities`` are ``cdc`` on the
    ``lastModifiedOn`` cursor (int64 epoch ms). Because there is no
    server-side modified-since filter, the connector sorts by LAST_MODIFIED
    ascending and applies the cursor client-side. An init-time upper bound
    (``self._init_ts``) caps the returned cursor so a Trigger.AvailableNow
    microbatch terminates. ``domains`` is a ``snapshot``.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.collibra.collibra_schemas import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.collibra.collibra_utils import (
    cursor_paginate,
    offset_paginate,
    resource_ref,
)

_LOG = logging.getLogger(__name__)

DEFAULT_MAX_RECORDS_PER_BATCH = 5000


class CollibraLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Collibra Core REST API v2."""

    def __init__(self, options: dict[str, str]) -> None:
        """Initialize the Collibra connector.

        Expected options:
            - org: Collibra Cloud subdomain (e.g. ``"databricks"`` for
              ``https://databricks.collibra.com``). Alternatively pass
              ``base_url`` with the full REST base
              (``https://{org}.collibra.com/rest/2.0``).
            - access_token: OAuth 2.0 bearer token, minted and injected by
              the UC COMMUNITY connection (m2m / client-credentials).
            - token: personal/API token fallback used the same way as
              ``access_token`` (``Authorization: Bearer``) for ad-hoc use.
        """
        super().__init__(options)

        base_url = options.get("base_url")
        org = options.get("org")

        if base_url:
            self.base_url = base_url.rstrip("/")
            # Derive a stable org label from the host for row disambiguation.
            self.org = org or self._org_from_base_url(self.base_url)
        elif org:
            self.org = org
            self.base_url = f"https://{org}.collibra.com/rest/2.0"
        else:
            raise ValueError(
                "Collibra connector requires 'org' (Collibra subdomain) "
                "or 'base_url' (full REST base URL)"
            )

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
        # fresh _init_ts. This is the termination guard. Epoch milliseconds to
        # match Collibra's lastModifiedOn cursor unit.
        self._init_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

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
                "Collibra connector requires 'access_token' (OAuth m2m "
                "bearer token, injected by the UC connection) or 'token'"
            )
        self._session.headers["Authorization"] = f"Bearer {access_token}"

    @staticmethod
    def _org_from_base_url(base_url: str) -> str:
        """Best-effort extraction of the org subdomain from a base URL."""
        try:
            host = base_url.split("://", 1)[-1].split("/", 1)[0]
            return host.split(".", 1)[0]
        except (IndexError, AttributeError):
            return base_url

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
        if table_name == "assets":
            return self._read_assets(start_offset, table_options)
        if table_name == "attributes":
            return self._read_attributes(start_offset, table_options)
        if table_name == "responsibilities":
            return self._read_responsibilities(start_offset, table_options)
        if table_name == "domains":
            return self._read_domains(table_options)
        raise ValueError(f"Unsupported table: {table_name!r}")

    # ------------------------------------------------------------------ #
    # Table readers
    # ------------------------------------------------------------------ #

    def _read_assets(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read assets incrementally on the ``lastModifiedOn`` cursor.

        ``sortField`` is required on ``/assets``. The incremental strategy
        needs LAST_MODIFIED ascending so the cursor advances monotonically
        and client-side truncation is safe.

        needs-live-testing: the ``/assets`` object schema documents
        ``sortField`` as NAME / DISPLAY_NAME / ID, while the incremental
        section documents ``sortField=LAST_MODIFIED``. We default to
        LAST_MODIFIED (overridable via the ``sort_field`` table option); if
        the live instance rejects it, fall back to ID-keyset pagination.
        """
        sort_field = table_options.get("sort_field", "LAST_MODIFIED")
        base_params = {
            "limit": str(self._page_size),
            "sortField": sort_field,
            "sortOrder": "ASC",
            # Fast path: skip the expensive count query (see gotcha #2).
            "countLimit": "0",
        }
        if table_options.get("domain_id"):
            base_params["domainId"] = table_options["domain_id"]
        if table_options.get("community_id"):
            base_params["communityId"] = table_options["community_id"]

        record_iter = cursor_paginate(
            self._session, f"{self.base_url}/assets", base_params, "assets"
        )
        return self._incremental_from_iter(
            record_iter, start_offset, table_options
        )

    def _read_attributes(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read attributes incrementally on the ``lastModifiedOn`` cursor.

        The ``value`` field is polymorphic (keyed off
        ``attributeDiscriminator``); we keep the raw value coerced to a
        string plus the discriminator and defer any typed join downstream.
        """
        base_params = {
            "limit": str(self._page_size),
            "sortField": "LAST_MODIFIED",
            "sortOrder": "ASC",
        }
        if table_options.get("asset_id"):
            base_params["assetId"] = table_options["asset_id"]
        if table_options.get("type_public_ids"):
            base_params["typePublicIds"] = table_options["type_public_ids"]

        record_iter = cursor_paginate(
            self._session,
            f"{self.base_url}/attributes",
            base_params,
            "attributes",
        )
        return self._incremental_from_iter(
            record_iter, start_offset, table_options, transform=self._shape_attribute
        )

    def _read_responsibilities(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read responsibilities incrementally (offset/limit paginated).

        ``/responsibilities`` does not support cursor pagination, so we page
        by offset. ``includeInherited=true`` is set explicitly — without it
        domain/community-level owner assignments that apply to child assets
        are dropped (gotcha #1).
        """
        base_params = {
            "sortField": "LAST_MODIFIED",
            "sortOrder": "ASC",
            "includeInherited": "true",
        }
        if table_options.get("resource_ids"):
            base_params["resourceIds"] = table_options["resource_ids"]
        if table_options.get("role_ids"):
            # needs-live-testing: role UUIDs for Data Owner / Data Steward /
            # SME are environment-specific; pass them through when supplied.
            base_params["roleIds"] = table_options["role_ids"]

        record_iter = offset_paginate(
            self._session,
            f"{self.base_url}/responsibilities",
            base_params,
            "responsibilities",
            page_size=self._page_size,
        )
        return self._incremental_from_iter(
            record_iter, start_offset, table_options,
            transform=self._shape_responsibility,
        )

    def _read_domains(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read domains as a snapshot (cursor-paginated, full read)."""
        base_params = {"limit": str(self._page_size)}
        if table_options.get("community_id"):
            base_params["communityId"] = table_options["community_id"]

        records: list[dict[str, Any]] = []
        for raw in cursor_paginate(
            self._session, f"{self.base_url}/domains", base_params, "domains"
        ):
            records.append(self._shape_domain(raw))
        return iter(records), {}

    # ------------------------------------------------------------------ #
    # Incremental engine
    # ------------------------------------------------------------------ #

    def _incremental_from_iter(
        self,
        record_iter: Iterator[dict[str, Any]],
        start_offset: dict,
        table_options: dict[str, str],
        transform=None,
    ) -> tuple[Iterator[dict], dict]:
        """Apply the client-side incremental cursor over a record iterator.

        Records are assumed sorted ascending by ``lastModifiedOn``. Applies:
          * strict ``> since`` filtering (exclusive lower bound),
          * an init-time upper cap (skip records modified after
            ``self._init_ts``) so Trigger.AvailableNow terminates,
          * ``max_records_per_batch`` admission control — safe to truncate
            client-side because these are CDC tables (upsert dedup).

        Returns ``(records, end_offset)``. When nothing new is emitted, the
        offset is returned unchanged so ``end_offset == start_offset`` and the
        trigger converges.
        """
        start_offset = start_offset or {}
        since = start_offset.get("cursor")

        # Already caught up to init time — nothing new can be emitted.
        if since is not None and self._as_cursor(since) is not None:
            since_val = self._as_cursor(since)
            if since_val >= self._init_ts:
                return iter([]), start_offset
        else:
            since_val = None

        max_records = self._parse_max_records(table_options)

        records: list[dict[str, Any]] = []
        max_seen = since_val
        for raw in record_iter:
            cursor = self._as_cursor(raw.get("lastModifiedOn"))
            # Strict `>` so the inclusive nature of a resumed `since` doesn't
            # re-emit the boundary record.
            if since_val is not None and cursor is not None and cursor <= since_val:
                continue
            # Init-time cap: skip records modified after the connector started.
            if cursor is not None and cursor > self._init_ts:
                continue

            rec = transform(raw) if transform else self._shape_common(raw)
            records.append(rec)
            if cursor is not None and (max_seen is None or cursor > max_seen):
                max_seen = cursor

            if max_records is not None and len(records) >= max_records:
                break

        if not records or max_seen is None or max_seen == since_val:
            # No forward progress — return the offset unchanged so the
            # framework sees end_offset == start_offset and terminates.
            return iter(records), start_offset

        return iter(records), {"cursor": max_seen}

    # ------------------------------------------------------------------ #
    # Record shaping
    # ------------------------------------------------------------------ #

    def _shape_common(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Shape an asset record: normalize nested refs, stamp org."""
        rec = dict(raw)
        rec["domain"] = resource_ref(raw.get("domain"))
        rec["type"] = resource_ref(raw.get("type"))
        rec["status"] = resource_ref(raw.get("status"))
        rec["collibra_org"] = self.org
        return rec

    def _shape_attribute(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["value"] = self._coerce_value_to_str(raw.get("value"))
        rec["type"] = resource_ref(raw.get("type"))
        rec["asset"] = resource_ref(raw.get("asset"))
        rec["collibra_org"] = self.org
        return rec

    def _shape_responsibility(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["role"] = resource_ref(raw.get("role"))
        rec["baseResource"] = resource_ref(raw.get("baseResource"))
        rec["owner"] = resource_ref(raw.get("owner"))
        rec["collibra_org"] = self.org
        return rec

    def _shape_domain(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["community"] = resource_ref(raw.get("community"))
        rec["type"] = resource_ref(raw.get("type"))
        rec["collibra_org"] = self.org
        return rec

    @staticmethod
    def _coerce_value_to_str(value: Any) -> str | None:
        """Coerce a polymorphic attribute ``value`` to a string.

        MultiValueListAttribute yields a list and other discriminators yield
        scalars; we JSON-encode non-string containers and stringify scalars so
        the raw value survives while the column stays a plain ``StringType``.
        The ``attributeDiscriminator`` field lets downstream jobs re-type it.
        """
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table {table_name!r}; "
                f"supported: {SUPPORTED_TABLES}"
            )

    @staticmethod
    def _parse_max_records(table_options: dict[str, str]) -> int | None:
        """Parse ``max_records_per_batch`` (defaults to a bounded batch)."""
        raw = table_options.get("max_records_per_batch")
        if raw is None:
            return DEFAULT_MAX_RECORDS_PER_BATCH
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return DEFAULT_MAX_RECORDS_PER_BATCH
        return value if value > 0 else None

    @staticmethod
    def _as_cursor(value: Any) -> int | None:
        """Coerce a ``lastModifiedOn`` value to a comparable int.

        Real Collibra returns int64 epoch ms; numeric strings are tolerated.
        Non-numeric values (e.g. absent cursors) return ``None`` and are
        treated as uncomparable so they never break ordering.
        """
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return None
