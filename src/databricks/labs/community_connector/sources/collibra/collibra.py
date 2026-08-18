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
    ``lastModifiedOn`` cursor (int64 epoch ms). There is no server-side
    modified-since filter, so the cursor is applied client-side. Sort order
    differs by endpoint: ``attributes`` / ``responsibilities`` are server-sorted
    ``LAST_MODIFIED ASC`` (so batches truncate on a soft, tie-group-aware cap),
    but ``/assets`` rejects ``LAST_MODIFIED`` (400) and is sorted by ``ID`` —
    records there do NOT arrive in cursor order, so the ``assets`` path drains
    the full collection per run rather than advancing the watermark on a
    partial slice (see ``_incremental_from_iter``). An init-time upper bound
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
    cursor_paginate_pages,
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

        ``sortField`` is required on ``/assets``. Live-validated
        (databricks.collibra.com, 2026-07-25): ``/assets`` accepts ONLY
        ``NAME`` / ``DISPLAY_NAME`` / ``ID`` — ``LAST_MODIFIED`` is rejected
        (400); that value is valid only on ``/responsibilities`` and
        ``/relations``. We therefore default to ``ID`` (stable keyset order),
        and the ``lastModifiedOn`` incremental cursor is applied CLIENT-SIDE
        (see ``_read_cdc_incremental``) rather than relying on server ordering.
        Overridable via the ``sort_field`` table option (NAME/DISPLAY_NAME/ID).
        """
        sort_field = table_options.get("sort_field", "ID")
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

        # `/assets` is keyset-paginated by the sort field (LAST_MODIFIED is
        # rejected 400), so records do NOT arrive in lastModifiedOn order and a
        # count-based batch cap cannot safely advance the lastModifiedOn
        # watermark. Two strategies:
        #
        #   * sort_field == "ID" (default, UNIQUE keyset): use the resumable
        #     path — page by the opaque nextCursor (which is an `AFTER:id:`
        #     keyset token), honor max_records_per_batch as a page-granular cap,
        #     and only advance the lastModifiedOn watermark when a full id-pass
        #     completes. This lets a run killed mid-collection (e.g. m2m token
        #     expiry on a large full-load) resume from the last page instead of
        #     restarting. See `_incremental_assets_resumable`.
        #   * sort_field in {NAME, DISPLAY_NAME} (NON-unique): fall back to the
        #     full-drain path (ignore the cap, drain the whole collection per
        #     run bounded by _init_ts). A non-unique sort key can't be a safe
        #     resume cursor, so we don't batch it.
        if sort_field == "ID":
            return self._incremental_assets_resumable(
                base_params, start_offset, table_options
            )

        record_iter = cursor_paginate(
            self._session, f"{self.base_url}/assets", base_params, "assets"
        )
        return self._incremental_from_iter(
            record_iter, start_offset, table_options, cursor_sorted=False
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
        *,
        cursor_sorted: bool = True,
    ) -> tuple[Iterator[dict], dict]:
        """Apply the client-side incremental cursor over a record iterator.

        Applies, per record:
          * strict ``> since`` filtering (exclusive lower bound),
          * an init-time upper cap (skip records modified after
            ``self._init_ts``) so Trigger.AvailableNow terminates.

        Truncation depends on the underlying sort order:

        * ``cursor_sorted=True`` (``attributes`` / ``responsibilities`` —
          server-sorted ``LAST_MODIFIED ASC``): ``max_records_per_batch`` acts
          as a **soft** limit. Once the cap is reached we keep draining records
          that share the current max ``lastModifiedOn`` (the boundary tie
          group) before stopping, so the batch never splits a group sharing one
          cursor value. Splitting it would advance the watermark to that value
          and the next run's strict ``> since`` filter would permanently skip
          the un-emitted remainder of the group.

        * ``cursor_sorted=False`` (``assets`` — sorted by ``ID``, because
          ``/assets`` rejects ``LAST_MODIFIED``): records do **not** arrive in
          ``lastModifiedOn`` order, so no count-based truncation can safely
          advance the ``lastModifiedOn`` watermark (a partial ID-ordered slice
          has an arbitrary max cursor; advancing to it silently drops every
          not-yet-seen asset modified at or before it). We therefore ignore
          ``max_records`` and drain the full collection, bounded only by the
          ``self._init_ts`` upper cap; ``max_seen`` is then the true max over
          the whole scan. (A resumable ID-keyset cursor is the follow-up that
          would let this path batch safely — see README.)

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

        # Count-based truncation is only sound when records arrive in cursor
        # order (see the assets note above).
        max_records = self._parse_max_records(table_options) if cursor_sorted else None

        records: list[dict[str, Any]] = []
        max_seen = since_val
        capped = False
        for raw in record_iter:
            cursor = self._as_cursor(raw.get("lastModifiedOn"))
            # Strict `>` so the inclusive nature of a resumed `since` doesn't
            # re-emit the boundary record.
            if since_val is not None and cursor is not None and cursor <= since_val:
                continue
            # Init-time cap: skip records modified after the connector started.
            if cursor is not None and cursor > self._init_ts:
                continue

            # Soft cap: past the limit we only keep draining the boundary tie
            # group; the first record with a strictly greater cursor ends it.
            if capped and cursor is not None and max_seen is not None and cursor > max_seen:
                break

            rec = transform(raw) if transform else self._shape_asset(raw)
            records.append(rec)
            if cursor is not None and (max_seen is None or cursor > max_seen):
                max_seen = cursor

            if max_records is not None and len(records) >= max_records:
                capped = True

        if not records or max_seen is None or max_seen == since_val:
            # No forward progress — return the offset unchanged so the
            # framework sees end_offset == start_offset and terminates.
            return iter(records), start_offset

        return iter(records), {"cursor": max_seen}

    def _incremental_assets_resumable(
        self,
        base_params: dict[str, str],
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Resumable incremental read for the ID-sorted ``assets`` path.

        ``/assets`` is keyset-paginated by ``id`` (its opaque ``nextCursor`` is
        an ``AFTER:id:<id>`` token), but filtered incrementally on
        ``lastModifiedOn``. Those are two independent dimensions, so we track
        them separately in the offset:

          * ``cursor``     — the committed ``lastModifiedOn`` watermark (floor).
                             Only advances when a full id-pass completes.
          * ``page_token`` — the opaque nextCursor to resume an in-progress
                             id-pass. Absent ⇒ start a fresh pass.
          * ``pass_ts``    — ``_init_ts`` frozen at the start of a pass, kept
                             across resumes so the pass sees one consistent
                             snapshot upper bound.

        Per run we page by id from ``page_token`` (or the start), emitting rows
        where ``cursor < lastModifiedOn <= pass_ts``. ``max_records_per_batch``
        is a page-granular cap: once reached we stop at the page boundary and
        persist ``page_token`` = that page's nextCursor, leaving the committed
        watermark UNCHANGED (we haven't proven we've seen everything ≤ any given
        lastModifiedOn yet). When pagination is exhausted the pass is complete —
        we advance the committed watermark to ``pass_ts`` and clear
        ``page_token``/``pass_ts`` so the next run starts a fresh pass from the
        new floor. A run killed mid-pass (e.g. m2m token expiry) resumes from
        the last committed ``page_token`` instead of restarting.

        Progress within a pass is by id (the true sort key), so truncation never
        skips a record; the watermark only moves after a provably complete pass,
        so the next fresh pass's strict ``> committed`` filter can't skip an
        un-emitted record.
        """
        start_offset = start_offset or {}
        committed = self._as_cursor(start_offset.get("cursor"))
        page_token = start_offset.get("page_token") or ""
        # Freeze the snapshot boundary at pass start; keep it across resumes.
        pass_ts = start_offset.get("pass_ts")
        if not page_token or pass_ts is None:
            # Fresh pass.
            page_token = ""
            pass_ts = self._init_ts
        pass_ts = int(pass_ts)

        # Already caught up: committed floor has reached the snapshot boundary
        # and no pass is in flight — nothing new to emit, converge.
        if not page_token and committed is not None and committed >= pass_ts:
            return iter([]), {"cursor": committed}

        max_records = self._parse_max_records(table_options)
        url = f"{self.base_url}/assets"

        records: list[dict[str, Any]] = []
        next_page_token: str | None = None
        pass_complete = True
        for batch, next_cursor in cursor_paginate_pages(
            self._session, url, base_params, "assets", start_cursor=page_token
        ):
            for raw in batch:
                cursor = self._as_cursor(raw.get("lastModifiedOn"))
                # Incremental window: (committed, pass_ts].
                if committed is not None and cursor is not None and cursor <= committed:
                    continue
                if cursor is not None and cursor > pass_ts:
                    continue
                records.append(self._shape_asset(raw))

            # Page-granular cap: stop at this boundary, resume here next run.
            # Only a page that has a real nextCursor can be a resume point; if
            # next_cursor is None this was the last page (pass completes).
            if (
                max_records is not None
                and len(records) >= max_records
                and next_cursor is not None
            ):
                next_page_token = next_cursor
                pass_complete = False
                break
        else:
            # Iterator exhausted without hitting the cap ⇒ pass complete.
            pass_complete = True

        if pass_complete:
            # Whole id-space scanned under one snapshot: safe to advance the
            # committed watermark to the snapshot boundary and clear pass state.
            new_committed = pass_ts if committed is None else max(committed, pass_ts)
            end_offset = {"cursor": new_committed}
        else:
            # Mid-pass: hold the watermark, persist the id resume point + the
            # frozen snapshot boundary.
            end_offset = {
                "cursor": committed if committed is not None else 0,
                "page_token": next_page_token,
                "pass_ts": pass_ts,
            }

        return iter(records), end_offset

    # ------------------------------------------------------------------ #
    # Record shaping
    # ------------------------------------------------------------------ #

    def _shape_asset(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Shape an asset record: normalize nested refs, stamp org.

        Used as the default transform for the ``assets`` table only;
        ``attributes`` / ``responsibilities`` / ``domains`` each pass their own
        explicit shaper.
        """
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
