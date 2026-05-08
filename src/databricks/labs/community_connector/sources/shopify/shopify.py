import logging
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.shopify.shopify_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.shopify.shopify_utils import (
    api_get,
    paginate_get,
)


_LOG = logging.getLogger(__name__)

DEFAULT_API_VERSION = "2026-04"


def _parse_max_records(table_options: dict[str, str]) -> int | None:
    """Parse the optional ``max_records_per_batch`` admission-control cap.

    Returns ``None`` when unset or non-numeric (uncapped).
    """
    raw = table_options.get("max_records_per_batch")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


class ShopifyLakeflowConnect(LakeflowConnect):
    """Shopify Admin REST API connector.

    Auth: Admin API access token (custom-app token, format ``shpat_...``).
    Pagination: Link-header cursor pagination (``page_info`` parameter).
    Versioning: API version pinned via the ``api_version`` connection
    option; defaults to the latest stable version supported by this
    connector.

    See ``shopify_api_doc.md`` for endpoint specifics, scope mapping,
    rate-limit behavior, and per-table edge cases.
    """

    def __init__(self, options: dict[str, str]) -> None:
        """Initialize the Shopify connector.

        Expected options:
            - shop: Shopify shop subdomain (e.g. ``"lakeflow-test-store"``)
            - access_token: Admin API access token
            - api_version: API version string (optional, default ``2026-04``)
        """
        shop = options.get("shop")
        access_token = options.get("access_token")
        api_version = options.get("api_version") or DEFAULT_API_VERSION

        if not shop:
            raise ValueError("Shopify connector requires 'shop'")
        if not access_token:
            raise ValueError("Shopify connector requires 'access_token'")

        self.shop = shop
        self.api_version = api_version
        self.base_url = (
            f"https://{shop}.myshopify.com/admin/api/{api_version}"
        )

        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-Shopify-Access-Token": access_token,
                "Accept": "application/json",
            }
        )

        # Cap incremental cursors at init time so a single trigger only
        # drains data that existed when the connector started; later data
        # is picked up by the next trigger with a fresh _init_time.
        # This is the AvailableNow termination guard.
        self._init_time = (
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )

    # ------------------------------------------------------------------ #
    # Interface methods
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        dispatch = {
            "locations": self._read_locations,
            "customers": self._read_customers,
            "products": self._read_products,
            "orders": self._read_orders,
            "refunds": self._read_refunds,
            "fulfillments": self._read_fulfillments,
            "inventory_levels": self._read_inventory_levels,
        }
        handler = dispatch.get(table_name)
        if handler is None:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return handler(start_offset, table_options)

    # ------------------------------------------------------------------ #
    # Table readers
    # ------------------------------------------------------------------ #

    def _read_locations(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read locations as a snapshot (no pagination, no incremental).

        Locations endpoint has no incremental filter and no cursor
        pagination — Shopify caps the response at the full set, which
        is small for almost all merchants. Snapshot ingestion is the
        right fit.
        """
        data, _ = api_get(
            self._session,
            f"{self.base_url}/locations.json",
            params=None,
            label="locations",
        )
        records: list[dict[str, Any]] = []
        for loc in data.get("locations", []):
            rec = dict(loc)
            rec["shop"] = self.shop
            records.append(rec)
        return iter(records), {}

    # -- CDC tables (customers, products, orders) ---------------------- #

    def _read_cdc_table(
        self,
        start_offset: dict,
        table_options: dict[str, str],
        table_name: str,
        response_key: str,
        extra_params: dict[str, str] | None = None,
    ) -> tuple[Iterator[dict], dict]:
        """Generic CDC reader using ``updated_at`` watermark.

        - Server-side filter via Shopify's ``updated_at_min`` (inclusive).
        - Client-side strict ``>`` dedup to make the boundary exclusive.
        - Cursor capped at ``self._init_time`` so AvailableNow terminates
          even on a busy store. Records edited after init time are picked
          up by the next trigger with a fresh ``_init_time``.
        - Admission control via ``max_records_per_batch`` table option.
          When set and reached mid-walk, returns a partial cursor at the
          last-emitted record's ``updated_at`` so the next microbatch
          resumes from there. Records are sorted ascending by
          ``updated_at`` server-side, so the cap point is well-defined.
        - Pagination via Link-header cursor (``page_info``) — see utils.
        """
        start_offset = start_offset or {}
        since: str | None = start_offset.get("updated_at")

        # If we've already drained up to init_time, return stable offset
        # so the framework terminates this microbatch.
        if since and since >= self._init_time:
            return iter([]), start_offset

        max_records = _parse_max_records(table_options)

        params: dict[str, str] = {
            "limit": "250",
            "order": "updated_at asc",  # ensures admission-cap boundary
        }
        if extra_params:
            params.update(extra_params)
        if since:
            params["updated_at_min"] = since
        # Cap server-side at init_time so the page never includes records
        # edited after the connector started.
        params["updated_at_max"] = self._init_time

        url = f"{self.base_url}/{table_name}.json"

        records: list[dict[str, Any]] = []
        max_seen: str | None = since
        cap_hit = False
        for raw in paginate_get(
            self._session, url, params, table_name, response_key
        ):
            rec_updated = raw.get("updated_at")
            # Strict `>` to make Shopify's inclusive filter exclusive.
            if since and rec_updated and rec_updated <= since:
                continue
            # Defensive: drop anything past the init-time cap that may
            # have slipped through if the server ignored updated_at_max.
            if rec_updated and rec_updated > self._init_time:
                continue
            rec = dict(raw)
            rec["shop"] = self.shop
            records.append(rec)
            if rec_updated and (
                max_seen is None or rec_updated > max_seen
            ):
                max_seen = rec_updated
            if max_records is not None and len(records) >= max_records:
                cap_hit = True
                break

        if cap_hit:
            # Resume from the last emitted record on the next call.
            return iter(records), {"updated_at": max_seen}

        # Window fully drained — advance to init_time so a follow-up
        # call with this offset terminates immediately via the early
        # exit above. Without this, drained-but-empty windows would keep
        # the offset at `since` forever and AvailableNow wouldn't see
        # forward progress between non-empty trigger windows.
        end_cursor = max_seen if max_seen and max_seen >= self._init_time else self._init_time
        return iter(records), {"updated_at": end_cursor}

    def _read_customers(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_cdc_table(
            start_offset,
            table_options,
            table_name="customers",
            response_key="customers",
        )

    def _read_products(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_cdc_table(
            start_offset,
            table_options,
            table_name="products",
            response_key="products",
        )

    def _read_orders(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # Default `status=any` so cancelled / closed orders are
        # included — Shopify's default of `status=open` would silently
        # drop them. See shopify_api_doc.md for the gotcha.
        return self._read_cdc_table(
            start_offset,
            table_options,
            table_name="orders",
            response_key="orders",
            extra_params={"status": "any"},
        )

    # -- Per-order child resources (refunds, fulfillments) ------------- #

    def _read_per_order_table(
        self,
        start_offset: dict,
        table_options: dict[str, str],
        sub_resource: str,
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Read a per-order child resource using the parent-orders trick.

        Discovers candidate orders via the orders endpoint
        (``updated_at_min=watermark``, Shopify's order ``updated_at``
        advances when refunds/fulfillments are added), then fetches the
        sub-resource per order. Records are filtered client-side by
        ``cursor_field > watermark`` and capped at ``self._init_time``.

        Honors ``max_records_per_batch`` for admission control: when
        reached mid-walk, stops at the next order boundary and returns
        a partial cursor at the last-emitted record's cursor.
        """
        start_offset = start_offset or {}
        since: str | None = start_offset.get(cursor_field)

        # Already drained up to init_time — nothing to do.
        if since and since >= self._init_time:
            return iter([]), start_offset

        max_records = _parse_max_records(table_options)

        orders_params: dict[str, str] = {
            "limit": "250",
            "status": "any",
            # Cap the parent walk at init_time so we don't chase orders
            # that arrived after the connector started.
            "updated_at_max": self._init_time,
        }
        if since:
            orders_params["updated_at_min"] = since

        all_records: list[dict[str, Any]] = []
        max_seen: str | None = since
        cap_hit = False

        for order in paginate_get(
            self._session,
            f"{self.base_url}/orders.json",
            orders_params,
            f"{sub_resource}-discovery",
            "orders",
        ):
            order_id = order.get("id")
            if not order_id:
                continue
            try:
                data, _ = api_get(
                    self._session,
                    f"{self.base_url}/orders/{order_id}/{sub_resource}.json",
                    params=None,
                    label=sub_resource,
                )
            except RuntimeError as exc:
                # Skip individual orders that fail (e.g. permissions on a
                # specific order) so the whole batch doesn't blow up.
                _LOG.warning(
                    "Skipping %s for order %s: %s",
                    sub_resource, order_id, exc,
                )
                continue
            for item in data.get(sub_resource, []):
                ts = item.get(cursor_field)
                if since and ts and ts <= since:
                    continue
                # Defensive: drop records past init_time cap.
                if ts and ts > self._init_time:
                    continue
                rec = dict(item)
                rec["shop"] = self.shop
                all_records.append(rec)
                if ts and (max_seen is None or ts > max_seen):
                    max_seen = ts
            if max_records is not None and len(all_records) >= max_records:
                cap_hit = True
                break

        if cap_hit:
            return iter(all_records), {cursor_field: max_seen}

        # Window fully drained — advance to init_time so next call sees
        # `since >= self._init_time` and terminates immediately.
        end_cursor = (
            max_seen
            if max_seen and max_seen >= self._init_time
            else self._init_time
        )
        return iter(all_records), {cursor_field: end_cursor}

    def _read_refunds(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_per_order_table(
            start_offset,
            table_options,
            sub_resource="refunds",
            cursor_field="created_at",
        )

    def _read_fulfillments(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_per_order_table(
            start_offset,
            table_options,
            sub_resource="fulfillments",
            cursor_field="updated_at",
        )

    # -- Inventory ----------------------------------------------------- #

    def _read_inventory_levels(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read inventory levels across all locations.

        The /inventory_levels.json endpoint requires ``location_ids``
        or ``inventory_item_ids`` as a query param, so we first list
        locations and then iterate. The endpoint has no date filter,
        so we fetch all levels each run; the framework's CDC merge on
        the composite ``(inventory_item_id, location_id)`` PK handles
        deltas idempotently.

        Watermark is capped at ``self._init_time`` and advanced to it
        on every call. This guarantees AvailableNow termination even
        when stock is updated continuously: the trigger only ingests
        the snapshot that existed at connector init.

        Honors ``max_records_per_batch`` for admission control: when
        the cap is hit mid-walk we stop and emit the location ID we
        were on so the next microbatch resumes from there.
        """
        start_offset = start_offset or {}
        # Resume cursor: for in-flight microbatches we track which
        # location index we last finished. None = start from the top.
        resume_loc = start_offset.get("location_id")
        max_records = _parse_max_records(table_options)

        locs_data, _ = api_get(
            self._session,
            f"{self.base_url}/locations.json",
            params=None,
            label="locations",
        )
        active_locs = sorted(
            str(loc["id"])
            for loc in locs_data.get("locations", [])
            if loc.get("id") and loc.get("active")
        )

        # Find resume point if this is a continuation microbatch.
        try:
            start_idx = (
                active_locs.index(resume_loc) if resume_loc else 0
            )
        except ValueError:
            start_idx = 0

        all_records: list[dict[str, Any]] = []
        last_loc: str | None = resume_loc
        cap_hit = False
        for loc_id in active_locs[start_idx:]:
            for level in paginate_get(
                self._session,
                f"{self.base_url}/inventory_levels.json",
                {"limit": "250", "location_ids": loc_id},
                "inventory_levels",
                "inventory_levels",
            ):
                rec = dict(level)
                rec["shop"] = self.shop
                all_records.append(rec)
            last_loc = loc_id
            if max_records is not None and len(all_records) >= max_records:
                cap_hit = True
                break

        if cap_hit:
            return iter(all_records), {"location_id": last_loc}

        # All locations drained — return a stable terminal offset that
        # signals "done for this trigger". Anchored to init_time so a
        # subsequent trigger with a fresh _init_time supersedes it.
        return iter(all_records), {"updated_at": self._init_time}
