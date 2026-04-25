import logging
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
        table_name: str,
        response_key: str,
        extra_params: dict[str, str] | None = None,
    ) -> tuple[Iterator[dict], dict]:
        """Generic CDC reader using ``updated_at`` watermark.

        - Uses Shopify's ``updated_at_min`` server-side filter (inclusive)
        - Client-side strict ``> watermark`` to make filter exclusive for
          stable termination semantics
        - Watermark advances to ``max(updated_at)`` across returned records,
          so ``end_offset == start_offset`` exactly when no records have
          changed since the last sync
        - Pagination via Link-header cursor (``page_info``) — see utils
        """
        start_offset = start_offset or {}
        since: str | None = start_offset.get("updated_at")

        params: dict[str, str] = {"limit": "250"}
        if extra_params:
            params.update(extra_params)
        if since:
            params["updated_at_min"] = since

        url = f"{self.base_url}/{table_name}.json"

        records: list[dict[str, Any]] = []
        max_seen: str | None = since
        for raw in paginate_get(
            self._session, url, params, table_name, response_key
        ):
            rec_updated = raw.get("updated_at")
            # Strict `>` to keep boundary records from re-emitting on
            # every run (Shopify's filter is inclusive).
            if since and rec_updated and rec_updated <= since:
                continue
            rec = dict(raw)
            rec["shop"] = self.shop
            records.append(rec)
            if rec_updated and (
                max_seen is None or rec_updated > max_seen
            ):
                max_seen = rec_updated

        end_offset = {"updated_at": max_seen} if max_seen else {}
        return iter(records), end_offset

    def _read_customers(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_cdc_table(
            start_offset,
            table_name="customers",
            response_key="customers",
        )

    def _read_products(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._read_cdc_table(
            start_offset,
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
            table_name="orders",
            response_key="orders",
            extra_params={"status": "any"},
        )
