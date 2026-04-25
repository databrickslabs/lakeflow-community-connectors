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
