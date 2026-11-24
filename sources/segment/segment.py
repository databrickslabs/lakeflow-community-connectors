# pylint: disable=too-many-lines
"""
Segment Public API Connector for Lakeflow Connect.

This connector implements the LakeflowConnect interface to ingest data from
Segment's Public API, which provides access to workspace configuration and metadata.
"""
from typing import Iterator, Any

import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    ArrayType,
    MapType,
)


class LakeflowConnect:
    """Segment Public API connector implementation."""

    # Supported tables
    SUPPORTED_TABLES = [
        "sources",
        "destinations",
        "warehouses",
        "catalog_sources",
        "catalog_destinations",
        "catalog_warehouses",
        "tracking_plans",
        "users",
        "labels",
        "audit_events",
        "spaces",
        "reverse_etl_models",
        "transformations",
        "usage_api_calls_daily",
        "usage_mtu_daily",
    ]

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Segment connector with connection-level options.

        Expected options:
            - api_token (required): Public API token from Segment's Workspace settings.
            - region (optional): API region - 'api' for US or 'eu1' for EU (default: 'api').
        """
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("Segment connector requires 'api_token' in options")

        region = options.get("region", "api")
        if region not in ("api", "eu1"):
            raise ValueError(
                f"Invalid region '{region}'. Must be 'api' (US) or 'eu1' (EU)"
            )

        self.base_url = f"https://{region}.segmentapis.com"

        # Configure a session with proper headers for Segment Public API
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
            }
        )

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.
        """
        return self.SUPPORTED_TABLES.copy()

    def _validate_table(self, table_name: str) -> None:
        """Validate that the table is supported."""
        if table_name not in self.SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table: {table_name!r}. "
                f"Supported tables: {self.SUPPORTED_TABLES}"
            )

    # -------------------------------------------------------------------------
    # Schema definitions
    # -------------------------------------------------------------------------

    def _get_logos_struct(self) -> StructType:
        """Return the logos struct schema."""
        return StructType(
            [
                StructField("default", StringType(), True),
                StructField("mark", StringType(), True),
                StructField("alt", StringType(), True),
            ]
        )

    def _get_metadata_struct(self) -> StructType:
        """Return the common metadata struct schema for sources/destinations/warehouses."""
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("slug", StringType(), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("categories", ArrayType(StringType(), True), True),
                StructField("logos", self._get_logos_struct(), True),
            ]
        )

    def _get_label_struct(self) -> StructType:
        """Return the label struct schema."""
        return StructType(
            [
                StructField("key", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

    def _get_sources_schema(self) -> StructType:
        """Return the sources table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("slug", StringType(), True),
                StructField("name", StringType(), True),
                StructField("workspaceId", StringType(), True),
                StructField("enabled", BooleanType(), True),
                StructField("writeKeys", ArrayType(StringType(), True), True),
                StructField("metadata", self._get_metadata_struct(), True),
                StructField("settings", MapType(StringType(), StringType(), True), True),
                StructField("labels", ArrayType(self._get_label_struct(), True), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
            ]
        )

    def _get_destinations_schema(self) -> StructType:
        """Return the destinations table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("enabled", BooleanType(), True),
                StructField("workspaceId", StringType(), True),
                StructField("sourceId", StringType(), True),
                StructField("metadata", self._get_metadata_struct(), True),
                StructField("settings", MapType(StringType(), StringType(), True), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
            ]
        )

    def _get_warehouses_schema(self) -> StructType:
        """Return the warehouses table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("workspaceId", StringType(), True),
                StructField("enabled", BooleanType(), True),
                StructField("metadata", self._get_metadata_struct(), True),
                StructField("settings", MapType(StringType(), StringType(), True), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
            ]
        )

    def _get_catalog_sources_schema(self) -> StructType:
        """Return the catalog_sources table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("slug", StringType(), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("categories", ArrayType(StringType(), True), True),
                StructField("logos", self._get_logos_struct(), True),
                StructField("isCloudEventSource", BooleanType(), True),
            ]
        )

    def _get_catalog_destinations_schema(self) -> StructType:
        """Return the catalog_destinations table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("slug", StringType(), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("categories", ArrayType(StringType(), True), True),
                StructField("logos", self._get_logos_struct(), True),
                StructField("status", StringType(), True),
                StructField(
                    "supportedMethods", MapType(StringType(), BooleanType(), True), True
                ),
            ]
        )

    def _get_catalog_warehouses_schema(self) -> StructType:
        """Return the catalog_warehouses table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("slug", StringType(), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("logos", self._get_logos_struct(), True),
            ]
        )

    def _get_tracking_plans_schema(self) -> StructType:
        """Return the tracking_plans table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("slug", StringType(), True),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("type", StringType(), True),
                StructField("workspaceId", StringType(), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
            ]
        )

    def _get_users_schema(self) -> StructType:
        """Return the users table schema."""
        permission_struct = StructType(
            [
                StructField("roleId", StringType(), True),
                StructField("roleName", StringType(), True),
                StructField("resources", ArrayType(MapType(StringType(), StringType(), True), True), True),
            ]
        )
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("permissions", ArrayType(permission_struct, True), True),
            ]
        )

    def _get_labels_schema(self) -> StructType:
        """Return the labels table schema."""
        return StructType(
            [
                StructField("key", StringType(), False),
                StructField("value", StringType(), False),
                StructField("description", StringType(), True),
            ]
        )

    def _get_audit_events_schema(self) -> StructType:
        """Return the audit_events table schema."""
        actor_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("type", StringType(), True),
                StructField("email", StringType(), True),
            ]
        )
        resource_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("type", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("timestamp", StringType(), True),
                StructField("type", StringType(), True),
                StructField("actor", actor_struct, True),
                StructField("resource", resource_struct, True),
            ]
        )

    def _get_spaces_schema(self) -> StructType:
        """Return the spaces table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("slug", StringType(), True),
                StructField("name", StringType(), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
            ]
        )

    def _get_reverse_etl_models_schema(self) -> StructType:
        """Return the reverse_etl_models table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("enabled", BooleanType(), True),
                StructField("sourceId", StringType(), True),
                StructField("scheduleStrategy", StringType(), True),
                StructField("query", StringType(), True),
                StructField("queryIdentifierColumn", StringType(), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
            ]
        )

    def _get_transformations_schema(self) -> StructType:
        """Return the transformations table schema."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("sourceId", StringType(), True),
                StructField("destinationMetadataId", StringType(), True),
                StructField("enabled", BooleanType(), True),
                StructField("code", StringType(), True),
                StructField("createdAt", StringType(), True),
                StructField("updatedAt", StringType(), True),
            ]
        )

    def _get_usage_api_calls_daily_schema(self) -> StructType:
        """Return the usage_api_calls_daily table schema."""
        return StructType(
            [
                StructField("timestamp", StringType(), False),
                StructField("apiCalls", LongType(), True),
                StructField("sourceId", StringType(), True),
                StructField("workspaceId", StringType(), True),
            ]
        )

    def _get_usage_mtu_daily_schema(self) -> StructType:
        """Return the usage_mtu_daily table schema."""
        return StructType(
            [
                StructField("timestamp", StringType(), False),
                StructField("mtu", LongType(), True),
                StructField("sourceId", StringType(), True),
                StructField("workspaceId", StringType(), True),
            ]
        )

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        """
        self._validate_table(table_name)

        schema_map = {
            "sources": self._get_sources_schema,
            "destinations": self._get_destinations_schema,
            "warehouses": self._get_warehouses_schema,
            "catalog_sources": self._get_catalog_sources_schema,
            "catalog_destinations": self._get_catalog_destinations_schema,
            "catalog_warehouses": self._get_catalog_warehouses_schema,
            "tracking_plans": self._get_tracking_plans_schema,
            "users": self._get_users_schema,
            "labels": self._get_labels_schema,
            "audit_events": self._get_audit_events_schema,
            "spaces": self._get_spaces_schema,
            "reverse_etl_models": self._get_reverse_etl_models_schema,
            "transformations": self._get_transformations_schema,
            "usage_api_calls_daily": self._get_usage_api_calls_daily_schema,
            "usage_mtu_daily": self._get_usage_mtu_daily_schema,
        }

        return schema_map[table_name]()

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table.
        """
        self._validate_table(table_name)

        metadata_map = {
            "sources": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "destinations": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "warehouses": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "catalog_sources": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "catalog_destinations": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "catalog_warehouses": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "tracking_plans": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "users": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "labels": {
                "primary_keys": ["key", "value"],
                "ingestion_type": "snapshot",
            },
            "audit_events": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "spaces": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "reverse_etl_models": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "transformations": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "usage_api_calls_daily": {
                "primary_keys": ["timestamp"],
                "cursor_field": "timestamp",
                "ingestion_type": "cdc",
            },
            "usage_mtu_daily": {
                "primary_keys": ["timestamp"],
                "cursor_field": "timestamp",
                "ingestion_type": "cdc",
            },
        }

        return metadata_map[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a table and return raw JSON-like dictionaries.
        """
        self._validate_table(table_name)

        reader_map = {
            "sources": self._read_sources,
            "destinations": self._read_destinations,
            "warehouses": self._read_warehouses,
            "catalog_sources": self._read_catalog_sources,
            "catalog_destinations": self._read_catalog_destinations,
            "catalog_warehouses": self._read_catalog_warehouses,
            "tracking_plans": self._read_tracking_plans,
            "users": self._read_users,
            "labels": self._read_labels,
            "audit_events": self._read_audit_events,
            "spaces": self._read_spaces,
            "reverse_etl_models": self._read_reverse_etl_models,
            "transformations": self._read_transformations,
            "usage_api_calls_daily": self._read_usage_api_calls_daily,
            "usage_mtu_daily": self._read_usage_mtu_daily,
        }

        return reader_map[table_name](start_offset, table_options)

    # -------------------------------------------------------------------------
    # Helper methods for API requests
    # -------------------------------------------------------------------------

    def _paginated_get(
        self,
        endpoint: str,
        data_key: str,
        table_options: dict[str, str],
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Perform a paginated GET request and return all records.

        Args:
            endpoint: API endpoint path (e.g., '/sources')
            data_key: Key in response.data containing the array of records
            table_options: Table options that may contain pagination settings
            params: Optional query parameters

        Returns:
            List of all records from all pages
        """
        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        try:
            page_size = int(table_options.get("page_size", 100))
        except (TypeError, ValueError):
            page_size = 100

        url = f"{self.base_url}{endpoint}"
        records: list[dict[str, Any]] = []
        cursor: str | None = None
        pages_fetched = 0

        while pages_fetched < max_pages:
            request_params = params.copy() if params else {}
            request_params["pagination.count"] = page_size
            if cursor:
                request_params["pagination.cursor"] = cursor

            response = self._session.get(url, params=request_params, timeout=60)
            if response.status_code == 403:
                # 403 Forbidden typically means the feature is not enabled for this workspace
                # (e.g., Audit Trail requires enterprise plan). Return empty results gracefully.
                return records
            if response.status_code != 200:
                raise RuntimeError(
                    f"Segment API error for {endpoint}: "
                    f"{response.status_code} {response.text}"
                )

            response_data = response.json()
            data = response_data.get("data", {})

            # Extract records from the response
            items = data.get(data_key, [])
            if isinstance(items, list):
                records.extend(items)
            elif isinstance(items, dict):
                # Some endpoints return a single object, wrap in list
                records.append(items)

            # Check for next page
            pagination = response_data.get("pagination", {})
            next_cursor = pagination.get("next")
            if not next_cursor or next_cursor == cursor:
                break

            cursor = next_cursor
            pages_fetched += 1

        return records

    # -------------------------------------------------------------------------
    # Table reader implementations
    # -------------------------------------------------------------------------

    def _read_sources(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the sources table."""
        records = self._paginated_get("/sources", "sources", table_options)
        return iter(records), {}

    def _read_destinations(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the destinations table."""
        records = self._paginated_get("/destinations", "destinations", table_options)
        return iter(records), {}

    def _read_warehouses(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the warehouses table."""
        records = self._paginated_get("/warehouses", "warehouses", table_options)
        return iter(records), {}

    def _read_catalog_sources(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the catalog_sources table."""
        records = self._paginated_get(
            "/catalog/sources", "sourcesCatalog", table_options
        )
        return iter(records), {}

    def _read_catalog_destinations(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the catalog_destinations table."""
        records = self._paginated_get(
            "/catalog/destinations", "destinationsCatalog", table_options
        )
        return iter(records), {}

    def _read_catalog_warehouses(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the catalog_warehouses table."""
        records = self._paginated_get(
            "/catalog/warehouses", "warehousesCatalog", table_options
        )
        return iter(records), {}

    def _read_tracking_plans(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the tracking_plans table."""
        records = self._paginated_get(
            "/tracking-plans", "trackingPlans", table_options
        )
        return iter(records), {}

    def _read_users(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the users table."""
        records = self._paginated_get("/users", "users", table_options)
        return iter(records), {}

    def _read_labels(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the labels table."""
        records = self._paginated_get("/labels", "labels", table_options)
        return iter(records), {}

    def _read_audit_events(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the audit_events table."""
        records = self._paginated_get("/audit-events", "auditEvents", table_options)
        return iter(records), {}

    def _read_spaces(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the spaces table."""
        records = self._paginated_get("/spaces", "spaces", table_options)
        return iter(records), {}

    def _read_reverse_etl_models(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the reverse_etl_models table."""
        records = self._paginated_get(
            "/reverse-etl-models", "reverseEtlModels", table_options
        )
        return iter(records), {}

    def _read_transformations(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read the transformations table."""
        records = self._paginated_get(
            "/transformations", "transformations", table_options
        )
        return iter(records), {}

    def _read_usage_api_calls_daily(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read the usage_api_calls_daily table.

        This stream supports incremental sync using timestamp as the cursor.
        """
        # Determine the start date from offset or table_options
        cursor = None
        if start_offset and isinstance(start_offset, dict):
            cursor = start_offset.get("cursor")
        if not cursor:
            cursor = table_options.get("start_date")

        params = {}
        if cursor:
            params["period"] = cursor

        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        url = f"{self.base_url}/usage/api-calls/daily"
        records: list[dict[str, Any]] = []
        max_timestamp: str | None = None
        pages_fetched = 0
        next_cursor: str | None = None

        while pages_fetched < max_pages:
            request_params = params.copy()
            if next_cursor:
                request_params["pagination.cursor"] = next_cursor

            response = self._session.get(url, params=request_params, timeout=60)
            if response.status_code != 200:
                raise RuntimeError(
                    f"Segment API error for /usage/api-calls/daily: "
                    f"{response.status_code} {response.text}"
                )

            response_data = response.json()
            data = response_data.get("data", {})
            items = data.get("dailyPerSourceAPICallsUsage", [])

            if isinstance(items, list):
                for item in items:
                    records.append(item)
                    timestamp = item.get("timestamp")
                    if isinstance(timestamp, str):
                        if max_timestamp is None or timestamp > max_timestamp:
                            max_timestamp = timestamp

            pagination = response_data.get("pagination", {})
            next_cursor = pagination.get("next")
            if not next_cursor:
                break

            pages_fetched += 1

        # Compute next offset
        if max_timestamp:
            next_offset = {"cursor": max_timestamp}
        elif start_offset:
            next_offset = start_offset
        else:
            next_offset = {}

        return iter(records), next_offset

    def _read_usage_mtu_daily(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read the usage_mtu_daily table.

        This stream supports incremental sync using timestamp as the cursor.
        """
        # Determine the start date from offset or table_options
        cursor = None
        if start_offset and isinstance(start_offset, dict):
            cursor = start_offset.get("cursor")
        if not cursor:
            cursor = table_options.get("start_date")

        params = {}
        if cursor:
            params["period"] = cursor

        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        url = f"{self.base_url}/usage/mtu/daily"
        records: list[dict[str, Any]] = []
        max_timestamp: str | None = None
        pages_fetched = 0
        next_cursor: str | None = None

        while pages_fetched < max_pages:
            request_params = params.copy()
            if next_cursor:
                request_params["pagination.cursor"] = next_cursor

            response = self._session.get(url, params=request_params, timeout=60)
            if response.status_code != 200:
                raise RuntimeError(
                    f"Segment API error for /usage/mtu/daily: "
                    f"{response.status_code} {response.text}"
                )

            response_data = response.json()
            data = response_data.get("data", {})
            items = data.get("dailyPerSourceMTUUsage", [])

            if isinstance(items, list):
                for item in items:
                    records.append(item)
                    timestamp = item.get("timestamp")
                    if isinstance(timestamp, str):
                        if max_timestamp is None or timestamp > max_timestamp:
                            max_timestamp = timestamp

            pagination = response_data.get("pagination", {})
            next_cursor = pagination.get("next")
            if not next_cursor:
                break

            pages_fetched += 1

        # Compute next offset
        if max_timestamp:
            next_offset = {"cursor": max_timestamp}
        elif start_offset:
            next_offset = start_offset
        else:
            next_offset = {}

        return iter(records), next_offset
