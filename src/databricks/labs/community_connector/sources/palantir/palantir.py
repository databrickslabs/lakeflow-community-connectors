"""
Palantir Foundry Connector for Lakeflow Community Connectors.

This connector enables ingestion of data from Palantir Foundry ontologies
into Databricks using the LakeflowConnect interface.
"""

import requests
import time
from typing import Dict, List, Iterator, Any, Tuple
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    DataType,
)

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect


class PalantirLakeflowConnect(LakeflowConnect):
    """
    Palantir Foundry connector implementing the LakeflowConnect interface.

    Supports:
    - Dynamic schema discovery from Palantir object type definitions
    - Both snapshot and incremental (CDC) ingestion modes
    - Cursor-based pagination using nextPageToken
    - User-configurable cursor fields for incremental sync
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize Palantir Foundry connector with authentication.

        Args:
            options: Connection parameters including:
                - token: Bearer token for API authentication (required)
                - hostname: Palantir Foundry hostname without https:// (required)
                - ontology_api_name: Target ontology API identifier (required)

        Raises:
            ValueError: If required parameters are missing
        """
        # Extract and validate required parameters
        self.token = options.get("token")
        if not self.token:
            raise ValueError("Missing required parameter 'token'")

        self.hostname = options.get("hostname")
        if not self.hostname:
            raise ValueError("Missing required parameter 'hostname'")

        self.ontology_api_name = options.get("ontology_api_name")
        if not self.ontology_api_name:
            raise ValueError("Missing required parameter 'ontology_api_name'")

        # Build base URL
        self.base_url = f"https://{self.hostname}"

        # Configure authenticated session
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        })

        # Initialize caches for performance
        self._schema_cache: Dict[str, StructType] = {}
        self._metadata_cache: Dict[str, dict] = {}
        self._object_types_cache: List[str] = None

    def list_tables(self) -> List[str]:
        """
        List all object types (tables) available in the configured ontology.

        Returns:
            List of object type API names (e.g., ["ExampleFlight", "ExampleRouteAlert"])

        Raises:
            Exception: If API request fails
        """
        # Return cached result if available
        if self._object_types_cache is not None:
            return self._object_types_cache

        # Fetch object types from Palantir API
        url = f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}/objectTypes"

        try:
            response = self._session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise Exception(
                f"Failed to list object types from Palantir ontology "
                f"'{self.ontology_api_name}': {str(e)}"
            )

        data = response.json()
        object_types = [obj["apiName"] for obj in data.get("data", [])]

        # Cache the result
        self._object_types_cache = object_types
        return object_types

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Dynamically discover and return the Spark schema for a Palantir object type.

        Args:
            table_name: Object type API name (e.g., "ExampleFlight")
            table_options: Additional options (not used for schema discovery)

        Returns:
            Spark StructType schema

        Raises:
            ValueError: If table_name is not supported
            Exception: If API request fails
        """
        # Check cache first
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # Validate table exists
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {self.list_tables()}"
            )

        # Fetch object type definition from Palantir API
        url = f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}/objectTypes/{table_name}"

        try:
            response = self._session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise Exception(
                f"Failed to get schema for table '{table_name}': {str(e)}"
            )

        obj_type = response.json()
        schema = self._build_schema_from_object_type(obj_type)

        # Cache the result
        self._schema_cache[table_name] = schema
        return schema

    def _build_schema_from_object_type(self, obj_type: dict) -> StructType:
        """
        Build Spark StructType schema from Palantir object type definition.

        Args:
            obj_type: Object type definition from Palantir API

        Returns:
            Spark StructType schema
        """
        fields: List[StructField] = []

        # Collect primary key field names to avoid duplicates
        primary_key = obj_type.get("primaryKey")
        if isinstance(primary_key, str):
            pk_fields = {primary_key}
        elif isinstance(primary_key, list):
            pk_fields = set(primary_key)
        else:
            pk_fields = set()

        # Add properties from object type definition
        properties = obj_type.get("properties", {})
        for prop_name, prop_def in properties.items():
            data_type_def = prop_def.get("dataType", {})
            spark_type = self._map_palantir_type_to_spark(data_type_def)
            # Primary key fields are non-nullable
            nullable = prop_name not in pk_fields
            fields.append(StructField(prop_name, spark_type, nullable))

        # If primary key is not in properties, add it explicitly
        for pk in pk_fields:
            if pk not in properties:
                fields.insert(0, StructField(pk, StringType(), False))

        return StructType(fields)

    def _map_palantir_type_to_spark(self, palantir_type: dict) -> DataType:
        """
        Map Palantir data type to Spark DataType recursively.

        Args:
            palantir_type: Palantir data type definition

        Returns:
            Corresponding Spark DataType
        """
        type_name = palantir_type.get("type", "string")

        # Basic type mapping
        type_mapping = {
            "string": StringType(),
            "integer": LongType(),
            "long": LongType(),
            "double": DoubleType(),
            "float": DoubleType(),
            "boolean": BooleanType(),
            "timestamp": StringType(),  # Keep as ISO 8601 string
            "date": StringType(),  # Keep as ISO date string
            "datetime": StringType(),
            "attachment": StringType(),  # JSON representation
            "decimal": DoubleType(),  # Simplified for MVP
        }

        # Handle special/complex types
        if type_name == "geopoint":
            # Geopoint becomes struct with coordinates
            return StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ])

        if type_name == "array":
            # Recursive array type
            item_type = palantir_type.get("itemType", {"type": "string"})
            element_type = self._map_palantir_type_to_spark(item_type)
            return ArrayType(element_type, True)

        if type_name == "struct":
            # Recursive struct type
            sub_properties = palantir_type.get("properties", {})
            sub_fields = []
            for sub_prop_name, sub_prop_def in sub_properties.items():
                sub_data_type = sub_prop_def.get("dataType", {})
                sub_spark_type = self._map_palantir_type_to_spark(sub_data_type)
                sub_fields.append(StructField(sub_prop_name, sub_spark_type, True))
            return StructType(sub_fields)

        # Default to mapped type or string
        return type_mapping.get(type_name, StringType())

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Return metadata for the specified table.

        Args:
            table_name: Object type API name
            table_options: Options including optional 'cursor_field' for incremental sync

        Returns:
            Metadata dict with primary_keys, cursor_field (optional), and ingestion_type

        Raises:
            ValueError: If table_name is not supported
            Exception: If API request fails
        """
        # Create cache key based on table name and cursor field
        cursor_field = table_options.get("cursor_field")
        cache_key = f"{table_name}_{cursor_field or 'snapshot'}"

        # Check cache first
        if cache_key in self._metadata_cache:
            return self._metadata_cache[cache_key]

        # Validate table exists
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {self.list_tables()}"
            )

        # Fetch object type to get primary key information
        url = f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}/objectTypes/{table_name}"

        try:
            response = self._session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise Exception(
                f"Failed to get metadata for table '{table_name}': {str(e)}"
            )

        obj_type = response.json()
        primary_key = obj_type.get("primaryKey")

        # Determine primary key field(s)
        if isinstance(primary_key, str):
            pk_fields = [primary_key]
        elif isinstance(primary_key, list):
            pk_fields = primary_key
        else:
            pk_fields = ["__primaryKey"]

        # Build metadata based on whether cursor field is specified
        if cursor_field:
            # Incremental (CDC) mode
            metadata = {
                "primary_keys": pk_fields,
                "cursor_field": cursor_field,
                "ingestion_type": "cdc",
            }
        else:
            # Snapshot mode (full refresh)
            metadata = {
                "primary_keys": pk_fields,
                "ingestion_type": "snapshot",
            }

        # Cache the result
        self._metadata_cache[cache_key] = metadata
        return metadata

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: Dict[str, str],
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read data from the specified Palantir object type.

        Args:
            table_name: Object type API name
            start_offset: Starting offset for pagination (None for first call)
            table_options: Options including 'cursor_field' and 'page_size'

        Returns:
            Tuple of (iterator of records, end offset dict)

        Raises:
            ValueError: If table_name is not supported
            Exception: If API request fails
        """
        # Get metadata to determine ingestion mode
        metadata = self.read_table_metadata(table_name, table_options)
        cursor_field = metadata.get("cursor_field")

        if cursor_field is not None:
            return self._read_incremental(
                table_name, start_offset, table_options, cursor_field
            )
        else:
            return self._read_snapshot(table_name, start_offset, table_options)

    def _generate_all_pages(
        self, table_name: str, page_size: int,
        where_clause: dict = None,
    ) -> Iterator[dict]:
        """
        Generator that yields records page by page, keeping only one page
        in memory at a time. This avoids OOM when reading large datasets.
        """
        object_set = self._build_object_set(table_name, where_clause)
        page_token = None
        while True:
            records, next_page_token = self._fetch_page(
                object_set, page_token, page_size
            )
            yield from records

            if not next_page_token:
                break
            page_token = next_page_token

    def _read_snapshot(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read all data in snapshot mode using a streaming generator.

        Yields records page by page to avoid loading the entire dataset
        into memory. Only one page (~10K records) is held at a time.

        Args:
            table_name: Object type API name
            start_offset: Starting offset (unused for snapshot full refresh)
            table_options: Options including optional 'page_size'

        Returns:
            Tuple of (generator of all records, end offset dict)
        """
        page_size = int(table_options.get("page_size", "1000"))

        if start_offset:
            next_offset = start_offset
        else:
            next_offset = {"done": "true"}

        return self._generate_all_pages(table_name, page_size), next_offset

    def _get_max_cursor_value(
        self, table_name: str, cursor_field: str
    ) -> Any:
        """
        Get the maximum cursor field value. Tries the aggregate endpoint
        first (lightweight, no records fetched). Falls back to orderBy
        desc + limit 1 if aggregate fails (some object types don't
        support aggregation).
        """
        result = self._get_max_cursor_via_aggregate(table_name, cursor_field)
        if result is not None:
            return result

        return self._get_max_cursor_via_search(table_name, cursor_field)

    def _get_max_cursor_via_aggregate(
        self, table_name: str, cursor_field: str
    ) -> Any:
        """Get max cursor value using the aggregate endpoint."""
        url = (
            f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}"
            f"/objectSets/aggregate"
        )
        body = {
            "objectSet": {"type": "base", "objectType": table_name},
            "aggregation": [
                {"type": "max", "field": cursor_field, "name": "max_cursor"},
            ],
            "groupBy": [],
        }
        try:
            response = self._session.post(url, json=body, timeout=30)
            response.raise_for_status()
        except requests.RequestException:
            return None

        data = response.json()
        groups = data.get("data", [])
        if groups:
            for metric in groups[0].get("metrics", []):
                if metric.get("name") == "max_cursor":
                    return metric.get("value")
        return None

    def _get_max_cursor_via_search(
        self, table_name: str, cursor_field: str
    ) -> Any:
        """Fallback: get max cursor value using orderBy desc, limit 1."""
        url = (
            f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}"
            f"/objects/{table_name}/search"
        )
        body = {
            "orderBy": {
                "fields": [{"field": cursor_field, "direction": "desc"}]
            },
            "pageSize": 1,
        }
        try:
            response = self._session.post(url, json=body, timeout=30)
            response.raise_for_status()
        except requests.RequestException:
            return None

        data = response.json()
        records = data.get("data", [])
        if records:
            return records[0].get(cursor_field)
        return None

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: Dict[str, str],
        cursor_field: str,
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read data in incremental (CDC) mode using a generator with
        server-side filtering.

        Flow:
        1. Call the aggregate endpoint to get the current max cursor value
           (lightweight, no records fetched).
        2. If max hasn't changed since checkpoint → return empty (no new data).
        3. If this is an incremental run (prev checkpoint exists), use a
           server-side 'where: gt' filter so the API only returns records
           newer than the checkpoint — avoids full scan.
        4. If this is the first run (no checkpoint), fetch all records
           (no where clause).
        5. Yield records via generator, one page at a time.

        Works with both SCD_TYPE_1 and SCD_TYPE_2 — the SCD type is
        handled by the framework's apply_changes, not by this method.

        Args:
            table_name: Object type API name
            start_offset: Offset with optional 'max_cursor_value'
            table_options: Options including optional 'page_size'
            cursor_field: Field name to use for cursor tracking

        Returns:
            Tuple of (generator of records, end offset dict)
        """
        page_size = int(table_options.get("page_size", "1000"))
        prev_max_cursor = start_offset.get("max_cursor_value") if start_offset else None

        # Step 1: Get current max cursor via aggregate (lightweight).
        current_max_cursor = self._get_max_cursor_value(table_name, cursor_field)

        # Use whichever is greater: previous checkpoint or current max
        if prev_max_cursor and current_max_cursor:
            new_max_cursor = max(prev_max_cursor, current_max_cursor)
        else:
            new_max_cursor = current_max_cursor or prev_max_cursor

        # Step 2: Check if there's new data.
        final_offset = {"max_cursor_value": new_max_cursor}
        if start_offset and final_offset == start_offset:
            # No new data — return empty iterator and same offset to stop.
            return iter([]), start_offset
        next_offset = final_offset

        # Step 3: Build server-side where clause for incremental runs.
        # On first run (prev_max_cursor is None): no filter → full load.
        # On subsequent runs: gt filter → only new/updated records from API.
        where_clause = None
        if prev_max_cursor is not None:
            where_clause = {
                "type": "gt",
                "field": cursor_field,
                "value": prev_max_cursor,
            }

        # Step 4: Generator that yields records page by page.
        # Records with null cursor_field are excluded (not yielded).
        def _filtered_pages() -> Iterator[dict]:
            for record in self._generate_all_pages(
                table_name, page_size, where_clause=where_clause
            ):
                # Skip records with null cursor (server-side gt filter
                # already excludes them on incremental runs, but on
                # the first full load we need this client-side check).
                if record.get(cursor_field) is not None:
                    yield record

        return _filtered_pages(), next_offset

    def _build_object_set(
        self, table_name: str, where_clause: dict = None
    ) -> dict:
        """
        Build an objectSet definition for the loadObjects endpoint.

        Args:
            table_name: Object type API name
            where_clause: Optional filter (e.g.,
                {"type": "gt", "field": "cursor", "value": "2024-01-01"})

        Returns:
            objectSet dict for use in loadObjects request body
        """
        base = {"type": "base", "objectType": table_name}
        if where_clause:
            return {
                "type": "filter",
                "objectSet": base,
                "where": where_clause,
            }
        return base

    def _fetch_page(
        self,
        object_set: dict,
        page_token: str = None,
        page_size: int = 1000,
    ) -> Tuple[List[dict], str]:
        """
        Fetch a single page of data from Palantir API using the
        loadObjectSet endpoint with snapshot=true for consistent
        pagination across large datasets.

        Uses exponential backoff on transient errors (429, 503).

        Args:
            object_set: objectSet definition (from _build_object_set)
            page_token: Optional pagination token from previous call
            page_size: Number of records to fetch (default 1000, max 10000)

        Returns:
            Tuple of (list of records, next page token or None)

        Raises:
            Exception: If API request fails after retries
        """
        url = (
            f"{self.base_url}/api/v2/ontologies/{self.ontology_api_name}"
            f"/objectSets/loadObjects?snapshot=true"
        )

        body = {
            "objectSet": object_set,
            "pageSize": min(page_size, 10000),
        }
        if page_token:
            body["pageToken"] = page_token

        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = self._session.post(url, json=body, timeout=120)
                if response.status_code in (429, 503):
                    wait = 2 ** attempt
                    time.sleep(wait)
                    continue
                response.raise_for_status()
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise Exception(
                    f"Failed to fetch data after {max_retries} retries: {str(e)}"
                )

            data = response.json()
            records = data.get("data", [])
            next_page_token = data.get("nextPageToken")

            # Small delay for rate limiting
            time.sleep(0.1)

            return records, next_page_token

        raise Exception(
            f"Failed to fetch data after {max_retries} retries: "
            f"last status {response.status_code}"
        )
