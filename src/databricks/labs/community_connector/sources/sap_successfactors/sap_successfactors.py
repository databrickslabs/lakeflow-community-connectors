# SAP SuccessFactors Connector for Lakeflow Connect.
#
# This connector implements the LakeflowConnect interface to ingest data from
# SAP SuccessFactors using OData v2 APIs with Basic Authentication.
#
# Authentication:
#     - Username format: username@companyId (e.g., "sfadmin@SFPART067615")
#     - Basic Auth with base64 encoding
#
# OData v2 Features:
#     - Handles __next continuation links for pagination
#     - Handles $top/$skip offset pagination as fallback
#     - Parses SAP DateTime format: /Date(milliseconds)/
#
# Metadata Modes:
#     - "static": Uses hardcoded TABLE_CONFIG and TABLE_SCHEMAS (default)
#     - "dynamic": Fetches tables and schemas from OData $metadata API at runtime

import base64
import re
import time
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Tuple, Any, Optional
import requests
from pyspark.sql.types import StructType
from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.sap_successfactors.table_schemas import (
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.sap_successfactors.table_metadata import (
    TABLE_CONFIG,
    DEDUPE_TABLES,
)
from databricks.labs.community_connector.sources.sap_successfactors.odata_metadata import (
    fetch_odata_metadata,
    build_schema_from_metadata,
    build_table_config_from_metadata,
)


class SapSuccessFactorsLakeflowConnect(LakeflowConnect):
    """
    SAP SuccessFactors connector implementing the LakeflowConnect interface.

    This connector supports:
    - Basic Authentication with username@companyId format
    - OData v2 API with full pagination support
    - Both CDC (incremental) and snapshot ingestion types
    - SAP DateTime format parsing
    - Two metadata modes:
        - "static": Uses hardcoded TABLE_CONFIG and TABLE_SCHEMAS (default)
        - "dynamic": Fetches tables and schemas from OData $metadata API at runtime
    """

    # Default page size for OData queries
    DEFAULT_PAGE_SIZE = 100

    # Maximum retries for API requests
    MAX_RETRIES = 3

    # Delay between retries (seconds)
    RETRY_DELAY = 1.0

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the SAP SuccessFactors connector.

        Args:
            options: Dictionary containing connection parameters:
                - endpoint_url: Base URL for SuccessFactors API
                    (e.g., "https://apisalesdemo8.successfactors.com/")
                - username: Username in format username@companyId
                    (e.g., "sfadmin@SFPART067615")
                - password: User password
                - metadata_mode: "static" (default) or "dynamic"
                    - static: Uses hardcoded table definitions
                    - dynamic: Fetches from OData $metadata API
        """
        self.endpoint_url = options["endpoint_url"].rstrip("/")
        self.username = options["username"]
        self.password = options["password"]
        self.metadata_mode = options.get("metadata_mode", "static").lower()

        if self.metadata_mode not in ("static", "dynamic"):
            raise ValueError(
                f"Invalid metadata_mode: '{self.metadata_mode}'. Must be 'static' or 'dynamic'."
            )

        # Build the OData API base URL
        self.base_url = f"{self.endpoint_url}/odata/v2"

        # Create Basic Auth header
        auth_string = f"{self.username}:{self.password}"
        auth_bytes = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

        self.headers = {
            "Authorization": f"Basic {auth_bytes}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Cache for dynamically fetched metadata (populated lazily)
        self._metadata_cache: Optional[Dict[str, Any]] = None

    def list_tables(self) -> List[str]:
        """
        List all available tables supported by this connector.

        In static mode, returns table names from the hardcoded TABLE_CONFIG.
        In dynamic mode, fetches entity sets from the OData $metadata API.

        Returns:
            List of table names
        """
        if self.metadata_mode == "dynamic":
            metadata = self._get_metadata()
            return list(metadata["entity_sets"].keys())
        return list(TABLE_CONFIG.keys())

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the Spark schema for a table.

        In static mode, returns the schema from the hardcoded TABLE_SCHEMAS.
        In dynamic mode, builds the schema from the OData $metadata API.

        Args:
            table_name: Name of the table
            table_options: Additional options (not used currently)

        Returns:
            StructType representing the table schema

        Raises:
            ValueError: If table is not supported
        """
        if self.metadata_mode == "dynamic":
            metadata = self._get_metadata()
            entity_sets = metadata["entity_sets"]
            if table_name not in entity_sets:
                supported = list(entity_sets.keys())
                raise ValueError(f"Unsupported table: {table_name}. Supported tables: {supported}")
            entity_info = entity_sets[table_name]
            return build_schema_from_metadata(entity_info)

        if table_name not in TABLE_SCHEMAS:
            supported = list(TABLE_SCHEMAS.keys())
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables: {supported}"
            )
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Get metadata for a table including primary keys, cursor field, and ingestion type.

        In static mode, returns metadata from the hardcoded TABLE_CONFIG.
        In dynamic mode, derives metadata from the OData $metadata API:
            - primary_keys: Extracted from EntityType Key/PropertyRef elements
            - cursor_field: Auto-detected if lastModifiedDateTime (or similar) exists
            - ingestion_type: "cdc" if a cursor field is detected, otherwise "snapshot"

        Args:
            table_name: Name of the table
            table_options: Additional options (not used currently)

        Returns:
            Dictionary with:
                - primary_keys: List of primary key column names
                - cursor_field: Column to use for incremental reads
                - ingestion_type: "cdc" or "snapshot"

        Raises:
            ValueError: If table is not supported
        """
        if self.metadata_mode == "dynamic":
            metadata = self._get_metadata()
            entity_sets = metadata["entity_sets"]
            if table_name not in entity_sets:
                supported = list(entity_sets.keys())
                raise ValueError(f"Unsupported table: {table_name}. Supported tables: {supported}")
            entity_info = entity_sets[table_name]
            return build_table_config_from_metadata(entity_info)

        if table_name not in TABLE_CONFIG:
            supported = list(TABLE_CONFIG.keys())
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables: {supported}"
            )

        config = TABLE_CONFIG[table_name]
        return {
            "primary_keys": config["primary_keys"],
            "cursor_field": config.get("cursor_field"),
            "ingestion_type": config.get("ingestion_type", "snapshot"),
        }

    def read_table(
        self, table_name: str, start_offset: Dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read data from a SAP SuccessFactors table.

        Supports both CDC (incremental) and snapshot reads:
        - CDC: Uses cursor_field with $filter for incremental reads
        - Snapshot: Full table read each time

        Args:
            table_name: Name of the table to read
            start_offset: Offset from previous read for incremental sync
                - For CDC: {"cursor_value": "<datetime_string>"}
                - For snapshot: {} or None
            table_options: Additional options (not used currently)

        Returns:
            Tuple of (records_iterator, new_offset)
            - records_iterator: Iterator yielding record dicts
            - new_offset: Offset for next incremental read

        Raises:
            ValueError: If table is not supported
        """
        config = self._resolve_table_config(table_name)
        ingestion_type = config.get("ingestion_type", "snapshot")

        if ingestion_type == "cdc":
            return self._read_table_cdc(table_name, config, start_offset)
        else:
            return self._read_table_snapshot(table_name, config)

    def _resolve_table_config(self, table_name: str) -> Dict[str, Any]:
        """
        Resolve the table configuration, using either static or dynamic metadata.

        In static mode, looks up TABLE_CONFIG.
        In dynamic mode, builds configuration from the OData $metadata API.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with entity_set, primary_keys, cursor_field, ingestion_type

        Raises:
            ValueError: If table is not supported
        """
        if self.metadata_mode == "dynamic":
            metadata = self._get_metadata()
            entity_sets = metadata["entity_sets"]
            if table_name not in entity_sets:
                supported = list(entity_sets.keys())
                raise ValueError(f"Unsupported table: {table_name}. Supported tables: {supported}")
            entity_info = entity_sets[table_name]
            table_meta = build_table_config_from_metadata(entity_info)
            # Add entity_set which is the EntitySet name (same as table_name in dynamic mode)
            table_meta["entity_set"] = entity_info["entity_set_name"]
            return table_meta

        if table_name not in TABLE_CONFIG:
            supported = list(TABLE_CONFIG.keys())
            raise ValueError(f"Unsupported table: {table_name}. Supported tables: {supported}")
        return TABLE_CONFIG[table_name]

    def _read_table_cdc(
        self, table_name: str, config: Dict[str, Any], start_offset: Dict
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read table data incrementally using CDC (Change Data Capture).

        Args:
            table_name: Name of the table
            config: Table configuration from TABLE_CONFIG
            start_offset: Previous offset with cursor_value

        Returns:
            Tuple of (records_iterator, new_offset)
        """
        entity_set = config["entity_set"]
        cursor_field = config["cursor_field"]
        select_fields = config.get("select_fields")

        # Build the base URL
        url = f"{self.base_url}/{entity_set}"

        # Build query parameters
        params = {
            "$format": "json",
            "$top": str(self.DEFAULT_PAGE_SIZE),
        }

        # Add $select if specified
        if select_fields:
            params["$select"] = ",".join(select_fields)

        # Add $orderby for consistent ordering
        params["$orderby"] = f"{cursor_field} asc"

        # Add $filter for incremental read if we have a previous cursor
        if start_offset and start_offset.get("cursor_value"):
            cursor_value = start_offset["cursor_value"]
            # Convert to SAP datetime format for filter
            params["$filter"] = f"{cursor_field} gt datetime'{cursor_value}'"

        # Fetch all pages of data
        all_records, max_cursor = self._fetch_all_pages(url, params, cursor_field)

        # Build new offset
        if max_cursor:
            new_offset = {"cursor_value": max_cursor}
        elif start_offset:
            new_offset = start_offset
        else:
            new_offset = {}

        return iter(all_records), new_offset

    def _read_table_snapshot(
        self, table_name: str, config: Dict[str, Any]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read full table data (snapshot mode).

        Args:
            table_name: Name of the table
            config: Table configuration from TABLE_CONFIG

        Returns:
            Tuple of (records_iterator, empty_offset)
        """
        entity_set = config["entity_set"]
        select_fields = config.get("select_fields")
        cursor_field = config.get("cursor_field")

        # Build the base URL
        url = f"{self.base_url}/{entity_set}"

        # Build query parameters
        params = {
            "$format": "json",
            "$top": str(self.DEFAULT_PAGE_SIZE),
        }

        # Add $select if specified
        if select_fields:
            params["$select"] = ",".join(select_fields)

        # Add $orderby if cursor field exists for consistent ordering
        if cursor_field:
            params["$orderby"] = f"{cursor_field} asc"

        # Fetch all pages of data
        all_records, max_cursor = self._fetch_all_pages(url, params, cursor_field)

        # De-duplicate records for tables known to have identical duplicates from SAP API
        if table_name in DEDUPE_TABLES:
            all_records = self._deduplicate_records(all_records)

        # For snapshot, we can optionally track the max cursor for future CDC
        if max_cursor:
            new_offset = {"cursor_value": max_cursor}
        else:
            new_offset = {}

        return iter(all_records), new_offset

    def _deduplicate_records(self, records: List[Dict]) -> List[Dict]:
        """
        Remove duplicate records based on full row content.

        SAP SuccessFactors API sometimes returns identical duplicate rows
        for certain entities. This method filters them out.

        Args:
            records: List of record dictionaries

        Returns:
            List of unique records (preserving order)
        """
        seen = set()
        unique_records = []

        for record in records:
            # Create a hashable key from all field values
            # Sort keys for consistent ordering, convert values to strings for hashability
            record_key = tuple(
                (k, str(v) if v is not None else None)
                for k, v in sorted(record.items())
            )

            if record_key not in seen:
                seen.add(record_key)
                unique_records.append(record)

        return unique_records

    # pylint: disable=too-many-locals
    def _fetch_all_pages(
        self, base_url: str, params: Dict[str, str], cursor_field: Optional[str] = None
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Fetch all pages of data from an OData endpoint.

        CRITICAL: This method handles complete pagination to ensure ALL data is fetched.
        It supports both:
        1. __next continuation links (preferred)
        2. $top/$skip offset pagination (fallback)

        Args:
            base_url: Base OData endpoint URL
            params: Query parameters
            cursor_field: Optional field to track for max cursor value

        Returns:
            Tuple of (all_records, max_cursor_value)
        """
        all_records = []
        max_cursor = None
        skip = 0
        next_url = None

        # Build initial URL with params
        current_url = base_url
        current_params = params.copy()

        while True:
            # Make the request
            response = self._make_request(current_url, current_params if not next_url else None)

            if response is None:
                break

            # Parse response
            data = response.json()

            # Extract records from response
            # OData v2 format: {"d": {"results": [...]}}
            # or {"d": {"results": [...], "__next": "..."}}
            d = data.get("d", {})

            if isinstance(d, dict):
                results = d.get("results", [])
                next_link = d.get("__next")
            else:
                # Sometimes "d" is directly the results array
                results = d if isinstance(d, list) else []
                next_link = None

            if not results:
                break

            # Process each record
            for record in results:
                # Parse SAP DateTime fields
                processed_record = self._process_record(record)
                all_records.append(processed_record)

                # Track max cursor value if cursor_field is specified
                if cursor_field and cursor_field in processed_record:
                    cursor_value = processed_record[cursor_field]
                    if cursor_value:
                        if max_cursor is None or cursor_value > max_cursor:
                            max_cursor = cursor_value

            # Check for __next continuation link (OData standard pagination)
            if next_link:
                next_url = next_link
                current_url = next_url
                current_params = None  # __next URL contains all params
            else:
                # Fallback to $skip pagination if we got a full page
                if len(results) >= self.DEFAULT_PAGE_SIZE:
                    skip += self.DEFAULT_PAGE_SIZE
                    current_params = params.copy()
                    current_params["$skip"] = str(skip)
                    current_url = base_url
                    next_url = None
                else:
                    # Less than a full page means we've reached the end
                    break

            # Small delay to be nice to the API
            time.sleep(0.05)

        return all_records, max_cursor

    def _make_request(
        self, url: str, params: Optional[Dict[str, str]] = None
    ) -> Optional[requests.Response]:
        """
        Make an HTTP request with retry logic.

        Args:
            url: Request URL
            params: Optional query parameters

        Returns:
            Response object or None if all retries failed
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, timeout=60)

                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    # Rate limited - wait and retry
                    retry_after = int(response.headers.get("Retry-After", 5))
                    time.sleep(retry_after)
                    continue
                elif response.status_code >= 500:
                    # Server error - retry
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                    continue
                else:
                    # Client error - don't retry
                    raise Exception(
                        f"SAP SuccessFactors API error: {response.status_code} - {response.text}"
                    )
            except requests.exceptions.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                    continue
                raise Exception(f"Request failed after {self.MAX_RETRIES} retries: {e}")

        return None

    def _process_record(self, record: Dict) -> Dict:
        """
        Process a record from the API response.

        - Parses SAP DateTime format: /Date(milliseconds)/
        - Removes OData metadata fields (__metadata, __deferred, etc.)

        Args:
            record: Raw record from API

        Returns:
            Processed record with parsed dates and cleaned fields
        """
        processed = {}

        for key, value in record.items():
            # Skip OData metadata fields
            if key.startswith("__"):
                continue

            # Parse SAP DateTime format
            if isinstance(value, str) and value.startswith("/Date("):
                processed[key] = self._parse_sap_datetime(value)
            elif isinstance(value, dict):
                # Handle nested objects - check for __deferred (navigation property)
                if "__deferred" in value:
                    # Skip deferred navigation properties
                    continue
                if "__metadata" in value:
                    # Process nested entity, removing metadata
                    processed[key] = self._process_record(value)
                else:
                    processed[key] = value
            else:
                processed[key] = value

        return processed

    def _parse_sap_datetime(self, sap_datetime: str) -> str:
        """
        Parse SAP DateTime format to ISO 8601 string.

        SAP format: /Date(milliseconds)/ or /Date(milliseconds+offset)/
        Examples:
            /Date(1234567890000)/
            /Date(1234567890000+0000)/
            /Date(-1234567890000)/

        Args:
            sap_datetime: SAP DateTime string

        Returns:
            ISO 8601 formatted datetime string
        """
        # Extract milliseconds from /Date(milliseconds)/ or /Date(milliseconds+offset)/
        match = re.match(r"/Date\((-?\d+)([+-]\d{4})?\)/", sap_datetime)

        if not match:
            return sap_datetime  # Return original if format doesn't match

        milliseconds = int(match.group(1))

        # Convert milliseconds to seconds
        seconds = milliseconds / 1000.0

        try:
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, OSError):
            # Handle dates outside valid range
            return sap_datetime

    # ----------------------------------------------------------------
    # Dynamic metadata support (OData $metadata API)
    # ----------------------------------------------------------------

    def _get_metadata(self) -> Dict[str, Any]:
        """
        Get the parsed OData metadata, fetching and caching it on first call.

        Delegates to :func:`odata_metadata.fetch_odata_metadata` for the
        actual HTTP request and EDMX parsing.

        Returns:
            Parsed metadata dictionary (see :func:`odata_metadata.parse_edmx`).
        """
        if self._metadata_cache is None:
            self._metadata_cache = fetch_odata_metadata(self.session, self.base_url, self.headers)
        return self._metadata_cache

    def test_connection(self) -> Dict[str, str]:
        """
        Test the connection to SAP SuccessFactors API.

        Returns:
            Dictionary with status and message
        """
        try:
            # Try to access the service root
            url = f"{self.base_url}/"
            response = self.session.get(url, timeout=30)

            if response.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API returned status {response.status_code}: {response.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
