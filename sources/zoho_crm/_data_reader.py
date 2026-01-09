"""
Data reader for Zoho CRM connector.

This module handles reading data from Zoho CRM APIs and transforming
it for ingestion into Delta Lake. It provides:
- Reading records from standard CRM modules
- Reading data from derived tables (settings, subforms, junctions)
- Incremental data loading using cursor fields (CDC)
- Pagination handling for large datasets
- Deleted records tracking for CDC

The DataReader works with the AuthClient for API access, MetadataManager
for field information, and SchemaGenerator for data transformation.

Example:
    >>> reader = DataReader(auth_client, metadata_manager, schema_gen, derived_tables, None)
    >>> for record in reader.read("Leads", {"cursor_position": "2024-01-01T00:00:00Z"}):
    ...     print(record)
"""

import json
import logging
from datetime import datetime
from typing import Iterator, Optional
from itertools import chain

from ._auth_client import AuthClient
from ._metadata_manager import MetadataManager
from ._schema_generator import SchemaGenerator


# Configure logging for debug output instead of print statements
logger = logging.getLogger(__name__)


class DataReader:
    """
    Reads and transforms data from Zoho CRM for ingestion.

    This class handles the complexity of reading data from various
    Zoho CRM endpoints and normalizing it for Delta Lake ingestion:

    - **Standard Modules**: Uses /crm/v8/{module}/search for CDC reads
      and /crm/v8/{module} for full reads. Handles pagination using
      the more_records flag and page parameter.

    - **Derived Tables**: Routes to specialized readers for each type:
      - Settings: /crm/v8/settings/users, /crm/v8/settings/roles, etc.
      - Subforms: Extracts embedded line-item data from parent records
      - Junctions: Uses /crm/v8/{module}/{id}/{related} API

    - **CDC (Change Data Capture)**: Uses Modified_Time field to fetch
      only records changed since the last sync.

    - **Deleted Records**: Fetches deleted record IDs for soft delete
      handling in the destination.

    Attributes:
        PAGE_SIZE: Number of records to fetch per API call (max 200).
    """

    # Zoho CRM API maximum page size
    PAGE_SIZE = 200

    def __init__(
        self,
        auth_client: AuthClient,
        metadata_manager: MetadataManager,
        schema_generator: SchemaGenerator,
        derived_tables: dict,
        initial_load_start_date: Optional[datetime],
    ) -> None:
        """
        Initialize the DataReader.

        Args:
            auth_client: Authenticated client for API calls.
            metadata_manager: Manager for field/module metadata.
            schema_generator: Generator for schema information.
            derived_tables: Dictionary of derived table definitions.
            initial_load_start_date: Optional start date for initial loads.
                If specified, only records modified after this date are
                fetched on initial sync.
        """
        self._auth_client = auth_client
        self._metadata_manager = metadata_manager
        self._schema_generator = schema_generator
        self.DERIVED_TABLES = derived_tables
        self._initial_load_start_date = initial_load_start_date

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records and deleted records from a table.

        This is the main interface method called by LakeflowConnect. It:
        1. Reads new/modified records from the table
        2. Reads deleted records for CDC
        3. Combines both into a single iterator
        4. Returns an empty offset dict (cursor management handled upstream)

        Args:
            table_name: Name of the table to read from.
            start_offset: Offset dictionary containing:
                - cursor_position: ISO timestamp for incremental reads
            table_options: Additional table-specific options.

        Returns:
            Tuple of (record_iterator, offset_dict) where:
            - record_iterator: Iterator yielding all records (data + deletes)
            - offset_dict: Empty dict (cursor position managed by pipeline)

        Example:
            >>> records_iter, offset = reader.read_table(
            ...     "Leads",
            ...     {"cursor_position": "2024-06-15T10:30:00Z"},
            ...     {}
            ... )
            >>> for record in records_iter:
            ...     process(record)
        """
        # Merge start_offset into options for the internal readers
        options = {**table_options, **start_offset}

        # Read data records and deleted records
        data_records = self.read(table_name, options)
        deleted_records = self.read_deletes(table_name, options)

        # Combine both iterators efficiently without loading into memory
        combined_records = chain(data_records, deleted_records)

        # Return empty offset - cursor management is handled by the pipeline
        return combined_records, {}

    def read(self, table_name: str, options: dict[str, str]) -> Iterator[dict]:
        """
        Read data from the specified table.

        This is the internal entry point for data reading. It determines the
        table type and routes to the appropriate reading strategy.

        Args:
            table_name: Name of the table to read from.
            options: Reading options including:
                - cursor_position: ISO timestamp for incremental reads
                - Additional table-specific options

        Yields:
            Dict records ready for ingestion.

        Example:
            >>> # Full read (no cursor)
            >>> records = list(reader.read("Leads", {}))

            >>> # Incremental read
            >>> records = list(reader.read("Leads", {
            ...     "cursor_position": "2024-06-15T10:30:00Z"
            ... }))
        """
        # Route to appropriate reader based on table type
        if table_name in self.DERIVED_TABLES:
            yield from self._read_derived_table(table_name, options)
        else:
            yield from self._read_module(table_name, options)

    def read_deletes(self, table_name: str, options: dict[str, str]) -> Iterator[dict]:
        """
        Read deleted records from the specified table.

        Zoho CRM provides a deleted records API that returns IDs of
        records deleted within a time range. This is used for CDC to
        handle deletions in the destination.

        Args:
            table_name: Name of the module to check for deletes.
            options: Reading options including:
                - cursor_position: ISO timestamp - fetch deletes after this time

        Yields:
            Dict containing deleted record information:
            - id: The deleted record's ID
            - deleted_time: When the record was deleted

        Note:
            - Only standard CRM modules support the deleted records API
            - Derived tables return empty iterator (deletions handled differently)
            - Maximum retention period for deleted records is 60 days
        """
        # Derived tables don't have a separate deleted records API
        if table_name in self.DERIVED_TABLES:
            return

        cursor_position = options.get("cursor_position")
        if not cursor_position:
            # No cursor means full load - don't process deletes
            return

        # Fetch deleted records using the dedicated API
        params = {
            "type": "recycle",  # "recycle" = soft delete, "permanent" = hard delete
            "per_page": self.PAGE_SIZE,
        }

        # Add time filter if cursor is available
        # The API uses different parameter name: 'from_time' instead of 'from'
        params["from_time"] = cursor_position

        page = 1
        while True:
            params["page"] = page
            endpoint = f"/crm/v8/{table_name}/deleted"

            try:
                response = self._auth_client.make_request("GET", endpoint, params=params)
            except Exception as e:
                logger.warning(f"Failed to fetch deleted records for {table_name}: {e}")
                return

            deleted_records = response.get("data", [])

            for record in deleted_records:
                yield {
                    "id": record.get("id"),
                    "deleted_time": record.get("deleted_time"),
                }

            # Check for more pages
            info = response.get("info", {})
            if not info.get("more_records", False):
                break
            page += 1

    def _read_module(self, module_name: str, options: dict[str, str]) -> Iterator[dict]:
        """
        Read records from a standard CRM module.

        Uses the appropriate API based on whether this is a CDC read:
        - CDC read: Uses /search endpoint with Modified_Time criteria
        - Full read: Uses standard /module endpoint

        Args:
            module_name: The API name of the CRM module.
            options: Reading options including cursor_position.

        Yields:
            Normalized record dictionaries.
        """
        cursor_position = options.get("cursor_position")

        # Determine start time for the read
        # Priority: cursor_position > initial_load_start_date > full load
        if cursor_position:
            start_time = cursor_position
        elif self._initial_load_start_date:
            start_time = self._initial_load_start_date.isoformat()
        else:
            start_time = None

        yield from self._read_records(module_name, start_time)

    def _read_records(
        self,
        module_name: str,
        start_time: Optional[str],
    ) -> Iterator[dict]:
        """
        Read records from a module with optional time-based filtering.

        This method handles:
        - Pagination through large result sets
        - Field normalization (lookups, multiselect, JSON fields)
        - CDC filtering using Modified_Time

        Args:
            module_name: The API name of the CRM module.
            start_time: Optional ISO timestamp for CDC reads.

        Yields:
            Normalized record dictionaries with:
            - Lookup fields converted to ID strings
            - Multiselect values as JSON arrays
            - Complex fields serialized to JSON
        """
        # Get field metadata for normalization
        json_fields = self._get_json_fields(module_name)
        all_field_names = [f.get("api_name") for f in self._metadata_manager.get_fields(module_name)]

        page = 1
        while True:
            # Build request based on whether we have a time filter
            if start_time:
                # Use search endpoint for time-based filtering
                # COQL (Criteria Query Language) syntax
                endpoint = f"/crm/v8/{module_name}/search"
                params = {
                    "criteria": f"Modified_Time:greater_equal:{start_time}",
                    "per_page": self.PAGE_SIZE,
                    "page": page,
                }
            else:
                # Full read - use standard endpoint
                endpoint = f"/crm/v8/{module_name}"
                params = {
                    "per_page": self.PAGE_SIZE,
                    "page": page,
                }

            response = self._auth_client.make_request("GET", endpoint, params=params)
            records = response.get("data", [])

            for record in records:
                yield self._normalize_record(record, json_fields, all_field_names)

            # Check for more pages
            info = response.get("info", {})
            if not info.get("more_records", False):
                break
            page += 1

    def _get_json_fields(self, module_name: str) -> set[str]:
        """
        Get the set of fields that should be serialized as JSON.

        Certain Zoho CRM field types contain complex nested data that
        needs to be serialized to JSON strings for storage in Delta Lake.

        Args:
            module_name: The API name of the CRM module.

        Returns:
            Set of field API names that should be JSON-serialized.
        """
        fields = self._metadata_manager.get_fields(module_name)
        json_types = {"jsonobject", "jsonarray"}

        return {
            field.get("api_name")
            for field in fields
            if field.get("json_type") in json_types
        }

    def _normalize_record(
        self,
        record: dict,
        json_fields: set[str],
        all_field_names: list[str],
    ) -> dict:
        """
        Normalize a Zoho CRM record for Delta Lake ingestion.

        Transformations applied:
        1. Lookup fields: Extract ID from {id, name} object
        2. Multi-value fields: Convert to JSON array string
        3. JSON fields: Serialize complex objects to JSON strings
        4. Owner field: Extract ID from owner object

        Args:
            record: Raw record from Zoho CRM API.
            json_fields: Set of field names requiring JSON serialization.
            all_field_names: List of all valid field names for the module.

        Returns:
            Normalized record dictionary.
        """
        normalized = {}

        for key, value in record.items():
            # Skip fields not in schema (system fields, etc.)
            if key not in all_field_names and key != "id":
                continue

            if value is None:
                normalized[key] = None

            # Handle lookup fields (object with id and name)
            elif isinstance(value, dict) and "id" in value:
                normalized[key] = str(value["id"])

            # Handle multi-value fields (list of objects or primitives)
            elif isinstance(value, list):
                # If list of objects with IDs, extract IDs
                if value and isinstance(value[0], dict) and "id" in value[0]:
                    normalized[key] = json.dumps([str(item["id"]) for item in value])
                else:
                    normalized[key] = json.dumps(value)

            # Handle JSON fields
            elif key in json_fields:
                normalized[key] = json.dumps(value) if value else None

            else:
                normalized[key] = value

        return normalized

    def _read_derived_table(
        self,
        table_name: str,
        options: dict[str, str],
    ) -> Iterator[dict]:
        """
        Read data from a connector-defined derived table.

        Routes to the appropriate reader based on derived table type:
        - settings: Users, Roles, Profiles from Settings API
        - subform: Line items embedded in parent records
        - related: Junction tables from Related Records API

        Args:
            table_name: Name of the derived table.
            options: Reading options.

        Yields:
            Record dictionaries for the derived table.
        """
        config = self.DERIVED_TABLES[table_name]
        table_type = config["type"]

        if table_type == "settings":
            yield from self._read_settings_table(table_name, options)
        elif table_type == "subform":
            yield from self._read_subform_table(table_name, config, options)
        elif table_type == "related":
            yield from self._read_related_table(table_name, config, options)

    def _read_settings_table(
        self,
        table_name: str,
        options: dict[str, str],
    ) -> Iterator[dict]:
        """
        Read data from a settings-type derived table.

        Settings tables include Users, Roles, and Profiles. These are
        accessed via the Settings API rather than the standard CRM API.

        Args:
            table_name: Name of the settings table.
            options: Reading options (cursor_position for CDC on Users).

        Yields:
            Normalized settings records.

        Note:
            Only the Users table supports CDC via Modified_Time filtering.
            Roles and Profiles always do a full read.
        """
        # Map table name to settings endpoint
        endpoint_map = {
            "Users": "/crm/v8/settings/users",
            "Roles": "/crm/v8/settings/roles",
            "Profiles": "/crm/v8/settings/profiles",
        }

        endpoint = endpoint_map.get(table_name)
        if not endpoint:
            logger.warning(f"Unknown settings table: {table_name}")
            return

        params = {"per_page": self.PAGE_SIZE}

        # Users table supports CDC
        cursor_position = options.get("cursor_position")
        if table_name == "Users" and cursor_position:
            params["modified_since"] = cursor_position

        page = 1
        while True:
            params["page"] = page
            response = self._auth_client.make_request("GET", endpoint, params=params)

            # Settings API uses different response keys
            data_key = table_name.lower()  # "users", "roles", "profiles"
            records = response.get(data_key, [])

            for record in records:
                # Normalize lookup fields in settings records
                normalized = {}
                for key, value in record.items():
                    if isinstance(value, dict) and "id" in value:
                        normalized[key] = str(value["id"])
                    else:
                        normalized[key] = value
                yield normalized

            # Check for more pages
            info = response.get("info", {})
            if not info.get("more_records", False):
                break
            page += 1

    def _read_subform_table(
        self,
        table_name: str,
        config: dict,
        options: dict[str, str],
    ) -> Iterator[dict]:
        """
        Read data from a subform-type derived table.

        Subforms are line-item data embedded within parent records
        (e.g., Quote_Line_Items within Quotes). To read them:
        1. Fetch parent records from the parent module
        2. Extract subform data from each parent record
        3. Add parent reference (_parent_id) to each subform record

        Args:
            table_name: Name of the subform table.
            config: Derived table configuration:
                - parent_module: Parent module API name
                - subform_field: Subform field name in parent
            options: Reading options.

        Yields:
            Subform records with _parent_id added.

        Note:
            Subform data doesn't have its own Modified_Time, so CDC
            is based on the parent record's Modified_Time.
        """
        parent_module = config["parent_module"]
        subform_field = config["subform_field"]

        cursor_position = options.get("cursor_position")
        start_time = cursor_position if cursor_position else None

        # Read parent records and extract subform data
        for parent_record in self._read_records(parent_module, start_time):
            parent_id = parent_record.get("id")

            # Get subform data from the parent record
            # Note: We need to fetch the full record to get subform data
            # The search endpoint may not include subform fields
            try:
                full_record_response = self._auth_client.make_request(
                    "GET", f"/crm/v8/{parent_module}/{parent_id}"
                )
                full_record = full_record_response.get("data", [{}])[0]
            except Exception as e:
                logger.warning(f"Failed to fetch subform data for {parent_module}/{parent_id}: {e}")
                continue

            subform_data = full_record.get(subform_field, [])
            if not subform_data:
                continue

            for item in subform_data:
                # Add parent reference
                item["_parent_id"] = parent_id
                yield item

    def _read_related_table(
        self,
        table_name: str,
        config: dict,
        options: dict[str, str],
    ) -> Iterator[dict]:
        """
        Read data from a junction/related-type derived table.

        Junction tables represent many-to-many relationships between
        modules (e.g., Contacts linked to Deals). The data is accessed
        via the Related Records API.

        Args:
            table_name: Name of the junction table.
            config: Derived table configuration:
                - parent_module: Primary module API name
                - related_module: Related module API name
            options: Reading options.

        Yields:
            Junction records with:
            - _junction_id: Composite key (parent_id + related_id)
            - {parent_module}_id: Parent record ID
            - {related_module}_id: Related record ID

        Note:
            Junction tables always do a full read because there's no
            Modified_Time on the relationship itself.
        """
        parent_module = config["parent_module"]
        related_module = config["related_module"]

        # First, get all parent records to iterate through
        for parent_record in self._read_records(parent_module, None):
            parent_id = parent_record.get("id")

            # Fetch related records for this parent
            endpoint = f"/crm/v8/{parent_module}/{parent_id}/{related_module}"
            params = {"per_page": self.PAGE_SIZE}

            page = 1
            while True:
                params["page"] = page
                try:
                    response = self._auth_client.make_request("GET", endpoint, params=params)
                except Exception as e:
                    logger.warning(f"Failed to fetch related records for {parent_module}/{parent_id}: {e}")
                    break

                related_records = response.get("data", [])

                for related_record in related_records:
                    related_id = related_record.get("id")

                    # Create junction record with composite key
                    yield {
                        "_junction_id": f"{parent_id}_{related_id}",
                        f"{parent_module}_id": parent_id,
                        f"{related_module}_id": related_id,
                    }

                # Check for more pages
                info = response.get("info", {})
                if not info.get("more_records", False):
                    break
                page += 1
