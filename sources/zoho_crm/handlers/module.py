"""
Handler for standard Zoho CRM modules.

Handles standard CRM modules like Leads, Contacts, Accounts, Deals, etc.
These modules support the standard Records API with CDC via Modified_Time.
"""

import logging
from datetime import datetime, timedelta
from typing import Iterator, Optional

from pyspark.sql.types import StructType, StructField, LongType

from .base import TableHandler
from ..zoho_types import zoho_field_to_spark_type, normalize_record

logger = logging.getLogger(__name__)


class ModuleHandler(TableHandler):
    """
    Handler for standard Zoho CRM modules.

    Standard modules:
    - Support the Records API (/crm/v8/{module})
    - Have a Modified_Time field for CDC
    - Support the Deleted Records API for tracking deletions
    """

    # Modules to exclude from listing
    EXCLUDED_MODULES = {
        "Visits",  # No fields available
        "Actions_Performed",  # No fields available
        "Email_Sentiment",  # Analytics module with different API
        "Email_Analytics",  # Analytics module with different API
        "Email_Template_Analytics",  # Analytics module with different API
        "Locking_Information__s",  # System module (403 forbidden)
    }

    def __init__(self, client) -> None:
        super().__init__(client)
        self._modules_cache: Optional[list[dict]] = None
        self._fields_cache: dict[str, list[dict]] = {}

    def get_modules(self) -> list[dict]:
        """
        Retrieve all available modules from Zoho CRM.
        Results are cached to avoid repeated API calls.
        """
        if self._modules_cache is not None:
            return self._modules_cache

        response = self.client.request("GET", "/crm/v8/settings/modules")
        modules = response.get("modules", [])

        # Filter for API-supported modules
        supported = [
            m for m in modules
            if m.get("api_supported")
            and m.get("generated_type") in ("default", "custom")
            and m.get("api_name") not in self.EXCLUDED_MODULES
        ]

        self._modules_cache = supported
        return supported

    def get_fields(self, module_name: str) -> list[dict]:
        """
        Retrieve field metadata for a specific module.
        Results are cached per module.
        """
        if module_name in self._fields_cache:
            return self._fields_cache[module_name]

        response = self.client.request(
            "GET",
            "/crm/v8/settings/fields",
            params={"module": module_name},
        )
        fields = response.get("fields", [])

        self._fields_cache[module_name] = fields
        return fields

    def get_json_fields(self, module_name: str) -> set:
        """
        Get field names that should be serialized as JSON strings.

        Args:
            module_name: Name of the Zoho CRM module

        Returns:
            Set of field API names with json_type 'jsonobject' or 'jsonarray'
        """
        fields = self.get_fields(module_name)
        return {
            f.get("api_name") for f in fields
            if f.get("json_type") in ("jsonobject", "jsonarray")
        }

    def get_schema(self, table_name: str, config: dict) -> StructType:
        """
        Get Spark schema for a standard CRM module.

        Dynamically builds the schema by fetching field metadata from the
        Zoho CRM Fields API and converting each field to a Spark StructField.

        Args:
            table_name: Name of the Zoho CRM module
            config: Table configuration (unused for standard modules)

        Returns:
            Spark StructType representing the module schema
        """
        fields = self.get_fields(table_name)

        if not fields:
            logger.warning("No fields available for %s, using minimal schema", table_name)
            return StructType([StructField("id", LongType(), False)])

        struct_fields = []
        for field in fields:
            try:
                struct_fields.append(zoho_field_to_spark_type(field))
            except Exception as e:
                logger.warning("Could not convert field %s: %s", field.get("api_name"), e)
                continue

        return StructType(struct_fields)

    def get_metadata(self, table_name: str, config: dict) -> dict:
        """
        Get ingestion metadata for a standard CRM module.

        Determines the appropriate ingestion strategy based on available fields:
        - CDC for modules with Modified_Time field
        - Append for Attachments
        - Snapshot for modules without Modified_Time

        Args:
            table_name: Name of the Zoho CRM module
            config: Table configuration (unused for standard modules)

        Returns:
            Dictionary with primary_keys, cursor_field, and ingestion_type
        """
        schema = self.get_schema(table_name, config)
        field_names = schema.fieldNames()
        has_modified_time = "Modified_Time" in field_names
        has_id = "id" in field_names

        # Attachments are append-only
        if table_name == "Attachments":
            return {
                "primary_keys": ["id"] if has_id else [],
                "ingestion_type": "append",
            }

        # Modules without Modified_Time use snapshot
        if not has_modified_time:
            return {
                "primary_keys": ["id"] if has_id else [],
                "ingestion_type": "snapshot",
            }

        # Standard modules support CDC
        return {
            "primary_keys": ["id"],
            "cursor_field": "Modified_Time",
            "ingestion_type": "cdc",
        }

    def read(
        self,
        table_name: str,
        config: dict,
        start_offset: dict,
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a standard CRM module.

        Supports incremental reads using Modified_Time cursor with a 5-minute
        lookback window to catch late updates. For CDC modules, also fetches
        deleted records via the Deleted Records API.

        Args:
            table_name: Name of the Zoho CRM module
            config: Table configuration with optional initial_load_start_date
            start_offset: Dictionary with cursor_time for incremental reads

        Returns:
            Tuple of (records iterator, next offset dictionary)
        """
        # Determine cursor time for incremental reads
        cursor_time = start_offset.get("cursor_time") if start_offset else None
        initial_load_start_date = config.get("initial_load_start_date")

        if not cursor_time and initial_load_start_date:
            cursor_time = initial_load_start_date

        # Apply 5-minute lookback window to catch late updates
        if cursor_time:
            cursor_dt = datetime.fromisoformat(cursor_time.replace("Z", "+00:00"))
            lookback_dt = cursor_dt - timedelta(minutes=5)
            cursor_time = lookback_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

        # Check if this is a snapshot table (no cursor support)
        metadata = self.get_metadata(table_name, config)
        if metadata.get("ingestion_type") == "snapshot":
            cursor_time = None

        # Track max modified time for next offset
        max_modified_time = cursor_time

        def records_generator():
            nonlocal max_modified_time
            json_fields = self.get_json_fields(table_name)
            fields = self.get_fields(table_name)
            field_names = [f["api_name"] for f in fields] if fields else []

            # Read regular records
            for record in self._read_records(table_name, field_names, cursor_time, json_fields):
                modified_time = record.get("Modified_Time")
                if modified_time and (not max_modified_time or modified_time > max_modified_time):
                    max_modified_time = modified_time
                yield record

            # Read deleted records for CDC
            if metadata.get("ingestion_type") == "cdc" and cursor_time:
                for record in self._read_deleted_records(table_name, cursor_time):
                    deleted_time = record.get("deleted_time")
                    if deleted_time and (not max_modified_time or deleted_time > max_modified_time):
                        max_modified_time = deleted_time
                    yield record

        # Materialize generator to get final max_modified_time
        records = list(records_generator())

        if max_modified_time:
            next_offset = {"cursor_time": max_modified_time}
        else:
            next_offset = start_offset or {}
        return iter(records), next_offset

    def _read_records(
        self,
        module_name: str,
        field_names: list[str],
        cursor_time: Optional[str],
        json_fields: set,
    ) -> Iterator[dict]:
        """Read records from a module with pagination."""
        params = {
            "sort_order": "asc",
            "sort_by": "Modified_Time",
        }

        if field_names:
            params["fields"] = ",".join(field_names)

        if cursor_time:
            params["criteria"] = f"(Modified_Time:greater_equal:{cursor_time})"

        for record in self.client.paginate(f"/crm/v8/{module_name}", params=params):
            yield normalize_record(record, json_fields)

    def _read_deleted_records(
        self,
        module_name: str,
        cursor_time: str,
    ) -> Iterator[dict]:
        """Read deleted records from a module."""
        params = {"type": "all"}

        for record in self.client.paginate(f"/crm/v8/{module_name}/deleted", params=params):
            deleted_time = record.get("deleted_time")

            # Only include records deleted after cursor_time
            if deleted_time and deleted_time >= cursor_time:
                record["_zoho_deleted"] = True
                yield record
