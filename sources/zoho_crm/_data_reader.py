import json
import re
import requests
import time
from datetime import datetime, timedelta
from typing import Any, Iterator, Optional

from pyspark.sql.types import StructType

from ._auth_client import AuthClient
from ._metadata_manager import MetadataManager
from ._schema_generator import SchemaGenerator


class DataReader:
    """
    Handles reading data from Zoho CRM, including standard modules and derived tables.

    This class manages pagination, incremental reads (CDC), fetching deleted records,
    and normalizing records for Spark compatibility.
    """

    def __init__(self, auth_client: AuthClient, metadata_manager: MetadataManager, schema_generator: SchemaGenerator, derived_tables: dict, initial_load_start_date: Optional[str]) -> None:
        """
        Initializes the DataReader with necessary client instances and configuration.

        Args:
            auth_client: An instance of AuthClient for making API requests.
            metadata_manager: An instance of MetadataManager for fetching metadata.
            schema_generator: An instance of SchemaGenerator for retrieving table schemas.
            derived_tables: A dictionary defining derived tables.
            initial_load_start_date: Starting point for the first sync, if provided.
        """
        self._auth_client = auth_client
        self._metadata_manager = metadata_manager
        self._schema_generator = schema_generator
        self.DERIVED_TABLES = derived_tables
        self.initial_load_start_date = initial_load_start_date

        # Attribute to track the maximum 'Modified_Time' encountered during data reads,
        # essential for Change Data Capture (CDC) to determine the next ingestion point.
        self._current_max_modified_time: Optional[str] = None

    def read_table(self, table_name: str, start_offset: dict, table_options: dict[str, str]) -> (Iterator[dict], dict):
        """
        Reads records from a specified Zoho CRM module or derived table.

        This is the primary method for data ingestion, supporting:
        - Incremental reads using a 'Modified_Time' cursor.
        - Pagination to fetch large datasets efficiently.
        - Fetching deleted records for Change Data Capture (CDC).
        - Routing requests to specialized readers for derived tables.

        Args:
            table_name: The name of the table (module or derived table) to read.
            start_offset: A dictionary containing the 'cursor_time' for incremental reads.
            table_options: Additional options specific to the table, if any.

        Returns:
            A tuple containing:
            - An iterator yielding dictionaries, where each dictionary represents a record.
            - A dictionary representing the next 'start_offset' for subsequent reads (containing 'cursor_time').
        """
        print(f"[DEBUG] Invoking read_table for '{table_name}' with start_offset: {start_offset} and initial_load_start_date: {self.initial_load_start_date}")

        # If the table is a derived type, delegate the read operation to a specialized handler.
        if table_name in self.DERIVED_TABLES:
            return self._read_derived_table(table_name, start_offset, table_options)

        # Validate that the requested table (module) is supported by the connector.
        available_modules = self._metadata_manager.list_tables()
        if table_name not in available_modules:
            raise ValueError(f"Table '{table_name}' is not supported. Available tables: {', '.join(available_modules)}")

        # Determine the effective cursor time for incremental data fetching.
        # This prioritizes the provided start_offset, then initial_load_start_date, otherwise fetches all data.
        effective_cursor_time = None
        if start_offset and "cursor_time" in start_offset:
            effective_cursor_time = start_offset["cursor_time"]
            print(f"[DEBUG] Using cursor_time from start_offset: {effective_cursor_time}")
        elif self.initial_load_start_date:
            effective_cursor_time = self.initial_load_start_date
            print(f"[DEBUG] Using initial_load_start_date as cursor_time: {effective_cursor_time}")
        else:
            print("[DEBUG] No cursor_time provided; performing a full historical data load.")

        # Apply a lookback window to the cursor time for robust CDC, ensuring no recent updates are missed.
        if effective_cursor_time:
            cursor_datetime_obj = datetime.fromisoformat(effective_cursor_time.replace("Z", "+00:00"))
            lookback_datetime_obj = cursor_datetime_obj - timedelta(minutes=5)
            effective_cursor_time = lookback_datetime_obj.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            print(f"[DEBUG] Cursor_time adjusted with 5-minute lookback: {effective_cursor_time}")

        # Retrieve table metadata to determine the appropriate ingestion strategy (CDC, snapshot, append).
        table_metadata = self._metadata_manager.read_table_metadata(table_name, self._schema_generator.get_table_schema, table_options)
        ingestion_strategy = table_metadata.get("ingestion_type")

        # For snapshot ingestion, the cursor time is irrelevant as all data is re-fetched.
        if ingestion_strategy == "snapshot":
            effective_cursor_time = None
            print("[DEBUG] Table configured for snapshot ingestion; cursor_time reset to None.")

        # Fetch active records from the module based on the effective cursor time.
        active_records_iterator = self._read_records(table_name, effective_cursor_time)

        # If CDC is enabled and a cursor time is present, also fetch deleted records.
        if ingestion_strategy == "cdc" and effective_cursor_time:
            # Convert the active records iterator to a list to allow for multiple iterations.
            all_records_list = list(active_records_iterator)
            deleted_records_list = list(self._read_deleted_records(table_name, effective_cursor_time))

            # Combine both active and deleted records into a single iterator for output.
            def combined_records_generator():
                yield from all_records_list
                yield from deleted_records_list

            active_records_iterator = combined_records_generator()
            print(f"[DEBUG] Combined {len(all_records_list)} active and {len(deleted_records_list)} deleted records for CDC.")

        # Calculate the next offset for the subsequent incremental read based on the latest modified/deleted time seen.
        next_offset_value = self._current_max_modified_time
        if next_offset_value:
            next_read_offset = {"cursor_time": next_offset_value}
        else:
            # If no records were processed, maintain the original start_offset for the next run.
            next_read_offset = start_offset or {}

        print(f"[DEBUG] Exiting read_table. Next offset for subsequent reads: {next_read_offset}")
        return active_records_iterator, next_read_offset

    def _read_derived_table(self, table_name: str, start_offset: dict, table_options: dict[str, str]) -> (Iterator[dict], dict):
        """
        Reads records from a specific derived table by delegating to specialized
        reader methods based on the derived table's type (settings, subforms, or related).

        Args:
            table_name: The name of the derived table to read.
            start_offset: The offset for the read operation.
            table_options: Additional options specific to the table.

        Returns:
            A tuple containing an iterator of records and the next offset.
        """
        derived_table_config = self.DERIVED_TABLES[table_name]
        derived_table_type = derived_table_config["type"]

        # Delegate to the appropriate specialized reader based on the derived table type.
        if derived_table_type == "settings":
            return self._read_settings_table(table_name, derived_table_config, start_offset)
        elif derived_table_type == "subform":
            return self._read_subform_table(table_name, derived_table_config, start_offset)
        elif derived_table_type == "related":
            return self._read_related_table(table_name, derived_table_config, start_offset)
        else:
            # Raise an error if an unknown derived table type is encountered.
            raise ValueError(f"Unknown derived table type encountered: {derived_table_type}")

    def _read_settings_table(self, table_name: str, config: dict, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read records from settings/organization tables (Users, Roles, Profiles).

        Note: Users table requires additional OAuth scope: ZohoCRM.users.READ
        If you get 401 errors, re-authorize with the additional scope.
        """
        endpoint = config["endpoint"]
        data_key = config["data_key"]

        def records_generator():
            page = 1
            per_page = 200

            while True:
                params = {"page": page, "per_page": per_page}

                if table_name == "Users":
                    params["type"] = "AllUsers"

                try:
                    response = self._auth_client.make_request("GET", endpoint, params=params)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 401 and table_name == "Users":
                        print(f"[WARNING] Users table requires ZohoCRM.users.READ scope. " f"Please re-authorize with additional scopes to access user data.")
                        return  # Return empty generator
                    raise
                except Exception as e:
                    print(f"[DEBUG] Error fetching {table_name}: {e}")
                    raise

                data = response.get(data_key, [])
                info = response.get("info", {})

                print(f"[DEBUG] {table_name} page {page}: got {len(data)} records")

                for record in data:
                    yield record

                more_records = info.get("more_records", False)
                if not more_records or not data:
                    break

                page += 1

        # Settings tables use snapshot (no cursor tracking needed)
        return records_generator(), {}

    def _read_subform_table(self, table_name: str, config: dict, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read subform/line item records by extracting them from parent records.
        Example: Quoted_Items extracted from Quotes.Quoted_Items
        """
        parent_module = config["parent_module"]
        subform_field = config["subform_field"]

        def records_generator():
            print(f"[DEBUG] Reading {table_name} from {parent_module}.{subform_field}")

            # Get fields for parent module to include the subform
            fields_meta = self._metadata_manager.get_fields(parent_module)
            field_names = [f["api_name"] for f in fields_meta] if fields_meta else []

            page = 1
            per_page = 200
            total_items = 0

            while True:
                params = {
                    "page": page,
                    "per_page": per_page,
                    "sort_order": "asc",
                    "sort_by": "Modified_Time",
                }

                if field_names:
                    params["fields"] = ",".join(field_names)

                try:
                    response = self._auth_client.make_request("GET", f"/crm/v8/{parent_module}", params=params)
                except Exception as e:
                    print(f"[DEBUG] Error fetching {parent_module}: {e}")
                    raise

                data = response.get("data", [])
                info = response.get("info", {})

                print(f"[DEBUG] {parent_module} page {page}: got {len(data)} parent records")

                # Extract subform items from each parent record
                for parent_record in data:
                    parent_id = parent_record.get("id")
                    subform_items = parent_record.get(subform_field, [])

                    if subform_items:
                        for item in subform_items:
                            # Add parent reference
                            item["_parent_id"] = parent_id
                            item["_parent_module"] = parent_module
                            total_items += 1
                            yield item

                more_records = info.get("more_records", False)
                if not more_records or not data:
                    break

                page += 1

            print(f"[DEBUG] Total {table_name} items extracted: {total_items}")

        # Subforms use snapshot (no cursor tracking)
        return records_generator(), {}

    def _read_related_table(self, table_name: str, config: dict, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read junction/related records by iterating through parent records
        and fetching their related records.
        Example: Campaigns_Leads by fetching Leads for each Campaign.
        """
        parent_module = config["parent_module"]
        related_module = config["related_module"]

        # Get fields for the related module (required by API)
        related_fields = self._get_related_module_fields(related_module)

        def records_generator():
            print(f"[DEBUG] Reading {table_name}: {parent_module} -> {related_module}")

            # First, get all parent records
            parent_ids = []
            page = 1
            per_page = 200

            while True:
                params = {
                    "page": page,
                    "per_page": per_page,
                    "fields": "id",  # Only need ID
                }

                try:
                    response = self._auth_client.make_request("GET", f"/crm/v8/{parent_module}", params=params)
                except Exception as e:
                    print(f"[DEBUG] Error fetching {parent_module}: {e}")
                    raise

                data = response.get("data", [])
                info = response.get("info", {})

                parent_ids.extend([r.get("id") for r in data if r.get("id")])

                more_records = info.get("more_records", False)
                if not more_records or not data:
                    break

                page += 1

            print(f"[DEBUG] Found {len(parent_ids)} parent records in {parent_module}")

            # For each parent, fetch related records
            total_related = 0
            for parent_id in parent_ids:
                related_page = 1

                while True:
                    params = {
                        "page": related_page,
                        "per_page": per_page,
                        "fields": related_fields,  # Required by Zoho API
                    }

                    try:
                        response = self._auth_client.make_request("GET", f"/crm/v8/{parent_module}/{parent_id}/{related_module}", params=params)
                    except requests.exceptions.HTTPError as e:
                        # 204 No Content, 400 (no data), or 404 means no related records
                        if e.response.status_code in (204, 400, 404):
                            break
                        raise
                    except Exception as e:
                        print(f"[DEBUG] Error fetching related {related_module} for {parent_id}: {e}")
                        break

                    data = response.get("data", [])
                    info = response.get("info", {})

                    for record in data:
                        # Add junction metadata
                        record["_junction_id"] = f"{parent_id}_{record.get('id')}"
                        record["_parent_id"] = parent_id
                        record["_parent_module"] = parent_module
                        total_related += 1
                        yield record

                    more_records = info.get("more_records", False)
                    if not more_records or not data:
                        break

                    related_page += 1

            print(f"[DEBUG] Total {table_name} junction records: {total_related}")

        # Junction tables use snapshot (no cursor tracking)
        return records_generator(), {}

    def _get_related_module_fields(self, related_module: str) -> str:
        """
        Get field names for a related module to pass to Related Records API.
        Returns common fields based on the module type.
        """
        # Map related module types to their field lists
        field_maps = {
            "Leads": "id,First_Name,Last_Name,Email,Company,Phone,Lead_Status",
            "Contacts": "id,First_Name,Last_Name,Email,Phone,Account_Name",
            "Deals": "id,Deal_Name,Stage,Amount,Closing_Date,Account_Name",
            "Contact_Roles": "id,Contact_Role,name,Email",
        }
        return field_maps.get(related_module, "id,name")

    def _get_json_fields(self, module_name: str) -> set:
        """
        Get field names that should be serialized as JSON strings.
        These are fields with json_type 'jsonobject' or 'jsonarray'.
        """
        # Fetch field metadata for standard CRM modules.
        fields = self._metadata_manager.get_fields(module_name)
        json_fields = set()
        for field in fields:
            json_type = field.get("json_type")
            if json_type in ("jsonobject", "jsonarray"):
                json_fields.add(field.get("api_name"))
        return json_fields

    def _normalize_record(self, record: dict, json_fields: set) -> dict:
        """
        Normalize a record for Spark compatibility.
        Only serializes fields that are declared as JSON strings in schema.
        """
        normalized = {}
        for key, value in record.items():
            if value is None:
                normalized[key] = None
            elif key in json_fields and isinstance(value, (dict, list)):
                # Only serialize fields that are declared as StringType for JSON
                normalized[key] = json.dumps(value)
            else:
                normalized[key] = value
        return normalized

    def _read_records(self, module_name: str, cursor_time: Optional[str] = None) -> Iterator[dict]:
        """
        Read records from a module with pagination.
        """
        print(f"[DEBUG] _read_records called for '{module_name}' with cursor_time: {cursor_time}")
        self._current_max_modified_time = cursor_time

        # Get fields that need JSON serialization
        json_fields = self._get_json_fields(module_name)
        print(f"[DEBUG] JSON fields to serialize: {json_fields}")

        # Get field names for this module (required by Zoho API)
        fields = self._metadata_manager.get_fields(module_name)
        field_names = [f["api_name"] for f in fields] if fields else []
        print(f"[DEBUG] Field names for {module_name}: {len(field_names)} fields")

        page = 1
        per_page = 200  # Maximum allowed by Zoho CRM
        total_records_yielded = 0

        while True:
            params = {
                "page": page,
                "per_page": per_page,
                "sort_order": "asc",
                "sort_by": "Modified_Time",
            }

            # Add fields parameter (required by Zoho API despite docs saying optional)
            if field_names:
                params["fields"] = ",".join(field_names)

            # Add incremental filter if cursor_time is provided
            if cursor_time:
                # URL encode the criteria
                criteria = f"(Modified_Time:greater_equal:{cursor_time})"
                params["criteria"] = criteria

            print(f"[DEBUG] API request: GET /crm/v8/{module_name} with params: {params}")

            try:
                response = self._auth_client.make_request("GET", f"/crm/v8/{module_name}", params=params)
                print(f"[DEBUG] API response keys: {response.keys() if response else 'None'}")
            except Exception as e:
                print(f"[DEBUG] ERROR fetching page {page} for {module_name}: {e}")
                raise

            data = response.get("data", [])
            info = response.get("info", {})
            print(f"[DEBUG] Page {page}: got {len(data)} records, info: {info}")

            # Track the maximum Modified_Time seen
            for record in data:
                modified_time = record.get("Modified_Time")
                if modified_time:
                    if not self._current_max_modified_time or modified_time > self._current_max_modified_time:
                        self._current_max_modified_time = modified_time

                total_records_yielded += 1
                yield self._normalize_record(record, json_fields)

            # Check if there are more pages
            more_records = info.get("more_records", False)
            print(f"[DEBUG] more_records: {more_records}, data empty: {len(data) == 0}")
            if not more_records or not data:
                print(f"[DEBUG] Stopping pagination. Total records yielded: {total_records_yielded}")
                break

            page += 1

    def _read_deleted_records(self, module_name: str, cursor_time: Optional[str] = None) -> Iterator[dict]:
        """
        Read deleted records from a module.
        Returns records marked for deletion with a special field.
        """
        page = 1
        per_page = 200

        while True:
            params = {
                "type": "all",
                "page": page,
                "per_page": per_page,
            }

            try:
                response = self._auth_client.make_request("GET", f"/crm/v8/{module_name}/deleted", params=params)
            except Exception as e:
                print(f"[DEBUG] ERROR fetching deleted records for {module_name}: {e}")
                raise

            data = response.get("data", [])
            info = response.get("info", {})

            # Filter by cursor_time if provided
            for record in data:
                deleted_time = record.get("deleted_time")

                # Only include records deleted after cursor_time
                if cursor_time and deleted_time:
                    if deleted_time < cursor_time:
                        continue

                # Mark this record as deleted
                record["_zoho_deleted"] = True

                # Update max modified time to deleted_time
                if deleted_time:
                    if not self._current_max_modified_time or deleted_time > self._current_max_modified_time:
                        self._current_max_modified_time = deleted_time

                yield record

            # Check if there are more pages
            more_records = info.get("more_records", False)
            if not more_records or not data:
                break

            page += 1
