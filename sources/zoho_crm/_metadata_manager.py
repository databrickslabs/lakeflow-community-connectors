from typing import Optional
from ._auth_client import AuthClient


class MetadataManager:
    """
    Manages Zoho CRM module and field metadata, including caching and determining
    table metadata (primary keys, cursor fields, ingestion types).
    """

    def __init__(self, auth_client: AuthClient, derived_tables: dict) -> None:
        """
        Initializes the MetadataManager with an AuthClient instance and derived table configuration.

        Args:
            auth_client: An instance of AuthClient for making API requests.
            derived_tables: A dictionary defining derived tables.
        """
        self._auth_client = auth_client
        self.DERIVED_TABLES = derived_tables

        # Initialize caches to store fetched metadata (modules, fields)
        # to minimize redundant API calls and improve performance.
        self._modules_cache: Optional[list[dict]] = None
        self._fields_cache: dict[str, list[dict]] = {}

    def _get_modules(self) -> list[dict]:
        """
        Retrieves and caches a list of all available Zoho CRM modules.
        Filters out analytics, system, and other modules not suitable for data ingestion.
        """
        if self._modules_cache is not None:
            return self._modules_cache  # Return cached modules if available.

        # Fetch modules from the Zoho CRM settings API.
        modules_response = self._auth_client.make_request("GET", "/crm/v8/settings/modules")
        all_modules = modules_response.get("modules", [])

        # Define modules to be explicitly excluded from the supported list.
        modules_to_exclude = {
            "Visits",  # No fields available, typically analytics.
            "Actions_Performed",  # No fields available.
            "Email_Sentiment",  # Analytics module, separate API.
            "Email_Analytics",  # Analytics module, separate API.
            "Email_Template_Analytics",  # Analytics module, separate API.
            "Locking_Information__s",  # Internal system module, often forbidden.
        }

        # Filter modules to include only API-supported, standard/custom types, and not in the exclusion list.
        supported_modules = [
            module for module in all_modules
            if module.get("api_supported")  # Ensure the module supports API access.
            and module.get("generated_type") in ["default", "custom"]  # Include standard and custom modules.
            and module.get("api_name") not in modules_to_exclude  # Exclude known problematic modules.
        ]

        self._modules_cache = supported_modules  # Cache the filtered list of modules.
        return supported_modules

    def get_fields(self, module_name: str) -> list[dict]:
        """
        Retrieves and caches field metadata for a specified Zoho CRM module.
        Handles cases where a module might not have accessible field metadata.
        """
        if module_name in self._fields_cache:
            return self._fields_cache[module_name]  # Return cached fields if available.

        params = {"module": module_name}
        try:
            # Fetch field metadata from the Zoho CRM settings API.
            fields_response = self._auth_client.make_request("GET", "/crm/v8/settings/fields", params=params)
            module_fields = fields_response.get("fields", [])
        except Exception as e:
            # Log a warning and return an empty list if field metadata cannot be fetched.
            print(f"Warning: Could not fetch fields for module '{module_name}': {e}")
            module_fields = []

        self._fields_cache[module_name] = module_fields  # Cache the fetched fields.
        return module_fields

    def list_tables(self) -> list[str]:
        """
        Lists the names of all supported tables (modules) by this connector.
        This includes both standard Zoho CRM modules discovered via API and
        connector-defined derived tables (settings, subforms, and junction tables).
        """
        # Retrieve standard CRM modules that are accessible via API.
        standard_modules = self._get_modules()
        table_names = [module["api_name"] for module in standard_modules]

        # Add the names of all derived tables defined within the connector.
        table_names.extend(self.DERIVED_TABLES.keys())

        return sorted(table_names)  # Return a sorted list of all unique table names.

    def read_table_metadata(self, table_name: str, get_table_schema_func, table_options: dict[str, str]) -> dict:
        """
        Retrieves metadata for a specified Zoho CRM module or derived table.
        This metadata includes primary keys, the cursor field for Change Data Capture (CDC),
        and the recommended ingestion type (CDC, snapshot, or append).
        """
        # Delegate metadata retrieval to a specialized method if it's a derived table.
        if table_name in self.DERIVED_TABLES:
            return self._get_derived_table_metadata(table_name)

        # Ensure the requested table (module) is supported by the connector.
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(f"Table '{table_name}' is not supported. Available tables: {', '.join(supported_tables)}")

        # Fetch the table's schema to determine the presence of 'Modified_Time' and 'id' fields.
        table_schema = get_table_schema_func(table_name, table_options)
        schema_field_names = table_schema.fieldNames()
        has_modified_time_field = "Modified_Time" in schema_field_names
        has_id_field = "id" in schema_field_names

        # Special handling for 'Attachments': they are typically append-only and lack CDC.
        if table_name == "Attachments":
            return {
                "primary_keys": ["id"] if has_id_field else [],
                "ingestion_type": "append",
            }

        # Modules without a 'Modified_Time' field are best ingested as snapshots.
        if not has_modified_time_field:
            return {
                "primary_keys": ["id"] if has_id_field else [],
                "ingestion_type": "snapshot",
            }

        # By default, most modules support CDC with 'Modified_Time' as the cursor.
        return {
            "primary_keys": ["id"],
            "cursor_field": "Modified_Time",
            "ingestion_type": "cdc",
        }

    def _get_derived_table_metadata(self, table_name: str) -> dict:
        """
        Retrieves metadata specifically for derived tables.
        This includes defining primary keys and the ingestion type (snapshot or CDC)
        based on the derived table's inherent characteristics.
        """
        derived_table_config = self.DERIVED_TABLES[table_name]
        table_type = derived_table_config["type"]

        if table_type == "settings":
            # For settings tables, 'Users' supports CDC, while 'Roles' and 'Profiles' are snapshot.
            if table_name == "Users":
                return {
                    "primary_keys": ["id"],
                    "cursor_field": "Modified_Time",
                    "ingestion_type": "cdc",
                }
            else:
                return {
                    "primary_keys": ["id"],
                    "ingestion_type": "snapshot",
                }
        elif table_type == "subform":
            # Subforms are ingested as snapshots as individual line items don't have CDC tracking.
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }
        elif table_type == "related":
            # Junction/related tables are ingested as snapshots, using a composite key.
            return {
                "primary_keys": ["_junction_id"],
                "ingestion_type": "snapshot",
            }
        else:
            # Default metadata for any other unrecognized derived table types.
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }
