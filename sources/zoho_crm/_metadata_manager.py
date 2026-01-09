"""
Metadata manager for Zoho CRM connector.

This module handles discovery and caching of Zoho CRM module and field metadata.
It provides:
- Dynamic module discovery via the Zoho CRM Modules API
- Field metadata retrieval and caching
- Table listing (combining standard modules and derived tables)
- Ingestion metadata (primary keys, cursor fields, ingestion types)

The MetadataManager acts as a central repository for all metadata operations,
reducing API calls through intelligent caching.

Example:
    >>> manager = MetadataManager(auth_client, derived_tables)
    >>> tables = manager.list_tables()
    >>> fields = manager.get_fields("Leads")
    >>> metadata = manager.read_table_metadata("Leads", get_schema_func, {})
"""

from typing import Optional, Callable

from ._auth_client import AuthClient


class MetadataManager:
    """
    Manages Zoho CRM module and field metadata with caching.

    This class is responsible for:
    - Discovering available CRM modules via the Zoho API
    - Fetching and caching field definitions for each module
    - Determining table metadata (primary keys, cursor fields, ingestion type)
    - Managing the list of supported tables (modules + derived tables)

    The class implements aggressive caching to minimize API calls:
    - Module list is cached after first retrieval
    - Field definitions are cached per module

    Attributes:
        DERIVED_TABLES: Dictionary of connector-defined derived tables
            (settings, subforms, junction tables).

    Note:
        Derived tables are virtual tables constructed by the connector from
        various Zoho APIs - they don't exist as standalone modules in Zoho CRM.
    """

    # Modules to exclude from the supported list
    # These are either analytics modules, system modules, or have API issues
    EXCLUDED_MODULES = {
        "Visits",                    # No fields available
        "Actions_Performed",         # No fields available
        "Email_Sentiment",           # Analytics module, different API
        "Email_Analytics",           # Analytics module, different API
        "Email_Template_Analytics",  # Analytics module, different API
        "Locking_Information__s",    # System module, often returns 403
    }

    def __init__(self, auth_client: AuthClient, derived_tables: dict) -> None:
        """
        Initialize the MetadataManager.

        Args:
            auth_client: An authenticated AuthClient instance for API calls.
            derived_tables: Dictionary defining connector-specific derived tables.
                Structure: {
                    "TableName": {
                        "type": "settings" | "subform" | "related",
                        # Additional type-specific keys...
                    }
                }
        """
        self._auth_client = auth_client
        self.DERIVED_TABLES = derived_tables

        # Caches to minimize API calls
        self._modules_cache: Optional[list[dict]] = None
        self._fields_cache: dict[str, list[dict]] = {}

    def _get_modules(self) -> list[dict]:
        """
        Retrieve and cache the list of available Zoho CRM modules.

        This method filters modules to include only those that:
        - Support API access (api_supported=True)
        - Are standard or custom modules (generated_type in ["default", "custom"])
        - Are not in the exclusion list

        Returns:
            List of module dictionaries containing module metadata.

        Note:
            Results are cached after first call. Subsequent calls return
            cached data without making API requests.
        """
        if self._modules_cache is not None:
            return self._modules_cache

        # Fetch all modules from the Zoho CRM Settings API
        modules_response = self._auth_client.make_request("GET", "/crm/v8/settings/modules")
        all_modules = modules_response.get("modules", [])

        # Filter to supported modules only
        supported_modules = [
            module for module in all_modules
            if module.get("api_supported")
            and module.get("generated_type") in ["default", "custom"]
            and module.get("api_name") not in self.EXCLUDED_MODULES
        ]

        self._modules_cache = supported_modules
        return supported_modules

    def get_fields(self, module_name: str) -> list[dict]:
        """
        Retrieve and cache field metadata for a specific module.

        Args:
            module_name: The API name of the module (e.g., "Leads", "Contacts").

        Returns:
            List of field dictionaries containing field metadata.
            Returns empty list if fields cannot be fetched.

        Note:
            Results are cached per module. Each field dictionary contains:
            - api_name: Field API name
            - data_type: Zoho data type (text, bigint, lookup, etc.)
            - required: Whether the field is required
            - json_type: JSON type for complex fields (jsonobject, jsonarray)
        """
        # Return cached fields if available
        if module_name in self._fields_cache:
            return self._fields_cache[module_name]

        params = {"module": module_name}
        try:
            fields_response = self._auth_client.make_request(
                "GET", "/crm/v8/settings/fields", params=params
            )
            module_fields = fields_response.get("fields", [])
        except Exception as e:
            # Some modules may not support the fields API
            print(f"Warning: Could not fetch fields for module '{module_name}': {e}")
            module_fields = []

        self._fields_cache[module_name] = module_fields
        return module_fields

    def list_tables(self) -> list[str]:
        """
        List all tables (modules) supported by this connector.

        This combines:
        1. Standard CRM modules discovered via the Modules API
        2. Derived tables defined by the connector (settings, subforms, junctions)

        Returns:
            Sorted list of table names.

        Example:
            >>> tables = manager.list_tables()
            >>> print(tables[:5])
            ['Accounts', 'Campaigns', 'Campaigns_Contacts', 'Campaigns_Leads', 'Cases']
        """
        # Get standard CRM modules
        standard_modules = self._get_modules()
        table_names = [module["api_name"] for module in standard_modules]

        # Add derived tables
        table_names.extend(self.DERIVED_TABLES.keys())

        return sorted(table_names)

    def read_table_metadata(
        self,
        table_name: str,
        get_table_schema_func: Callable,
        table_options: dict[str, str]
    ) -> dict:
        """
        Retrieve ingestion metadata for a table.

        This method determines:
        - Primary key columns for deduplication
        - Cursor field for incremental loading (CDC)
        - Ingestion type (cdc, snapshot, or append)

        Args:
            table_name: The name of the table to get metadata for.
            get_table_schema_func: Callback to get the table's schema
                (used to verify field existence).
            table_options: Additional table-specific options.

        Returns:
            Dictionary containing:
            - primary_keys: List of primary key column names
            - cursor_field: Name of the cursor field (for CDC/append)
            - ingestion_type: One of "cdc", "snapshot", or "append"

        Raises:
            ValueError: If the table is not supported.

        Note:
            Ingestion types:
            - "cdc": Change Data Capture using Modified_Time cursor
            - "snapshot": Full table refresh each sync
            - "append": Append-only tables (like Attachments)
        """
        # Handle derived tables separately
        if table_name in self.DERIVED_TABLES:
            return self._get_derived_table_metadata(table_name)

        # Validate table exists
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {', '.join(supported_tables)}"
            )

        # Get schema to check for key fields
        table_schema = get_table_schema_func(table_name, table_options)
        schema_field_names = table_schema.fieldNames()
        has_modified_time = "Modified_Time" in schema_field_names
        has_id = "id" in schema_field_names

        # Special case: Attachments are append-only
        if table_name == "Attachments":
            return {
                "primary_keys": ["id"] if has_id else [],
                "ingestion_type": "append",
            }

        # Tables without Modified_Time use snapshot ingestion
        if not has_modified_time:
            return {
                "primary_keys": ["id"] if has_id else [],
                "ingestion_type": "snapshot",
            }

        # Default: CDC with Modified_Time cursor
        return {
            "primary_keys": ["id"],
            "cursor_field": "Modified_Time",
            "ingestion_type": "cdc",
        }

    def _get_derived_table_metadata(self, table_name: str) -> dict:
        """
        Get ingestion metadata for derived tables.

        Different derived table types have different characteristics:
        - Settings tables: Users support CDC, Roles/Profiles use snapshot
        - Subform tables: Always snapshot (no individual CDC tracking)
        - Junction tables: Always snapshot with composite primary key

        Args:
            table_name: Name of the derived table.

        Returns:
            Dictionary with primary_keys, cursor_field (if applicable),
            and ingestion_type.
        """
        derived_config = self.DERIVED_TABLES[table_name]
        table_type = derived_config["type"]

        if table_type == "settings":
            # Users table supports CDC via Modified_Time
            if table_name == "Users":
                return {
                    "primary_keys": ["id"],
                    "cursor_field": "Modified_Time",
                    "ingestion_type": "cdc",
                }
            # Other settings tables (Roles, Profiles) use snapshot
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        elif table_type == "subform":
            # Subforms (line items) don't have individual CDC tracking
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        elif table_type == "related":
            # Junction tables use composite key (_junction_id = parent_id + related_id)
            return {
                "primary_keys": ["_junction_id"],
                "ingestion_type": "snapshot",
            }

        # Fallback for unknown types
        return {
            "primary_keys": ["id"],
            "ingestion_type": "snapshot",
        }
