"""
# Zoho CRM connector for Lakeflow/Databricks.

# This module provides the main LakeflowConnect class that orchestrates
# data ingestion from Zoho CRM into Databricks.

# =============================================================================
# CODE STRUCTURE
# =============================================================================

# The Zoho CRM connector is organized into modular components:

#     zoho_crm/
#     ├── zoho_crm.py          # Main orchestrator (this file)
#     ├── zoho_client.py       # API client: auth, HTTP, pagination
#     ├── zoho_types.py        # Spark type mappings and schema definitions
#     └── handlers/
#         ├── base.py          # Abstract TableHandler interface
#         ├── module.py        # Standard CRM modules (Leads, Contacts, etc.)
#         ├── settings.py      # Org tables (Users, Roles, Profiles)
#         ├── subform.py       # Line items (disabled by default, see file)
#         └── related.py       # Junction tables (Campaigns_Leads, etc.)

# =============================================================================
# ARCHITECTURE
# =============================================================================

# LakeflowConnect (this file)
#     │
#     ├── ZohoAPIClient (zoho_client.py)
#     │       Handles OAuth2 authentication, HTTP requests, rate limiting,
#     │       and pagination. Shared by all handlers.
#     │
#     └── TableHandlers (handlers/)
#             Each handler implements get_schema(), get_metadata(), read()
#             for a specific table type. The orchestrator routes requests
#             to the appropriate handler based on table name.

# =============================================================================
# SUPPORTED TABLE TYPES
# =============================================================================

# 1. Standard Modules (ModuleHandler)
#    - Dynamically discovered via Modules API
#    - Schemas fetched from Fields API
#    - Support CDC via Modified_Time cursor
#    - Examples: Leads, Contacts, Accounts, Deals, Tasks, Notes

# 2. Settings Tables (SettingsHandler)
#    - Fixed set of org-level tables
#    - Predefined schemas (stable structure)
#    - Use different API endpoints than modules
#    - Tables: Users, Roles, Profiles

# 3. Subform Tables (SubformHandler)
#    - Extracted from parent record subform fields
#    - Line items from Quotes, Sales Orders, Invoices, Purchase Orders
#    - Include _parent_id, _parent_module for traceability
#    - NOTE: Disabled by default (requires Zoho Inventory/Books)
#    - See handlers/subform.py to enable if you have these products

# 4. Junction Tables (RelatedHandler)
#    - Many-to-many relationships between modules
#    - Fetched via Related Records API
#    - Include _junction_id, _parent_id, _parent_module
#    - Tables: Campaigns_Leads, Campaigns_Contacts, Contacts_X_Deals
"""

import logging
from typing import Iterator

from pyspark.sql.types import StructType

from .zoho_client import ZohoAPIClient
from .handlers import (
    ModuleHandler,
    SettingsHandler,
    SubformHandler,
    RelatedHandler,
)

logger = logging.getLogger(__name__)


class LakeflowConnect:
    """
    Zoho CRM connector for Lakeflow/Databricks.

    This class serves as the main entry point and orchestrator for the connector.
    It delegates actual work to specialized handlers for different table types.
    """

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Zoho CRM connector with connection-level options.

        Expected options:
            - client_id: OAuth Client ID from Zoho API Console
            - client_value_tmp: OAuth Client Secret from Zoho API Console
            - refresh_value_tmp: Long-lived refresh token obtained from OAuth flow
            - base_url (optional): Zoho accounts URL for OAuth.
              Defaults to https://accounts.zoho.com
              Examples: US=accounts.zoho.com, EU=accounts.zoho.eu,
              IN=accounts.zoho.in, AU=accounts.zoho.com.au
            - initial_load_start_date (optional): Starting point for the first sync.
              If omitted, syncs all historical data.

        Note: To obtain the refresh_value_tmp, follow the OAuth setup guide in
        sources/zoho_crm/configs/README.md
        """
        # Validate required options
        client_id = options.get("client_id")
        client_secret = options.get("client_value_tmp")
        refresh_token = options.get("refresh_value_tmp")

        if not all([client_id, client_secret, refresh_token]):
            raise ValueError("Zoho CRM connector requires 'client_id', 'client_value_tmp', " "and 'refresh_value_tmp' in the UC connection")

        # Store configuration
        self.initial_load_start_date = options.get("initial_load_start_date")
        accounts_url = options.get("base_url", "https://accounts.zoho.com")

        # Initialize API client
        self._client = ZohoAPIClient(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            accounts_url=accounts_url,
        )

        # Initialize handlers
        self._module_handler = ModuleHandler(self._client)
        self._settings_handler = SettingsHandler(self._client)
        self._subform_handler = SubformHandler(self._client, self._module_handler)
        self._related_handler = RelatedHandler(self._client)

        # Build derived tables lookup
        self._derived_tables = self._build_derived_tables_map()

    def _build_derived_tables_map(self) -> dict[str, tuple[object, dict]]:
        """
        Build a mapping of derived table names to their handlers and configs.

        Derived tables are non-module tables that require special handling:
        - Settings tables (Users, Roles, Profiles)
        - Subform tables (Quoted_Items, etc.)
        - Junction/related tables (Campaigns_Leads, etc.)

        Returns:
            Dictionary mapping table name to (handler, config) tuple
        """
        derived = {}

        # Settings tables
        for name, config in SettingsHandler.get_tables().items():
            derived[name] = (self._settings_handler, {"type": "settings", **config})

        # Subform tables
        for name, config in SubformHandler.get_tables().items():
            derived[name] = (self._subform_handler, {"type": "subform", **config})

        # Related tables
        for name, config in RelatedHandler.get_tables().items():
            derived[name] = (self._related_handler, {"type": "related", **config})

        return derived

    def _get_handler_and_config(self, table_name: str) -> tuple[object, dict]:
        """
        Get the appropriate handler and configuration for a table.

        Returns:
            Tuple of (handler, config) for the table
        """
        if table_name in self._derived_tables:
            return self._derived_tables[table_name]

        # Standard module
        return self._module_handler, {"initial_load_start_date": self.initial_load_start_date}

    def list_tables(self) -> list[str]:
        """
        List names of all tables (modules) supported by this connector.

        Uses the Modules API to dynamically discover available modules,
        plus derived tables for settings, subforms, and junction tables.

        Returns:
            Sorted list of table names
        """
        # Get standard CRM modules
        modules = self._module_handler.get_modules()
        table_names = [m["api_name"] for m in modules]

        # Add derived tables
        table_names.extend(self._derived_tables.keys())

        return sorted(table_names)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """
        Fetch the schema of a table dynamically from Zoho CRM.

        Args:
            table_name: Name of the table to get schema for
            table_options: Additional options for accessing the table

        Returns:
            Spark StructType representing the table schema
        """
        self._validate_table_exists(table_name)
        handler, config = self._get_handler_and_config(table_name)
        return handler.get_schema(table_name, config)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """
        Fetch the metadata of a table.

        Args:
            table_name: Name of the table
            table_options: Additional options for accessing the table

        Returns:
            Dictionary containing:
                - primary_keys: List of primary key column names
                - cursor_field: (optional) Field name for incremental loading
                - ingestion_type: "snapshot", "cdc", "cdc_with_deletes", or "append"
        """
        self._validate_table_exists(table_name)
        handler, config = self._get_handler_and_config(table_name)
        return handler.get_metadata(table_name, config)

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a Zoho CRM table.

        Supports incremental reads using Modified_Time cursor and pagination.
        Routes to specialized handlers based on table type.

        Args:
            table_name: Name of the table to read
            start_offset: Offset to start reading from (for incremental loads)
            table_options: Additional options for accessing the table

        Returns:
            Tuple of (records_iterator, next_offset)
        """
        self._validate_table_exists(table_name)
        handler, config = self._get_handler_and_config(table_name)
        return handler.read(table_name, config, start_offset)

    def _validate_table_exists(self, table_name: str) -> None:
        """
        Validate that a table exists and is supported.

        Args:
            table_name: Name of the table to validate

        Raises:
            ValueError: If the table is not found in available tables
        """
        # Derived tables are always valid
        if table_name in self._derived_tables:
            return

        # Check standard modules
        available_tables = self.list_tables()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' is not supported. " f"Available tables: {', '.join(available_tables)}")
