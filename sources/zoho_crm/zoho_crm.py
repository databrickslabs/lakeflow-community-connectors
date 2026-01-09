"""
Zoho CRM Connector for LakeFlow/Databricks.

This module implements the LakeflowConnect interface for Zoho CRM, enabling
data ingestion from Zoho CRM into Delta Lake tables via Databricks LakeFlow.

Architecture Overview:
    The connector is organized into focused helper classes for maintainability:

    ┌─────────────────────────────────────────────────────────────────────┐
    │                         LakeflowConnect                             │
    │  (Main entry point - orchestrates the connector components)         │
    └─────────────────────────────────────────────────────────────────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            ▼                       ▼                       ▼
    ┌───────────────┐     ┌─────────────────┐     ┌───────────────┐
    │  AuthClient   │     │ MetadataManager │     │ SchemaGenerator│
    │  (_auth_      │     │ (_metadata_     │     │ (_schema_      │
    │   client.py)  │     │  manager.py)    │     │  generator.py) │
    └───────────────┘     └─────────────────┘     └───────────────┘
            │                       │                       │
            │                       ▼                       │
            │             ┌─────────────────┐               │
            └────────────▶│   DataReader    │◀──────────────┘
                          │ (_data_reader.py)│
                          └─────────────────┘

    Component Responsibilities:
    - AuthClient: OAuth 2.0 authentication, token management, API requests
    - MetadataManager: Module/field discovery, caching, table listing
    - SchemaGenerator: PySpark schema generation, type mapping
    - DataReader: Data fetching, pagination, CDC, record normalization

Supported Data Sources:
    1. Standard CRM Modules: Leads, Contacts, Accounts, Deals, Quotes,
       Sales_Orders, Invoices, Purchase_Orders, Campaigns, Cases, Tasks,
       Events, Calls, Notes, Attachments, and custom modules.

    2. Organization/Settings Tables: Users, Roles, Profiles - accessed
       via the Settings API rather than the standard CRM API.

    3. Subform/Line Item Tables: Quoted_Items, Ordered_Items, Invoiced_Items,
       Purchase_Items - line-item data embedded within parent records.

    4. Junction/Relationship Tables: Campaigns_Leads, Campaigns_Contacts,
       Contacts_X_Deals - many-to-many relationships between modules.

Change Data Capture (CDC):
    The connector supports incremental data loading using the Modified_Time
    field as a cursor. On subsequent syncs, only records modified since the
    last sync are fetched, significantly reducing API calls and data transfer.

    Deleted records are tracked via the Deleted Records API and surfaced
    for soft-delete handling in the destination.

Usage Example:
    >>> # Connection-level options (typically from Unity Catalog connection)
    >>> options = {
    ...     "client_id": "your_client_id",
    ...     "client_value_tmp": "your_client_secret",
    ...     "refresh_value_tmp": "your_refresh_token",
    ...     "base_url": "https://accounts.zoho.com",
    ... }
    >>> connector = LakeflowConnect(options)

    >>> # List available tables
    >>> tables = connector.list_tables()
    >>> print(tables[:5])
    ['Accounts', 'Campaigns', 'Cases', 'Contacts', 'Deals']

    >>> # Get schema for a table
    >>> schema = connector.get_table_schema("Leads", {})
    >>> print(schema.simpleString())
    struct<id:string,First_Name:string,Last_Name:string,...>

    >>> # Read table data
    >>> records_iter, offset = connector.read_table("Leads", {}, {})
    >>> for record in records_iter:
    ...     print(record)

For detailed setup instructions, see:
    - sources/zoho_crm/README.md - Connector documentation
    - sources/zoho_crm/configs/README.md - OAuth setup guide
"""

from typing import Iterator

from pyspark.sql.types import StructType

# Helper classes - each handles a specific concern
from ._auth_client import AuthClient          # OAuth & API requests
from ._metadata_manager import MetadataManager  # Module/field discovery
from ._schema_generator import SchemaGenerator  # PySpark schema generation
from ._data_reader import DataReader          # Data fetching & transformation


class LakeflowConnect:
    """
    Zoho CRM connector implementing the LakeflowConnect interface.

    This class serves as the main entry point for the Zoho CRM connector.
    It orchestrates the helper classes to provide a unified interface for:
    - Discovering available tables (list_tables)
    - Fetching table schemas (get_table_schema)
    - Reading table metadata for ingestion (read_table_metadata)
    - Reading table data with CDC support (read_table)

    The connector supports both standard CRM modules and "derived tables"
    that are constructed from various Zoho APIs (settings, subforms, junctions).

    Attributes:
        DERIVED_TABLES: Dictionary defining connector-specific derived tables.
            These are virtual tables that don't exist as standalone modules
            in Zoho CRM but are constructed by the connector.
        initial_load_start_date: Optional start date for historical data.
            If specified, only records modified after this date are synced
            on the initial load.

    Example:
        >>> connector = LakeflowConnect({
        ...     "client_id": "client_id",
        ...     "client_value_tmp": "client_secret",
        ...     "refresh_value_tmp": "refresh_token",
        ... })
        >>> tables = connector.list_tables()
        >>> schema = connector.get_table_schema("Leads", {})
    """

    # =========================================================================
    # DERIVED TABLES CONFIGURATION
    # =========================================================================
    # These tables don't exist as standalone modules in Zoho CRM but are
    # constructed by the connector from various APIs. They fall into three
    # categories:
    #
    # 1. Settings Tables: Organization-level data (Users, Roles, Profiles)
    #    accessed via the Settings API.
    #
    # 2. Subform Tables: Line-item data embedded within parent records
    #    (e.g., Quoted_Items within Quotes). Extracted during ingestion.
    #
    # 3. Junction Tables: Many-to-many relationships between modules
    #    (e.g., Campaigns linked to Leads). Accessed via Related Records API.
    # =========================================================================
    DERIVED_TABLES = {
        # Organization/Settings tables - use Settings API endpoints
        "Users": {
            "type": "settings",
            "endpoint": "/crm/v8/users",
            "data_key": "users",
        },
        "Roles": {
            "type": "settings",
            "endpoint": "/crm/v8/settings/roles",
            "data_key": "roles",
        },
        "Profiles": {
            "type": "settings",
            "endpoint": "/crm/v8/settings/profiles",
            "data_key": "profiles",
        },

        # Subform tables - line items extracted from parent records
        "Quoted_Items": {
            "type": "subform",
            "parent_module": "Quotes",
            "subform_field": "Quoted_Items",
        },
        "Ordered_Items": {
            "type": "subform",
            "parent_module": "Sales_Orders",
            "subform_field": "Ordered_Items",
        },
        "Invoiced_Items": {
            "type": "subform",
            "parent_module": "Invoices",
            "subform_field": "Invoiced_Items",
        },
        "Purchase_Items": {
            "type": "subform",
            "parent_module": "Purchase_Orders",
            "subform_field": "Purchased_Items",
        },

        # Junction/Related tables - many-to-many relationships
        "Campaigns_Leads": {
            "type": "related",
            "parent_module": "Campaigns",
            "related_module": "Leads",
        },
        "Campaigns_Contacts": {
            "type": "related",
            "parent_module": "Campaigns",
            "related_module": "Contacts",
        },
        "Contacts_X_Deals": {
            "type": "related",
            "parent_module": "Deals",
            "related_module": "Contact_Roles",
        },
    }

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Zoho CRM connector with connection-level options.

        This constructor sets up all helper components required for connector
        operations. Each helper is initialized with the dependencies it needs.

        Args:
            options: Connection configuration dictionary containing:
                - client_id (required): OAuth Client ID from Zoho API Console.
                - client_value_tmp (required): OAuth Client Secret.
                - refresh_value_tmp (required): Long-lived refresh token.
                - base_url (optional): Zoho accounts URL for OAuth.
                  Defaults to "https://accounts.zoho.com" (US region).
                  Regional URLs:
                    - US: https://accounts.zoho.com
                    - EU: https://accounts.zoho.eu
                    - IN: https://accounts.zoho.in
                    - AU: https://accounts.zoho.com.au
                    - CN: https://accounts.zoho.com.cn
                - initial_load_start_date (optional): ISO timestamp to limit
                  historical data. If omitted, syncs all available data.

        Raises:
            ValueError: If required credentials are missing.
            Exception: If OAuth token refresh fails on first request.

        Note:
            The refresh_token is a long-lived token obtained through the
            OAuth 2.0 authorization flow. See sources/zoho_crm/configs/README.md
            for setup instructions.
        """
        # Validate required credentials
        required_options = ["client_id", "client_value_tmp", "refresh_value_tmp"]
        missing = [opt for opt in required_options if not options.get(opt)]
        if missing:
            raise ValueError(
                f"Missing required connection options: {', '.join(missing)}. "
                f"Please provide: client_id, client_value_tmp (client secret), "
                f"and refresh_value_tmp (refresh token)."
            )

        # Initialize AuthClient - handles OAuth and API requests
        # This is the foundation that other components depend on for API access
        self._auth_client = AuthClient(options)

        # Store initial load start date for historical data limiting
        self.initial_load_start_date = options.get("initial_load_start_date")

        # Initialize MetadataManager - handles module/field discovery and caching
        # Uses AuthClient for API calls, provides metadata to other components
        self._metadata_manager = MetadataManager(self._auth_client, self.DERIVED_TABLES)

        # Initialize SchemaGenerator - generates PySpark schemas from metadata
        # Uses MetadataManager for field information
        self._schema_generator = SchemaGenerator(self._metadata_manager, self.DERIVED_TABLES)

        # Initialize DataReader - handles all data reading operations
        # Uses all other components: AuthClient for API calls, MetadataManager
        # for field info, SchemaGenerator for record normalization
        self._data_reader = DataReader(
            self._auth_client,
            self._metadata_manager,
            self._schema_generator,
            self.DERIVED_TABLES,
            self.initial_load_start_date,
        )

    def list_tables(self) -> list[str]:
        """
        List all tables (modules) supported by this connector.

        This method discovers available tables by:
        1. Querying the Zoho CRM Modules API for standard modules
        2. Adding connector-defined derived tables (settings, subforms, junctions)
        3. Filtering out unsupported or problematic modules

        Returns:
            Sorted list of table names available for ingestion.

        Example:
            >>> connector = LakeflowConnect(options)
            >>> tables = connector.list_tables()
            >>> print(tables[:5])
            ['Accounts', 'Campaigns', 'Cases', 'Contacts', 'Deals']

        Note:
            Results are cached after first call. The list includes both
            standard CRM modules (Leads, Contacts, etc.) and derived tables
            (Users, Quoted_Items, Campaigns_Leads, etc.).
        """
        return self._metadata_manager.list_tables()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """
        Get the PySpark schema for a table.

        This method generates a StructType schema by:
        1. For standard modules: Querying the Fields API and mapping types
        2. For derived tables: Using predefined schema definitions

        Args:
            table_name: Name of the table to get schema for.
            table_options: Table-specific options (currently unused but
                included for interface compatibility).

        Returns:
            PySpark StructType schema for the table.

        Example:
            >>> schema = connector.get_table_schema("Leads", {})
            >>> print(schema.simpleString())
            struct<id:string,First_Name:string,Last_Name:string,Email:string,...>

            >>> # Check schema fields
            >>> field_names = schema.fieldNames()
            >>> print("id" in field_names)
            True

        Note:
            The schema maps Zoho CRM types to PySpark types:
            - text, email, phone -> StringType
            - bigint, integer -> LongType
            - double, currency -> DoubleType
            - boolean -> BooleanType
            - datetime -> TimestampType
            - lookup -> StringType (stores ID)
            - multiselectpicklist -> ArrayType(StringType)
        """
        return self._schema_generator.get_table_schema(table_name, table_options)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """
        Get ingestion metadata for a table.

        This method returns information needed by the ingestion pipeline:
        - Primary keys for deduplication
        - Cursor field for incremental loading (CDC)
        - Ingestion type (cdc, snapshot, or append)

        Args:
            table_name: Name of the table to get metadata for.
            table_options: Table-specific options.

        Returns:
            Dictionary containing:
            - primary_keys (list[str]): Columns for record deduplication.
              Most tables use ["id"]. Junction tables use ["_junction_id"].
            - cursor_field (str, optional): Column for tracking changes.
              Usually "Modified_Time" for CDC tables.
            - ingestion_type (str): One of:
              - "cdc": Change Data Capture using cursor field
              - "snapshot": Full table refresh each sync
              - "append": Append-only (e.g., Attachments)

        Example:
            >>> metadata = connector.read_table_metadata("Leads", {})
            >>> print(metadata)
            {
                "primary_keys": ["id"],
                "cursor_field": "Modified_Time",
                "ingestion_type": "cdc"
            }

            >>> metadata = connector.read_table_metadata("Attachments", {})
            >>> print(metadata)
            {
                "primary_keys": ["id"],
                "ingestion_type": "append"
            }

        Raises:
            ValueError: If the table is not supported.
        """
        return self._metadata_manager.read_table_metadata(
            table_name, self._schema_generator.get_table_schema, table_options
        )

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a table with CDC support.

        This is the main data reading method. It:
        1. Fetches new/modified records since the cursor position
        2. Fetches deleted records for soft-delete handling
        3. Returns an iterator of all records

        Args:
            table_name: Name of the table to read from.
            start_offset: Offset dictionary containing:
                - cursor_position (str, optional): ISO timestamp of last sync.
                  If omitted, performs a full initial load.
            table_options: Table-specific options (e.g., filters).

        Returns:
            Tuple of (records_iterator, offset_dict):
            - records_iterator: Iterator yielding dict records.
              Each record contains field values normalized for Delta Lake.
            - offset_dict: Empty dict (cursor managed by pipeline).

        Example:
            >>> # Initial full load
            >>> records_iter, offset = connector.read_table("Leads", {}, {})
            >>> for record in records_iter:
            ...     print(record["id"], record["Last_Name"])

            >>> # Incremental load (CDC)
            >>> records_iter, offset = connector.read_table(
            ...     "Leads",
            ...     {"cursor_position": "2024-06-15T10:30:00Z"},
            ...     {}
            ... )

        Note:
            The returned iterator is lazy - records are fetched on-demand
            as you iterate, which is memory-efficient for large datasets.
        """
        return self._data_reader.read_table(table_name, start_offset, table_options)
