from typing import Iterator

from ._auth_client import AuthClient
from ._metadata_manager import MetadataManager
from ._schema_generator import SchemaGenerator


class LakeflowConnect:
    """
    Zoho CRM connector for Lakeflow/Databricks.

    Supports:
    - Standard CRM modules (Leads, Contacts, Accounts, Deals, etc.)
    - Organization/Settings tables (Users, Roles, Profiles)
    - Subform/Line Item tables (Quoted_Items, Ordered_Items, Invoiced_Items, Purchase_Items)
    - Junction/Relationship tables (Campaigns_Leads, Campaigns_Contacts, Contacts_X_Deals)
    """

    # Derived tables that don't exist as standalone modules but we construct from other APIs
    DERIVED_TABLES = {
        # Organization/Settings tables - use different API endpoints
        "Users": {"type": "settings", "endpoint": "/crm/v8/users", "data_key": "users"},
        "Roles": {"type": "settings", "endpoint": "/crm/v8/settings/roles", "data_key": "roles"},
        "Profiles": {"type": "settings", "endpoint": "/crm/v8/settings/profiles", "data_key": "profiles"},
        # Subform tables - extracted from parent records
        "Quoted_Items": {"type": "subform", "parent_module": "Quotes", "subform_field": "Quoted_Items"},
        "Ordered_Items": {"type": "subform", "parent_module": "Sales_Orders", "subform_field": "Ordered_Items"},
        "Invoiced_Items": {"type": "subform", "parent_module": "Invoices", "subform_field": "Invoiced_Items"},
        "Purchase_Items": {"type": "subform", "parent_module": "Purchase_Orders", "subform_field": "Purchased_Items"},
        # Junction/Related tables - fetched via Related Records API
        "Campaigns_Leads": {"type": "related", "parent_module": "Campaigns", "related_module": "Leads"},
        "Campaigns_Contacts": {"type": "related", "parent_module": "Campaigns", "related_module": "Contacts"},
        "Contacts_X_Deals": {"type": "related", "parent_module": "Deals", "related_module": "Contact_Roles"},
    }

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initializes the Zoho CRM connector with connection-level options.

        Expected options:
            - client_value_temp: OAuth Client ID from Zoho API Console.
            - client_secret: OAuth Client Secret from Zoho API Console.
            - refresh_value_tmp: Long-lived refresh token obtained from OAuth flow.
            - base_url (optional): Zoho accounts URL for OAuth. Defaults to https://accounts.zoho.com.
              Examples: https://accounts.zoho.com (US), https://accounts.zoho.eu (EU),
                        https://accounts.zoho.in (IN), https://accounts.zoho.com.au (AU).
            - initial_load_start_date (optional): Starting point for the first sync. If omitted, syncs all historical data.

        Note: To obtain the refresh_token, follow the OAuth setup guide in
        sources/zoho_crm/configs/README.md or visit:
        https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html
        """
        # Initialize the authentication client with provided OAuth options.
        self._auth_client = AuthClient(options)

        # Set the optional initial load start date for historical data synchronization.
        # Set the optional initial load start date for historical data synchronization.
        self.initial_load_start_date = options.get("initial_load_start_date")

        # Initialize the metadata manager to handle module and field metadata operations.
        self._metadata_manager = MetadataManager(self._auth_client, self.DERIVED_TABLES)

        # Initialize the schema generator to manage PySpark schema definitions.
        self._schema_generator = SchemaGenerator(self._metadata_manager, self.DERIVED_TABLES)


        # Initialize the data reader to manage all data ingestion operations.
        self._data_reader = DataReader(self._auth_client, self._metadata_manager, self._schema_generator, self.DERIVED_TABLES, self.initial_load_start_date)


    








    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """
        Retrieves metadata for a specified Zoho CRM module or derived table.
        This method delegates the metadata retrieval operation to the `MetadataManager` instance.
        """
        return self._metadata_manager.read_table_metadata(table_name, self._schema_generator.get_table_schema, table_options)











