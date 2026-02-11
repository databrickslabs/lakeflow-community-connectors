"""
Zoho CRM connector for Lakeflow/Databricks.

This is the main connector class that orchestrates data extraction from Zoho CRM.
It delegates actual work to specialized handlers for different table types.

See handlers/ directory for implementation details.
"""

import logging
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.zoho_crm.zoho_client import ZohoAPIClient
from databricks.labs.community_connector.sources.zoho_crm.handlers import (
    ModuleHandler,
    SettingsHandler,
    SubformHandler,
    RelatedHandler,
)

logger = logging.getLogger(__name__)


class ZohoCRMLakeflowConnect(LakeflowConnect):
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
            - client_secret: OAuth Client Secret from Zoho API Console
            - refresh_token: Long-lived refresh token obtained from OAuth flow
            - base_url (optional): Zoho accounts URL for OAuth.
              Defaults to https://accounts.zoho.com
            - initial_load_start_date (optional): Starting point for the first sync.
        """
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        refresh_token = options.get("refresh_token")

        if not all([client_id, client_secret, refresh_token]):
            raise ValueError(
                "Zoho CRM connector requires 'client_id', 'client_secret', "
                "and 'refresh_token' in the UC connection"
            )

        self.initial_load_start_date = options.get("initial_load_start_date")
        accounts_url = options.get("base_url", "https://accounts.zoho.com")

        self._client = ZohoAPIClient(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            accounts_url=accounts_url,
        )

        self._module_handler = ModuleHandler(self._client)
        self._settings_handler = SettingsHandler(self._client)
        self._subform_handler = SubformHandler(self._client, self._module_handler)
        self._related_handler = RelatedHandler(self._client)

        self._derived_tables = self._build_derived_tables_map()

    def _build_derived_tables_map(self) -> dict[str, tuple[object, dict]]:
        """Build a mapping of derived table names to their handlers and configs."""
        derived = {}

        for name, config in SettingsHandler.get_tables().items():
            derived[name] = (self._settings_handler, {"type": "settings", **config})

        for name, config in SubformHandler.get_tables().items():
            derived[name] = (self._subform_handler, {"type": "subform", **config})

        for name, config in RelatedHandler.get_tables().items():
            derived[name] = (self._related_handler, {"type": "related", **config})

        return derived

    def _get_handler_and_config(self, table_name: str) -> tuple[object, dict]:
        """Get the appropriate handler and configuration for a table."""
        if table_name in self._derived_tables:
            return self._derived_tables[table_name]

        return self._module_handler, {"initial_load_start_date": self.initial_load_start_date}

    def list_tables(self) -> list[str]:
        """List names of all tables (modules) supported by this connector."""
        modules = self._module_handler.get_modules()
        table_names = [m["api_name"] for m in modules]

        table_names.extend(self._derived_tables.keys())

        return sorted(table_names)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Fetch the schema of a table dynamically from Zoho CRM."""
        self._validate_table_exists(table_name)
        handler, config = self._get_handler_and_config(table_name)
        return handler.get_schema(table_name, config)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Fetch the metadata of a table."""
        self._validate_table_exists(table_name)
        handler, config = self._get_handler_and_config(table_name)
        return handler.get_metadata(table_name, config)

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read records from a Zoho CRM table."""
        self._validate_table_exists(table_name)
        handler, config = self._get_handler_and_config(table_name)
        return handler.read(table_name, config, start_offset)

    def _validate_table_exists(self, table_name: str) -> None:
        """Validate that a table exists and is supported."""
        if table_name in self._derived_tables:
            return

        available_tables = self.list_tables()
        if table_name not in available_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {', '.join(available_tables)}"
            )
