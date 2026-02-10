"""
Handler for Zoho CRM settings/organization tables.

Handles Users, Roles, and Profiles tables which use different API endpoints
than standard CRM modules.
"""

import logging
from typing import Iterator

from pyspark.sql.types import StructType, StructField, StringType

from databricks.labs.community_connector.sources.zoho_crm.handlers.base import TableHandler
from databricks.labs.community_connector.sources.zoho_crm.zoho_types import SETTINGS_SCHEMAS

logger = logging.getLogger(__name__)


# Configuration for settings tables
SETTINGS_TABLES = {
    "Users": {
        "endpoint": "/crm/v8/users",
        "data_key": "users",
        "supports_cdc": True,
    },
    "Roles": {
        "endpoint": "/crm/v8/settings/roles",
        "data_key": "roles",
        "supports_cdc": False,
    },
    "Profiles": {
        "endpoint": "/crm/v8/settings/profiles",
        "data_key": "profiles",
        "supports_cdc": False,
    },
}


class SettingsHandler(TableHandler):
    """
    Handler for Zoho CRM settings/organization tables.

    Settings tables:
    - Users: All users in the organization (requires ZohoCRM.users.READ scope)
    - Roles: User roles hierarchy
    - Profiles: Permission profiles

    These tables use different API endpoints than standard modules.
    """

    @staticmethod
    def get_tables() -> dict[str, dict]:
        """Return configuration for all settings tables."""
        return SETTINGS_TABLES

    def get_schema(self, table_name: str, config: dict) -> StructType:
        """
        Get Spark schema for a settings table.

        Settings tables have predefined schemas since they have fixed structures.

        Args:
            table_name: Name of the settings table (Users, Roles, or Profiles)
            config: Table configuration (unused for settings)

        Returns:
            Spark StructType representing the table schema
        """
        if table_name in SETTINGS_SCHEMAS:
            return SETTINGS_SCHEMAS[table_name]

        # Fallback minimal schema
        return StructType([StructField("id", StringType(), False)])

    def get_metadata(self, table_name: str, config: dict) -> dict:
        """
        Get ingestion metadata for a settings table.

        Users supports CDC via Modified_Time. Roles and Profiles use snapshot
        since they don't have modification timestamps.

        Args:
            table_name: Name of the settings table
            config: Table configuration (unused for settings)

        Returns:
            Dictionary with primary_keys, cursor_field (if CDC), and ingestion_type
        """
        table_config = SETTINGS_TABLES.get(table_name, {})

        if table_config.get("supports_cdc"):
            return {
                "primary_keys": ["id"],
                "cursor_field": "Modified_Time",
                "ingestion_type": "cdc",
            }

        return {
            "primary_keys": ["id"],
            "ingestion_type": "snapshot",
        }

    def read(
        self,
        table_name: str,
        config: dict,
        start_offset: dict,
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a settings table.

        Uses the appropriate Zoho settings API endpoint based on table type.
        Users endpoint requires the ZohoCRM.users.READ OAuth scope.

        Args:
            table_name: Name of the settings table
            config: Table configuration with endpoint and data_key
            start_offset: Offset dictionary (unused - settings use snapshot)

        Returns:
            Tuple of (records iterator, empty offset dict)
        """
        table_config = SETTINGS_TABLES.get(table_name, {})
        endpoint = table_config.get("endpoint", "")
        data_key = table_config.get("data_key", "data")

        def records_generator():
            params = {}
            if table_name == "Users":
                params["type"] = "AllUsers"

            yield from self.client.paginate(endpoint, params=params, data_key=data_key)

        # Settings tables use snapshot - no cursor tracking
        return records_generator(), {}
