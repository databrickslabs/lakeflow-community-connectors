"""
Table handlers for different Zoho CRM table types.

Each handler implements schema, metadata, and read operations
for a specific category of tables.
"""

from databricks.labs.community_connector.sources.zoho_crm.handlers.base import TableHandler
from databricks.labs.community_connector.sources.zoho_crm.handlers.module import ModuleHandler
from databricks.labs.community_connector.sources.zoho_crm.handlers.settings import (
    SettingsHandler,
    SETTINGS_TABLES,
)
from databricks.labs.community_connector.sources.zoho_crm.handlers.subform import (
    SubformHandler,
    SUBFORM_TABLES,
)
from databricks.labs.community_connector.sources.zoho_crm.handlers.related import (
    RelatedHandler,
    RELATED_TABLES,
)

__all__ = [
    "TableHandler",
    "ModuleHandler",
    "SettingsHandler",
    "SubformHandler",
    "RelatedHandler",
    "SETTINGS_TABLES",
    "SUBFORM_TABLES",
    "RELATED_TABLES",
]
