"""
Table handlers for different Zoho CRM table types.

Each handler implements schema, metadata, and read operations
for a specific category of tables.
"""

from .base import TableHandler
from .module import ModuleHandler
from .settings import SettingsHandler
from .subform import SubformHandler
from .related import RelatedHandler

__all__ = [
    "TableHandler",
    "ModuleHandler",
    "SettingsHandler",
    "SubformHandler",
    "RelatedHandler",
]
