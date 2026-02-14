"""
Handler for Zoho CRM subform/line item tables.

Handles tables like Quoted_Items, Ordered_Items, Invoiced_Items that are
extracted from subform fields in parent records.

=============================================================================
NOTE ON INVENTORY/ORDER MANAGEMENT TABLES
=============================================================================

The following subform tables are DISABLED by default because they require
Zoho Inventory or Zoho Books integration (separate paid products):

    - Quoted_Items (parent: Quotes)
    - Ordered_Items (parent: Sales_Orders)
    - Invoiced_Items (parent: Invoices)
    - Purchase_Items (parent: Purchase_Orders)

Without these products enabled, the parent modules return HTTP 400 errors.

To enable these tables if you have Zoho Inventory/Books:
1. Uncomment the relevant entries in SUBFORM_TABLES below
2. Ensure your OAuth token has the appropriate scopes

See: https://www.zoho.com/inventory/api/v1/
=============================================================================
"""

import logging
from typing import Iterator, Optional

from pyspark.sql.types import StructType

from databricks.labs.community_connector.sources.zoho_crm.handlers.base import TableHandler
from databricks.labs.community_connector.sources.zoho_crm.zoho_types import LINE_ITEM_SCHEMA

logger = logging.getLogger(__name__)


# Configuration for subform tables
# NOTE: Inventory/Order Management tables are commented out - see module docstring
SUBFORM_TABLES: dict[str, dict] = {
    # Uncomment if you have Zoho Inventory or Zoho Books enabled:
    # "Quoted_Items": {
    #     "parent_module": "Quotes",
    #     "subform_field": "Quoted_Items",
    # },
    # "Ordered_Items": {
    #     "parent_module": "Sales_Orders",
    #     "subform_field": "Ordered_Items",
    # },
    # "Invoiced_Items": {
    #     "parent_module": "Invoices",
    #     "subform_field": "Invoiced_Items",
    # },
    # "Purchase_Items": {
    #     "parent_module": "Purchase_Orders",
    #     "subform_field": "Purchased_Items",
    # },
}


class SubformHandler(TableHandler):
    """
    Handler for Zoho CRM subform/line item tables.

    Subform tables are extracted from array fields within parent records.
    For example, Quoted_Items are extracted from Quotes.Quoted_Items.

    These tables:
    - Don't exist as standalone API endpoints
    - Are extracted by reading parent records and their subform fields
    - Use snapshot ingestion (no individual CDC tracking)
    - Include _parent_id and _parent_module for traceability
    """

    def __init__(self, client, module_handler=None) -> None:
        super().__init__(client)
        self._module_handler = module_handler
        self._schema_cache: dict[str, StructType] = {}

    @staticmethod
    def get_tables() -> dict[str, dict]:
        """Return configuration for all subform tables."""
        return SUBFORM_TABLES

    def get_schema(self, table_name: str, config: dict) -> StructType:
        """
        Get Spark schema for a subform table.

        All line item tables share the LINE_ITEM_SCHEMA since they have
        the same structure (product, quantity, pricing, etc.).

        Args:
            table_name: Name of the subform table
            config: Table configuration (unused for subforms)

        Returns:
            Spark StructType representing the line item schema
        """
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # All line item tables share the same schema
        self._schema_cache[table_name] = LINE_ITEM_SCHEMA
        return LINE_ITEM_SCHEMA

    def get_metadata(self, table_name: str, config: dict) -> dict:
        """
        Get ingestion metadata for a subform table.

        Subform tables use snapshot ingestion since individual items
        don't have their own modification timestamps.

        Args:
            table_name: Name of the subform table
            config: Table configuration (unused for subforms)

        Returns:
            Dictionary with primary_keys and ingestion_type='snapshot'
        """
        # Subforms use snapshot (no individual CDC tracking)
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
        Read records from a subform table by extracting from parent records.

        Iterates through all parent records and extracts items from the
        corresponding subform array field. Each item is enriched with
        _parent_id and _parent_module for traceability.

        Args:
            table_name: Name of the subform table
            config: Table configuration with parent_module and subform_field
            start_offset: Offset dictionary (unused - subforms use snapshot)

        Returns:
            Tuple of (records iterator, empty offset dict)
        """
        table_config = SUBFORM_TABLES.get(table_name, {})
        parent_module = table_config.get("parent_module", "")
        subform_field = table_config.get("subform_field", "")

        def records_generator():
            # Get field names for parent module
            field_names = self._get_parent_field_names(parent_module)

            params = {
                "sort_order": "asc",
                "sort_by": "Modified_Time",
            }
            if field_names:
                params["fields"] = ",".join(field_names)

            for parent_record in self.client.paginate(f"/crm/v8/{parent_module}", params=params):
                parent_id = parent_record.get("id")
                subform_items = parent_record.get(subform_field, [])

                if subform_items:
                    for item in subform_items:
                        item["_parent_id"] = parent_id
                        item["_parent_module"] = parent_module
                        yield item

        # Subforms use snapshot - no cursor tracking
        return records_generator(), {}

    def _get_parent_field_names(self, parent_module: str) -> list[str]:
        """
        Get field API names for a parent module.

        Used to request all fields when fetching parent records so that
        subform data is included in the response.

        Args:
            parent_module: Name of the parent Zoho CRM module

        Returns:
            List of field API names, or empty list if unavailable
        """
        if self._module_handler:
            fields = self._module_handler.get_fields(parent_module)
            return [f["api_name"] for f in fields] if fields else []
        return []
