"""
Handler for Zoho CRM junction/related record tables.

Handles tables like Campaigns_Leads, Campaigns_Contacts, Contacts_X_Deals
that represent many-to-many relationships between modules.
"""

import logging
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from .base import TableHandler
from ..zoho_types import get_related_table_schema, RELATED_MODULE_API_FIELDS

logger = logging.getLogger(__name__)


# Configuration for related/junction tables
RELATED_TABLES = {
    "Campaigns_Leads": {
        "parent_module": "Campaigns",
        "related_module": "Leads",
    },
    "Campaigns_Contacts": {
        "parent_module": "Campaigns",
        "related_module": "Contacts",
    },
    "Contacts_X_Deals": {
        "parent_module": "Deals",
        "related_module": "Contact_Roles",
    },
}


class RelatedHandler(TableHandler):
    """
    Handler for Zoho CRM junction/related record tables.
    
    Junction tables represent many-to-many relationships by fetching
    related records for each parent record via the Related Records API.
    
    These tables:
    - Use the Related Records API (/crm/v8/{parent}/{id}/{related})
    - Include junction metadata (_junction_id, _parent_id, _parent_module)
    - Use snapshot ingestion (relationships can change without timestamp)
    """

    @staticmethod
    def get_tables() -> dict[str, dict]:
        """Return configuration for all related tables."""
        return RELATED_TABLES

    def get_schema(self, table_name: str, config: dict) -> StructType:
        """
        Get Spark schema for a junction table.
        
        Junction table schemas include standard junction metadata fields
        (_junction_id, _parent_id, _parent_module) plus related module fields.
        
        Args:
            table_name: Name of the junction table
            config: Table configuration (unused for junction tables)
        
        Returns:
            Spark StructType representing the junction table schema
        """
        table_config = RELATED_TABLES.get(table_name, {})
        related_module = table_config.get("related_module", "")
        return get_related_table_schema(related_module)

    def get_metadata(self, table_name: str, config: dict) -> dict:
        """
        Get ingestion metadata for a junction table.
        
        Junction tables use snapshot ingestion with a composite key since
        relationships can be created/deleted without timestamps.
        
        Args:
            table_name: Name of the junction table
            config: Table configuration (unused for junction tables)
        
        Returns:
            Dictionary with primary_keys=['_junction_id'] and ingestion_type='snapshot'
        """
        # Junction tables use snapshot with composite key
        return {
            "primary_keys": ["_junction_id"],
            "ingestion_type": "snapshot",
        }

    def read(
        self,
        table_name: str,
        config: dict,
        start_offset: dict,
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a junction table.
        
        Iterates through all parent records and fetches their related records
        using the Zoho CRM Related Records API. Each junction record includes
        the related record data plus metadata (_junction_id, _parent_id, _parent_module).
        
        Args:
            table_name: Name of the junction table
            config: Table configuration with parent_module and related_module
            start_offset: Offset dictionary (unused - junction tables use snapshot)
        
        Returns:
            Tuple of (records iterator, empty offset dict)
        """
        table_config = RELATED_TABLES.get(table_name, {})
        parent_module = table_config.get("parent_module", "")
        related_module = table_config.get("related_module", "")

        # Get API fields for related module
        related_fields = RELATED_MODULE_API_FIELDS.get(related_module, "id,name")

        def records_generator():
            # First, collect all parent IDs
            parent_ids = list(self._get_parent_ids(parent_module))

            # Fetch related records for each parent
            for parent_id in parent_ids:
                related_records = self._get_related_records(
                    parent_module, parent_id, related_module, related_fields
                )
                for record in related_records:
                    record["_junction_id"] = f"{parent_id}_{record.get('id')}"
                    record["_parent_id"] = parent_id
                    record["_parent_module"] = parent_module
                    yield record

        # Junction tables use snapshot - no cursor tracking
        return records_generator(), {}

    def _get_parent_ids(self, parent_module: str) -> Iterator[str]:
        """
        Get all record IDs from a parent module.
        
        Args:
            parent_module: Name of the parent Zoho CRM module
        
        Yields:
            Record IDs as strings
        """
        params = {"fields": "id"}
        for record in self.client.paginate(f"/crm/v8/{parent_module}", params=params):
            if record.get("id"):
                yield record["id"]

    def _get_related_records(
        self,
        parent_module: str,
        parent_id: str,
        related_module: str,
        fields: str,
    ) -> Iterator[dict]:
        """
        Get related records for a specific parent record.
        
        Uses the Zoho CRM Related Records API to fetch records linked
        to a parent record via a many-to-many relationship.
        
        Args:
            parent_module: Name of the parent module (e.g., "Campaigns")
            parent_id: ID of the parent record
            related_module: Name of the related module (e.g., "Leads")
            fields: Comma-separated field names to retrieve
        
        Yields:
            Related record dictionaries
        """
        endpoint = f"/crm/v8/{parent_module}/{parent_id}/{related_module}"
        params = {"fields": fields}

        try:
            yield from self.client.paginate(endpoint, params=params)
        except requests.exceptions.HTTPError as e:
            # 204/400/404 means no related records - not an error
            if e.response.status_code in (204, 400, 404):
                return
            raise
