import requests
import json
from pyspark.sql.types import *
from datetime import datetime
import time
import random
from typing import Dict, List, Tuple, Iterator, Any


class LakeflowConnect:
    def __init__(self, options: dict) -> None:
        self.access_token = options["access_token"]
        self.base_url = "https://api.hubapi.com"
        self.auth_header = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        # Cache for discovered schemas to avoid repeated API calls
        self._schema_cache = {}
        # Cache for table metadata
        self._metadata_cache = {}

        # Centralized object metadata configuration
        # supports_deletes: HubSpot only supports archived/deleted queries for core CRM objects
        self._object_config = {
            "contacts": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "lastmodifieddate",
                "associations": ["companies"],
                "supports_deletes": True,
            },
            "companies": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts"],
                "supports_deletes": True,
            },
            "deals": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "tickets"],
                "supports_deletes": True,
            },
            "tickets": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals"],
                "supports_deletes": True,
            },
            "calls": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": False,  # HubSpot doesn't support archived queries for calls
            },
            "emails": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
            "meetings": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": False,  # HubSpot doesn't support archived queries for meetings
            },
            "tasks": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
            "notes": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": ["contacts", "companies", "deals", "tickets"],
                "supports_deletes": True,
            },
            "deal_split": {
                "primary_keys": ["id"],
                "cursor_field": "updatedAt",
                "cursor_property_field": "hs_lastmodifieddate",
                "associations": [],
                "supports_deletes": False,
            },
        }

        # Default config for custom objects
        self._default_object_config = {
            "primary_keys": ["id"],
            "cursor_field": "updatedAt",
            "cursor_property_field": "hs_lastmodifieddate",
            "associations": [],
            "supports_deletes": False,  # Custom objects don't support archived queries by default
        }

    def list_tables(self) -> list[str]:
        """
        List available tables including standard CRM objects and custom objects.
        """
        # Standard HubSpot CRM objects
        standard_tables = [
            "contacts",
            "companies",
            "deals",
            "tickets",
            "calls",
            "emails",
            "meetings",
            "tasks",
            "notes",
        ]

        # Add dynamic discovery of custom objects
        try:
            custom_objects = self._discover_custom_objects()
            standard_tables.extend(custom_objects)
        except Exception as e:
            print(f"Warning: Could not discover custom objects: {e}")

        return standard_tables

    def _discover_custom_objects(self) -> List[str]:
        """
        Discover custom objects from HubSpot CRM schemas API
        """
        try:
            url = f"{self.base_url}/crm/v3/schemas"
            resp = requests.get(url, headers=self.auth_header)

            if resp.status_code != 200:
                return []

            data = resp.json()
            custom_objects = []

            # Extract custom object names
            for schema in data.get("results", []):
                object_type = schema.get("objectTypeId", "")
                name = schema.get("name", "")

                # Skip standard objects, only include custom ones
                if object_type not in ["0-1", "0-2", "0-3", "0-5"] and name:
                    custom_objects.append(name.lower())

            return custom_objects
        except Exception as e:
            print(f"Error discovering custom objects: {e}")
            return []

    def _get_object_config(self, table_name: str) -> Dict:
        """Get configuration for a specific object type"""
        return self._object_config.get(table_name, self._default_object_config)

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        Args:
            table_name: The name of the table to fetch the schema for.

        Returns:
            A StructType object representing the schema of the table.
        """
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(f"Unsupported table: {table_name}. Supported tables are: {supported_tables}")
        
        # Check cache first
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # Discover schema via API
        schema = self._discover_table_schema(table_name)

        # Cache the result
        self._schema_cache[table_name] = schema

        return schema

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.

        Args:
            table_name: The name of the table to fetch the metadata for.

        Returns:
            A dictionary containing the metadata of the table. It includes the following keys:
                - primary_keys: The name of the primary key columns of the table.
                - cursor_field: The name of the field to use as a cursor for incremental loading.
                - ingestion_type: The type of ingestion to use for the table. It should be one of:
                    - "snapshot": For snapshot loading.
                    - "cdc": capture incremental changes
                    - "append": incremental append
        """
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(f"Unsupported table: {table_name}. Supported tables are: {supported_tables}")
        
        # Check cache first
        if table_name in self._metadata_cache:
            return self._metadata_cache[table_name]

        # Get metadata from object configuration
        metadata = self._get_table_metadata(table_name)

        # Cache the result
        self._metadata_cache[table_name] = metadata

        return metadata

    def _discover_table_schema(self, table_name: str) -> StructType:
        """
        Discover table schema by calling HubSpot Properties API.

        Args:
            table_name: Name of the table/object to discover schema for

        Returns:
            StructType representing the table schema
        """
        # All CRM objects follow the same schema pattern
        return self._discover_crm_object_schema(table_name)

    def _get_table_metadata(self, table_name: str) -> dict:
        """
        Get metadata for a table based on object configuration.
        """
        config = self._get_object_config(table_name)

        # Get property names and cursor property field for API calls
        properties = self._get_object_properties(table_name)
        property_names = [prop["name"] for prop in properties]

        # Use cdc_with_deletes only for tables that support archived queries
        supports_deletes = config.get("supports_deletes", False)
        ingestion_type = "cdc_with_deletes" if supports_deletes else "cdc"

        return {
            "primary_keys": config["primary_keys"],
            "cursor_field": config["cursor_field"],
            "cursor_property_field": config["cursor_property_field"],
            "property_names": property_names,
            "associations": config.get("associations", []),
            "ingestion_type": ingestion_type,
        }

    def _discover_crm_object_schema(self, table_name: str) -> StructType:
        """
        Discover CRM object schema using HubSpot Properties API.
        Works for contacts, companies, deals, tickets, and custom objects.
        """
        # Get object configuration and properties
        config = self._get_object_config(table_name)
        properties = self._get_object_properties(table_name)

        # Build base schema fields (these are always present for CRM objects)
        base_fields = [
            StructField("id", StringType(), True),
            StructField("createdAt", StringType(), True),
            StructField("updatedAt", StringType(), True),
            StructField("archived", BooleanType(), True),
        ]

        # Add association fields based on configuration
        for association in config["associations"]:
            base_fields.append(StructField(association, ArrayType(StringType()), True))

        # Build nested properties schema based on API response
        properties_fields = []

        if isinstance(properties, list):
            for prop in properties:
                prop_name = prop.get("name", "")
                prop_type = prop.get("type", "string")

                # Map HubSpot property types to Spark types
                spark_type = self._map_hubspot_type_to_spark(prop_type)
                properties_fields.append(StructField(prop_name, spark_type, True))

        # Create nested properties StructType
        properties_struct = StructType(properties_fields) if properties_fields else StructType([])

        # Add properties as a nested field
        base_fields.append(StructField("properties", properties_struct, True))

        schema = StructType(base_fields)

        return schema

    def _get_associations_for_object(self, table_name: str) -> List[str]:
        """Get associations to include for the given object type"""
        config = self._get_object_config(table_name)
        return config["associations"]

    def _get_object_properties(self, object_type: str) -> List[Dict]:
        """
        Fetch object properties from HubSpot Properties API
        """
        url = f"{self.base_url}/properties/v2/{object_type}/properties"

        try:
            resp = requests.get(url, headers=self.auth_header)
            if resp.status_code != 200:
                raise Exception("API error: {resp.status_code} {resp.text}")

            return resp.json()
        except Exception as e:
            return {"error": f"Failed to get object properties: {str(e)}"}

    def _map_hubspot_type_to_spark(self, hubspot_type: str) -> DataType:
        """
        Map HubSpot property types to Spark DataTypes
        Following the requirement: strings -> StringType, integers -> LongType
        """
        type_mapping = {
            "string": StringType(),
            "enumeration": StringType(),
            "bool": BooleanType(),  # Keep boolean as boolean for better data handling
            "number": LongType(),
            "date": StringType(),  # Store as string to preserve ISO format
            "date-time": StringType(),  # Store as string to preserve ISO format
            "datetime": StringType(),
            "json": StringType(),
            "phone_number": StringType(),
            "object_coordinates": StringType(),
        }

        return type_mapping.get(hubspot_type.lower(), StringType())

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read data from HubSpot API.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing cursor information for incremental reads
            table_options: Additional options for reading

        Returns:
            Tuple of (records, new_offset)
        """
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(f"Unsupported table: {table_name}. Supported tables are: {supported_tables}")

        # Determine if this is an incremental read
        is_incremental = (
            start_offset is not None and start_offset.get("updatedAt") is not None
        )

        return self._read_data(table_name, start_offset, incremental=is_incremental, table_options=table_options)

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read deleted (archived) records from HubSpot API.

        HubSpot uses "archived" status to represent deleted records. This method
        fetches all archived records and filters them client-side for incremental reads.
        
        Internally uses archivedAt for filtering, but copies it to updatedAt in the
        output records and offset for consistency with the normal read flow cursor.

        Args:
            table_name: Name of the table to read deleted records from
            start_offset: Dictionary containing cursor information for incremental reads
                         (uses 'updatedAt' key, which stores archivedAt values)
            table_options: Additional options for reading

        Returns:
            Tuple of (deleted_records, new_offset) where updatedAt contains archivedAt value
        """
        supported_tables = self.list_tables()
        if table_name not in supported_tables:
            raise ValueError(f"Unsupported table: {table_name}. Supported tables are: {supported_tables}")

        # Get discovered properties (no associations needed for deletes)
        metadata = self.read_table_metadata(table_name, table_options)
        property_names = metadata.get("property_names", [])

        all_records = []
        after = None
        # Use updatedAt from offset (which stores archivedAt values for delete flow)
        checkpoint = start_offset.get("updatedAt") if start_offset else None
        latest_archived = checkpoint

        while True:
            # Fetch archived records using the Objects API with archived=true
            records, after = self._fetch_full_refresh_batch(
                table_name, property_names, associations=[], after=after, archived=True
            )

            if not records:
                break

            # Filter client-side for incremental deletes based on archivedAt (from raw records)
            if checkpoint:
                records = [r for r in records if r.get("archivedAt", "") > checkpoint]

            # Transform records
            transformed_records = self._transform_records(records, table_name)

            # Copy archivedAt to updatedAt for consistency with normal flow cursor
            for i, transformed in enumerate(transformed_records):
                archived_at = records[i].get("archivedAt")
                if archived_at:
                    transformed["updatedAt"] = archived_at
                    if not latest_archived or archived_at > latest_archived:
                        latest_archived = archived_at

            all_records.extend(transformed_records)

            if not after:
                break

            # Rate limiting
            time.sleep(0.1)

        # Return offset with updatedAt key for consistency with normal flow
        offset = {"updatedAt": latest_archived} if latest_archived else {}
        return all_records, offset

    def _read_data(
        self, table_name: str, start_offset: dict = None, incremental: bool = False,
        table_options: Dict[str, str] = None
    ):
        """Read active (non-archived) data from HubSpot API"""

        # Get discovered properties and object configuration
        metadata = self.read_table_metadata(table_name, table_options)
        property_names = metadata.get("property_names", [])
        cursor_property_field = metadata.get("cursor_property_field")
        associations = metadata.get("associations", [])

        all_records = []
        after = None
        checkpoint = start_offset.get("updatedAt") if start_offset else None
        latest_updated = checkpoint

        while True:
            if incremental:
                # Use search API for incremental reads
                records, after, updated_time = self._fetch_incremental_batch(
                    table_name,
                    property_names,
                    cursor_property_field,
                    start_offset,
                    after,
                )
                if updated_time and (
                    not latest_updated or updated_time > latest_updated
                ):
                    latest_updated = updated_time
            else:
                # Use objects API for full refresh
                records, after = self._fetch_full_refresh_batch(
                    table_name, property_names, associations, after, archived=False
                )

            if not records:
                break

            # Transform records
            transformed_records = self._transform_records(records, table_name)
            all_records.extend(transformed_records)

            # Update latest timestamp
            for record in transformed_records:
                updated_at = record.get("updatedAt")
                if updated_at and (not latest_updated or updated_at > latest_updated):
                    latest_updated = updated_at

            if not after:
                break

            # Rate limiting
            time.sleep(0.1)

        offset = {"updatedAt": latest_updated} if latest_updated else {}
        return all_records, offset

    def _fetch_full_refresh_batch(
        self,
        table_name: str,
        property_names: List[str],
        associations: List[str],
        after: str = None,
        archived: bool = False,
    ):
        """Fetch a batch of records using full refresh API"""
        archived_param = "true" if archived else "false"
        url = f"{self.base_url}/crm/v3/objects/{table_name}?limit=100&archived={archived_param}"

        if after:
            url += f"&after={after}"
        if property_names:
            url += f"&properties={','.join(property_names)}"
        if associations:
            url += f"&associations={','.join(associations)}"

        resp = requests.get(url, headers=self.auth_header)
        if resp.status_code != 200:
            raise Exception(
                f"HubSpot API error for {table_name}: {resp.status_code} {resp.text}"
            )

        data = resp.json()
        records = data.get("results", [])
        next_after = data.get("paging", {}).get("next", {}).get("after")

        return records, next_after

    def _fetch_incremental_batch(
        self,
        table_name: str,
        property_names: List[str],
        cursor_property_field: str,
        start_offset: dict,
        after: str = None,
    ):
        """Fetch a batch of records using incremental search API"""
        last_updated = start_offset.get("updatedAt", "1970-01-01T00:00:00.000Z")

        # Convert to milliseconds for HubSpot
        try:
            last_updated_ms = int(
                datetime.fromisoformat(last_updated.replace("Z", "+00:00")).timestamp()
                * 1000
            )
        except:
            last_updated_ms = 0

        search_body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": cursor_property_field,
                            "operator": "GTE",
                            "value": str(last_updated_ms),
                        }
                    ]
                }
            ],
            "sorts": [
                {"propertyName": cursor_property_field, "direction": "ASCENDING"}
            ],
            "limit": 100,
            "properties": property_names or [],
        }

        if after:
            search_body["after"] = after

        url = f"{self.base_url}/crm/v3/objects/{table_name}/search"
        resp = requests.post(url, headers=self.auth_header, json=search_body)

        if resp.status_code != 200:
            raise Exception(
                f"HubSpot API error for {table_name}: {resp.status_code} {resp.text}"
            )

        data = resp.json()
        records = data.get("results", [])
        next_after = data.get("paging", {}).get("next", {}).get("after")

        # Get latest update time from this batch
        latest_in_batch = last_updated
        for record in records:
            updated_at = record.get("updatedAt")
            if updated_at and updated_at > latest_in_batch:
                latest_in_batch = updated_at

        return records, next_after, latest_in_batch

    def _transform_records(self, records: List[Dict], table_name: str) -> List[Dict]:
        """Transform HubSpot records by flattening properties and associations"""
        return [self._transform_single_record(record, table_name) for record in records]

    def _transform_single_record(self, record: Dict, table_name: str) -> Dict:
        """Transform a single HubSpot record"""
        transformed_record = {}

        # Copy base fields
        for field in ["id", "createdAt", "updatedAt", "archived", "properties"]:
            if field in record:
                transformed_record[field] = record[field]

        # Handle associations
        transformed_record.update(self._extract_associations(record, table_name))

        return transformed_record

    def _extract_associations(self, record: Dict, table_name: str) -> Dict:
        """Extract association IDs from record"""
        associations_data = record.get("associations", {})
        config = self._get_object_config(table_name)
        expected_associations = config["associations"]
        result = {}

        for association_type in expected_associations:
            association_list = []
            if association_type in associations_data:
                assoc_data = associations_data[association_type]
                if isinstance(assoc_data, dict) and "results" in assoc_data:
                    association_list = [
                        item.get("id", "") for item in assoc_data["results"]
                    ]
                elif isinstance(assoc_data, list):
                    association_list = [
                        str(item) if not isinstance(item, dict) else item.get("id", "")
                        for item in assoc_data
                    ]

            result[association_type] = association_list

        return result

    def test_connection(self) -> dict:
        """Test the connection to HubSpot API"""
        try:
            url = f"{self.base_url}/crm/v3/objects/contacts?limit=1"
            resp = requests.get(url, headers=self.auth_header)

            if resp.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {resp.status_code} {resp.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
