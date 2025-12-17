import requests
import time
import json
from typing import Dict, List, Tuple, Iterator, Any, Optional
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DoubleType,
    ArrayType,
    TimestampType,
)


class LakeflowConnect:
    """
    Airtable connector for Lakeflow.
    
    Connects to an Airtable base and reads tables (records) using the Airtable REST API.
    Uses Personal Access Token (PAT) for authentication.
    """

    # Mapping from Airtable field types to PySpark types
    FIELD_TYPE_MAPPING = {
        # Text fields
        "singleLineText": StringType(),
        "multilineText": StringType(),
        "richText": StringType(),
        "email": StringType(),
        "url": StringType(),
        "phoneNumber": StringType(),
        # Numeric fields
        "number": DoubleType(),
        "currency": DoubleType(),
        "percent": DoubleType(),
        "duration": LongType(),
        "rating": LongType(),
        "count": LongType(),
        "autoNumber": LongType(),
        # Boolean
        "checkbox": BooleanType(),
        # Date/Time
        "date": StringType(),  # ISO 8601 date string
        "dateTime": StringType(),  # ISO 8601 datetime string
        "createdTime": StringType(),  # System field
        "lastModifiedTime": StringType(),  # User-configured field
        # Select fields
        "singleSelect": StringType(),
        "multipleSelects": ArrayType(StringType()),
        # Links and lookups
        "multipleRecordLinks": ArrayType(StringType()),
        "lookup": ArrayType(StringType()),
        "multipleLookupValues": ArrayType(StringType()),
        # Computed fields
        "formula": StringType(),  # Result type varies, store as string
        "rollup": StringType(),  # Result type varies, store as string
        # External sync
        "externalSyncSource": StringType(),
    }

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Airtable connector with API credentials.

        Args:
            options: Dictionary containing:
                - personal_access_token: Airtable Personal Access Token
                - base_id: The Airtable base ID (e.g., 'appxxxxxxxxxxxxxxx')
        """
        self.personal_access_token = options["personal_access_token"]
        self.base_id = options["base_id"]
        self.base_url = "https://api.airtable.com/v0"
        self.meta_url = "https://api.airtable.com/v0/meta"
        
        self._headers = {
            "Authorization": f"Bearer {self.personal_access_token}",
            "Content-Type": "application/json",
        }
        
        # Cache for table schemas
        self._table_schemas_cache: Optional[Dict[str, Any]] = None
        self._tables_cache: Optional[List[Dict[str, Any]]] = None

    def _get_tables_metadata(self) -> List[Dict[str, Any]]:
        """
        Fetch table metadata from Airtable Metadata API.
        
        Returns:
            List of table metadata dictionaries including fields.
        """
        if self._tables_cache is not None:
            return self._tables_cache
            
        url = f"{self.meta_url}/bases/{self.base_id}/tables"
        response = requests.get(url, headers=self._headers)
        
        if response.status_code != 200:
            raise Exception(
                f"Airtable API error fetching tables: {response.status_code} {response.text}"
            )
        
        data = response.json()
        self._tables_cache = data.get("tables", [])
        return self._tables_cache

    def _get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata for a specific table by name or ID.
        
        Args:
            table_name: Table name or table ID
            
        Returns:
            Table metadata dictionary
        """
        tables = self._get_tables_metadata()
        
        for table in tables:
            if table.get("name") == table_name or table.get("id") == table_name:
                return table
        
        raise ValueError(
            f"Table '{table_name}' not found in base '{self.base_id}'. "
            f"Available tables: {[t.get('name') for t in tables]}"
        )

    def _build_collaborator_schema(self) -> StructType:
        """Build schema for collaborator fields."""
        return StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("name", StringType(), True),
        ])

    def _build_attachment_schema(self) -> StructType:
        """Build schema for attachment fields."""
        return StructType([
            StructField("id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("size", LongType(), True),
            StructField("type", StringType(), True),
            StructField("width", LongType(), True),
            StructField("height", LongType(), True),
            StructField("thumbnails", StringType(), True),  # JSON string for nested thumbnails
        ])

    def _build_barcode_schema(self) -> StructType:
        """Build schema for barcode fields."""
        return StructType([
            StructField("text", StringType(), True),
            StructField("type", StringType(), True),
        ])

    def _get_spark_type_for_field(self, field: Dict[str, Any]) -> Any:
        """
        Convert an Airtable field definition to a PySpark data type.
        
        Args:
            field: Field definition from Airtable schema
            
        Returns:
            PySpark DataType
        """
        field_type = field.get("type", "singleLineText")
        
        # Handle collaborator fields
        if field_type in ["singleCollaborator", "createdBy", "lastModifiedBy"]:
            return self._build_collaborator_schema()
        
        if field_type == "multipleCollaborators":
            return ArrayType(self._build_collaborator_schema())
        
        # Handle attachments
        if field_type == "multipleAttachments":
            return ArrayType(self._build_attachment_schema())
        
        # Handle barcode
        if field_type == "barcode":
            return self._build_barcode_schema()
        
        # Handle button (typically not synced, but include for completeness)
        if field_type == "button":
            return StructType([
                StructField("label", StringType(), True),
                StructField("url", StringType(), True),
            ])
        
        # Use mapping for standard types
        return self.FIELD_TYPE_MAPPING.get(field_type, StringType())

    def list_tables(self) -> List[str]:
        """
        List available tables in the Airtable base.

        Returns:
            List of table names
        """
        tables = self._get_tables_metadata()
        return [table.get("name") for table in tables if table.get("name")]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the Spark schema for an Airtable table.

        Args:
            table_name: Name or ID of the table
            table_options: Additional options (not used currently)

        Returns:
            StructType representing the table schema
        """
        # Validate table exists
        table_metadata = self._get_table_metadata(table_name)
        
        fields = table_metadata.get("fields", [])
        
        # Build schema from Airtable fields
        schema_fields = [
            # System fields always present
            StructField("id", StringType(), False),  # Primary key, never null
            StructField("createdTime", StringType(), True),
        ]
        
        # Add user-defined fields
        for field in fields:
            field_name = field.get("name")
            if field_name:
                spark_type = self._get_spark_type_for_field(field)
                schema_fields.append(
                    StructField(field_name, spark_type, True)
                )
        
        return StructType(schema_fields)

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Get metadata for an Airtable table.

        Args:
            table_name: Name or ID of the table
            table_options: Additional options (not used currently)

        Returns:
            Dictionary with primary_keys and ingestion_type
        """
        # Validate table exists
        self._get_table_metadata(table_name)
        
        # Airtable uses snapshot ingestion as there's no native incremental support
        return {
            "primary_keys": ["id"],
            "ingestion_type": "snapshot",
        }

    def read_table(
        self, table_name: str, start_offset: Dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read records from an Airtable table.

        Args:
            table_name: Name or ID of the table to read
            start_offset: Offset to start reading from (not used for snapshot)
            table_options: Additional options:
                - view: Optional view name to filter/sort records
                - fields: Optional comma-separated list of field names to return
                - filter_by_formula: Optional Airtable formula for filtering

        Returns:
            Tuple of (records iterator, new offset)
        """
        # Validate table exists
        table_metadata = self._get_table_metadata(table_name)
        table_id_or_name = table_metadata.get("id", table_name)
        
        # Build query parameters
        params = {
            "pageSize": 100,  # Max allowed by Airtable
        }
        
        # Add optional parameters from table_options
        if table_options.get("view"):
            params["view"] = table_options["view"]
        
        if table_options.get("fields"):
            # fields should be a comma-separated string
            field_names = table_options["fields"].split(",")
            for field_name in field_names:
                params.setdefault("fields[]", []).append(field_name.strip())
        
        if table_options.get("filter_by_formula"):
            params["filterByFormula"] = table_options["filter_by_formula"]
        
        # Return iterator and empty offset (snapshot mode)
        return self._read_records_iterator(table_id_or_name, params), {}

    def _read_records_iterator(
        self, table_id_or_name: str, params: Dict[str, Any]
    ) -> Iterator[Dict]:
        """
        Generator that yields records from Airtable with pagination.
        
        Args:
            table_id_or_name: Table ID or name
            params: Query parameters
            
        Yields:
            Record dictionaries
        """
        url = f"{self.base_url}/{self.base_id}/{table_id_or_name}"
        offset = None
        
        while True:
            # Add offset for pagination
            request_params = params.copy()
            if offset:
                request_params["offset"] = offset
            
            # Make API request with rate limit handling
            response = self._make_request_with_retry(url, request_params)
            
            data = response.json()
            records = data.get("records", [])
            
            # Yield each record with flattened structure
            for record in records:
                yield self._transform_record(record)
            
            # Check for more pages
            offset = data.get("offset")
            if not offset:
                break

    def _make_request_with_retry(
        self, url: str, params: Dict[str, Any], max_retries: int = 5
    ) -> requests.Response:
        """
        Make a request with exponential backoff for rate limiting.
        
        Args:
            url: Request URL
            params: Query parameters
            max_retries: Maximum number of retries
            
        Returns:
            Response object
        """
        for attempt in range(max_retries):
            response = requests.get(url, headers=self._headers, params=params)
            
            if response.status_code == 200:
                return response
            
            if response.status_code == 429:
                # Rate limited - use Retry-After header or exponential backoff
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)
                else:
                    wait_time = (2 ** attempt) + (0.1 * attempt)  # Exponential backoff
                
                time.sleep(wait_time)
                continue
            
            # Other errors
            raise Exception(
                f"Airtable API error: {response.status_code} {response.text}"
            )
        
        raise Exception(f"Max retries exceeded for Airtable API request to {url}")

    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform an Airtable record to a flat structure.
        
        Airtable returns records as:
        {
            "id": "recXXX",
            "createdTime": "2024-01-01T00:00:00.000Z",
            "fields": {"field1": "value1", ...}
        }
        
        This transforms to:
        {
            "id": "recXXX",
            "createdTime": "2024-01-01T00:00:00.000Z",
            "field1": "value1",
            ...
        }
        
        Args:
            record: Raw Airtable record
            
        Returns:
            Transformed record dictionary
        """
        result = {
            "id": record.get("id"),
            "createdTime": record.get("createdTime"),
        }
        
        # Flatten fields into the result
        fields = record.get("fields", {})
        for field_name, field_value in fields.items():
            result[field_name] = field_value
        
        return result

    def test_connection(self) -> Dict[str, str]:
        """
        Test the connection to Airtable API.

        Returns:
            Dictionary with status and message
        """
        try:
            # Try to list bases to verify token works
            url = f"{self.meta_url}/bases"
            response = requests.get(url, headers=self._headers)

            if response.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {response.status_code} {response.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}

