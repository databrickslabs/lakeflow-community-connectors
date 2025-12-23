"""
LanceDB Connector for Lakeflow Community Connectors (Refactored with Base Class).

This module provides a secure, thread-safe connector for LanceDB Cloud
that inherits from BaseVectorDBConnector for maximum code reuse.

Key Features:
- Inherits security, HTTP, and iterator patterns from base class
- Only implements LanceDB-specific API logic
- Apache Arrow IPC format support
- Automatic vector dimension detection
- Full table scans with dummy vectors

API Endpoints:
- List Tables: GET /v1/table/
- Get Schema: POST /v1/table/{name}/describe/
- Query Data: POST /v1/table/{name}/query/
"""

import json
import logging
import sys
import io
from pathlib import Path
from typing import Dict, List, Iterator, Any, Optional
from urllib.parse import quote

import requests
from pydantic import BaseModel, Field, ConfigDict
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    BinaryType,
    DateType,
    TimestampType,
    ArrayType,
)

# Import base connector
sys.path.insert(0, str(Path(__file__).parent.parent))
from interface.base_vector_connector import BaseVectorDBConnector, BaseTableOptions

logger = logging.getLogger(__name__)


class LanceDBTableOptions(BaseTableOptions):
    """
    LanceDB-specific table options extending base options.
    
    Adds vector database-specific parameters:
    - query_vector: For similarity search
    - use_full_scan: Enable full scans (generates dummy vector if needed)
    """
    query_vector: Optional[List[float]] = Field(
        default=None,
        description="Query vector for similarity search"
    )
    use_full_scan: bool = Field(
        default=True,
        description="Enable full scan mode (uses dummy vector if needed)"
    )


class LakeflowConnect(BaseVectorDBConnector):
    """
    LanceDB connector implementing BaseVectorDBConnector.
    
    Reuses from base class:
    - Security validation (SQL injection prevention)
    - HTTP retry logic with exponential backoff
    - Rate limiting handling
    - Iterator patterns
    - Error handling
    - Logging
    - Thread safety
    
    LanceDB-specific implementation:
    - API endpoints and authentication
    - Apache Arrow IPC parsing
    - Vector dimension detection
    - Schema mapping (Arrow → Spark)
    """
    
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize LanceDB connector.
        
        Args:
            options: Connection parameters:
                - api_key: LanceDB Cloud API key
                - project_name: Project/database name
                - region: Cloud region (e.g., 'us-east-1')
        """
        # Store LanceDB-specific options before calling super().__init__()
        self._project_name = options.get("project_name", "")
        self._region = options.get("region", "")
        self._api_key = options.get("api_key", "")
        
        # Call parent initializer (handles validation, session setup, etc.)
        super().__init__(options)
        
        logger.info(f"Initialized LanceDB connector for project {self._project_name}")
    
    # ========================================================================
    # VALIDATION (Overrides base class)
    # ========================================================================
    
    def _validate_options(self, options: Dict[str, str]) -> None:
        """Validate required LanceDB connection parameters."""
        if not options.get("api_key"):
            raise ValueError("Missing required parameter: api_key")
        if not options.get("project_name"):
            raise ValueError("Missing required parameter: project_name")
        if not options.get("region"):
            raise ValueError("Missing required parameter: region")
    
    # ========================================================================
    # BASE URL & AUTHENTICATION (Implements abstract methods)
    # ========================================================================
    
    def _build_base_url(self, options: Dict[str, str]) -> str:
        """
        Construct LanceDB Cloud base URL.
        
        Format: https://{project_name}.{region}.api.lancedb.com
        """
        project = self._sanitize_identifier(options["project_name"])
        region = self._sanitize_identifier(options["region"])
        return f"https://{project}.{region}.api.lancedb.com"
    
    def _setup_authentication(
        self, session: requests.Session, options: Dict[str, str]
    ) -> None:
        """
        Configure LanceDB authentication headers.
        
        LanceDB uses custom 'x-api-key' header.
        """
        session.headers.update({
            "x-api-key": options["api_key"],
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Lakeflow-LanceDB-Connector/2.0-Base"
        })
    
    # ========================================================================
    # LIST TABLES (Implements abstract methods)
    # ========================================================================
    
    def _build_list_tables_request(
        self
    ) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """Build request for listing tables."""
        return ("GET", "/v1/table/", None, {"limit": 100})
    
    def _parse_list_tables_response(self, response: requests.Response) -> List[str]:
        """
        Parse table names from LanceDB response.
        
        Handles pagination with page_token.
        """
        all_tables = []
        data = response.json()
        
        # Extract tables from first response
        tables = data.get("tables", [])
        all_tables.extend([t.get("name", t) if isinstance(t, dict) else t for t in tables])
        
        # Handle pagination
        page_token = data.get("page_token")
        while page_token:
            response = self._make_request(
                "GET", "/v1/table/", params={"limit": 100, "page_token": page_token}
            )
            data = response.json()
            tables = data.get("tables", [])
            all_tables.extend([t.get("name", t) if isinstance(t, dict) else t for t in tables])
            page_token = data.get("page_token")
        
        logger.info(f"Found {len(all_tables)} tables in project {self._project_name}")
        return all_tables
    
    # ========================================================================
    # SCHEMA (Implements abstract methods)
    # ========================================================================
    
    def _build_schema_request(
        self, table_name: str, table_options: Dict[str, str]
    ) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """Build request for retrieving table schema."""
        endpoint = f"/v1/table/{quote(table_name)}/describe/"
        return ("POST", endpoint, {}, None)
    
    def _parse_schema_response(
        self, response: requests.Response, table_name: str
    ) -> StructType:
        """
        Parse LanceDB schema and convert to Spark StructType.
        
        LanceDB returns Arrow schema format which we convert to Spark types.
        """
        data = response.json()
        schema = data.get("schema", {})
        fields = schema.get("fields", [])
        
        spark_fields = []
        for field in fields:
            field_name = field.get("name", "")
            field_type = field.get("type", {})
            nullable = field.get("nullable", True)
            
            spark_type = self._arrow_type_to_spark_type(field_type)
            spark_fields.append(StructField(field_name, spark_type, nullable))
        
        logger.info(f"Retrieved schema for table '{table_name}' with {len(spark_fields)} fields")
        return StructType(spark_fields)
    
    def _arrow_type_to_spark_type(self, arrow_type):
        """
        Convert Arrow type to Spark SQL type.
        
        Handles primitive types, lists, and nested structures.
        """
        # Handle string representation (e.g., "fixed_size_list<float32>[768]")
        if isinstance(arrow_type, str):
            arrow_type_lower = arrow_type.lower()
            
            if "fixed_size_list" in arrow_type_lower or "list" in arrow_type_lower:
                # Vector/array type - use array of strings as generic representation
                return ArrayType(StringType(), True)
            elif "string" in arrow_type_lower or "utf8" in arrow_type_lower:
                return StringType()
            elif "int64" in arrow_type_lower or "long" in arrow_type_lower:
                return LongType()
            elif "int32" in arrow_type_lower or "int" in arrow_type_lower:
                return IntegerType()
            elif "float64" in arrow_type_lower or "double" in arrow_type_lower:
                return DoubleType()
            elif "float32" in arrow_type_lower or "float" in arrow_type_lower:
                return FloatType()
            elif "bool" in arrow_type_lower:
                return BooleanType()
            elif "binary" in arrow_type_lower:
                return BinaryType()
            elif "date" in arrow_type_lower:
                return DateType()
            elif "timestamp" in arrow_type_lower:
                return TimestampType()
            else:
                return StringType()  # Default fallback
        
        # Handle dict representation
        if isinstance(arrow_type, dict):
            type_name = arrow_type.get("type", "")
            
            if type_name == "fixed_size_list" or type_name == "list":
                return ArrayType(StringType(), True)
            elif type_name == "string" or type_name == "utf8":
                return StringType()
            elif type_name == "int64" or type_name == "long":
                return LongType()
            elif type_name == "int32" or type_name == "int":
                return IntegerType()
            elif type_name == "float64" or type_name == "double":
                return DoubleType()
            elif type_name == "float32" or type_name == "float":
                return FloatType()
            elif type_name == "bool" or type_name == "boolean":
                return BooleanType()
            elif type_name == "binary":
                return BinaryType()
            elif type_name == "date":
                return DateType()
            elif type_name == "timestamp":
                return TimestampType()
            else:
                return StringType()
        
        return StringType()  # Default fallback
    
    # ========================================================================
    # METADATA (Implements abstract methods)
    # ========================================================================
    
    def _build_metadata_request(
        self, table_name: str, table_options: Dict[str, str]
    ) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """Build request for retrieving table metadata."""
        # LanceDB uses same endpoint as schema for metadata
        endpoint = f"/v1/table/{quote(table_name)}/describe/"
        return ("POST", endpoint, {}, None)
    
    def _parse_metadata_response(
        self, response: requests.Response, table_name: str
    ) -> Dict[str, Any]:
        """
        Parse metadata from LanceDB response.
        
        Returns standard metadata format for Lakeflow.
        """
        data = response.json()
        
        metadata = {
            "primary_keys": [],  # LanceDB doesn't expose primary keys in API
            "ingestion_type": "snapshot",  # Default for vector DBs
        }
        
        logger.info(f"Retrieved metadata for table '{table_name}'")
        return metadata
    
    # ========================================================================
    # QUERY DATA (Implements abstract methods)
    # ========================================================================
    
    def _build_query_request(
        self,
        table_name: str,
        offset: int,
        batch_size: int,
        filter_expr: Optional[str],
        columns: Optional[List[str]],
        cursor_value: Optional[Any],
        **kwargs
    ) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """
        Build request for querying table data.
        
        LanceDB requires a vector parameter for queries.
        """
        endpoint = f"/v1/table/{quote(table_name)}/query/"
        
        # Build query payload
        query_payload = {}
        
        # Get vector-specific options
        query_vector = kwargs.get("query_vector")
        use_full_scan = kwargs.get("use_full_scan", True)
        
        # Handle vector requirement
        if query_vector:
            query_payload["vector"] = query_vector
        elif use_full_scan:
            # Auto-detect vector dimension and create dummy vector
            vector_dim = self._get_vector_dimension(table_name)
            if vector_dim:
                query_payload["vector"] = [0.0] * vector_dim
            else:
                query_payload["vector"] = [0.0]  # Fallback
        
        # LanceDB uses 'k' for k-nearest neighbors
        query_payload["k"] = batch_size
        
        # Add filter if provided
        if filter_expr:
            query_payload["filter"] = filter_expr
        
        # Add cursor filter for incremental reads
        if cursor_value and kwargs.get("cursor_field"):
            cursor_filter = f"{kwargs['cursor_field']} > '{cursor_value}'"
            if "filter" in query_payload:
                query_payload["filter"] += f" AND {cursor_filter}"
            else:
                query_payload["filter"] = cursor_filter
        
        return ("POST", endpoint, query_payload, None)
    
    def _parse_query_response(self, response: requests.Response) -> List[dict]:
        """
        Parse records from LanceDB query response.
        
        LanceDB returns data in Apache Arrow IPC format.
        """
        # Check response format
        content_type = response.headers.get("Content-Type", "")
        
        if "arrow" in content_type.lower() or response.content.startswith(b"ARROW"):
            # Parse Apache Arrow format
            try:
                import pyarrow as pa
                
                # Try multiple parsing methods
                try:
                    reader = pa.ipc.open_stream(io.BytesIO(response.content))
                    table = reader.read_all()
                except:
                    try:
                        reader = pa.ipc.open_file(io.BytesIO(response.content))
                        table = reader.read_all()
                    except:
                        buf = pa.py_buffer(response.content)
                        reader = pa.ipc.open_stream(buf)
                        table = reader.read_all()
                
                records = table.to_pylist()
                return records
                
            except ImportError:
                raise ImportError("PyArrow is required: pip install pyarrow")
            except Exception as e:
                logger.error(f"Failed to parse Arrow format: {e}")
                raise
        else:
            # Try JSON format
            try:
                data = response.json()
                return data.get("data", [])
            except:
                logger.error(f"Unknown response format: {content_type}")
                return []
    
    def _get_vector_dimension(self, table_name: str) -> Optional[int]:
        """
        Detect vector dimension from table schema.
        
        Parses Arrow schema to find fixed_size_list dimension.
        """
        try:
            endpoint = f"/v1/table/{quote(table_name)}/describe/"
            response = self._make_request("POST", endpoint)
            data = response.json()
            
            schema = data.get("schema", {})
            fields = schema.get("fields", [])
            
            for field in fields:
                field_type = field.get("type", {})
                
                if isinstance(field_type, dict):
                    type_name = field_type.get("type", "")
                    if type_name == "fixed_size_list":
                        dimension = field_type.get("length", 0)
                        if dimension > 0:
                            logger.info(f"Detected vector dimension: {dimension}")
                            return dimension
            
            return None
        except Exception as e:
            logger.warning(f"Could not detect vector dimension: {e}")
            return None
    
    # ========================================================================
    # LAKEFLOW read_table (Implements using base class patterns)
    # ========================================================================
    
    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read data from LanceDB table.
        
        Uses base class iterator pattern with LanceDB-specific query logic.
        """
        table_name = self._sanitize_identifier(table_name)
        
        # Parse options
        options = LanceDBTableOptions(**table_options)
        
        # Prepare offsets
        current_offset = start_offset.get("offset", 0) if start_offset else 0
        cursor_value = start_offset.get("cursor_value") if start_offset else None
        
        # Build filter
        filter_expr = options.filter_expression
        
        # Create iterator with closure variables
        max_cursor = cursor_value
        
        def _iterator():
            nonlocal max_cursor
            
            # Fetch batch
            method, endpoint, json_data, params = self._build_query_request(
                table_name=table_name,
                offset=current_offset,
                batch_size=options.batch_size,
                filter_expr=filter_expr,
                columns=options.columns,
                cursor_value=cursor_value,
                cursor_field=options.cursor_field,
                query_vector=options.query_vector,
                use_full_scan=options.use_full_scan
            )
            
            response = self._make_request(method, endpoint, json_data, params)
            records = self._parse_query_response(response)
            
            # Yield records
            for record in records:
                # Track max cursor value
                if options.cursor_field and options.cursor_field in record:
                    cursor_val = record[options.cursor_field]
                    if max_cursor is None or cursor_val > max_cursor:
                        max_cursor = cursor_val
                
                yield record
        
        end_offset = {
            "offset": current_offset + options.batch_size,
            "cursor_value": max_cursor
        }
        
        return _iterator(), end_offset


# For backwards compatibility, export with original name
__all__ = ["LakeflowConnect", "LanceDBTableOptions"]

