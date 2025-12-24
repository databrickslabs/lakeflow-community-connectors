# ==============================================================================
# LanceDB Connector for Lakeflow Community Connectors
# ==============================================================================
# This module provides a secure, thread-safe connector for LanceDB Cloud
# with all base functionality inlined for simplified deployment.
#
# Key Features:
# - Security: Input validation, SQL injection prevention, credential protection
# - HTTP: Session management, retry logic, exponential backoff, rate limiting
# - Performance: Iterator patterns, connection pooling, configurable batching
# - LanceDB-specific: Apache Arrow IPC format support, vector dimension detection
# - Thread Safety: Immutable configuration, safe session management
#
# API Endpoints:
# - List Tables: GET /v1/table/
# - Get Schema: POST /v1/table/{name}/describe/
# - Query Data: POST /v1/table/{name}/query/
# ==============================================================================

import io
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Iterator, Any, Optional, Callable
from urllib.parse import quote, urljoin
import requests
from pydantic import BaseModel, Field, ConfigDict, field_validator
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

logger = logging.getLogger(__name__)


# ============================================================================
# BASE CLASSES (Shared Infrastructure)
# ============================================================================

class BaseTableOptions(BaseModel):
    """
    Base configuration options for reading from any vector database table.

    This model provides common validation and sanitization that applies across
    all vector database implementations. Subclasses can extend with DB-specific options.

    Security Features:
    - SQL injection prevention through column name validation
    - Resource protection through batch size limits
    - Type safety through Pydantic validation

    Attributes:
        primary_keys: List of column names that form the primary key
        cursor_field: Column name used for incremental reads
        batch_size: Number of rows to fetch per API request (1-10000)
        filter_expression: Optional filter expression (implementation-specific)
        columns: Optional list of specific columns to retrieve
    """

    model_config = ConfigDict(extra="allow")  # Allow DB-specific extensions

    primary_keys: Optional[List[str]] = Field(
        default=None, description="Primary key columns")
    cursor_field: Optional[str] = Field(
        default=None, description="Cursor field for incremental reads")
    batch_size: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Rows per batch")
    filter_expression: Optional[str] = Field(
        default=None, description="Filter expression")
    columns: Optional[List[str]] = Field(
        default=None, description="Specific columns to retrieve")

    @field_validator("primary_keys", "columns", mode="before")
    @classmethod
    def parse_and_validate_column_lists(cls, v):
        """
        Parse and validate column names (handles JSON strings from Databricks).

        Databricks sends list options as JSON strings, so we need to parse them.
        Also validates column names to prevent SQL injection.
        """
        if v is None:
            return v

        # If it's a string, try to parse it as JSON
        if isinstance(v, str):
            import json
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                # If it's not JSON, treat as single column name
                v = [v]

        # Now validate each column name
        if not isinstance(v, list):
            raise ValueError(f"Expected list of column names, got {type(v)}")

        for col in v:
            if not isinstance(col, str):
                raise ValueError(
                    f"Column name must be string, got {
                        type(col)}")
            if not col.replace("_", "").replace(".", "").isalnum():
                raise ValueError(
                    f"Invalid column name: {col}. "
                    "Only alphanumeric, underscore, and dot allowed."
                )
        return v

    @field_validator("cursor_field")
    @classmethod
    def validate_cursor_field(cls, v: Optional[str]) -> Optional[str]:
        """
        Validate cursor field name to prevent SQL injection.

        Args:
            v: Cursor field name to validate

        Returns:
            Validated cursor field name

        Raises:
            ValueError: If cursor field name contains invalid characters
        """
        if v is None:
            return v
        if not v.replace("_", "").replace(".", "").isalnum():
            raise ValueError(
                f"Invalid cursor field: {v}. "
                "Only alphanumeric, underscore, and dot allowed."
            )
        return v


class BaseVectorDBConnector(ABC):
    """
    Abstract base class for vector database connectors.

    This class provides reusable infrastructure for building vector database connectors:
    - Secure HTTP communication with retry logic
    - Input validation and sanitization
    - Thread-safe session management
    - Error handling and logging
    - Resource cleanup

    Subclasses must implement:
    - _build_base_url(): Construct DB-specific base URL
    - _setup_authentication(): Configure DB-specific auth headers
    - _build_list_tables_request(): Prepare table listing request
    - _parse_list_tables_response(): Extract table names from response
    - _build_schema_request(): Prepare schema retrieval request
    - _parse_schema_response(): Convert DB schema to Spark StructType
    - _build_metadata_request(): Prepare metadata retrieval request
    - _parse_metadata_response(): Extract metadata from response
    - _build_query_request(): Prepare data query request
    - _parse_query_response(): Extract records from response

    Thread Safety:
    - Configuration is immutable after initialization
    - HTTP session is thread-safe by design
    - No shared mutable state between method calls

    Performance:
    - Connection pooling via requests.Session
    - Iterator-based data streaming
    - Configurable batch sizes and retry behavior
    """

    def __init__(
        self,
        options: Dict[str, str],
        max_retries: int = 5,
        initial_retry_delay: float = 1.0,
        request_timeout: int = 30
    ) -> None:
        """
        Initialize the vector database connector.

        Args:
            options: Dictionary containing connection parameters (DB-specific)
            max_retries: Maximum number of retry attempts for failed requests (default: 5)
            initial_retry_delay: Initial delay in seconds between retries,
                               doubles with each attempt (default: 1.0)
            request_timeout: Request timeout in seconds (default: 30)

        Raises:
            ValueError: If required parameters are missing or invalid
        """
        # Validate required parameters (subclass-specific)
        self._validate_options(options)

        # Store configuration (immutable after initialization)
        self._options = options

        # Configure retry and timeout parameters (externalized for flexibility)
        self.max_retries = max_retries
        self.retry_delay = initial_retry_delay
        self.request_timeout = request_timeout

        # Build base URL (DB-specific implementation)
        self.base_url = self._build_base_url(options)

        # Initialize thread-safe HTTP session with connection pooling
        self.session = requests.Session()

        # Setup authentication (DB-specific implementation)
        self._setup_authentication(self.session, options)

        logger.info("Initialized %s connector", self.__class__.__name__)

    def __del__(self):
        """Clean up resources when connector is destroyed."""
        if hasattr(self, 'session'):
            self.session.close()
            logger.debug("Closed HTTP session")

    # ========================================================================
    # REUSABLE SECURITY METHODS (No changes needed for new DBs)
    # ========================================================================

    def _validate_options(self, options: Dict[str, str]) -> None:
        """
        Validate required connection options.

        Subclasses should override to add DB-specific validation.

        Args:
            options: Connection options dictionary

        Raises:
            ValueError: If required options are missing or invalid
        """
        # Subclass implements validation

    def _sanitize_identifier(self, identifier: str) -> str:
        """
        Sanitize identifiers to prevent injection attacks.

        Validates that identifiers (table names, project names, etc.) contain
        only safe characters to prevent URL injection and other security issues.

        Args:
            identifier: String identifier to sanitize

        Returns:
            Sanitized identifier

        Raises:
            ValueError: If identifier contains invalid characters
        """
        # Allow alphanumeric, hyphens, and underscores only
        if not identifier.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                f"Invalid identifier: {identifier}. "
                "Only alphanumeric, hyphen, and underscore allowed."
            )
        return identifier

    # ========================================================================
    # REUSABLE HTTP METHODS (No changes needed for new DBs)
    # ========================================================================

    def _make_request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make a secure HTTP request with retry logic and rate limiting.

        This method implements exponential backoff for transient failures and
        handles rate limiting gracefully. It sanitizes inputs and logs errors
        without exposing sensitive information.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., '/v1/table/')
            json_data: Optional JSON payload for POST requests
            params: Optional query parameters

        Returns:
            Response object from the API

        Raises:
            requests.exceptions.HTTPError: For non-retryable HTTP errors
            requests.exceptions.RequestException: For connection errors after max retries
        """
        url = urljoin(self.base_url, endpoint)
        retry_count = 0

        while retry_count <= self.max_retries:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    json=json_data,
                    params=params,
                    timeout=self.request_timeout
                )

                # Handle rate limiting (HTTP 429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get(
                        "Retry-After", self.retry_delay * (2 ** retry_count)))
                    logger.warning(
                        "Rate limited. Retrying after %d seconds...",
                        retry_after)
                    time.sleep(retry_after)
                    retry_count += 1
                    continue

                # Raise for other HTTP errors
                if response.status_code >= 400:
                    error_msg = f"{
                        response.status_code} {
                        response.reason} for url: {
                        response.url}"
                    try:
                        error_detail = response.json()
                        logger.error("API Error Response: %s", error_detail)
                        error_msg += f" - Detail: {error_detail}"
                    except Exception:  # pylint: disable=broad-except
                        error_body = response.text[:500]  # First 500 chars
                        logger.error("API Error Response: %s", error_body)
                        error_msg += f" - Response: {error_body}"
                    raise requests.exceptions.HTTPError(
                        error_msg, response=response)

                return response

            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count > self.max_retries:
                    logger.error(
                        "Request failed after %d retries: %s",
                        self.max_retries,
                        e)
                    raise

                # Exponential backoff
                delay = self.retry_delay * (2 ** (retry_count - 1))
                logger.warning(
                    "Request failed, retrying in %s seconds... (attempt %s/%s)",
                    delay, retry_count, self.max_retries
                )
                time.sleep(delay)

        raise requests.exceptions.RequestException("Max retries exceeded")

    # ========================================================================
    # REUSABLE ITERATOR PATTERN (No changes needed for new DBs)
    # ========================================================================

    def _create_batch_iterator(
        self,
        fetch_batch_func: Callable,
        initial_offset: dict,
        **kwargs
    ) -> tuple[Iterator[dict], dict]:
        """
        Create a memory-efficient iterator for batch data retrieval.

        This method implements the iterator pattern for streaming large datasets
        with minimal memory footprint. It calls the provided fetch function
        repeatedly until all data is retrieved.

        Args:
            fetch_batch_func: Function that fetches a batch of records
            initial_offset: Starting offset for pagination
            **kwargs: Additional arguments passed to fetch_batch_func

        Returns:
            Tuple of (iterator of records, final offset)
        """
        current_offset = initial_offset
        end_offset = initial_offset

        def _iterator():
            nonlocal current_offset, end_offset

            while True:
                # Fetch batch using provided function
                records, new_offset = fetch_batch_func(
                    current_offset, **kwargs)

                # Yield records
                for record in records:
                    yield record

                # Update offset
                end_offset = new_offset

                # Check if we're done (offset unchanged = no more data)
                if new_offset == current_offset or not records:
                    break

                current_offset = new_offset

        return _iterator(), end_offset

    # ========================================================================
    # ABSTRACT METHODS (Must be implemented by subclasses)
    # ========================================================================

    @abstractmethod
    def _build_base_url(self, options: Dict[str, str]) -> str:
        """
        Construct the base URL for API requests.

        Args:
            options: Connection options

        Returns:
            Base URL string (e.g., "https://api.vectordb.com")
        """

    @abstractmethod
    def _setup_authentication(
            self, session: requests.Session, options: Dict[str, str]) -> None:
        """
        Configure authentication headers for the session.

        Args:
            session: HTTP session to configure
            options: Connection options containing credentials
        """

    @abstractmethod
    def _build_list_tables_request(
            self) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """
        Build request parameters for listing tables.

        Returns:
            Tuple of (method, endpoint, json_data, params)
        """

    @abstractmethod
    def _parse_list_tables_response(
            self, response: requests.Response) -> List[str]:
        """
        Parse table names from list tables response.

        Args:
            response: HTTP response

        Returns:
            List of table names
        """

    @abstractmethod
    def _build_schema_request(
        self, table_name: str, table_options: Dict[str, str]
    ) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """
        Build request parameters for retrieving table schema.

        Args:
            table_name: Name of the table
            table_options: Table-specific options

        Returns:
            Tuple of (method, endpoint, json_data, params)
        """

    @abstractmethod
    def _parse_schema_response(
        self, response: requests.Response, table_name: str
    ) -> StructType:
        """
        Parse schema from response and convert to Spark StructType.

        Args:
            response: HTTP response
            table_name: Name of the table

        Returns:
            Spark StructType representing the table schema
        """

    @abstractmethod
    def _build_metadata_request(
        self, table_name: str, table_options: Dict[str, str]
    ) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """
        Build request parameters for retrieving table metadata.

        Args:
            table_name: Name of the table
            table_options: Table-specific options

        Returns:
            Tuple of (method, endpoint, json_data, params)
        """

    @abstractmethod
    def _parse_metadata_response(
        self, response: requests.Response, table_name: str
    ) -> Dict[str, Any]:
        """
        Parse metadata from response.

        Args:
            response: HTTP response
            table_name: Name of the table

        Returns:
            Dictionary with keys: primary_keys, cursor_field, ingestion_type
        """

    @abstractmethod
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
        Build request parameters for querying table data.

        Args:
            table_name: Name of the table
            offset: Pagination offset
            batch_size: Number of rows to fetch
            filter_expr: Optional filter expression
            columns: Optional list of columns to retrieve
            cursor_value: Optional cursor value for incremental reads
            **kwargs: Additional DB-specific parameters

        Returns:
            Tuple of (method, endpoint, json_data, params)
        """

    @abstractmethod
    def _parse_query_response(
        self, response: requests.Response
    ) -> List[dict]:
        """
        Parse records from query response.

        Args:
            response: HTTP response

        Returns:
            List of records as dictionaries
        """

    # ========================================================================
    # LAKEFLOW INTERFACE IMPLEMENTATION (Uses abstract methods above)
    # ========================================================================

    def list_tables(self) -> List[str]:
        """
        List all tables in the vector database.

        Returns:
            List of table names
        """
        method, endpoint, json_data, params = self._build_list_tables_request()
        response = self._make_request(method, endpoint, json_data, params)
        return self._parse_list_tables_response(response)

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        Args:
            table_name: Name of the table
            table_options: Table-specific options

        Returns:
            Spark StructType representing the schema
        """
        table_name = self._sanitize_identifier(table_name)
        method, endpoint, json_data, params = self._build_schema_request(
            table_name, table_options
        )
        response = self._make_request(method, endpoint, json_data, params)
        return self._parse_schema_response(response, table_name)

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Fetch the metadata of a table.

        Args:
            table_name: Name of the table
            table_options: Table-specific options

        Returns:
            Dictionary containing: primary_keys, cursor_field, ingestion_type
        """
        table_name = self._sanitize_identifier(table_name)
        method, endpoint, json_data, params = self._build_metadata_request(
            table_name, table_options
        )
        response = self._make_request(method, endpoint, json_data, params)
        return self._parse_metadata_response(response, table_name)

    def read_table(self,
                   table_name: str,
                   start_offset: dict,
                   table_options: Dict[str,
                                       str]) -> tuple[Iterator[dict],
                                                      dict]:
        """
        Read records from a table with pagination support.

        Args:
            table_name: Name of the table
            start_offset: Starting offset for pagination
            table_options: Table-specific options

        Returns:
            Tuple of (iterator of records, end offset)
        """
        # This method should be overridden by subclasses for DB-specific implementation
        # But provides a framework using the abstract methods
        raise NotImplementedError(
            "Subclass must implement read_table() using the provided abstract methods"
        )


# ============================================================================
# LANCEDB-SPECIFIC IMPLEMENTATION
# ============================================================================


class LanceDBTableOptions(BaseTableOptions):
    """
    LanceDB-specific table options extending base options.

    Supports LanceDB REST API query parameters:
    https://docs.lancedb.com/api-reference/rest/table/query-a-table

    Vector Search:
    - query_vector: Query vector for similarity search
    - use_full_scan: Enable full scans (generates dummy vector if needed)
    - vector_column: Name of vector column to search (default: auto-detect)
    - distance_type: Distance metric (e.g., "cosine", "l2", "dot")

    Search Optimization:
    - nprobes: Number of probes for IVF index (higher = more accurate, slower)
    - ef: Search effort parameter for HNSW index (higher = more accurate, slower)
    - refine_factor: Refine factor for search quality
    - fast_search: Use fast search mode
    - bypass_vector_index: Skip vector index (full scan)

    Filtering:
    - prefilter: Apply filter before vector search (default: true, recommended)
    - lower_bound: Lower bound for distance filtering
    - upper_bound: Upper bound for distance filtering

    Results:
    - with_row_id: Include _rowid column in results
    - offset: Skip first N results (for pagination)
    - version: Query specific table version
    """
    # Vector search parameters
    query_vector: Optional[List[float]] = Field(
        default=None,
        description="Query vector for similarity search"
    )
    use_full_scan: bool = Field(
        default=True,
        description="Enable full scan mode (generates dummy vector if needed)"
    )
    vector_column: Optional[str] = Field(
        default=None,
        description="Name of the vector column to search"
    )
    distance_type: Optional[str] = Field(
        default=None,
        description="Distance metric: cosine, l2, dot"
    )

    # Index optimization parameters
    nprobes: Optional[int] = Field(
        default=None,
        ge=0,
        description="Number of probes for IVF index (higher = more accurate)"
    )
    ef: Optional[int] = Field(
        default=None,
        ge=0,
        description="Search effort for HNSW index (higher = more accurate)"
    )
    refine_factor: Optional[int] = Field(
        default=None,
        ge=0,
        description="Refine factor for search quality"
    )
    fast_search: Optional[bool] = Field(
        default=None,
        description="Use fast search mode"
    )
    bypass_vector_index: Optional[bool] = Field(
        default=None,
        description="Bypass vector index (full scan)"
    )

    # Filtering parameters
    prefilter: Optional[bool] = Field(
        default=True,
        description="Apply filter before vector search (recommended for large datasets)"
    )
    lower_bound: Optional[float] = Field(
        default=None,
        description="Lower bound for distance filtering"
    )
    upper_bound: Optional[float] = Field(
        default=None,
        description="Upper bound for distance filtering"
    )

    # Result parameters
    with_row_id: Optional[bool] = Field(
        default=None,
        description="Include _rowid column in results"
    )
    offset: Optional[int] = Field(
        default=None,
        ge=0,
        description="Number of results to skip (for pagination)"
    )
    version: Optional[int] = Field(
        default=None,
        ge=0,
        description="Table version to query"
    )


class LakeflowConnect(BaseVectorDBConnector):
    """
    LanceDB Cloud Connector for Lakeflow Community Connectors.

    This connector provides secure, high-performance access to LanceDB Cloud
    instances, with built-in support for:
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

    def __init__(
        self,
        options: Dict[str, str],
        max_retries: int = 5,
        initial_retry_delay: float = 1.0,
        request_timeout: int = 30
    ) -> None:
        """
        Initialize LanceDB connector.

        Args:
            options: Connection parameters:
                - api_key: LanceDB Cloud API key
                - project_name: Project/database name
                - region: Cloud region (e.g., 'us-east-1')
            max_retries: Maximum number of retry attempts (default: 5)
            initial_retry_delay: Initial retry delay in seconds (default: 1.0)
            request_timeout: Request timeout in seconds (default: 30)
        """
        # Store LanceDB-specific options before calling super().__init__()
        self._project_name = options.get("project_name", "")
        self._region = options.get("region", "")
        self._api_key = options.get("api_key", "")

        # Call parent initializer (handles validation, session setup, etc.)
        super().__init__(options, max_retries, initial_retry_delay, request_timeout)

        logger.info(
            "Initialized LanceDB connector for project %s",
            self._project_name)

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

    def _parse_list_tables_response(
            self, response: requests.Response) -> List[str]:
        """
        Parse table names from LanceDB response.

        Handles pagination with page_token.
        """
        all_tables = []
        data = response.json()

        # Extract tables from first response
        tables = data.get("tables", [])
        all_tables.extend(
            [t.get("name", t) if isinstance(t, dict) else t for t in tables])

        # Handle pagination
        page_token = data.get("page_token")
        while page_token:
            response = self._make_request(
                "GET",
                "/v1/table/",
                params={
                    "limit": 100,
                    "page_token": page_token})
            data = response.json()
            tables = data.get("tables", [])
            all_tables.extend(
                [t.get("name", t) if isinstance(t, dict) else t for t in tables])
            page_token = data.get("page_token")

        logger.info(
            "Found %d tables in project %s",
            len(all_tables),
            self._project_name)
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

        logger.info(
            "Retrieved schema for table '%s' with %d fields",
            table_name,
            len(spark_fields))
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
                # Vector/array type - use array of strings as generic
                # representation
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
        self, table_name: str, table_options: Dict[str, str]  # pylint: disable=unused-argument
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
        # Verify response is valid
        _ = response.json()

        metadata = {
            "primary_keys": [],  # LanceDB doesn't expose primary keys in API
            "ingestion_type": "snapshot",  # Default for vector DBs
        }

        logger.info("Retrieved metadata for table '%s'", table_name)
        return metadata

    # ========================================================================
    # QUERY DATA (Implements abstract methods)
    # ========================================================================

    def _build_query_request(
        self,
        table_name: str,
        offset: int,  # pylint: disable=unused-argument
        batch_size: int,
        filter_expr: Optional[str],
        columns: Optional[List[str]],
        cursor_value: Optional[Any],
        **kwargs
    ) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """
        Build request for querying table data.

        LanceDB requires a vector parameter for queries.
        Supports full LanceDB REST API parameters:
        https://docs.lancedb.com/api-reference/rest/table/query-a-table
        """
        endpoint = f"/v1/table/{quote(table_name)}/query/"

        # Build query payload
        query_payload = {}

        # Get LanceDB-specific options from kwargs
        query_vector = kwargs.get("query_vector")
        use_full_scan = kwargs.get("use_full_scan", True)
        vector_column = kwargs.get("vector_column")
        distance_type = kwargs.get("distance_type")
        nprobes = kwargs.get("nprobes")
        ef = kwargs.get("ef")
        refine_factor = kwargs.get("refine_factor")
        fast_search = kwargs.get("fast_search")
        bypass_vector_index = kwargs.get("bypass_vector_index")
        prefilter = kwargs.get("prefilter")
        lower_bound = kwargs.get("lower_bound")
        upper_bound = kwargs.get("upper_bound")
        with_row_id = kwargs.get("with_row_id")
        result_offset = kwargs.get("result_offset")
        version = kwargs.get("version")

        # Handle vector requirement
        if query_vector:
            query_payload["vector"] = {"single_vector": query_vector}
        elif use_full_scan:
            # Auto-detect vector dimension and create dummy vector
            vector_dim = self._get_vector_dimension(table_name)
            if vector_dim:
                query_payload["vector"] = {"single_vector": [0.0] * vector_dim}
            else:
                query_payload["vector"] = {"single_vector": [0.0]}  # Fallback

        # LanceDB uses 'k' for k-nearest neighbors
        query_payload["k"] = batch_size

        # Add optional vector search parameters
        if vector_column:
            query_payload["vector_column"] = vector_column
        if distance_type:
            query_payload["distance_type"] = distance_type
        if nprobes is not None:
            query_payload["nprobes"] = nprobes
        if ef is not None:
            query_payload["ef"] = ef
        if refine_factor is not None:
            query_payload["refine_factor"] = refine_factor
        if fast_search is not None:
            query_payload["fast_search"] = fast_search
        if bypass_vector_index is not None:
            query_payload["bypass_vector_index"] = bypass_vector_index

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

        # Add filtering parameters
        if prefilter is not None:
            query_payload["prefilter"] = prefilter
        if lower_bound is not None:
            query_payload["lower_bound"] = lower_bound
        if upper_bound is not None:
            query_payload["upper_bound"] = upper_bound

        # Add result parameters
        if with_row_id is not None:
            query_payload["with_row_id"] = with_row_id
        if result_offset is not None:
            query_payload["offset"] = result_offset
        if version is not None:
            query_payload["version"] = version

        # ✅ Add column projection for performance
        # https://docs.lancedb.com/api-reference/rest/table/query-a-table
        # columns object: {column_names: [...]} or {column_aliases: {...}}
        if columns:
            query_payload["columns"] = {"column_names": columns}
            logger.debug(
                "Requesting specific columns from LanceDB: %s",
                columns)

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
                except Exception:  # pylint: disable=broad-except
                    try:
                        reader = pa.ipc.open_file(io.BytesIO(response.content))
                        table = reader.read_all()
                    except Exception:  # pylint: disable=broad-except
                        buf = pa.py_buffer(response.content)
                        reader = pa.ipc.open_stream(buf)
                        table = reader.read_all()

                records = table.to_pylist()
                return records

            except ImportError as exc:
                raise ImportError(
                    "PyArrow is required: pip install pyarrow") from exc
            except Exception as e:
                logger.error("Failed to parse Arrow format: %s", e)
                raise
        else:
            # Try JSON format
            try:
                data = response.json()
                return data.get("data", [])
            except Exception:  # pylint: disable=broad-except
                logger.error("Unknown response format: %s", content_type)
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
                            logger.info(
                                "Detected vector dimension: %d", dimension)
                            return dimension

            return None
        except Exception as e:
            logger.warning("Could not detect vector dimension: %s", e)
            return None

    # ========================================================================
    # LAKEFLOW read_table (Implements using base class patterns)
    # ========================================================================

    def read_table(self,
                   table_name: str,
                   start_offset: dict,
                   table_options: Dict[str,
                                       str]) -> tuple[Iterator[dict],
                                                      dict]:
        """
        Read data from LanceDB table.

        Uses base class iterator pattern with LanceDB-specific query logic.
        """
        table_name = self._sanitize_identifier(table_name)

        # Parse options
        options = LanceDBTableOptions(**table_options)

        # Prepare offsets
        current_offset = start_offset.get("offset", 0) if start_offset else 0
        cursor_value = start_offset.get(
            "cursor_value") if start_offset else None

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
                # Vector search parameters
                query_vector=options.query_vector,
                use_full_scan=options.use_full_scan,
                vector_column=options.vector_column,
                distance_type=options.distance_type,
                # Index optimization parameters
                nprobes=options.nprobes,
                ef=options.ef,
                refine_factor=options.refine_factor,
                fast_search=options.fast_search,
                bypass_vector_index=options.bypass_vector_index,
                # Filtering parameters
                prefilter=options.prefilter,
                lower_bound=options.lower_bound,
                upper_bound=options.upper_bound,
                # Result parameters
                with_row_id=options.with_row_id,
                result_offset=options.offset,
                version=options.version
            )

            response = self._make_request(method, endpoint, json_data, params)
            records = self._parse_query_response(response)

            # Fallback: Filter columns post-fetch if API didn't honor the request
            # This happens if LanceDB API doesn't support column projection yet
            # Performance note: This downloads ALL columns, then filters
            # in-memory
            if options.columns:
                # Check if filtering is needed (API may have already filtered)
                if records and not all(
                        k in options.columns for k in records[0].keys()):
                    logger.debug(
                        "Applying post-fetch column filtering (API didn't support projection)")
                    filtered_records = []
                    for record in records:
                        filtered_record = {
                            k: v for k, v in record.items() if k in options.columns}
                        filtered_records.append(filtered_record)
                    records = filtered_records
                else:
                    logger.debug(
                        "API returned only requested columns (projection supported)")

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
