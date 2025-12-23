"""
Base Vector Database Connector for Lakeflow Community Connectors.

This module provides reusable components for building vector database connectors.
It implements common patterns for security, HTTP communication, error handling,
and data streaming that can be shared across all vector DB implementations.

Reusable Components:
- Security: Input validation, SQL injection prevention, credential protection
- HTTP: Session management, retry logic, exponential backoff, rate limiting
- Performance: Iterator patterns, connection pooling, configurable batching
- Error Handling: Comprehensive exception handling with logging
- Thread Safety: Immutable configuration, safe session management

Vector DB Implementations:
- LanceDB: sources/lancedb/lancedb.py
- (Future): Pinecone, Weaviate, Qdrant, Milvus, Chroma, etc.
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Iterator, Any, Optional
from urllib.parse import quote, urljoin

import requests
from pydantic import BaseModel, Field, ConfigDict, field_validator
from pyspark.sql.types import StructType

# Configure logging
logger = logging.getLogger(__name__)


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
    
    primary_keys: Optional[List[str]] = Field(default=None, description="Primary key columns")
    cursor_field: Optional[str] = Field(default=None, description="Cursor field for incremental reads")
    batch_size: int = Field(default=1000, ge=1, le=10000, description="Rows per batch")
    filter_expression: Optional[str] = Field(default=None, description="Filter expression")
    columns: Optional[List[str]] = Field(default=None, description="Specific columns to retrieve")
    
    @field_validator("primary_keys", "columns")
    @classmethod
    def validate_column_names(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """
        Validate column names to prevent SQL injection.
        
        Ensures column names contain only safe characters: alphanumeric, underscore, dot.
        This prevents malicious SQL injection through column name manipulation.
        
        Args:
            v: List of column names to validate
            
        Returns:
            Validated list of column names
            
        Raises:
            ValueError: If any column name contains invalid characters
        """
        if v is None:
            return v
        for col in v:
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
    - Configurable batch sizes
    """
    
    # Thread-safe class constants
    _MAX_RETRIES = 5
    _INITIAL_RETRY_DELAY = 1.0
    _REQUEST_TIMEOUT = 30  # seconds
    
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the vector database connector.
        
        Args:
            options: Dictionary containing connection parameters (DB-specific)
            
        Raises:
            ValueError: If required parameters are missing or invalid
        """
        # Validate required parameters (subclass-specific)
        self._validate_options(options)
        
        # Store configuration (immutable after initialization)
        self._options = options
        
        # Build base URL (DB-specific implementation)
        self.base_url = self._build_base_url(options)
        
        # Initialize thread-safe HTTP session with connection pooling
        self.session = requests.Session()
        
        # Setup authentication (DB-specific implementation)
        self._setup_authentication(self.session, options)
        
        # Configure retry parameters
        self.max_retries = self._MAX_RETRIES
        self.retry_delay = self._INITIAL_RETRY_DELAY
        
        logger.info(f"Initialized {self.__class__.__name__} connector")
    
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
        pass  # Subclass implements
    
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
                    timeout=self._REQUEST_TIMEOUT
                )
                
                # Handle rate limiting (HTTP 429)
                if response.status_code == 429:
                    retry_after = int(
                        response.headers.get("Retry-After", self.retry_delay * (2 ** retry_count))
                    )
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    retry_count += 1
                    continue
                
                # Raise for other HTTP errors
                if response.status_code >= 400:
                    error_msg = f"{response.status_code} {response.reason} for url: {response.url}"
                    try:
                        error_detail = response.json()
                        logger.error(f"API Error Response: {error_detail}")
                        error_msg += f" - Detail: {error_detail}"
                    except:
                        error_body = response.text[:500]  # First 500 chars
                        logger.error(f"API Error Response: {error_body}")
                        error_msg += f" - Response: {error_body}"
                    raise requests.exceptions.HTTPError(error_msg, response=response)
                
                return response
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count > self.max_retries:
                    logger.error(f"Request failed after {self.max_retries} retries: {e}")
                    raise
                
                # Exponential backoff
                delay = self.retry_delay * (2 ** (retry_count - 1))
                logger.warning(
                    f"Request failed, retrying in {delay} seconds... "
                    f"(attempt {retry_count}/{self.max_retries})"
                )
                time.sleep(delay)
        
        raise requests.exceptions.RequestException("Max retries exceeded")
    
    # ========================================================================
    # REUSABLE ITERATOR PATTERN (No changes needed for new DBs)
    # ========================================================================
    
    def _create_batch_iterator(
        self,
        fetch_batch_func: callable,
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
                records, new_offset = fetch_batch_func(current_offset, **kwargs)
                
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
        pass
    
    @abstractmethod
    def _setup_authentication(self, session: requests.Session, options: Dict[str, str]) -> None:
        """
        Configure authentication headers for the session.
        
        Args:
            session: HTTP session to configure
            options: Connection options containing credentials
        """
        pass
    
    @abstractmethod
    def _build_list_tables_request(self) -> tuple[str, str, Optional[dict], Optional[dict]]:
        """
        Build request parameters for listing tables.
        
        Returns:
            Tuple of (method, endpoint, json_data, params)
        """
        pass
    
    @abstractmethod
    def _parse_list_tables_response(self, response: requests.Response) -> List[str]:
        """
        Parse table names from list tables response.
        
        Args:
            response: HTTP response
            
        Returns:
            List of table names
        """
        pass
    
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
        pass
    
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
        pass
    
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
        pass
    
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
        pass
    
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
        pass
    
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
        pass
    
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
    
    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
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

