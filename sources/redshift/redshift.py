"""
Amazon Redshift connector implementation using the Redshift Data API.

This connector enables reading data from Amazon Redshift clusters and serverless
workgroups using the AWS Redshift Data API (REST-based, asynchronous execution model).
"""

import time
from typing import Dict, List, Iterator, Any, Optional
import boto3
from botocore.exceptions import ClientError

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    BinaryType,
)


class LakeflowConnect:
    """
    Redshift connector using AWS Redshift Data API.
    
    Supports both provisioned clusters and serverless workgroups.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Redshift connector with AWS credentials and cluster/workgroup details.

        Expected options:
            - region (required): AWS region (e.g., 'us-east-1')
            - database (required): Database name
            - access_key_id (optional): AWS access key ID (uses default credentials if omitted)
            - secret_access_key (optional): AWS secret access key
            - session_token (optional): AWS session token (required for temporary credentials)
            - cluster_identifier (conditional): Redshift cluster identifier (for provisioned)
            - workgroup_name (conditional): Serverless workgroup name (for serverless)
            - db_user (optional): Database user for cluster (uses IAM if omitted)
            - secret_arn (optional): ARN of AWS Secrets Manager secret containing credentials
            - schema_filter (optional): Comma-separated list of schemas to include (default: all except system)
            - poll_interval (optional): Seconds between statement status polls (default: 2)
            - max_poll_attempts (optional): Maximum polling attempts (default: 300, ~10 minutes)
        """
        self.options = options

        # Required parameters
        self.region = options.get("region")
        self.database = options.get("database")
        
        if not self.region:
            raise ValueError("Redshift connector requires 'region' in options")
        if not self.database:
            raise ValueError("Redshift connector requires 'database' in options")

        # Cluster or workgroup (one required)
        self.cluster_identifier = options.get("cluster_identifier")
        self.workgroup_name = options.get("workgroup_name")
        
        if not self.cluster_identifier and not self.workgroup_name:
            raise ValueError(
                "Redshift connector requires either 'cluster_identifier' or 'workgroup_name'"
            )
        if self.cluster_identifier and self.workgroup_name:
            raise ValueError(
                "Redshift connector cannot specify both 'cluster_identifier' and 'workgroup_name'"
            )

        # Optional parameters
        self.db_user = options.get("db_user")
        self.secret_arn = options.get("secret_arn")
        self.schema_filter = self._parse_schema_filter(options.get("schema_filter", ""))
        self.poll_interval = int(options.get("poll_interval", "2"))
        self.max_poll_attempts = int(options.get("max_poll_attempts", "300"))

        # Store client configuration for lazy initialization
        # We don't create the boto3 client here because it contains thread locks
        # that cannot be pickled/serialized by Spark
        self._client = None
        self._client_kwargs = {"region_name": self.region}
        
        access_key_id = options.get("access_key_id")
        secret_access_key = options.get("secret_access_key")
        session_token = options.get("session_token")
        
        if access_key_id and secret_access_key:
            self._client_kwargs["aws_access_key_id"] = access_key_id
            self._client_kwargs["aws_secret_access_key"] = secret_access_key
            if session_token:
                self._client_kwargs["aws_session_token"] = session_token

    @property
    def client(self):
        """Lazily initialize the boto3 client on first access."""
        if self._client is None:
            self._client = boto3.client("redshift-data", **self._client_kwargs)
        return self._client

    def __getstate__(self):
        """Custom pickle method to exclude unpicklable boto3 client."""
        state = self.__dict__.copy()
        # Remove the unpicklable boto3 client
        state['_client'] = None
        return state

    def __setstate__(self, state):
        """Custom unpickle method to restore state without boto3 client."""
        self.__dict__.update(state)
        # Client will be lazily recreated when first accessed

    def _parse_schema_filter(self, schema_filter: str) -> Optional[List[str]]:
        """Parse comma-separated schema filter string."""
        if not schema_filter or schema_filter.strip() == "":
            return None
        return [s.strip() for s in schema_filter.split(",") if s.strip()]

    def _execute_sql(self, sql: str) -> str:
        """
        Execute a SQL statement asynchronously and return the statement ID.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            Statement ID for tracking execution
        """
        params = {
            "Database": self.database,
            "Sql": sql,
        }

        # Add cluster or workgroup
        if self.cluster_identifier:
            params["ClusterIdentifier"] = self.cluster_identifier
            if self.db_user:
                params["DbUser"] = self.db_user
        else:
            params["WorkgroupName"] = self.workgroup_name

        # Add secret ARN if provided
        if self.secret_arn:
            params["SecretArn"] = self.secret_arn

        try:
            response = self.client.execute_statement(**params)
            return response["Id"]
        except ClientError as e:
            raise RuntimeError(f"Failed to execute SQL statement: {e}") from e

    def _wait_for_statement(self, statement_id: str) -> Dict[str, Any]:
        """
        Poll statement status until completion (FINISHED, FAILED, or ABORTED).
        
        Args:
            statement_id: Statement ID to poll
            
        Returns:
            Final statement description
            
        Raises:
            RuntimeError: If statement fails or times out
        """
        for attempt in range(self.max_poll_attempts):
            try:
                response = self.client.describe_statement(Id=statement_id)
                status = response["Status"]

                if status == "FINISHED":
                    return response
                elif status == "FAILED":
                    error_msg = response.get("Error", "Unknown error")
                    raise RuntimeError(f"SQL statement failed: {error_msg}")
                elif status == "ABORTED":
                    raise RuntimeError("SQL statement was aborted")
                
                # Status is SUBMITTED, PICKED, or STARTED - continue polling
                time.sleep(self.poll_interval)
                
            except ClientError as e:
                raise RuntimeError(f"Failed to describe statement: {e}") from e

        raise RuntimeError(
            f"Statement timed out after {self.max_poll_attempts} polling attempts"
        )

    def _get_statement_results(self, statement_id: str) -> List[List[Dict[str, Any]]]:
        """
        Retrieve all results from a completed statement, handling pagination.
        
        Args:
            statement_id: Statement ID to retrieve results from
            
        Returns:
            List of records, where each record is a list of field values
        """
        records = []
        next_token = None

        try:
            while True:
                params = {"Id": statement_id}
                if next_token:
                    params["NextToken"] = next_token

                response = self.client.get_statement_result(**params)
                records.extend(response.get("Records", []))

                next_token = response.get("NextToken")
                if not next_token:
                    break

        except ClientError as e:
            raise RuntimeError(f"Failed to get statement results: {e}") from e

        return records

    def _execute_and_fetch(self, sql: str) -> List[List[Dict[str, Any]]]:
        """
        Execute SQL and fetch all results (convenience method).
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            List of records
        """
        statement_id = self._execute_sql(sql)
        self._wait_for_statement(statement_id)
        return self._get_statement_results(statement_id)

    def list_tables(self) -> List[str]:
        """
        List all tables in the database, optionally filtered by schema.
        
        Returns table names in format: "schema_name.table_name"
        Excludes system schemas (pg_catalog, information_schema, pg_internal).
        """
        # Build schema filter clause
        schema_filter_clause = ""
        if self.schema_filter:
            schema_list = ", ".join(f"'{s}'" for s in self.schema_filter)
            schema_filter_clause = f"AND table_schema IN ({schema_list})"
        
        sql = f"""
            SELECT 
                table_schema,
                table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
                AND table_type IN ('BASE TABLE', 'VIEW')
                {schema_filter_clause}
            ORDER BY table_schema, table_name
        """

        records = self._execute_and_fetch(sql)
        
        tables = []
        for record in records:
            schema = self._extract_value(record[0])
            table = self._extract_value(record[1])
            if schema and table:
                tables.append(f"{schema}.{table}")
        
        return tables

    def _extract_value(self, field_data: Dict[str, Any]) -> Any:
        """
        Extract typed value from Redshift Data API field response.
        
        Args:
            field_data: Field data from API response
            
        Returns:
            Extracted value (appropriate Python type) or None
        """
        if field_data.get("isNull", False):
            return None
        elif "booleanValue" in field_data:
            return field_data["booleanValue"]
        elif "longValue" in field_data:
            return field_data["longValue"]
        elif "doubleValue" in field_data:
            return field_data["doubleValue"]
        elif "stringValue" in field_data:
            return field_data["stringValue"]
        elif "blobValue" in field_data:
            return field_data["blobValue"]  # base64 encoded binary
        else:
            return None

    def _parse_table_name(self, table_name: str) -> tuple[str, str]:
        """
        Parse qualified table name into schema and table.
        
        Args:
            table_name: Table name in format "schema.table" or just "table"
            
        Returns:
            Tuple of (schema, table)
        """
        parts = table_name.split(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        else:
            return "public", parts[0]

    def _map_redshift_type_to_spark(
        self,
        redshift_type: str,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        char_max_length: Optional[int] = None,
    ) -> Any:
        """
        Map Redshift data type to Spark data type.
        
        Args:
            redshift_type: Redshift type name (e.g., 'integer', 'varchar', 'numeric')
            precision: Numeric precision (for DECIMAL/NUMERIC types)
            scale: Numeric scale (for DECIMAL/NUMERIC types)
            char_max_length: Maximum character length (for CHAR/VARCHAR types)
            
        Returns:
            Spark DataType instance
        """
        # Normalize type name to lowercase
        redshift_type = redshift_type.lower()

        # Integer types - prefer LongType to avoid overflow
        if redshift_type in ("smallint", "int2"):
            return LongType()
        elif redshift_type in ("integer", "int", "int4"):
            return LongType()
        elif redshift_type in ("bigint", "int8"):
            return LongType()

        # Decimal/numeric types
        elif redshift_type in ("numeric", "decimal"):
            # Use provided precision/scale or defaults
            p = precision if precision is not None else 18
            s = scale if scale is not None else 0
            return DecimalType(p, s)

        # Floating point types
        elif redshift_type in ("real", "float4"):
            return FloatType()
        elif redshift_type in ("double precision", "float8", "float"):
            return DoubleType()

        # Boolean type
        elif redshift_type in ("boolean", "bool"):
            return BooleanType()

        # String types
        elif redshift_type in ("character", "char", "bpchar"):
            return StringType()
        elif redshift_type in ("character varying", "varchar"):
            return StringType()
        elif redshift_type == "text":
            return StringType()

        # Date/time types
        elif redshift_type == "date":
            return DateType()
        elif redshift_type in ("timestamp", "timestamp without time zone"):
            return TimestampType()
        elif redshift_type in ("timestamptz", "timestamp with time zone"):
            return TimestampType()
        elif redshift_type in ("time", "time without time zone", "timetz", "time with time zone"):
            # Time types without date component - map to String
            return StringType()
        elif redshift_type == "interval":
            return StringType()

        # Binary type
        elif redshift_type == "varbyte":
            return BinaryType()

        # Special Redshift types
        elif redshift_type == "super":
            # Semi-structured JSON-like data
            return StringType()
        elif redshift_type in ("geometry", "geography"):
            # Spatial types
            return StringType()
        elif redshift_type == "hllsketch":
            # HyperLogLog sketch
            return StringType()

        # Default fallback
        else:
            # Unknown type - default to String for safety
            return StringType()

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table by querying information_schema.columns.
        
        Args:
            table_name: Table name in format "schema.table" or just "table"
            table_options: Additional options (currently unused)
            
        Returns:
            StructType representing the table schema
        """
        # Validate table exists
        available_tables = self.list_tables()
        if table_name not in available_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {', '.join(available_tables[:10])}..."
                if len(available_tables) > 10
                else f"Available tables: {', '.join(available_tables)}"
            )
        
        schema_name, table = self._parse_table_name(table_name)

        sql = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
                AND table_name = '{table}'
            ORDER BY ordinal_position
        """

        records = self._execute_and_fetch(sql)

        if not records:
            raise ValueError(
                f"Table '{table_name}' not found or has no columns in schema '{schema_name}'"
            )

        fields = []
        for record in records:
            col_name = self._extract_value(record[0])
            data_type = self._extract_value(record[1])
            is_nullable_str = self._extract_value(record[2])
            char_max_length = self._extract_value(record[3])
            numeric_precision = self._extract_value(record[4])
            numeric_scale = self._extract_value(record[5])

            # Convert is_nullable from 'YES'/'NO' to boolean
            is_nullable = is_nullable_str == "YES" if is_nullable_str else True

            # Map Redshift type to Spark type
            spark_type = self._map_redshift_type_to_spark(
                data_type, numeric_precision, numeric_scale, char_max_length
            )

            fields.append(StructField(col_name, spark_type, is_nullable))

        return StructType(fields)

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Fetch metadata for a table including primary keys and ingestion type.
        
        Args:
            table_name: Table name in format "schema.table" or just "table"
            table_options: Additional options (currently unused)
            
        Returns:
            Dictionary containing:
                - primary_keys: List of primary key column names
                - ingestion_type: "snapshot" (Redshift Data API does not support CDC)
        """
        # Validate table exists
        available_tables = self.list_tables()
        if table_name not in available_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {', '.join(available_tables[:10])}..."
                if len(available_tables) > 10
                else f"Available tables: {', '.join(available_tables)}"
            )
        
        schema_name, table = self._parse_table_name(table_name)

        # Query for primary key constraints
        sql = f"""
            SELECT 
                kcu.column_name,
                kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = '{schema_name}'
                AND tc.table_name = '{table}'
            ORDER BY kcu.ordinal_position
        """

        records = self._execute_and_fetch(sql)

        primary_keys = []
        for record in records:
            col_name = self._extract_value(record[0])
            if col_name:
                primary_keys.append(col_name)

        # If no primary key found, return empty list
        # Downstream systems can handle this appropriately
        metadata = {
            "primary_keys": primary_keys,
            "ingestion_type": "snapshot",  # Redshift Data API only supports full snapshots
        }

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read all records from a table.
        
        For Redshift, this performs a full table scan (snapshot ingestion).
        The start_offset is ignored since we do not support incremental reads via Data API.
        
        Args:
            table_name: Table name in format "schema.table" or just "table"
            start_offset: Ignored (snapshot ingestion only)
            table_options: Additional options:
                - limit: Optional row limit for testing (default: no limit)
                - where_clause: Optional WHERE clause to filter rows (future enhancement)
                
        Returns:
            Tuple of (iterator of records as dicts, final offset dict)
            The offset dict is empty for snapshot ingestion
        """
        # Validate table exists
        available_tables = self.list_tables()
        if table_name not in available_tables:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Available tables: {', '.join(available_tables[:10])}..."
                if len(available_tables) > 10
                else f"Available tables: {', '.join(available_tables)}"
            )
        
        schema_name, table = self._parse_table_name(table_name)
        full_table_name = f'"{schema_name}"."{table}"'

        # Build SQL query
        sql = f"SELECT * FROM {full_table_name}"

        # Add optional WHERE clause (table option)
        where_clause = table_options.get("where_clause")
        if where_clause:
            sql += f" WHERE {where_clause}"

        # Add optional LIMIT (for testing)
        limit = table_options.get("limit")
        if limit:
            try:
                limit_int = int(limit)
                sql += f" LIMIT {limit_int}"
            except ValueError:
                pass  # Ignore invalid limit

        # Execute query
        statement_id = self._execute_sql(sql)
        self._wait_for_statement(statement_id)

        # Get column metadata from the statement description
        try:
            result_response = self.client.get_statement_result(Id=statement_id)
            column_metadata = result_response.get("ColumnMetadata", [])
            column_names = [col["name"] for col in column_metadata]
        except ClientError as e:
            raise RuntimeError(f"Failed to get column metadata: {e}") from e

        # Generator to yield records with pagination
        def record_generator() -> Iterator[dict]:
            next_token = None
            
            while True:
                params = {"Id": statement_id}
                if next_token:
                    params["NextToken"] = next_token

                try:
                    response = self.client.get_statement_result(**params)
                except ClientError as e:
                    raise RuntimeError(f"Failed to get statement results: {e}") from e

                records = response.get("Records", [])
                
                for record in records:
                    # Convert record (list of field values) to dictionary
                    record_dict = {}
                    for idx, field_data in enumerate(record):
                        if idx < len(column_names):
                            col_name = column_names[idx]
                            value = self._extract_value(field_data)
                            record_dict[col_name] = value
                    
                    yield record_dict

                next_token = response.get("NextToken")
                if not next_token:
                    break

        # Return iterator and empty offset (snapshot ingestion)
        return record_generator(), {}
