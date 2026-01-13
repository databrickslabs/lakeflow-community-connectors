"""
Petrinex Connector for LakeFlow
================================
Provides access to Alberta Petrinex volumetric production data.

This connector implements incremental loading with configurable date filtering
to efficiently handle large volumetric datasets.

Connection Configuration:
- No authentication required (public data)

Table Configuration:
- updated_after (optional): Load files updated after this date (YYYY-MM-DD)
- from_date (optional): Load all production data from this date onwards (YYYY-MM-DD)
- request_timeout_s (optional): HTTP request timeout in seconds (default: 60)
"""

from typing import Iterator, Dict, List
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timedelta


# ============================================================================
# Constants
# ============================================================================

SUPPORTED_TABLES = ["volumetrics"]
DEFAULT_HISTORICAL_MONTHS = 6  # 6 months of history by default
DEFAULT_TIMEOUT_S = 60


# ============================================================================
# Main Connector Class
# ============================================================================

class LakeflowConnect:
    """
    Petrinex connector implementing the LakeflowConnect interface.
    
    Provides incremental CDC loading for Alberta oil & gas production data
    from Petrinex PublicData.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Petrinex connector.
        
        Args:
            options: Configuration dictionary (no auth required for Petrinex)
        
        Raises:
            ImportError: If petrinex package is not installed
            RuntimeError: If client initialization fails
        """
        # Store configuration - Petrinex doesn't require authentication
        self.jurisdiction = options.get("jurisdiction", "AB")
        self.file_format = options.get("file_format", "CSV")
        
        # Initialize the Petrinex client
        self._initialize_petrinex_client()

    def _initialize_petrinex_client(self) -> None:
        """Initialize the Petrinex client with error handling."""
        try:
            from petrinex import PetrinexVolumetricsClient
            from pyspark.sql import SparkSession
            
            # Get or create SparkSession
            spark = SparkSession.builder.getOrCreate()
            
            self.petrinex_client = PetrinexVolumetricsClient(
                spark=spark,
                jurisdiction=self.jurisdiction,
                file_format=self.file_format
            )
        except ImportError:
            raise ImportError(
                "petrinex package is required. "
                "Install it with: pip install petrinex"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Petrinex client: {e}")

    # ========================================================================
    # LakeflowConnect Interface Methods
    # ========================================================================

    def list_tables(self) -> List[str]:
        """
        List all available tables in this connector.
        
        Returns:
            List of table names available for ingestion
        """
        return SUPPORTED_TABLES

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        """
        Get the PySpark schema for a table.
        
        Args:
            table_name: Name of the table
            table_options: Table-specific configuration (unused for Petrinex)
        
        Returns:
            PySpark StructType defining the table schema
        
        Raises:
            ValueError: If table_name is not supported
        """
        self._validate_table_name(table_name)
        
        if table_name == "volumetrics":
            # Schema is dynamically determined by Petrinex data
            # All fields are StringType to handle variability in CSV data
            # Users can cast to appropriate types in downstream processing
            return StructType([
                # Core fields (from Petrinex CSV)
                StructField("BA_ID", StringType(), nullable=True),
                StructField("Licence_No", StringType(), nullable=True),
                StructField("Licensee_BA_Code", StringType(), nullable=True),
                StructField("Licensee_Full_Name", StringType(), nullable=True),
                StructField("Licencee_Abbr_Name", StringType(), nullable=True),
                StructField("ProductionMonth", StringType(), nullable=False),
                StructField("PoolCode", StringType(), nullable=True),
                StructField("Product", StringType(), nullable=True),
                StructField("Activity", StringType(), nullable=True),
                StructField("Source", StringType(), nullable=True),
                StructField("Volume", StringType(), nullable=True),
                StructField("EnergyGJ", StringType(), nullable=True),
                StructField("Hours", StringType(), nullable=True),
                
                # Provenance fields added by connector
                StructField("production_month", StringType(), nullable=False),
                StructField("file_updated_ts", TimestampType(), nullable=False),
                StructField("source_url", StringType(), nullable=True),
                StructField("ingestion_time", TimestampType(), nullable=False),
            ])
        
        raise ValueError(f"Unknown table: {table_name}")

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict[str, any]:
        """
        Get table metadata for CDC configuration.
        
        Args:
            table_name: Name of the table
            table_options: Table-specific configuration (unused for Petrinex)
        
        Returns:
            Dictionary containing:
                - primary_keys: List of fields forming the primary key
                - cursor_field: Field used for incremental watermark tracking
                - sequence_by: Field used to determine record freshness (SCD Type 1)
                - ingestion_type: Type of ingestion (cdc or append)
        
        Raises:
            ValueError: If table_name is not supported
        """
        self._validate_table_name(table_name)
        
        if table_name == "volumetrics":
            return {
                # Composite primary key for volumetric records
                "primary_keys": ["BA_ID", "ProductionMonth", "Product", "Activity", "Source"],
                
                # Use file_updated_ts for watermark (when Petrinex updated the file)
                "cursor_field": "file_updated_ts",
                
                # Use ingestion_time to sequence records for SCD Type 1 merges
                "sequence_by": "ingestion_time",
                
                # CDC mode with file update tracking
                "ingestion_type": "cdc"
            }
        
        raise ValueError(f"Unknown table: {table_name}")

    def read_table(
        self, 
        table_name: str, 
        start_offset: dict, 
        table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read table data with incremental CDC support.
        
        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing high_watermark for incremental reads
            table_options: Table-specific configuration:
                - updated_after (optional): Load files updated after this date (YYYY-MM-DD)
                - from_date (optional): Load production data from this month onwards (YYYY-MM)
                - request_timeout_s (optional): HTTP timeout in seconds (default: 60)
        
        Returns:
            Tuple of (record iterator, next offset dictionary)
        
        Raises:
            ValueError: If table_name is not supported
        """
        self._validate_table_name(table_name)
        
        if table_name == "volumetrics":
            # Extract table-specific configuration
            updated_after = table_options.get("updated_after")
            from_date = table_options.get("from_date")
            timeout_s = int(table_options.get("request_timeout_s", DEFAULT_TIMEOUT_S))
            
            return self._read_volumetrics_table(
                start_offset, 
                updated_after, 
                from_date, 
                timeout_s
            )
        
        raise ValueError(f"Unknown table: {table_name}")

    # ========================================================================
    # Table-Specific Read Methods
    # ========================================================================

    def _read_volumetrics_table(
        self, 
        start_offset: dict, 
        updated_after: str, 
        from_date: str,
        timeout_s: int
    ) -> (Iterator[dict], dict):
        """
        Read Petrinex volumetric data with CDC incremental loading.
        
        Strategy:
        - Initial load: Use from_date or default to 6 months ago
        - Incremental load: Use updated_after based on high_watermark
        - Always track file_updated_ts as watermark
        
        Args:
            start_offset: Dictionary with optional high_watermark for incremental reads
            updated_after: Load files updated after this date (YYYY-MM-DD)
            from_date: Load production data from this month onwards (YYYY-MM)
            timeout_s: HTTP request timeout in seconds
        
        Returns:
            Tuple of (record iterator, next offset with updated high_watermark)
        """
        start_offset = start_offset or {}
        high_watermark = start_offset.get("high_watermark")
        
        # Determine date filtering strategy
        if high_watermark:
            # Incremental mode: use high_watermark to determine updated_after
            fetch_updated_after = self._calculate_incremental_date(high_watermark)
        elif updated_after:
            # User-specified updated_after
            fetch_updated_after = updated_after
        else:
            # Initial load: calculate based on from_date or default
            fetch_updated_after = self._calculate_initial_date(from_date)
        
        # Capture ingestion timestamp for this batch
        ingestion_timestamp = datetime.utcnow()
        
        print(f"Fetching Petrinex volumetrics: updated_after={fetch_updated_after}")
        
        try:
            # Fetch data using Petrinex client
            # Note: The client handles date filtering internally
            if from_date and not high_watermark:
                # Initial historical load from production month
                df = self.petrinex_client.read_spark_df(from_date=from_date)
            else:
                # Incremental load based on file updates
                df = self.petrinex_client.read_spark_df(updated_after=fetch_updated_after)
            
            # Add ingestion_time to DataFrame
            from pyspark.sql.functions import lit
            from pyspark.sql.types import TimestampType
            
            df = df.withColumn(
                "ingestion_time", 
                lit(ingestion_timestamp).cast(TimestampType())
            )
            
            # Convert to list of dictionaries
            records = [row.asDict() for row in df.collect()]
            
            print(f"Success: {len(records)} records fetched")
            
            # Calculate next watermark
            next_offset = self._calculate_next_offset(records, high_watermark)
            
            return iter(records), next_offset
            
        except Exception as e:
            print(f"Error fetching Petrinex data: {e}")
            # Return empty iterator on error
            return iter([]), start_offset

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def _validate_table_name(self, table_name: str) -> None:
        """Validate that the table name is supported."""
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {', '.join(SUPPORTED_TABLES)}"
            )

    def _calculate_incremental_date(self, high_watermark: str) -> str:
        """
        Calculate updated_after date for incremental load.
        
        Args:
            high_watermark: ISO format timestamp of last processed file update
        
        Returns:
            Date string in YYYY-MM-DD format
        """
        try:
            hwm_dt = datetime.fromisoformat(high_watermark.replace('Z', '+00:00'))
            # Use the watermark date directly
            fetch_date = hwm_dt.strftime("%Y-%m-%d")
            print(f"Incremental: watermark={high_watermark}, fetch_date={fetch_date}")
            return fetch_date
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid high_watermark: {high_watermark}, error: {e}")

    def _calculate_initial_date(self, from_date: str) -> str:
        """
        Calculate updated_after date for initial load.
        
        Args:
            from_date: Optional production month to start from (YYYY-MM)
        
        Returns:
            Date string in YYYY-MM-DD format
        """
        if from_date:
            # Convert production month (YYYY-MM) to date (YYYY-MM-01)
            fetch_date = f"{from_date}-01"
        else:
            # Default to 6 months ago
            fetch_date = (datetime.now() - timedelta(days=DEFAULT_HISTORICAL_MONTHS * 30)).strftime("%Y-%m-%d")
        
        print(f"Initial load: from_date={fetch_date}")
        return fetch_date

    def _calculate_next_offset(self, records: List[dict], current_watermark: str) -> dict:
        """
        Calculate the next high watermark based on fetched records.
        
        Uses file_updated_ts as the watermark to track which Petrinex files
        have been processed.
        
        Args:
            records: List of fetched records
            current_watermark: Current high watermark (for fallback)
        
        Returns:
            Dictionary with updated high_watermark
        """
        if records:
            # Use max file_updated_ts as watermark
            latest_timestamp = max(
                r["file_updated_ts"] for r in records 
                if r.get("file_updated_ts") is not None
            )
            
            # Convert to ISO format if it's a datetime object
            if isinstance(latest_timestamp, datetime):
                next_watermark = latest_timestamp.isoformat()
            else:
                next_watermark = str(latest_timestamp)
        else:
            # Fallback: use current watermark or now
            if current_watermark:
                next_watermark = current_watermark
            else:
                next_watermark = datetime.utcnow().isoformat()
        
        return {"high_watermark": next_watermark}

