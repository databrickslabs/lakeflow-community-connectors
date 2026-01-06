"""
AESO Connector for LakeFlow
============================
Provides access to Alberta Electric System Operator (AESO) pool price data.

This connector implements SCD Type 1 with a lookback window to capture:
- Frequent forecast price updates (AESO updates forecasts as settlement hours approach)
- Settlement adjustments to actual prices (finalized within 24-72 hours)

Connection Configuration:
- api_key (required): AESO API authentication key

Table Configuration:
- start_date (optional): Historical start date in YYYY-MM-DD format (default: 30 days ago)
- lookback_hours (optional): Hours to look back for backfill and forecast updates (default: 24, min: 24)
- batch_size_days (optional): Number of days to fetch per API batch (default: 30)
- rate_limit_delay (optional): Seconds to wait between API calls (default: 0.1)
"""

from typing import Iterator, Dict, List
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType
from datetime import datetime, timedelta
import time


# ============================================================================
# Constants
# ============================================================================

SUPPORTED_TABLES = ["pool_price"]
MIN_LOOKBACK_HOURS = 24  # Minimum safe lookback to capture settlement adjustments
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_HISTORICAL_DAYS = 30
MAX_BATCH_DAYS = 30
RATE_LIMIT_DELAY = 0.1  # seconds between API calls


# ============================================================================
# Main Connector Class
# ============================================================================

class LakeflowConnect:
    """
    AESO connector implementing the LakeflowConnect interface.
    
    Provides incremental CDC loading with automatic lookback for capturing
    late-arriving updates to electricity market data.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the AESO connector.
        
        Args:
            options: Configuration dictionary with the following keys:
                - api_key (required): AESO API authentication key
        
        Raises:
            ValueError: If required options are missing or invalid
            ImportError: If aeso-python-api package is not installed
            RuntimeError: If API client initialization fails
        """
        # Validate required configuration
        if "api_key" not in options:
            raise ValueError("api_key is required in options")
        
        # Store configuration
        self.api_key = options["api_key"]
        
        # Initialize the AESO API client
        self._initialize_api_client()

    def _initialize_api_client(self) -> None:
        """Initialize the AESO API client with error handling."""
        try:
            from aeso import AESOAPI
            self.aeso_client = AESOAPI(self.api_key)
        except ImportError:
            raise ImportError(
                "aeso-python-api package is required. "
                "Install it with: pip install aeso-python-api"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize AESO API client: {e}")

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
            table_options: Table-specific configuration (unused for AESO)
        
        Returns:
            PySpark StructType defining the table schema
        
        Raises:
            ValueError: If table_name is not supported
        """
        self._validate_table_name(table_name)
        
        if table_name == "pool_price":
            return StructType([
                # Primary key: hourly timestamp in UTC
                StructField("begin_datetime_utc", TimestampType(), nullable=False),
                
                # Actual pool price (nullable - future hours only have forecasts)
                StructField("pool_price", DoubleType(), nullable=True),
                
                # Forecasted pool price
                StructField("forecast_pool_price", DoubleType(), nullable=True),
                
                # Rolling 30-day average
                StructField("rolling_30day_avg", DoubleType(), nullable=True),
                
                # Ingestion timestamp for CDC sequencing
                StructField("ingestion_time", TimestampType(), nullable=False),
            ])
        
        raise ValueError(f"Unknown table: {table_name}")

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict[str, any]:
        """
        Get table metadata for CDC configuration.
        
        Args:
            table_name: Name of the table
            table_options: Table-specific configuration (unused for AESO)
        
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
        
        if table_name == "pool_price":
            return {
                # Unique identifier for each hourly record
                "primary_keys": ["begin_datetime_utc"],
                
                # Use ingestion_time for watermark to avoid Spark dropping old updates
                "cursor_field": "ingestion_time",
                
                # Use ingestion_time to sequence records for SCD Type 1 merges
                "sequence_by": "ingestion_time",
                
                # CDC mode with lookback for late-arriving data
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
                - start_date (optional): Historical start date YYYY-MM-DD (default: 30 days ago)
                - lookback_hours (optional): CDC lookback window in hours (default: 24, min: 24)
                - batch_size_days (optional): Days per API batch (default: 30)
                - rate_limit_delay (optional): Seconds between API calls (default: 0.1)
        
        Returns:
            Tuple of (record iterator, next offset dictionary)
        
        Raises:
            ValueError: If table_name is not supported or lookback_hours < 24
        """
        self._validate_table_name(table_name)
        
        if table_name == "pool_price":
            # Extract table-specific configuration
            start_date = table_options.get("start_date")
            lookback_hours = int(table_options.get("lookback_hours", DEFAULT_LOOKBACK_HOURS))
            batch_size_days = int(table_options.get("batch_size_days", MAX_BATCH_DAYS))
            rate_limit_delay = float(table_options.get("rate_limit_delay", RATE_LIMIT_DELAY))
            
            # Validate start_date format if provided
            if start_date:
                try:
                    datetime.strptime(start_date, "%Y-%m-%d")
                except ValueError:
                    raise ValueError("start_date must be in YYYY-MM-DD format")
            
            # Validate lookback_hours minimum
            if lookback_hours < MIN_LOOKBACK_HOURS:
                raise ValueError(
                    f"lookback_hours must be at least {MIN_LOOKBACK_HOURS} hours "
                    f"to safely capture settlement adjustments and late-arriving data. "
                    f"Provided: {lookback_hours}"
                )
            
            return self._read_pool_price_table(
                start_offset, 
                start_date,
                lookback_hours,
                batch_size_days, 
                rate_limit_delay
            )
        
        raise ValueError(f"Unknown table: {table_name}")

    # ========================================================================
    # Table-Specific Read Methods
    # ========================================================================

    def _read_pool_price_table(
        self, 
        start_offset: dict, 
        start_date: str,
        lookback_hours: int,
        batch_size_days: int, 
        rate_limit_delay: float
    ) -> (Iterator[dict], dict):
        """
        Read pool price data with CDC incremental loading.
        
        Strategy:
        - Initial load: Start from start_date or default to 30 days ago
        - Incremental load: Start from (high_watermark - lookback_hours) to capture updates
        - Always fetch through today for continuous ingestion
        
        The lookback window ensures we recapture records that may have been updated
        due to settlement adjustments or late-arriving data.
        
        Args:
            start_offset: Dictionary with optional high_watermark for incremental reads
            start_date: Historical start date (YYYY-MM-DD) or None for default
            lookback_hours: Hours to look back for CDC updates
            batch_size_days: Number of days to fetch per API batch
            rate_limit_delay: Seconds to wait between API calls
        
        Returns:
            Tuple of (record iterator, next offset with updated high_watermark)
        """
        start_offset = start_offset or {}
        high_watermark = start_offset.get("high_watermark")
        
        # Determine fetch start date based on mode
        if high_watermark:
            fetch_start_date = self._calculate_incremental_start_date(high_watermark, lookback_hours)
        else:
            fetch_start_date = self._calculate_initial_start_date(start_date)
        
        # Always fetch through today for continuous ingestion
        fetch_end_date = datetime.now().strftime("%Y-%m-%d")
        
        # Validate date range
        if fetch_start_date > fetch_end_date:
            print(f"Start date {fetch_start_date} is in future, no data to fetch")
            return iter([]), start_offset
        
        # Fetch data in batches
        all_records = self._fetch_data_in_batches(fetch_start_date, fetch_end_date, batch_size_days, rate_limit_delay)
        
        # Calculate next watermark
        next_offset = self._calculate_next_offset(all_records, high_watermark, fetch_end_date)
        
        print(f"Success: {len(all_records)} records, watermark: {next_offset['high_watermark']}")
        return iter(all_records), next_offset

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

    def _calculate_incremental_start_date(self, high_watermark: str, lookback_hours: int) -> str:
        """
        Calculate start date for incremental load with lookback.
        
        Args:
            high_watermark: ISO format timestamp of last processed record
            lookback_hours: Hours to look back for CDC updates
        
        Returns:
            Start date string in YYYY-MM-DD format
        """
        try:
            hwm_dt = datetime.fromisoformat(high_watermark.replace('Z', '+00:00'))
            fetch_start_dt = hwm_dt - timedelta(hours=lookback_hours)
            fetch_start_date = fetch_start_dt.strftime("%Y-%m-%d")
            print(f"Incremental: watermark={high_watermark}, lookback={lookback_hours}h, start={fetch_start_date}")
            return fetch_start_date
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid high_watermark: {high_watermark}, error: {e}")

    def _calculate_initial_start_date(self, start_date: str) -> str:
        """
        Calculate start date for initial load.
        
        Args:
            start_date: Configured start date (YYYY-MM-DD) or None for default
        
        Returns:
            Start date string in YYYY-MM-DD format
        """
        if start_date:
            fetch_start_date = start_date
        else:
            fetch_start_date = (datetime.now() - timedelta(days=DEFAULT_HISTORICAL_DAYS)).strftime("%Y-%m-%d")
        
        print(f"Initial load from {fetch_start_date}")
        return fetch_start_date

    def _fetch_data_in_batches(self, start_date: str, end_date: str, batch_size_days: int, rate_limit_delay: float) -> List[dict]:
        """
        Fetch data from AESO API in batches to handle large date ranges.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            batch_size_days: Number of days to fetch per API batch
            rate_limit_delay: Seconds to wait between API calls
        
        Returns:
            List of all fetched records
        """
        all_records = []
        current_date = start_date
        
        # Capture ingestion timestamp once for this entire batch
        # This ensures all records from the same fetch have the same ingestion_time
        ingestion_timestamp = datetime.utcnow()
        
        print(f"Fetching pool price data: {start_date} to {end_date} (batch size: {batch_size_days} days, rate limit: {rate_limit_delay}s)")
        
        while current_date <= end_date:
            # Calculate batch end date using configured batch size
            batch_end_dt = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=batch_size_days - 1)
            batch_end_date = min(batch_end_dt.strftime("%Y-%m-%d"), end_date)
            
            # Fetch batch
            batch_records = self._fetch_single_batch(current_date, batch_end_date, ingestion_timestamp)
            all_records.extend(batch_records)
            
            # Move to next batch
            current_date = (datetime.strptime(batch_end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Rate limiting - configurable delay between API calls
            time.sleep(rate_limit_delay)
        
        return all_records

    def _fetch_single_batch(self, start_date: str, end_date: str, ingestion_timestamp: datetime) -> List[dict]:
        """
        Fetch a single batch of data from the AESO API.
        
        Args:
            start_date: Batch start date in YYYY-MM-DD format
            end_date: Batch end date in YYYY-MM-DD format
            ingestion_timestamp: UTC timestamp to stamp all records in this batch
        
        Returns:
            List of records for this batch
        """
        try:
            print(f"  Batch: {start_date} to {end_date}")
            
            # Fetch from AESO API (both dates required by API)
            pool_prices = self.aeso_client.get_pool_price_report(
                start_date=start_date,
                end_date=end_date
            )
            
            # Transform API objects to dictionary records
            records = [
                self._transform_price_object(price_obj, ingestion_timestamp)
                for price_obj in pool_prices
            ]
            
            print(f"  Fetched {len(records)} records")
            return records
            
        except Exception as e:
            print(f"  Error fetching batch: {e}")
            # Return empty list on error; caller will handle partial success
            return []

    def _transform_price_object(self, price_obj, ingestion_timestamp: datetime) -> dict:
        """
        Transform an AESO API price object into a dictionary record.
        
        Args:
            price_obj: Price object from aeso-python-api
            ingestion_timestamp: UTC timestamp to stamp this record
        
        Returns:
            Dictionary with transformed record data
        """
        # Extract UTC datetime (natively UTC from source)
        utc_dt = price_obj.begin_datetime_utc
        
        # Strip timezone info to keep as naive UTC datetime for PySpark
        if hasattr(utc_dt, 'tzinfo') and utc_dt.tzinfo is not None:
            utc_dt = utc_dt.replace(tzinfo=None)
        
        return {
            "begin_datetime_utc": utc_dt,
            "pool_price": float(price_obj.pool_price) if price_obj.pool_price is not None else None,
            "forecast_pool_price": float(price_obj.forecast_pool_price) if price_obj.forecast_pool_price is not None else None,
            "rolling_30day_avg": float(price_obj.rolling_30day_avg) if price_obj.rolling_30day_avg is not None else None,
            "ingestion_time": ingestion_timestamp,
        }

    def _calculate_next_offset(self, records: List[dict], current_watermark: str, end_date: str) -> dict:
        """
        Calculate the next high watermark based on fetched records.
        
        Uses ingestion_time as the watermark to avoid Spark's event-time watermarking
        from dropping late-arriving updates.
        
        Args:
            records: List of fetched records
            current_watermark: Current high watermark (for fallback)
            end_date: End date of fetch range (for fallback)
        
        Returns:
            Dictionary with updated high_watermark
        """
        if records:
            # Use max ingestion_time as watermark (all records have same ingestion_time per batch)
            latest_timestamp = max(r["ingestion_time"] for r in records)
            next_watermark = latest_timestamp.isoformat()
        else:
            # Fallback: use current watermark or end of fetch period
            if current_watermark:
                next_watermark = current_watermark
            else:
                fallback_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                next_watermark = fallback_dt.isoformat()
        
        return {"high_watermark": next_watermark}

