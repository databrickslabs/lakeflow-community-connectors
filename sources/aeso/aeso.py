from typing import Iterator, Dict, List
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType
from datetime import datetime, timedelta
import time


class LakeflowConnect:
    """AESO connector - provides access to Alberta electricity pool price data."""

    def __init__(self, options: Dict[str, str]) -> None:
        """Initialize AESO connector.
        
        Args:
            options: Configuration with api_key (required), start_date (optional), 
                    lookback_hours (optional, default 24)
        """
        if "api_key" not in options:
            raise ValueError("api_key is required in options")
        
        self.api_key = options["api_key"]
        self.start_date = options.get("start_date")
        self.lookback_hours = int(options.get("lookback_hours", 24))
        
        if self.start_date:
            try:
                datetime.strptime(self.start_date, "%Y-%m-%d")
            except ValueError:
                raise ValueError("start_date must be in YYYY-MM-DD format")
        
        # Initialize the AESO API client
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

    def list_tables(self) -> List[str]:
        """List available tables."""
        return ["pool_price"]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """Get table schema."""
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {', '.join(self.list_tables())}"
            )
        
        if table_name == "pool_price":
            return StructType([
                StructField("begin_datetime_utc", TimestampType(), False),
                StructField("pool_price", DoubleType(), True),  # Nullable - null for future hours with only forecasts
                StructField("forecast_pool_price", DoubleType(), True),
                StructField("rolling_30day_avg", DoubleType(), True),
                StructField("ingestion_time", TimestampType(), False),  # UTC timestamp when row was ingested
            ])
        
        raise ValueError(f"Unknown table: {table_name}")

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, any]:
        """Get table metadata (primary keys, cursor, ingestion type)."""
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {', '.join(self.list_tables())}"
            )
        
        if table_name == "pool_price":
            return {
                "primary_keys": ["begin_datetime_utc"],
                "cursor_field": "begin_datetime_utc",
                "sequence_by": "ingestion_time",
                "ingestion_type": "cdc"
            }
        
        raise ValueError(f"Unknown table: {table_name}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read table data with CDC incremental support."""
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {', '.join(self.list_tables())}"
            )
        
        if table_name == "pool_price":
            return self._read_pool_price_table(start_offset)
        
        raise ValueError(f"Unknown table: {table_name}")

    def _read_pool_price_table(self, start_offset: dict) -> (Iterator[dict], dict):
        """Read pool price data with CDC incremental loading.
        
        Uses lookback window to recapture recently updated records due to
        late-arriving data and settlement adjustments.
        """
        if not start_offset:
            start_offset = {}
        
        high_watermark = start_offset.get("high_watermark")
        
        if high_watermark:
            # Incremental: Apply lookback window for CDC
            try:
                hwm_dt = datetime.fromisoformat(high_watermark.replace('Z', '+00:00'))
                fetch_start_dt = hwm_dt - timedelta(hours=self.lookback_hours)
                fetch_start_date = fetch_start_dt.strftime("%Y-%m-%d")
                print(f"CDC: watermark={high_watermark}, lookback={self.lookback_hours}h, start={fetch_start_date}")
            except (ValueError, AttributeError) as e:
                raise ValueError(f"Invalid high_watermark: {high_watermark}, error: {e}")
        else:
            # Initial: use start_date or default to 30 days ago
            fetch_start_date = self.start_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            print(f"Initial load from {fetch_start_date}")
        
        # End date always set to today for continuous ingestion
        today = datetime.now().strftime("%Y-%m-%d")
        
        if fetch_start_date > today:
            print(f"Start date {fetch_start_date} is in future, no data to fetch")
            return iter([]), start_offset
        
        all_records = []
        current_date = fetch_start_date
        fetch_end_date = today
        max_batch_days = 30  # Fetch in 7-day batches
        
        print(f"Fetching: {fetch_start_date} to {fetch_end_date}")
        
        # Capture ingestion timestamp once for this batch
        ingestion_timestamp = datetime.utcnow()
        
        while current_date <= fetch_end_date:
            batch_end_dt = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=max_batch_days - 1)
            batch_end_date = min(batch_end_dt.strftime("%Y-%m-%d"), fetch_end_date)
            
            try:
                print(f"Batch: {current_date} to {batch_end_date}")
                
                # Both start_date and end_date required by AESO API
                pool_prices = self.aeso_client.get_pool_price_report(
                    start_date=current_date,
                    end_date=batch_end_date
                )
                
                for price_obj in pool_prices:
                    # Get UTC datetime from source (natively UTC)
                    utc_dt = price_obj.begin_datetime_utc
                    
                    # Remove timezone info if present (keep as naive UTC datetime)
                    if hasattr(utc_dt, 'tzinfo') and utc_dt.tzinfo is not None:
                        utc_dt = utc_dt.replace(tzinfo=None)
                    
                    all_records.append({
                        "begin_datetime_utc": utc_dt,
                        "pool_price": float(price_obj.pool_price) if price_obj.pool_price is not None else None,
                        "forecast_pool_price": float(price_obj.forecast_pool_price) if price_obj.forecast_pool_price is not None else None,
                        "rolling_30day_avg": float(price_obj.rolling_30day_avg) if price_obj.rolling_30day_avg is not None else None,
                        "ingestion_time": ingestion_timestamp,
                    })
                
                print(f"Fetched {len(pool_prices)} records")
                time.sleep(0.1)  # Rate limit protection
                
            except Exception as e:
                print(f"Error: {e}")
                if all_records:
                    latest_timestamp = max(r["begin_datetime_utc"] for r in all_records)
                    next_offset = {"high_watermark": latest_timestamp.isoformat()}
                    print(f"Partial success: {len(all_records)} records, watermark: {next_offset['high_watermark']}")
                    return iter(all_records), next_offset
                else:
                    print("No records fetched, retrying with same offset")
                    return iter([]), start_offset
            
            current_date = (datetime.strptime(batch_end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Calculate new high watermark
        if all_records:
            latest_timestamp = max(r["begin_datetime_utc"] for r in all_records)
            next_high_watermark = latest_timestamp.isoformat()
        else:
            next_high_watermark = high_watermark or datetime.strptime(fetch_end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            ).isoformat()
        
        print(f"Success: {len(all_records)} records, watermark: {next_high_watermark}")
        return iter(all_records), {"high_watermark": next_high_watermark}

