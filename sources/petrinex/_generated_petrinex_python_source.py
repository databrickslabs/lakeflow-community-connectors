# ==============================================================================
# Merged Lakeflow Source: petrinex
# ==============================================================================
# This file contains the merged Petrinex connector for deployment to Databricks.
# It includes utilities and the LakeflowConnect connector class.
# ==============================================================================

import base64
from datetime import datetime, timedelta
from decimal import Decimal
from typing import (
    Any,
    Dict,
    Iterator,
    List,
)

from pyspark.sql import Row, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, SimpleDataSourceStreamReader
from pyspark.sql.types import *


def register_lakeflow_source(spark):
    """Register the Lakeflow Python source with Spark."""

    ########################################################
    # libs/utils.py
    ########################################################

    def _parse_struct(value: Any, field_type: StructType) -> Row:
        """Parse a dictionary into a PySpark Row based on StructType schema."""
        if not isinstance(value, dict):
            raise ValueError(f"Expected a dictionary for StructType, got {type(value)}")
        if value == {}:
            raise ValueError(
                "field in StructType cannot be an empty dict. "
                "Please assign None as the default value instead."
            )
        field_dict = {}
        for field in field_type.fields:
            if field.name in value:
                field_dict[field.name] = parse_value(value.get(field.name), field.dataType)
            elif field.nullable:
                field_dict[field.name] = None
            else:
                raise ValueError(f"Field {field.name} is not nullable but not found in the input")
        return Row(**field_dict)

    def _parse_array(value: Any, field_type: ArrayType) -> list:
        """Parse a list into a PySpark array based on ArrayType schema."""
        if not isinstance(value, list):
            if field_type.containsNull:
                return [parse_value(value, field_type.elementType)]
            raise ValueError(f"Expected a list for ArrayType, got {type(value)}")
        return [parse_value(v, field_type.elementType) for v in value]

    def _parse_map(value: Any, field_type: MapType) -> dict:
        """Parse a dictionary into a PySpark map based on MapType schema."""
        if not isinstance(value, dict):
            raise ValueError(f"Expected a dictionary for MapType, got {type(value)}")
        return {
            parse_value(k, field_type.keyType): parse_value(v, field_type.valueType)
            for k, v in value.items()
        }

    def _parse_string(value: Any) -> str:
        """Convert value to string."""
        return str(value)

    def _parse_integer(value: Any) -> int:
        """Convert value to integer."""
        if isinstance(value, str) and value.strip():
            return int(float(value)) if "." in value else int(value)
        if isinstance(value, (int, float)):
            return int(value)
        raise ValueError(f"Cannot convert {value} to integer")

    def _parse_float(value: Any) -> float:
        """Convert value to float."""
        return float(value)

    def _parse_decimal(value: Any) -> Decimal:
        """Convert value to Decimal."""
        return Decimal(value) if isinstance(value, str) and value.strip() else Decimal(str(value))

    def _parse_boolean(value: Any) -> bool:
        """Convert value to boolean."""
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in ("true", "t", "yes", "y", "1"):
                return True
            if lowered in ("false", "f", "no", "n", "0"):
                return False
        return bool(value)

    def _parse_date(value: Any) -> datetime.date:
        """Convert value to date."""
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
            return datetime.fromisoformat(value).date()
        if isinstance(value, datetime):
            return value.date()
        raise ValueError(f"Cannot convert {value} to date")

    def _parse_timestamp(value: Any) -> datetime:
        """Convert value to timestamp."""
        if isinstance(value, str):
            ts_value = value.replace("Z", "+00:00") if value.endswith("Z") else value
            try:
                return datetime.fromisoformat(ts_value)
            except ValueError:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                    try:
                        return datetime.strptime(ts_value, fmt)
                    except ValueError:
                        continue
        elif isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        elif isinstance(value, datetime):
            return value
        raise ValueError(f"Cannot convert {value} to timestamp")

    def _decode_string_to_bytes(value: str) -> bytes:
        """Try to decode a string as base64, then hex, then UTF-8."""
        try:
            return base64.b64decode(value)
        except Exception:
            pass
        try:
            return bytes.fromhex(value)
        except Exception:
            pass
        return value.encode("utf-8")

    def _parse_binary(value: Any) -> bytes:
        """Convert value to bytes. Tries base64, then hex, then UTF-8 for strings."""
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, str):
            return _decode_string_to_bytes(value)
        if isinstance(value, list):
            return bytes(value)
        return str(value).encode("utf-8")

    _PRIMITIVE_PARSERS = {
        StringType: _parse_string,
        IntegerType: _parse_integer,
        LongType: _parse_integer,
        FloatType: _parse_float,
        DoubleType: _parse_float,
        DecimalType: _parse_decimal,
        BooleanType: _parse_boolean,
        DateType: _parse_date,
        TimestampType: _parse_timestamp,
        BinaryType: _parse_binary,
    }

    def parse_value(value: Any, field_type: DataType) -> Any:
        """
        Converts a JSON value into a PySpark-compatible data type based on the provided field type.
        """
        if value is None:
            return None

        if isinstance(field_type, StructType):
            return _parse_struct(value, field_type)
        if isinstance(field_type, ArrayType):
            return _parse_array(value, field_type)
        if isinstance(field_type, MapType):
            return _parse_map(value, field_type)

        try:
            field_type_class = type(field_type)
            if field_type_class in _PRIMITIVE_PARSERS:
                return _PRIMITIVE_PARSERS[field_type_class](value)

            if hasattr(field_type, "fromJson"):
                return field_type.fromJson(value)

            raise TypeError(f"Unsupported field type: {field_type}")
        except (ValueError, TypeError) as e:
            raise ValueError(f"Error converting '{value}' ({type(value)}) to {field_type}: {str(e)}")

    ########################################################
    # sources/petrinex/petrinex.py - Constants
    ########################################################

    SUPPORTED_TABLES = ["volumetrics"]
    DEFAULT_HISTORICAL_MONTHS = 6
    DEFAULT_TIMEOUT_S = 60

    ########################################################
    # sources/petrinex/petrinex.py - LakeflowConnect
    ########################################################

    class LakeflowConnect:
        """
        Petrinex connector implementing the LakeflowConnect interface.
        
        Provides incremental CDC loading for Alberta oil & gas production data
        from Petrinex PublicData.
        """

        def __init__(self, options: Dict[str, str]) -> None:
            """Initialize the Petrinex connector."""
            self.jurisdiction = options.get("jurisdiction", "AB")
            self.file_format = options.get("file_format", "CSV")
            self._initialize_petrinex_client()

        def _initialize_petrinex_client(self) -> None:
            """Initialize the Petrinex client with error handling."""
            try:
                from petrinex import PetrinexVolumetricsClient
                
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

        def list_tables(self) -> List[str]:
            """List all available tables in this connector."""
            return SUPPORTED_TABLES

        def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
            """Get the PySpark schema for a table."""
            self._validate_table_name(table_name)
            
            if table_name == "volumetrics":
                return StructType([
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
                    StructField("production_month", StringType(), nullable=False),
                    StructField("file_updated_ts", TimestampType(), nullable=False),
                    StructField("source_url", StringType(), nullable=True),
                    StructField("ingestion_time", TimestampType(), nullable=False),
                ])
            
            raise ValueError(f"Unknown table: {table_name}")

        def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict[str, any]:
            """Get table metadata for CDC configuration."""
            self._validate_table_name(table_name)
            
            if table_name == "volumetrics":
                return {
                    "primary_keys": ["BA_ID", "ProductionMonth", "Product", "Activity", "Source"],
                    "cursor_field": "file_updated_ts",
                    "sequence_by": "ingestion_time",
                    "ingestion_type": "cdc"
                }
            
            raise ValueError(f"Unknown table: {table_name}")

        def read_table(
            self, 
            table_name: str, 
            start_offset: dict, 
            table_options: Dict[str, str]
        ) -> (Iterator[dict], dict):
            """Read table data with incremental CDC support."""
            self._validate_table_name(table_name)
            
            if table_name == "volumetrics":
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

        def _read_volumetrics_table(
            self, 
            start_offset: dict, 
            updated_after: str, 
            from_date: str,
            timeout_s: int
        ) -> (Iterator[dict], dict):
            """Read Petrinex volumetric data with CDC incremental loading."""
            start_offset = start_offset or {}
            high_watermark = start_offset.get("high_watermark")
            
            if high_watermark:
                fetch_updated_after = self._calculate_incremental_date(high_watermark)
            elif updated_after:
                fetch_updated_after = updated_after
            else:
                fetch_updated_after = self._calculate_initial_date(from_date)
            
            ingestion_timestamp = datetime.utcnow()
            
            print(f"Fetching Petrinex volumetrics: updated_after={fetch_updated_after}")
            
            try:
                if from_date and not high_watermark:
                    df = self.petrinex_client.read_spark_df(from_date=from_date)
                else:
                    df = self.petrinex_client.read_spark_df(updated_after=fetch_updated_after)
                
                from pyspark.sql.functions import lit
                
                df = df.withColumn(
                    "ingestion_time", 
                    lit(ingestion_timestamp).cast(TimestampType())
                )
                
                records = [row.asDict() for row in df.collect()]
                
                print(f"Success: {len(records)} records fetched")
                
                next_offset = self._calculate_next_offset(records, high_watermark)
                
                return iter(records), next_offset
                
            except Exception as e:
                print(f"Error fetching Petrinex data: {e}")
                return iter([]), start_offset

        def _validate_table_name(self, table_name: str) -> None:
            """Validate that the table name is supported."""
            if table_name not in SUPPORTED_TABLES:
                raise ValueError(
                    f"Table '{table_name}' is not supported. "
                    f"Supported tables: {', '.join(SUPPORTED_TABLES)}"
                )

        def _calculate_incremental_date(self, high_watermark: str) -> str:
            """Calculate updated_after date for incremental load."""
            try:
                hwm_dt = datetime.fromisoformat(high_watermark.replace('Z', '+00:00'))
                fetch_date = hwm_dt.strftime("%Y-%m-%d")
                print(f"Incremental: watermark={high_watermark}, fetch_date={fetch_date}")
                return fetch_date
            except (ValueError, AttributeError) as e:
                raise ValueError(f"Invalid high_watermark: {high_watermark}, error: {e}")

        def _calculate_initial_date(self, from_date: str) -> str:
            """Calculate updated_after date for initial load."""
            if from_date:
                fetch_date = f"{from_date}-01"
            else:
                fetch_date = (datetime.now() - timedelta(days=DEFAULT_HISTORICAL_MONTHS * 30)).strftime("%Y-%m-%d")
            
            print(f"Initial load: from_date={fetch_date}")
            return fetch_date

        def _calculate_next_offset(self, records: List[dict], current_watermark: str) -> dict:
            """Calculate the next high watermark based on fetched records."""
            if records:
                latest_timestamp = max(
                    r["file_updated_ts"] for r in records 
                    if r.get("file_updated_ts") is not None
                )
                
                if isinstance(latest_timestamp, datetime):
                    next_watermark = latest_timestamp.isoformat()
                else:
                    next_watermark = str(latest_timestamp)
            else:
                if current_watermark:
                    next_watermark = current_watermark
                else:
                    next_watermark = datetime.utcnow().isoformat()
            
            return {"high_watermark": next_watermark}

    ########################################################
    # DataSource Implementation
    ########################################################

    class PetrinexDataSource(DataSource):
        """PySpark DataSource implementation for Petrinex connector."""
        
        @classmethod
        def name(cls):
            return "petrinex"
        
        def schema(self) -> str:
            """Return the schema of the data source."""
            options = self.options
            connector = LakeflowConnect(options)
            
            table_name = options.get("table", "volumetrics")
            table_options = {}
            
            schema = connector.get_table_schema(table_name, table_options)
            return schema.simpleString()
        
        def reader(self, schema: StructType) -> "PetrinexDataSourceReader":
            """Create a DataSourceReader for batch reads."""
            return PetrinexDataSourceReader(self.options, schema)

    class PetrinexDataSourceReader(DataSourceReader):
        """DataSource reader for Petrinex connector."""
        
        def __init__(self, options: Dict[str, str], schema: StructType):
            self.options = options
            self.schema = schema
        
        def read(self, partition):
            """Read data from Petrinex."""
            connector = LakeflowConnect(self.options)
            
            table_name = self.options.get("table", "volumetrics")
            start_offset = {}
            table_options = self.options
            
            records, next_offset = connector.read_table(table_name, start_offset, table_options)
            
            for record in records:
                parsed_record = {}
                for field in self.schema.fields:
                    value = record.get(field.name)
                    parsed_record[field.name] = parse_value(value, field.dataType)
                yield tuple(parsed_record[f.name] for f in self.schema.fields)

    # Register the data source
    spark.dataSource.register(PetrinexDataSource)

# Export for direct usage
__all__ = ['register_lakeflow_source']

