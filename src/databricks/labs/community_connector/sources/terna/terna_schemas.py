"""Static schema definitions and metadata for the Terna connector.

Schemas are derived from the Terna Public API response structures
(documentation: terna_api_doc.md).
"""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# =============================================================================
# Supported tables (static list)
# =============================================================================

SUPPORTED_TABLES = [
    "total_load",
    "market_load",
    "daily_prices",
    "peak_valley_load"
]

# =============================================================================
# Table schema definitions (API returns strings for numeric fields)
# Defined here to avoid circular import: readers import from this module.
# =============================================================================

# total_load: date, date_tz, date_offset, total_load_MW, forecast_total_load_MW, bidding_zone
TOTAL_LOAD_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("date_tz", StringType(), True),
        StructField("date_offset", StringType(), True),
        StructField("total_load_MW", StringType(), True),
        StructField("forecast_total_load_MW", StringType(), True),
        StructField("bidding_zone", StringType(), True),
    ]
)

TOTAL_LOAD_METADATA = {
    "primary_keys": ["date", "bidding_zone"],
    "cursor_field": "date",
    "ingestion_type": "append",
}

# market_load: date, date_tz, date_offset, market_load_MW, bidding_zone
MARKET_LOAD_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("date_tz", StringType(), True),
        StructField("date_offset", StringType(), True),
        StructField("market_load_MW", StringType(), True),
        StructField("forecast_market_load_MW", StringType(), True),
        StructField("bidding_zone", StringType(), True),
    ]
)

MARKET_LOAD_METADATA = {
    "primary_keys": ["date", "bidding_zone"],
    "cursor_field": "date",
    "ingestion_type": "append",
}

# peak_valley_load: date, date_tz, date_offset, peak_load_MW, valley_load_MW, bidding_zone
PEAK_VALLEY_LOAD_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("date_tz", StringType(), True),
        StructField("date_offset", StringType(), True),
        StructField("peak_load_GW", StringType(), True),
        StructField("valley_load_GW", StringType(), True)
    ]
)

PEAK_VALLEY_LOAD_METADATA = {
    "primary_keys": ["date"],
    "cursor_field": "date",
    "ingestion_type": "append",
}

# daily_prices: date, date_tz, date_offset, price_eur_mwh, bidding_zone
DAILY_PRICES_SCHEMA = StructType(
    [
        StructField("publication_date", StringType(), True),
        StructField("reference_date", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("date_tz", StringType(), True),
        StructField("macrozone", StringType(), True),
        StructField("base_price_EURxMWh", StringType(), True),
        StructField("incentive_component_EURxMWh", StringType(), True),
        StructField("unbalance_price_EURxMWh", StringType(), True),
    ]
)

DAILY_PRICES_METADATA = {
    "primary_keys": ["reference_date", "macrozone"],
    "cursor_field": "reference_date",
    "ingestion_type": "append",
}

# =============================================================================
# Legacy / commented-out schema definitions
# =============================================================================

TABLE_SCHEMAS = {
    "total_load": TOTAL_LOAD_SCHEMA,
    "market_load": MARKET_LOAD_SCHEMA,
    "daily_prices": DAILY_PRICES_SCHEMA,
    "peak_valley_load": PEAK_VALLEY_LOAD_SCHEMA,
}

# =============================================================================
# Table metadata: primary keys, cursor field, ingestion type (all append)
# =============================================================================

TABLE_METADATA = {
    "total_load": TOTAL_LOAD_METADATA,
    "market_load": MARKET_LOAD_METADATA,
    "daily_prices": DAILY_PRICES_METADATA,
    "peak_valley_load": PEAK_VALLEY_LOAD_METADATA,
}
