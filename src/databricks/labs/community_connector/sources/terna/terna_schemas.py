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
    #"actual_generation",
    #"renewable_generation",
    #"physical_foreign_flow",
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

# =============================================================================
# Legacy / commented-out schema definitions
# =============================================================================

'''
# actual_generation: date, date_tz, date_offset, actual_generation_GWh, primary_source
ACTUAL_GENERATION_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("date_tz", StringType(), True),
        StructField("date_offset", StringType(), True),
        StructField("actual_generation_GWh", StringType(), True),
        StructField("primary_source", StringType(), True),
    ]
)

# renewable_generation: date, date_tz, date_offset, renewable_generation_GWh, energy_source
RENEWABLE_GENERATION_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("date_tz", StringType(), True),
        StructField("date_offset", StringType(), True),
        StructField("renewable_generation_GWh", StringType(), True),
        StructField("energy_source", StringType(), True),
    ]
)

# physical_foreign_flow: date, date_tz, date_offset, country, import, export, physical_foreign_flow_MW
# "import" is a Python keyword; API field is "import" -> we use "import_" in schema or keep "import" as string key
PHYSICAL_FOREIGN_FLOW_SCHEMA = StructType(
    [
        StructField("date", StringType(), True),
        StructField("date_tz", StringType(), True),
        StructField("date_offset", StringType(), True),
        StructField("country", StringType(), True),
        StructField("import", StringType(), True),
        StructField("export", StringType(), True),
        StructField("physical_foreign_flow_MW", StringType(), True),
    ]
)
'''

TABLE_SCHEMAS = {
    "total_load": TOTAL_LOAD_SCHEMA,
    "market_load": MARKET_LOAD_SCHEMA,
    #"actual_generation": ACTUAL_GENERATION_SCHEMA,
    #"renewable_generation": RENEWABLE_GENERATION_SCHEMA,
    #"physical_foreign_flow": PHYSICAL_FOREIGN_FLOW_SCHEMA,
}

# =============================================================================
# Table metadata: primary keys, cursor field, ingestion type (all append)
# =============================================================================

TABLE_METADATA = {
    "total_load": TOTAL_LOAD_METADATA,
    "market_load": MARKET_LOAD_METADATA,
}

'''
    "actual_generation": {
        "primary_keys": ["date", "primary_source"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
    "renewable_generation": {
        "primary_keys": ["date", "energy_source"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
    "physical_foreign_flow": {
        "primary_keys": ["date", "country"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
    '''