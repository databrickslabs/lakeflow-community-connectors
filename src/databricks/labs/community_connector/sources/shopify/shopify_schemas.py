"""Static schema definitions for Shopify connector tables.

Schemas are derived from the Shopify Admin REST API (version 2026-04).
See ``shopify_api_doc.md`` for field references and ``Edge cases`` notes.

Table-level metadata (ingestion type, primary keys, cursor field) lives
in ``TABLE_METADATA`` so the connector's ``read_table_metadata`` is a
simple dict lookup.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
)


# =============================================================================
# Table Schemas
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    "locations": StructType(
        [
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("address1", StringType(), True),
            StructField("address2", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("province", StringType(), True),
            StructField("country", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("province_code", StringType(), True),
            StructField("legacy", BooleanType(), True),
            StructField("active", BooleanType(), True),
            StructField("admin_graphql_api_id", StringType(), True),
            StructField("localized_country_name", StringType(), True),
            StructField("localized_province_name", StringType(), True),
            StructField("shop", StringType(), False),
        ]
    ),
}


# =============================================================================
# Table Metadata
# =============================================================================

TABLE_METADATA: dict[str, dict] = {
    "locations": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
