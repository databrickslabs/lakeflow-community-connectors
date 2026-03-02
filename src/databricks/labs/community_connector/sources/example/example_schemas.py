"""Schemas, metadata, and constants for the Example connector.

The ``metrics`` table is hidden (not discoverable via the API), so its
Spark schema and metadata must be hard-coded here.  All other table
schemas are fetched at runtime from ``GET /tables/{table}/schema`` and
converted with ``build_spark_type``.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Maps API type strings to Spark SQL types.  ``integer`` maps to
# ``LongType`` (not ``IntegerType``) to avoid overflow on large values.
SPARK_TYPE_MAP = {
    "string": StringType(),
    "integer": LongType(),
    "double": DoubleType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}

# Tables whose ingestion type cannot be inferred from metadata alone.
# The API does not expose ingestion type directly; these overrides are
# applied before the fallback logic (cursor_field present → cdc, absent → snapshot).
INGESTION_TYPE_OVERRIDES = {
    "metrics": "cdc",
    "events": "append",
    "orders": "cdc_with_deletes",
}

# Hard-coded schema for the hidden ``metrics`` table whose /schema
# endpoint returns 404.  The ``value`` column is a nested struct.
METRICS_SCHEMA = StructType(
    [
        StructField("metric_id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField(
            "value",
            StructType(
                [
                    StructField("count", LongType(), nullable=True),
                    StructField("label", StringType(), nullable=True),
                    StructField("measure", DoubleType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField("host", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

METRICS_METADATA = {
    "primary_keys": ["metric_id"],
    "cursor_field": "updated_at",
}

RETRIABLE_STATUS_CODES = {429, 500, 503}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry


def build_spark_type(field_descriptor: dict) -> StructField:
    """Convert an API field descriptor to a Spark ``StructField``.

    Recurses into nested ``fields`` for ``struct`` types so the full
    hierarchy is preserved without flattening.
    """
    field_type = field_descriptor["type"]
    if field_type == "struct":
        children = [build_spark_type(f) for f in field_descriptor.get("fields", [])]
        spark_type = StructType(children)
    else:
        spark_type = SPARK_TYPE_MAP[field_type]
    return StructField(
        field_descriptor["name"],
        spark_type,
        nullable=field_descriptor.get("nullable", True),
    )
