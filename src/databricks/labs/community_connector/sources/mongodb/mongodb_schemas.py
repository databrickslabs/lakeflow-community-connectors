"""Deterministic Spark schema for the MongoDB connector bronze layer.

The schema is a pure constant — it never samples live data — so it satisfies the
framework harness's determinism contract (``get_table_schema`` must be a pure
function of ``(table, table_options)``). The full document lands as a single
VARIANT column (STRING fallback on runtimes without ``VariantType``); metadata
columns carry CDC plumbing.
"""

from __future__ import annotations

from pyspark.sql.types import LongType, StringType, StructField, StructType

from databricks.labs.community_connector.sources.mongodb.mongodb_utils import (
    COL_CLUSTER_TIME,
    COL_COLLECTION,
    COL_DB,
    COL_DOCUMENT,
    COL_ID,
    COL_OP,
)

try:
    from pyspark.sql.types import VariantType

    HAS_VARIANT = True
except ImportError:  # pyspark < 4.0 / DBR < 17.1
    HAS_VARIANT = False


def document_field_type(as_variant: bool = False):
    """Type for the ``document`` column.

    STRING by default — works in BOTH batch and streaming. VARIANT output from a
    Python DataSource is **not supported in Structured Streaming** on current DBR
    (``Not implemented. VariantType`` in a streaming MicroBatchScan), so VARIANT is
    opt-in (``document_as_variant=true``, snapshot/append only) and STRING bronze +
    ``parse_json`` in silver is the safe default. The connector emits JSON either way.
    """
    return VariantType() if (as_variant and HAS_VARIANT) else StringType()


def variant_bronze_schema(as_variant: bool = False) -> StructType:
    """Fixed bronze schema returned for every MongoDB table (deterministic)."""
    return StructType(
        [
            StructField(COL_ID, StringType(), nullable=False),
            StructField(COL_OP, StringType(), nullable=True),
            StructField(COL_CLUSTER_TIME, LongType(), nullable=True),
            StructField(COL_DB, StringType(), nullable=True),
            StructField(COL_COLLECTION, StringType(), nullable=True),
            StructField(COL_DOCUMENT, document_field_type(as_variant), nullable=True),
        ]
    )
