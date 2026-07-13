"""Static schema and metadata definitions for the LSEG LDMS (RDMS) connector.

LDMS is curve-keyed rather than table-keyed: real data lives in hundreds of
thousands of individual curves discovered by metadata search. We expose a
small set of *logical* tables whose row shapes are effectively static; which
curves / datasets each returns is driven by per-table ``table_options``.

Reference: ``lseg_ldms_api_doc.md`` (LDMS REST API Interface Guide v25.0.0).

Field-type mapping (from the doc):
  * number (value)                         -> DoubleType
  * date / datetime (ValueDate,            -> TimestampType (ISO 8601, UTC)
    ForecastDate, LastUpdateTime)
  * CurveID / alias / tag values / status  -> StringType
  * ScenarioID                             -> IntegerType
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logical tables
# ---------------------------------------------------------------------------
CURVE_VALUES = "curve_values"
CURVE_METADATA = "curve_metadata"
TABULAR_DATA = "tabular_data"

SUPPORTED_TABLES: list[str] = [CURVE_VALUES, CURVE_METADATA, TABULAR_DATA]


# ---------------------------------------------------------------------------
# curve_values — one row per observation (from the CurveValues* responses)
# ---------------------------------------------------------------------------
# Every LDMS value is uniquely keyed by CurveID + ScenarioID + ForecastDate +
# ValueDate. Actuals use ForecastDate = 2000-01-01T00:00:00; ScenarioID = 0 for
# standard PointConnect data.
CURVE_VALUES_SCHEMA: StructType = StructType(
    [
        StructField("curve_id", StringType(), False),
        StructField("scenario_id", IntegerType(), False),
        StructField("forecast_date", TimestampType(), False),
        StructField("value_date", TimestampType(), False),
        StructField("value", DoubleType(), True),
        StructField("last_update_time", TimestampType(), True),
    ]
)


# ---------------------------------------------------------------------------
# curve_metadata — curve catalog (one row per matched curve)
# ---------------------------------------------------------------------------
# Real LDMS exposes one column per metadata tag (Metadata/TagTypes enumerates
# them). Tags vary per deployment, so we keep a small typed core plus a
# JSON-encoded ``metadata_json`` escape hatch for the full tag set — mirroring
# the ``*_json`` approach used by other connectors for schema-drift-prone data.
CURVE_METADATA_SCHEMA: StructType = StructType(
    [
        StructField("curve_id", StringType(), False),
        StructField("alias", StringType(), True),
        StructField("name", StringType(), True),
        StructField("metadata_json", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# tabular_data — provider tabular datasets (JODI, OPIS, IIR, ...)
# ---------------------------------------------------------------------------
# Columns are data-type dependent (discoverable via TabularData/DataFields).
# For the static bootstrap schema we model the canonical JODI shape documented
# as the primary-key example (country + product + flow + period). The connector
# projects the requested ``fields`` subset at read time and stamps the
# requested ``data_type`` onto every row.
TABULAR_DATA_SCHEMA: StructType = StructType(
    [
        StructField("data_type", StringType(), False),
        StructField("country", StringType(), True),
        StructField("product", StringType(), True),
        StructField("flow", StringType(), True),
        StructField("period", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    CURVE_VALUES: CURVE_VALUES_SCHEMA,
    CURVE_METADATA: CURVE_METADATA_SCHEMA,
    TABULAR_DATA: TABULAR_DATA_SCHEMA,
}


# ---------------------------------------------------------------------------
# Per-table metadata
# ---------------------------------------------------------------------------
# curve_values is append (POC) keyed on value_date; production callers can flip
# it to cdc keyed on last_update_time via the ``ingestion_mode`` table option
# (see LSEGLDMSLakeflowConnect.read_table_metadata). curve_metadata is a
# full-refresh catalog. tabular_data is a snapshot by default.
CURVE_VALUES_PRIMARY_KEYS: list[str] = [
    "curve_id",
    "scenario_id",
    "forecast_date",
    "value_date",
]

TABULAR_DATA_PRIMARY_KEYS: list[str] = [
    "data_type",
    "country",
    "product",
    "flow",
    "period",
]


def curve_values_metadata(ingestion_mode: str) -> dict:
    """Return curve_values metadata for the given ingestion mode.

    ``append`` (default, POC): cursor on ``value_date`` — actuals stream
    forward by observation date.
    ``cdc`` (production): cursor on ``last_update_time`` so restatements
    (corrections, new forecasts) upsert on the primary key. Deletes are not
    published by LDMS, so ``cdc`` (not ``cdc_with_deletes``) is correct.
    """
    if ingestion_mode == "cdc":
        return {
            "primary_keys": list(CURVE_VALUES_PRIMARY_KEYS),
            "cursor_field": "last_update_time",
            "ingestion_type": "cdc",
        }
    return {
        "primary_keys": list(CURVE_VALUES_PRIMARY_KEYS),
        "cursor_field": "value_date",
        "ingestion_type": "append",
    }


TABLE_METADATA: dict[str, dict] = {
    CURVE_METADATA: {
        "primary_keys": ["curve_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABULAR_DATA: {
        "primary_keys": list(TABULAR_DATA_PRIMARY_KEYS),
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}
