"""Static schema definitions for the Energy Quantified connector.

Schemas are derived from the Energy Quantified REST API (the public
SaaS endpoint at ``https://app.energyquantified.com/api``).  See
``energy_quantified_api_doc.md`` for endpoint references and
per-table edge cases.

Design rules followed by every schema below:
- ``LongType`` everywhere an integer might exceed 32 bits.  None of
  the Energy Quantified counters are routinely large, but the project
  rule is to standardise on ``LongType`` regardless.
- ``StructType`` for shape-stable nested objects (place / subscription)
  rather than ``MapType``.  No genuinely open-ended dicts are present
  in EQ responses, so ``MapType(StringType, StringType)`` is not used.
- Flat top-level columns for everything that participates in a primary
  key.  In particular, ``curve.name`` is hoisted to a top-level
  ``curve_name`` column on every non-``curves`` table so the framework
  (which does not walk nested structs for PKs) can find it.
- All datetimes are stored as ISO 8601 strings; downstream consumers
  can ``CAST(... AS TIMESTAMP)`` per their preferred timezone policy.
  The connector normalises everything to UTC at the wire by passing
  ``timezone=UTC`` on queries.
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# Reusable nested struct definitions
# =============================================================================

PLACE_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("key", StringType(), True),
        StructField("name", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("fuels", ArrayType(StringType()), True),
        StructField("areas", ArrayType(StringType()), True),
    ]
)

SUBSCRIPTION_STRUCT = StructType(
    [
        StructField("access", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

# Curve sub-object embedded on timeseries / instance / period / ohlc /
# srmc responses.  Kept slim — the full curve catalog lives in the
# ``curves`` table; here we keep only the fields useful for joins and
# self-describing metadata at the row level.
EMBEDDED_CURVE_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("curve_type", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("area", StringType(), True),
        StructField("frequency", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("denominator", StringType(), True),
        StructField("source", StringType(), True),
        StructField("commodity", StringType(), True),
    ]
)

SRMC_OPTIONS_STRUCT = StructType(
    [
        StructField("efficiency", DoubleType(), True),
        StructField("carbon_emissions", DoubleType(), True),
        StructField("api2_tonne_to_mwh", DoubleType(), True),
        StructField("gas_therm_to_mwh", DoubleType(), True),
        StructField("carbon_tax_area", StringType(), True),
    ]
)


# =============================================================================
# Table Schemas
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    # ---- /metadata/curves/ catalog ----
    "curves": StructType(
        [
            StructField("name", StringType(), False),
            StructField("curve_type", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("area", StringType(), True),
            StructField("area_sink", StringType(), True),
            StructField("area_source", StringType(), True),
            StructField("place", PLACE_STRUCT, True),
            StructField("frequency", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("categories", ArrayType(StringType()), True),
            StructField("unit", StringType(), True),
            StructField("denominator", StringType(), True),
            StructField("source", StringType(), True),
            StructField("commodity", StringType(), True),
            StructField("subscription", SUBSCRIPTION_STRUCT, True),
        ]
    ),
    # ---- /timeseries/{curve_name}/ ----
    # One row per data point with curve name and resolution metadata
    # stamped flat for easy joining.  Scenario ensembles (``s``) are
    # preserved as a nullable array; this is the one place ArrayType
    # is genuinely necessary because the ensemble size is fixed (40).
    "timeseries": StructType(
        [
            StructField("curve_name", StringType(), False),
            StructField("datetime", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("scenarios", ArrayType(DoubleType()), True),
            StructField("unit", StringType(), True),
            StructField("denominator", StringType(), True),
            StructField("frequency", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("curve", EMBEDDED_CURVE_STRUCT, True),
        ]
    ),
    # ---- /instances/{curve_name}/ ----
    # One row per (instance, delivery) tuple.  ``issued`` and ``tag``
    # together identify the forecast issue; ``datetime`` is the
    # delivery timestamp inside the forecast horizon.
    "instances": StructType(
        [
            StructField("curve_name", StringType(), False),
            StructField("issued", StringType(), False),
            StructField("tag", StringType(), False),
            StructField("datetime", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("denominator", StringType(), True),
            StructField("frequency", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("curve", EMBEDDED_CURVE_STRUCT, True),
        ]
    ),
    # ---- /periods/{curve_name}/ ----
    # Open-interval rows.  ``end`` may be null for ongoing periods.
    # Because EQ allows nullable ``end`` we fall back to (curve_name,
    # begin) as the effective PK and surface ``end`` as a regular
    # nullable column.  ``capacity`` is populated for outage curves
    # only.
    "periods": StructType(
        [
            StructField("curve_name", StringType(), False),
            StructField("begin", StringType(), False),
            StructField("end", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("capacity", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("denominator", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("curve", EMBEDDED_CURVE_STRUCT, True),
        ]
    ),
    # ---- /ohlc/{curve_name}/ ----
    # Trading-day OHLC rows.  ``product`` is exploded so the PK
    # columns (``traded_at``, ``period``, ``delivery``) live at top
    # level — the framework cannot use dotted-path PKs.
    "ohlc": StructType(
        [
            StructField("curve_name", StringType(), False),
            StructField("traded_at", StringType(), False),
            StructField("period", StringType(), False),
            StructField("front", LongType(), True),
            StructField("delivery", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("settlement", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("open_interest", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("denominator", StringType(), True),
            StructField("curve", EMBEDDED_CURVE_STRUCT, True),
        ]
    ),
    # ---- /srmc/{curve_name}/timeseries/ ----
    # Daily SRMC values for the configured contract.  All PK columns
    # (curve_name / date / period / front / delivery) are flat.
    # ``efficiency`` and ``carbon_emissions`` are hoisted out of the
    # response ``options`` block for auditability while the full
    # struct is retained too.
    "srmc": StructType(
        [
            StructField("curve_name", StringType(), False),
            StructField("date", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("period", StringType(), False),
            StructField("front", LongType(), True),
            StructField("delivery", StringType(), True),
            StructField("efficiency", DoubleType(), True),
            StructField("carbon_emissions", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("denominator", StringType(), True),
            StructField("frequency", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("options", SRMC_OPTIONS_STRUCT, True),
            StructField("curve", EMBEDDED_CURVE_STRUCT, True),
        ]
    ),
}


# =============================================================================
# Table Metadata
# =============================================================================
#
# ``primary_keys`` lists flat top-level columns.  Dotted paths are not
# permitted by the framework, so anything nested has been hoisted in
# the schema above.

TABLE_METADATA: dict[str, dict] = {
    "curves": {
        "primary_keys": ["name"],
        "ingestion_type": "snapshot",
    },
    "timeseries": {
        "primary_keys": ["curve_name", "datetime"],
        "cursor_field": "datetime",
        "ingestion_type": "append",
    },
    "instances": {
        "primary_keys": ["curve_name", "issued", "tag", "datetime"],
        "cursor_field": "issued",
        "ingestion_type": "append",
    },
    "periods": {
        "primary_keys": ["curve_name", "begin"],
        "cursor_field": "begin",
        "ingestion_type": "cdc",
    },
    "ohlc": {
        "primary_keys": ["curve_name", "traded_at", "period", "front", "delivery"],
        "cursor_field": "traded_at",
        "ingestion_type": "append",
    },
    "srmc": {
        "primary_keys": ["curve_name", "date", "period", "front", "delivery"],
        "cursor_field": "date",
        "ingestion_type": "append",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())


# =============================================================================
# Endpoint classification
# =============================================================================

# Tables that use range-query (begin/end) reads — these can be split
# across partitions.  ``instances`` uses a backward-walking cursor and
# ``curves`` is a single paginated catalog; both stay on the single-
# driver path.
PARTITIONED_TABLES: frozenset[str] = frozenset({"timeseries", "periods", "ohlc", "srmc"})

# Tables that require a ``curve_name`` table option.  Everything
# except ``curves`` is curve-scoped.
CURVE_SCOPED_TABLES: frozenset[str] = frozenset(
    {"timeseries", "instances", "periods", "ohlc", "srmc"}
)


# =============================================================================
# Retry & request constants
# =============================================================================

# Energy Quantified's public SDK retries on connection / 5xx errors,
# not on 429.  The CLAUDE.md instructions require explicit 429 handling
# with Retry-After respect, so the connector treats 429 as retriable
# alongside 5xx with exponential backoff + jitter.
RETRIABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry
REQUEST_CONNECT_TIMEOUT = 5
REQUEST_READ_TIMEOUT = 30

# Proactive rate limiting.  The official SDK enforces ~15 req/s by
# sleeping 0.0667s between calls; the connector replicates this so a
# single Spark driver can't out-shoot Energy Quantified's per-account
# quota.  Per-executor pacing is independent because each partition
# constructs its own client.
MIN_INTERVAL_BETWEEN_REQUESTS = 0.0667  # seconds (~15 req/s)


# =============================================================================
# Defaults
# =============================================================================

API_BASE_URL = "https://app.energyquantified.com/api"

# Default lookback window for ``periods`` (a CDC table) when no
# explicit ``lookback_days`` is provided.  EQ periods can be revised
# (notably outages), so we re-scan a small recent window to catch
# updates.  Seven days matches the recommendation in the api_doc.
DEFAULT_LOOKBACK_DAYS = 7

# Default partition window for range-query tables.  Each partition
# covers up to this many days of data; tune via the
# ``partition_days`` table option per workload.
DEFAULT_PARTITION_DAYS = 30

# Default initial start date for range-query tables when the user has
# not supplied ``start_date`` AND no checkpoint exists.  EQ's trial
# accounts are capped at 30 days, paying accounts at full history;
# 30 days is a safe default that works for both.
DEFAULT_INITIAL_LOOKBACK_DAYS = 30

# Catalog page size.  EQ's stated default is 50; 500 reduces round
# trips and the api_doc notes it is known to work in practice.
DEFAULT_CURVES_PAGE_SIZE = 500

# Per-call instance list cap.  EQ docs cap this at 25 (or 10 with
# ensembles); the connector uses 25 and walks backward via
# ``issued_at_latest`` for incremental reads.
DEFAULT_INSTANCES_LIMIT = 25
