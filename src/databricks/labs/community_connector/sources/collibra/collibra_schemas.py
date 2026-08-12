"""Static schema definitions and table metadata for the Collibra connector.

Schemas are derived from the Collibra Data Intelligence Platform Core REST
API v2 documentation (see ``collibra_api_doc.md``).

Design notes:
    * UUID identifiers (``id``, ``createdBy``, ``lastModifiedBy``, and the
      ``id`` inside nested resource references) are ``StringType`` — never
      cast UUIDs to binary/long.
    * ``createdOn`` / ``lastModifiedOn`` are UTC epoch milliseconds and use
      ``LongType`` (not ``IntegerType``) to avoid overflow.
    * Nested resource references (``domain``, ``type``, ``status``,
      ``community``, ``asset``, ``role``, ``baseResource``, ``owner``) are kept
      as ``StructType`` — not flattened.
    * Every row carries a non-null ``collibra_org`` column for multi-instance
      disambiguation (mirrors how the Shopify connector adds ``shop``).
    * The attribute ``value`` field is polymorphic (its type depends on
      ``attributeDiscriminator``). We keep the raw value as a ``StringType``
      plus the discriminator; downstream jobs coerce/join as needed.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# =============================================================================
# Reusable nested struct definitions
# =============================================================================

# Generic Collibra resource reference: {id, name, resourceType,
# resourceDiscriminator}. ``resourceType`` is deprecated in favour of
# ``resourceDiscriminator`` (2024.10+) but both are still emitted, so we keep
# both. ``name`` is absent on some references (e.g. responsibility owners), in
# which case the field is simply null.
RESOURCE_REF_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("resourceType", StringType(), True),
        StructField("resourceDiscriminator", StringType(), True),
    ]
)


# =============================================================================
# Table schemas
# =============================================================================

ASSETS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("createdBy", StringType(), True),
        StructField("createdOn", LongType(), True),
        StructField("lastModifiedBy", StringType(), True),
        StructField("lastModifiedOn", LongType(), True),
        StructField("system", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("articulationScore", DoubleType(), True),
        StructField("excludedFromAutoHyperlinking", BooleanType(), True),
        StructField("domain", RESOURCE_REF_STRUCT, True),
        StructField("type", RESOURCE_REF_STRUCT, True),
        StructField("status", RESOURCE_REF_STRUCT, True),
        StructField("avgRating", DoubleType(), True),
        StructField("ratingsCount", IntegerType(), True),
        StructField("collibra_org", StringType(), False),
    ]
)

ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("createdBy", StringType(), True),
        StructField("createdOn", LongType(), True),
        StructField("lastModifiedBy", StringType(), True),
        StructField("lastModifiedOn", LongType(), True),
        StructField("system", BooleanType(), True),
        StructField("attributeDiscriminator", StringType(), True),
        # Polymorphic value kept as a raw string; use attributeDiscriminator
        # downstream to coerce (StringAttribute / BooleanAttribute / ...).
        StructField("value", StringType(), True),
        StructField("type", RESOURCE_REF_STRUCT, True),
        StructField("asset", RESOURCE_REF_STRUCT, True),
        StructField("collibra_org", StringType(), False),
    ]
)

RESPONSIBILITIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("createdBy", StringType(), True),
        StructField("createdOn", LongType(), True),
        StructField("lastModifiedBy", StringType(), True),
        StructField("lastModifiedOn", LongType(), True),
        StructField("system", BooleanType(), True),
        StructField("role", RESOURCE_REF_STRUCT, True),
        # baseResource is where the role was directly assigned (asset, domain,
        # or community). For inherited responsibilities this points at the
        # parent, not the asset — see collibra_api_doc.md gotcha #1.
        StructField("baseResource", RESOURCE_REF_STRUCT, True),
        # owner.resourceDiscriminator is "User" or "Group".
        StructField("owner", RESOURCE_REF_STRUCT, True),
        StructField("collibra_org", StringType(), False),
    ]
)

DOMAINS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("createdBy", StringType(), True),
        StructField("createdOn", LongType(), True),
        StructField("lastModifiedBy", StringType(), True),
        StructField("lastModifiedOn", LongType(), True),
        StructField("system", BooleanType(), True),
        StructField("name", StringType(), True),
        # Domain description is a native field (not stored as an attribute).
        StructField("description", StringType(), True),
        StructField("meta", BooleanType(), True),
        StructField("excludedFromAutoHyperlinking", BooleanType(), True),
        StructField("community", RESOURCE_REF_STRUCT, True),
        StructField("type", RESOURCE_REF_STRUCT, True),
        StructField("collibra_org", StringType(), False),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "assets": ASSETS_SCHEMA,
    "attributes": ATTRIBUTES_SCHEMA,
    "responsibilities": RESPONSIBILITIES_SCHEMA,
    "domains": DOMAINS_SCHEMA,
}


# =============================================================================
# Table metadata
# =============================================================================

TABLE_METADATA: dict[str, dict] = {
    "assets": {
        "primary_keys": ["id"],
        "cursor_field": "lastModifiedOn",
        "ingestion_type": "cdc",
    },
    "attributes": {
        "primary_keys": ["id"],
        "cursor_field": "lastModifiedOn",
        "ingestion_type": "cdc",
    },
    "responsibilities": {
        "primary_keys": ["id"],
        "cursor_field": "lastModifiedOn",
        "ingestion_type": "cdc",
    },
    "domains": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())


# =============================================================================
# HTTP retry tuning
# =============================================================================

RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

# Collibra caps page size at 1000 across all endpoints.
MAX_PAGE_SIZE = 1000
DEFAULT_PAGE_SIZE = 1000
