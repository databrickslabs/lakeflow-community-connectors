"""Static schema definitions and table metadata for the Microsoft Purview connector.

Schemas are derived from the Microsoft Purview **Unified Catalog Data Governance**
REST API (data plane, ``api-version=2026-03-20-preview``). See
``microsoft_purview_api_doc.md`` and the live Microsoft Learn reference:
https://learn.microsoft.com/en-us/rest/api/purview/purview-unified-catalog

Design notes:
    * UUID identifiers (``id``, ``domain``, ``parentId``, the ``id`` inside
      contacts, and the ``*By`` audit fields inside ``systemData``) are
      ``StringType`` — never cast UUIDs to binary/long.
    * ``systemData.createdAt`` / ``systemData.lastModifiedAt`` /
      ``systemData.expiredAt`` are ISO-8601 UTC date-time **strings** in this
      API (not epoch ms like the older Atlas Data Map surface). They are kept
      as ``StringType``; the incremental cursor sorts them lexicographically
      (ISO-8601 UTC strings are lexicographically ordered).
    * Nested objects (``systemData``, ``contacts``, ``thumbnail``,
      ``additionalProperties``) are kept as ``StructType`` — never ``MapType``.
    * Repeated objects (``contacts.owner``, ``termsOfUse``, ``domains``,
      ``managedAttributes``, ...) are ``ArrayType(StructType(...))``.
    * Integer-valued fields use ``LongType`` (never ``IntegerType``) to avoid
      overflow: ``additionalProperties.assetCount`` is int64 in the API, and
      ``activeSubscriberCount`` is kept ``LongType`` for headroom.
    * Every row carries a non-null ``purview_tenant_id`` column for
      multi-tenant disambiguation (mirrors Collibra's ``collibra_org``).
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# =============================================================================
# Reusable nested struct definitions
# =============================================================================

# CatalogModelSystemDataWithExpired — audit metadata common to every catalog
# entity. ``*At`` fields are ISO-8601 UTC strings; ``*By`` fields are AAD oids.
SYSTEM_DATA_STRUCT = StructType(
    [
        StructField("createdAt", StringType(), True),
        StructField("createdBy", StringType(), True),
        StructField("lastModifiedAt", StringType(), True),
        StructField("lastModifiedBy", StringType(), True),
        StructField("expiredAt", StringType(), True),
        StructField("expiredBy", StringType(), True),
        StructField("provisioningState", StringType(), True),
    ]
)

# CatalogModelContactsValueInner — one contact assignment.
CONTACT_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

# ContactsMap — contacts keyed by role. Kept as an explicit struct (not a map)
# so each role is a first-class, typed array.
CONTACTS_STRUCT = StructType(
    [
        StructField("owner", ArrayType(CONTACT_STRUCT), True),
        StructField("expert", ArrayType(CONTACT_STRUCT), True),
        StructField("databaseAdmin", ArrayType(CONTACT_STRUCT), True),
    ]
)

# CatalogModelExternalLink — used by data product termsOfUse / documentation.
EXTERNAL_LINK_STRUCT = StructType(
    [
        StructField("url", StringType(), True),
        StructField("name", StringType(), True),
        StructField("dataAssetId", StringType(), True),
    ]
)

# CatalogModelManagedAttribute — governance custom attribute on any entity.
MANAGED_ATTRIBUTE_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("isRequired", BooleanType(), True),
    ]
)

# CatalogModelTermResource — external resource link on a term.
TERM_RESOURCE_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# CatalogModelDataProductAllOfAdditionalProperties.
ADDITIONAL_PROPERTIES_STRUCT = StructType(
    [
        StructField("assetCount", LongType(), True),
    ]
)

# CatalogModelDomainAllOfThumbnail.
THUMBNAIL_STRUCT = StructType(
    [
        StructField("color", StringType(), True),
    ]
)

# CatalogModelRelatedCollectionParentCollection.
PARENT_COLLECTION_STRUCT = StructType(
    [
        StructField("refName", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

# CatalogModelRelatedCollection.
RELATED_COLLECTION_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("friendlyName", StringType(), True),
        StructField("parentCollection", PARENT_COLLECTION_STRUCT, True),
    ]
)

# CatalogModelPlatformDomain — a physical-platform mapping under a domain.
PLATFORM_DOMAIN_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("friendlyName", StringType(), True),
        StructField("relatedCollections", ArrayType(RELATED_COLLECTION_STRUCT), True),
    ]
)


# =============================================================================
# Table schemas
# =============================================================================

BUSINESS_DOMAINS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("parentId", StringType(), True),
        StructField("status", StringType(), True),
        StructField("type", StringType(), True),
        StructField("isRestricted", BooleanType(), True),
        StructField("thumbnail", THUMBNAIL_STRUCT, True),
        StructField("domains", ArrayType(PLATFORM_DOMAIN_STRUCT), True),
        StructField("managedAttributes", ArrayType(MANAGED_ATTRIBUTE_STRUCT), True),
        StructField("systemData", SYSTEM_DATA_STRUCT, True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)

DATA_PRODUCTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("businessUse", StringType(), True),
        StructField("status", StringType(), True),
        StructField("endorsed", BooleanType(), True),
        StructField("updateFrequency", StringType(), True),
        StructField("sensitivityLabel", StringType(), True),
        StructField("audience", ArrayType(StringType()), True),
        StructField("dataQualityScore", DoubleType(), True),
        StructField("activeSubscriberCount", LongType(), True),
        StructField("contacts", CONTACTS_STRUCT, True),
        StructField("termsOfUse", ArrayType(EXTERNAL_LINK_STRUCT), True),
        StructField("documentation", ArrayType(EXTERNAL_LINK_STRUCT), True),
        StructField("managedAttributes", ArrayType(MANAGED_ATTRIBUTE_STRUCT), True),
        StructField("additionalProperties", ADDITIONAL_PROPERTIES_STRUCT, True),
        StructField("systemData", SYSTEM_DATA_STRUCT, True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)

TERMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("status", StringType(), True),
        StructField("parentId", StringType(), True),
        StructField("isLeaf", BooleanType(), True),
        StructField("acronyms", ArrayType(StringType()), True),
        StructField("contacts", CONTACTS_STRUCT, True),
        StructField("resources", ArrayType(TERM_RESOURCE_STRUCT), True),
        StructField("managedAttributes", ArrayType(MANAGED_ATTRIBUTE_STRUCT), True),
        StructField("systemData", SYSTEM_DATA_STRUCT, True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "business_domains": BUSINESS_DOMAINS_SCHEMA,
    "data_products": DATA_PRODUCTS_SCHEMA,
    "terms": TERMS_SCHEMA,
}


# =============================================================================
# Table metadata
# =============================================================================

# The incremental cursor lives at a nested path: ``systemData.lastModifiedAt``.
# The framework supports dotted-path cursor_field / primary_keys.
CURSOR_FIELD = "systemData.lastModifiedAt"

TABLE_METADATA: dict[str, dict] = {
    # Governance domains are a low-volume taxonomy that changes infrequently —
    # snapshotted in full each run (mirrors Collibra's ``domains``).
    "business_domains": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "data_products": {
        "primary_keys": ["id"],
        "cursor_field": CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
    "terms": {
        "primary_keys": ["id"],
        "cursor_field": CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())


# =============================================================================
# API + HTTP tuning
# =============================================================================

# Unified Catalog Data Governance data-plane API version (public preview).
API_VERSION = "2026-03-20-preview"

# Shared data-plane endpoint for the Unified Catalog (not per-account).
DEFAULT_ENDPOINT = "https://api.purview-service.microsoft.com"

# ``top`` page size for skip/top-paginated endpoints (dataProducts / terms).
# The API does not document a hard max; 100 is a conservative default that
# keeps well under the per-operation rate windows (100 req / 20 s).
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000

RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry
