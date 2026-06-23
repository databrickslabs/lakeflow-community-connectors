"""Static schemas and metadata for the Tabs Platform connector.

Kept in a dedicated module so the main connector file stays focused on
read logic. Every table's StructType and ingestion metadata lives here.

Type choices follow the API doc's field-type mapping:

* ``string (YYYY-MM-DD)`` date fields → ``DateType``.
* ``string (ISO 8601)`` datetime fields → ``TimestampType``.
* Money fields (``total``, ``balanceRemaining``, ``unitPrice``, ``amount``)
  → ``DecimalType(18, 2)`` to avoid float rounding on currency values.
* Nested objects → ``StructType``; nested arrays → ``ArrayType``.

Incremental cursors on Tabs are **date-only** strings (YYYY-MM-DD) even
though the underlying field (``lastUpdatedAt`` / ``receivedAt``) is an ISO
timestamp — the API's ``filter`` parameter only accepts the date part. The
connector therefore tracks the cursor as a YYYY-MM-DD string and applies a
configurable lookback (default 2 days) to avoid missing same-day edits.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------
# Shared nested structs
# --------------------------------------------------------------------------

# externalIds[] — present on most objects. ``metadata`` is free-form
# key-value context; modelled as a JSON-serialised string to avoid an
# unbounded/heterogeneous MapType schema mismatch.
_EXTERNAL_ID_STRUCT = StructType(
    [
        StructField("externalId", StringType()),
        StructField("sourceType", StringType()),
        StructField("metadata", MapType(StringType(), StringType())),
    ]
)

_ADDRESS_STRUCT = StructType(
    [
        StructField("address1", StringType()),
        StructField("address2", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("zip", StringType()),
        StructField("addressee", StringType()),
        StructField("externalId", StringType()),
    ]
)

# item reference embedded in an invoice line item (ItemV3Dto).
_LINE_ITEM_ITEM_STRUCT = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("sku", StringType()),
    ]
)

# lineItems[] embedded inside an invoice. Also re-used (exploded) into the
# ``invoice_line_items`` child table.
_LINE_ITEM_STRUCT = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("description", StringType()),
        StructField("quantity", DecimalType(18, 4)),
        StructField("unitPrice", DecimalType(18, 2)),
        StructField("total", DecimalType(18, 2)),
        StructField("item", _LINE_ITEM_ITEM_STRUCT),
    ]
)

_BILLING_SCHEDULE_STRUCT = StructType(
    [
        StructField("interval", StringType()),
        StructField("intervalFrequency", DecimalType(18, 4)),
        StructField("billingType", StringType()),
        StructField("pricingType", StringType()),
        StructField("netPaymentTerms", LongType()),
        StructField("isArrears", BooleanType()),
        StructField("isRecurring", BooleanType()),
        StructField("name", StringType()),
        StructField("description", StringType()),
        StructField("quantity", DecimalType(18, 4)),
        StructField("invoiceType", StringType()),
        StructField(
            "pricingTiers",
            ArrayType(
                StructType(
                    [
                        StructField("tierNumber", LongType()),
                        StructField("amount", DecimalType(18, 2)),
                        StructField("amountType", StringType()),
                        StructField("tierMinimum", DecimalType(18, 4)),
                    ]
                )
            ),
        ),
    ]
)


# --------------------------------------------------------------------------
# Per-table schemas
# --------------------------------------------------------------------------

_INVOICES_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("issueDate", DateType()),
        StructField("dueDate", DateType()),
        StructField("originalIssueDate", DateType()),
        StructField("originalDueDate", DateType()),
        StructField("sentDate", DateType()),
        StructField("createdAt", TimestampType()),
        StructField("lastUpdatedAt", TimestampType()),
        StructField("status", StringType()),
        StructField("customerId", StringType()),
        StructField("contractId", StringType()),
        StructField("obligationId", StringType()),
        StructField("source", StringType()),
        StructField("total", DecimalType(18, 2)),
        StructField("balanceRemaining", DecimalType(18, 2)),
        StructField("externalIds", ArrayType(_EXTERNAL_ID_STRUCT)),
        StructField("lineItems", ArrayType(_LINE_ITEM_STRUCT)),
    ]
)

# invoice_line_items — exploded from invoices.lineItems. ``invoiceId`` is the
# foreign key back to the parent invoice; ``invoiceLastUpdatedAt`` carries the
# parent cursor so the child table can be ingested incrementally in lockstep.
_INVOICE_LINE_ITEMS_SCHEMA = StructType(
    [
        StructField("invoiceId", StringType()),
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("description", StringType()),
        StructField("quantity", DecimalType(18, 4)),
        StructField("unitPrice", DecimalType(18, 2)),
        StructField("total", DecimalType(18, 2)),
        StructField("item", _LINE_ITEM_ITEM_STRUCT),
        StructField("invoiceLastUpdatedAt", TimestampType()),
    ]
)

_PAYMENTS_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("invoiceId", StringType()),
        StructField("customerId", StringType()),
        StructField("total", DecimalType(18, 2)),
        StructField("receivedAt", TimestampType()),
        StructField("status", StringType()),
        StructField("type", StringType()),
        StructField("method", StringType()),
        StructField("reference", StringType()),
        StructField("stripeId", StringType()),
        StructField("externalIds", ArrayType(_EXTERNAL_ID_STRUCT)),
    ]
)

_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("defaultCurrency", StringType()),
        StructField("primaryBillingContactName", StringType()),
        StructField("primaryBillingContactEmail", StringType()),
        StructField("secondaryBillingContacts", ArrayType(StringType())),
        StructField("billingAddress", _ADDRESS_STRUCT),
        StructField("shippingAddress", _ADDRESS_STRUCT),
        StructField("externalIds", ArrayType(_EXTERNAL_ID_STRUCT)),
        StructField("lastUpdatedAt", TimestampType()),
    ]
)

_CONTRACTS_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("fileName", StringType()),
        StructField("status", StringType()),
        StructField("customerId", StringType()),
        StructField("customerName", StringType()),
        StructField("uploaderId", StringType()),
        StructField("source", StringType()),
        StructField("createdAt", TimestampType()),
        StructField("lastUpdatedAt", TimestampType()),
        StructField("deletedAt", TimestampType()),
        StructField("externalIds", ArrayType(_EXTERNAL_ID_STRUCT)),
    ]
)

_OBLIGATIONS_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("contractId", StringType()),
        StructField("customerId", StringType()),
        StructField("categoryId", StringType()),
        StructField("serviceStartDate", DateType()),
        StructField("serviceEndDate", DateType()),
        StructField("startDate", DateType()),
        StructField("endDate", DateType()),
        StructField("billingSchedule", _BILLING_SCHEDULE_STRUCT),
        StructField("erpItemId", StringType()),
        StructField("eventTypeId", StringType()),
    ]
)

_ITEMS_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("sku", StringType()),
        StructField("externalIds", ArrayType(_EXTERNAL_ID_STRUCT)),
        StructField("lastUpdatedAt", TimestampType()),
    ]
)

_CATEGORIES_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("lastUpdatedAt", TimestampType()),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "invoices": _INVOICES_SCHEMA,
    "invoice_line_items": _INVOICE_LINE_ITEMS_SCHEMA,
    "payments": _PAYMENTS_SCHEMA,
    "customers": _CUSTOMERS_SCHEMA,
    "contracts": _CONTRACTS_SCHEMA,
    "obligations": _OBLIGATIONS_SCHEMA,
    "items": _ITEMS_SCHEMA,
    "categories": _CATEGORIES_SCHEMA,
}


# Ingestion metadata. For cdc / cdc_with_deletes tables both ``primary_keys``
# and ``cursor_field`` are required. ``cursor_field`` names the connector's
# row-level field that carries the incremental watermark. For Tabs the API
# filter only accepts the date part, but the row still carries the full ISO
# timestamp, so the cursor_field points at the timestamp column.
TABLE_METADATA: dict[str, dict] = {
    "invoices": {
        "primary_keys": ["id"],
        "cursor_field": "lastUpdatedAt",
        "ingestion_type": "cdc",
    },
    # Child table — inherits the invoice cursor. Composite PK of
    # (invoiceId, id) since a line-item id is only unique within its invoice.
    "invoice_line_items": {
        "primary_keys": ["invoiceId", "id"],
        "cursor_field": "invoiceLastUpdatedAt",
        "ingestion_type": "cdc",
    },
    "payments": {
        "primary_keys": ["id"],
        "cursor_field": "receivedAt",
        "ingestion_type": "cdc",
    },
    "customers": {
        "primary_keys": ["id"],
        "cursor_field": "lastUpdatedAt",
        "ingestion_type": "cdc",
    },
    # Contracts expose a soft-delete field (``deletedAt``); model as
    # cdc_with_deletes so the framework calls read_table_deletes().
    "contracts": {
        "primary_keys": ["id"],
        "cursor_field": "lastUpdatedAt",
        "ingestion_type": "cdc_with_deletes",
    },
    "obligations": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "items": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "categories": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())

# Tables read incrementally via the date-range partitioned-stream path.
INCREMENTAL_TABLES = {
    "invoices",
    "invoice_line_items",
    "payments",
    "customers",
    "contracts",
}

# Maps each incremental table to the API endpoint path it reads from and the
# API filter field used for the date-range query. ``invoice_line_items`` reads
# the invoices endpoint (it is an explode of invoices.lineItems).
INCREMENTAL_SOURCE = {
    "invoices": {"path": "/v3/invoices", "filter_field": "lastUpdatedAt"},
    "invoice_line_items": {"path": "/v3/invoices", "filter_field": "lastUpdatedAt"},
    "payments": {"path": "/v3/payments", "filter_field": "receivedAt"},
    "customers": {"path": "/v3/customers", "filter_field": "lastUpdatedAt"},
    "contracts": {"path": "/v3/contracts", "filter_field": "lastUpdatedAt"},
}

# Snapshot table → endpoint path.
SNAPSHOT_SOURCE = {
    "obligations": "/v3/obligations",
    "items": "/v3/items",
    "categories": "/v3/categories",
}
