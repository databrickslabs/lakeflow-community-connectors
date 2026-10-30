"""Static schemas, metadata and endpoint routing for the e-conomic connector.

All four tables come from the e-conomic OpenAPI family
(``https://apis.e-conomic.com/``). Each domain is a separate "resource API"
with its own version in the URL path (``/{resource-api}/v{version}/{Resource}``):

* ``employees``, ``projects``, ``project_time_entries`` -> ``projectsapi``
* ``customers`` -> ``customersapi``

The record structures are flat (no nested objects / arrays). Nullable and
``false`` boolean properties are frequently *omitted* from responses, so
every column is declared nullable and missing keys surface as null.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Connection defaults
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://apis.e-conomic.com"
DEFAULT_PROJECTS_API_VERSION = "1.1.0"
DEFAULT_CUSTOMERS_API_VERSION = "3.1.0"

# Resource API identifiers -> connection option holding its version.
PROJECTS_API = "projectsapi"
CUSTOMERS_API = "customersapi"

# ---------------------------------------------------------------------------
# HTTP behaviour
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 6
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Safety bound on cursor pages followed within a single read call
# (1000 items per page -> 10M rows). Guards against a misbehaving cursor.
MAX_PAGES_PER_READ = 10_000

# ---------------------------------------------------------------------------
# Incremental read defaults (table options)
# ---------------------------------------------------------------------------

DEFAULT_MAX_RECORDS_PER_BATCH = 5000
# lastUpdated has whole-minute granularity in observed data and may be
# bumped by changes to related models; re-read a short window on every
# trigger and rely on PK upsert to de-duplicate.
DEFAULT_LOOKBACK_SECONDS = 600

# Format used for datetime literals in ``filter=`` expressions and for the
# stored cursor. The API returns naive ISO-8601 timestamps (no offset).
CURSOR_FORMAT = "%Y-%m-%dT%H:%M:%S"

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

EMPLOYEES_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("groupNumber", LongType(), True),
        StructField("name", StringType(), True),
        StructField("isBarred", BooleanType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customerNumber", LongType(), False),
        StructField("customerGroupNumber", LongType(), True),
        StructField("zone", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("access", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("address1", StringType(), True),
        StructField("address2", StringType(), True),
        StructField("postCode", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("county", StringType(), True),
        StructField("contactPerson", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("mobilePhone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("paymentDays", LongType(), True),
        StructField("paymentTermId", LongType(), True),
        StructField("bankAccount", StringType(), True),
        StructField("bankGiroNumber", StringType(), True),
        StructField("iban", StringType(), True),
        StructField("swift", StringType(), True),
        StructField("cvrNo", StringType(), True),
        StructField("vatNo", StringType(), True),
        StructField("pnr", StringType(), True),
        StructField("ourRef", LongType(), True),
        StructField("layoutGroup", LongType(), True),
        StructField("layoutGroupPrint", LongType(), True),
        StructField("defaultTheirRef", LongType(), True),
        StructField("defaultAttention", LongType(), True),
        StructField("language", LongType(), True),
        StructField("emailAttachmentLanguage", LongType(), True),
        StructField("discountGroupNumber", LongType(), True),
        StructField("priceGroupNumber", LongType(), True),
        StructField("defaultDiscountPct", DoubleType(), True),
        StructField("creditMaximum", DoubleType(), True),
        StructField("defaultDeliveryLocation", LongType(), True),
        StructField("eanLocationNumber", StringType(), True),
        StructField("voucherNumber", StringType(), True),
        StructField("recipientPeppol", StringType(), True),
        StructField("nemHandelReceiverType", LongType(), True),
        StructField("directDebitMandateDate", TimestampType(), True),
        StructField("defaultDisableEInvoicing", BooleanType(), True),
        StructField("defaultInvoiceRecipient", StringType(), True),
        StructField("defaultOrderRecipient", StringType(), True),
        StructField("defaultQuoteRecipient", StringType(), True),
        StructField("defaultReminderRecipient", StringType(), True),
        StructField("defaultStatementOfAccountRecipient", StringType(), True),
        StructField("defaultInvoiceTemplate", LongType(), True),
        StructField("defaultOrderTemplate", LongType(), True),
        StructField("defaultQuoteTemplate", LongType(), True),
        StructField("defaultReminderTemplate", LongType(), True),
        StructField("defaultStatementOfAccountTemplate", LongType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

PROJECTS_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("name", StringType(), True),
        StructField("projectGroupNumber", LongType(), True),
        StructField("customerNumber", LongType(), True),
        StructField("isClosed", BooleanType(), True),
        StructField("mileage", DoubleType(), True),
        StructField("isBarred", BooleanType(), True),
        StructField("isMainProject", BooleanType(), True),
        StructField("isMileageInvoiced", BooleanType(), True),
        StructField("lastUpdated", TimestampType(), True),
        StructField("responsibleEmployeeNumber", LongType(), True),
        StructField("otherResponsibleEmployeeNumber", LongType(), True),
        StructField("mainProjectNumber", LongType(), True),
        StructField("status", LongType(), True),
        StructField("description", StringType(), True),
        StructField("deliveryDate", TimestampType(), True),
        StructField("departmentNumber", LongType(), True),
        StructField("closedDate", TimestampType(), True),
        StructField("fixedPrice", DoubleType(), True),
        StructField("costPrice", DoubleType(), True),
        StructField("salesPrice", DoubleType(), True),
        StructField("invoicedTotal", DoubleType(), True),
        StructField("contactPersonId", LongType(), True),
        StructField("deliveryLocationNumber", LongType(), True),
        StructField("otherReference", StringType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

PROJECT_TIME_ENTRIES_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("projectNumber", LongType(), True),
        StructField("activityNumber", LongType(), True),
        StructField("employeeNumber", LongType(), True),
        StructField("date", TimestampType(), True),
        StructField("text", StringType(), True),
        StructField("numberOfHours", DoubleType(), True),
        StructField("isApproved", BooleanType(), True),
        StructField("isReconciled", BooleanType(), True),
        StructField("lastUpdated", TimestampType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

TABLE_SCHEMAS = {
    "employees": EMPLOYEES_SCHEMA,
    "customers": CUSTOMERS_SCHEMA,
    "projects": PROJECTS_SCHEMA,
    "project_time_entries": PROJECT_TIME_ENTRIES_SCHEMA,
}

# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

TABLE_METADATA = {
    "employees": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "customers": {
        "primary_keys": ["customerNumber"],
        "ingestion_type": "snapshot",
    },
    "projects": {
        "primary_keys": ["number"],
        "cursor_field": "lastUpdated",
        "ingestion_type": "cdc",
    },
    "project_time_entries": {
        "primary_keys": ["number"],
        "cursor_field": "lastUpdated",
        "ingestion_type": "cdc",
    },
}

# ---------------------------------------------------------------------------
# Endpoint routing: table -> (resource API, resource path, PK field)
# ---------------------------------------------------------------------------

TABLE_ENDPOINTS = {
    "employees": (PROJECTS_API, "Employees", "number"),
    "customers": (CUSTOMERS_API, "Customers", "customerNumber"),
    "projects": (PROJECTS_API, "Projects", "number"),
    "project_time_entries": (PROJECTS_API, "TimeEntries", "number"),
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
