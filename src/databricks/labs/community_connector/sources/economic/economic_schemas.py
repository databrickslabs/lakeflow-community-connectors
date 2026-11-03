"""Static schemas, metadata and endpoint routing for the e-conomic connector.

All tables come from the e-conomic OpenAPI family
(``https://apis.e-conomic.com/``). Each domain is a separate "resource API"
with its own version in the URL path (``/{resource-api}/v{version}/{Resource}``):

* ``projectsapi``: ``employees``, ``projects``, ``project_time_entries``,
  ``activities``, ``activity_groups``, ``employee_groups``,
  ``project_groups``, ``project_statuses``, ``mileages``,
  ``project_employees``, ``project_customers``, ``time_entry_prices``,
  ``cost_types``
* ``customersapi``: ``customers``, ``customer_contacts``,
  ``customer_delivery_locations``

The record structures are flat (no nested objects / arrays). Nullable and
``false`` boolean properties are frequently *omitted* from responses, so
every column is declared nullable and missing keys surface as null.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
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

ACTIVITIES_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("groupNumber", LongType(), True),
        StructField("name", StringType(), True),
        StructField("isBarred", BooleanType(), True),
        StructField("inLieuCode", LongType(), True),
        StructField("hideInSearch", BooleanType(), True),
        StructField("cutoffDate", TimestampType(), True),
        StructField("salesPriceBefore", DoubleType(), True),
        StructField("salesPriceAfter", DoubleType(), True),
        StructField("costPriceMarkupPercentage", DoubleType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

ACTIVITY_GROUPS_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("name", StringType(), True),
        StructField("type", LongType(), True),
        StructField("includeCostPriceInFinance", BooleanType(), True),
        StructField("costAccountClosed", LongType(), True),
        StructField("costAccountOngoing", LongType(), True),
        StructField("costAccountOngoingType", LongType(), True),
        StructField("costContraAccountOngoing", LongType(), True),
        StructField("includeSalesPriceInFinance", BooleanType(), True),
        StructField("salesAccountClosed", LongType(), True),
        StructField("salesAccountOngoing", LongType(), True),
        StructField("salesAccountOngoingType", LongType(), True),
        StructField("salesContraAccountOngoing", LongType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

EMPLOYEE_GROUPS_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("name", StringType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

PROJECT_GROUPS_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("name", StringType(), True),
        StructField("type", LongType(), True),
        StructField("includeCostPriceInFinance", BooleanType(), True),
        StructField("costAccountOngoingType", LongType(), True),
        StructField("costAccountOngoing", LongType(), True),
        StructField("costContraAccountOngoing", LongType(), True),
        StructField("costAccountClosed", LongType(), True),
        StructField("includeSalesPriceInFinance", BooleanType(), True),
        StructField("salesAccountOngoingType", LongType(), True),
        StructField("salesAccountOngoing", LongType(), True),
        StructField("salesContraAccountOngoing", LongType(), True),
        StructField("salesAccountClosed", LongType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

PROJECT_STATUSES_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("name", StringType(), True),
        StructField("type", LongType(), True),
        StructField("priority", LongType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

# ``from`` / ``to`` are the literal JSON keys (quote them in SQL).
MILEAGES_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("projectNumber", LongType(), True),
        StructField("employeeNumber", LongType(), True),
        StructField("date", TimestampType(), True),
        StructField("distance", DoubleType(), True),
        StructField("from", StringType(), True),
        StructField("to", StringType(), True),
        StructField("isApproved", BooleanType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

# ``cutOffDate`` is a plain ``date`` in the spec; live values carry a
# ``T00:00:00`` suffix, which the DateType parser accepts.
PROJECT_EMPLOYEES_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("groupNumber", LongType(), True),
        StructField("name", StringType(), True),
        StructField("isUser", BooleanType(), True),
        StructField("userId", StringType(), True),
        StructField("canApprove", BooleanType(), True),
        StructField("employeeType", LongType(), True),
        StructField("canInvoice", BooleanType(), True),
        StructField("isBarred", BooleanType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zipCode", StringType(), True),
        StructField("cutOffDate", DateType(), True),
        StructField("salesPriceBefore", DoubleType(), True),
        StructField("salesPriceAfter", DoubleType(), True),
        StructField("costPriceBefore", DoubleType(), True),
        StructField("costPriceAfter", DoubleType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

PROJECT_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("name", StringType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

TIME_ENTRY_PRICES_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("costPrice", DoubleType(), True),
        StructField("salesPrice", DoubleType(), True),
    ]
)

COST_TYPES_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("name", StringType(), True),
        StructField("costGroupNumber", LongType(), True),
        # Fixed-width code; trailing spaces are kept exactly as returned.
        StructField("vatCode", StringType(), True),
        StructField("isBarred", BooleanType(), True),
        StructField("lastUpdated", TimestampType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

CUSTOMER_CONTACTS_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("customerNumber", LongType(), True),
        StructField("userInterfaceNumber", LongType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("eInvoiceId", StringType(), True),
        StructField("lastUpdated", TimestampType(), True),
        StructField("receiveQuotes", BooleanType(), True),
        StructField("receiveOrders", BooleanType(), True),
        StructField("receiveInvoices", BooleanType(), True),
        StructField("receiveEInvoices", BooleanType(), True),
        StructField("receiveStatementOfAccounts", BooleanType(), True),
        StructField("receiveReminders", BooleanType(), True),
        StructField("objectVersion", StringType(), True),
        StructField("isDeleted", BooleanType(), True),
        StructField("active", BooleanType(), True),
    ]
)

CUSTOMER_DELIVERY_LOCATIONS_SCHEMA = StructType(
    [
        StructField("number", LongType(), False),
        StructField("customerNumber", LongType(), True),
        StructField("userInterfaceNumber", LongType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("termsOfDelivery", StringType(), True),
        StructField("eInvoiceId", StringType(), True),
        StructField("isBarred", BooleanType(), True),
        StructField("lastUpdated", TimestampType(), True),
        StructField("objectVersion", StringType(), True),
    ]
)

TABLE_SCHEMAS = {
    "employees": EMPLOYEES_SCHEMA,
    "customers": CUSTOMERS_SCHEMA,
    "projects": PROJECTS_SCHEMA,
    "project_time_entries": PROJECT_TIME_ENTRIES_SCHEMA,
    "activities": ACTIVITIES_SCHEMA,
    "activity_groups": ACTIVITY_GROUPS_SCHEMA,
    "employee_groups": EMPLOYEE_GROUPS_SCHEMA,
    "project_groups": PROJECT_GROUPS_SCHEMA,
    "project_statuses": PROJECT_STATUSES_SCHEMA,
    "mileages": MILEAGES_SCHEMA,
    "project_employees": PROJECT_EMPLOYEES_SCHEMA,
    "project_customers": PROJECT_CUSTOMERS_SCHEMA,
    "time_entry_prices": TIME_ENTRY_PRICES_SCHEMA,
    "cost_types": COST_TYPES_SCHEMA,
    "customer_contacts": CUSTOMER_CONTACTS_SCHEMA,
    "customer_delivery_locations": CUSTOMER_DELIVERY_LOCATIONS_SCHEMA,
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
    "activities": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "activity_groups": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "employee_groups": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "project_groups": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "project_statuses": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "mileages": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "project_employees": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "project_customers": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    # Prices are calculated server-side and can change without the parent
    # time entry's lastUpdated moving, so they are re-read in full.
    "time_entry_prices": {
        "primary_keys": ["number"],
        "ingestion_type": "snapshot",
    },
    "cost_types": {
        "primary_keys": ["number"],
        "cursor_field": "lastUpdated",
        "ingestion_type": "cdc",
    },
    # isDeleted is kept as an ordinary column: the list endpoint has not been
    # observed returning soft-deleted rows, so this is not cdc_with_deletes.
    "customer_contacts": {
        "primary_keys": ["number"],
        "cursor_field": "lastUpdated",
        "ingestion_type": "cdc",
    },
    "customer_delivery_locations": {
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
    "activities": (PROJECTS_API, "Activities", "number"),
    "activity_groups": (PROJECTS_API, "ActivityGroups", "number"),
    "employee_groups": (PROJECTS_API, "EmployeeGroups", "number"),
    "project_groups": (PROJECTS_API, "ProjectGroups", "number"),
    "project_statuses": (PROJECTS_API, "ProjectStatuses", "number"),
    "mileages": (PROJECTS_API, "Mileages", "number"),
    # Spec casing: these two resources are lower-kebab, not PascalCase.
    "project_employees": (PROJECTS_API, "project-employees", "number"),
    "project_customers": (PROJECTS_API, "project-customers", "number"),
    "time_entry_prices": (PROJECTS_API, "TimeEntryPrices", "number"),
    "cost_types": (PROJECTS_API, "CostTypes", "number"),
    "customer_contacts": (CUSTOMERS_API, "Contacts", "number"),
    "customer_delivery_locations": (CUSTOMERS_API, "DeliveryLocations", "number"),
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
