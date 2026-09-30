"""Static schemas, metadata and constants for the HiBob (Bob) connector."""

from pyspark.sql.types import (
    ArrayType,
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
# Connection / HTTP constants
# ---------------------------------------------------------------------------

PRODUCTION_BASE_URL = "https://api.hibob.com/v1"
SANDBOX_BASE_URL = "https://api.sandbox.hibob.com/v1"

REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 120.0
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Bulk history endpoints: page size bounds (API: 1-200, default 50).
BULK_DEFAULT_PAGE_SIZE = 200
BULK_MAX_PAGE_SIZE = 200
# Max employeeIds per bulk request (API limit).
BULK_MAX_EMPLOYEE_IDS = 200

# Time off changes: the API rejects ``since`` older than ~6 months.
TIME_OFF_DEFAULT_MAX_LOOKBACK_DAYS = 180
TIME_OFF_DEFAULT_WINDOW_DAYS = 7
TIME_OFF_DEFAULT_MAX_RECORDS_PER_BATCH = 1000

# Recursion guard for named-list children.
NAMED_LIST_MAX_DEPTH = 32

# Default people-search field list (dot-notation field IDs). Unknown or
# unpermitted field IDs are silently dropped by the API.
DEFAULT_EMPLOYEE_FIELDS = [
    "root.id",
    "root.firstName",
    "root.surname",
    "root.email",
    "root.displayName",
    "root.fullName",
    "root.creationDateTime",
    "work.department",
    "work.title",
    "work.startDate",
    "work.manager",
    "work.site",
    "work.siteId",
    "work.reportsTo",
    "internal.status",
    "internal.lifecycleStatus",
    "internal.terminationDate",
    "personal.birthDate",
]
MAX_EMPLOYEE_FIELDS = 400
HUMAN_READABLE_MODES = {"", "APPEND", "REPLACE"}

# Bulk history endpoint suffix per table.
BULK_TABLE_ENDPOINTS = {
    "employee_work_history": "work",
    "employee_employment_history": "employment",
    "employee_lifecycle_history": "lifecycle",
    "employee_salary_history": "salaries",
}

# Bulk entry fields that hold opaque objects and are serialised to JSON strings.
BULK_JSON_FIELDS = {"customColumns", "actualWorkingPattern"}

# ---------------------------------------------------------------------------
# Reusable struct types
# ---------------------------------------------------------------------------

EMPLOYEE_REF_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("surname", StringType(), True),
        StructField("email", StringType(), True),
        StructField("displayName", StringType(), True),
    ]
)

CHANGE_STRUCT = StructType(
    [
        StructField("reason", StringType(), True),
        StructField("changedBy", StringType(), True),
        StructField("changedById", StringType(), True),
    ]
)

CURRENCY_STRUCT = StructType(
    [
        StructField("value", DoubleType(), True),
        StructField("currency", StringType(), True),
    ]
)

_BULK_COMMON_FIELDS = [
    StructField("employeeId", StringType(), False),
    StructField("id", LongType(), False),
    StructField("effectiveDate", DateType(), True),
    StructField("activeEffectiveDate", DateType(), True),
    StructField("isCurrent", BooleanType(), True),
    StructField("creationDate", TimestampType(), True),
    StructField("modificationDate", TimestampType(), True),
    StructField("change", CHANGE_STRUCT, True),
    # Opaque object keyed by backend column IDs; stored as JSON string.
    StructField("customColumns", StringType(), True),
]

# ---------------------------------------------------------------------------
# Table schemas
# ---------------------------------------------------------------------------

EMPLOYEES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("firstName", StringType(), True),
        StructField("surname", StringType(), True),
        StructField("email", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("fullName", StringType(), True),
        StructField("creationDateTime", TimestampType(), True),
        StructField(
            "work",
            StructType(
                [
                    StructField("department", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("site", StringType(), True),
                    StructField("siteId", LongType(), True),
                    StructField("startDate", DateType(), True),
                    StructField("manager", StringType(), True),
                    StructField("reportsTo", EMPLOYEE_REF_STRUCT, True),
                ]
            ),
            True,
        ),
        StructField(
            "internal",
            StructType(
                [
                    StructField("status", StringType(), True),
                    StructField("lifecycleStatus", StringType(), True),
                    StructField("terminationDate", DateType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "personal",
            StructType([StructField("birthDate", DateType(), True)]),
            True,
        ),
        # Human-readable values (humanReadable=APPEND), as a JSON string.
        StructField("humanReadable", StringType(), True),
        # Full normalised employee record (incl. custom / extra fields), JSON.
        StructField("raw_json", StringType(), True),
    ]
)

EMPLOYEE_WORK_HISTORY_SCHEMA = StructType(
    _BULK_COMMON_FIELDS
    + [
        StructField("endEffectiveDate", DateType(), True),
        StructField("workChangeType", StringType(), True),
        StructField("department", StringType(), True),
        StructField("title", StringType(), True),
        StructField("site", StringType(), True),
        StructField("siteId", LongType(), True),
        StructField("reportsTo", EMPLOYEE_REF_STRUCT, True),
        StructField("canBeDeleted", BooleanType(), True),
    ]
)

EMPLOYEE_EMPLOYMENT_HISTORY_SCHEMA = StructType(
    _BULK_COMMON_FIELDS
    + [
        StructField("endEffectiveDate", DateType(), True),
        StructField("contract", StringType(), True),
        StructField("type", StringType(), True),
        StructField("salaryPayType", StringType(), True),
        StructField("weeklyHours", DoubleType(), True),
        StructField("fte", DoubleType(), True),
        StructField("hoursInDayNotWorked", DoubleType(), True),
        StructField("calendarName", StringType(), True),
        StructField("calendarId", LongType(), True),
        StructField("flsaCode", StringType(), True),
        # Pattern shape varies by type (hourly / fortnightly / flexible).
        StructField("actualWorkingPattern", StringType(), True),
    ]
)

EMPLOYEE_LIFECYCLE_HISTORY_SCHEMA = StructType(
    _BULK_COMMON_FIELDS
    + [
        # Documented as a string for lifecycle entries.
        StructField("endEffectiveDate", StringType(), True),
        StructField("status", StringType(), True),
        StructField("employeeStatus", StringType(), True),
        StructField("reasonType", StringType(), True),
        StructField("leaveReason", StringType(), True),
    ]
)

EMPLOYEE_SALARY_HISTORY_SCHEMA = StructType(
    _BULK_COMMON_FIELDS
    + [
        StructField("endEffectiveDate", DateType(), True),
        StructField("base", CURRENCY_STRUCT, True),
        StructField("payPeriod", StringType(), True),
        StructField("payFrequency", StringType(), True),
    ]
)

TIME_OFF_REQUEST_CHANGES_SCHEMA = StructType(
    [
        StructField("changeType", StringType(), False),
        StructField("requestId", LongType(), False),
        StructField("originalRequestId", LongType(), True),
        StructField("previousRequestId", LongType(), True),
        StructField("employeeId", StringType(), True),
        StructField("employeeDisplayName", StringType(), True),
        StructField("employeeEmail", StringType(), True),
        StructField("policyTypeDisplayName", StringType(), True),
        StructField("type", StringType(), True),
        StructField("createdOn", TimestampType(), True),
        StructField("durationUnit", StringType(), True),
        StructField("totalDuration", DoubleType(), True),
        StructField("totalCost", DoubleType(), True),
        StructField("changeReason", StringType(), True),
        StructField("visibility", StringType(), True),
        StructField("startDate", DateType(), True),
        StructField("endDate", DateType(), True),
        StructField("timeZone", StringType(), True),
        # Type-specific fields (portions, start/end times, day durations...).
        StructField("additional_fields", StringType(), True),
    ]
)

NAMED_LISTS_SCHEMA = StructType(
    [
        StructField("list_name", StringType(), False),
        StructField("item_id", StringType(), False),
        StructField("value", StringType(), True),
        StructField("name", StringType(), True),
        StructField("archived", BooleanType(), True),
        StructField("parent_id", StringType(), True),
    ]
)

TIME_OFF_POLICY_TYPES_SCHEMA = StructType(
    [StructField("name", StringType(), False)]
)

EMPLOYEE_FIELDS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("categoryId", StringType(), True),
        StructField("categoryDisplayName", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("jsonPath", StringType(), True),
        StructField("type", StringType(), True),
        # Type-specific metadata (e.g. listId); stored as JSON string.
        StructField("typeData", StringType(), True),
        StructField("historical", BooleanType(), True),
    ]
)

CUSTOM_TABLE_COLUMN_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("mandatory", BooleanType(), True),
        StructField("type", StringType(), True),
        StructField("typeData", StringType(), True),
    ]
)

CUSTOM_TABLES_METADATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("columns", ArrayType(CUSTOM_TABLE_COLUMN_STRUCT, True), True),
    ]
)

TABLE_SCHEMAS: dict[str, StructType] = {
    "employees": EMPLOYEES_SCHEMA,
    "employee_work_history": EMPLOYEE_WORK_HISTORY_SCHEMA,
    "employee_employment_history": EMPLOYEE_EMPLOYMENT_HISTORY_SCHEMA,
    "employee_lifecycle_history": EMPLOYEE_LIFECYCLE_HISTORY_SCHEMA,
    "employee_salary_history": EMPLOYEE_SALARY_HISTORY_SCHEMA,
    "time_off_request_changes": TIME_OFF_REQUEST_CHANGES_SCHEMA,
    "named_lists": NAMED_LISTS_SCHEMA,
    "time_off_policy_types": TIME_OFF_POLICY_TYPES_SCHEMA,
    "employee_fields": EMPLOYEE_FIELDS_SCHEMA,
    "custom_tables_metadata": CUSTOM_TABLES_METADATA_SCHEMA,
}

_BULK_PK = ["employeeId", "id"]

TABLE_METADATA: dict[str, dict] = {
    "employees": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "employee_work_history": {"primary_keys": _BULK_PK, "ingestion_type": "snapshot"},
    "employee_employment_history": {"primary_keys": _BULK_PK, "ingestion_type": "snapshot"},
    "employee_lifecycle_history": {"primary_keys": _BULK_PK, "ingestion_type": "snapshot"},
    "employee_salary_history": {"primary_keys": _BULK_PK, "ingestion_type": "snapshot"},
    "time_off_request_changes": {
        "primary_keys": ["requestId", "changeType"],
        "cursor_field": "createdOn",
        "ingestion_type": "append",
    },
    "named_lists": {"primary_keys": ["list_name", "item_id"], "ingestion_type": "snapshot"},
    "time_off_policy_types": {"primary_keys": ["name"], "ingestion_type": "snapshot"},
    "employee_fields": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "custom_tables_metadata": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())

# Tables read through the partitioned-stream path.
PARTITIONED_TABLES = {"time_off_request_changes"}
