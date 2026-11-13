"""Static schemas, metadata and tuning constants for the Power BI connector.

"Semantic model" is Microsoft's current product name for what the Power BI
REST API still calls a *dataset*.  Table and column names below deliberately
mirror the REST API's ``dataset``/``datasets`` vocabulary so that a reader can
map every field back to the official reference; only the connector's display
name says "semantic model".
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Endpoints / auth
# ---------------------------------------------------------------------------

POWER_BI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
ENTRA_LOGIN_BASE = "https://login.microsoftonline.com"
POWER_BI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

# OData paging on the Admin list endpoints.  ``$top`` is mandatory there and
# capped at 5000 by the service.
DEFAULT_PAGE_SIZE = 1000
MAX_ADMIN_PAGE_SIZE = 5000
# Hard stop so a server that silently ignores ``$skip`` cannot spin forever.
DEFAULT_MAX_PAGES = 50

# POST /admin/workspaces/getInfo accepts at most 100 workspace IDs per scan.
MAX_SCAN_WORKSPACES = 100
DEFAULT_SCAN_BATCH_SIZE = 100

# Scanner API polling.  The scan is asynchronous and can take seconds to
# minutes depending on how many workspaces/datasets are in the batch.
DEFAULT_SCAN_POLL_SECONDS = 3
DEFAULT_SCAN_MAX_POLL_SECONDS = 30
DEFAULT_SCAN_TIMEOUT_SECONDS = 600

# Refresh history fan-out.
DEFAULT_REFRESH_TOP = 60
DEFAULT_DATASETS_PER_PARTITION = 20

# POST /groups/{groupId}/datasets/{datasetId}/executeQueries.
#
# Rate limit: 120 query requests per minute per user, tenant-wide — not per
# dataset, and an order of magnitude tighter than anything else this connector
# calls. One configured DAX query costs exactly one request per micro-batch
# (the endpoint is not pageable and the connector never fans it out), so the
# shared 429/Retry-After backoff in power_bi_semantic_model_utils is sufficient;
# no extra client-side pacing is warranted. Anyone stacking many DAX tables
# into one pipeline has to budget against this number themselves.

# Documented hard caps on a single executeQueries response (100k rows / 1M
# values / 15 MB body). The API *truncates* silently at these boundaries and
# reports it as a warning rather than an error, so hitting them looks exactly
# like a complete result — hence the heuristic `truncated` flag on every
# emitted row.
DAX_MAX_ROWS = 100_000
DAX_MAX_VALUES = 1_000_000

# Generic HTTP behaviour.
DEFAULT_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
RETRIABLE_STATUS_CODES = (429, 500, 502, 503, 504)
# 401/403 on an ``/admin/*`` call means the service principal has not been
# enabled for the Power BI Admin APIs — that is the trigger for the
# membership-scoped fallback, not a hard failure.
ADMIN_DENIED_STATUS_CODES = (401, 403)

EPOCH_ISO = "1970-01-01T00:00:00.000Z"

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

WORKSPACES = "workspaces"
DATASETS = "datasets"
DATASET_TABLES = "dataset_tables"
DATASET_COLUMNS = "dataset_columns"
DATASET_MEASURES = "dataset_measures"
DATASET_REFRESH_HISTORY = "dataset_refresh_history"
DAX_QUERY_RESULT = "dax_query_result"

SUPPORTED_TABLES = [
    WORKSPACES,
    DATASETS,
    DATASET_TABLES,
    DATASET_COLUMNS,
    DATASET_MEASURES,
    DATASET_REFRESH_HISTORY,
    DAX_QUERY_RESULT,
]

# Tables derived from a single Admin metadata scan (getInfo -> scanStatus ->
# scanResult).  They share one scan per partition.
SCANNER_TABLES = (DATASET_TABLES, DATASET_COLUMNS, DATASET_MEASURES)

SNAPSHOT_TABLES = (
    WORKSPACES,
    DATASETS,
    DATASET_TABLES,
    DATASET_COLUMNS,
    DATASET_MEASURES,
    # The DAX query is re-executed in full on every run; there is no cursor to
    # read it incrementally by, so it is a snapshot like the metadata tables.
    DAX_QUERY_RESULT,
)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

WORKSPACES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", StringType(), True),
        StructField("state", StringType(), True),
        StructField("isReadOnly", BooleanType(), True),
        StructField("isOnDedicatedCapacity", BooleanType(), True),
        StructField("capacityId", StringType(), True),
        StructField("defaultDatasetStorageFormat", StringType(), True),
        StructField("dataflowStorageId", StringType(), True),
        StructField("hasWorkspaceLevelSettings", BooleanType(), True),
        StructField("pipelineId", StringType(), True),
    ]
)

DATASETS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        # Native on Admin API rows; connector-derived on non-admin rows.
        StructField("workspaceId", StringType(), True),
        StructField("description", StringType(), True),
        StructField("configuredBy", StringType(), True),
        StructField("isRefreshable", BooleanType(), True),
        StructField("isEffectiveIdentityRequired", BooleanType(), True),
        StructField("isEffectiveIdentityRolesRequired", BooleanType(), True),
        StructField("isOnPremGatewayRequired", BooleanType(), True),
        StructField("isInPlaceSharingEnabled", BooleanType(), True),
        StructField("addRowsAPIEnabled", BooleanType(), True),
        StructField("targetStorageMode", StringType(), True),
        StructField("ContentProviderType", StringType(), True),
        StructField("createdDate", TimestampType(), True),
        StructField("webUrl", StringType(), True),
        StructField("qnaEmbedURL", StringType(), True),
        StructField(
            "upstreamDataflows",
            ArrayType(
                StructType(
                    [
                        StructField("groupId", StringType(), True),
                        StructField("targetDataflowId", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "queryScaleOutSettings",
            StructType(
                [
                    StructField("autoSyncReadOnlyReplicas", BooleanType(), True),
                    StructField("maxReadOnlyReplicas", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "encryption",
            StructType([StructField("encryptionStatus", StringType(), True)]),
            True,
        ),
    ]
)

DATASET_TABLES_SCHEMA = StructType(
    [
        StructField("workspace_id", StringType(), True),
        StructField("dataset_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("isHidden", BooleanType(), True),
        StructField("description", StringType(), True),
        StructField(
            "source",
            ArrayType(StructType([StructField("expression", StringType(), True)])),
            True,
        ),
    ]
)

DATASET_COLUMNS_SCHEMA = StructType(
    [
        StructField("workspace_id", StringType(), True),
        StructField("dataset_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("name", StringType(), False),
        # The semantic model's own type label (Int64/String/...), not a Spark
        # type — passed through verbatim.
        StructField("dataType", StringType(), True),
        StructField("dataCategory", StringType(), True),
        StructField("formatString", StringType(), True),
        StructField("isHidden", BooleanType(), True),
        StructField("sortByColumn", StringType(), True),
        StructField("summarizeBy", StringType(), True),
    ]
)

DATASET_MEASURES_SCHEMA = StructType(
    [
        StructField("workspace_id", StringType(), True),
        StructField("dataset_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("name", StringType(), False),
        # Only populated when datasetExpressions=true *and* the tenant's
        # metadata-scanning settings are enabled.
        StructField("expression", StringType(), True),
        StructField("description", StringType(), True),
        StructField("formatString", StringType(), True),
        StructField("isHidden", BooleanType(), True),
    ]
)

DATASET_REFRESH_HISTORY_SCHEMA = StructType(
    [
        StructField("workspace_id", StringType(), True),
        StructField("dataset_id", StringType(), False),
        StructField("requestId", StringType(), False),
        StructField("refreshType", StringType(), True),
        StructField("status", StringType(), True),
        StructField("startTime", TimestampType(), True),
        StructField("endTime", TimestampType(), True),
        StructField("serviceExceptionJson", StringType(), True),
        StructField(
            "refreshAttempts",
            ArrayType(
                StructType(
                    [
                        StructField("attemptId", LongType(), True),
                        StructField("startTime", TimestampType(), True),
                        StructField("endTime", TimestampType(), True),
                        StructField("type", StringType(), True),
                        StructField("serviceExceptionJson", StringType(), True),
                        # Free-form metrics blob; JSON-encoded by the connector.
                        StructField("executionMetrics", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

# ---------------------------------------------------------------------------
# dax_query_result — the one table whose columns are not statically knowable
# ---------------------------------------------------------------------------
#
# A DAX ``EVALUATE`` result is a table whose column set is defined by the query
# the *user* configures, so unlike the six metadata tables it cannot be typed
# ahead of time.  ``get_table_schema`` is handed ``table_options``, and the repo
# already resolves schemas from config that way — google_analytics_aggregated
# builds a StructType out of the user's ``dimensions``/``metrics``, and
# google_sheets_docs returns a header-derived schema when configured and a
# static fallback when not.  This table follows that same two-mode shape:
#
# * ``dax_columns`` supplied  -> one properly typed Spark column per declared
#   DAX column.  Preferred: a real StructType beats a stringly-typed map.
# * ``dax_columns`` omitted   -> the fallback below, where the query's own
#   columns live in a ``columns`` map keyed by their DAX names
#   (``Sales[Amount]``, ``[Total Units]``, ...), as osipi's ``af_table_rows``
#   does for user-defined AF tables.
#
# The connector deliberately does *not* execute the query at schema time to
# infer columns (the google_sheets_docs "peek at row 1" trick).  There is no way
# to row-limit an arbitrary user ``EVALUATE`` statement, so schema inference
# would mean running the full query on every planning call, against a budget of
# 120 requests/minute for the whole tenant.

# Columns the connector always contributes, regardless of the DAX query.  Also
# the reserved-name set: a declared DAX column may not collide with these.
DAX_IDENTITY_FIELDS = [
    StructField("workspace_id", StringType(), False),
    StructField("dataset_id", StringType(), False),
    # sha256 prefix of the configured DAX query text.  Two pipelines pointing
    # different queries at one dataset stay distinguishable, and editing the
    # query changes the key rather than silently rewriting existing rows.
    StructField("query_hash", StringType(), False),
    # Position in the response.  Only meaningful as a key — DAX row order is
    # arbitrary unless the query itself carries an ORDER BY.
    StructField("row_index", LongType(), False),
]

DAX_TRAILER_FIELDS = [
    # The untouched JSON object for the row.  Kept in both modes: it is the
    # lossless copy when the map has stringified numerics/dates, and it is where
    # columns the user did not declare in ``dax_columns`` survive.
    StructField("row_json", StringType(), True),
    # Heuristic, set when the response lands on one of the documented truncation
    # boundaries.  The API truncates silently and reports it as a warning rather
    # than an error, so a true here means "this result is very likely incomplete
    # — narrow the query".
    StructField("truncated", BooleanType(), True),
    StructField("ingestion_timestamp", TimestampType(), False),
]

DAX_RESERVED_COLUMN_NAMES = frozenset(
    field.name for field in DAX_IDENTITY_FIELDS + DAX_TRAILER_FIELDS
) | {"columns"}

# Type names accepted in a ``dax_columns`` entry.  Deliberately small: DAX's own
# result types are just text / whole number / decimal / boolean / date-time.
DAX_COLUMN_TYPES = {
    "string": StringType(),
    "long": LongType(),
    "int": LongType(),
    "integer": LongType(),
    "double": DoubleType(),
    "decimal": DoubleType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "timestamp": TimestampType(),
    "datetime": TimestampType(),
    "date": DateType(),
}

DAX_QUERY_RESULT_SCHEMA = StructType(
    DAX_IDENTITY_FIELDS
    + [StructField("columns", MapType(StringType(), StringType()), True)]
    + DAX_TRAILER_FIELDS
)


def build_dax_query_result_schema(column_specs: list[dict]) -> StructType:
    """Build the typed ``dax_query_result`` schema for declared DAX columns.

    ``column_specs`` is the parsed ``dax_columns`` option: a list of
    ``{"dax": <DAX column name>, "name": <Spark column name>, "type": <type>}``
    dicts.  An empty list yields the map-based fallback schema.
    """
    if not column_specs:
        return DAX_QUERY_RESULT_SCHEMA

    declared = [
        StructField(
            spec["name"],
            DAX_COLUMN_TYPES[spec["type"]],
            # DAX emits BLANK() freely, and `includeNulls` turns those into
            # JSON nulls, so every declared column has to be nullable.
            True,
        )
        for spec in column_specs
    ]
    return StructType(DAX_IDENTITY_FIELDS + declared + DAX_TRAILER_FIELDS)


TABLE_SCHEMAS = {
    WORKSPACES: WORKSPACES_SCHEMA,
    DATASETS: DATASETS_SCHEMA,
    DATASET_TABLES: DATASET_TABLES_SCHEMA,
    DATASET_COLUMNS: DATASET_COLUMNS_SCHEMA,
    DATASET_MEASURES: DATASET_MEASURES_SCHEMA,
    DATASET_REFRESH_HISTORY: DATASET_REFRESH_HISTORY_SCHEMA,
    DAX_QUERY_RESULT: DAX_QUERY_RESULT_SCHEMA,
}

# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

TABLE_METADATA = {
    WORKSPACES: {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    DATASETS: {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    DATASET_TABLES: {
        "primary_keys": ["dataset_id", "name"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    DATASET_COLUMNS: {
        "primary_keys": ["dataset_id", "table_name", "name"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    DATASET_MEASURES: {
        "primary_keys": ["dataset_id", "table_name", "name"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    DATASET_REFRESH_HISTORY: {
        "primary_keys": ["dataset_id", "requestId"],
        "cursor_field": "startTime",
        "ingestion_type": "append",
    },
    DAX_QUERY_RESULT: {
        # A DAX result has no server-side identity — no row IDs, no modified
        # timestamps — so the key is the connector's own coordinates: which
        # model, which configured query, which position in its output.
        "primary_keys": ["dataset_id", "query_hash", "row_index"],
        # executeQueries takes no since/until and returns no watermark; the
        # only correct read is to re-run the query in full each time.
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}

# Struct-typed columns that must be normalised from ``{}`` to ``None`` before
# the framework coerces them (an empty dict is rejected by parse_value).
STRUCT_FIELDS_BY_TABLE = {
    DATASETS: ("queryScaleOutSettings", "encryption"),
}
