"""Schemas, metadata, and constants for the Amplitude connector.

Amplitude's object list is static (not discoverable via API), so every
table's Spark schema and metadata is hard-coded here.  The connector code
in ``amplitude.py`` fetches data and returns the raw parsed JSON; the
framework handles type coercion against these schemas.

Dynamic, free-form objects (``event_properties``, ``user_properties`` …)
are modelled as ``MapType(StringType, StringType)`` since their keys vary
per event type / project.  The deeply-nested cohort ``definition`` blob is
modelled as ``StringType`` (the reader serialises it to a JSON string).
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Retry policy shared by every request (see ``_request_with_retry``).
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

# Default request timeout (seconds).  The Export API can be slow, so the
# events reader overrides this with a larger value.
DEFAULT_TIMEOUT = 60
EXPORT_TIMEOUT = 120

# Base URLs by data-residency region.
BASE_URLS = {
    "standard": "https://amplitude.com",
    "eu": "https://analytics.eu.amplitude.com",
}

# ----- table catalogue --------------------------------------------------

TABLES = [
    "events",
    "events_list",
    "active_users_counts",
    "average_session_length",
    "cohorts",
    "annotations",
]

# A free-form key/value object whose keys vary per event type / project.
_DYNAMIC_MAP = MapType(StringType(), StringType())


def _events_schema() -> StructType:
    """Raw event records exported via the Export API (one per gzipped NDJSON line)."""
    return StructType(
        [
            StructField("uuid", StringType(), nullable=False),
            StructField("event_id", LongType(), nullable=True),
            StructField("event_type", StringType(), nullable=True),
            StructField("event_time", TimestampType(), nullable=True),
            StructField("server_received_time", TimestampType(), nullable=True),
            # Incremental cursor — when Amplitude processed the event.
            StructField("server_upload_time", TimestampType(), nullable=True),
            StructField("client_event_time", TimestampType(), nullable=True),
            StructField("client_upload_time", TimestampType(), nullable=True),
            StructField("processed_time", TimestampType(), nullable=True),
            StructField("user_id", StringType(), nullable=True),
            StructField("device_id", StringType(), nullable=True),
            StructField("amplitude_id", LongType(), nullable=True),
            StructField("$insert_id", StringType(), nullable=True),
            StructField("app", LongType(), nullable=True),
            StructField("platform", StringType(), nullable=True),
            StructField("os_name", StringType(), nullable=True),
            StructField("os_version", StringType(), nullable=True),
            StructField("device_family", StringType(), nullable=True),
            StructField("device_type", StringType(), nullable=True),
            StructField("device_carrier", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("dma", StringType(), nullable=True),
            StructField("language", StringType(), nullable=True),
            StructField("ip_address", StringType(), nullable=True),
            StructField("location_lat", DoubleType(), nullable=True),
            StructField("location_lng", DoubleType(), nullable=True),
            StructField("version_name", StringType(), nullable=True),
            StructField("start_version", StringType(), nullable=True),
            StructField("session_id", LongType(), nullable=True),
            StructField("paying", BooleanType(), nullable=True),
            StructField("library", StringType(), nullable=True),
            StructField("event_properties", _DYNAMIC_MAP, nullable=True),
            StructField("user_properties", _DYNAMIC_MAP, nullable=True),
            StructField("group_properties", _DYNAMIC_MAP, nullable=True),
            StructField("groups", _DYNAMIC_MAP, nullable=True),
            StructField("data", _DYNAMIC_MAP, nullable=True),
            StructField("amplitude_attribution_ids", StringType(), nullable=True),
            StructField("sample_rate", StringType(), nullable=True),
        ]
    )


def _events_list_schema() -> StructType:
    """Catalogue of visible event types with this week's totals."""
    return StructType(
        [
            StructField("value", StringType(), nullable=False),
            StructField("display", StringType(), nullable=True),
            StructField("non_active", BooleanType(), nullable=True),
            StructField("deleted", BooleanType(), nullable=True),
            StructField("hidden", BooleanType(), nullable=True),
            StructField("flow_hidden", BooleanType(), nullable=True),
            StructField("totals", LongType(), nullable=True),
        ]
    )


def _active_users_counts_schema() -> StructType:
    """Daily active/new user counts, flattened to one row per (date, segment)."""
    return StructType(
        [
            StructField("date", StringType(), nullable=False),
            StructField("count", LongType(), nullable=True),
            StructField("segment", StringType(), nullable=True),
        ]
    )


def _average_session_length_schema() -> StructType:
    """Daily average session length in seconds, one row per date."""
    return StructType(
        [
            StructField("date", StringType(), nullable=False),
            StructField("length", DoubleType(), nullable=True),
        ]
    )


def _cohorts_schema() -> StructType:
    """Behavioral cohort definitions."""
    return StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("appId", LongType(), nullable=True),
            StructField("archived", BooleanType(), nullable=True),
            StructField("description", StringType(), nullable=True),
            StructField("finished", BooleanType(), nullable=True),
            StructField("published", BooleanType(), nullable=True),
            StructField("hidden", BooleanType(), nullable=True),
            StructField("size", LongType(), nullable=True),
            StructField("type", StringType(), nullable=True),
            StructField("owners", ArrayType(StringType()), nullable=True),
            StructField("viewers", ArrayType(StringType()), nullable=True),
            StructField("lastMod", LongType(), nullable=True),
            StructField("createdAt", LongType(), nullable=True),
            StructField("lastComputed", LongType(), nullable=True),
            StructField("metadata", ArrayType(StringType()), nullable=True),
            StructField("view_count", LongType(), nullable=True),
            StructField("popularity", LongType(), nullable=True),
            StructField("last_viewed", LongType(), nullable=True),
            StructField("chart_id", StringType(), nullable=True),
            StructField("edit_id", StringType(), nullable=True),
            StructField("is_predictive", BooleanType(), nullable=True),
            StructField("is_official_content", BooleanType(), nullable=True),
            StructField("location_id", StringType(), nullable=True),
            StructField("shortcut_ids", ArrayType(StringType()), nullable=True),
            # Opaque, deeply-nested blob — serialised to a JSON string.
            StructField("definition", StringType(), nullable=True),
        ]
    )


def _annotations_schema() -> StructType:
    """Chart annotations (releases, incidents, …)."""
    return StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("label", StringType(), nullable=True),
            StructField("start", TimestampType(), nullable=True),
            StructField("end", TimestampType(), nullable=True),
            StructField("details", StringType(), nullable=True),
            StructField("chart_id", StringType(), nullable=True),
            StructField(
                "category",
                StructType(
                    [
                        StructField("id", LongType(), nullable=True),
                        StructField("name", StringType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )


TABLE_SCHEMAS = {
    "events": _events_schema(),
    "events_list": _events_list_schema(),
    "active_users_counts": _active_users_counts_schema(),
    "average_session_length": _average_session_length_schema(),
    "cohorts": _cohorts_schema(),
    "annotations": _annotations_schema(),
}

TABLE_METADATA = {
    "events": {
        "primary_keys": ["uuid"],
        "cursor_field": "server_upload_time",
        "ingestion_type": "append",
    },
    "events_list": {
        "primary_keys": ["value"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "active_users_counts": {
        "primary_keys": ["date"],
        "cursor_field": "date",
        "ingestion_type": "cdc",
    },
    "average_session_length": {
        "primary_keys": ["date"],
        "cursor_field": "date",
        "ingestion_type": "cdc",
    },
    "cohorts": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "annotations": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}
