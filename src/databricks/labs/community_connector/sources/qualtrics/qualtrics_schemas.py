"""Static schema definitions for Qualtrics connector tables.

This module contains all Spark StructType schema definitions and table metadata
for the Qualtrics Lakeflow connector. These are derived from the Qualtrics REST API
documentation.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# Table Schema Definitions
# =============================================================================

SURVEYS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("creation_date", StringType(), True),
    StructField("last_modified", StringType(), True),
])
"""Schema for the surveys table."""

SURVEY_DEFINITIONS_SCHEMA = StructType([
    # Survey identification - these are consistently typed
    StructField("survey_id", StringType(), True),
    StructField("survey_name", StringType(), True),
    StructField("survey_status", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("creator_id", StringType(), True),
    StructField("brand_id", StringType(), True),
    StructField("brand_base_url", StringType(), True),
    StructField("last_modified", StringType(), True),
    StructField("last_accessed", StringType(), True),
    StructField("last_activated", StringType(), True),
    StructField("question_count", StringType(), True),
    # Complex nested structures - stored as StringType (JSON)
    # These fields have variable structure depending on survey configuration
    StructField("questions", StringType(), True),
    StructField("blocks", StringType(), True),
    StructField("survey_flow", StringType(), True),
    StructField("survey_options", StringType(), True),
    StructField("response_sets", StringType(), True),
    StructField("scoring", StringType(), True),
    StructField("project_info", StringType(), True),
])
"""Schema for the survey_definitions table.

Returns full survey structure including questions, blocks, and flow.
Complex nested structures are stored as JSON strings for flexibility.
"""

SURVEY_RESPONSES_SCHEMA = StructType([
    StructField("response_id", StringType(), True),
    StructField("survey_id", StringType(), True),
    StructField("recorded_date", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("status", LongType(), True),
    StructField("ip_address", StringType(), True),
    StructField("progress", LongType(), True),
    StructField("duration", LongType(), True),
    StructField("finished", BooleanType(), True),
    StructField("distribution_channel", StringType(), True),
    StructField("user_language", StringType(), True),
    StructField("location_latitude", StringType(), True),
    StructField("location_longitude", StringType(), True),
    StructField("values", MapType(
        StringType(),
        StructType([
            StructField("choice_text", StringType(), True),
            StructField("choice_id", StringType(), True),
            StructField("text_entry", StringType(), True),
        ]),
    ), True),
    StructField("labels", MapType(StringType(), StringType()), True),
    StructField("displayed_fields", ArrayType(StringType()), True),
    StructField("displayed_values", MapType(StringType(), StringType()), True),
    StructField("embedded_data", MapType(StringType(), StringType()), True),
])
"""Schema for the survey_responses table."""

DISTRIBUTIONS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("parent_distribution_id", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("organization_id", StringType(), True),
    StructField("request_type", StringType(), True),
    StructField("request_status", StringType(), True),
    StructField("send_date", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("modified_date", StringType(), True),
    StructField("headers", StructType([
        StructField("from_email", StringType(), True),
        StructField("from_name", StringType(), True),
        StructField("reply_to_email", StringType(), True),
        StructField("subject", StringType(), True),
    ]), True),
    StructField("recipients", StructType([
        StructField("mailing_list_id", StringType(), True),
        StructField("contact_id", StringType(), True),
        StructField("library_id", StringType(), True),
        StructField("sample_id", StringType(), True),
    ]), True),
    StructField("message", StructType([
        StructField("library_id", StringType(), True),
        StructField("message_id", StringType(), True),
        StructField("message_type", StringType(), True),
    ]), True),
    StructField("survey_link", StructType([
        StructField("survey_id", StringType(), True),
        StructField("expiration_date", StringType(), True),
        StructField("link_type", StringType(), True),
    ]), True),
    StructField("stats", StructType([
        StructField("sent", LongType(), True),
        StructField("failed", LongType(), True),
        StructField("started", LongType(), True),
        StructField("bounced", LongType(), True),
        StructField("opened", LongType(), True),
        StructField("skipped", LongType(), True),
        StructField("finished", LongType(), True),
        StructField("complaints", LongType(), True),
        StructField("blocked", LongType(), True),
    ]), True),
])
"""Schema for the distributions table.

Includes nested structures for surveyLink, recipients, message, and stats.
"""

MAILING_LISTS_SCHEMA = StructType([
    StructField("mailing_list_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("owner_id", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("last_modified_date", StringType(), True),
    StructField("contact_count", LongType(), True),
])
"""Schema for the mailing_lists table.

Returns mailing list metadata with dates as ISO 8601 strings (consistent with surveys).
"""

MAILING_LIST_CONTACTS_SCHEMA = StructType([
    StructField("contact_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("ext_ref", StringType(), True),
    StructField("language", StringType(), True),
    StructField("unsubscribed", BooleanType(), True),
    StructField("mailing_list_unsubscribed", BooleanType(), True),
    StructField("contact_lookup_id", StringType(), True),
])
"""Schema for the mailing_list_contacts table."""

DIRECTORY_CONTACTS_SCHEMA = StructType([
    StructField("contact_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("ext_ref", StringType(), True),
    StructField("language", StringType(), True),
    StructField("unsubscribed", BooleanType(), True),
    StructField("embedded_data", MapType(StringType(), StringType()), True),
])
"""Schema for the directory_contacts table."""

DIRECTORIES_SCHEMA = StructType([
    StructField("directory_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("contact_count", LongType(), True),
    StructField("is_default", BooleanType(), True),
    StructField("deduplication_criteria", StructType([
        StructField("email", BooleanType(), True),
        StructField("first_name", BooleanType(), True),
        StructField("last_name", BooleanType(), True),
        StructField("external_data_reference", BooleanType(), True),
        StructField("phone", BooleanType(), True),
    ]), True),
])
"""Schema for the directories table."""

USERS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("user_type", StringType(), True),
    StructField("division_id", StringType(), True),
    StructField("account_status", StringType(), True),
])
"""Schema for the users table."""


# =============================================================================
# Schema Mapping
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    "surveys": SURVEYS_SCHEMA,
    "survey_definitions": SURVEY_DEFINITIONS_SCHEMA,
    "survey_responses": SURVEY_RESPONSES_SCHEMA,
    "distributions": DISTRIBUTIONS_SCHEMA,
    "mailing_lists": MAILING_LISTS_SCHEMA,
    "mailing_list_contacts": MAILING_LIST_CONTACTS_SCHEMA,
    "directory_contacts": DIRECTORY_CONTACTS_SCHEMA,
    "directories": DIRECTORIES_SCHEMA,
    "users": USERS_SCHEMA,
}
"""Mapping of table names to their StructType schemas."""


# =============================================================================
# Table Metadata Definitions
# =============================================================================

TABLE_METADATA: dict[str, dict] = {
    "surveys": {
        "primary_keys": ["id"],
        "cursor_field": "last_modified",
        "ingestion_type": "cdc",
    },
    "survey_definitions": {
        "primary_keys": ["survey_id"],
        "cursor_field": "last_modified",
        "ingestion_type": "cdc",
    },
    "survey_responses": {
        "primary_keys": ["response_id"],
        "cursor_field": "recorded_date",
        "ingestion_type": "append",
    },
    "distributions": {
        "primary_keys": ["id"],
        "cursor_field": "modified_date",
        "ingestion_type": "cdc",
    },
    "mailing_lists": {
        "primary_keys": ["mailing_list_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "mailing_list_contacts": {
        "primary_keys": ["contact_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "directory_contacts": {
        "primary_keys": ["contact_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "directories": {
        "primary_keys": ["directory_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "users": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}
"""Metadata for each table including primary keys, cursor field, and ingestion type."""


# =============================================================================
# Supported Tables
# =============================================================================

SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
"""List of all table names supported by the Qualtrics connector."""
