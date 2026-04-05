"""Schemas, metadata, and constants for the Notion connector."""

from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NOTION_VERSION = "2026-03-11"
BASE_URL = "https://api.notion.com"

SUPPORTED_TABLES = ["databases", "pages", "blocks", "users", "comments"]

RETRIABLE_STATUS_CODES = {429, 500, 502, 503}
MAX_RETRIES = 10
INITIAL_BACKOFF = 1.0
MAX_PAGE_SIZE = 100

# Lookback window in seconds to account for Notion search indexing delays.
LOOKBACK_SECONDS = 60

# Maximum block recursion depth (Notion supports up to 30 levels).
MAX_BLOCK_DEPTH = 30

# ---------------------------------------------------------------------------
# Partial user struct (reused across schemas)
# ---------------------------------------------------------------------------

PARTIAL_USER_TYPE = StructType(
    [
        StructField("object", StringType(), nullable=True),
        StructField("id", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Parent struct
# ---------------------------------------------------------------------------

PARENT_TYPE = StructType(
    [
        StructField("type", StringType(), nullable=True),
        StructField("page_id", StringType(), nullable=True),
        StructField("database_id", StringType(), nullable=True),
        StructField("block_id", StringType(), nullable=True),
        StructField("workspace", BooleanType(), nullable=True),
        StructField("data_source_id", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Databases schema
# ---------------------------------------------------------------------------

DATABASES_SCHEMA = StructType(
    [
        StructField("object", StringType(), nullable=True),
        StructField("id", StringType(), nullable=False),
        StructField("created_time", TimestampType(), nullable=True),
        StructField("created_by", PARTIAL_USER_TYPE, nullable=True),
        StructField("last_edited_time", TimestampType(), nullable=True),
        StructField("last_edited_by", PARTIAL_USER_TYPE, nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("icon", StringType(), nullable=True),
        StructField("cover", StringType(), nullable=True),
        StructField("parent", PARENT_TYPE, nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("in_trash", BooleanType(), nullable=True),
        StructField("is_inline", BooleanType(), nullable=True),
        StructField("public_url", StringType(), nullable=True),
        StructField("properties", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Pages schema
# ---------------------------------------------------------------------------

PAGES_SCHEMA = StructType(
    [
        StructField("object", StringType(), nullable=True),
        StructField("id", StringType(), nullable=False),
        StructField("created_time", TimestampType(), nullable=True),
        StructField("created_by", PARTIAL_USER_TYPE, nullable=True),
        StructField("last_edited_time", TimestampType(), nullable=True),
        StructField("last_edited_by", PARTIAL_USER_TYPE, nullable=True),
        StructField("in_trash", BooleanType(), nullable=True),
        StructField("is_archived", BooleanType(), nullable=True),
        StructField("is_locked", BooleanType(), nullable=True),
        StructField("icon", StringType(), nullable=True),
        StructField("cover", StringType(), nullable=True),
        StructField("parent", PARENT_TYPE, nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("public_url", StringType(), nullable=True),
        StructField("properties", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Blocks schema
# ---------------------------------------------------------------------------

BLOCKS_SCHEMA = StructType(
    [
        StructField("object", StringType(), nullable=True),
        StructField("id", StringType(), nullable=False),
        StructField("parent", PARENT_TYPE, nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("created_time", TimestampType(), nullable=True),
        StructField("created_by", PARTIAL_USER_TYPE, nullable=True),
        StructField("last_edited_time", TimestampType(), nullable=True),
        StructField("last_edited_by", PARTIAL_USER_TYPE, nullable=True),
        StructField("has_children", BooleanType(), nullable=True),
        StructField("in_trash", BooleanType(), nullable=True),
        StructField("content", StringType(), nullable=True),
        StructField("page_id", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Users schema
# ---------------------------------------------------------------------------

USERS_SCHEMA = StructType(
    [
        StructField("object", StringType(), nullable=True),
        StructField("id", StringType(), nullable=False),
        StructField("type", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("avatar_url", StringType(), nullable=True),
        StructField("person", StructType(
            [StructField("email", StringType(), nullable=True)]
        ), nullable=True),
        StructField("bot", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Comments schema
# ---------------------------------------------------------------------------

COMMENTS_SCHEMA = StructType(
    [
        StructField("object", StringType(), nullable=True),
        StructField("id", StringType(), nullable=False),
        StructField("parent", PARENT_TYPE, nullable=True),
        StructField("discussion_id", StringType(), nullable=True),
        StructField("created_time", TimestampType(), nullable=True),
        StructField("last_edited_time", TimestampType(), nullable=True),
        StructField("created_by", PARTIAL_USER_TYPE, nullable=True),
        StructField("rich_text", StringType(), nullable=True),
        StructField("page_id", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Schema and metadata lookups
# ---------------------------------------------------------------------------

TABLE_SCHEMAS = {
    "databases": DATABASES_SCHEMA,
    "pages": PAGES_SCHEMA,
    "blocks": BLOCKS_SCHEMA,
    "users": USERS_SCHEMA,
    "comments": COMMENTS_SCHEMA,
}

TABLE_METADATA = {
    "databases": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "pages": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "blocks": {
        "primary_keys": ["id"],
        "cursor_field": "last_edited_time",
        "ingestion_type": "cdc",
    },
    "users": {
        "primary_keys": ["id"],
        "cursor_field": "",
        "ingestion_type": "snapshot",
    },
    "comments": {
        "primary_keys": ["id"],
        "cursor_field": "created_time",
        "ingestion_type": "cdc",
    },
}
