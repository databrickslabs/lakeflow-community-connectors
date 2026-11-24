"""Schemas, metadata, and per-table configuration for the WordPress connector.

Schemas are static per WordPress core (``wp/v2``) and are hard-coded here so
the simulate-mode test corpus can be bootstrapped from ``TABLE_SCHEMAS`` via
``tools.corpus_from_schema.write_corpus_from_schemas``.

Only ``context=view`` (unauthenticated-safe) fields are modelled.  Edit-only
fields (``raw`` bodies, ``password``, user PII, ``author_email``/``author_ip``),
HATEOAS ``_links``, and the free-form ``meta`` object are intentionally omitted:
their shapes are install-specific and would break type coercion.

See ``wordpress_api_doc.md`` for field-level provenance.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------- #
# Reusable nested struct builders
# --------------------------------------------------------------------------- #


def _rendered_struct() -> StructType:
    """``{ "rendered": "<html>" }`` — guid / title / description / caption."""
    return StructType([StructField("rendered", StringType(), nullable=True)])


def _rendered_protected_struct() -> StructType:
    """``{ "rendered": "<html>", "protected": bool }`` — content / excerpt."""
    return StructType(
        [
            StructField("rendered", StringType(), nullable=True),
            StructField("protected", BooleanType(), nullable=True),
        ]
    )


def _avatar_urls_struct() -> StructType:
    """Avatar/gravatar URLs keyed by pixel size (24 / 48 / 96)."""
    return StructType(
        [
            StructField("24", StringType(), nullable=True),
            StructField("48", StringType(), nullable=True),
            StructField("96", StringType(), nullable=True),
        ]
    )


def _media_details_struct() -> StructType:
    """Shallow subset of ``media_details`` common to image attachments.

    The full object varies by ``media_type`` (image vs file) and includes a
    per-size ``sizes`` map with install-specific keys, so only the stable
    scalar fields are modelled here.
    """
    return StructType(
        [
            StructField("width", LongType(), nullable=True),
            StructField("height", LongType(), nullable=True),
            StructField("file", StringType(), nullable=True),
            StructField("filesize", LongType(), nullable=True),
        ]
    )


# --------------------------------------------------------------------------- #
# Table schemas
# --------------------------------------------------------------------------- #

_POSTS_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("date", TimestampType(), nullable=True),
        StructField("date_gmt", TimestampType(), nullable=True),
        StructField("modified", TimestampType(), nullable=True),
        StructField("modified_gmt", TimestampType(), nullable=True),
        StructField("guid", _rendered_struct(), nullable=True),
        StructField("slug", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("title", _rendered_struct(), nullable=True),
        StructField("content", _rendered_protected_struct(), nullable=True),
        StructField("excerpt", _rendered_protected_struct(), nullable=True),
        StructField("author", LongType(), nullable=True),
        StructField("featured_media", LongType(), nullable=True),
        StructField("comment_status", StringType(), nullable=True),
        StructField("ping_status", StringType(), nullable=True),
        StructField("sticky", BooleanType(), nullable=True),
        StructField("template", StringType(), nullable=True),
        StructField("format", StringType(), nullable=True),
        StructField("categories", ArrayType(LongType()), nullable=True),
        StructField("tags", ArrayType(LongType()), nullable=True),
    ]
)

_PAGES_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("date", TimestampType(), nullable=True),
        StructField("date_gmt", TimestampType(), nullable=True),
        StructField("modified", TimestampType(), nullable=True),
        StructField("modified_gmt", TimestampType(), nullable=True),
        StructField("guid", _rendered_struct(), nullable=True),
        StructField("slug", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("title", _rendered_struct(), nullable=True),
        StructField("content", _rendered_protected_struct(), nullable=True),
        StructField("excerpt", _rendered_protected_struct(), nullable=True),
        StructField("author", LongType(), nullable=True),
        StructField("featured_media", LongType(), nullable=True),
        StructField("comment_status", StringType(), nullable=True),
        StructField("ping_status", StringType(), nullable=True),
        StructField("template", StringType(), nullable=True),
        StructField("parent", LongType(), nullable=True),
        StructField("menu_order", LongType(), nullable=True),
    ]
)

_MEDIA_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("date", TimestampType(), nullable=True),
        StructField("date_gmt", TimestampType(), nullable=True),
        StructField("modified", TimestampType(), nullable=True),
        StructField("modified_gmt", TimestampType(), nullable=True),
        StructField("guid", _rendered_struct(), nullable=True),
        StructField("slug", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("title", _rendered_struct(), nullable=True),
        StructField("author", LongType(), nullable=True),
        StructField("comment_status", StringType(), nullable=True),
        StructField("ping_status", StringType(), nullable=True),
        StructField("template", StringType(), nullable=True),
        StructField("description", _rendered_struct(), nullable=True),
        StructField("caption", _rendered_struct(), nullable=True),
        StructField("alt_text", StringType(), nullable=True),
        StructField("media_type", StringType(), nullable=True),
        StructField("mime_type", StringType(), nullable=True),
        StructField("media_details", _media_details_struct(), nullable=True),
        StructField("post", LongType(), nullable=True),
        StructField("source_url", StringType(), nullable=True),
    ]
)

_COMMENTS_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("post", LongType(), nullable=True),
        StructField("parent", LongType(), nullable=True),
        StructField("author", LongType(), nullable=True),
        StructField("author_name", StringType(), nullable=True),
        StructField("author_url", StringType(), nullable=True),
        StructField("date", TimestampType(), nullable=True),
        StructField("date_gmt", TimestampType(), nullable=True),
        StructField("content", _rendered_struct(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("author_avatar_urls", _avatar_urls_struct(), nullable=True),
    ]
)

_CATEGORIES_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("count", LongType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("slug", StringType(), nullable=True),
        StructField("taxonomy", StringType(), nullable=True),
        StructField("parent", LongType(), nullable=True),
    ]
)

_TAGS_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("count", LongType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("slug", StringType(), nullable=True),
        StructField("taxonomy", StringType(), nullable=True),
    ]
)

_USERS_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("link", StringType(), nullable=True),
        StructField("slug", StringType(), nullable=True),
        StructField("avatar_urls", _avatar_urls_struct(), nullable=True),
    ]
)

_TAXONOMIES_SCHEMA = StructType(
    [
        StructField("name", StringType(), nullable=True),
        StructField("slug", StringType(), nullable=False),
        StructField("description", StringType(), nullable=True),
        StructField("types", ArrayType(StringType()), nullable=True),
        StructField("hierarchical", BooleanType(), nullable=True),
        StructField("rest_base", StringType(), nullable=True),
    ]
)

TABLE_SCHEMAS: dict[str, StructType] = {
    "posts": _POSTS_SCHEMA,
    "pages": _PAGES_SCHEMA,
    "media": _MEDIA_SCHEMA,
    "comments": _COMMENTS_SCHEMA,
    "categories": _CATEGORIES_SCHEMA,
    "tags": _TAGS_SCHEMA,
    "users": _USERS_SCHEMA,
    "taxonomies": _TAXONOMIES_SCHEMA,
}

# --------------------------------------------------------------------------- #
# Per-table configuration
# --------------------------------------------------------------------------- #
#
# ``endpoint``       — path segment under ``/wp-json/wp/v2/``.
# ``ingestion``      — snapshot / cdc / append.
# ``cursor``         — record field holding the incremental watermark (UTC).
# ``after_param`` /  — WordPress query params bounding the cursor range.
#   ``before_param``
# ``primary_keys``   — used for merge + corpus PK uniqueness.
# ``partitioned``    — True → partitioned streaming path; False → snapshot via
#                      ``read_table`` (simpleStreamReader).
# ``dict_shaped``    — response is a dict keyed by slug rather than an array.
# ``sort_field``     — WordPress ``orderby`` value for a stable ascending read.

TABLE_CONFIG: dict[str, dict] = {
    "posts": {
        "endpoint": "posts",
        "ingestion": "cdc",
        "cursor": "modified_gmt",
        "after_param": "modified_after",
        "before_param": "modified_before",
        "primary_keys": ["id"],
        "partitioned": True,
        "sort_field": "modified",
    },
    "pages": {
        "endpoint": "pages",
        "ingestion": "cdc",
        "cursor": "modified_gmt",
        "after_param": "modified_after",
        "before_param": "modified_before",
        "primary_keys": ["id"],
        "partitioned": True,
        "sort_field": "modified",
    },
    "media": {
        "endpoint": "media",
        "ingestion": "cdc",
        "cursor": "modified_gmt",
        "after_param": "modified_after",
        "before_param": "modified_before",
        "primary_keys": ["id"],
        "partitioned": True,
        "sort_field": "modified",
    },
    "comments": {
        "endpoint": "comments",
        "ingestion": "append",
        "cursor": "date_gmt",
        "after_param": "after",
        "before_param": "before",
        "primary_keys": ["id"],
        "partitioned": True,
        "sort_field": "date",
    },
    "categories": {
        "endpoint": "categories",
        "ingestion": "snapshot",
        "cursor": None,
        "primary_keys": ["id"],
        "partitioned": False,
    },
    "tags": {
        "endpoint": "tags",
        "ingestion": "snapshot",
        "cursor": None,
        "primary_keys": ["id"],
        "partitioned": False,
    },
    "users": {
        "endpoint": "users",
        "ingestion": "snapshot",
        "cursor": None,
        "primary_keys": ["id"],
        "partitioned": False,
    },
    "taxonomies": {
        "endpoint": "taxonomies",
        "ingestion": "snapshot",
        "cursor": None,
        "primary_keys": ["slug"],
        "partitioned": False,
        "dict_shaped": True,
    },
}

SUPPORTED_TABLES: list[str] = list(TABLE_CONFIG.keys())


def build_metadata(table_name: str) -> dict:
    """Return LakeflowConnect metadata for ``table_name``."""
    cfg = TABLE_CONFIG[table_name]
    return {
        "primary_keys": list(cfg["primary_keys"]),
        "cursor_field": cfg.get("cursor"),
        "ingestion_type": cfg["ingestion"],
    }
