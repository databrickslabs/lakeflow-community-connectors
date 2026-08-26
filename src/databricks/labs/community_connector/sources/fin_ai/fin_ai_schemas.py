"""Static schemas, metadata, and constants for the Fin.ai (Intercom) connector.

`fin_ai` is an Intercom REST API connector: Fin AI Agent resolution/CSAT data
is carried on the ``conversations`` resource via the nested ``ai_agent`` object.

Timestamp handling: Intercom returns every timestamp as an integer number of
**Unix epoch seconds** (e.g. ``created_at``, ``updated_at``, all ``statistics.*``
timestamps).  These are declared as ``LongType`` and returned verbatim — the
connector never converts them, so downstream users cast to timestamps as
needed.  This keeps the incremental cursor (``updated_at``) a plain integer that
sorts/compares correctly and matches the Search API's numeric ``updated_at``
filter values.

Dynamic, workspace-defined dicts (``custom_attributes``, ``ticket_attributes``)
are modelled as ``MapType(StringType, StringType)`` because their keys are not
knowable ahead of time; the ``data_attributes`` stream describes them.
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
)

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------

# Region -> REST API host. ``api.intercom.io`` auto-routes, but pinning the
# regional host avoids an extra redirect hop for EU/AU workspaces.
BASE_URLS: dict[str, str] = {
    "us": "https://api.intercom.io",
    "eu": "https://api.eu.intercom.io",
    "au": "https://api.au.intercom.io",
}

# Pin the API version explicitly so response shapes don't drift when the app's
# default version is bumped in the Developer Hub.
INTERCOM_VERSION = "2.15"

# Search API page size (default 150, max 150).
DEFAULT_PER_PAGE = 150
MAX_PER_PAGE = 150

# Default partition window (days) for the incremental Search-API tables.
DEFAULT_WINDOW_DAYS = 7

DEFAULT_TIMEOUT = 30  # seconds

RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

# Safety cap on the number of scroll pages read for ``companies`` to avoid an
# unbounded loop if the source never signals end-of-scroll.
MAX_SCROLL_PAGES = 10_000


# ---------------------------------------------------------------------------
# Table sets
# ---------------------------------------------------------------------------

# Tables read incrementally via the Intercom Search API (``updated_at`` cursor
# + range filter). These support time-window partitioning across executors.
PARTITIONED_TABLES: tuple[str, ...] = ("conversations", "contacts", "tickets")

SUPPORTED_TABLES: list[str] = [
    "conversations",
    "contacts",
    "companies",
    "tickets",
    "admins",
    "tags",
    "segments",
    "data_attributes",
    "teams",
]

# Search-API tables: table -> (path, records_key in the response envelope).
SEARCH_ENDPOINTS: dict[str, tuple[str, str]] = {
    "conversations": ("/conversations/search", "conversations"),
    "contacts": ("/contacts/search", "data"),
    "tickets": ("/tickets/search", "tickets"),
}

# Simple GET snapshot tables: table -> (path, records_key).
SNAPSHOT_ENDPOINTS: dict[str, tuple[str, str]] = {
    "admins": ("/admins", "admins"),
    "tags": ("/tags", "data"),
    "segments": ("/segments", "segments"),
    "data_attributes": ("/data_attributes", "data"),
    "teams": ("/teams", "teams"),
}


# ---------------------------------------------------------------------------
# Reusable nested structs
# ---------------------------------------------------------------------------

_AUTHOR = StructType(
    [
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ]
)

_CONTACT_REF = StructType(
    [
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("external_id", StringType(), True),
    ]
)

_CONTACT_LIST = StructType(
    [
        StructField("type", StringType(), True),
        StructField("contacts", ArrayType(_CONTACT_REF), True),
    ]
)

_TAG_EMBED = StructType(
    [
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("applied_at", LongType(), True),
        StructField("applied_by", MapType(StringType(), StringType()), True),
    ]
)

_TAG_LIST = StructType(
    [
        StructField("type", StringType(), True),
        StructField("tags", ArrayType(_TAG_EMBED), True),
    ]
)

# ``list`` reference sub-objects on contacts (tags/notes/companies): the newer
# ``{"type": "list", "data": [...], "total_count": N}`` shape.
_REF_LIST = StructType(
    [
        StructField("type", StringType(), True),
        StructField("data", ArrayType(MapType(StringType(), StringType())), True),
        StructField("url", StringType(), True),
        StructField("total_count", LongType(), True),
        StructField("has_more", BooleanType(), True),
    ]
)

_SOURCE = StructType(
    [
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("delivered_as", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("body", StringType(), True),
        StructField("author", _AUTHOR, True),
        StructField(
            "attachments", ArrayType(MapType(StringType(), StringType())), True
        ),
        StructField("url", StringType(), True),
        StructField("redacted", BooleanType(), True),
    ]
)

_CONVERSATION_RATING = StructType(
    [
        StructField("rating", LongType(), True),
        StructField("remark", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),
        StructField("contact", _CONTACT_REF, True),
        StructField("teammate", _AUTHOR, True),
    ]
)

_STATISTICS = StructType(
    [
        StructField("type", StringType(), True),
        StructField("time_to_assignment", LongType(), True),
        StructField("time_to_admin_reply", LongType(), True),
        StructField("time_to_first_close", LongType(), True),
        StructField("time_to_last_close", LongType(), True),
        StructField("median_time_to_reply", LongType(), True),
        StructField("first_contact_reply_at", LongType(), True),
        StructField("first_assignment_at", LongType(), True),
        StructField("first_admin_reply_at", LongType(), True),
        StructField("first_close_at", LongType(), True),
        StructField("last_assignment_at", LongType(), True),
        StructField("last_assignment_admin_reply_at", LongType(), True),
        StructField("last_contact_reply_at", LongType(), True),
        StructField("last_admin_reply_at", LongType(), True),
        StructField("last_close_at", LongType(), True),
        StructField("last_closed_by_id", StringType(), True),
        StructField("count_reopens", LongType(), True),
        StructField("count_assignments", LongType(), True),
        StructField("count_conversation_parts", LongType(), True),
        StructField("handling_time", LongType(), True),
        StructField("adjusted_handling_time", LongType(), True),
    ]
)

# The Fin AI Agent sub-object — the core "Fin.ai" data this connector surfaces.
_AI_AGENT = StructType(
    [
        StructField("source_type", StringType(), True),
        StructField("source_title", StringType(), True),
        StructField("last_answer_type", StringType(), True),
        StructField("resolution_state", StringType(), True),
        StructField("rating", LongType(), True),
        StructField("rating_remark", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),
        StructField("content_sources", MapType(StringType(), StringType()), True),
    ]
)

_LINKED_OBJECTS = StructType(
    [
        StructField("type", StringType(), True),
        StructField("data", ArrayType(MapType(StringType(), StringType())), True),
        StructField("total_count", LongType(), True),
        StructField("has_more", BooleanType(), True),
    ]
)

_TICKET_REF = StructType(
    [
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# 1. conversations
# ---------------------------------------------------------------------------
CONVERSATIONS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),  # incremental cursor
        StructField("waiting_since", LongType(), True),
        StructField("snoozed_until", LongType(), True),
        StructField("open", BooleanType(), True),
        StructField("state", StringType(), True),
        StructField("read", BooleanType(), True),
        StructField("priority", StringType(), True),
        StructField("admin_assignee_id", LongType(), True),
        StructField("team_assignee_id", LongType(), True),
        StructField("source", _SOURCE, True),
        StructField("contacts", _CONTACT_LIST, True),
        StructField("teammates", MapType(StringType(), StringType()), True),
        StructField("tags", _TAG_LIST, True),
        StructField("conversation_rating", _CONVERSATION_RATING, True),
        StructField("statistics", _STATISTICS, True),
        StructField("custom_attributes", MapType(StringType(), StringType()), True),
        StructField("topics", MapType(StringType(), StringType()), True),
        StructField("ticket", _TICKET_REF, True),
        StructField("linked_objects", _LINKED_OBJECTS, True),
        StructField("ai_agent_participated", BooleanType(), True),
        StructField("ai_agent", _AI_AGENT, True),
    ]
)


# ---------------------------------------------------------------------------
# 2. contacts
# ---------------------------------------------------------------------------
CONTACTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("external_id", StringType(), True),
        StructField("workspace_id", StringType(), True),
        StructField("role", StringType(), True),
        StructField("email", StringType(), True),
        StructField("email_domain", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("name", StringType(), True),
        StructField("owner_id", LongType(), True),
        StructField("has_hard_bounced", BooleanType(), True),
        StructField("marked_email_as_spam", BooleanType(), True),
        StructField("unsubscribed_from_emails", BooleanType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),  # incremental cursor
        StructField("signed_up_at", LongType(), True),
        StructField("last_seen_at", LongType(), True),
        StructField("last_replied_at", LongType(), True),
        StructField("last_contacted_at", LongType(), True),
        StructField("last_email_opened_at", LongType(), True),
        StructField("last_email_clicked_at", LongType(), True),
        StructField("language_override", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("browser_version", StringType(), True),
        StructField("browser_language", StringType(), True),
        StructField("os", StringType(), True),
        StructField("android_app_name", StringType(), True),
        StructField("android_app_version", StringType(), True),
        StructField("android_device", StringType(), True),
        StructField("android_os_version", StringType(), True),
        StructField("android_sdk_version", StringType(), True),
        StructField("android_last_seen_at", LongType(), True),
        StructField("ios_app_name", StringType(), True),
        StructField("ios_app_version", StringType(), True),
        StructField("ios_device", StringType(), True),
        StructField("ios_os_version", StringType(), True),
        StructField("ios_sdk_version", StringType(), True),
        StructField("ios_last_seen_at", LongType(), True),
        StructField("custom_attributes", MapType(StringType(), StringType()), True),
        StructField(
            "avatar",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("image_url", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("tags", _REF_LIST, True),
        StructField("notes", _REF_LIST, True),
        StructField("companies", _REF_LIST, True),
        StructField(
            "location",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("region", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("country_code", StringType(), True),
                    StructField("continent_code", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("social_profiles", _REF_LIST, True),
    ]
)


# ---------------------------------------------------------------------------
# 3. companies
# ---------------------------------------------------------------------------
_SEGMENT_EMBED = StructType(
    [
        StructField("type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

COMPANIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("company_id", StringType(), True),
        StructField("app_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "plan",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("remote_created_at", LongType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),
        StructField("last_request_at", LongType(), True),
        StructField("size", LongType(), True),
        StructField("website", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("monthly_spend", LongType(), True),
        StructField("session_count", LongType(), True),
        StructField("user_count", LongType(), True),
        StructField("custom_attributes", MapType(StringType(), StringType()), True),
        StructField("tags", _TAG_LIST, True),
        StructField(
            "segments",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("segments", ArrayType(_SEGMENT_EMBED), True),
                ]
            ),
            True,
        ),
    ]
)


# ---------------------------------------------------------------------------
# 4. tickets
# ---------------------------------------------------------------------------
_TICKET_STATE = StructType(
    [
        StructField("id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("internal_label", StringType(), True),
        StructField("external_label", StringType(), True),
    ]
)

_TICKET_TYPE = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("category", StringType(), True),
        StructField("archived", BooleanType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),
        StructField(
            "ticket_type_attributes",
            MapType(StringType(), StringType()),
            True,
        ),
    ]
)

TICKETS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("ticket_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("ticket_attributes", MapType(StringType(), StringType()), True),
        StructField("ticket_state", _TICKET_STATE, True),
        StructField("ticket_type", _TICKET_TYPE, True),
        StructField("contacts", _CONTACT_LIST, True),
        StructField("admin_assignee_id", StringType(), True),
        StructField("team_assignee_id", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),  # incremental cursor
        StructField("open", BooleanType(), True),
        StructField("snoozed_until", LongType(), True),
        StructField("linked_objects", _LINKED_OBJECTS, True),
        StructField(
            "ticket_parts",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField(
                        "ticket_parts",
                        ArrayType(MapType(StringType(), StringType())),
                        True,
                    ),
                    StructField("total_count", LongType(), True),
                ]
            ),
            True,
        ),
        StructField("is_shared", BooleanType(), True),
    ]
)


# ---------------------------------------------------------------------------
# 5. tags
# ---------------------------------------------------------------------------
TAGS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("applied_at", LongType(), True),
        StructField("applied_by", MapType(StringType(), StringType()), True),
    ]
)


# ---------------------------------------------------------------------------
# 6. segments
# ---------------------------------------------------------------------------
SEGMENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),
        StructField("person_type", StringType(), True),
        StructField("count", LongType(), True),
    ]
)


# ---------------------------------------------------------------------------
# 7. data_attributes  (composite PK: full_name + model)
# ---------------------------------------------------------------------------
DATA_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("model", StringType(), False),
        StructField("name", StringType(), True),
        StructField("full_name", StringType(), False),
        StructField("label", StringType(), True),
        StructField("description", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("options", ArrayType(StringType()), True),
        StructField("api_writable", BooleanType(), True),
        StructField("messenger_writable", BooleanType(), True),
        StructField("ui_writable", BooleanType(), True),
        StructField("custom", BooleanType(), True),
        StructField("archived", BooleanType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True),
        StructField("admin_id", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# 8. admins
# ---------------------------------------------------------------------------
ADMINS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("away_mode_enabled", BooleanType(), True),
        StructField("away_mode_reassign", BooleanType(), True),
        StructField("away_status_reason_id", LongType(), True),
        StructField("has_inbox_seat", BooleanType(), True),
        StructField("team_ids", ArrayType(LongType()), True),
        # Intercom returns ``avatar`` as an object ``{type, image_url}`` (verified
        # live against GET /admins), not a bare URL string — model it as a struct
        # so the image URL is preserved rather than stringified.
        StructField(
            "avatar",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("image_url", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("team_priority_level", MapType(StringType(), StringType()), True),
    ]
)


# ---------------------------------------------------------------------------
# 9. teams
# ---------------------------------------------------------------------------
TEAMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("admin_ids", ArrayType(LongType()), True),
        StructField(
            "admin_priority_level",
            StructType(
                [
                    StructField(
                        "primary_admin_ids", ArrayType(LongType()), True
                    ),
                    StructField(
                        "secondary_admin_ids", ArrayType(LongType()), True
                    ),
                ]
            ),
            True,
        ),
        StructField("assignment_limit", LongType(), True),
        StructField("distribution_method", StringType(), True),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "conversations": CONVERSATIONS_SCHEMA,
    "contacts": CONTACTS_SCHEMA,
    "companies": COMPANIES_SCHEMA,
    "tickets": TICKETS_SCHEMA,
    "admins": ADMINS_SCHEMA,
    "tags": TAGS_SCHEMA,
    "segments": SEGMENTS_SCHEMA,
    "data_attributes": DATA_ATTRIBUTES_SCHEMA,
    "teams": TEAMS_SCHEMA,
}


# ---------------------------------------------------------------------------
# Per-table metadata
# ---------------------------------------------------------------------------
# CDC tables use ``updated_at`` (Unix seconds) as the incremental cursor; the
# snapshot tables have no server-side time filter and are re-read in full.
# None of these expose a hard-delete feed, so ``cdc`` (no deletes) is correct.
TABLE_METADATA: dict[str, dict] = {
    "conversations": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "contacts": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "tickets": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "companies": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "admins": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "tags": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "segments": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "data_attributes": {
        "primary_keys": ["full_name", "model"],
        "ingestion_type": "snapshot",
    },
    "teams": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
}
