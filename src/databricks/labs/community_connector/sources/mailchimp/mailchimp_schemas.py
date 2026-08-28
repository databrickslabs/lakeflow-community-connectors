"""Schemas, metadata, and per-table configuration for the Mailchimp connector.

Mailchimp exposes no schema-discovery endpoint, so every table schema is
hard-coded here from the documented v3.0 response shapes (see
``mailchimp_api_doc.md``).

Type conventions (per the implement-connector skill):
- Integers use ``LongType`` (not ``IntegerType``) to avoid overflow.
- Fractional rates (open_rate, click_rate, ...) use ``DoubleType``.
- Nested objects are preserved as ``StructType`` (never flattened).
- ``tags`` is an ``ArrayType`` of a ``StructType``.
- ``merge_fields`` and ``interests`` have a *dynamic* key set that varies per
  audience, so they cannot map to a fixed ``StructType``. They are modeled as
  ``StringType`` columns holding the raw JSON object as a string. The connector
  serializes those two fields to JSON in ``read_table`` (the only place it
  reshapes a value) precisely because a fixed struct cannot represent them.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RETRIABLE_STATUS_CODES = {429, 500, 503}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

# Default table-option values.
DEFAULT_MAX_RECORDS_PER_BATCH = 200
DEFAULT_WINDOW_SECONDS = 86400  # 1 day
DEFAULT_PAGE_COUNT = 1000  # Mailchimp max page size

# Fields whose dynamic key set is serialized to a JSON string on read.
JSON_STRING_FIELDS = {
    "campaigns": [],
    "reports": [],
    "lists": [],
    "members": ["merge_fields", "interests"],
}


# --------------------------------------------------------------------------
# Reusable nested structs
# --------------------------------------------------------------------------

_RECIPIENTS = StructType(
    [
        StructField("list_id", StringType(), True),
        StructField("list_is_active", BooleanType(), True),
        StructField("list_name", StringType(), True),
        StructField("segment_text", StringType(), True),
        StructField("recipient_count", LongType(), True),
    ]
)

_SETTINGS = StructType(
    [
        StructField("subject_line", StringType(), True),
        StructField("preview_text", StringType(), True),
        StructField("title", StringType(), True),
        StructField("from_name", StringType(), True),
        StructField("reply_to", StringType(), True),
        StructField("use_conversation", BooleanType(), True),
        StructField("to_name", StringType(), True),
        StructField("folder_id", StringType(), True),
        StructField("authenticate", BooleanType(), True),
        StructField("auto_footer", BooleanType(), True),
        StructField("inline_css", BooleanType(), True),
        StructField("auto_tweet", BooleanType(), True),
        StructField("fb_comments", BooleanType(), True),
        StructField("template_id", LongType(), True),
        StructField("drag_and_drop", BooleanType(), True),
    ]
)

_TRACKING = StructType(
    [
        StructField("opens", BooleanType(), True),
        StructField("html_clicks", BooleanType(), True),
        StructField("text_clicks", BooleanType(), True),
        StructField("goal_tracking", BooleanType(), True),
        StructField("ecomm360", BooleanType(), True),
        StructField("google_analytics", StringType(), True),
        StructField("clicktale", StringType(), True),
    ]
)

_REPORT_SUMMARY = StructType(
    [
        StructField("opens", LongType(), True),
        StructField("unique_opens", LongType(), True),
        StructField("open_rate", DoubleType(), True),
        StructField("clicks", LongType(), True),
        StructField("subscriber_clicks", LongType(), True),
        StructField("click_rate", DoubleType(), True),
        StructField(
            "ecommerce",
            StructType(
                [
                    StructField("total_orders", LongType(), True),
                    StructField("total_spent", DoubleType(), True),
                    StructField("total_revenue", DoubleType(), True),
                ]
            ),
            True,
        ),
    ]
)

_DELIVERY_STATUS = StructType(
    [
        StructField("enabled", BooleanType(), True),
        StructField("can_cancel", BooleanType(), True),
        StructField("status", StringType(), True),
        StructField("emails_sent", LongType(), True),
        StructField("emails_canceled", LongType(), True),
    ]
)

# --- report-specific nested structs ---

_BOUNCES = StructType(
    [
        StructField("hard_bounces", LongType(), True),
        StructField("soft_bounces", LongType(), True),
        StructField("syntax_errors", LongType(), True),
    ]
)

_FORWARDS = StructType(
    [
        StructField("forwards_count", LongType(), True),
        StructField("forwards_opens", LongType(), True),
    ]
)

_OPENS = StructType(
    [
        StructField("opens_total", LongType(), True),
        StructField("unique_opens", LongType(), True),
        StructField("open_rate", DoubleType(), True),
        StructField("last_open", StringType(), True),
    ]
)

_CLICKS = StructType(
    [
        StructField("clicks_total", LongType(), True),
        StructField("unique_clicks", LongType(), True),
        StructField("unique_subscriber_clicks", LongType(), True),
        StructField("click_rate", DoubleType(), True),
        StructField("last_click", StringType(), True),
    ]
)

_FACEBOOK_LIKES = StructType(
    [
        StructField("recipient_likes", LongType(), True),
        StructField("unique_likes", LongType(), True),
        StructField("facebook_likes", LongType(), True),
    ]
)

_LIST_STATS = StructType(
    [
        StructField("sub_rate", DoubleType(), True),
        StructField("unsub_rate", DoubleType(), True),
        StructField("open_rate", DoubleType(), True),
        StructField("click_rate", DoubleType(), True),
    ]
)

_ECOMMERCE = StructType(
    [
        StructField("total_orders", LongType(), True),
        StructField("total_spent", DoubleType(), True),
        StructField("total_revenue", DoubleType(), True),
    ]
)

# --- list-specific nested structs ---

_CONTACT = StructType(
    [
        StructField("company", StringType(), True),
        StructField("address1", StringType(), True),
        StructField("address2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("country", StringType(), True),
        StructField("phone", StringType(), True),
    ]
)

_CAMPAIGN_DEFAULTS = StructType(
    [
        StructField("from_name", StringType(), True),
        StructField("from_email", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("language", StringType(), True),
    ]
)

_LIST_STATS_FULL = StructType(
    [
        StructField("member_count", LongType(), True),
        StructField("unsubscribe_count", LongType(), True),
        StructField("cleaned_count", LongType(), True),
        StructField("member_count_since_send", LongType(), True),
        StructField("unsubscribe_count_since_send", LongType(), True),
        StructField("cleaned_count_since_send", LongType(), True),
        StructField("campaign_count", LongType(), True),
        StructField("campaign_last_sent", StringType(), True),
        StructField("merge_field_count", LongType(), True),
        StructField("avg_sub_rate", DoubleType(), True),
        StructField("avg_unsub_rate", DoubleType(), True),
        StructField("target_sub_rate", DoubleType(), True),
        StructField("open_rate", DoubleType(), True),
        StructField("click_rate", DoubleType(), True),
        StructField("last_sub_date", StringType(), True),
        StructField("last_unsub_date", StringType(), True),
    ]
)

# --- member-specific nested structs ---

_MEMBER_STATS = StructType(
    [
        StructField("avg_open_rate", DoubleType(), True),
        StructField("avg_click_rate", DoubleType(), True),
        StructField(
            "ecommerce_data",
            StructType(
                [
                    StructField("total_revenue", DoubleType(), True),
                    StructField("number_of_orders", LongType(), True),
                    StructField("currency_code", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

_LOCATION = StructType(
    [
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("gmtoff", LongType(), True),
        StructField("dstoff", LongType(), True),
        StructField("country_code", StringType(), True),
        StructField("timezone", StringType(), True),
    ]
)

_TAGS = ArrayType(
    StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
    )
)


# --------------------------------------------------------------------------
# Top-level table schemas
# --------------------------------------------------------------------------

CAMPAIGNS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("web_id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("create_time", StringType(), True),
        StructField("send_time", StringType(), True),
        StructField("status", StringType(), True),
        StructField("emails_sent", LongType(), True),
        StructField("archive_url", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("recipients", _RECIPIENTS, True),
        StructField("settings", _SETTINGS, True),
        StructField("tracking", _TRACKING, True),
        StructField("report_summary", _REPORT_SUMMARY, True),
        StructField("delivery_status", _DELIVERY_STATUS, True),
    ]
)

REPORTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("campaign_title", StringType(), True),
        StructField("type", StringType(), True),
        StructField("list_id", StringType(), True),
        StructField("list_is_active", BooleanType(), True),
        StructField("list_name", StringType(), True),
        StructField("subject_line", StringType(), True),
        StructField("preview_text", StringType(), True),
        StructField("emails_sent", LongType(), True),
        StructField("abuse_reports", LongType(), True),
        StructField("unsubscribed", LongType(), True),
        StructField("send_time", StringType(), True),
        StructField("bounces", _BOUNCES, True),
        StructField("forwards", _FORWARDS, True),
        StructField("opens", _OPENS, True),
        StructField("clicks", _CLICKS, True),
        StructField("facebook_likes", _FACEBOOK_LIKES, True),
        StructField("list_stats", _LIST_STATS, True),
        StructField("ecommerce", _ECOMMERCE, True),
        StructField("delivery_status", _DELIVERY_STATUS, True),
    ]
)

LISTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("web_id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("contact", _CONTACT, True),
        StructField("permission_reminder", StringType(), True),
        StructField("use_archive_bar", BooleanType(), True),
        StructField("campaign_defaults", _CAMPAIGN_DEFAULTS, True),
        StructField("notify_on_subscribe", StringType(), True),
        StructField("notify_on_unsubscribe", StringType(), True),
        StructField("date_created", StringType(), True),
        StructField("list_rating", LongType(), True),
        StructField("email_type_option", BooleanType(), True),
        StructField("subscribe_url_short", StringType(), True),
        StructField("subscribe_url_long", StringType(), True),
        StructField("beamer_address", StringType(), True),
        StructField("visibility", StringType(), True),
        StructField("double_optin", BooleanType(), True),
        StructField("marketing_permissions", BooleanType(), True),
        StructField("stats", _LIST_STATS_FULL, True),
    ]
)

MEMBERS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("email_address", StringType(), True),
        StructField("unique_email_id", StringType(), True),
        StructField("contact_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("web_id", LongType(), True),
        StructField("email_type", StringType(), True),
        StructField("status", StringType(), True),
        # Dynamic key set per audience -> raw JSON string (see module docstring).
        StructField("merge_fields", StringType(), True),
        StructField("interests", StringType(), True),
        StructField("stats", _MEMBER_STATS, True),
        StructField("ip_signup", StringType(), True),
        StructField("timestamp_signup", StringType(), True),
        StructField("ip_opt", StringType(), True),
        StructField("timestamp_opt", StringType(), True),
        StructField("member_rating", LongType(), True),
        StructField("last_changed", StringType(), True),
        StructField("language", StringType(), True),
        StructField("vip", BooleanType(), True),
        StructField("email_client", StringType(), True),
        StructField("location", _LOCATION, True),
        StructField("tags_count", LongType(), True),
        StructField("tags", _TAGS, True),
        StructField("list_id", StringType(), False),
    ]
)

TABLE_SCHEMAS = {
    "campaigns": CAMPAIGNS_SCHEMA,
    "reports": REPORTS_SCHEMA,
    "lists": LISTS_SCHEMA,
    "members": MEMBERS_SCHEMA,
}


# --------------------------------------------------------------------------
# Per-table configuration driving reads / metadata
# --------------------------------------------------------------------------
#
# resource_key : key wrapping the record array in the list response envelope.
# path         : list endpoint path (relative to base_url). ``members`` is
#                nested and templated per list_id at read time.
# cursor_field : incremental cursor column.
# since_param  : query param for the window lower bound (exclusive / >; the
#                connector applies a small lookback so the boundary row is kept).
# before_param : query param for the window upper bound (<).
# primary_keys : effective PK for the unioned output table.
# supports_sort: whether the list endpoint honours sort_field/sort_dir. Only
#                campaigns and members do; reports/lists do not.
TABLE_CONFIG = {
    "campaigns": {
        "resource_key": "campaigns",
        "path": "/campaigns",
        "cursor_field": "create_time",
        "since_param": "since_create_time",
        "before_param": "before_create_time",
        "primary_keys": ["id"],
        "supports_sort": True,
        "nested": False,
    },
    "reports": {
        "resource_key": "reports",
        "path": "/reports",
        "cursor_field": "send_time",
        "since_param": "since_send_time",
        "before_param": "before_send_time",
        "primary_keys": ["id"],
        "supports_sort": False,
        "nested": False,
    },
    "lists": {
        "resource_key": "lists",
        "path": "/lists",
        "cursor_field": "date_created",
        "since_param": "since_date_created",
        "before_param": "before_date_created",
        "primary_keys": ["id"],
        "supports_sort": False,
        "nested": False,
    },
    "members": {
        "resource_key": "members",
        "path": "/lists/{list_id}/members",
        "cursor_field": "last_changed",
        "since_param": "since_last_changed",
        "before_param": "before_last_changed",
        "primary_keys": ["list_id", "id"],
        "supports_sort": True,
        "nested": True,
    },
}
