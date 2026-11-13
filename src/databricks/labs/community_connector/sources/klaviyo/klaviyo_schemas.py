"""Static schema and metadata definitions for the Klaviyo connector.

Schemas are derived from the Klaviyo JSON:API stable revision
``2024-10-15``.  See ``klaviyo_api_doc.md`` for field references and
per-table edge cases.

Schema design choices
---------------------

* **Flatten the JSON:API envelope.**  Klaviyo wraps records as
  ``{"type", "id", "attributes": {...}, "relationships": {...}}``.
  We hoist ``id`` and ``type`` to the top level alongside every
  attribute field, so primary keys (``id``) are flat top-level columns
  and don't carry dotted paths.  The runtime engine requires PKs to be
  flat names.

* **``StructType`` for shape-stable nested objects.**  Things like
  ``location`` on profiles, ``audiences`` / ``send_options`` on
  campaigns, and ``integration`` on metrics have a documented closed
  set of fields and merit explicit ``StructType``.

* **``MapType(StringType, StringType)``** for genuinely open-ended
  free-form objects.  Klaviyo's ``properties`` on profiles,
  ``event_properties`` on events, and ``definition`` on segments are
  arbitrary user-supplied JSON whose schema cannot be pinned.

* **No dotted PKs.**  All primary keys are flat top-level columns.

* **Datetimes as ``StringType``.**  Klaviyo emits ISO 8601 RFC 3339
  strings; we store them as strings and let downstream consumers cast.
  This matches the Shopify and Alchemy connectors in this repo.

* **``LongType``** for integer fields (e.g. ``timestamp`` on events
  in Unix-epoch-seconds).  ``IntegerType`` is too small for some
  numeric IDs and timestamps.
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
# Reusable nested struct definitions
# =============================================================================

# profiles.location — closed-set address-shape object.
PROFILE_LOCATION_STRUCT = StructType(
    [
        StructField("address1", StringType(), True),
        StructField("address2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("timezone", StringType(), True),
        # API emits these as strings despite being numeric — match the
        # API shape and let downstream cast if desired.
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("ip", StringType(), True),
    ]
)

# campaigns.audiences — list-of-ID arrays.
CAMPAIGN_AUDIENCES_STRUCT = StructType(
    [
        StructField("included", ArrayType(StringType()), True),
        StructField("excluded", ArrayType(StringType()), True),
    ]
)

# campaigns.send_options — boolean flags.
CAMPAIGN_SEND_OPTIONS_STRUCT = StructType(
    [
        StructField("use_smart_sending", BooleanType(), True),
        StructField("ignore_unsubscribes", BooleanType(), True),
    ]
)

# campaigns.tracking_options.custom_tracking_params[*]
#
# Verified live against API revision 2024-10-15: the field is
# ``custom_tracking_params``, not ``utm_params``.  Each entry has
# ``type`` ("static" or "dynamic") and ``value``; ``name`` is only
# present on dynamic entries.  We keep ``name`` nullable to cover both.
CUSTOM_TRACKING_PARAM_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

# campaigns.tracking_options — booleans plus optional tracking-param array.
#
# Verified live: the boolean field is ``add_tracking_params`` (not
# ``is_add_utm``) and the array field is ``custom_tracking_params``
# (not ``utm_params``).  The Klaviyo docs mis-document this; the real
# response shape comes straight from the 2024-10-15 cassette.
CAMPAIGN_TRACKING_OPTIONS_STRUCT = StructType(
    [
        StructField("add_tracking_params", BooleanType(), True),
        StructField(
            "custom_tracking_params",
            ArrayType(CUSTOM_TRACKING_PARAM_STRUCT),
            True,
        ),
        StructField("is_tracking_clicks", BooleanType(), True),
        StructField("is_tracking_opens", BooleanType(), True),
    ]
)

# campaigns.send_strategy.options_static
CAMPAIGN_SEND_STRATEGY_OPTIONS_STATIC_STRUCT = StructType(
    [
        StructField("datetime", StringType(), True),
        StructField("is_local", BooleanType(), True),
        StructField("send_past_recipients_immediately", BooleanType(), True),
    ]
)

# campaigns.send_strategy.options_throttled
CAMPAIGN_SEND_STRATEGY_OPTIONS_THROTTLED_STRUCT = StructType(
    [
        StructField("datetime", StringType(), True),
        StructField("throttle_percentage", LongType(), True),
    ]
)

# campaigns.send_strategy.options_sto (send-time optimization)
CAMPAIGN_SEND_STRATEGY_OPTIONS_STO_STRUCT = StructType(
    [
        StructField("date", StringType(), True),
    ]
)

# campaigns.send_strategy
#
# Verified live: ``send_strategy`` is a discriminated container, not a
# flat ``{method, datetime}`` pair.  ``method`` selects one of
# ``options_static`` / ``options_throttled`` / ``options_sto``; the
# inactive options are present but set to null.  This matches the
# 2024-10-15 response shape exactly.
CAMPAIGN_SEND_STRATEGY_STRUCT = StructType(
    [
        StructField("method", StringType(), True),
        StructField(
            "options_static",
            CAMPAIGN_SEND_STRATEGY_OPTIONS_STATIC_STRUCT,
            True,
        ),
        StructField(
            "options_throttled",
            CAMPAIGN_SEND_STRATEGY_OPTIONS_THROTTLED_STRUCT,
            True,
        ),
        StructField(
            "options_sto",
            CAMPAIGN_SEND_STRATEGY_OPTIONS_STO_STRUCT,
            True,
        ),
    ]
)

# metrics.integration — sub-object identifying the source integration.
#
# Verified live against API revision 2024-10-15: the response includes
# ``object`` (constant ``"integration"``) and ``key`` (slug, e.g.
# ``"klaviyo"``) alongside ``id``, ``name``, ``category``.
METRIC_INTEGRATION_STRUCT = StructType(
    [
        StructField("object", StringType(), True),
        StructField("id", StringType(), True),
        StructField("key", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
    ]
)


# =============================================================================
# Table schemas
# =============================================================================
#
# Each table's record shape after the connector flattens the JSON:API
# envelope.  The connector hoists ``id`` and ``type`` from the outer
# wrapper and unwraps every key under ``attributes`` to the top level.
# Relationship IDs (where the api_doc documents them) are added as
# discrete columns.

TABLE_SCHEMAS: dict[str, StructType] = {
    # ----- profiles ----------------------------------------------------- #
    "profiles": StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("external_id", StringType(), True),
            # Verified live: ``anonymous_id`` is returned alongside
            # ``external_id`` on the 2024-10-15 revision.
            StructField("anonymous_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("organization", StringType(), True),
            StructField("locale", StringType(), True),
            StructField("title", StringType(), True),
            StructField("image", StringType(), True),
            StructField("created", StringType(), True),
            StructField("updated", StringType(), True),
            StructField("last_event_date", StringType(), True),
            StructField("location", PROFILE_LOCATION_STRUCT, True),
            # ``properties`` is an open-ended user-defined object — Map.
            StructField("properties", MapType(StringType(), StringType()), True),
        ]
    ),
    # ----- events ------------------------------------------------------- #
    "events": StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            # Unix epoch seconds — LongType to be safe.
            StructField("timestamp", LongType(), True),
            StructField("datetime", StringType(), True),
            StructField("uuid", StringType(), True),
            # event_properties is arbitrary per-event payload — Map.
            StructField(
                "event_properties",
                MapType(StringType(), StringType()),
                True,
            ),
            # Hoisted relationship FKs.
            StructField("profile_id", StringType(), True),
            StructField("metric_id", StringType(), True),
        ]
    ),
    # ----- lists -------------------------------------------------------- #
    "lists": StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("created", StringType(), True),
            StructField("updated", StringType(), True),
            StructField("opt_in_process", StringType(), True),
        ]
    ),
    # ----- campaigns ---------------------------------------------------- #
    "campaigns": StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            # Channel is hoisted from the queried filter so callers can
            # join / partition by it without re-parsing ``messages``.
            StructField("channel", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("archived", BooleanType(), True),
            StructField("audiences", CAMPAIGN_AUDIENCES_STRUCT, True),
            StructField("send_options", CAMPAIGN_SEND_OPTIONS_STRUCT, True),
            StructField(
                "tracking_options",
                CAMPAIGN_TRACKING_OPTIONS_STRUCT,
                True,
            ),
            StructField("send_strategy", CAMPAIGN_SEND_STRATEGY_STRUCT, True),
            StructField("created_at", StringType(), True),
            StructField("scheduled_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("send_time", StringType(), True),
        ]
    ),
    # ----- metrics ------------------------------------------------------ #
    "metrics": StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("created", StringType(), True),
            StructField("updated", StringType(), True),
            StructField("integration", METRIC_INTEGRATION_STRUCT, True),
        ]
    ),
    # ----- flows -------------------------------------------------------- #
    "flows": StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("archived", BooleanType(), True),
            StructField("created", StringType(), True),
            StructField("updated", StringType(), True),
            StructField("trigger_type", StringType(), True),
        ]
    ),
    # ----- segments ----------------------------------------------------- #
    "segments": StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
            # ``definition`` is a nested condition tree of arbitrary
            # shape — open-ended Map.
            StructField("definition", MapType(StringType(), StringType()), True),
            StructField("created", StringType(), True),
            StructField("updated", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("is_processing", BooleanType(), True),
            StructField("is_starred", BooleanType(), True),
        ]
    ),
    # ----- templates ---------------------------------------------------- #
    "templates": StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("editor_type", StringType(), True),
            StructField("html", StringType(), True),
            StructField("text", StringType(), True),
            StructField("amp", StringType(), True),
            StructField("created", StringType(), True),
            StructField("updated", StringType(), True),
        ]
    ),
}


# =============================================================================
# Table metadata — ingestion type, PK, cursor
# =============================================================================
#
# Per api_doc:
#   - profiles   cdc       cursor=updated
#   - events     append    cursor=datetime
#   - lists      cdc       cursor=updated  (greater-than-only filter)
#   - campaigns  cdc       cursor=updated_at  (channel fan-out)
#   - metrics    snapshot  (no time filter available)
#   - flows      cdc       cursor=updated
#   - segments   cdc       cursor=updated  (greater-than-only filter)
#   - templates  cdc       cursor=updated

TABLE_METADATA: dict[str, dict] = {
    "profiles": {
        "primary_keys": ["id"],
        "cursor_field": "updated",
        "ingestion_type": "cdc",
    },
    "events": {
        "primary_keys": ["id"],
        "cursor_field": "datetime",
        "ingestion_type": "append",
    },
    "lists": {
        "primary_keys": ["id"],
        "cursor_field": "updated",
        "ingestion_type": "cdc",
    },
    "campaigns": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "metrics": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "flows": {
        "primary_keys": ["id"],
        "cursor_field": "updated",
        "ingestion_type": "cdc",
    },
    "segments": {
        "primary_keys": ["id"],
        "cursor_field": "updated",
        "ingestion_type": "cdc",
    },
    "templates": {
        "primary_keys": ["id"],
        "cursor_field": "updated",
        "ingestion_type": "cdc",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())


# =============================================================================
# Per-table API hints
# =============================================================================
#
# Centralises the per-table API quirks the connector consults at runtime.
# Kept in the schemas module so all the source-shape knowledge lives in
# one place.

# Maximum page size the Klaviyo API allows per endpoint.
#
# A value of 0 means the endpoint rejects the ``page[size]`` parameter
# at runtime — the connector omits it for those tables and lets the
# server pick its own page size.  Verified live against the 2024-10-15
# API revision: both ``metrics`` and ``campaigns`` return HTTP 400
# ``'page_size' is not a valid field for the resource 'metric'/'campaign'``
# when ``page[size]`` is supplied.  This contradicts the older
# documentation entries for those endpoints but matches reality.  Both
# still support cursor pagination via ``page[cursor]``.
MAX_PAGE_SIZE: dict[str, int] = {
    "profiles": 100,
    "events": 1000,
    "lists": 10,
    "campaigns": 0,
    "metrics": 0,
    "flows": 50,
    "segments": 10,
    "templates": 10,
}

# Tables whose ``updated``/``updated_at`` filter only supports
# ``greater-than`` (no ``greater-or-equal``).  We subtract a small
# lookback from the cursor before issuing the next call to avoid
# missing records whose timestamp equals the cursor value.
GREATER_THAN_ONLY_TABLES: set[str] = {"lists", "segments"}

# Tables whose ``updated``/``updated_at`` filter supports
# ``greater-or-equal``.  Strict client-side ``>`` is applied after the
# server-side filter to make the boundary exclusive (avoid re-emitting
# the boundary record on every microbatch).
GTE_FILTER_TABLES: set[str] = {
    "profiles",
    "events",
    "campaigns",
    "flows",
    "templates",
}

# Default lookback (seconds) for the ``greater-than``-only tables.
DEFAULT_LOOKBACK_SECONDS: dict[str, int] = {
    "lists": 1,
    "segments": 1,
}

# Channel values the connector fans-out over for ``campaigns``.  The
# Klaviyo API requires a ``messages.channel`` filter on every campaigns
# request, so the connector issues one request per channel and unions
# the results.
#
# API revision 2024-10-15 only accepts ``email`` and ``sms`` for this
# filter — verified live: ``mobile_push`` returns HTTP 400 ``'channel'
# must be one of: email, sms (got mobile_push)``.  The Klaviyo docs
# advertise ``mobile_push`` as a channel value but the campaigns filter
# does not accept it on this revision.  If a future revision adds
# support, expand this tuple and re-record the cassette.
CAMPAIGN_CHANNELS: tuple[str, ...] = ("email", "sms")

# Per-table cursor field name as it appears in the response
# ``attributes`` (i.e. after flattening).  ``campaigns`` uses
# ``updated_at`` with the ``_at`` suffix; every other table uses
# ``updated`` / ``datetime``.
CURSOR_FIELD: dict[str, str] = {
    "profiles": "updated",
    "events": "datetime",
    "lists": "updated",
    "campaigns": "updated_at",
    "flows": "updated",
    "segments": "updated",
    "templates": "updated",
}

# Per-table sort param value (matches the cursor field).
SORT_FIELD: dict[str, str] = {
    "profiles": "updated",
    "events": "datetime",
    "lists": "updated",
    "campaigns": "updated_at",
    "flows": "updated",
    "segments": "updated",
    "templates": "updated",
}
