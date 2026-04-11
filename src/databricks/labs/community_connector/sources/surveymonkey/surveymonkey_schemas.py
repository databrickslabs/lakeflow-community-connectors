# SurveyMonkey Schemas and Table Metadata
# Defines all Spark schemas, table metadata, and supported tables for the SurveyMonkey connector.

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    ArrayType,
    MapType,
    DoubleType,
)

# ─── Shared Structs ──────────────────────────────────────────────────────────

BUTTONS_TEXT_STRUCT = StructType(
    [
        StructField("next_button", StringType(), True),
        StructField("prev_button", StringType(), True),
        StructField("done_button", StringType(), True),
        StructField("exit_button", StringType(), True),
    ]
)

CONTACT_STRUCT = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
    ]
)

METADATA_STRUCT = StructType(
    [
        StructField("contact", CONTACT_STRUCT, True),
    ]
)

CHOICE_METADATA_STRUCT = StructType(
    [
        StructField("weight", LongType(), True),
    ]
)

ANSWER_STRUCT = StructType(
    [
        StructField("choice_id", StringType(), True),
        StructField("choice_metadata", CHOICE_METADATA_STRUCT, True),
        StructField("row_id", StringType(), True),
        StructField("col_id", StringType(), True),
        StructField("other_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("download_url", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("tag_data", ArrayType(StringType()), True),
    ]
)

RESPONSE_QUESTION_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("variable_id", StringType(), True),
        StructField("answers", ArrayType(ANSWER_STRUCT), True),
    ]
)

RESPONSE_PAGE_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("questions", ArrayType(RESPONSE_QUESTION_STRUCT), True),
    ]
)

LOGIC_PATH_STRUCT = StructType(
    [
        StructField("path", ArrayType(StringType()), True),
    ]
)

CHOICE_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("position", LongType(), True),
        StructField("text", StringType(), True),
        StructField("visible", BooleanType(), True),
        StructField("is_na", BooleanType(), True),
        StructField("weight", LongType(), True),
    ]
)

OTHER_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("visible", BooleanType(), True),
        StructField("is_answer_choice", BooleanType(), True),
        StructField("position", LongType(), True),
    ]
)

QUESTION_ANSWERS_STRUCT = StructType(
    [
        StructField("choices", ArrayType(CHOICE_STRUCT), True),
        StructField("rows", ArrayType(CHOICE_STRUCT), True),
        StructField("cols", ArrayType(CHOICE_STRUCT), True),
        StructField("other", OTHER_STRUCT, True),
    ]
)

REQUIRED_STRUCT = StructType(
    [
        StructField("text", StringType(), True),
        StructField("type", StringType(), True),
        StructField("amount", StringType(), True),
    ]
)

SORTING_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("ignore_last", BooleanType(), True),
    ]
)

VALIDATION_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("text", StringType(), True),
        StructField("min", StringType(), True),
        StructField("max", StringType(), True),
        StructField("sum", LongType(), True),
        StructField("sum_text", StringType(), True),
    ]
)

THANK_YOU_PAGE_STRUCT = StructType(
    [
        StructField("is_enabled", BooleanType(), True),
        StructField("message", StringType(), True),
    ]
)

WORKGROUP_METADATA_STRUCT = StructType(
    [
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

DEFAULT_ROLE_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

WORKGROUP_MEMBER_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
    ]
)

ROLLUP_SUMMARY_STRUCT = StructType(
    [
        StructField("text", StringType(), True),
        StructField("choice_id", StringType(), True),
        StructField("count", LongType(), True),
        StructField("percentage", DoubleType(), True),
        StructField("other_text", ArrayType(StringType()), True),
        StructField("min", DoubleType(), True),
        StructField("max", DoubleType(), True),
        StructField("mean", DoubleType(), True),
        StructField("median", DoubleType(), True),
    ]
)

BENCHMARK_DETAILS_STRUCT = StructType(
    [
        StructField("industry", StringType(), True),
        StructField("sample_size", LongType(), True),
    ]
)

QUESTION_TYPES_STRUCT = StructType(
    [
        StructField("available", ArrayType(StringType()), True),
    ]
)

SCOPES_STRUCT = StructType(
    [
        StructField("available", ArrayType(StringType()), True),
        StructField("granted", ArrayType(StringType()), True),
    ]
)

FEATURES_STRUCT = StructType(
    [
        StructField("collector_create_limit", LongType(), True),
        StructField("email_enabled", BooleanType(), True),
    ]
)

# ─── Table Schemas ────────────────────────────────────────────────────────────

TABLE_SCHEMAS = {
    "surveys": StructType(
        [
            StructField("id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("nickname", StringType(), True),
            StructField("href", StringType(), True),
            StructField("folder_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("language", StringType(), True),
            StructField("question_count", LongType(), True),
            StructField("page_count", LongType(), True),
            StructField("date_created", StringType(), True),
            StructField("date_modified", StringType(), True),
            StructField("response_count", LongType(), True),
            StructField("buttons_text", BUTTONS_TEXT_STRUCT, True),
            StructField("is_owner", BooleanType(), True),
            StructField("footer", BooleanType(), True),
            StructField("custom_variables", MapType(StringType(), StringType()), True),
            StructField("preview", StringType(), True),
            StructField("edit_url", StringType(), True),
            StructField("collect_url", StringType(), True),
            StructField("analyze_url", StringType(), True),
            StructField("summary_url", StringType(), True),
        ]
    ),
    "survey_responses": StructType(
        [
            StructField("id", StringType(), False),
            StructField("survey_id", StringType(), True),
            StructField("recipient_id", StringType(), True),
            StructField("collector_id", StringType(), True),
            StructField("custom_value", StringType(), True),
            StructField("edit_url", StringType(), True),
            StructField("analyze_url", StringType(), True),
            StructField("total_time", LongType(), True),
            StructField("date_created", StringType(), True),
            StructField("date_modified", StringType(), True),
            StructField("response_status", StringType(), True),
            StructField("collection_mode", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("logic_path", LOGIC_PATH_STRUCT, True),
            StructField("metadata", METADATA_STRUCT, True),
            StructField("page_path", ArrayType(StringType()), True),
            StructField("pages", ArrayType(RESPONSE_PAGE_STRUCT), True),
        ]
    ),
    "survey_pages": StructType(
        [
            StructField("id", StringType(), False),
            StructField("survey_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("position", LongType(), True),
            StructField("question_count", LongType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "survey_questions": StructType(
        [
            StructField("id", StringType(), False),
            StructField("survey_id", StringType(), True),
            StructField("page_id", StringType(), True),
            StructField("position", LongType(), True),
            StructField("visible", BooleanType(), True),
            StructField("family", StringType(), True),
            StructField("subtype", StringType(), True),
            StructField("heading", StringType(), True),
            StructField("href", StringType(), True),
            StructField("sorting", SORTING_STRUCT, True),
            StructField("required", REQUIRED_STRUCT, True),
            StructField("validation", VALIDATION_STRUCT, True),
            StructField("forced_ranking", BooleanType(), True),
            StructField("answers", QUESTION_ANSWERS_STRUCT, True),
        ]
    ),
    "collectors": StructType(
        [
            StructField("id", StringType(), False),
            StructField("survey_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("redirect_url", StringType(), True),
            StructField("thank_you_page", THANK_YOU_PAGE_STRUCT, True),
            StructField("response_count", LongType(), True),
            StructField("date_created", StringType(), True),
            StructField("date_modified", StringType(), True),
            StructField("url", StringType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "contact_lists": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "contacts": StructType(
        [
            StructField("id", StringType(), False),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("custom_fields", MapType(StringType(), StringType()), True),
            StructField("href", StringType(), True),
        ]
    ),
    "users": StructType(
        [
            StructField("id", StringType(), False),
            StructField("username", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("email_verified", BooleanType(), True),
            StructField("account_type", StringType(), True),
            StructField("language", StringType(), True),
            StructField("date_created", StringType(), True),
            StructField("date_last_login", StringType(), True),
            StructField("question_types", QUESTION_TYPES_STRUCT, True),
            StructField("scopes", SCOPES_STRUCT, True),
            StructField("sso_connections", ArrayType(StringType()), True),
            StructField("features", FEATURES_STRUCT, True),
            StructField("href", StringType(), True),
        ]
    ),
    "groups": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("member_count", LongType(), True),
            StructField("max_invites", LongType(), True),
            StructField("date_created", StringType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "group_members": StructType(
        [
            StructField("id", StringType(), False),
            StructField("group_id", StringType(), True),
            StructField("username", StringType(), True),
            StructField("email", StringType(), True),
            StructField("type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("date_created", StringType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "workgroups": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("is_visible", BooleanType(), True),
            StructField("metadata", WORKGROUP_METADATA_STRUCT, True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("members", ArrayType(WORKGROUP_MEMBER_STRUCT), True),
            StructField("members_count", LongType(), True),
            StructField("shares_count", LongType(), True),
            StructField("default_role", DEFAULT_ROLE_STRUCT, True),
            StructField("organization_id", StringType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "survey_folders": StructType(
        [
            StructField("id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("num_surveys", LongType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "survey_categories": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
        ]
    ),
    "survey_templates": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("available", BooleanType(), True),
            StructField("num_questions", LongType(), True),
            StructField("preview_link", StringType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "survey_languages": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("native_name", StringType(), True),
        ]
    ),
    "webhooks": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("object_type", StringType(), True),
            StructField("object_ids", ArrayType(StringType()), True),
            StructField("subscription_url", StringType(), True),
            StructField("authorization", StringType(), True),
            StructField("href", StringType(), True),
        ]
    ),
    "survey_rollups": StructType(
        [
            StructField("id", StringType(), False),
            StructField("survey_id", StringType(), True),
            StructField("family", StringType(), True),
            StructField("subtype", StringType(), True),
            StructField("href", StringType(), True),
            StructField("summary", ArrayType(MapType(StringType(), StringType())), True),
        ]
    ),
    "benchmark_bundles": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("title", StringType(), True),
            StructField("details", BENCHMARK_DETAILS_STRUCT, True),
            StructField("country_code", StringType(), True),
            StructField("href", StringType(), True),
        ]
    ),
}

# ─── Table Metadata ──────────────────────────────────────────────────────────

OBJECT_CONFIG = {
    "surveys": {
        "primary_keys": ["id"],
        "cursor_field": "date_modified",
        "ingestion_type": "cdc",
        "endpoint": "/surveys",
        "per_page": 1000,
    },
    "survey_responses": {
        "primary_keys": ["id"],
        "cursor_field": "date_modified",
        "ingestion_type": "cdc",
        "endpoint": "/surveys/{survey_id}/responses/bulk",
        "per_page": 100,
        "requires_survey_id": True,
    },
    "survey_pages": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/surveys/{survey_id}/pages",
        "per_page": 1000,
        "requires_survey_id": True,
    },
    "survey_questions": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/surveys/{survey_id}/pages/{page_id}/questions",
        "per_page": 1000,
        "requires_survey_id": True,
        "requires_page_id": True,
    },
    "collectors": {
        "primary_keys": ["id"],
        "cursor_field": "date_modified",
        "ingestion_type": "cdc",
        "endpoint": "/surveys/{survey_id}/collectors",
        "per_page": 1000,
        "requires_survey_id": True,
    },
    "contact_lists": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/contact_lists",
        "per_page": 1000,
    },
    "contacts": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/contacts",
        "per_page": 1000,
    },
    "users": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/users/me",
        "per_page": 1,
    },
    "groups": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/groups",
        "per_page": 1000,
    },
    "group_members": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/groups/{group_id}/members",
        "per_page": 1000,
        "requires_group_id": True,
    },
    "workgroups": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/workgroups",
        "per_page": 1000,
    },
    "survey_folders": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/survey_folders",
        "per_page": 1000,
    },
    "survey_categories": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/survey_categories",
        "per_page": 1000,
    },
    "survey_templates": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/survey_templates",
        "per_page": 1000,
    },
    "survey_languages": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/survey_languages",
        "per_page": 1000,
    },
    "webhooks": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/webhooks",
        "per_page": 1000,
    },
    "survey_rollups": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/surveys/{survey_id}/rollups",
        "per_page": 100,
        "requires_survey_id": True,
    },
    "benchmark_bundles": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "/benchmark_bundles",
        "per_page": 1000,
    },
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
