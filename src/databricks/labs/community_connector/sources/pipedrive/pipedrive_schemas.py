"""Static schemas, metadata and per-table read configuration for Pipedrive.

Pipedrive has no schema-discovery API for its entity tables, so every schema
here is static and taken from the official v1 / v2 OpenAPI specs.  See
``pipedrive_api_doc.md``.

Conventions:

* Timestamps and dates are kept as ``StringType`` (ISO / ``YYYY-MM-DD HH:MM:SS``
  strings).  Pipedrive mixes several formats across API generations, and string
  columns never fail to parse; they can be cast downstream.
* Free-form / company-specific shapes (``custom_fields``, field ``options``,
  ``subfields``, deal-flow ``old_value`` / ``new_value`` / ``additional_data``)
  are stored as JSON strings.  The connector serialises them before returning
  records (see ``pipedrive_utils.normalize_record``).
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


def _s(name: str) -> StructField:
    return StructField(name, StringType(), True)


def _l(name: str) -> StructField:
    return StructField(name, LongType(), True)


def _d(name: str) -> StructField:
    return StructField(name, DoubleType(), True)


def _b(name: str) -> StructField:
    return StructField(name, BooleanType(), True)


def _arr(name: str, element) -> StructField:
    return StructField(name, ArrayType(element, True), True)


# --------------------------------------------------------------------------- #
# Shared nested shapes
# --------------------------------------------------------------------------- #

ADDRESS_STRUCT = StructType(
    [
        _s("value"),
        _s("country"),
        _s("admin_area_level_1"),
        _s("admin_area_level_2"),
        _s("locality"),
        _s("sublocality"),
        _s("route"),
        _s("street_number"),
        _s("subpremise"),
        _s("postal_code"),
    ]
)

CONTACT_ENTRY_STRUCT = StructType([_s("value"), _b("primary"), _s("label")])


# --------------------------------------------------------------------------- #
# Entity schemas
# --------------------------------------------------------------------------- #

DEAL_SCHEMA = StructType(
    [
        _l("id"),
        _s("title"),
        _l("creator_user_id"),
        _l("owner_id"),
        _l("person_id"),
        _l("org_id"),
        _l("pipeline_id"),
        _l("stage_id"),
        _d("value"),
        _s("currency"),
        _s("add_time"),
        _s("update_time"),
        _s("stage_change_time"),
        _b("is_deleted"),
        _b("is_archived"),
        _s("archive_time"),
        _s("status"),
        _d("probability"),
        _s("lost_reason"),
        _l("visible_to"),
        _s("close_time"),
        _s("won_time"),
        _s("lost_time"),
        _s("local_won_date"),
        _s("local_lost_date"),
        _s("local_close_date"),
        _s("expected_close_date"),
        _arr("label_ids", LongType()),
        _s("origin"),
        _s("origin_id"),
        _l("channel"),
        _s("channel_id"),
        _s("source_lead_id"),
        _d("arr"),
        _d("mrr"),
        _d("acv"),
        _s("custom_fields"),
    ]
)

PERSON_SCHEMA = StructType(
    [
        _l("id"),
        _s("name"),
        _s("first_name"),
        _s("last_name"),
        _l("owner_id"),
        _l("org_id"),
        _s("add_time"),
        _s("update_time"),
        _arr("emails", CONTACT_ENTRY_STRUCT),
        _arr("phones", CONTACT_ENTRY_STRUCT),
        _b("is_deleted"),
        _l("visible_to"),
        _arr("label_ids", LongType()),
        _l("picture_id"),
        StructField("postal_address", ADDRESS_STRUCT, True),
        _s("notes"),
        # The spec schema calls this ``im``; the migration guide says ``ims``.
        # The connector maps ``ims`` onto ``im``.
        _arr("im", CONTACT_ENTRY_STRUCT),
        _s("birthday"),
        _s("job_title"),
        _s("custom_fields"),
    ]
)

ORGANIZATION_SCHEMA = StructType(
    [
        _l("id"),
        _s("name"),
        _l("owner_id"),
        _s("add_time"),
        _s("update_time"),
        _b("is_deleted"),
        _l("visible_to"),
        StructField("address", ADDRESS_STRUCT, True),
        _arr("label_ids", LongType()),
        _s("website"),
        _s("linkedin"),
        _l("industry"),
        _l("annual_revenue"),
        _l("employee_count"),
        _s("custom_fields"),
    ]
)

ACTIVITY_SCHEMA = StructType(
    [
        _l("id"),
        _s("subject"),
        _s("type"),
        _l("owner_id"),
        _l("creator_user_id"),
        _b("is_deleted"),
        _s("add_time"),
        _s("update_time"),
        _l("deal_id"),
        _s("lead_id"),
        _l("person_id"),
        _l("org_id"),
        _l("project_id"),
        _s("due_date"),
        _s("due_time"),
        _s("duration"),
        _b("busy"),
        _b("done"),
        _s("marked_as_done_time"),
        StructField("location", ADDRESS_STRUCT, True),
        _arr("participants", StructType([_l("person_id"), _b("primary")])),
        _arr(
            "attendees",
            StructType(
                [
                    _s("email"),
                    _s("name"),
                    _s("status"),
                    _b("is_organizer"),
                    _l("person_id"),
                    _l("user_id"),
                ]
            ),
        ),
        _s("conference_meeting_client"),
        _s("conference_meeting_url"),
        _s("conference_meeting_id"),
        _s("public_description"),
        _l("priority"),
        _l("outcome"),
        _s("note"),
    ]
)

PRODUCT_SCHEMA = StructType(
    [
        _l("id"),
        _s("name"),
        _s("code"),
        _s("unit"),
        _d("tax"),
        _b("is_deleted"),
        _b("is_linkable"),
        _l("visible_to"),
        _l("owner_id"),
        _s("add_time"),
        _s("update_time"),
        _s("description"),
        _s("category"),
        _s("billing_frequency"),
        _l("billing_frequency_cycles"),
        _arr(
            "prices",
            StructType(
                [
                    _d("price"),
                    _d("cost"),
                    _l("product_id"),
                    _s("currency"),
                    _d("direct_cost"),
                    _s("notes"),
                ]
            ),
        ),
        _s("custom_fields"),
    ]
)

PIPELINE_SCHEMA = StructType(
    [
        _l("id"),
        _s("name"),
        _l("order_nr"),
        _b("is_deleted"),
        _b("is_deal_probability_enabled"),
        _s("add_time"),
        _s("update_time"),
    ]
)

STAGE_SCHEMA = StructType(
    [
        _l("id"),
        _l("order_nr"),
        _s("name"),
        _b("is_deleted"),
        _l("deal_probability"),
        _l("pipeline_id"),
        _b("is_deal_rot_enabled"),
        _l("days_to_rotten"),
        _s("add_time"),
        _s("update_time"),
    ]
)

USER_SCHEMA = StructType(
    [
        _l("id"),
        _s("name"),
        _s("default_currency"),
        _s("locale"),
        _l("lang"),
        _s("email"),
        _s("phone"),
        _b("activated"),
        _s("last_login"),
        _s("created"),
        _s("modified"),
        _b("has_created_company"),
        _arr("access", StructType([_s("app"), _b("admin"), _s("permission_set_id")])),
        _b("active_flag"),
        _s("timezone_name"),
        _s("timezone_offset"),
        _l("role_id"),
        _s("icon_url"),
        _b("is_you"),
        _b("is_deleted"),
        _l("is_admin"),
        _s("signup_flow_variation"),
    ]
)

NOTE_SCHEMA = StructType(
    [
        _l("id"),
        _l("user_id"),
        _l("deal_id"),
        _l("person_id"),
        _l("org_id"),
        _s("lead_id"),
        _l("project_id"),
        _l("task_id"),
        _s("content"),
        _s("add_time"),
        _s("update_time"),
        _b("active_flag"),
        _b("pinned_to_deal_flag"),
        _b("pinned_to_person_flag"),
        _b("pinned_to_organization_flag"),
        _b("pinned_to_lead_flag"),
        _b("pinned_to_project_flag"),
        _b("pinned_to_task_flag"),
        _l("last_update_user_id"),
        StructField("organization", StructType([_s("name")]), True),
        StructField("person", StructType([_s("name")]), True),
        StructField("deal", StructType([_s("title")]), True),
        _s("lead"),
        StructField("project", StructType([_s("title")]), True),
        StructField("task", StructType([_s("title")]), True),
        StructField(
            "user",
            StructType([_s("email"), _s("name"), _s("icon_url"), _b("is_you")]),
            True,
        ),
    ]
)

LEAD_SCHEMA = StructType(
    [
        _s("id"),
        _s("title"),
        _l("owner_id"),
        _l("creator_id"),
        _arr("label_ids", StringType()),
        _l("person_id"),
        _l("organization_id"),
        _s("source_name"),
        _s("origin"),
        _s("origin_id"),
        _l("channel"),
        _s("channel_id"),
        _l("source_deal_id"),
        _b("is_archived"),
        _s("archive_reason"),
        _s("archive_time"),
        _b("was_seen"),
        StructField("value", StructType([_d("amount"), _s("currency")]), True),
        _s("expected_close_date"),
        _l("next_activity_id"),
        _s("add_time"),
        _s("update_time"),
        _s("visible_to"),
        _s("cc_email"),
        # Root-level 40-hex deal custom-field keys, gathered into one JSON string.
        _s("custom_fields"),
    ]
)

LEAD_LABEL_SCHEMA = StructType(
    [_s("id"), _s("name"), _s("color"), _s("add_time"), _s("update_time")]
)

FILE_SCHEMA = StructType(
    [
        _l("id"),
        _l("user_id"),
        _l("deal_id"),
        _l("person_id"),
        _l("org_id"),
        _l("product_id"),
        _l("activity_id"),
        _s("lead_id"),
        _l("project_id"),
        _l("log_id"),
        _s("add_time"),
        _s("update_time"),
        _s("file_name"),
        _s("file_type"),
        _l("file_size"),
        _b("active_flag"),
        _b("inline_flag"),
        _s("remote_location"),
        _s("remote_id"),
        _s("cid"),
        _s("s3_bucket"),
        _s("mail_message_id"),
        _s("mail_template_id"),
        _s("deal_name"),
        _s("person_name"),
        _s("people_name"),
        _s("org_name"),
        _s("product_name"),
        _s("lead_name"),
        _s("project_name"),
        _s("url"),
        _s("name"),
        _s("description"),
    ]
)

PROJECT_SCHEMA = StructType(
    [
        _l("id"),
        _s("title"),
        _s("description"),
        _s("status"),
        _l("board_id"),
        _l("phase_id"),
        _l("owner_id"),
        _s("start_date"),
        _s("end_date"),
        _arr("deal_ids", LongType()),
        _arr("person_ids", LongType()),
        _arr("org_ids", LongType()),
        _arr("label_ids", LongType()),
        _l("health_status"),
        _s("add_time"),
        _s("update_time"),
        _s("status_change_time"),
        _s("archive_time"),
        _s("custom_fields"),
    ]
)

TASK_SCHEMA = StructType(
    [
        _l("id"),
        _s("title"),
        _l("creator_id"),
        _s("description"),
        _l("project_id"),
        _b("is_done"),
        _b("is_milestone"),
        _s("due_date"),
        _s("start_date"),
        _l("parent_task_id"),
        _arr("assignee_ids", LongType()),
        _l("priority"),
        _s("add_time"),
        _s("update_time"),
        _s("marked_as_done_time"),
    ]
)

ACTIVITY_TYPE_SCHEMA = StructType(
    [
        _l("id"),
        _s("name"),
        _s("icon_key"),
        _s("color"),
        _l("order_nr"),
        _s("key_string"),
        _b("active_flag"),
        _b("is_custom_flag"),
        _s("add_time"),
        _s("update_time"),
    ]
)

CURRENCY_SCHEMA = StructType(
    [
        _l("id"),
        _s("code"),
        _s("name"),
        _l("decimal_points"),
        _s("symbol"),
        _b("active_flag"),
        _b("is_custom_flag"),
    ]
)

FIELD_SCHEMA = StructType(
    [
        _s("key"),
        _l("id"),
        _s("name"),
        _l("order_nr"),
        _s("field_type"),
        _s("add_time"),
        _s("update_time"),
        _l("last_updated_by_user_id"),
        _l("created_by_user_id"),
        _b("active_flag"),
        _b("edit_flag"),
        _b("index_visible_flag"),
        _b("details_visible_flag"),
        _b("add_visible_flag"),
        _b("important_flag"),
        _b("bulk_edit_allowed"),
        _b("searchable_flag"),
        _b("filtering_allowed"),
        _b("sortable_flag"),
        # Usually a bool, occasionally an object: serialised to a JSON string.
        _s("mandatory_flag"),
        # Free-form option / subfield objects, each serialised to a JSON string.
        _arr("options", StringType()),
        _arr("options_deleted", StringType()),
        _b("is_subfield"),
        _arr("subfields", StringType()),
    ]
)

DEAL_PRODUCT_SCHEMA = StructType(
    [
        _l("id"),
        _l("deal_id"),
        _l("product_id"),
        _l("product_variation_id"),
        _s("name"),
        _d("sum"),
        _d("tax"),
        _l("order_nr"),
        _s("add_time"),
        _s("update_time"),
        _s("comments"),
        _s("currency"),
        _d("discount"),
        _s("discount_type"),
        _d("quantity"),
        _d("item_price"),
        _s("tax_method"),
        _b("is_enabled"),
        _s("billing_frequency"),
        _l("billing_frequency_cycles"),
        _s("billing_start_date"),
    ]
)

DEAL_FLOW_SCHEMA = StructType(
    [
        _l("id"),
        _l("item_id"),
        _l("user_id"),
        _s("field_key"),
        _s("old_value"),
        _s("new_value"),
        _b("is_bulk_update_flag"),
        _s("log_time"),
        _s("change_source"),
        _s("change_source_user_agent"),
        _s("origin"),
        _s("origin_id"),
        _s("additional_data"),
    ]
)


# --------------------------------------------------------------------------- #
# Per-table configuration
# --------------------------------------------------------------------------- #
#
# kind:
#   v2_incremental  cursor-paginated v2 list, server-side updated_since + sort
#   v1_incremental  offset-paginated v1 list, server-side updated_since, id sort
#   v2_snapshot     cursor-paginated v2 list, full read
#   v1_snapshot     offset-paginated (or unpaginated) v1 list, full read
#   projects        union of /projects and /projects/archived (v2 snapshot)
#   deal_products   child of deals, batched by deal_ids (v2 snapshot)
#   deal_flow       child of deals, one v1 call per changed deal (cdc on id)

V2 = "/api/v2"
V1 = "/v1"

TABLE_CONFIG: dict[str, dict] = {
    "deals": {
        "kind": "v2_incremental",
        "path": f"{V2}/deals",
        "schema": DEAL_SCHEMA,
        "ingestion_type": "cdc_with_deletes",
        "supports_until": True,
        "deal_status": True,
        "default_include_fields": "source_lead_id",
    },
    "deals_archived": {
        "kind": "v2_incremental",
        "path": f"{V2}/deals/archived",
        "schema": DEAL_SCHEMA,
        "ingestion_type": "cdc_with_deletes",
        "supports_until": True,
        "deal_status": True,
        "default_include_fields": "source_lead_id",
    },
    "persons": {
        "kind": "v2_incremental",
        "path": f"{V2}/persons",
        "schema": PERSON_SCHEMA,
        "ingestion_type": "cdc",
        "supports_until": True,
    },
    "organizations": {
        "kind": "v2_incremental",
        "path": f"{V2}/organizations",
        "schema": ORGANIZATION_SCHEMA,
        "ingestion_type": "cdc",
        "supports_until": True,
    },
    "activities": {
        "kind": "v2_incremental",
        "path": f"{V2}/activities",
        "schema": ACTIVITY_SCHEMA,
        "ingestion_type": "cdc",
        "supports_until": True,
    },
    "products": {
        "kind": "v2_incremental",
        "path": f"{V2}/products",
        "schema": PRODUCT_SCHEMA,
        "ingestion_type": "cdc",
        # /api/v2/products documents updated_since but not updated_until:
        # the init-time cap is enforced client-side.
        "supports_until": False,
    },
    "pipelines": {
        "kind": "v2_snapshot",
        "path": f"{V2}/pipelines",
        "schema": PIPELINE_SCHEMA,
    },
    "stages": {
        "kind": "v2_snapshot",
        "path": f"{V2}/stages",
        "schema": STAGE_SCHEMA,
    },
    "users": {
        "kind": "v1_snapshot",
        "path": f"{V1}/users",
        "schema": USER_SCHEMA,
        "paginated": False,
    },
    "notes": {
        "kind": "v1_incremental",
        "path": f"{V1}/notes",
        "schema": NOTE_SCHEMA,
        "ingestion_type": "cdc",
        "page_max": 500,
    },
    "leads": {
        "kind": "v1_incremental",
        "path": f"{V1}/leads",
        "schema": LEAD_SCHEMA,
        # Archiving a lead moves it to leads_archived; those archive rows are
        # the delete stream (see pipedrive.py read_table_deletes).
        "ingestion_type": "cdc_with_deletes",
        "page_max": 500,
        "lead_custom_fields": True,
    },
    "leads_archived": {
        "kind": "v1_snapshot",
        "path": f"{V1}/leads/archived",
        "schema": LEAD_SCHEMA,
        "paginated": True,
        "page_max": 500,
        "sort": "add_time ASC, id ASC",
        "lead_custom_fields": True,
    },
    "lead_labels": {
        "kind": "v1_snapshot",
        "path": f"{V1}/leadLabels",
        "schema": LEAD_LABEL_SCHEMA,
        "paginated": False,
    },
    "files": {
        "kind": "v1_snapshot",
        "path": f"{V1}/files",
        "schema": FILE_SCHEMA,
        "paginated": True,
        "page_max": 100,
        "sort": "id ASC",
    },
    "projects": {
        "kind": "projects",
        "paths": [f"{V2}/projects", f"{V2}/projects/archived"],
        "schema": PROJECT_SCHEMA,
        "optional": True,
    },
    "tasks": {
        "kind": "v2_snapshot",
        "path": f"{V2}/tasks",
        "schema": TASK_SCHEMA,
        "optional": True,
    },
    "activity_types": {
        "kind": "v1_snapshot",
        "path": f"{V1}/activityTypes",
        "schema": ACTIVITY_TYPE_SCHEMA,
        "paginated": False,
    },
    "currencies": {
        "kind": "v1_snapshot",
        "path": f"{V1}/currencies",
        "schema": CURRENCY_SCHEMA,
        "paginated": False,
    },
    "deal_fields": {
        "kind": "v1_snapshot",
        "path": f"{V1}/dealFields",
        "schema": FIELD_SCHEMA,
        "paginated": True,
        "page_max": 500,
        "primary_keys": ["key"],
    },
    "person_fields": {
        "kind": "v1_snapshot",
        "path": f"{V1}/personFields",
        "schema": FIELD_SCHEMA,
        "paginated": True,
        "page_max": 500,
        "primary_keys": ["key"],
    },
    "organization_fields": {
        "kind": "v1_snapshot",
        "path": f"{V1}/organizationFields",
        "schema": FIELD_SCHEMA,
        "paginated": True,
        "page_max": 500,
        "primary_keys": ["key"],
    },
    "product_fields": {
        "kind": "v1_snapshot",
        "path": f"{V1}/productFields",
        "schema": FIELD_SCHEMA,
        "paginated": True,
        "page_max": 500,
        "primary_keys": ["key"],
    },
    "activity_fields": {
        "kind": "v1_snapshot",
        "path": f"{V1}/activityFields",
        "schema": FIELD_SCHEMA,
        "paginated": True,
        "page_max": 500,
        "primary_keys": ["key"],
    },
    "deal_products": {
        "kind": "deal_products",
        "path": f"{V2}/deals/products",
        "schema": DEAL_PRODUCT_SCHEMA,
    },
    "deal_flow": {
        "kind": "deal_flow",
        "path": V1 + "/deals/{id}/flow",
        "schema": DEAL_FLOW_SCHEMA,
        # Change-log rows are immutable and keyed on ``id``.  The parent walk
        # can legitimately visit a deal twice in one window (edited / archived
        # mid-walk), so re-emitted rows must upsert, not append.
        "ingestion_type": "cdc",
        "cursor_field": "log_time",
    },
}

SUPPORTED_TABLES: list[str] = list(TABLE_CONFIG)

TABLE_SCHEMAS: dict[str, StructType] = {
    name: cfg["schema"] for name, cfg in TABLE_CONFIG.items()
}

# Parent tables that deal_products / deal_flow walk to collect deal IDs.
DEAL_PARENT_PATHS: list[str] = [f"{V2}/deals", f"{V2}/deals/archived"]

# deal_flow walks these in order.  The final /deals pass re-scans only deals
# updated since the archived phase began, catching deals unarchived mid-walk
# (they leave /deals/archived and re-enter the already-walked /deals list).
DEAL_FLOW_PHASE_PATHS: list[str] = [f"{V2}/deals", f"{V2}/deals/archived", f"{V2}/deals"]

# JSON-serialised columns per table (values are re-encoded with json.dumps).
JSON_STRING_FIELDS: dict[str, tuple[str, ...]] = {
    "deals": ("custom_fields",),
    "deals_archived": ("custom_fields",),
    "persons": ("custom_fields",),
    "organizations": ("custom_fields",),
    "products": ("custom_fields",),
    "projects": ("custom_fields",),
    "notes": ("lead",),
    "leads": ("custom_fields",),
    "leads_archived": ("custom_fields",),
    "deal_flow": ("old_value", "new_value", "additional_data", "origin_id"),
}

JSON_STRING_ARRAY_FIELDS: tuple[str, ...] = ("options", "options_deleted", "subfields")

FIELD_TABLES: frozenset[str] = frozenset(
    name for name, cfg in TABLE_CONFIG.items() if cfg["schema"] is FIELD_SCHEMA
)


def build_metadata(table_name: str) -> dict:
    """Return the static ``read_table_metadata`` dict for a table."""
    cfg = TABLE_CONFIG[table_name]
    ingestion_type = cfg.get("ingestion_type", "snapshot")
    metadata: dict = {
        "primary_keys": list(cfg.get("primary_keys", ["id"])),
        "ingestion_type": ingestion_type,
    }
    if ingestion_type != "snapshot":
        metadata["cursor_field"] = cfg.get("cursor_field", "update_time")
    return metadata
