"""Static schemas, table metadata, and tuning constants for the Egnyte connector.

Schemas are derived from the Egnyte Public API documentation captured in
``egnyte_api_doc.md``.

Type conventions
----------------
* **Opaque identifiers** (``folder_id``, ``entry_id``, ``group_id``,
  ``parent_id``, link ``id``, group ``id``) are ``StringType``. The doc warns
  they are not necessarily UUID v4 — never parse or narrow them.
* **User ids** are large integers (e.g. ``9967960066``) → ``LongType``, never
  ``IntegerType``.
* **Epoch-millisecond** fields (``uploaded``, ``lastModified``) stay
  ``LongType``. The framework's ``TimestampType`` coercion calls
  ``datetime.fromtimestamp(value)``, which interprets the number as *seconds* —
  feeding it milliseconds would land in the year 56000. Keep them numeric and
  let downstream jobs divide by 1000.
* **ISO-8601 string** fields (``last_modified``, ``creation_date``, event
  ``timestamp``) are ``TimestampType``; the framework parses both the
  ``...Z`` and ``...+00:00`` spellings, with or without fractional seconds
  ("parse permissively" per the doc's field-type table).
* The one exception is audit-report ``time``, which stays ``StringType``.
  Audit rows carry no server-assigned id, so ``time`` is a *component of the
  natural key* (see ``TABLE_METADATA``) — a key column has to survive
  byte-for-byte rather than being re-rendered by a parser, and the doc flags
  report-row timestamp formats as UNVERIFIED. Cast downstream.
* Nested objects (user ``name``, event ``data``, group ``members``) stay
  ``StructType`` / ``ArrayType<StructType>`` — not flattened.
* Every row carries ``egnyte_domain`` so several tenants can land in one
  table (mirrors ``collibra_org`` / Shopify's ``shop``).
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

# =============================================================================
# Table names
# =============================================================================

TABLE_FILES = "files"
TABLE_FOLDERS = "folders"
TABLE_USERS = "users"
TABLE_GROUPS = "groups"
TABLE_LINKS = "links"
TABLE_EVENTS = "events"
TABLE_AUDIT_LOGINS = "audit_logins"
TABLE_AUDIT_FILES = "audit_files"
TABLE_AUDIT_PERMISSIONS = "audit_permissions"

# Audit report tables → the ``{type}`` path segment of
# ``POST /pubapi/v1/audit/{type}`` and ``GET /pubapi/v1/audit/{type}/{job}``.
AUDIT_TABLE_TYPES: dict[str, str] = {
    TABLE_AUDIT_LOGINS: "logins",
    TABLE_AUDIT_FILES: "files",
    TABLE_AUDIT_PERMISSIONS: "permissions",
}

# Tables read through SupportsPartitionedStream (server-side range queries).
PARTITIONED_TABLES: frozenset[str] = frozenset(
    {TABLE_LINKS, *AUDIT_TABLE_TYPES}
)


# =============================================================================
# Reusable nested structs
# =============================================================================

# SCIM-style user name sub-object.
USER_NAME_STRUCT = StructType(
    [
        StructField("givenName", StringType(), True),
        StructField("familyName", StringType(), True),
        StructField("formatted", StringType(), True),
    ]
)

# Group membership entry. ``value`` is the integer user id that joins to
# ``users.id`` (this is how the doc confirms the users PK).
GROUP_MEMBER_STRUCT = StructType(
    [
        StructField("username", StringType(), True),
        StructField("value", LongType(), True),
        StructField("display", StringType(), True),
    ]
)

# Events ``data`` payload. The shape varies by ``type``/``action`` (copy and
# move carry both source and target, create/delete only target), so every key
# is nullable and absent keys land as NULL rather than an empty struct.
EVENT_DATA_STRUCT = StructType(
    [
        StructField("target_path", StringType(), True),
        StructField("target_id", StringType(), True),
        StructField("target_group_id", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("source_group_id", StringType(), True),
        StructField("is_folder", BooleanType(), True),
    ]
)


# =============================================================================
# Table schemas
# =============================================================================

# One row per file, emitted from the ``files[]`` array of each folder listing
# during the recursive ``pubapi/v1/fs`` tree walk.
FILES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("path", StringType(), False),
        # SHA-512 hex digest of the current version.
        StructField("checksum", StringType(), True),
        StructField("size", LongType(), True),
        # entry_id identifies ONE VERSION and changes on every upload;
        # group_id is the stable identity of the file across versions.
        StructField("entry_id", StringType(), True),
        StructField("group_id", StringType(), False),
        StructField("parent_id", StringType(), True),
        StructField("is_folder", BooleanType(), True),
        StructField("locked", BooleanType(), True),
        # ISO-8601 string on the fs API.
        StructField("last_modified", TimestampType(), True),
        # Epoch milliseconds — see the module docstring.
        StructField("uploaded", LongType(), True),
        StructField("uploaded_by", StringType(), True),
        StructField("num_versions", LongType(), True),
        StructField("permission", StringType(), True),
        # Stamped by the connector: the folder this file was listed under.
        StructField("parent_path", StringType(), True),
        StructField("egnyte_domain", StringType(), False),
    ]
)

# One row per folder visited by the tree walk. The record is the folder's own
# ``GET /pubapi/v1/fs/{path}`` envelope with the ``folders``/``files`` child
# arrays removed, merged over the parent's child-entry stub (which is the only
# place ``lastModified`` appears).
FOLDERS_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("path", StringType(), False),
        StructField("folder_id", StringType(), False),
        StructField("parent_id", StringType(), True),
        StructField("is_folder", BooleanType(), True),
        StructField("permission", StringType(), True),
        StructField("folder_description", StringType(), True),
        StructField("public_links", StringType(), True),
        StructField("allow_links", BooleanType(), True),
        StructField("allow_upload_links", BooleanType(), True),
        StructField("restrict_move_delete", BooleanType(), True),
        # Epoch milliseconds, only present on child-entry stubs.
        StructField("lastModified", LongType(), True),
        StructField("parent_path", StringType(), True),
        StructField("egnyte_domain", StringType(), False),
    ]
)

# ``GET /pubapi/v2/users`` — SCIM-influenced.
#
# UNVERIFIED (per egnyte_api_doc.md): createdDate, lastModificationDate,
# lastActiveDate, isServiceAccount, language, idpUserId, userPrincipalName are
# documented by Egnyte but absent from the Python SDK's attribute list. They
# are declared here as nullable so they populate if present and stay NULL
# otherwise — confirm during live validation.
USERS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("userName", StringType(), True),
        StructField("email", StringType(), True),
        StructField("externalId", StringType(), True),
        StructField("name", USER_NAME_STRUCT, True),
        StructField("active", BooleanType(), True),
        StructField("locked", BooleanType(), True),
        StructField("authType", StringType(), True),
        StructField("userType", StringType(), True),
        StructField("role", StringType(), True),
        StructField("idpUserId", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("isServiceAccount", BooleanType(), True),
        StructField("language", StringType(), True),
        StructField("createdDate", TimestampType(), True),
        StructField("lastModificationDate", TimestampType(), True),
        StructField("lastActiveDate", TimestampType(), True),
        StructField("egnyte_domain", StringType(), False),
    ]
)

# ``GET /pubapi/v2/groups``. ``members`` is NOT returned by the list endpoint —
# it stays NULL unless the ``include_members`` table option is enabled, which
# triggers a per-group ``GET /pubapi/v2/groups/{id}`` fan-out.
GROUPS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("displayName", StringType(), True),
        StructField("members", ArrayType(GROUP_MEMBER_STRUCT), True),
        StructField("egnyte_domain", StringType(), False),
    ]
)

# ``GET /pubapi/v2/links`` — v2 returns full Link objects, avoiding the v1
# id-list + per-id fan-out.
LINKS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("url", StringType(), True),
        StructField("path", StringType(), True),
        StructField("type", StringType(), True),
        StructField("accessibility", StringType(), True),
        StructField("protection", StringType(), True),
        StructField("recipients", ArrayType(StringType()), True),
        StructField("notify", BooleanType(), True),
        StructField("link_to_current", BooleanType(), True),
        StructField("creation_date", TimestampType(), False),
        StructField("created_by", StringType(), True),
        StructField("resource_id", StringType(), True),
        StructField("egnyte_domain", StringType(), False),
    ]
)

# ``GET /pubapi/v1|v2/events`` — the always-on file-system activity feed.
EVENTS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("action_source", StringType(), True),
        # Joins to users.id.
        StructField("actor", LongType(), True),
        StructField("type", StringType(), True),
        StructField("action", StringType(), True),
        StructField("data", EVENT_DATA_STRUCT, True),
        StructField("egnyte_domain", StringType(), False),
    ]
)

# ``POST /pubapi/v1/audit/logins`` report rows.
AUDIT_LOGINS_SCHEMA = StructType(
    [
        # Rendered as "Jane Smith (jsmith@company.com)" — not a bare username.
        StructField("username", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("event", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("access", StringType(), True),
        StructField("time", StringType(), False),
        StructField("egnyte_domain", StringType(), False),
    ]
)

# ``POST /pubapi/v1/audit/files`` report rows.
AUDIT_FILES_SCHEMA = StructType(
    [
        StructField("username", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("file", StringType(), True),
        StructField("target_path", StringType(), True),
        StructField("transaction", StringType(), True),
        StructField("access", StringType(), True),
        StructField("time", StringType(), False),
        StructField("egnyte_domain", StringType(), False),
    ]
)

# ``POST /pubapi/v1/audit/permissions`` report rows.
AUDIT_PERMISSIONS_SCHEMA = StructType(
    [
        StructField("folder", StringType(), True),
        StructField("assignee", StringType(), True),
        StructField("assignee_id", LongType(), True),
        StructField("assigner", StringType(), True),
        StructField("assigner_id", LongType(), True),
        StructField("change", StringType(), True),
        StructField("time", StringType(), False),
        StructField("egnyte_domain", StringType(), False),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    TABLE_FILES: FILES_SCHEMA,
    TABLE_FOLDERS: FOLDERS_SCHEMA,
    TABLE_USERS: USERS_SCHEMA,
    TABLE_GROUPS: GROUPS_SCHEMA,
    TABLE_LINKS: LINKS_SCHEMA,
    TABLE_EVENTS: EVENTS_SCHEMA,
    TABLE_AUDIT_LOGINS: AUDIT_LOGINS_SCHEMA,
    TABLE_AUDIT_FILES: AUDIT_FILES_SCHEMA,
    TABLE_AUDIT_PERMISSIONS: AUDIT_PERMISSIONS_SCHEMA,
}


# =============================================================================
# Table metadata
# =============================================================================
#
# ``files`` / ``folders`` / ``users`` / ``groups`` are snapshots: the File
# System API has no domain-wide "changed since" endpoint and the SCIM-style
# user/group ``filter`` only supports exact match on name-like fields, so
# change detection has to be a full pull + downstream diff.
#
# ``links``, ``events`` and the three audit report tables are append-only
# logs with genuine server-side incremental filters.
TABLE_METADATA: dict[str, dict] = {
    TABLE_FILES: {
        # group_id is stable across versions but a file can be renamed/moved
        # without it changing, and two entries can share a group_id after a
        # copy — the doc recommends the (group_id, path) composite.
        "primary_keys": ["group_id", "path"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_FOLDERS: {
        "primary_keys": ["folder_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_USERS: {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_GROUPS: {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_LINKS: {
        "primary_keys": ["id"],
        "cursor_field": "creation_date",
        # `created_after` filters on creation only, and link deletion is not
        # surfaced by the endpoint — this is a create-feed, not a CDC stream.
        "ingestion_type": "append",
    },
    TABLE_EVENTS: {
        "primary_keys": ["id"],
        "cursor_field": "id",
        "ingestion_type": "append",
    },
    # Audit report rows carry no server-assigned row id (doc: UNVERIFIED).
    # The composite below is the doc's recommended natural key for dedup.
    TABLE_AUDIT_LOGINS: {
        "primary_keys": ["user_id", "time", "event"],
        "cursor_field": "time",
        "ingestion_type": "append",
    },
    TABLE_AUDIT_FILES: {
        "primary_keys": ["user_id", "time", "transaction"],
        "cursor_field": "time",
        "ingestion_type": "append",
    },
    TABLE_AUDIT_PERMISSIONS: {
        "primary_keys": ["assignee_id", "time", "change"],
        "cursor_field": "time",
        "ingestion_type": "append",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())


# =============================================================================
# HTTP / rate-limit tuning
# =============================================================================

DEFAULT_TIMEOUT = 30  # seconds; every request passes an explicit timeout

# Retried with backoff. 429 is the "modern style" throttle; 403 is only
# retried when the Mashery error header marks it as a throttle (see
# egnyte_client.is_throttled) — a plain 403 is a permissions failure.
RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds, doubled per retry

# Legacy throttle signalling: 403 + X-Mashery-Error-Code.
MASHERY_ERROR_HEADER = "X-Mashery-Error-Code"
MASHERY_THROTTLE_CODES = (
    "ERR_403_DEVELOPER_OVER_QPS",
    "ERR_403_DEVELOPER_OVER_RATE",
)

# Proactive QPS headroom headers. When current is at/above allotted we sleep
# briefly rather than earning a 403/429.
QPS_CURRENT_HEADER = "X-Accesstoken-Qps-Current"
QPS_ALLOTTED_HEADER = "X-Accesstoken-Qps-Allotted"
QPS_COOLDOWN_SECONDS = 1.0

# Standard quota is 2 calls/second/token. Space calls out so a tree walk or a
# wide partition fan-out does not trip the per-second limit on its own.
DEFAULT_MIN_REQUEST_INTERVAL = 0.5  # seconds between requests on one client

# The OAuth token endpoint is capped at 100 requests/hour and answers 409
# (not 429) when throttled.
OAUTH_THROTTLE_STATUS = 409


# =============================================================================
# Read tuning defaults (all overridable through table_options)
# =============================================================================

# Filesystem tree walk.
DEFAULT_ROOT_PATHS = "/Shared"
DEFAULT_FS_PAGE_SIZE = 100
DEFAULT_MAX_DEPTH = 20

# SCIM list endpoints cap page size at 100.
DEFAULT_SCIM_PAGE_SIZE = 100
MAX_SCIM_PAGE_SIZE = 100

# Links: docs allow up to 500 per request.
DEFAULT_LINKS_PAGE_SIZE = 100
MAX_LINKS_PAGE_SIZE = 500
DEFAULT_LINKS_WINDOW_DAYS = 7

# Events.
DEFAULT_EVENTS_PAGE_SIZE = 100
DEFAULT_EVENTS_API_VERSION = "v1"

# Audit reporting v1.
DEFAULT_AUDIT_WINDOW_DAYS = 7
DEFAULT_AUDIT_BACKFILL_DAYS = 7
DEFAULT_AUDIT_PAGE_SIZE = 100
# Official guidance: poll the job endpoint no more than once every 2 minutes.
DEFAULT_AUDIT_POLL_INTERVAL_SECONDS = 120
DEFAULT_AUDIT_POLL_MAX_ATTEMPTS = 30
DEFAULT_AUDIT_LOGIN_EVENTS = ("successful_login", "failed_attempts")
# `permissions` reports require all four filter keys per the SDK signature.
DEFAULT_AUDIT_PERMISSION_FOLDERS = ("/Shared",)
AUDIT_RUNNING_STATUSES = frozenset(
    {"running", "in_progress", "in progress", "pending", "queued", "started"}
)

# Hard stop for every pagination loop. Egnyte's pagination metadata is
# inconsistent across resources (the v2 links list's field names are
# UNVERIFIED in the doc), so each loop also carries an absolute page ceiling
# rather than trusting the server to eventually return a short page.
MAX_PAGES_PER_READ = 1000

# Generic microbatch admission control for the single-driver read_table path.
DEFAULT_MAX_RECORDS_PER_BATCH = 5000
DEFAULT_MAX_PARTITIONS_PER_BATCH = 1
