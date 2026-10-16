"""Static schemas, metadata, and GraphQL documents for the Linear connector.

Linear exposes a single GraphQL endpoint (``POST /graphql``). Schemas
here are derived from the official Linear SDK generated fragments — see
``linear_api_doc.md`` for the field-level reference and per-field type
mapping.

Design notes:
- Nested foreign-key references (``team { id }``, ``state { id }`` …) are
  kept as ``StructType`` with a single ``id`` field rather than flattened
  to ``team_id``. This matches the raw GraphQL response shape exactly, so
  ``read_table`` can return parsed nodes without any transformation.
- ``LongType`` is used for all integer fields (incl. enum ints like
  ``priority``) to avoid overflow.
- ``reactionData`` is a JSON blob serialized to a string.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SUPPORTED_TABLES = ["issues", "projects"]

# Linear's single GraphQL endpoint.
GRAPHQL_URL = "https://api.linear.app/graphql"

# Retry/backoff configuration for transient errors and rate limiting.
RETRIABLE_STATUS_CODES = {429, 500, 502, 503}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

# Default Relay page size. The doc caps ``first`` at 250; 50 is a safe
# default for the moderately nested issue/project queries.
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 250


def _fk_struct() -> StructType:
    """A nested foreign-key reference exposing just the related object id."""
    return StructType([StructField("id", StringType(), True)])


ISSUE_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("identifier", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("number", LongType(), True),
        StructField("url", StringType(), True),
        StructField("priority", LongType(), True),
        StructField("priorityLabel", StringType(), True),
        StructField("estimate", DoubleType(), True),
        StructField("dueDate", DateType(), True),
        StructField("boardOrder", DoubleType(), True),
        StructField("sortOrder", DoubleType(), True),
        StructField("prioritySortOrder", DoubleType(), True),
        StructField("subIssueSortOrder", DoubleType(), True),
        StructField("branchName", StringType(), True),
        StructField("labelIds", ArrayType(StringType()), True),
        StructField("previousIdentifiers", ArrayType(StringType()), True),
        StructField("integrationSourceType", StringType(), True),
        StructField("customerTicketCount", LongType(), True),
        StructField("reactionData", StringType(), True),
        StructField("trashed", BooleanType(), True),
        StructField("inheritsSharedAccess", BooleanType(), True),
        StructField("slaType", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), False),
        StructField("archivedAt", TimestampType(), True),
        StructField("completedAt", TimestampType(), True),
        StructField("canceledAt", TimestampType(), True),
        StructField("startedAt", TimestampType(), True),
        StructField("autoArchivedAt", TimestampType(), True),
        StructField("autoClosedAt", TimestampType(), True),
        StructField("addedToCycleAt", TimestampType(), True),
        StructField("addedToProjectAt", TimestampType(), True),
        StructField("addedToTeamAt", TimestampType(), True),
        StructField("startedTriageAt", TimestampType(), True),
        StructField("triagedAt", TimestampType(), True),
        StructField("snoozedUntilAt", TimestampType(), True),
        StructField("slaStartedAt", TimestampType(), True),
        StructField("slaBreachesAt", TimestampType(), True),
        StructField("slaHighRiskAt", TimestampType(), True),
        StructField("slaMediumRiskAt", TimestampType(), True),
        StructField("team", _fk_struct(), True),
        StructField("state", _fk_struct(), True),
        StructField("assignee", _fk_struct(), True),
        StructField("creator", _fk_struct(), True),
        StructField("parent", _fk_struct(), True),
        StructField("project", _fk_struct(), True),
        StructField("projectMilestone", _fk_struct(), True),
        StructField("cycle", _fk_struct(), True),
    ]
)

PROJECT_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("content", StringType(), True),
        StructField("slugId", StringType(), True),
        StructField("url", StringType(), True),
        StructField("color", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("priority", LongType(), True),
        StructField("priorityLabel", StringType(), True),
        StructField("prioritySortOrder", DoubleType(), True),
        StructField("sortOrder", DoubleType(), True),
        StructField("progress", DoubleType(), True),
        StructField("scope", DoubleType(), True),
        StructField("health", StringType(), True),
        StructField("state", StringType(), True),
        StructField("targetDate", DateType(), True),
        StructField("targetDateResolution", StringType(), True),
        StructField("startDate", DateType(), True),
        StructField("startDateResolution", StringType(), True),
        StructField("labelIds", ArrayType(StringType()), True),
        StructField("trashed", BooleanType(), True),
        StructField("slackIssueComments", BooleanType(), True),
        StructField("slackNewIssue", BooleanType(), True),
        StructField("slackIssueStatuses", BooleanType(), True),
        StructField("completedScopeHistory", ArrayType(DoubleType()), True),
        StructField("completedIssueCountHistory", ArrayType(LongType()), True),
        StructField("inProgressScopeHistory", ArrayType(DoubleType()), True),
        StructField("scopeHistory", ArrayType(DoubleType()), True),
        StructField("issueCountHistory", ArrayType(LongType()), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), False),
        StructField("archivedAt", TimestampType(), True),
        StructField("autoArchivedAt", TimestampType(), True),
        StructField("canceledAt", TimestampType(), True),
        StructField("completedAt", TimestampType(), True),
        StructField("startedAt", TimestampType(), True),
        StructField("healthUpdatedAt", TimestampType(), True),
        StructField("projectUpdateRemindersPausedUntilAt", TimestampType(), True),
        StructField("status", _fk_struct(), True),
        StructField("lead", _fk_struct(), True),
        StructField("creator", _fk_struct(), True),
        StructField("convertedFromIssue", _fk_struct(), True),
        StructField("lastUpdate", _fk_struct(), True),
    ]
)

TABLE_SCHEMAS = {
    "issues": ISSUE_SCHEMA,
    "projects": PROJECT_SCHEMA,
}

# Both objects are CDC (upsert-only incremental on ``updatedAt``). Linear
# exposes no soft-delete feed, so deletes are not captured — see the
# "Deleted Records" section of the API doc.
TABLE_METADATA = {
    "issues": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "projects": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
}

# Map a table name to its GraphQL root connection field name.
GRAPHQL_CONNECTION = {
    "issues": "issues",
    "projects": "projects",
}

# The selection set (node fields) requested per table. Built from the
# schema so the query and the schema never drift apart.
_ISSUE_NODE_FIELDS = """
      id
      identifier
      title
      description
      number
      url
      priority
      priorityLabel
      estimate
      dueDate
      boardOrder
      sortOrder
      prioritySortOrder
      subIssueSortOrder
      branchName
      labelIds
      previousIdentifiers
      integrationSourceType
      customerTicketCount
      reactionData
      trashed
      inheritsSharedAccess
      slaType
      createdAt
      updatedAt
      archivedAt
      completedAt
      canceledAt
      startedAt
      autoArchivedAt
      autoClosedAt
      addedToCycleAt
      addedToProjectAt
      addedToTeamAt
      startedTriageAt
      triagedAt
      snoozedUntilAt
      slaStartedAt
      slaBreachesAt
      slaHighRiskAt
      slaMediumRiskAt
      team { id }
      state { id }
      assignee { id }
      creator { id }
      parent { id }
      project { id }
      projectMilestone { id }
      cycle { id }
"""

_PROJECT_NODE_FIELDS = """
      id
      name
      description
      content
      slugId
      url
      color
      icon
      priority
      priorityLabel
      prioritySortOrder
      sortOrder
      progress
      scope
      health
      state
      targetDate
      targetDateResolution
      startDate
      startDateResolution
      labelIds
      trashed
      slackIssueComments
      slackNewIssue
      slackIssueStatuses
      completedScopeHistory
      completedIssueCountHistory
      inProgressScopeHistory
      scopeHistory
      issueCountHistory
      createdAt
      updatedAt
      archivedAt
      autoArchivedAt
      canceledAt
      completedAt
      startedAt
      healthUpdatedAt
      projectUpdateRemindersPausedUntilAt
      status { id }
      lead { id }
      creator { id }
      convertedFromIssue { id }
      lastUpdate { id }
"""

_NODE_FIELDS = {
    "issues": _ISSUE_NODE_FIELDS,
    "projects": _PROJECT_NODE_FIELDS,
}


def build_query(table_name: str) -> str:
    """Build the incremental Relay-paginated GraphQL query for ``table_name``.

    The query filters by ``updatedAt`` between ``$since`` (inclusive lower
    bound, the resume watermark) and ``$until`` (inclusive upper bound, the
    init-time cap), orders ascending by ``updatedAt`` so client-side
    truncation at ``max_records_per_batch`` is safe, and paginates with the
    Relay ``first``/``after`` cursor protocol.
    """
    connection = GRAPHQL_CONNECTION[table_name]
    node_fields = _NODE_FIELDS[table_name]
    return f"""
query {table_name.capitalize()}Incremental(
  $first: Int!
  $after: String
  $since: DateTimeOrDuration
  $until: DateTimeOrDuration
) {{
  {connection}(
    first: $first
    after: $after
    orderBy: updatedAt
    filter: {{ updatedAt: {{ gte: $since, lte: $until }} }}
    includeArchived: true
  ) {{
    nodes {{
{node_fields}    }}
    pageInfo {{
      hasNextPage
      endCursor
    }}
  }}
}}
"""
