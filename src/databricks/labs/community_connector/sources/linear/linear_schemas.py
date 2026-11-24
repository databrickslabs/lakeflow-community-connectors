"""Schemas, metadata, GraphQL queries, and constants for the Linear connector.

Linear is a single-endpoint GraphQL API. The set of objects and their fields
is static (defined by the GraphQL schema), so the connector hard-codes the
Spark schema, the metadata, and the GraphQL document for each table rather
than discovering them at runtime.

Connection-type fields (``labels``, ``members``, ``teams``,
``projectMilestones``) arrive from GraphQL wrapped in a ``{ "nodes": [...] }``
envelope. The connector flattens them to plain arrays before returning records,
so the schemas below declare them as ``ArrayType(StructType(...))`` to match the
flattened shape.
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

# --- Reusable nested struct types -----------------------------------------

# WorkflowState: { id, name, type, color }
_STATE_STRUCT = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("color", StringType(), nullable=True),
    ]
)

# User: { id, name, email }
_USER_STRUCT = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
    ]
)

# Team: { id, key, name }
_TEAM_STRUCT = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("key", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
    ]
)

# IssueLabel: { id, name, color }
_LABEL_STRUCT = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("color", StringType(), nullable=True),
    ]
)

# Project reference on an issue: { id, name, slugId }
_PROJECT_REF_STRUCT = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("slugId", StringType(), nullable=True),
    ]
)

# Parent issue reference: { id, identifier, title }
_PARENT_STRUCT = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("identifier", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
    ]
)

# Cycle: { id, name, number }
_CYCLE_STRUCT = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("number", LongType(), nullable=True),
    ]
)

# ProjectMilestone: { id, name, targetDate, status, progress }
_MILESTONE_STRUCT = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("targetDate", DateType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("progress", DoubleType(), nullable=True),
    ]
)

# --- Table schemas ---------------------------------------------------------

ISSUES_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("identifier", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("priority", LongType(), nullable=True),
        StructField("priorityLabel", StringType(), nullable=True),
        StructField("estimate", DoubleType(), nullable=True),
        StructField("dueDate", DateType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("trashed", BooleanType(), nullable=True),
        StructField("createdAt", TimestampType(), nullable=True),
        StructField("updatedAt", TimestampType(), nullable=False),
        StructField("archivedAt", TimestampType(), nullable=True),
        StructField("startedAt", TimestampType(), nullable=True),
        StructField("completedAt", TimestampType(), nullable=True),
        StructField("canceledAt", TimestampType(), nullable=True),
        StructField("autoArchivedAt", TimestampType(), nullable=True),
        StructField("autoClosedAt", TimestampType(), nullable=True),
        StructField("state", _STATE_STRUCT, nullable=True),
        StructField("assignee", _USER_STRUCT, nullable=True),
        StructField("team", _TEAM_STRUCT, nullable=True),
        StructField("labels", ArrayType(_LABEL_STRUCT), nullable=True),
        StructField("project", _PROJECT_REF_STRUCT, nullable=True),
        StructField("parent", _PARENT_STRUCT, nullable=True),
        StructField("creator", _USER_STRUCT, nullable=True),
        StructField("cycle", _CYCLE_STRUCT, nullable=True),
    ]
)

PROJECTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("slugId", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("progress", DoubleType(), nullable=True),
        StructField("scope", DoubleType(), nullable=True),
        StructField("priority", LongType(), nullable=True),
        StructField("priorityLabel", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("startDate", DateType(), nullable=True),
        StructField("targetDate", DateType(), nullable=True),
        StructField("startedAt", TimestampType(), nullable=True),
        StructField("completedAt", TimestampType(), nullable=True),
        StructField("canceledAt", TimestampType(), nullable=True),
        StructField("autoArchivedAt", TimestampType(), nullable=True),
        StructField("issueCountHistory", ArrayType(LongType()), nullable=True),
        StructField("scopeHistory", ArrayType(DoubleType()), nullable=True),
        StructField("createdAt", TimestampType(), nullable=True),
        StructField("updatedAt", TimestampType(), nullable=False),
        StructField("archivedAt", TimestampType(), nullable=True),
        StructField("lead", _USER_STRUCT, nullable=True),
        StructField("creator", _USER_STRUCT, nullable=True),
        StructField("members", ArrayType(_USER_STRUCT), nullable=True),
        StructField("teams", ArrayType(_TEAM_STRUCT), nullable=True),
        StructField("projectMilestones", ArrayType(_MILESTONE_STRUCT), nullable=True),
    ]
)

TABLE_SCHEMAS = {
    "issues": ISSUES_SCHEMA,
    "projects": PROJECTS_SCHEMA,
}

# All tables are incremental via ``updatedAt``. Linear exposes no hard-delete
# event stream, so deletes are not synchronized (cdc, not cdc_with_deletes).
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

# The GraphQL root connection field per table.
ROOT_FIELD = {
    "issues": "issues",
    "projects": "projects",
}

# Connection fields that GraphQL wraps in ``{ "nodes": [...] }``. The connector
# flattens these to plain arrays so the returned records match the schemas.
CONNECTION_FIELDS = {
    "issues": ["labels"],
    "projects": ["members", "teams", "projectMilestones"],
}

# --- GraphQL documents -----------------------------------------------------

ISSUES_QUERY = """
query Issues($first: Int, $after: String, $filter: IssueFilter) {
  issues(
    first: $first
    after: $after
    includeArchived: true
    orderBy: updatedAt
    filter: $filter
  ) {
    nodes {
      id
      identifier
      title
      description
      priority
      priorityLabel
      estimate
      dueDate
      url
      trashed
      createdAt
      updatedAt
      archivedAt
      startedAt
      completedAt
      canceledAt
      autoArchivedAt
      autoClosedAt
      state { id name type color }
      assignee { id name email }
      team { id key name }
      labels { nodes { id name color } }
      project { id name slugId }
      parent { id identifier title }
      creator { id name email }
      cycle { id name number }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

PROJECTS_QUERY = """
query Projects($first: Int, $after: String, $filter: ProjectFilter) {
  projects(
    first: $first
    after: $after
    includeArchived: true
    orderBy: updatedAt
    filter: $filter
  ) {
    nodes {
      id
      name
      description
      slugId
      state
      progress
      scope
      priority
      priorityLabel
      url
      startDate
      targetDate
      startedAt
      completedAt
      canceledAt
      autoArchivedAt
      issueCountHistory
      scopeHistory
      createdAt
      updatedAt
      archivedAt
      lead { id name email }
      creator { id name email }
      members { nodes { id name email } }
      teams { nodes { id key name } }
      projectMilestones { nodes { id name targetDate status progress } }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""".strip()

TABLE_QUERIES = {
    "issues": ISSUES_QUERY,
    "projects": PROJECTS_QUERY,
}

# --- Connection / retry constants ------------------------------------------

GRAPHQL_URL = "https://api.linear.app/graphql"
OAUTH_TOKEN_URL = "https://api.linear.app/oauth/token"

# Linear recommends first: 50 to keep per-query complexity low (250 max).
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 250

# Cap records returned to the framework per read_table call (admission control).
DEFAULT_MAX_RECORDS_PER_BATCH = 200

# Subtract a short lookback from the cursor at query time to catch records
# updated concurrently around the previous high-water mark (clock skew / late
# writes). Applied to the query only; the stored offset remains the raw max.
DEFAULT_LOOKBACK_SECONDS = 300

REQUEST_TIMEOUT_SECONDS = 30
RETRIABLE_STATUS_CODES = {429, 500, 502, 503}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry
