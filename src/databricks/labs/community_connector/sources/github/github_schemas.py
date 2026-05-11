"""Static schema definitions for GitHub connector tables.

This module contains all Spark StructType schema definitions and table metadata
for the GitHub Lakeflow connector. These are derived from the GitHub REST API
documentation.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    ArrayType,
    MapType,
)


# =============================================================================
# Reusable Nested Struct Definitions
# =============================================================================

USER_STRUCT = StructType(
    [
        StructField("login", StringType(), True),
        StructField("id", LongType(), True),
        StructField("node_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("site_admin", BooleanType(), True),
    ]
)
"""Nested user/assignee struct schema used across multiple tables."""

PERMISSIONS_STRUCT = StructType(
    [
        StructField("admin", BooleanType(), True),
        StructField("push", BooleanType(), True),
        StructField("pull", BooleanType(), True),
    ]
)
"""Permissions struct schema for repositories and collaborators."""

LABEL_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("node_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("color", StringType(), True),
        StructField("description", StringType(), True),
        StructField("default", BooleanType(), True),
    ]
)
"""Label struct schema used in issues."""

MILESTONE_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("number", LongType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("state", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("due_on", StringType(), True),
    ]
)
"""Milestone struct schema used in issues."""

LICENSE_STRUCT = StructType(
    [
        StructField("key", StringType(), True),
        StructField("name", StringType(), True),
        StructField("spdx_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("node_id", StringType(), True),
    ]
)
"""License struct schema used in repositories."""

TEMPLATE_REPOSITORY_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("node_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("full_name", StringType(), True),
    ]
)
"""Template repository struct schema."""

COMMIT_REF_STRUCT = StructType(
    [
        StructField("sha", StringType(), True),
        StructField("url", StringType(), True),
    ]
)
"""Commit reference struct schema used in branches."""


# =============================================================================
# Table Schema Definitions
# =============================================================================

ISSUES_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("node_id", StringType(), True),
        StructField("number", LongType(), False),
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("title", StringType(), True),
        StructField("body", StringType(), True),
        StructField("state", StringType(), True),
        StructField("locked", BooleanType(), True),
        StructField("comments", LongType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("closed_at", StringType(), True),
        StructField("author_association", StringType(), True),
        StructField("url", StringType(), True),
        StructField("html_url", StringType(), True),
        StructField("labels_url", StringType(), True),
        StructField("comments_url", StringType(), True),
        StructField("events_url", StringType(), True),
        StructField("timeline_url", StringType(), True),
        StructField("state_reason", StringType(), True),
        StructField("user", USER_STRUCT, True),
        StructField("assignee", USER_STRUCT, True),
        StructField("assignees", ArrayType(USER_STRUCT, True), True),
        StructField("labels", ArrayType(LABEL_STRUCT, True), True),
        StructField("milestone", MILESTONE_STRUCT, True),
        StructField("pull_request", MapType(StringType(), StringType(), True), True),
        StructField("reactions", MapType(StringType(), StringType(), True), True),
    ]
)
"""Schema for the issues table."""

REPOSITORIES_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("node_id", StringType(), True),
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("owner", USER_STRUCT, True),
        StructField("private", BooleanType(), True),
        StructField("html_url", StringType(), True),
        StructField("description", StringType(), True),
        StructField("fork", BooleanType(), True),
        StructField("url", StringType(), True),
        StructField("archive_url", StringType(), True),
        StructField("assignees_url", StringType(), True),
        StructField("blobs_url", StringType(), True),
        StructField("branches_url", StringType(), True),
        StructField("collaborators_url", StringType(), True),
        StructField("comments_url", StringType(), True),
        StructField("commits_url", StringType(), True),
        StructField("compare_url", StringType(), True),
        StructField("contents_url", StringType(), True),
        StructField("contributors_url", StringType(), True),
        StructField("deployments_url", StringType(), True),
        StructField("downloads_url", StringType(), True),
        StructField("events_url", StringType(), True),
        StructField("forks_url", StringType(), True),
        StructField("git_commits_url", StringType(), True),
        StructField("git_refs_url", StringType(), True),
        StructField("git_tags_url", StringType(), True),
        StructField("git_url", StringType(), True),
        StructField("issue_comment_url", StringType(), True),
        StructField("issue_events_url", StringType(), True),
        StructField("issues_url", StringType(), True),
        StructField("keys_url", StringType(), True),
        StructField("labels_url", StringType(), True),
        StructField("languages_url", StringType(), True),
        StructField("merges_url", StringType(), True),
        StructField("milestones_url", StringType(), True),
        StructField("notifications_url", StringType(), True),
        StructField("pulls_url", StringType(), True),
        StructField("releases_url", StringType(), True),
        StructField("ssh_url", StringType(), True),
        StructField("stargazers_url", StringType(), True),
        StructField("statuses_url", StringType(), True),
        StructField("subscribers_url", StringType(), True),
        StructField("subscription_url", StringType(), True),
        StructField("tags_url", StringType(), True),
        StructField("teams_url", StringType(), True),
        StructField("trees_url", StringType(), True),
        StructField("clone_url", StringType(), True),
        StructField("mirror_url", StringType(), True),
        StructField("hooks_url", StringType(), True),
        StructField("svn_url", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("language", StringType(), True),
        StructField("forks_count", LongType(), True),
        StructField("stargazers_count", LongType(), True),
        StructField("watchers_count", LongType(), True),
        StructField("size", LongType(), True),
        StructField("default_branch", StringType(), True),
        StructField("open_issues_count", LongType(), True),
        StructField("is_template", BooleanType(), True),
        StructField("topics", ArrayType(StringType(), True), True),
        StructField("has_issues", BooleanType(), True),
        StructField("has_projects", BooleanType(), True),
        StructField("has_wiki", BooleanType(), True),
        StructField("has_pages", BooleanType(), True),
        StructField("has_downloads", BooleanType(), True),
        StructField("archived", BooleanType(), True),
        StructField("disabled", BooleanType(), True),
        StructField("visibility", StringType(), True),
        StructField("pushed_at", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("permissions", PERMISSIONS_STRUCT, True),
        StructField("allow_rebase_merge", BooleanType(), True),
        StructField("template_repository", TEMPLATE_REPOSITORY_STRUCT, True),
        StructField("temp_clone_token", StringType(), True),
        StructField("allow_squash_merge", BooleanType(), True),
        StructField("allow_merge_commit", BooleanType(), True),
        StructField("subscribers_count", LongType(), True),
        StructField("network_count", LongType(), True),
        StructField("license", LICENSE_STRUCT, True),
    ]
)
"""Schema for the repositories table."""

PULL_REQUESTS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("node_id", StringType(), True),
        StructField("number", LongType(), False),
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("state", StringType(), True),
        StructField("title", StringType(), True),
        StructField("body", StringType(), True),
        StructField("draft", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("closed_at", StringType(), True),
        StructField("merged_at", StringType(), True),
        StructField("merge_commit_sha", StringType(), True),
        StructField("user", USER_STRUCT, True),
        StructField("base", MapType(StringType(), StringType(), True), True),
        StructField("head", MapType(StringType(), StringType(), True), True),
        StructField("html_url", StringType(), True),
        StructField("url", StringType(), True),
    ]
)
"""Schema for the pull_requests table."""

COMMENTS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("node_id", StringType(), True),
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("issue_url", StringType(), True),
        StructField("html_url", StringType(), True),
        StructField("body", StringType(), True),
        StructField("user", USER_STRUCT, True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("author_association", StringType(), True),
    ]
)
"""Schema for the comments table."""

COMMITS_SCHEMA = StructType(
    [
        StructField("sha", StringType(), False),
        StructField("node_id", StringType(), True),
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("commit_message", StringType(), True),
        StructField("commit_author_name", StringType(), True),
        StructField("commit_author_email", StringType(), True),
        StructField("commit_author_date", StringType(), True),
        StructField("commit_committer_name", StringType(), True),
        StructField("commit_committer_email", StringType(), True),
        StructField("commit_committer_date", StringType(), True),
        StructField("html_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("author", USER_STRUCT, True),
        StructField("committer", USER_STRUCT, True),
    ]
)
"""Schema for the commits table."""

USERS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("login", StringType(), False),
        StructField("node_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("site_admin", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("company", StringType(), True),
        StructField("blog", StringType(), True),
        StructField("location", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)
"""Schema for the users table."""

ORGANIZATIONS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("login", StringType(), False),
        StructField("node_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("repos_url", StringType(), True),
        StructField("events_url", StringType(), True),
        StructField("hooks_url", StringType(), True),
        StructField("issues_url", StringType(), True),
        StructField("members_url", StringType(), True),
        StructField("public_members_url", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("description", StringType(), True),
    ]
)
"""Schema for the organizations table."""

TEAMS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("node_id", StringType(), True),
        StructField("organization_login", StringType(), False),
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("description", StringType(), True),
        StructField("privacy", StringType(), True),
        StructField("permission", StringType(), True),
    ]
)
"""Schema for the teams table."""

ASSIGNEES_SCHEMA = StructType(
    [
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("login", StringType(), False),
        StructField("id", LongType(), False),
        StructField("node_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("site_admin", BooleanType(), True),
    ]
)
"""Schema for the assignees table."""

COLLABORATORS_SCHEMA = StructType(
    [
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("login", StringType(), False),
        StructField("id", LongType(), False),
        StructField("node_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("site_admin", BooleanType(), True),
        StructField("permissions", PERMISSIONS_STRUCT, True),
    ]
)
"""Schema for the collaborators table."""

BRANCHES_SCHEMA = StructType(
    [
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("name", StringType(), False),
        StructField("commit", COMMIT_REF_STRUCT, True),
        StructField("protected", BooleanType(), True),
        StructField("protection_url", StringType(), True),
    ]
)
"""Schema for the branches table."""

REVIEWS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("node_id", StringType(), True),
        StructField("repository_owner", StringType(), False),
        StructField("repository_name", StringType(), False),
        StructField("pull_number", LongType(), False),
        StructField("state", StringType(), True),
        StructField("body", StringType(), True),
        StructField("user", USER_STRUCT, True),
        StructField("commit_id", StringType(), True),
        StructField("submitted_at", StringType(), True),
        StructField("html_url", StringType(), True),
    ]
)
"""Schema for the reviews table."""


# =============================================================================
# Schema Mapping
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    "issues": ISSUES_SCHEMA,
    "repositories": REPOSITORIES_SCHEMA,
    "pull_requests": PULL_REQUESTS_SCHEMA,
    "comments": COMMENTS_SCHEMA,
    "commits": COMMITS_SCHEMA,
    "users": USERS_SCHEMA,
    "organizations": ORGANIZATIONS_SCHEMA,
    "teams": TEAMS_SCHEMA,
    "assignees": ASSIGNEES_SCHEMA,
    "collaborators": COLLABORATORS_SCHEMA,
    "branches": BRANCHES_SCHEMA,
    "reviews": REVIEWS_SCHEMA,
}
"""Mapping of table names to their StructType schemas."""


# =============================================================================
# Table Metadata Definitions
# =============================================================================

TABLE_METADATA: dict[str, dict] = {
    "issues": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "repositories": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "pull_requests": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "comments": {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "ingestion_type": "cdc",
    },
    "commits": {
        "primary_keys": ["sha"],
        "ingestion_type": "append",
    },
    "users": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "organizations": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "teams": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "assignees": {
        "primary_keys": ["repository_owner", "repository_name", "id"],
        "ingestion_type": "snapshot",
    },
    "collaborators": {
        "primary_keys": ["repository_owner", "repository_name", "id"],
        "ingestion_type": "snapshot",
    },
    "branches": {
        "primary_keys": ["repository_owner", "repository_name", "name"],
        "ingestion_type": "snapshot",
    },
    "reviews": {
        "primary_keys": ["id"],
        "ingestion_type": "append",
    },
}
"""Metadata for each table including primary keys, cursor field, and ingestion type."""


# =============================================================================
# Supported Tables
# =============================================================================

SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
"""List of all table names supported by the GitHub connector."""
