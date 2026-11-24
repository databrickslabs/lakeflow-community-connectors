"""Static schema definitions for Azure DevOps connector tables.

This module contains all Spark StructType schema definitions and table metadata
for the Azure DevOps Lakeflow connector. These are derived from the Azure DevOps
REST API documentation.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    IntegerType,
    ArrayType,
)


# =============================================================================
# Reusable Nested Struct Definitions
# =============================================================================

PROJECT_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("revision", LongType(), True),
        StructField("visibility", StringType(), True),
        StructField("lastUpdateTime", StringType(), True),
    ]
)

PARENT_REPOSITORY_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("project", PROJECT_STRUCT, True),
    ]
)

LINK_STRUCT = StructType([StructField("href", StringType(), True)])

LINKS_STRUCT = StructType(
    [
        StructField("self", LINK_STRUCT, True),
        StructField("project", LINK_STRUCT, True),
        StructField("web", LINK_STRUCT, True),
        StructField("ssh", LINK_STRUCT, True),
        StructField("commits", LINK_STRUCT, True),
        StructField("refs", LINK_STRUCT, True),
        StructField("pullRequests", LINK_STRUCT, True),
        StructField("items", LINK_STRUCT, True),
        StructField("pushes", LINK_STRUCT, True),
    ]
)

IDENTITY_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("uniqueName", StringType(), True),
        StructField("url", StringType(), True),
        StructField("imageUrl", StringType(), True),
    ]
)

GIT_USER_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("date", StringType(), True),
    ]
)

CHANGE_COUNTS_STRUCT = StructType(
    [
        StructField("Add", IntegerType(), True),
        StructField("Edit", IntegerType(), True),
        StructField("Delete", IntegerType(), True),
    ]
)

COMMIT_REF_STRUCT = StructType(
    [
        StructField("commitId", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

REF_UPDATE_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("oldObjectId", StringType(), True),
        StructField("newObjectId", StringType(), True),
    ]
)

COMMENT_STRUCT = StructType(
    [
        StructField("id", LongType(), True),
        StructField("parentCommentId", LongType(), True),
        StructField("author", IDENTITY_STRUCT, True),
        StructField("content", StringType(), True),
        StructField("publishedDate", StringType(), True),
        StructField("lastUpdatedDate", StringType(), True),
        StructField("lastContentUpdatedDate", StringType(), True),
        StructField("commentType", StringType(), True),
    ]
)

POSITION_STRUCT = StructType(
    [
        StructField("line", LongType(), True),
        StructField("offset", LongType(), True),
    ]
)

THREAD_CONTEXT_STRUCT = StructType(
    [
        StructField("filePath", StringType(), True),
        StructField("rightFileStart", POSITION_STRUCT, True),
        StructField("rightFileEnd", POSITION_STRUCT, True),
        StructField("leftFileStart", POSITION_STRUCT, True),
        StructField("leftFileEnd", POSITION_STRUCT, True),
    ]
)

RELATION_STRUCT = StructType(
    [
        StructField("rel", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

FIELD_DEF_STRUCT = StructType(
    [
        StructField("referenceName", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("readOnly", BooleanType(), True),
        StructField("required", BooleanType(), True),
        StructField("defaultValue", StringType(), True),
        StructField("helpText", StringType(), True),
    ]
)

STATE_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("color", StringType(), True),
        StructField("category", StringType(), True),
    ]
)

ICON_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("url", StringType(), True),
    ]
)


# =============================================================================
# Table Schemas
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    "projects": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("url", StringType(), True),
            StructField("state", StringType(), True),
            StructField("revision", LongType(), True),
            StructField("visibility", StringType(), True),
            StructField("lastUpdateTime", StringType(), True),
            StructField("organization", StringType(), False),
        ]
    ),
    "repositories": StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
            StructField("project", PROJECT_STRUCT, True),
            StructField("defaultBranch", StringType(), True),
            StructField("size", LongType(), True),
            StructField("remoteUrl", StringType(), True),
            StructField("sshUrl", StringType(), True),
            StructField("webUrl", StringType(), True),
            StructField("isDisabled", BooleanType(), True),
            StructField("isInMaintenance", BooleanType(), True),
            StructField("isFork", BooleanType(), True),
            StructField(
                "parentRepository", PARENT_REPOSITORY_STRUCT, True
            ),
            StructField("_links", LINKS_STRUCT, True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
        ]
    ),
    "commits": StructType(
        [
            StructField("commitId", StringType(), False),
            StructField("author", GIT_USER_STRUCT, True),
            StructField("committer", GIT_USER_STRUCT, True),
            StructField("comment", StringType(), True),
            StructField("commentTruncated", BooleanType(), True),
            StructField("changeCounts", CHANGE_COUNTS_STRUCT, True),
            StructField("url", StringType(), True),
            StructField("remoteUrl", StringType(), True),
            StructField("treeId", StringType(), True),
            StructField("parents", ArrayType(StringType()), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
            StructField("repository_id", StringType(), False),
        ]
    ),
    "pullrequests": StructType(
        [
            StructField("pullRequestId", LongType(), False),
            StructField("codeReviewId", LongType(), True),
            StructField("status", StringType(), True),
            StructField("createdBy", IDENTITY_STRUCT, True),
            StructField("creationDate", StringType(), True),
            StructField("closedDate", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("sourceRefName", StringType(), True),
            StructField("targetRefName", StringType(), True),
            StructField("mergeStatus", StringType(), True),
            StructField("mergeId", StringType(), True),
            StructField(
                "lastMergeSourceCommit", COMMIT_REF_STRUCT, True
            ),
            StructField(
                "lastMergeTargetCommit", COMMIT_REF_STRUCT, True
            ),
            StructField("lastMergeCommit", COMMIT_REF_STRUCT, True),
            StructField("url", StringType(), True),
            StructField("supportsIterations", BooleanType(), True),
            StructField("artifactId", StringType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
            StructField("repository_id", StringType(), False),
        ]
    ),
    "refs": StructType(
        [
            StructField("name", StringType(), False),
            StructField("objectId", StringType(), True),
            StructField("creator", IDENTITY_STRUCT, True),
            StructField("url", StringType(), True),
            StructField("peeledObjectId", StringType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
            StructField("repository_id", StringType(), False),
        ]
    ),
    "pushes": StructType(
        [
            StructField("pushId", LongType(), False),
            StructField("date", StringType(), True),
            StructField("pushedBy", IDENTITY_STRUCT, True),
            StructField("url", StringType(), True),
            StructField(
                "refUpdates", ArrayType(REF_UPDATE_STRUCT), True
            ),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
            StructField("repository_id", StringType(), False),
        ]
    ),
    "users": StructType(
        [
            StructField("descriptor", StringType(), False),
            StructField("displayName", StringType(), True),
            StructField("mailAddress", StringType(), True),
            StructField("principalName", StringType(), True),
            StructField("origin", StringType(), True),
            StructField("originId", StringType(), True),
            StructField("subjectKind", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("directoryAlias", StringType(), True),
            StructField("url", StringType(), True),
            StructField("_links", LINKS_STRUCT, True),
            StructField("organization", StringType(), False),
        ]
    ),
    "pullrequest_threads": StructType(
        [
            StructField("id", LongType(), False),
            StructField("publishedDate", StringType(), True),
            StructField("lastUpdatedDate", StringType(), True),
            StructField(
                "comments", ArrayType(COMMENT_STRUCT), True
            ),
            StructField("status", StringType(), True),
            StructField(
                "threadContext", THREAD_CONTEXT_STRUCT, True
            ),
            StructField("isDeleted", BooleanType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
            StructField("repository_id", StringType(), False),
            StructField("pullrequest_id", LongType(), False),
        ]
    ),
    "pullrequest_workitems": StructType(
        [
            StructField("id", StringType(), False),
            StructField("url", StringType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
            StructField("repository_id", StringType(), False),
            StructField("pullrequest_id", LongType(), False),
        ]
    ),
    "pullrequest_commits": StructType(
        [
            StructField("commitId", StringType(), False),
            StructField("author", GIT_USER_STRUCT, True),
            StructField("committer", GIT_USER_STRUCT, True),
            StructField("comment", StringType(), True),
            StructField("commentTruncated", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
            StructField("repository_id", StringType(), False),
            StructField("pullrequest_id", LongType(), False),
        ]
    ),
    "pullrequest_reviewers": StructType(
        [
            StructField("reviewerUrl", StringType(), True),
            StructField("vote", LongType(), True),
            StructField("hasDeclined", BooleanType(), True),
            StructField("isFlagged", BooleanType(), True),
            StructField("displayName", StringType(), True),
            StructField("id", StringType(), False),
            StructField("uniqueName", StringType(), True),
            StructField("url", StringType(), True),
            StructField("imageUrl", StringType(), True),
            StructField("isRequired", BooleanType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
            StructField("repository_id", StringType(), False),
            StructField("pullrequest_id", LongType(), False),
        ]
    ),
    "workitems": StructType(
        [
            StructField("id", LongType(), False),
            StructField("rev", LongType(), True),
            StructField("fields", StringType(), True),
            StructField(
                "relations", ArrayType(RELATION_STRUCT), True
            ),
            StructField("url", StringType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
        ]
    ),
    "workitem_revisions": StructType(
        [
            StructField("id", LongType(), False),
            StructField("rev", LongType(), False),
            StructField("workItemId", LongType(), True),
            StructField("revisedDate", StringType(), True),
            StructField("fields", StringType(), True),
            StructField("url", StringType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
        ]
    ),
    "workitem_types": StructType(
        [
            StructField("name", StringType(), True),
            StructField("referenceName", StringType(), False),
            StructField("description", StringType(), True),
            StructField("color", StringType(), True),
            StructField("icon", ICON_STRUCT, True),
            StructField("isDisabled", BooleanType(), True),
            StructField(
                "fields", ArrayType(FIELD_DEF_STRUCT), True
            ),
            StructField(
                "states", ArrayType(STATE_STRUCT), True
            ),
            StructField("url", StringType(), True),
            StructField("organization", StringType(), False),
            StructField("project_name", StringType(), False),
        ]
    ),
}


# =============================================================================
# Table Metadata
# =============================================================================

TABLE_METADATA: dict[str, dict] = {
    "projects": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "repositories": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "commits": {
        "primary_keys": ["commitId", "repository_id"],
        "ingestion_type": "append",
    },
    "pullrequests": {
        "primary_keys": ["pullRequestId", "repository_id"],
        "cursor_field": "closedDate",
        "ingestion_type": "cdc",
    },
    "refs": {
        "primary_keys": ["name", "repository_id"],
        "ingestion_type": "snapshot",
    },
    "pushes": {
        "primary_keys": ["pushId", "repository_id"],
        "ingestion_type": "append",
    },
    "users": {
        "primary_keys": ["descriptor"],
        "ingestion_type": "snapshot",
    },
    "pullrequest_threads": {
        "primary_keys": ["id", "pullrequest_id", "repository_id"],
        "ingestion_type": "append",
        "cursor_field": "publishedDate",
    },
    "pullrequest_workitems": {
        "primary_keys": ["id", "pullrequest_id", "repository_id"],
        "ingestion_type": "snapshot",
    },
    "pullrequest_commits": {
        "primary_keys": [
            "commitId", "pullrequest_id", "repository_id",
        ],
        "ingestion_type": "snapshot",
    },
    "pullrequest_reviewers": {
        "primary_keys": ["id", "pullrequest_id", "repository_id"],
        "ingestion_type": "snapshot",
    },
    "workitems": {
        "primary_keys": ["id"],
        "ingestion_type": "cdc",
        "cursor_field": "rev",
    },
    "workitem_revisions": {
        "primary_keys": ["id", "rev"],
        "ingestion_type": "append",
    },
    "workitem_types": {
        "primary_keys": ["referenceName", "project_name"],
        "ingestion_type": "snapshot",
    },
}

SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
