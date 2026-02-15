"""Static schema definitions for Microsoft Teams connector tables.

This module contains all Spark StructType schema definitions and table metadata
for the Microsoft Teams Lakeflow connector. These are derived from the
Microsoft Graph API v1.0 documentation.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    ArrayType,
)


# =============================================================================
# Reusable Nested Struct Definitions
# =============================================================================

IDENTITY_SET_SCHEMA = StructType(
    [
        StructField("application", StringType(), True),
        StructField("device", StringType(), True),
        StructField(
            "user",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("displayName", StringType(), True),
                    StructField("userIdentityType", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)
"""Nested identity set schema used for message senders and mentions."""

BODY_SCHEMA = StructType(
    [
        StructField("contentType", StringType(), True),
        StructField("content", StringType(), True),
    ]
)
"""Message body schema with content type and content."""

ATTACHMENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("contentType", StringType(), True),
        StructField("contentUrl", StringType(), True),
        StructField("content", StringType(), True),
        StructField("name", StringType(), True),
        StructField("thumbnailUrl", StringType(), True),
    ]
)
"""Attachment schema for message attachments."""

MENTION_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("mentionText", StringType(), True),
        StructField("mentioned", IDENTITY_SET_SCHEMA, True),
    ]
)
"""Mention schema for @mentions in messages."""

REACTION_SCHEMA = StructType(
    [
        StructField("reactionType", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("user", IDENTITY_SET_SCHEMA, True),
    ]
)
"""Reaction schema for emoji reactions on messages."""

CHANNEL_IDENTITY_SCHEMA = StructType(
    [
        StructField("teamId", StringType(), True),
        StructField("channelId", StringType(), True),
    ]
)
"""Channel identity schema for message channel references."""


# =============================================================================
# Table Schema Definitions
# =============================================================================

TEAMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
        StructField("classification", StringType(), True),
        StructField("visibility", StringType(), True),
        StructField("webUrl", StringType(), True),
        StructField("isArchived", BooleanType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("internalId", StringType(), True),
        StructField("tenantId", StringType(), True),
        StructField("specialization", StringType(), True),
        StructField("memberSettings", StringType(), True),
        StructField("guestSettings", StringType(), True),
        StructField("messagingSettings", StringType(), True),
        StructField("funSettings", StringType(), True),
    ]
)
"""Schema for the teams table."""

CHANNELS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("team_id", StringType(), False),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
        StructField("email", StringType(), True),
        StructField("webUrl", StringType(), True),
        StructField("membershipType", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("isFavoriteByDefault", BooleanType(), True),
        StructField("isArchived", BooleanType(), True),
        StructField("tenantId", StringType(), True),
    ]
)
"""Schema for the channels table."""

MESSAGES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("team_id", StringType(), False),
        StructField("channel_id", StringType(), False),
        StructField("replyToId", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("messageType", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("lastModifiedDateTime", StringType(), True),
        StructField("lastEditedDateTime", StringType(), True),
        StructField("deletedDateTime", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("importance", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("webUrl", StringType(), True),
        StructField("from", IDENTITY_SET_SCHEMA, True),
        StructField("body", BODY_SCHEMA, True),
        StructField("attachments", ArrayType(ATTACHMENT_SCHEMA), True),
        StructField("mentions", ArrayType(MENTION_SCHEMA), True),
        StructField("reactions", ArrayType(REACTION_SCHEMA), True),
        StructField("channelIdentity", CHANNEL_IDENTITY_SCHEMA, True),
        StructField("policyViolation", StringType(), True),
        StructField("eventDetail", StringType(), True),
        StructField("messageHistory", StringType(), True),
    ]
)
"""Schema for the messages table."""

MEMBERS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("team_id", StringType(), False),
        StructField("roles", ArrayType(StringType()), True),
        StructField("displayName", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("email", StringType(), True),
        StructField("visibleHistoryStartDateTime", StringType(), True),
        StructField("tenantId", StringType(), True),
    ]
)
"""Schema for the members table."""

MESSAGE_REPLIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("parent_message_id", StringType(), False),
        StructField("team_id", StringType(), False),
        StructField("channel_id", StringType(), False),
        StructField("replyToId", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("messageType", StringType(), True),
        StructField("createdDateTime", StringType(), True),
        StructField("lastModifiedDateTime", StringType(), True),
        StructField("lastEditedDateTime", StringType(), True),
        StructField("deletedDateTime", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("importance", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("webUrl", StringType(), True),
        StructField("from", IDENTITY_SET_SCHEMA, True),
        StructField("body", BODY_SCHEMA, True),
        StructField("attachments", ArrayType(ATTACHMENT_SCHEMA), True),
        StructField("mentions", ArrayType(MENTION_SCHEMA), True),
        StructField("reactions", ArrayType(REACTION_SCHEMA), True),
        StructField("channelIdentity", CHANNEL_IDENTITY_SCHEMA, True),
        StructField("policyViolation", StringType(), True),
        StructField("eventDetail", StringType(), True),
        StructField("messageHistory", StringType(), True),
    ]
)
"""Schema for the message_replies table."""


# =============================================================================
# Schema Mapping
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    "teams": TEAMS_SCHEMA,
    "channels": CHANNELS_SCHEMA,
    "messages": MESSAGES_SCHEMA,
    "members": MEMBERS_SCHEMA,
    "message_replies": MESSAGE_REPLIES_SCHEMA,
}
"""Mapping of table names to their StructType schemas."""


# =============================================================================
# Table Metadata Definitions
# =============================================================================

TABLE_METADATA: dict[str, dict] = {
    "teams": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
        "endpoint": "groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')",
    },
    "channels": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
        "endpoint": "teams/{team_id}/channels",
    },
    "messages": {
        "primary_keys": ["id"],
        "cursor_field": "lastModifiedDateTime",
        "ingestion_type": "cdc",
        "endpoint": "teams/{team_id}/channels/{channel_id}/messages",
    },
    "members": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
        "endpoint": "teams/{team_id}/members",
    },
    "message_replies": {
        "primary_keys": ["id"],
        "cursor_field": "lastModifiedDateTime",
        "ingestion_type": "cdc",
        "endpoint": "teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies",
    },
}
"""Metadata for each table including primary keys, cursor field, and ingestion type."""


# =============================================================================
# Supported Tables
# =============================================================================

SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
"""List of all table names supported by the Microsoft Teams connector."""
