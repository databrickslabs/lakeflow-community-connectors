# Gmail Schemas and Table Metadata
# Defines all Spark schemas, table metadata, and supported tables for the Gmail connector.

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    ArrayType,
)

# ─── Shared Structs ──────────────────────────────────────────────────────────

HEADER_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

BODY_STRUCT = StructType(
    [
        StructField("attachmentId", StringType(), True),
        StructField("size", LongType(), True),
        StructField("data", StringType(), True),
    ]
)

PART_STRUCT = StructType(
    [
        StructField("partId", StringType(), True),
        StructField("mimeType", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("headers", ArrayType(HEADER_STRUCT), True),
        StructField("body", BODY_STRUCT, True),
    ]
)

PAYLOAD_STRUCT = StructType(
    [
        StructField("partId", StringType(), True),
        StructField("mimeType", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("headers", ArrayType(HEADER_STRUCT), True),
        StructField("body", BODY_STRUCT, True),
        StructField("parts", ArrayType(PART_STRUCT), True),
    ]
)

# ─── Table Schemas ────────────────────────────────────────────────────────────

MESSAGES_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("threadId", StringType(), True),
        StructField("labelIds", ArrayType(StringType()), True),
        StructField("snippet", StringType(), True),
        StructField("historyId", StringType(), True),
        StructField("internalDate", StringType(), True),
        StructField("sizeEstimate", LongType(), True),
        StructField("payload", PAYLOAD_STRUCT, True),
    ]
)

THREADS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("snippet", StringType(), True),
        StructField("historyId", StringType(), True),
        StructField(
            "messages",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("threadId", StringType(), True),
                        StructField("labelIds", ArrayType(StringType()), True),
                        StructField("snippet", StringType(), True),
                        StructField("historyId", StringType(), True),
                        StructField("internalDate", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

LABELS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("messageListVisibility", StringType(), True),
        StructField("labelListVisibility", StringType(), True),
        StructField("type", StringType(), True),
        StructField("messagesTotal", LongType(), True),
        StructField("messagesUnread", LongType(), True),
        StructField("threadsTotal", LongType(), True),
        StructField("threadsUnread", LongType(), True),
        StructField(
            "color",
            StructType(
                [
                    StructField("textColor", StringType(), True),
                    StructField("backgroundColor", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

DRAFTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField(
            "message",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("threadId", StringType(), True),
                    StructField("labelIds", ArrayType(StringType()), True),
                    StructField("snippet", StringType(), True),
                    StructField("historyId", StringType(), True),
                    StructField("internalDate", StringType(), True),
                    StructField("sizeEstimate", LongType(), True),
                    StructField("payload", PAYLOAD_STRUCT, True),
                ]
            ),
            True,
        ),
    ]
)

PROFILE_SCHEMA = StructType(
    [
        StructField("emailAddress", StringType(), False),
        StructField("messagesTotal", LongType(), True),
        StructField("threadsTotal", LongType(), True),
        StructField("historyId", StringType(), True),
    ]
)

SETTINGS_SCHEMA = StructType(
    [
        StructField("emailAddress", StringType(), False),
        StructField(
            "autoForwarding",
            StructType(
                [
                    StructField("enabled", BooleanType(), True),
                    StructField("emailAddress", StringType(), True),
                    StructField("disposition", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "imap",
            StructType(
                [
                    StructField("enabled", BooleanType(), True),
                    StructField("autoExpunge", BooleanType(), True),
                    StructField("expungeBehavior", StringType(), True),
                    StructField("maxFolderSize", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "pop",
            StructType(
                [
                    StructField("accessWindow", StringType(), True),
                    StructField("disposition", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "language",
            StructType(
                [
                    StructField("displayLanguage", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "vacation",
            StructType(
                [
                    StructField("enableAutoReply", BooleanType(), True),
                    StructField("responseSubject", StringType(), True),
                    StructField("responseBodyPlainText", StringType(), True),
                    StructField("responseBodyHtml", StringType(), True),
                    StructField("restrictToContacts", BooleanType(), True),
                    StructField("restrictToDomain", BooleanType(), True),
                    StructField("startTime", StringType(), True),
                    StructField("endTime", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

FILTERS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField(
            "criteria",
            StructType(
                [
                    StructField("from", StringType(), True),
                    StructField("to", StringType(), True),
                    StructField("subject", StringType(), True),
                    StructField("query", StringType(), True),
                    StructField("negatedQuery", StringType(), True),
                    StructField("hasAttachment", BooleanType(), True),
                    StructField("excludeChats", BooleanType(), True),
                    StructField("size", LongType(), True),
                    StructField("sizeComparison", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "action",
            StructType(
                [
                    StructField("addLabelIds", ArrayType(StringType()), True),
                    StructField("removeLabelIds", ArrayType(StringType()), True),
                    StructField("forward", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

FORWARDING_ADDRESSES_SCHEMA = StructType(
    [
        StructField("forwardingEmail", StringType(), False),
        StructField("verificationStatus", StringType(), True),
    ]
)

SEND_AS_SCHEMA = StructType(
    [
        StructField("sendAsEmail", StringType(), False),
        StructField("displayName", StringType(), True),
        StructField("replyToAddress", StringType(), True),
        StructField("signature", StringType(), True),
        StructField("isPrimary", BooleanType(), True),
        StructField("isDefault", BooleanType(), True),
        StructField("treatAsAlias", BooleanType(), True),
        StructField("verificationStatus", StringType(), True),
        StructField(
            "smtpMsa",
            StructType(
                [
                    StructField("host", StringType(), True),
                    StructField("port", LongType(), True),
                    StructField("username", StringType(), True),
                    StructField("securityMode", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

DELEGATES_SCHEMA = StructType(
    [
        StructField("delegateEmail", StringType(), False),
        StructField("verificationStatus", StringType(), True),
    ]
)

# ─── Lookup Tables ────────────────────────────────────────────────────────────

TABLE_SCHEMAS = {
    "messages": MESSAGES_SCHEMA,
    "threads": THREADS_SCHEMA,
    "labels": LABELS_SCHEMA,
    "drafts": DRAFTS_SCHEMA,
    "profile": PROFILE_SCHEMA,
    "settings": SETTINGS_SCHEMA,
    "filters": FILTERS_SCHEMA,
    "forwarding_addresses": FORWARDING_ADDRESSES_SCHEMA,
    "send_as": SEND_AS_SCHEMA,
    "delegates": DELEGATES_SCHEMA,
}

TABLE_METADATA = {
    "messages": {
        "primary_keys": ["id"],
        "cursor_field": "historyId",
        "ingestion_type": "cdc",
    },
    "threads": {
        "primary_keys": ["id"],
        "cursor_field": "historyId",
        "ingestion_type": "cdc",
    },
    "labels": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "drafts": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "profile": {
        "primary_keys": ["emailAddress"],
        "ingestion_type": "snapshot",
    },
    "settings": {
        "primary_keys": ["emailAddress"],
        "ingestion_type": "snapshot",
    },
    "filters": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "forwarding_addresses": {
        "primary_keys": ["forwardingEmail"],
        "ingestion_type": "snapshot",
    },
    "send_as": {
        "primary_keys": ["sendAsEmail"],
        "ingestion_type": "snapshot",
    },
    "delegates": {
        "primary_keys": ["delegateEmail"],
        "ingestion_type": "snapshot",
    },
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
