"""Schemas and metadata for the YouTube Data API v3 connector.

Defines Spark StructTypes and table metadata (primary keys, cursor field,
ingestion type) for all connector tables. Fields are flattened from the API
nested structure (snippet, statistics, contentDetails) into top-level columns.
All fields are StringType to match API response types (e.g. viewCount as string).
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
)

# ---------------------------------------------------------------------------
# Supported tables (Tier 1–3 per plan)
# ---------------------------------------------------------------------------
SUPPORTED_TABLES = [
    "channels",
    "playlists",
    "playlist_items",
    "videos",
    "search",
    "activities",
    "comment_threads",
    "subscriptions",
    "video_categories",
]

# ---------------------------------------------------------------------------
# Tier 1 – Core content
# ---------------------------------------------------------------------------

# channels: GET /channels (snippet, statistics, contentDetails)
CHANNELS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("snippet_title", StringType(), nullable=True),
        StructField("snippet_description", StringType(), nullable=True),
        StructField("snippet_publishedAt", StringType(), nullable=True),
        StructField("snippet_thumbnails_default_url", StringType(), nullable=True),
        StructField("snippet_defaultLanguage", StringType(), nullable=True),
        StructField("statistics_viewCount", StringType(), nullable=True),
        StructField("statistics_subscriberCount", StringType(), nullable=True),
        StructField("statistics_videoCount", StringType(), nullable=True),
        StructField("contentDetails_relatedPlaylists_uploads", StringType(), nullable=True),
    ]
)

# playlists: GET /playlists (snippet, contentDetails)
PLAYLISTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("snippet_publishedAt", StringType(), nullable=True),
        StructField("snippet_channelId", StringType(), nullable=True),
        StructField("snippet_title", StringType(), nullable=True),
        StructField("snippet_description", StringType(), nullable=True),
        StructField("snippet_thumbnails_default_url", StringType(), nullable=True),
        StructField("contentDetails_itemCount", StringType(), nullable=True),
    ]
)

# playlist_items: GET /playlistItems (snippet, contentDetails)
PLAYLIST_ITEMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("snippet_publishedAt", StringType(), nullable=True),
        StructField("snippet_channelId", StringType(), nullable=True),
        StructField("snippet_title", StringType(), nullable=True),
        StructField("snippet_description", StringType(), nullable=True),
        StructField("snippet_playlistId", StringType(), nullable=True),
        StructField("snippet_position", StringType(), nullable=True),
        StructField("snippet_resourceId_videoId", StringType(), nullable=True),
        StructField("contentDetails_videoId", StringType(), nullable=True),
    ]
)

# videos: GET /videos (snippet, statistics, contentDetails)
VIDEOS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("snippet_publishedAt", StringType(), nullable=True),
        StructField("snippet_channelId", StringType(), nullable=True),
        StructField("snippet_title", StringType(), nullable=True),
        StructField("snippet_description", StringType(), nullable=True),
        StructField("snippet_thumbnails_default_url", StringType(), nullable=True),
        StructField("snippet_channelTitle", StringType(), nullable=True),
        StructField("snippet_categoryId", StringType(), nullable=True),
        StructField("snippet_liveBroadcastContent", StringType(), nullable=True),
        StructField("statistics_viewCount", StringType(), nullable=True),
        StructField("statistics_likeCount", StringType(), nullable=True),
        StructField("statistics_commentCount", StringType(), nullable=True),
        StructField("contentDetails_duration", StringType(), nullable=True),
        StructField("contentDetails_definition", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Tier 2 – Discovery and engagement
# ---------------------------------------------------------------------------

# search: GET /search (returns id.videoId, id.channelId, id.playlistId + snippet)
# search_query + result_index: unique across runs (each run emits 0,1,2,... so query disambiguates)
SEARCH_SCHEMA = StructType(
    [
        StructField("search_query", StringType(), nullable=False),
        StructField("result_index", StringType(), nullable=False),
        StructField("kind", StringType(), nullable=True),
        StructField("id_videoId", StringType(), nullable=True),
        StructField("id_channelId", StringType(), nullable=True),
        StructField("id_playlistId", StringType(), nullable=True),
        StructField("snippet_publishedAt", StringType(), nullable=True),
        StructField("snippet_channelId", StringType(), nullable=True),
        StructField("snippet_title", StringType(), nullable=True),
        StructField("snippet_description", StringType(), nullable=True),
        StructField("snippet_channelTitle", StringType(), nullable=True),
    ]
)

# activities: GET /activities (snippet, contentDetails)
ACTIVITIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("snippet_publishedAt", StringType(), nullable=True),
        StructField("snippet_channelId", StringType(), nullable=True),
        StructField("snippet_title", StringType(), nullable=True),
        StructField("snippet_type", StringType(), nullable=True),
        StructField("snippet_channelTitle", StringType(), nullable=True),
        StructField("contentDetails_upload_videoId", StringType(), nullable=True),
        StructField("contentDetails_like_videoId", StringType(), nullable=True),
    ]
)

# comment_threads: GET /commentThreads (snippet with topLevelComment)
COMMENT_THREADS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("snippet_videoId", StringType(), nullable=True),
        StructField("snippet_topLevelComment_id", StringType(), nullable=True),
        StructField("snippet_topLevelComment_snippet_textDisplay", StringType(), nullable=True),
        StructField("snippet_topLevelComment_snippet_authorDisplayName", StringType(), nullable=True),
        StructField("snippet_topLevelComment_snippet_publishedAt", StringType(), nullable=True),
        StructField("snippet_topLevelComment_snippet_likeCount", StringType(), nullable=True),
        StructField("snippet_canReply", StringType(), nullable=True),
        StructField("snippet_totalReplyCount", StringType(), nullable=True),
    ]
)

# subscriptions: GET /subscriptions (snippet)
SUBSCRIPTIONS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("snippet_publishedAt", StringType(), nullable=True),
        StructField("snippet_title", StringType(), nullable=True),
        StructField("snippet_description", StringType(), nullable=True),
        StructField("snippet_resourceId_channelId", StringType(), nullable=True),
        StructField("snippet_thumbnails_default_url", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Tier 3 – Reference
# ---------------------------------------------------------------------------

# video_categories: GET /videoCategories (snippet)
VIDEO_CATEGORIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("snippet_title", StringType(), nullable=True),
        StructField("snippet_assignable", StringType(), nullable=True),
        StructField("snippet_channelId", StringType(), nullable=True),
    ]
)

TABLE_SCHEMAS = {
    "channels": CHANNELS_SCHEMA,
    "playlists": PLAYLISTS_SCHEMA,
    "playlist_items": PLAYLIST_ITEMS_SCHEMA,
    "videos": VIDEOS_SCHEMA,
    "search": SEARCH_SCHEMA,
    "activities": ACTIVITIES_SCHEMA,
    "comment_threads": COMMENT_THREADS_SCHEMA,
    "subscriptions": SUBSCRIPTIONS_SCHEMA,
    "video_categories": VIDEO_CATEGORIES_SCHEMA,
}

# ---------------------------------------------------------------------------
# Table metadata (primary_keys, cursor_field, ingestion_type)
# ---------------------------------------------------------------------------
TABLE_METADATA = {
    "channels": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "playlists": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "playlist_items": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "videos": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "search": {
        "primary_keys": ["search_query", "result_index"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "activities": {
        "primary_keys": ["id"],
        "cursor_field": "snippet_publishedAt",
        "ingestion_type": "snapshot",
    },
    "comment_threads": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "subscriptions": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "video_categories": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}
