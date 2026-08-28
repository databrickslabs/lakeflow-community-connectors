"""YouTube Data API v3 source connector.

This package provides the LakeflowConnect implementation for ingesting
YouTube channels, playlists, videos, search results, activities,
comment threads, subscriptions, and video categories.
Supports API key (public data) or OAuth (client_id, client_secret, refresh_token).
"""

from databricks.labs.community_connector.sources.youtube.youtube import (
    YouTubeLakeflowConnect,
)

__all__ = ["YouTubeLakeflowConnect"]
