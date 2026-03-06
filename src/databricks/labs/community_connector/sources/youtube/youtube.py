"""YouTube Data API v3 connector for Lakeflow Community Connectors.

Implements LakeflowConnect for ingesting channels, playlists, playlist_items,
videos, search, activities, comment_threads, subscriptions, and video_categories.
Supports API key (public data) or OAuth 2.0 (client_id, client_secret, refresh_token)
for private/mine data. Pagination uses pageToken/nextPageToken.
"""

import time
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.youtube import youtube_schemas

SUPPORTED_TABLES = youtube_schemas.SUPPORTED_TABLES
TABLE_SCHEMAS = youtube_schemas.TABLE_SCHEMAS
TABLE_METADATA = youtube_schemas.TABLE_METADATA

BASE_URL = "https://www.googleapis.com/youtube/v3"
TOKEN_URL = "https://oauth2.googleapis.com/token"
INITIAL_BACKOFF = 1.0
MAX_RETRIES = 5
RETRIABLE_STATUS_CODES = {429, 500, 503}
MAX_RESULTS_DEFAULT = 50


def _get_nested(d: dict, path: str, default: str | None = None) -> str | None:
    """Get a nested key like 'snippet.title'; return as string or default."""
    if not d or not path:
        return default
    parts = path.split(".")
    cur: Any = d
    for p in parts:
        cur = cur.get(p) if isinstance(cur, dict) else None
        if cur is None:
            return default
    return str(cur) if cur is not None else default


def _flatten_channel(item: dict) -> dict:
    """Flatten a channel resource to schema fields."""
    return {
        "id": item.get("id") or "",
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_description": _get_nested(item, "snippet.description"),
        "snippet_publishedAt": _get_nested(item, "snippet.publishedAt"),
        "snippet_thumbnails_default_url": _get_nested(
            item, "snippet.thumbnails.default.url"
        ),
        "snippet_defaultLanguage": _get_nested(item, "snippet.defaultLanguage"),
        "statistics_viewCount": _get_nested(item, "statistics.viewCount"),
        "statistics_subscriberCount": _get_nested(item, "statistics.subscriberCount"),
        "statistics_videoCount": _get_nested(item, "statistics.videoCount"),
        "contentDetails_relatedPlaylists_uploads": _get_nested(
            item, "contentDetails.relatedPlaylists.uploads"
        ),
    }


def _flatten_playlist(item: dict) -> dict:
    """Flatten a playlist resource to schema fields."""
    return {
        "id": item.get("id") or "",
        "snippet_publishedAt": _get_nested(item, "snippet.publishedAt"),
        "snippet_channelId": _get_nested(item, "snippet.channelId"),
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_description": _get_nested(item, "snippet.description"),
        "snippet_thumbnails_default_url": _get_nested(
            item, "snippet.thumbnails.default.url"
        ),
        "contentDetails_itemCount": _get_nested(item, "contentDetails.itemCount"),
    }


def _flatten_playlist_item(item: dict) -> dict:
    """Flatten a playlistItem resource to schema fields."""
    return {
        "id": item.get("id") or "",
        "snippet_publishedAt": _get_nested(item, "snippet.publishedAt"),
        "snippet_channelId": _get_nested(item, "snippet.channelId"),
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_description": _get_nested(item, "snippet.description"),
        "snippet_playlistId": _get_nested(item, "snippet.playlistId"),
        "snippet_position": _get_nested(item, "snippet.position"),
        "snippet_resourceId_videoId": _get_nested(
            item, "snippet.resourceId.videoId"
        ),
        "contentDetails_videoId": _get_nested(item, "contentDetails.videoId"),
    }


def _flatten_video(item: dict) -> dict:
    """Flatten a video resource to schema fields."""
    return {
        "id": item.get("id") or "",
        "snippet_publishedAt": _get_nested(item, "snippet.publishedAt"),
        "snippet_channelId": _get_nested(item, "snippet.channelId"),
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_description": _get_nested(item, "snippet.description"),
        "snippet_thumbnails_default_url": _get_nested(
            item, "snippet.thumbnails.default.url"
        ),
        "snippet_channelTitle": _get_nested(item, "snippet.channelTitle"),
        "snippet_categoryId": _get_nested(item, "snippet.categoryId"),
        "snippet_liveBroadcastContent": _get_nested(
            item, "snippet.liveBroadcastContent"
        ),
        "statistics_viewCount": _get_nested(item, "statistics.viewCount"),
        "statistics_likeCount": _get_nested(item, "statistics.likeCount"),
        "statistics_commentCount": _get_nested(item, "statistics.commentCount"),
        "contentDetails_duration": _get_nested(item, "contentDetails.duration"),
        "contentDetails_definition": _get_nested(item, "contentDetails.definition"),
    }


def _flatten_search_result(item: dict) -> dict:
    """Flatten a search result to schema fields."""
    id_obj = item.get("id") or {}
    return {
        "kind": item.get("kind"),
        "id_videoId": id_obj.get("videoId") if isinstance(id_obj, dict) else None,
        "id_channelId": id_obj.get("channelId") if isinstance(id_obj, dict) else None,
        "id_playlistId": id_obj.get("playlistId") if isinstance(id_obj, dict) else None,
        "snippet_publishedAt": _get_nested(item, "snippet.publishedAt"),
        "snippet_channelId": _get_nested(item, "snippet.channelId"),
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_description": _get_nested(item, "snippet.description"),
        "snippet_channelTitle": _get_nested(item, "snippet.channelTitle"),
    }


def _flatten_activity(item: dict) -> dict:
    """Flatten an activity resource to schema fields."""
    content = item.get("contentDetails") or {}
    upload = content.get("upload") or {}
    like = content.get("like") or {}
    return {
        "id": item.get("id") or "",
        "snippet_publishedAt": _get_nested(item, "snippet.publishedAt"),
        "snippet_channelId": _get_nested(item, "snippet.channelId"),
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_type": _get_nested(item, "snippet.type"),
        "snippet_channelTitle": _get_nested(item, "snippet.channelTitle"),
        "contentDetails_upload_videoId": upload.get("videoId") if isinstance(upload, dict) else None,
        "contentDetails_like_videoId": (
            like.get("resourceId", {}).get("videoId")
            if isinstance(like, dict) and isinstance(like.get("resourceId"), dict)
            else None
        ),
    }


def _flatten_comment_thread(item: dict) -> dict:
    """Flatten a commentThread resource to schema fields."""
    top = (item.get("snippet") or {}).get("topLevelComment") or {}
    top_snip = top.get("snippet") if isinstance(top, dict) else {}
    return {
        "id": item.get("id") or "",
        "snippet_videoId": _get_nested(item, "snippet.videoId"),
        "snippet_topLevelComment_id": top.get("id") if isinstance(top, dict) else None,
        "snippet_topLevelComment_snippet_textDisplay": (
            top_snip.get("textDisplay") if isinstance(top_snip, dict) else None
        ),
        "snippet_topLevelComment_snippet_authorDisplayName": (
            top_snip.get("authorDisplayName") if isinstance(top_snip, dict) else None
        ),
        "snippet_topLevelComment_snippet_publishedAt": (
            top_snip.get("publishedAt") if isinstance(top_snip, dict) else None
        ),
        "snippet_topLevelComment_snippet_likeCount": (
            str(top_snip.get("likeCount")) if isinstance(top_snip, dict) and top_snip.get("likeCount") is not None else None
        ),
        "snippet_canReply": str(item.get("snippet", {}).get("canReply")) if item.get("snippet", {}).get("canReply") is not None else None,
        "snippet_totalReplyCount": (
            str(item.get("snippet", {}).get("totalReplyCount"))
            if item.get("snippet", {}).get("totalReplyCount") is not None
            else None
        ),
    }


def _flatten_subscription(item: dict) -> dict:
    """Flatten a subscription resource to schema fields."""
    return {
        "id": item.get("id") or "",
        "snippet_publishedAt": _get_nested(item, "snippet.publishedAt"),
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_description": _get_nested(item, "snippet.description"),
        "snippet_resourceId_channelId": _get_nested(
            item, "snippet.resourceId.channelId"
        ),
        "snippet_thumbnails_default_url": _get_nested(
            item, "snippet.thumbnails.default.url"
        ),
    }


def _flatten_video_category(item: dict) -> dict:
    """Flatten a videoCategory resource to schema fields."""
    return {
        "id": item.get("id") or "",
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_assignable": (
            str(item.get("snippet", {}).get("assignable"))
            if item.get("snippet", {}).get("assignable") is not None
            else None
        ),
        "snippet_channelId": _get_nested(item, "snippet.channelId"),
    }


class YouTubeLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for YouTube Data API v3.

    Uses either api_key (for public data) or OAuth (client_id, client_secret,
    refresh_token) for private/mine data.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._api_key = (options.get("api_key") or "").strip()
        self._client_id = (options.get("client_id") or "").strip()
        self._client_secret = (options.get("client_secret") or "").strip()
        self._refresh_token = (options.get("refresh_token") or "").strip()
        if self._api_key:
            self._access_token = None
            self._token_expires_at = 0.0
        else:
            if not self._client_id or not self._client_secret or not self._refresh_token:
                raise ValueError(
                    "YouTube connector requires either 'api_key' or all of "
                    "'client_id', 'client_secret', 'refresh_token' in options"
                )
            self._access_token = None
            self._token_expires_at = 0.0
        self._session = requests.Session()

    def _get_access_token(self) -> str:
        """Return Bearer token from cache or refresh via OAuth."""
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token
        resp = self._session.post(
            TOKEN_URL,
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
                "grant_type": "refresh_token",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if resp.status_code == 401:
            raise ValueError(
                "YouTube OAuth returned 401. Check client_id, client_secret, "
                "and refresh_token; re-run OAuth flow if needed."
            )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)
        return self._access_token

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        """Issue GET with auth and retry on 429/5xx."""
        params = dict(params or {})
        if self._api_key:
            params["key"] = self._api_key
        url = BASE_URL.rstrip("/") + "/" + path.lstrip("/") if path else BASE_URL
        headers = {}
        if not self._api_key:
            headers["Authorization"] = f"Bearer {self._get_access_token()}"
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            resp = self._session.get(url, params=params, headers=headers, timeout=60)
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        return resp

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported: {SUPPORTED_TABLES}"
            )

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        readers = {
            "channels": self._read_channels,
            "playlists": self._read_playlists,
            "playlist_items": self._read_playlist_items,
            "videos": self._read_videos,
            "search": self._read_search,
            "activities": self._read_activities,
            "comment_threads": self._read_comment_threads,
            "subscriptions": self._read_subscriptions,
            "video_categories": self._read_video_categories,
        }
        return readers[table_name](start_offset or {}, table_options)

    def _read_channels(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        part = "snippet,statistics,contentDetails"
        params = {"part": part, "maxResults": MAX_RESULTS_DEFAULT}
        channel_ids = (table_options.get("channel_ids") or "").strip()
        if channel_ids:
            params["id"] = channel_ids
        elif table_options.get("for_username"):
            params["forUsername"] = table_options["for_username"]
        elif (table_options.get("mine") or "").lower() in ("true", "1", "yes"):
            params["mine"] = "true"
        else:
            raise ValueError("channels requires channel_ids, for_username, or mine=true")
        page_token = start_offset.get("pageToken")
        if page_token:
            params["pageToken"] = page_token
        resp = self._request("GET", "channels", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_channel(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _read_playlists(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        params = {"part": "snippet,contentDetails", "maxResults": MAX_RESULTS_DEFAULT}
        pl_ids = (table_options.get("playlist_ids") or "").strip()
        ch_id = (table_options.get("channel_id") or "").strip()
        if pl_ids:
            params["id"] = pl_ids
        elif ch_id:
            params["channelId"] = ch_id
        elif (table_options.get("mine") or "").lower() in ("true", "1", "yes"):
            params["mine"] = "true"
        else:
            raise ValueError("playlists requires playlist_ids, channel_id, or mine=true")
        page_token = start_offset.get("pageToken")
        if page_token:
            params["pageToken"] = page_token
        resp = self._request("GET", "playlists", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_playlist(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _read_playlist_items(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        playlist_id = (table_options.get("playlist_id") or "").strip()
        if not playlist_id:
            raise ValueError("playlist_items requires playlist_id in table_options")
        params = {
            "part": "snippet,contentDetails",
            "playlistId": playlist_id,
            "maxResults": MAX_RESULTS_DEFAULT,
        }
        page_token = start_offset.get("pageToken")
        if page_token:
            params["pageToken"] = page_token
        resp = self._request("GET", "playlistItems", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_playlist_item(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _read_videos(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        params = {"part": "snippet,statistics,contentDetails", "maxResults": MAX_RESULTS_DEFAULT}
        video_ids = (table_options.get("video_ids") or "").strip()
        if video_ids:
            params["id"] = video_ids
        elif (table_options.get("chart") or "").lower() == "mostpopular":
            params["chart"] = "mostPopular"
            if table_options.get("region_code"):
                params["regionCode"] = table_options["region_code"]
            if table_options.get("video_category_id"):
                params["videoCategoryId"] = table_options["video_category_id"]
            page_token = start_offset.get("pageToken")
            if page_token:
                params["pageToken"] = page_token
        else:
            raise ValueError("videos requires video_ids or chart=mostPopular")
        resp = self._request("GET", "videos", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_video(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _read_search(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        q = (table_options.get("q") or "").strip()
        if not q:
            raise ValueError("search requires q (query) in table_options")
        params = {"part": "snippet", "q": q, "maxResults": MAX_RESULTS_DEFAULT}
        if table_options.get("type"):
            params["type"] = table_options["type"]
        if table_options.get("channel_id"):
            params["channelId"] = table_options["channel_id"]
        if table_options.get("published_after"):
            params["publishedAfter"] = table_options["published_after"]
        if table_options.get("order"):
            params["order"] = table_options["order"]
        page_token = start_offset.get("pageToken")
        if page_token:
            params["pageToken"] = page_token
        resp = self._request("GET", "search", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_search_result(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _read_activities(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        params = {"part": "snippet,contentDetails", "maxResults": MAX_RESULTS_DEFAULT}
        ch_id = (table_options.get("channel_id") or "").strip()
        if ch_id:
            params["channelId"] = ch_id
        elif (table_options.get("mine") or "").lower() in ("true", "1", "yes"):
            params["mine"] = "true"
        else:
            raise ValueError("activities requires channel_id or mine=true")
        if table_options.get("published_after"):
            params["publishedAfter"] = table_options["published_after"]
        page_token = start_offset.get("pageToken")
        if page_token:
            params["pageToken"] = page_token
        resp = self._request("GET", "activities", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_activity(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _read_comment_threads(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        video_id = (table_options.get("video_id") or "").strip()
        channel_id = (table_options.get("channel_id") or "").strip()
        if video_id:
            params = {"part": "snippet", "videoId": video_id, "maxResults": min(100, MAX_RESULTS_DEFAULT)}
        elif channel_id:
            params = {"part": "snippet", "allThreadsRelatedToChannelId": channel_id, "maxResults": min(100, MAX_RESULTS_DEFAULT)}
        else:
            raise ValueError("comment_threads requires video_id or channel_id")
        page_token = start_offset.get("pageToken")
        if page_token:
            params["pageToken"] = page_token
        resp = self._request("GET", "commentThreads", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_comment_thread(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _read_subscriptions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        params = {"part": "snippet", "maxResults": MAX_RESULTS_DEFAULT}
        ch_id = (table_options.get("channel_id") or "").strip()
        if ch_id:
            params["channelId"] = ch_id
        elif (table_options.get("mine") or "").lower() in ("true", "1", "yes"):
            params["mine"] = "true"
        else:
            raise ValueError("subscriptions requires channel_id or mine=true")
        page_token = start_offset.get("pageToken")
        if page_token:
            params["pageToken"] = page_token
        resp = self._request("GET", "subscriptions", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_subscription(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _read_video_categories(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        params = {"part": "snippet", "maxResults": MAX_RESULTS_DEFAULT}
        if table_options.get("region_code"):
            params["regionCode"] = table_options["region_code"]
        page_token = start_offset.get("pageToken")
        if page_token:
            params["pageToken"] = page_token
        resp = self._request("GET", "videoCategories", params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        records = [_flatten_video_category(i) for i in items]
        next_token = data.get("nextPageToken")
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset
