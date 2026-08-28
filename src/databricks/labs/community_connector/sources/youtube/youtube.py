"""YouTube Data API v3 connector for Lakeflow Community Connectors.

Implements LakeflowConnect for ingesting channels, playlists, playlist_items,
videos, search, activities, comment_threads, subscriptions, and video_categories.
Supports API key (public data) or OAuth 2.0 for private/mine data. OAuth accepts
either a UC-injected access_token (u2m / u2m_per_user) or legacy refresh-token
credentials. All tables are snapshot: each read drains API pages internally
and returns ``None`` offset for framework termination.
"""

import time
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.youtube.youtube_schemas import (
    SUPPORTED_TABLES,
    TABLE_SCHEMAS,
    TABLE_METADATA,
)

BASE_URL = "https://www.googleapis.com/youtube/v3"
TOKEN_URL = "https://oauth2.googleapis.com/token"
INITIAL_BACKOFF = 1.0
MAX_RETRIES = 5
RETRIABLE_STATUS_CODES = {429, 500, 503}
MAX_RESULTS_DEFAULT = 50
# commentThreads and comments support 1-100; other list endpoints support 1-50
MAX_RESULTS_COMMENT_THREADS = 100


def _max_results(
    table_options: dict[str, str],
    default: int = MAX_RESULTS_DEFAULT,
    cap: int = 50,
) -> int:
    """Parse max_results from table_options; clamp to [1, cap]. Used for pagination page size."""
    raw = table_options.get("max_results") or ""
    if not raw:
        return min(default, cap)
    try:
        n = int(raw.strip())
        return max(1, min(n, cap))
    except ValueError:
        return min(default, cap)


def _parse_max_pages(
    table_options: dict[str, str], default: int = 100, cap: int = 1000
) -> int:
    """Parse max_pages from table_options; clamp to [1, cap]."""
    raw = (table_options.get("max_pages") or "").strip()
    if not raw:
        return default
    try:
        return max(1, min(int(raw), cap))
    except ValueError:
        return default


def _chunked_ids(raw_ids: str, size: int = 50) -> list[list[str]]:
    """Split comma-separated IDs into de-duplicated fixed-size batches."""
    ids = [item.strip() for item in raw_ids.split(",") if item.strip()]
    unique_ids = list(dict.fromkeys(ids))
    return [unique_ids[i : i + size] for i in range(0, len(unique_ids), size)]


def _retry_wait_seconds(resp: requests.Response, backoff: float) -> float:
    """Seconds to sleep before retry; honor Retry-After when present."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except (TypeError, ValueError):
            pass
    return backoff


def _youtube_error_fields(resp: requests.Response) -> tuple[str, str]:
    """Parse Google API error JSON into (reason, message)."""
    try:
        err = resp.json().get("error", {})
        errors = err.get("errors") or [{}]
        reason = (errors[0].get("reason") or "") if errors else ""
        message = err.get("message") or reason or resp.reason
        return str(reason), str(message)[:300]
    except Exception:
        return "", (resp.text or resp.reason or "unknown error")[:300]


def _raise_for_youtube_api_error(
    resp: requests.Response,
    path: str,
    *,
    detail: str | None = None,
) -> None:
    """Raise ValueError with actionable text for common YouTube API failures."""
    if resp.ok:
        return
    reason, message = _youtube_error_fields(resp)
    status = resp.status_code

    if status == 401:
        raise ValueError(
            f"YouTube API {path} returned 401 Unauthorized. "
            "Verify API key or OAuth credentials. OAuth is required for mine=true "
            "and other private data. "
            f"Detail: {message}"
        )
    if status == 403:
        if reason == "quotaExceeded":
            raise ValueError(
                f"YouTube API {path} returned 403: quota exceeded. "
                "Reduce sync frequency (search costs 100 units per request) or "
                "request a quota increase in Google Cloud Console. "
                f"Detail: {message}"
            )
        if path == "commentThreads":
            ctx = f" ({detail})" if detail else ""
            raise ValueError(
                f"comment_threads returned 403 Forbidden{ctx}. Comments may be disabled "
                "for this video or channel, or access may be restricted. Prefer video_id "
                "over channel_id; see README. "
                f"API reason: {reason or message}"
            )
        raise ValueError(
            f"YouTube API {path} returned 403 Forbidden. "
            f"Reason: {reason or message}. "
            "Check credentials, API enablement, and table options (e.g. OAuth for mine=true)."
        )
    if status == 429:
        raise ValueError(
            f"YouTube API {path} returned 429 Too Many Requests after retries. "
            f"Detail: {message}"
        )
    raise ValueError(
        f"YouTube API {path} returned HTTP {status}. Detail: {message}"
    )


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


def _flatten_search_result(item: dict, result_index: str, search_query: str) -> dict:
    """Flatten a search result. PK is (search_query, result_index) with stable position index."""
    id_obj = item.get("id") or {}
    return {
        "search_query": search_query,
        "result_index": result_index,
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
        "contentDetails_upload_videoId": (
            upload.get("videoId") if isinstance(upload, dict) else None
        ),
        "contentDetails_like_videoId": (
            like.get("resourceId", {}).get("videoId")
            if isinstance(like, dict) and isinstance(like.get("resourceId"), dict)
            else None
        ),
    }


def _flatten_comment_thread(item: dict) -> dict:
    """Flatten a commentThread resource to schema fields."""
    snip = item.get("snippet") or {}
    top = snip.get("topLevelComment") or {}
    top_snip = (top.get("snippet") if isinstance(top, dict) else None) or {}
    return {
        "id": item.get("id") or "",
        "snippet_videoId": _get_nested(item, "snippet.videoId"),
        "snippet_topLevelComment_id": top.get("id") if isinstance(top, dict) else None,
        "snippet_topLevelComment_snippet_textDisplay": top_snip.get("textDisplay"),
        "snippet_topLevelComment_snippet_authorDisplayName": top_snip.get("authorDisplayName"),
        "snippet_topLevelComment_snippet_publishedAt": top_snip.get("publishedAt"),
        "snippet_topLevelComment_snippet_likeCount": (
            str(top_snip.get("likeCount")) if top_snip.get("likeCount") is not None else None
        ),
        "snippet_canReply": (
            str(snip.get("canReply")) if snip.get("canReply") is not None else None
        ),
        "snippet_totalReplyCount": (
            str(snip.get("totalReplyCount")) if snip.get("totalReplyCount") is not None else None
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
    snip = item.get("snippet") or {}
    return {
        "id": item.get("id") or "",
        "snippet_title": _get_nested(item, "snippet.title"),
        "snippet_assignable": (
            str(snip.get("assignable")) if snip.get("assignable") is not None else None
        ),
        "snippet_channelId": _get_nested(item, "snippet.channelId"),
    }


class YouTubeLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for YouTube Data API v3.

    Uses either api_key (for public data) or OAuth for private/mine data.
    OAuth supports a UC-injected access_token (preferred) or in-code refresh.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._api_key = (options.get("api_key") or "").strip()
        self._injected_access_token = (options.get("access_token") or "").strip() or None
        self._client_id = (options.get("client_id") or "").strip()
        self._client_secret = (options.get("client_secret") or "").strip()
        self._refresh_token = (options.get("refresh_token") or "").strip()
        has_refresh_oauth = bool(
            self._refresh_token and self._client_id and self._client_secret
        )
        has_injected_oauth = bool(self._injected_access_token)
        has_oauth = has_refresh_oauth or has_injected_oauth

        if self._api_key and has_oauth:
            raise ValueError(
                "YouTube connector requires either 'api_key' or OAuth credentials "
                "in options, not both"
            )
        if not self._api_key and not has_oauth:
            raise ValueError(
                "YouTube connector requires either 'api_key' (public data) or an "
                "OAuth credential: 'access_token' (the Unity Catalog COMMUNITY "
                "connection's u2m / u2m_per_user OAuth flow injects it at query "
                "time) or 'refresh_token' plus 'client_id' and 'client_secret' "
                "(in-code refresh for older connections)."
            )
        if self._refresh_token and not (self._client_id and self._client_secret):
            raise ValueError(
                "YouTube refresh-token auth requires 'client_id' and "
                "'client_secret' alongside 'refresh_token' in options."
            )
        self._refreshed_token: str | None = None
        self._token_expires_at = 0.0
        self._session = requests.Session()

    def _get_access_token(self) -> str:
        """Return Bearer token: opaque UC token or refresh-exchange cache."""
        if self._injected_access_token:
            return self._injected_access_token
        if self._refreshed_token and time.time() < self._token_expires_at - 60:
            return self._refreshed_token
        resp = self._session.post(
            TOKEN_URL,
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
                "grant_type": "refresh_token",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=60,
        )
        if resp.status_code == 401:
            raise ValueError(
                "YouTube OAuth returned 401. Check client_id, client_secret, "
                "and refresh_token; re-run OAuth flow if needed."
            )
        if resp.status_code == 400:
            try:
                if resp.json().get("error") == "invalid_grant":
                    raise ValueError(
                        "YouTube OAuth refresh failed (invalid_grant). The refresh token "
                        "may be revoked or expired. Re-run authenticate.py and update "
                        "the connection."
                    )
            except ValueError:
                raise
            except Exception:
                pass
        resp.raise_for_status()
        data = resp.json()
        self._refreshed_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)
        return self._refreshed_token

    def _request(self, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        """Issue GET with auth and retry on 429/5xx; honor Retry-After when present."""
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
                time.sleep(_retry_wait_seconds(resp, backoff))
                backoff *= 2
        return resp

    def _ensure_ok(
        self, resp: requests.Response, path: str, *, detail: str | None = None
    ) -> None:
        """Raise ValueError with a clear message when the API response is an error."""
        _raise_for_youtube_api_error(resp, path, detail=detail)

    def _drain_paginated_list(
        self,
        path: str,
        base_params: dict[str, Any],
        flatten_fn,
        table_options: dict[str, str],
        *,
        max_results_cap: int = 50,
        max_results_default: int = MAX_RESULTS_DEFAULT,
        max_pages_default: int = 100,
        max_pages_cap: int = 1000,
        detail: str | None = None,
    ) -> list[dict]:
        """Fetch all API pages in one read_table call; pageToken stays internal."""
        max_pages = _parse_max_pages(table_options, max_pages_default, max_pages_cap)
        all_records: list[dict] = []
        page_token: str | None = None
        for _ in range(max_pages):
            params = dict(base_params)
            params["maxResults"] = _max_results(
                table_options, default=max_results_default, cap=max_results_cap
            )
            if page_token:
                params["pageToken"] = page_token
            resp = self._request(path, params=params)
            self._ensure_ok(resp, path, detail=detail)
            data = resp.json()
            items = data.get("items") or []
            all_records.extend(flatten_fn(i) for i in items)
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return all_records

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
    ) -> tuple[Iterator[dict], dict | None]:
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
    ) -> tuple[Iterator[dict], dict | None]:
        params: dict[str, Any] = {"part": "snippet,statistics,contentDetails"}
        channel_ids = (table_options.get("channel_ids") or "").strip()
        if channel_ids:
            params["id"] = channel_ids
        elif table_options.get("for_username"):
            params["forUsername"] = table_options["for_username"]
        elif (table_options.get("mine") or "").lower() in ("true", "1", "yes"):
            params["mine"] = "true"
        else:
            raise ValueError("channels requires channel_ids, for_username, or mine=true")
        records = self._drain_paginated_list(
            "channels", params, _flatten_channel, table_options
        )
        return iter(records), None

    def _read_playlists(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        params: dict[str, Any] = {"part": "snippet,contentDetails"}
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
        records = self._drain_paginated_list(
            "playlists", params, _flatten_playlist, table_options
        )
        return iter(records), None

    def _read_playlist_items(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        playlist_id = (table_options.get("playlist_id") or "").strip()
        if not playlist_id:
            raise ValueError("playlist_items requires playlist_id in table_options")
        params: dict[str, Any] = {
            "part": "snippet,contentDetails",
            "playlistId": playlist_id,
        }
        records = self._drain_paginated_list(
            "playlistItems", params, _flatten_playlist_item, table_options
        )
        return iter(records), None

    def _read_videos(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        video_ids = (table_options.get("video_ids") or "").strip()
        use_chart = (table_options.get("chart") or "").lower() == "mostpopular"
        if video_ids:
            id_batches = _chunked_ids(video_ids, size=50)
            if not id_batches:
                raise ValueError("videos requires non-empty video_ids")
            records: list[dict] = []
            for id_batch in id_batches:
                params = {
                    "part": "snippet,statistics,contentDetails",
                    "id": ",".join(id_batch),
                }
                resp = self._request("videos", params=params)
                self._ensure_ok(resp, "videos")
                items = resp.json().get("items") or []
                records.extend(_flatten_video(i) for i in items)
            return iter(records), None
        if use_chart:
            params: dict[str, Any] = {
                "part": "snippet,statistics,contentDetails",
                "chart": "mostPopular",
            }
            if table_options.get("region_code"):
                params["regionCode"] = table_options["region_code"]
            if table_options.get("video_category_id"):
                params["videoCategoryId"] = table_options["video_category_id"]
            records = self._drain_paginated_list(
                "videos",
                params,
                _flatten_video,
                table_options,
                max_pages_default=20,
                max_pages_cap=100,
            )
            return iter(records), None
        raise ValueError("videos requires video_ids or chart=mostPopular")

    def _read_search(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        q = (table_options.get("q") or "").strip()
        if not q:
            raise ValueError("search requires q (query) in table_options")
        params: dict[str, Any] = {"part": "snippet", "q": q}
        if table_options.get("type"):
            params["type"] = table_options["type"]
        if table_options.get("channel_id"):
            params["channelId"] = table_options["channel_id"]
        if table_options.get("published_after"):
            params["publishedAfter"] = table_options["published_after"]
        if table_options.get("order"):
            params["order"] = table_options["order"]
        max_pages = _parse_max_pages(table_options, default=10, cap=100)
        all_records: list[dict] = []
        page_token: str | None = None
        position = 0
        for _ in range(max_pages):
            p = dict(params)
            p["maxResults"] = _max_results(table_options, cap=50)
            if page_token:
                p["pageToken"] = page_token
            resp = self._request("search", params=p)
            self._ensure_ok(resp, "search")
            data = resp.json()
            for it in data.get("items") or []:
                all_records.append(_flatten_search_result(it, str(position), q))
                position += 1
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return iter(all_records), None

    def _read_activities(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        params: dict[str, Any] = {"part": "snippet,contentDetails"}
        ch_id = (table_options.get("channel_id") or "").strip()
        if ch_id:
            params["channelId"] = ch_id
        elif (table_options.get("mine") or "").lower() in ("true", "1", "yes"):
            params["mine"] = "true"
        else:
            raise ValueError("activities requires channel_id or mine=true")
        if table_options.get("published_after"):
            params["publishedAfter"] = table_options["published_after"]
        records = self._drain_paginated_list(
            "activities", params, _flatten_activity, table_options
        )
        return iter(records), None

    def _read_comment_threads(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        video_id = (table_options.get("video_id") or "").strip()
        channel_id = (table_options.get("channel_id") or "").strip()
        if video_id:
            params: dict[str, Any] = {"part": "snippet", "videoId": video_id}
        elif channel_id:
            params = {"part": "snippet", "allThreadsRelatedToChannelId": channel_id}
        else:
            raise ValueError("comment_threads requires video_id or channel_id")
        ident = f"video_id={video_id}" if video_id else f"channel_id={channel_id}"
        records = self._drain_paginated_list(
            "commentThreads",
            params,
            _flatten_comment_thread,
            table_options,
            max_results_cap=100,
            max_results_default=MAX_RESULTS_COMMENT_THREADS,
            detail=ident,
        )
        return iter(records), None

    def _read_subscriptions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        params: dict[str, Any] = {"part": "snippet"}
        ch_id = (table_options.get("channel_id") or "").strip()
        if ch_id:
            params["channelId"] = ch_id
        elif (table_options.get("mine") or "").lower() in ("true", "1", "yes"):
            params["mine"] = "true"
        else:
            raise ValueError("subscriptions requires channel_id or mine=true")
        records = self._drain_paginated_list(
            "subscriptions", params, _flatten_subscription, table_options
        )
        return iter(records), None

    def _read_video_categories(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        region_code = (table_options.get("region_code") or "").strip()
        if not region_code:
            raise ValueError(
                "video_categories requires region_code in table_options "
                "(ISO 3166-1 alpha-2, e.g. US)"
            )
        params: dict[str, Any] = {"part": "snippet", "regionCode": region_code}
        records = self._drain_paginated_list(
            "videoCategories",
            params,
            _flatten_video_category,
            table_options,
            max_pages_default=20,
            max_pages_cap=100,
        )
        return iter(records), None
