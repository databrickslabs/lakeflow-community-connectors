# **YouTube API Documentation**

## **Authorization**

- **Supported methods**
  - **API key**: Pass as query parameter `key=YOUR_API_KEY`. Suitable for public data (list by id, search, charts). No user identity.
  - **OAuth 2.0**: Bearer token in `Authorization: Bearer ACCESS_TOKEN`. Required for private/mine data (`mine=true`, `myRating`, subscriptions, activities for the authenticated user, etc.).

- **Parameters and placement**
  - API key: query parameter `key` on every request.
  - OAuth: obtain access token via Google OAuth 2.0 (e.g. authorization code or refresh flow); send as header `Authorization: Bearer <access_token>`.

- **Connector preference**: For connector development, prefer **OAuth 2.0** with `client_id`, `client_secret`, and `refresh_token`. The connector stores these and exchanges the refresh token for an access token at runtime; it does **not** run user-facing OAuth consent flows. Use scope `https://www.googleapis.com/auth/youtube.readonly` for read-only ingestion.

- **Example (API key)**  
  `GET https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id=UC_x5XG1OV2P6uZZ5FSM9Ttw&key=YOUR_API_KEY`

- **Example (OAuth)**  
  `GET https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&mine=true`  
  Header: `Authorization: Bearer ya29.xxx`

---

## **Object List**

- The YouTube Data API v3 exposes **list** methods per resource. There is no single “catalog” endpoint; the set of objects (tables) is fixed and defined by the API.

- **Objects (tables) for this connector**

| Object (table)     | API resource      | List endpoint              | Notes |
|--------------------|-------------------|----------------------------|-------|
| channels           | channel           | `channels.list`            | By id, forHandle, forUsername, or mine |
| playlists          | playlist          | `playlists.list`           | By id, channelId, or mine |
| playlist_items     | playlistItem      | `playlistItems.list`       | By playlistId or id |
| videos             | video             | `videos.list`             | By id, chart, or myRating |
| search             | searchResult      | `search.list`              | By q (+ filters); returns video/channel/playlist refs |
| activities         | activity          | `activities.list`          | By channelId or mine |
| comment_threads     | commentThread     | `commentThreads.list`      | By videoId, allThreadsRelatedToChannelId, or id |
| subscriptions      | subscription      | `subscriptions.list`      | By channelId, id, or mine |
| video_categories    | videoCategory     | `videoCategories.list`    | By id or regionCode |

- **Layering**: `playlist_items` are under a playlist (filter by `playlistId`). `comment_threads` are under a video (`videoId`) or channel (`allThreadsRelatedToChannelId`). Other tables are top-level or filtered by channel/user.

- **Retrieving the list**: Object names are static; no API call returns “all table names.” To enumerate e.g. a user’s playlists use `playlists.list` with `mine=true` (OAuth).

---

## **Object Schema**

- Schemas are defined by the API resource structures. Request the desired **parts** to get the corresponding fields in the response.

- **Channel**  
  Parts: `id`, `snippet`, `contentDetails`, `statistics`, `topicDetails`, `status`, `brandingSettings`, `auditDetails`, `contentOwnerDetails`, `localizations`.  
  Key nested objects: `snippet` (title, description, publishedAt, thumbnails, defaultLanguage, country), `contentDetails.relatedPlaylists` (likes, uploads, favorites), `statistics` (viewCount, subscriberCount, videoCount).

- **Playlist**  
  Parts: `id`, `snippet`, `status`, `contentDetails`, `player`, `localizations`.  
  Key: `snippet` (publishedAt, channelId, title, description, thumbnails), `contentDetails.itemCount`.

- **PlaylistItem**  
  Parts: `id`, `snippet`, `contentDetails`, `status`.  
  Key: `snippet` (publishedAt, channelId, title, description, playlistId, position, resourceId.videoId), `contentDetails.videoId`, `contentDetails.videoPublishedAt`.

- **Video**  
  Parts: `id`, `snippet`, `contentDetails`, `status`, `statistics`, `topicDetails`, `recordingDetails`, `liveStreamingDetails`, `localizations`, etc.  
  Key: `snippet` (publishedAt, channelId, title, description, tags, categoryId, liveBroadcastContent), `contentDetails` (duration, dimension, definition, caption), `statistics` (viewCount, likeCount, commentCount), `status` (privacyStatus, uploadStatus).

- **Search result**  
  Part: `snippet` (required).  
  Each item has `id.kind`, `id.videoId` / `id.channelId` / `id.playlistId`, and `snippet` (publishedAt, channelId, title, description, thumbnails, channelTitle, liveBroadcastContent). No separate “search” resource schema; shape is the searchResult structure.

- **Activity**  
  Parts: `id`, `snippet`, `contentDetails`.  
  Key: `snippet` (publishedAt, channelId, title, type, groupId), `contentDetails` (upload.videoId, like.resourceId, subscription.resourceId.channelId, playlistItem.*, etc. by type).

- **CommentThread**  
  Parts: `id`, `snippet`, `replies`.  
  Key: `snippet` (topLevelComment, totalReplyCount, canReply, videoId), `snippet.topLevelComment.snippet` (authorDisplayName, textDisplay, publishedAt, likeCount), `replies.comments[]` (optional, for replies).

- **Subscription**  
  Parts: `id`, `snippet`, `contentDetails`, `subscriberSnippet`.  
  Key: `snippet` (publishedAt, channelId, title, description, resourceId.channelId, channelTitle), `contentDetails` (totalItemCount, newItemCount).

- **VideoCategory**  
  Part: `snippet` (only part; required).  
  Fields: `id`, `snippet.title`, `snippet.assignable`, `snippet.channelId`.

- **Example request (channel schema)**  
  `GET https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics,contentDetails&id=UC_x5XG1OV2P6uZZ5FSM9Ttw&key=KEY`  
  Response: `{ "kind": "youtube#channelListResponse", "etag": "...", "items": [ { "id": "...", "snippet": { "title": "...", ... }, "statistics": { "viewCount": "...", ... }, "contentDetails": { "relatedPlaylists": { "uploads": "..." } } } ] }`

---

## **Get Object Primary Keys**

- Primary keys are fixed per resource; the API does not expose a separate “primary key” endpoint.

| Object (table)     | Primary key |
|--------------------|-------------|
| channels           | `id`        |
| playlists          | `id`        |
| playlist_items     | `id`        |
| videos             | `id`        |
| search             | Composite: `id.kind` + `id.videoId` or `id.channelId` or `id.playlistId` (one set per result) |
| activities         | `id`        |
| comment_threads    | `id`        |
| subscriptions      | `id`        |
| video_categories    | `id`        |

- **Example**: Channels response includes `items[].id`; that `id` is the channel primary key.

---

## **Object's ingestion type**

| Object (table)   | Ingestion type | Notes |
|------------------|----------------|-------|
| channels         | snapshot       | No change feed; full refresh by id or mine |
| playlists        | snapshot       | Full refresh by id, channelId, or mine |
| playlist_items   | snapshot       | Full refresh per playlistId (or by id); order can use `snippet.position` |
| videos           | snapshot       | Full refresh by id or chart; myRating is user-specific snapshot |
| search           | snapshot       | Query-driven; no incremental cursor; use publishedAfter/publishedBefore for time window |
| activities       | append / cdc   | Use `publishedAfter` as cursor; activities are append-only events |
| comment_threads  | snapshot       | Per videoId or channel; pageToken for pagination only |
| subscriptions    | snapshot       | Full refresh by channelId, id, or mine |
| video_categories | snapshot       | Small reference set; typically full refresh by regionCode or id |

---

## **Read API for Data Retrieval**

- **Base URL**: `https://www.googleapis.com/youtube/v3`
- **Method**: GET for all list endpoints.
- **Pagination**: `pageToken` (request) and `nextPageToken` / `prevPageToken` (response). `maxResults` 1–50 (1–100 for commentThreads and comments). Continue until `nextPageToken` is absent or empty.
- **Quota**: Default 10,000 units/day. Each list call = 1 unit; **search.list = 100 units**. Throttle to stay under quota.

---

### **channels**

- **Endpoint**: `GET /youtube/v3/channels`
- **Required**: `part` (comma-separated, e.g. `snippet,statistics,contentDetails`).
- **Filter (exactly one)**: `id` (comma-separated channel IDs), `mine` (boolean, OAuth), `forHandle`, `forUsername`, `categoryId` (deprecated), `managedByMe` (partners).
- **Optional**: `maxResults` (0–50, default 5), `pageToken`, `hl`, `onBehalfOfContentOwner`.
- **Response**: `kind`, `etag`, `nextPageToken`, `prevPageToken`, `pageInfo` (totalResults, resultsPerPage), `items[]` → channel (id, snippet, statistics, contentDetails, etc.).
- **Primary key / cursor**: `id`. Cursor for pagination: `pageToken` → `nextPageToken`.

---

### **playlists**

- **Endpoint**: `GET /youtube/v3/playlists`
- **Required**: `part` (e.g. `snippet,contentDetails`).
- **Filter (exactly one)**: `id` (comma-separated), `channelId`, `mine` (boolean, OAuth).
- **Optional**: `maxResults` (0–50, default 5), `pageToken`, `hl`, `onBehalfOfContentOwner`, `onBehalfOfContentOwnerChannel`.
- **Response**: `kind`, `etag`, `nextPageToken`, `prevPageToken`, `pageInfo`, `items[]` → playlist (id, snippet, contentDetails, status, etc.).
- **Primary key / cursor**: `id`. Cursor: `pageToken` → `nextPageToken`.

---

### **playlist_items**

- **Endpoint**: `GET /youtube/v3/playlistItems`
- **Required**: `part` (e.g. `snippet,contentDetails`).
- **Filter (exactly one)**: `playlistId` or `id` (comma-separated playlist item IDs).
- **Optional**: `maxResults` (0–50, default 5), `pageToken`, `videoId` (filter items for one video), `onBehalfOfContentOwner`.
- **Response**: `kind`, `etag`, `nextPageToken`, `prevPageToken`, `pageInfo`, `items[]` → playlistItem (id, snippet, contentDetails, status).
- **Primary key / cursor**: `id`. Cursor: `pageToken` → `nextPageToken`. When listing by `playlistId`, order is by playlist position.

---

### **videos**

- **Endpoint**: `GET /youtube/v3/videos`
- **Required**: `part` (e.g. `snippet,statistics,contentDetails,status`).
- **Filter (exactly one)**: `id` (comma-separated video IDs), `chart` (e.g. `mostPopular`), or `myRating` (`like`/`dislike`, OAuth).
- **Optional**: `maxResults` (1–50, only with chart or myRating), `pageToken`, `hl`, `regionCode`, `videoCategoryId` (with chart), `maxWidth`/`maxHeight`, `onBehalfOfContentOwner`.
- **Response**: `kind`, `etag`, `nextPageToken`, `prevPageToken`, `pageInfo`, `items[]` → video (id, snippet, contentDetails, statistics, status, etc.).
- **Primary key / cursor**: `id`. Cursor for chart/myRating: `pageToken` → `nextPageToken`. Not used when filtering by `id`.

---

### **search**

- **Endpoint**: `GET /youtube/v3/search`
- **Required**: `part=snippet`.
- **Optional filters**: `q` (query), `channelId`, `type` (video, channel, playlist; default video,channel,playlist), `order`, `publishedAfter`, `publishedBefore`, `regionCode`, `relevanceLanguage`, `safeSearch`, `videoCategoryId`, `eventType`, `location`, `locationRadius`, `topicId`, and various video filters (videoDuration, videoType, etc.).
- **Optional**: `maxResults` (0–50, default 5), `pageToken`.
- **Response**: `kind`, `etag`, `nextPageToken`, `prevPageToken`, `regionCode`, `pageInfo`, `items[]` → searchResult (id.kind, id.videoId/id.channelId/id.playlistId, snippet).
- **Primary key / cursor**: Composite from `id` (kind + videoId or channelId or playlistId). Cursor: `pageToken` → `nextPageToken`. Time window: `publishedAfter` / `publishedBefore` (ISO 8601). **Quota: 100 units per request.**

---

### **activities**

- **Endpoint**: `GET /youtube/v3/activities`
- **Required**: `part` (e.g. `snippet,contentDetails`).
- **Filter (exactly one)**: `channelId`, `mine` (boolean, OAuth), or `home` (deprecated).
- **Optional**: `maxResults` (0–50, default 5), `pageToken`, `publishedAfter`, `publishedBefore` (ISO 8601), `regionCode`.
- **Response**: `kind`, `etag`, `nextPageToken`, `prevPageToken`, `pageInfo`, `items[]` → activity (id, snippet, contentDetails).
- **Primary key / cursor**: `id`. Cursor: `pageToken` for pages; **`publishedAfter`** for incremental sync (store latest activity time, request activities after that time).

---

### **comment_threads**

- **Endpoint**: `GET /youtube/v3/commentThreads`
- **Required**: `part` (e.g. `snippet,replies`).
- **Filter (exactly one)**: `videoId`, `allThreadsRelatedToChannelId`, or `id` (comma-separated thread IDs).
- **Optional**: `maxResults` (1–100, default 20), `pageToken`, `order` (relevance, time), `searchTerms`, `moderationStatus`, `textFormat`.
- **Response**: `kind`, `etag`, `nextPageToken`, `pageInfo`, `items[]` → commentThread (id, snippet, replies).
- **Primary key / cursor**: `id`. Cursor: `pageToken` → `nextPageToken`.

---

### **subscriptions**

- **Endpoint**: `GET /youtube/v3/subscriptions`
- **Required**: `part` (e.g. `snippet,contentDetails`).
- **Filter (exactly one)**: `channelId`, `id` (comma-separated), `mine` (boolean, OAuth), `mySubscribers`, or `myRecentSubscribers`.
- **Optional**: `maxResults` (0–50, default 5), `pageToken`, `order`, `forChannelId`, `onBehalfOfContentOwner`, `onBehalfOfContentOwnerChannel`.
- **Response**: `kind`, `etag`, `nextPageToken`, `prevPageToken`, `pageInfo`, `items[]` → subscription (id, snippet, contentDetails, subscriberSnippet).
- **Primary key / cursor**: `id`. Cursor: `pageToken` → `nextPageToken`.

---

### **video_categories**

- **Endpoint**: `GET /youtube/v3/videoCategories`
- **Required**: `part=snippet`.
- **Filter (exactly one)**: `id` (comma-separated) or `regionCode` (ISO 3166-1 alpha-2).
- **Optional**: `hl`.
- **Response**: `kind`, `etag`, `nextPageToken`, `prevPageToken`, `pageInfo`, `items[]` → videoCategory (id, snippet).
- **Primary key / cursor**: `id`. Cursor: `pageToken` if multiple pages (rare). No incremental cursor; small reference table.

---

## **Field Type Mapping**

- **Strings**: All identifier and text fields (id, channelId, videoId, title, description, etc.) → string.
- **Numbers**: API often returns numeric counts as **strings** (e.g. `statistics.viewCount`, `statistics.likeCount`). Map to integer/long in the warehouse if needed.
- **Datetime**: ISO 8601 / RFC 3339 (e.g. `publishedAt`, `snippet.publishedAt`) → datetime/timestamp.
- **Booleans**: e.g. `hiddenSubscriberCount`, `caption` (true/false) → boolean.
- **Enumerations**: e.g. `privacyStatus` (public, private, unlisted), `liveBroadcastContent` (none, live, upcoming), `snippet.type` in activities (upload, like, subscription, etc.) → string; normalize in ETL if needed.
- **Nested objects**: Flatten for tabular load (e.g. `snippet_title`, `statistics_viewCount`, `contentDetails_relatedPlaylists_uploads`).
- **Thumbnails**: Object keyed by default, medium, high, etc.; each has url, width, height → typically keep url (string) or full object as JSON.

---

## **Sources and References**

| Source | URL | Use | Confidence |
|--------|-----|-----|------------|
| YouTube Data API v3 – Channels: list | https://developers.google.com/youtube/v3/docs/channels/list | channels endpoint, params, parts, response | Official – highest |
| YouTube Data API v3 – Playlists: list | https://developers.google.com/youtube/v3/docs/playlists/list | playlists endpoint, params, pagination | Official – highest |
| YouTube Data API v3 – PlaylistItems: list | https://developers.google.com/youtube/v3/docs/playlistItems/list | playlist_items endpoint, params | Official – highest |
| YouTube Data API v3 – Videos: list | https://developers.google.com/youtube/v3/docs/videos/list | videos endpoint, parts, filters | Official – highest |
| YouTube Data API v3 – Search: list | https://developers.google.com/youtube/v3/docs/search/list | search endpoint, q, type, publishedAfter, quota 100 | Official – highest |
| YouTube Data API v3 – Activities: list | https://developers.google.com/youtube/v3/docs/activities/list | activities endpoint, publishedAfter/Before | Official – highest |
| YouTube Data API v3 – CommentThreads: list | https://developers.google.com/youtube/v3/docs/commentThreads/list | comment_threads endpoint, maxResults 1–100 | Official – highest |
| YouTube Data API v3 – Subscriptions: list | https://developers.google.com/youtube/v3/docs/subscriptions/list | subscriptions endpoint | Official – highest |
| YouTube Data API v3 – VideoCategories: list | https://developers.google.com/youtube/v3/docs/videoCategories/list | video_categories endpoint | Official – highest |
| YouTube Data API v3 – Comments: list | https://developers.google.com/youtube/v3/docs/comments/list | comments (replies) reference | Official – highest |
| Channel resource | https://developers.google.com/youtube/v3/docs/channels | channel schema, snippet, statistics, contentDetails | Official – highest |
| Video resource | https://developers.google.com/youtube/v3/docs/videos | video schema, snippet, statistics, contentDetails, status | Official – highest |
| Playlist resource | https://developers.google.com/youtube/v3/docs/playlists | playlist schema | Official – highest |
| PlaylistItem resource | https://developers.google.com/youtube/v3/docs/playlistItems | playlistItem schema | Official – highest |
| Search resource | https://developers.google.com/youtube/v3/docs/search | searchResult shape, id.kind, snippet | Official – highest |
| Activity resource | https://developers.google.com/youtube/v3/docs/activities | activity schema, contentDetails by type | Official – highest |
| YouTube connector schemas | src/databricks/labs/community_connector/sources/youtube/youtube_schemas.py | Table set, PKs, flattened field names | Internal – high |

- All list endpoints and resource schemas are from the official YouTube Data API v3 docs. Quota (10,000/day, search.list=100) and pagination (pageToken, maxResults 1–50 or 1–100) are as stated in the official docs. This doc prioritizes official API documentation for any conflict.
