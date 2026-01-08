# **Spotify Web API Documentation**

## **Authorization**

- **Chosen method**: OAuth 2.0 with Client Credentials Flow for catalog data, or Authorization Code Flow with refresh token for user-specific data.
- **Base URL**: `https://api.spotify.com/v1`
- **Token endpoint**: `https://accounts.spotify.com/api/token`

### Client Credentials Flow (for public catalog data)
For accessing public Spotify catalog data (artists, albums, tracks, etc.) without user context:

```bash
# Get access token using client credentials
curl -X POST "https://accounts.spotify.com/api/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=YOUR_CLIENT_ID&client_secret=YOUR_CLIENT_SECRET"
```

**Response:**
```json
{
  "access_token": "BQDl...",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

### Authorization Code Flow with Refresh Token (for user data)
For accessing user-specific data (saved tracks, playlists, top items), the connector stores `client_id`, `client_secret`, and `refresh_token`, exchanging for access tokens at runtime:

```bash
# Refresh access token
curl -X POST "https://accounts.spotify.com/api/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=refresh_token&refresh_token=YOUR_REFRESH_TOKEN&client_id=YOUR_CLIENT_ID&client_secret=YOUR_CLIENT_SECRET"
```

**Response:**
```json
{
  "access_token": "BQDl...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "scope": "user-read-private user-library-read playlist-read-private user-top-read"
}
```

### Auth Placement
- HTTP header: `Authorization: Bearer <access_token>`

### Required Scopes (for user data endpoints)
| Scope | Description |
|-------|-------------|
| `user-read-private` | Read access to user's profile |
| `user-library-read` | Read access to user's saved tracks and albums |
| `playlist-read-private` | Read access to user's private playlists |
| `playlist-read-collaborative` | Read access to collaborative playlists |
| `user-top-read` | Read access to user's top artists and tracks |
| `user-read-recently-played` | Read access to recently played tracks |

Example authenticated request:
```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me/tracks?limit=50"
```

**Rate Limits:**
- Spotify does not publish specific rate limit numbers
- Rate limiting returns HTTP 429 with `Retry-After` header
- Recommended: Implement exponential backoff with retry logic
- Best practice: Stay under ~100 requests per minute for sustained usage

---

## **Object List**

The object list is **static** (defined by the connector). These represent the primary data objects available through the Spotify Web API that are suitable for data ingestion.

| Object Name | Description | Primary Endpoint | Ingestion Type |
|-------------|-------------|------------------|----------------|
| `tracks` | User's saved tracks (library) | `GET /me/tracks` | `snapshot` |
| `albums` | User's saved albums (library) | `GET /me/albums` | `snapshot` |
| `playlists` | User's playlists (owned and followed) | `GET /me/playlists` | `snapshot` |
| `artists` | User's followed artists | `GET /me/following?type=artist` | `snapshot` |
| `top_tracks` | User's top tracks (short/medium/long term) | `GET /me/top/tracks` | `snapshot` |
| `top_artists` | User's top artists (short/medium/long term) | `GET /me/top/artists` | `snapshot` |
| `recently_played` | User's recently played tracks | `GET /me/player/recently-played` | `append` |
| `user_profile` | Current user's profile information | `GET /me` | `snapshot` |

**Notes:**
- All user-specific endpoints require Authorization Code Flow with appropriate scopes
- Catalog endpoints (artists, albums, tracks by ID) use Client Credentials but require specific IDs - not suitable for bulk ingestion
- `recently_played` is the only table with a cursor-based incremental pattern using `before`/`after` timestamps
- Most tables are treated as snapshots since Spotify doesn't provide `updated_at` fields for library items

---

## **Object Schema**

### General Notes
- Spotify API returns JSON with nested objects
- Nested objects (e.g., `artist`, `album`) are preserved as nested structures
- ISO 8601 timestamps are used for date fields
- IDs are strings (Spotify URIs/IDs)

---

### `tracks` object (User's Saved Tracks)

**Source endpoint:** `GET /me/tracks`

**Required scope:** `user-library-read`

**High-level schema (connector view):**

| Column Name | Type | Description |
|-------------|------|-------------|
| `added_at` | string (ISO 8601 datetime) | When the track was saved to the library |
| `track` | struct | The track object (see nested schema) |

**Nested `track` struct:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Spotify ID for the track |
| `name` | string | Track name |
| `uri` | string | Spotify URI for the track |
| `href` | string | API endpoint URL for the track |
| `disc_number` | integer | Disc number (usually 1) |
| `track_number` | integer | Track number on the album |
| `duration_ms` | integer | Track duration in milliseconds |
| `explicit` | boolean | Whether the track has explicit lyrics |
| `is_local` | boolean | Whether it's a local file |
| `popularity` | integer | Popularity score (0-100) |
| `preview_url` | string or null | URL to a 30-second preview |
| `external_urls` | struct | External URLs (Spotify web player link) |
| `external_ids` | struct | External IDs (ISRC, EAN, UPC) |
| `album` | struct | Album the track appears on |
| `artists` | array\<struct\> | Artists who performed the track |
| `available_markets` | array\<string\> | Markets where track is available |

**Nested `album` struct (simplified):**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Spotify ID for the album |
| `name` | string | Album name |
| `uri` | string | Spotify URI |
| `album_type` | string | Type: `album`, `single`, or `compilation` |
| `total_tracks` | integer | Total number of tracks |
| `release_date` | string | Release date |
| `release_date_precision` | string | Precision: `year`, `month`, or `day` |
| `images` | array\<struct\> | Album cover images |
| `artists` | array\<struct\> | Album artists |

**Nested `artist` struct (simplified):**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Spotify ID for the artist |
| `name` | string | Artist name |
| `uri` | string | Spotify URI |
| `href` | string | API endpoint URL |
| `external_urls` | struct | External URLs |

**Example request:**
```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me/tracks?limit=50&offset=0"
```

**Example response:**
```json
{
  "href": "https://api.spotify.com/v1/me/tracks?offset=0&limit=50",
  "items": [
    {
      "added_at": "2024-01-15T10:30:00Z",
      "track": {
        "id": "4iV5W9uYEdYUVa79Axb7Rh",
        "name": "Bohemian Rhapsody",
        "uri": "spotify:track:4iV5W9uYEdYUVa79Axb7Rh",
        "duration_ms": 354947,
        "explicit": false,
        "popularity": 85,
        "album": {
          "id": "6i6folBtxKV28WX3msQ4FE",
          "name": "A Night At The Opera",
          "album_type": "album"
        },
        "artists": [
          {
            "id": "1dfeR4HaWDbWqFHLkxsg1d",
            "name": "Queen"
          }
        ]
      }
    }
  ],
  "limit": 50,
  "next": "https://api.spotify.com/v1/me/tracks?offset=50&limit=50",
  "offset": 0,
  "previous": null,
  "total": 150
}
```

---

### `albums` object (User's Saved Albums)

**Source endpoint:** `GET /me/albums`

**Required scope:** `user-library-read`

**High-level schema (connector view):**

| Column Name | Type | Description |
|-------------|------|-------------|
| `added_at` | string (ISO 8601 datetime) | When the album was saved |
| `album` | struct | The album object |

**Nested `album` struct:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Spotify ID for the album |
| `name` | string | Album name |
| `uri` | string | Spotify URI |
| `href` | string | API endpoint URL |
| `album_type` | string | Type: `album`, `single`, or `compilation` |
| `total_tracks` | integer | Total number of tracks |
| `release_date` | string | Release date |
| `release_date_precision` | string | Precision: `year`, `month`, or `day` |
| `label` | string | Record label |
| `popularity` | integer | Popularity score (0-100) |
| `genres` | array\<string\> | List of genres |
| `images` | array\<struct\> | Album cover images (with url, height, width) |
| `artists` | array\<struct\> | Album artists |
| `copyrights` | array\<struct\> | Copyright statements |
| `external_ids` | struct | External IDs (UPC) |
| `external_urls` | struct | External URLs |
| `available_markets` | array\<string\> | Markets where available |
| `tracks` | struct | Paging object of simplified tracks |

---

### `playlists` object (User's Playlists)

**Source endpoint:** `GET /me/playlists`

**Required scope:** `playlist-read-private`, `playlist-read-collaborative`

**High-level schema (connector view):**

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Spotify ID for the playlist |
| `name` | string | Playlist name |
| `uri` | string | Spotify URI |
| `href` | string | API endpoint URL |
| `description` | string or null | Playlist description |
| `public` | boolean or null | Whether the playlist is public |
| `collaborative` | boolean | Whether it's collaborative |
| `snapshot_id` | string | Version identifier for the playlist |
| `owner` | struct | User who owns the playlist |
| `images` | array\<struct\> | Playlist cover images |
| `tracks` | struct | Tracks reference (href, total count) |
| `external_urls` | struct | External URLs |

**Nested `owner` struct:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Spotify user ID |
| `display_name` | string or null | User's display name |
| `uri` | string | Spotify URI |
| `href` | string | API endpoint URL |
| `external_urls` | struct | External URLs |

---

### `artists` object (User's Followed Artists)

**Source endpoint:** `GET /me/following?type=artist`

**Required scope:** `user-follow-read`

**High-level schema (connector view):**

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Spotify ID for the artist |
| `name` | string | Artist name |
| `uri` | string | Spotify URI |
| `href` | string | API endpoint URL |
| `popularity` | integer | Popularity score (0-100) |
| `genres` | array\<string\> | List of genres |
| `images` | array\<struct\> | Artist images |
| `followers` | struct | Followers info (href, total) |
| `external_urls` | struct | External URLs |

---

### `top_tracks` object (User's Top Tracks)

**Source endpoint:** `GET /me/top/tracks`

**Required scope:** `user-top-read`

**Query parameter:** `time_range` - `short_term` (4 weeks), `medium_term` (6 months), `long_term` (years)

**Schema:** Same as the `track` struct from `tracks` table, with additional connector-derived field:

| Column Name | Type | Description |
|-------------|------|-------------|
| `time_range` | string (connector-derived) | The time range used for the query |
| ... | ... | All fields from track object |

---

### `top_artists` object (User's Top Artists)

**Source endpoint:** `GET /me/top/artists`

**Required scope:** `user-top-read`

**Query parameter:** `time_range` - `short_term`, `medium_term`, `long_term`

**Schema:** Same as `artists` table, with additional connector-derived field:

| Column Name | Type | Description |
|-------------|------|-------------|
| `time_range` | string (connector-derived) | The time range used for the query |
| ... | ... | All fields from artist object |

---

### `recently_played` object (Recently Played Tracks)

**Source endpoint:** `GET /me/player/recently-played`

**Required scope:** `user-read-recently-played`

**High-level schema (connector view):**

| Column Name | Type | Description |
|-------------|------|-------------|
| `played_at` | string (ISO 8601 datetime) | When the track was played |
| `track` | struct | The track object (same as tracks table) |
| `context` | struct or null | Context from which track was played |

**Nested `context` struct:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Object type: `album`, `artist`, `playlist` |
| `uri` | string | Spotify URI |
| `href` | string | API endpoint URL |
| `external_urls` | struct | External URLs |

---

### `user_profile` object (Current User Profile)

**Source endpoint:** `GET /me`

**Required scope:** `user-read-private`, `user-read-email` (optional for email)

**High-level schema (connector view):**

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Spotify user ID |
| `display_name` | string or null | Display name |
| `uri` | string | Spotify URI |
| `href` | string | API endpoint URL |
| `email` | string or null | Email (requires scope) |
| `country` | string | ISO 3166-1 alpha-2 country code |
| `product` | string | Subscription level: `premium`, `free`, etc. |
| `explicit_content` | struct | Explicit content settings |
| `followers` | struct | Followers info |
| `images` | array\<struct\> | Profile images |
| `external_urls` | struct | External URLs |

---

## **Get Object Primary Keys**

Primary keys are defined **statically** based on the resource schema. There is no dedicated metadata endpoint.

| Object | Primary Key | Type | Notes |
|--------|-------------|------|-------|
| `tracks` | `track.id` | string | Spotify track ID, unique identifier |
| `albums` | `album.id` | string | Spotify album ID |
| `playlists` | `id` | string | Spotify playlist ID |
| `artists` | `id` | string | Spotify artist ID |
| `top_tracks` | `id`, `time_range` | composite | Track ID + time range for uniqueness |
| `top_artists` | `id`, `time_range` | composite | Artist ID + time range for uniqueness |
| `recently_played` | `played_at`, `track.id` | composite | Timestamp + track ID (same track can be played multiple times) |
| `user_profile` | `id` | string | Spotify user ID |

---

## **Object's Ingestion Type**

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `tracks` | `snapshot` | No `updated_at` field available; full refresh of library |
| `albums` | `snapshot` | No incremental cursor available |
| `playlists` | `snapshot` | `snapshot_id` changes on modification but no reliable cursor |
| `artists` | `snapshot` | No incremental cursor available |
| `top_tracks` | `snapshot` | Reflects current top tracks, full refresh |
| `top_artists` | `snapshot` | Reflects current top artists, full refresh |
| `recently_played` | `append` | Uses `before`/`after` cursors for incremental reads |
| `user_profile` | `snapshot` | Single record, full refresh |

For `recently_played`:
- **Cursor field**: `played_at`
- **Pagination**: Uses `before` parameter (Unix timestamp in ms) to get tracks played before a given time
- **Limit**: Maximum 50 items per request
- **Note**: Spotify only stores ~50 most recently played tracks

---

## **Read API for Data Retrieval**

### Common Pagination Pattern

Most Spotify endpoints use offset-based pagination:

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Maximum items per page (max 50, default 20) |
| `offset` | integer | Index of first item to return (default 0) |

**Response includes:**
- `items`: Array of results
- `total`: Total number of items available
- `limit`: Limit used for request
- `offset`: Current offset
- `next`: URL for next page (null if no more pages)
- `previous`: URL for previous page (null if first page)

### Endpoint Details

#### GET /me/tracks (Saved Tracks)

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me/tracks?limit=50&offset=0"
```

**Parameters:**
| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `limit` | No | 20 | Max items (1-50) |
| `offset` | No | 0 | Starting index |
| `market` | No | - | ISO 3166-1 alpha-2 country code |

#### GET /me/albums (Saved Albums)

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me/albums?limit=50&offset=0"
```

**Parameters:** Same as /me/tracks

#### GET /me/playlists (User's Playlists)

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me/playlists?limit=50&offset=0"
```

**Parameters:**
| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `limit` | No | 20 | Max items (1-50) |
| `offset` | No | 0 | Starting index |

#### GET /me/following (Followed Artists)

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me/following?type=artist&limit=50"
```

**Parameters:**
| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `type` | Yes | - | Must be `artist` |
| `limit` | No | 20 | Max items (1-50) |
| `after` | No | - | Last artist ID for cursor-based pagination |

**Note:** This endpoint uses cursor-based pagination with `after` parameter, not offset.

#### GET /me/top/tracks and /me/top/artists

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me/top/tracks?time_range=medium_term&limit=50"
```

**Parameters:**
| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `time_range` | No | `medium_term` | `short_term`, `medium_term`, or `long_term` |
| `limit` | No | 20 | Max items (1-50) |
| `offset` | No | 0 | Starting index |

#### GET /me/player/recently-played

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me/player/recently-played?limit=50"
```

**Parameters:**
| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `limit` | No | 20 | Max items (1-50) |
| `after` | No | - | Unix timestamp in ms; return items after this |
| `before` | No | - | Unix timestamp in ms; return items before this |

**Note:** Cannot use both `after` and `before` in the same request.

**Incremental Strategy:**
- Store the `played_at` timestamp of the most recent track
- On subsequent runs, use `after` parameter with stored timestamp
- Convert ISO 8601 to Unix timestamp in milliseconds

#### GET /me (User Profile)

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://api.spotify.com/v1/me"
```

No pagination needed - returns single user object.

---

## **Field Type Mapping**

| Spotify JSON Type | Example Fields | Connector Logical Type | Notes |
|-------------------|----------------|------------------------|-------|
| string | `id`, `name`, `uri` | string | UTF-8 text |
| integer | `duration_ms`, `popularity`, `total_tracks` | long | Use 64-bit for safety |
| boolean | `explicit`, `is_local`, `collaborative` | boolean | Standard true/false |
| string (ISO 8601) | `added_at`, `played_at`, `release_date` | string | Preserve as string |
| object | `album`, `artist`, `owner` | struct | Nested structure |
| array | `artists`, `images`, `genres` | array | Array of primitives or structs |
| null | Various optional fields | corresponding type + null | Handle nulls gracefully |

**Special behaviors:**
- `release_date` may be `YYYY`, `YYYY-MM`, or `YYYY-MM-DD` depending on `release_date_precision`
- `popularity` is a score from 0-100, updated frequently
- `images` arrays are ordered by size (largest first)
- `available_markets` can be very large arrays; consider omitting if not needed

---

## **Write API**

The Spotify connector is **read-only**. While Spotify API supports write operations (creating playlists, saving tracks), these are not implemented in the connector as it's designed for data ingestion purposes only.

For reference, write operations would include:
- `PUT /me/tracks` - Save tracks to library
- `DELETE /me/tracks` - Remove tracks from library
- `POST /users/{user_id}/playlists` - Create playlist
- `POST /playlists/{playlist_id}/tracks` - Add tracks to playlist

---

## **Known Quirks & Edge Cases**

1. **Token Expiration**: Access tokens expire after 1 hour. The connector must refresh tokens using the refresh_token before making API calls.

2. **Rate Limiting**: Spotify doesn't publish exact rate limits. Implement exponential backoff when receiving 429 responses.

3. **Recently Played Limit**: The `/me/player/recently-played` endpoint only stores approximately 50 tracks. Frequent polling is needed to capture complete history.

4. **Market Availability**: Track/album availability varies by market. Some fields may be null or tracks may not appear if not available in the user's market.

5. **Local Files**: Tracks from local files have `is_local: true` and limited metadata. They cannot be played via API.

6. **Pagination Limits**: Maximum 50 items per request for most endpoints. Large libraries require many paginated requests.

7. **Followed Artists Pagination**: Uses cursor-based pagination (`after` parameter) unlike other endpoints which use offset-based.

8. **Empty Libraries**: New accounts may have empty libraries. Handle empty `items` arrays gracefully.

9. **Image Arrays**: May be empty for some objects. Always check for null/empty before accessing.

10. **Time Range for Top Items**: Results vary significantly between `short_term`, `medium_term`, and `long_term`. Consider ingesting all three.

---

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://developer.spotify.com/documentation/web-api | 2024-12-22 | High | API overview, available endpoints |
| Official Docs | https://developer.spotify.com/documentation/web-api/concepts/authorization | 2024-12-22 | High | OAuth flows, token refresh |
| Official Docs | https://developer.spotify.com/documentation/web-api/concepts/api-calls | 2024-12-22 | High | Request format, rate limits |
| Official Docs | https://developer.spotify.com/documentation/web-api/reference | 2024-12-22 | High | Endpoint specifications, schemas |
| OSS Library | https://github.com/spotipy-dev/spotipy | 2024-12-22 | High | Python implementation patterns |

---

## **Sources and References**

- **Official Spotify Web API documentation** (highest confidence)
  - https://developer.spotify.com/documentation/web-api
  - https://developer.spotify.com/documentation/web-api/concepts/authorization
  - https://developer.spotify.com/documentation/web-api/concepts/api-calls
  - https://developer.spotify.com/documentation/web-api/concepts/scopes
  - https://developer.spotify.com/documentation/web-api/reference

- **Spotipy Python Library** (high confidence)
  - https://github.com/spotipy-dev/spotipy
  - Reference implementation for Python integration patterns

When conflicts arise, **official Spotify documentation** is treated as the source of truth.

