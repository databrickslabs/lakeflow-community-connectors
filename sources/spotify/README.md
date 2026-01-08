# Lakeflow Spotify Community Connector

This documentation describes how to configure and use the **Spotify** Lakeflow community connector to ingest data from the Spotify Web API into Databricks.

## Prerequisites

- **Spotify Developer Account**: You need a Spotify account to access the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
- **Spotify Application**: Create an application in the Developer Dashboard to obtain client credentials.
- **OAuth Refresh Token**: Required for accessing user-specific data (saved tracks, playlists, etc.).
- **Scopes**: Your refresh token must include the following scopes:
  - `user-library-read` - Read access to saved tracks and albums
  - `playlist-read-private` - Read access to private playlists
  - `playlist-read-collaborative` - Read access to collaborative playlists
  - `user-follow-read` - Read access to followed artists
  - `user-top-read` - Read access to top artists and tracks
  - `user-read-recently-played` - Read access to recently played tracks
  - `user-read-private` - Read access to user profile
- **Network access**: The environment running the connector must be able to reach `https://api.spotify.com` and `https://accounts.spotify.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `client_id` | string | yes | Spotify application client ID from the Developer Dashboard. | `abc123def456...` |
| `client_secret` | string | yes | Spotify application client secret from the Developer Dashboard. | `xyz789...` |
| `refresh_token` | string | yes | OAuth 2.0 refresh token obtained through the Authorization Code flow. | `AQC7...` |
| `externalOptionsAllowList` | string | no | Comma-separated list of table-specific option names. Only needed if using table-specific options. | `time_range,limit,max_pages` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`time_range,limit,max_pages`

> **Note**: Most tables do not require additional options. The `time_range` option is only applicable for `top_tracks` and `top_artists` tables.

### Obtaining the Required Parameters

#### Step 1: Create a Spotify Application

1. Go to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
2. Click **Create App**.
3. Fill in the app details:
   - **App name**: Your choice (e.g., "Lakeflow Connector")
   - **App description**: Brief description
   - **Redirect URI**: Add `http://localhost:8888/callback` (needed for OAuth flow)
4. Accept the terms and create the app.
5. Copy the **Client ID** and **Client Secret** from the app settings.

#### Step 2: Obtain a Refresh Token

The refresh token requires completing the OAuth 2.0 Authorization Code flow. Here's a simplified process:

1. **Authorize the application** by visiting this URL in a browser (replace placeholders):
   ```
   https://accounts.spotify.com/authorize?client_id=YOUR_CLIENT_ID&response_type=code&redirect_uri=http://localhost:8888/callback&scope=user-library-read%20playlist-read-private%20playlist-read-collaborative%20user-follow-read%20user-top-read%20user-read-recently-played%20user-read-private
   ```

2. **Login and authorize** the application when prompted.

3. **Copy the authorization code** from the redirect URL:
   ```
   http://localhost:8888/callback?code=AQD...
   ```

4. **Exchange the code for tokens** using curl:
   ```bash
   curl -X POST "https://accounts.spotify.com/api/token" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "grant_type=authorization_code" \
     -d "code=YOUR_AUTHORIZATION_CODE" \
     -d "redirect_uri=http://localhost:8888/callback" \
     -d "client_id=YOUR_CLIENT_ID" \
     -d "client_secret=YOUR_CLIENT_SECRET"
   ```

5. **Save the `refresh_token`** from the response. This token does not expire and can be used to generate new access tokens.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. If you need table-specific options like `time_range`, set `externalOptionsAllowList` to `time_range,limit,max_pages`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Spotify connector exposes a **static list** of tables:

- `tracks` - User's saved tracks (library)
- `albums` - User's saved albums (library)
- `playlists` - User's playlists (owned and followed)
- `artists` - User's followed artists
- `top_tracks` - User's top tracks
- `top_artists` - User's top artists
- `recently_played` - User's recently played tracks
- `user_profile` - Current user's profile information

### Object Summary, Primary Keys, and Ingestion Mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|-------|-------------|----------------|-------------|-------------------|
| `tracks` | Saved tracks in user's library | `snapshot` | `track.id` | n/a |
| `albums` | Saved albums in user's library | `snapshot` | `album.id` | n/a |
| `playlists` | User's playlists (owned & followed) | `snapshot` | `id` | n/a |
| `artists` | Artists followed by user | `snapshot` | `id` | n/a |
| `top_tracks` | User's top tracks by time range | `snapshot` | `id`, `time_range` | n/a |
| `top_artists` | User's top artists by time range | `snapshot` | `id`, `time_range` | n/a |
| `recently_played` | Recently played track history | `cdc` | `played_at`, `track.id` | `played_at` |
| `user_profile` | Current user's profile | `snapshot` | `id` | n/a |

### Required and Optional Table Options

| Option | Applicable Tables | Type | Required | Description |
|--------|-------------------|------|----------|-------------|
| `time_range` | `top_tracks`, `top_artists` | string | no | Time range for top items: `short_term` (4 weeks), `medium_term` (6 months), or `long_term` (years). Defaults to `medium_term`. |
| `limit` | All tables | integer | no | Number of items per page (max 50). Defaults to 50. |
| `max_pages` | All paginated tables | integer | no | Maximum pages to fetch. Defaults to 100. |

### Schema Highlights

The connector preserves nested structures from the Spotify API:

- **`tracks`**: Contains `added_at` timestamp and nested `track` object with album, artists, and metadata.
- **`albums`**: Contains `added_at` timestamp and nested `album` object with artists, images, copyrights, and track information.
- **`playlists`**: Includes owner information, track count, images, and collaborative/public flags.
- **`artists`**: Full artist objects with genres, popularity, followers, and images.
- **`top_tracks` / `top_artists`**: Include a `time_range` field to differentiate between time periods.
- **`recently_played`**: Includes `played_at` timestamp, full track object, and context (album/playlist/artist).
- **`user_profile`**: User metadata including display name, country, subscription product, and follower count.

## Data Type Mapping

Spotify JSON fields are mapped to logical types as follows:

| Spotify JSON Type | Example Fields | Connector Logical Type | Notes |
|-------------------|----------------|------------------------|-------|
| string | `id`, `name`, `uri` | string (`StringType`) | Spotify IDs are strings |
| integer | `duration_ms`, `popularity`, `total_tracks` | 64-bit integer (`LongType`) | All integers use LongType |
| boolean | `explicit`, `is_local`, `collaborative` | boolean (`BooleanType`) | Standard true/false |
| ISO 8601 datetime | `added_at`, `played_at`, `release_date` | string | Preserved as strings |
| object | `album`, `artist`, `owner` | struct (`StructType`) | Nested objects preserved |
| array | `artists`, `images`, `genres` | array | Arrays of primitives or structs |
| nullable fields | `preview_url`, `description` | same as base type + null | Missing fields are null |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Spotify connector source in your workspace. This will typically place the connector code (`spotify.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingestion_pipeline.py`), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** using this Spotify connector.
- One or more **tables** to ingest.

Example `pipeline_spec` for ingesting user library data:

```json
{
  "pipeline_spec": {
    "connection_name": "spotify_connection",
    "object": [
      {
        "table": {
          "source_table": "tracks"
        }
      },
      {
        "table": {
          "source_table": "albums"
        }
      },
      {
        "table": {
          "source_table": "playlists"
        }
      },
      {
        "table": {
          "source_table": "top_tracks",
          "time_range": "medium_term"
        }
      },
      {
        "table": {
          "source_table": "recently_played"
        }
      },
      {
        "table": {
          "source_table": "user_profile"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Spotify credentials.
- For `top_tracks` and `top_artists`, optionally specify `time_range` to control the time period.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration. For the `recently_played` table:

- On the **first run**, all available recently played tracks are fetched (up to ~50 most recent).
- On **subsequent runs**, the connector uses the stored `cursor` based on `played_at` to fetch only new plays.

> **Note**: Spotify only stores approximately 50 most recently played tracks. For complete listening history, schedule frequent syncs (e.g., every 30 minutes to 1 hour).

#### Best Practices

- **Start small**: Begin with a few tables (e.g., `tracks`, `playlists`) to validate configuration.
- **Use incremental sync for `recently_played`**: This table supports CDC and will only fetch new plays on subsequent runs.
- **Schedule frequently for listening history**: The `recently_played` endpoint only retains ~50 tracks, so frequent polling captures more complete history.
- **Handle rate limits gracefully**: The connector includes automatic retry logic with exponential backoff for rate-limited requests.

#### Troubleshooting

**Common Issues:**

- **Authentication failures (`401`)**:
  - Verify that `client_id`, `client_secret`, and `refresh_token` are correct.
  - Ensure the refresh token was generated with the required scopes.
  - Check that the Spotify application hasn't been deleted or its credentials rotated.

- **Missing data or empty results**:
  - Confirm the user account has saved tracks/albums or followed artists.
  - For `top_tracks`/`top_artists`, ensure the account has sufficient listening history.

- **Rate limiting (`429`)**:
  - The connector automatically retries with backoff. If persistent, reduce sync frequency.
  - Spotify doesn't publish explicit rate limits, but staying under 100 requests/minute is recommended.

- **Scope-related errors (`403`)**:
  - The refresh token may be missing required scopes. Re-authorize with all necessary scopes.

- **Empty `recently_played` on first run**:
  - Spotify only stores recent history. If the user hasn't played tracks recently, results may be empty.

## References

- Connector implementation: `sources/spotify/spotify.py`
- Connector API documentation: `sources/spotify/spotify_api_doc.md`
- Official Spotify Web API documentation:
  - https://developer.spotify.com/documentation/web-api
  - https://developer.spotify.com/documentation/web-api/concepts/authorization
  - https://developer.spotify.com/documentation/web-api/reference

