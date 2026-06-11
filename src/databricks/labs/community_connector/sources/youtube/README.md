# Lakeflow YouTube Community Connector

This documentation describes how to configure and use the **YouTube** Lakeflow community connector to ingest data from the YouTube Data API v3 into Databricks.

## Prerequisites

- **Google Cloud project**: A project in [Google Cloud Console](https://console.cloud.google.com/) with the YouTube Data API v3 enabled.
- **Authentication** (choose one):
  - **API key**: For public data only (channels by ID, playlists, videos, search). Create an API key in Google Cloud Console → APIs & Services → Credentials.
  - **OAuth 2.0**: For private or user-specific data (`mine=true`, subscriptions, activities for the authenticated user). Create OAuth 2.0 client credentials (Web application) and use the authenticate script to obtain a refresh token.
- **Network access**: The environment running the connector must be able to reach `https://www.googleapis.com/youtube/v3`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide **one** authentication method when configuring the connection (`connector_spec.yaml` defines two groups):

| Auth method | Parameters (all required within the group) | Use for |
|-------------|--------------------------------------------|---------|
| **api_key** | `api_key` | Public data (channels by ID, playlists, videos, search) |
| **oauth** | `client_id`, `client_secret`, `refresh_token` | Private / user data (`mine=true`, subscriptions, activities) |

Also set `externalOptionsAllowList` on the connection (comma-separated table option names). This connector requires table-specific options, so this parameter must be set. See list below.

The full list of supported table-specific options for `externalOptionsAllowList` is:

`channel_id,channel_ids,chart,for_username,max_pages,max_results,mine,order,playlist_id,playlist_ids,published_after,q,region_code,type,video_category_id,video_id,video_ids`

> **Note**: Table-specific options such as `channel_ids`, `playlist_id`, or `q` are **not** connection parameters. They are provided per table via `table_configuration` in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining Credentials

**API key (public data only):**

1. In [Google Cloud Console](https://console.cloud.google.com/), select your project (or create one).
2. Enable **YouTube Data API v3**: APIs & Services → Library → search "YouTube Data API v3" → Enable.
3. Go to **APIs & Services → Credentials** → Create credentials → API key.
4. Copy the key and store it securely. Use it as the `api_key` connection option. Restrict the key to YouTube Data API v3 if desired.

**OAuth (for mine=true, subscriptions, activities, etc.):**

1. In Google Cloud Console, enable YouTube Data API v3 as above.
2. Go to **APIs & Services → Credentials** → Create credentials → OAuth client ID.
3. Choose **Web application**. Add the redirect URI: **`http://localhost:9876/oauth/callback`** (or the port shown when you run the authenticate script in browser mode).
4. Note the **Client ID** and **Client secret**.
5. Run the authenticate script to obtain a refresh token:

   ```bash
   python tools/scripts/authenticate.py -s youtube -m browser
   ```

   The script starts a local web server and prints the redirect URI to register. Open the URL in a browser, enter your client ID and client secret, complete the Google sign-in and consent, and the script will output a JSON object containing `refresh_token`. Store `client_id`, `client_secret`, and `refresh_token` in your connection configuration.

**Redirect URI for Google OAuth:** When using browser mode, the script uses **`http://localhost:9876/oauth/callback`** by default (port 9876). You must add this exact URI to your OAuth 2.0 client's authorized redirect URIs in Google Cloud Console.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to:  
   `channel_id,channel_ids,chart,for_username,max_pages,max_results,mine,order,playlist_id,playlist_ids,published_after,q,region_code,type,video_category_id,video_id,video_ids`  
   (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

### Using the Connector in Databricks (Unity Catalog / Pipeline)

When you use this connector in Databricks with Unity Catalog or a declarative pipeline:

- **Repository URL**: Set to the URL of the repository that contains this connector source (e.g. the repo where `src/databricks/labs/community_connector/sources/youtube/` lives).
- **Branch**: Set to the branch that contains the connector code (e.g. `main` or `feature/youtube`).
- **Connector name**: Set so the UI can load the connector specification. The connector is identified by the `youtube` source folder; ensure the pipeline or connection points to the path that contains `connector_spec.yaml` (e.g. the `youtube` source directory). The UI uses this to load `connector_spec.yaml` and display connection parameters.

## Supported Objects

The YouTube connector exposes **nine** tables from the YouTube Data API v3:

| Table | Description | Ingestion Type | Primary Key |
|-------|-------------|----------------|--------------|
| `channels` | Channel metadata (snippet, statistics, contentDetails) | snapshot | `id` |
| `playlists` | Playlist metadata per channel or by ID | snapshot | `id` |
| `playlist_items` | Items in a playlist (videos) | snapshot | `id` |
| `videos` | Video metadata by ID or chart (e.g. mostPopular) | snapshot | `id` |
| `search` | Search results (videos, channels, playlists) | snapshot | `search_query`, `result_index` (0-based position in the result set) |
| `activities` | Channel activities (uploads, likes, etc.) | snapshot | `id` |
| `comment_threads` | Top-level comments on a video or channel | snapshot | `id` |
| `subscriptions` | Channel subscriptions (who subscribes or mine) | snapshot | `id` |
| `video_categories` | Video category list by region | snapshot | `id` |

### Pagination

All tables are **snapshot**: each pipeline run performs a full re-read. The YouTube API paginates with `pageToken` / `nextPageToken`, but that pagination stays **inside** the connector — it is not exposed to the Lakeflow framework as an incremental cursor.

**Drain-all snapshot** (all tables except `videos` by `video_ids`):

- One `read_table` call loops API pages until `nextPageToken` is absent or `max_pages` is reached.
- Returns all accumulated rows with offset `None` (snapshot termination).

**`videos` with `video_ids`**: connector chunks comma-separated IDs into batches of 50 (API limit), issues one request per batch, and returns offset `None`.

**`published_after` (activities, search):** optional **query filter** (ISO 8601 → API `publishedAfter`), not a framework cursor. `activities` ingestion is snapshot; each run re-reads unless you narrow the window with `published_after`.

**Search primary keys:** `(search_query, result_index)` uses position in the result list (`"0"`, `"1"`, …). This gives stable keys for snapshot + SCD_TYPE_1 **when YouTube returns the same result order**. If ordering changes between runs, the same index may refer to a different item.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|--------|----------|-------------|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

| Option | Required | Description |
|--------|----------|-------------|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

Table-specific options are passed via the pipeline spec under `table_configuration`. Required options depend on the table.

| Table | Required / optional options | Description |
|-------|-----------------------------|-------------|
| **channels** | **Exactly one of:** `channel_ids`, `for_username`, or `mine=true` | `channel_ids`: comma-separated channel IDs. `for_username`: YouTube username. `mine=true`: authenticated user's channel (OAuth). |
| **playlists** | **Exactly one of:** `playlist_ids`, `channel_id`, or `mine=true` | `playlist_ids`: comma-separated playlist IDs. `channel_id`: list playlists for this channel. `mine=true`: authenticated user's playlists (OAuth). |
| **playlist_items** | `playlist_id` (required) | The playlist ID whose items to list. Optional: **`max_pages`** (cap pages, e.g. `"20"` = at most 1,000 items). |
| **videos** | **Exactly one of:** `video_ids` or `chart=mostPopular` | `video_ids`: comma-separated video IDs (connector batches in chunks of 50 IDs/request). `chart=mostPopular`: popular videos. Optional: `region_code`, `video_category_id` (with chart), **`max_pages`** (cap pages when using chart, e.g. `"10"` = at most 500 results). |
| **search** | `q` (required) | Search query string. Optional: `type`, `channel_id`, `published_after`, `order`, **`max_pages`** (cap total pages, e.g. `"10"` = at most 10×50 = 500 results). |
| **activities** | **Exactly one of:** `channel_id` or `mine=true` | `channel_id`: list activities for this channel. `mine=true`: authenticated user's activities (OAuth). Optional: `published_after`. |
| **comment_threads** | **Exactly one of:** `video_id` or `channel_id` | `video_id`: comments for this video (recommended). Use a video from your channel: open the video on YouTube, copy the ID from the URL (`?v=VIDEO_ID`). If you get 403, that video may have comments disabled—try another. `channel_id`: often returns 403; prefer `video_id`. |
| **subscriptions** | **Exactly one of:** `channel_id` or `mine=true` | `channel_id`: list subscribers of this channel. `mine=true`: channels the authenticated user subscribes to (OAuth). |
| **video_categories** | `region_code` (required) | ISO 3166-1 alpha-2 region code (e.g. `US`, `GB`). Required by the YouTube API (`regionCode` filter). |

**`max_pages` (drain-all tables only):** Caps internal page fetches for **search** (default 10), **playlist_items** (default 100), and **videos** chart (default 20). Example: `"max_pages": "10"` with 50 results per page → at most 500 rows. Set in `table_configuration`. Optionally set **`max_results`** for page size (1–50 for most tables, 1–100 for `comment_threads`).

## Data Type Mapping

YouTube API responses use strings for many numeric and date fields. The connector maps them as follows:

| YouTube API | Connector / Databricks | Notes |
|-------------|------------------------|-------|
| String (id, title, description, etc.) | string | Preserved as-is. |
| Numeric counts (e.g. `statistics.viewCount`) | string | API returns strings; cast to integer/long in SQL if needed. |
| ISO 8601 datetime (`publishedAt`, etc.) | string | Cast to timestamp in downstream processing if needed. |
| Nested objects (snippet, statistics, contentDetails) | Flattened columns | e.g. `snippet_title`, `statistics_viewCount`, `contentDetails_relatedPlaylists_uploads`. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the YouTube connector source in your workspace so that the connector code (including `connector_spec.yaml`) is available for the pipeline.

### Step 2: Configure Your Pipeline

In your pipeline specification, reference the Unity Catalog connection configured with your YouTube credentials and set one or more tables with the required table options.

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "youtube_connection",
    "object": [
      {
        "table": {
          "source_table": "channels",
          "table_configuration": {
            "channel_ids": "UC_x5XG1OV2P6uZZ5FSM9Ttw"
          }
        }
      },
      {
        "table": {
          "source_table": "playlists",
          "table_configuration": {
            "channel_id": "UC_x5XG1OV2P6uZZ5FSM9Ttw"
          }
        }
      },
      {
        "table": {
          "source_table": "playlist_items",
          "table_configuration": {
            "playlist_id": "PLrAXtmErZgOeiKm4sgNOknGvNjby9efdfg"
          }
        }
      },
      {
        "table": {
          "source_table": "videos",
          "table_configuration": {
            "chart": "mostPopular",
            "region_code": "US"
          }
        }
      },
      {
        "table": {
          "source_table": "search",
          "table_configuration": {
            "q": "databricks"
          }
        }
      },
      {
        "table": {
          "source_table": "video_categories",
          "table_configuration": {
            "region_code": "US"
          }
        }
      }
    ]
  }
}
```

For OAuth-only tables (`mine=true`), ensure the connection uses `client_id`, `client_secret`, and `refresh_token`. Then you can add tables such as:

```json
{
  "table": {
    "source_table": "channels",
    "table_configuration": { "mine": "true" }
  }
},
{
  "table": {
    "source_table": "subscriptions",
    "table_configuration": { "mine": "true" }
  }
}
```

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your usual Lakeflow or Databricks orchestration. Each snapshot table drains all API pages in one `read_table` call and returns `None` offset so the framework terminates that read. Scheduled runs start fresh unless you narrow the window with options like `published_after`.

#### Best Practices

- **Start small**: Ingest one or two tables (e.g. `channels`, `video_categories`) to validate credentials and schema.
- **Respect quota**: YouTube Data API v3 has a default quota of 10,000 units/day. `search.list` costs 100 units per request; other list calls cost 1 unit. Plan schedules and filters accordingly.
- **Use API key for public data**: For channels, playlists, videos by ID, and search, an API key is sufficient. Use OAuth only when you need `mine=true` or user-specific data.

#### Troubleshooting

**Common issues:**

- **401 / 403 with OAuth**: Verify `client_id`, `client_secret`, and `refresh_token`. Re-run the authenticate script (`authenticate.py -s youtube -m browser`) to obtain a new refresh token if it was revoked or expired.
- **403 with API key**: Check that the API key is valid and that YouTube Data API v3 is enabled in your Google Cloud project. Restrict the key to the YouTube API if you have key restrictions.
- **Quota exceeded**: The connector raises a clear error when the API returns `quotaExceeded`. Reduce frequency of syncs, especially for `search` (100 units per request), or request a quota increase in Google Cloud Console.
- **"Found 2 rows for key" (search `search_query` / `result_index`)**: The connector emits each `result_index` (0, 1, 2, … per query) only once per read. If the same key appears twice, the pipeline is delivering the same batch twice (e.g. duplicate task, retry, or stream replayed). **Fix**: Deduplicate the search source before merge (e.g. by `(search_query, result_index)`) in the pipeline, or ensure exactly-once processing so the same connector output is not written twice.
- **"channels requires channel_ids, for_username, or mine=true"** (or similar): Ensure exactly one of the required table options is set for that table in `table_configuration`.
- **"api_key ... not both"**: The connection has API key and OAuth credentials. Use one auth method only (`api_key` for public data, or OAuth for `mine=true`).

### Running Tests

From the project root, run the YouTube connector tests with:

```bash
pytest tests/unit/sources/youtube/ -v
```

Credentials are read from `tests/unit/sources/youtube/configs/dev_config.json` (or the path configured for the test harness). Use an API key or OAuth credentials as required by the tests you run.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/youtube/youtube.py`
- Connector API documentation and schemas: `src/databricks/labs/community_connector/sources/youtube/youtube_api_doc.md`
- [YouTube Data API v3 – Overview](https://developers.google.com/youtube/v3/getting-started)
- [YouTube Data API v3 – Channels: list](https://developers.google.com/youtube/v3/docs/channels/list)
- [YouTube Data API v3 – Playlists: list](https://developers.google.com/youtube/v3/docs/playlists/list)
- [YouTube Data API v3 – PlaylistItems: list](https://developers.google.com/youtube/v3/docs/playlistItems/list)
- [YouTube Data API v3 – Videos: list](https://developers.google.com/youtube/v3/docs/videos/list)
- [YouTube Data API v3 – Search: list](https://developers.google.com/youtube/v3/docs/search/list)
- [YouTube Data API v3 – Activities: list](https://developers.google.com/youtube/v3/docs/activities/list)
- [YouTube Data API v3 – CommentThreads: list](https://developers.google.com/youtube/v3/docs/commentThreads/list)
- [YouTube Data API v3 – Subscriptions: list](https://developers.google.com/youtube/v3/docs/subscriptions/list)
- [YouTube Data API v3 – VideoCategories: list](https://developers.google.com/youtube/v3/docs/videoCategories/list)
