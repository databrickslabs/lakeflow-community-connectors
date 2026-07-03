# Lakeflow WordPress Community Connector

This documentation describes how to configure and use the **WordPress** Lakeflow community connector to ingest data from the [WordPress REST API](https://developer.wordpress.org/rest-api/) (`/wp-json/wp/v2/`) into Databricks.

The connector targets **WordPress core** (self-hosted, WordPress 5.6+) and authenticates with **Application Passwords** over HTTP Basic Auth. It is **read-only** — only `GET` endpoints are used.

## Prerequisites

- **WordPress site (5.6 or later)**: A self-hosted WordPress site with the REST API enabled and reachable over **HTTPS**. WordPress core only honors Application Passwords over HTTPS; plain HTTP requests are rejected. Application Passwords have shipped in WordPress core since 5.6.
- **WordPress user account**: A user with at least the `read` capability. The connector inherits the capabilities of this user, which directly affects how many rows and which fields are visible (see [Supported Objects](#supported-objects)).
- **Application Password**: Generated for that user in the WordPress admin panel and supplied to the connector as the `application_password` option.
- **Network access**: The environment running the connector must be able to reach your site's `https://<your-site>/wp-json/wp/v2/` endpoints.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the connection parameters exposed by the connector.

| Name                  | Type   | Required | Description                                                                                                                                        | Example                                  |
|-----------------------|--------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|
| `base_url`            | string | yes      | The WordPress site **root** URL (scheme + host, no trailing `/wp-json`). The connector appends `/wp-json/wp/v2/` for REST API access.               | `https://example.com`                    |
| `username`            | string | yes      | WordPress user's username for authentication. The user must have at least `read` capability.                                                       | `connector_user`                         |
| `application_password`| string | yes (secret) | Application Password generated in the WordPress admin panel (Users → Profile → Application Passwords). Used with HTTP Basic Auth over HTTPS. Space-grouped string (spaces are stripped internally, so it may be sent with or without them). | `xxxx xxxx xxxx xxxx xxxx xxxx`          |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to pass through to the connector. This connector uses table-specific options, so this parameter must be set. | `per_page,start_timestamp,num_partitions,window_seconds,lookback_seconds` |

The full, definitive list of supported table-specific options for `externalOptionsAllowList` is:

`per_page,start_timestamp,num_partitions,window_seconds,lookback_seconds`

> **Note**: Table-specific options such as `start_timestamp`, `num_partitions`, or `per_page` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **Site base URL (`base_url`)**:
  - Use the site root only, e.g. `https://example.com`. Do **not** include `/wp-json` or a trailing slash — the connector appends `/wp-json/wp/v2/<resource>` per request (an accidental `/wp-json` suffix or trailing slash is trimmed automatically).
  - The site must be served over **HTTPS**.
- **Username (`username`)**:
  - The login (username) of the WordPress user whose Application Password you generate. For read-only ingestion, prefer a low-privilege user with the `read` capability.
- **Application Password (`application_password`)**:
  1. Sign in to WordPress admin (`wp-admin`) as the user (or as an administrator managing that user).
  2. Navigate to **Users → Profile** (or **Users → All Users → *edit user***).
  3. Scroll to the **Application Passwords** section.
  4. Enter a name for the application (e.g. `lakeflow-connector`) and click **Add New Application Password**.
  5. Copy the generated 24-character, space-grouped secret (e.g. `xxxx xxxx xxxx xxxx xxxx xxxx`) and store it securely. It is shown only once. Use it as the `application_password` connection option.
  - Alternatively, generate one via the authenticated REST call `POST /wp-json/wp/v2/users/<id>/application-passwords`.

> **Capabilities affect what you can read.** The session inherits the capabilities of the configured user. A low-privilege (`read`) user sees only public content and public fields. Grant `edit_posts` / `moderate_comments` / `list_users` only if you need non-public statuses (draft/private/trash) or protected fields (comment author email/IP, full user PII). This changes **row counts**, not just field visibility.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `per_page,start_timestamp,num_partitions,window_seconds,lookback_seconds` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The WordPress connector exposes a **static list** of tables:

- `posts`
- `pages`
- `media`
- `comments`
- `categories`
- `tags`
- `users`
- `taxonomies`

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode, primary key, and (where applicable) incremental cursor for each table:

| Table        | Description                                              | Ingestion Type | Primary Key | Incremental Cursor (if any) |
|--------------|----------------------------------------------------------|----------------|-------------|------------------------------|
| `posts`      | Blog posts                                               | `cdc`          | `id`        | `modified_gmt`               |
| `pages`      | Static (hierarchical) pages                              | `cdc`          | `id`        | `modified_gmt`               |
| `media`      | Media library / attachment items                         | `cdc`          | `id`        | `modified_gmt`               |
| `comments`   | Comments across all commentable post types               | `append`       | `id`        | `date_gmt`                   |
| `categories` | Hierarchical taxonomy terms                              | `snapshot`     | `id`        | n/a                          |
| `tags`       | Flat (non-hierarchical) taxonomy terms                   | `snapshot`     | `id`        | n/a                          |
| `users`      | Authors / site users                                     | `snapshot`     | `id`        | n/a                          |
| `taxonomies` | Registered taxonomy definitions (metadata, dict-shaped)  | `snapshot`     | `slug`      | n/a                          |

**Ingestion strategy notes:**

- **`posts` / `pages` / `media` (`cdc`)**: WordPress exposes a real update cursor (`modified_gmt`) via the `modified_after` / `modified_before` query filters, so these tables re-surface edited records. Deletes are **not** detected by default — WordPress core exposes no deleted-records feed to a low-privilege connector, so `cdc_with_deletes` is not enabled.
- **`comments` (`append`)**: The only available cursor is the creation time (`date_gmt`, via `after` / `before`). Comments have **no `modified` field**, so edits or moderation-status changes to already-synced comments are not re-surfaced incrementally. New comments are captured; a full re-read is the only way to catch backdated changes.
- **`categories` / `tags` / `users` (`snapshot`)**: Terms and users have **no date/modified field at all**, so each sync does a full page-through. These are typically small.
- **`taxonomies` (`snapshot`)**: A metadata dictionary keyed by taxonomy slug (`category`, `post_tag`, plus any custom taxonomies). The connector flattens `{slug: {...}}` into rows, promoting the dict key to a `slug` column (the primary key). Effectively static per site.

### Schema highlights

Schemas are static per WordPress core and are defined by the connector. Only `context=view` (unauthenticated-safe) fields are modeled. **Edit-only** fields (`raw` bodies, `password`, user PII, comment `author_email` / `author_ip`), HATEOAS `_links`, and the free-form `meta` object are intentionally omitted because their shapes are install-specific.

- **Timestamps**: Prefer the `_gmt` fields (`date_gmt`, `modified_gmt`) — they are UTC and are used as the incremental cursors. The non-GMT siblings (`date`, `modified`) are site-local. All are modeled as `TimestampType`.
- **Rendered content**: `title`, `content`, `excerpt`, `guid`, `description`, `caption` are structs. `content` / `excerpt` carry a `rendered` string plus a `protected` boolean; the others carry just `rendered`.
- **`posts`**: includes `categories` and `tags` as `array<long>` foreign-key arrays, plus `author` and `featured_media` foreign keys.
- **`pages`**: like `posts` but hierarchical — has `parent` and `menu_order`, and no `categories` / `tags` / `sticky` / `format`.
- **`media`**: includes `media_type`, `mime_type`, `source_url` (direct file URL), and a shallow `media_details` struct (`width`, `height`, `file`, `filesize`).
- **`comments`**: `post` and `parent` foreign keys; `author` is `0` for anonymous commenters (with `author_name` set).
- **`categories`** has a `parent` field; **`tags`** does not (flat taxonomy).

You usually do not need to customize the schema; it is static and driven by the connector implementation.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

Table-specific options are passed via the pipeline spec under `table_configuration`. All of them are optional; sensible defaults are applied.

| Option | Type | Applies to | Default | Description |
|---|---|---|---|---|
| `per_page` | integer | all tables | `100` | Page size for WordPress pagination. WordPress caps this at `100` server-side; values above 100 are clamped, values below 1 are raised to 1. |
| `start_timestamp` | ISO 8601 string | `posts`, `pages`, `media`, `comments` | earliest (`1970-01-01T00:00:00Z`) | Initial cursor used for the first micro-batch when no offset is stored yet. Set to a recent cutoff to limit backfill history. Compared as UTC. |
| `num_partitions` | integer | `posts`, `pages`, `media`, `comments` | `4` | Number of contiguous, disjoint time-window partitions the cursor range is split into for distributed reads across executors. |
| `window_seconds` | integer | `posts`, `pages`, `media`, `comments` | `0` (disabled) | When set, advances the offset by one sliding time-window of this many seconds per micro-batch, bounding batch size. When `0`, the offset jumps straight to the init-time high-water mark. |
| `lookback_seconds` | integer | `posts`, `pages`, `media`, `comments` | `0` | Lookback applied only at the lower bound of the cursor range to re-capture records edited during a prior window. Handles clock skew / DST edge cases. The stored cursor is never widened by this. |

The snapshot tables (`categories`, `tags`, `users`, `taxonomies`) accept `per_page` but ignore the cursor/partition options, since they have no incremental filter and are read in full each sync.

## Data Type Mapping

WordPress REST JSON fields are mapped to Spark types as follows:

| WordPress / REST JSON Type | Example Fields | Connector Spark Type | Notes |
|---|---|---|---|
| integer | `id`, `author`, `featured_media`, `count`, `parent` | `LongType` | All numeric IDs and foreign keys stored as `LongType` to avoid overflow. |
| string | `slug`, `status`, `link`, `alt_text`, `mime_type` | `StringType` | Includes enum-valued fields (`status`, `format`, `media_type`, `comment_status`). |
| ISO 8601 datetime (string) | `date`, `date_gmt`, `modified`, `modified_gmt` | `TimestampType` | Prefer `_gmt` (UTC) fields; these are the incremental cursors. Non-GMT siblings are site-local. |
| boolean | `sticky`, `hierarchical`, `content.protected` | `BooleanType` | Standard `true`/`false`. |
| `object { rendered, protected? }` | `title`, `content`, `excerpt`, `guid`, `caption`, `description` | `StructType` | Rendered HTML preserved as a struct rather than flattened. |
| `object` (avatars / media details) | `avatar_urls`, `author_avatar_urls`, `media_details` | `StructType` | Avatar URLs keyed by pixel size (`24`/`48`/`96`); `media_details` modeled as a shallow scalar subset. |
| `array<integer>` | `categories`, `tags` (on posts) | `ArrayType(LongType)` | Foreign-key arrays preserved as nested collections. |
| `array<string>` | `types` (on taxonomies) | `ArrayType(StringType)` | |
| dict-keyed-by-slug object | `taxonomies` top-level response | rows with `slug` promoted | The connector flattens `{slug: {...}}` into rows, surfacing the dict key as `slug`. |

The connector is designed to:

- Prefer `LongType` for all identifier fields.
- Preserve nested JSON structures (rendered content, avatars, media details) as `StructType` instead of flattening them.
- Model only `context=view` fields; edit-only fields, `_links`, and free-form `meta` are omitted because their shapes are install-specific.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the WordPress connector source in your workspace. This will typically place the connector code (for example, `wordpress.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g. `ingest.py` or a similar entrypoint), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this WordPress connector.
- One or more **tables** to ingest, each with optional `table_configuration`.

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "wordpress_connection",
    "object": [
      {
        "table": {
          "source_table": "posts",
          "table_configuration": {
            "start_timestamp": "2024-01-01T00:00:00Z",
            "num_partitions": "4",
            "per_page": "100"
          }
        }
      },
      {
        "table": {
          "source_table": "comments",
          "table_configuration": {
            "start_timestamp": "2024-01-01T00:00:00Z"
          }
        }
      },
      {
        "table": {
          "source_table": "categories"
        }
      },
      {
        "table": {
          "source_table": "taxonomies"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your WordPress `base_url`, `username`, and `application_password`.
- For each `table`:
  - `source_table` must be one of the supported table names listed above.
  - Options such as `start_timestamp`, `num_partitions`, `window_seconds`, `lookback_seconds`, and `per_page` are placed under `table_configuration`.

You can ingest additional tables (e.g. `pages`, `media`, `tags`, `users`) by adding more `table` entries with the appropriate options.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g. a scheduled job or workflow). For the incremental (`cdc` / `append`) tables:

- On the **first run**, either:
  - Omit `start_timestamp` to backfill all data (may be heavy for long-lived sites), or
  - Set `start_timestamp` to a recent cutoff to limit history.
- On **subsequent runs**, the connector uses the stored cursor (`modified_gmt` for `cdc` tables, `date_gmt` for `comments`) plus `lookback_seconds` to pick up late updates safely.

#### Best Practices

- **Start small**: Begin with a single table (e.g. `posts`) to validate configuration and data shape before adding more.
- **Use incremental sync where possible**: `posts`, `pages`, and `media` support true CDC on `modified_gmt` — set a `start_timestamp` to bound the initial backfill.
- **Tune partitioning**: Increase `num_partitions` for large tables to spread the time-range read across more executors; use `window_seconds` to cap per-micro-batch size when catching up on a long history.
- **Use `_gmt` semantics**: Cursors and `start_timestamp` are compared in UTC. Apply a small `lookback_seconds` if you observe records skipped around DST boundaries or under clock skew.
- **Mind the user's capabilities**: The configured user's role determines which rows (public vs draft/private) and fields you see. Use a low-privilege `read` user for public content only.
- **Respect the host's rate limits**: WordPress core ships no built-in REST rate limiter, but hosting providers, WAFs/CDNs, or plugins may throttle. The connector retries `429`/`5xx` with exponential backoff (honoring `Retry-After`); widen schedules if you hit sustained throttling.

#### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`401` / `403`)**:
  - Verify `username` and `application_password` are correct and the Application Password has not been revoked.
  - Ensure the site is served over **HTTPS** — WordPress rejects Application Passwords over plain HTTP.
  - A `403` typically means the user is authenticated but lacks the capability for the requested data (e.g. non-public statuses or protected fields). The connector fails fast on `401`/`403` with an actionable message.
- **Fewer rows than expected**:
  - A low-privilege (`read`) user only sees public content. Draft/private/trashed content and full user PII require a user with the matching capability (`edit_posts`, `list_users`, etc.).
- **`404` on every request (including `wp-json/`)**:
  - A security/hardening plugin may have disabled the REST API. This means "REST API disabled on this site," not "site doesn't exist." Re-enable the `wp/v2` routes.
- **Rate limiting (`429`)**:
  - Comes from the hosting layer, a WAF/CDN, or a plugin (not WordPress core). The connector backs off automatically; if it persists, reduce concurrency or widen the schedule.
- **Missing comment edits**:
  - Comments are append-only on creation time and have no `modified` field, so edits and moderation-status changes to already-synced comments are not re-surfaced. Run a periodic full re-snapshot if you need to catch these.
- **WordPress.com sites**:
  - This connector targets the core `wp/v2` API, which works against self-hosted sites and WordPress.com sites that have not disabled it. WordPress.com's separate `public-api.wordpress.com/rest/v1.1/` API family (different pagination and OAuth2) is not supported.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/wordpress/wordpress.py`
- Connector schemas and per-table configuration: `src/databricks/labs/community_connector/sources/wordpress/wordpress_schemas.py`
- Connector API research and field-level provenance: `src/databricks/labs/community_connector/sources/wordpress/wordpress_api_doc.md`
- Official WordPress REST API documentation:
  - `https://developer.wordpress.org/rest-api/`
  - `https://developer.wordpress.org/rest-api/reference/posts/`
  - `https://developer.wordpress.org/rest-api/reference/pages/`
  - `https://developer.wordpress.org/rest-api/reference/media/`
  - `https://developer.wordpress.org/rest-api/reference/comments/`
  - `https://developer.wordpress.org/rest-api/reference/categories/`
  - `https://developer.wordpress.org/rest-api/reference/tags/`
  - `https://developer.wordpress.org/rest-api/reference/users/`
  - `https://developer.wordpress.org/rest-api/reference/taxonomies/`
  - `https://developer.wordpress.org/rest-api/using-the-rest-api/authentication/` (Application Passwords)
  - `https://developer.wordpress.org/rest-api/using-the-rest-api/pagination/`
