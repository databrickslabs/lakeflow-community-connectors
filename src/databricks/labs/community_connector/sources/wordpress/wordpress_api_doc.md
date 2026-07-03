# **WordPress API Documentation**

Scope: the **WordPress REST API** (`/wp-json/wp/v2/...`) as shipped in WordPress
core (self-hosted, WordPress 5.6+) for Application Passwords. WordPress.com
(hosted, `public-api.wordpress.com`) is noted as a variant where it differs
materially — see **Known Quirks & Edge Cases**. This is a **read-only**
connector; only `GET` endpoints are documented.

## **Authorization**

**Preferred method: Application Passwords (HTTP Basic Auth), WordPress ≥ 5.6.**

- WordPress core has shipped Application Passwords since 5.6 (May 2020). An
  admin generates one per user from **wp-admin → Users → Profile → Application
  Passwords**, or via the equivalent authenticated REST call
  (`POST /wp-json/wp/v2/users/<id>/application-passwords`, itself
  authenticated by some other means the first time).
- The generated secret is a 24-character, space-grouped string
  (e.g. `xxxx xxxx xxxx xxxx xxxx xxxx`); WordPress strips spaces internally,
  so callers may send it with or without them.
- Credentials are sent as standard **HTTP Basic Auth** (RFC 7617):
  `Authorization: Basic base64(username:application_password)`.
- **Requires HTTPS.** WordPress core only honors Application Passwords over an
  HTTPS connection (`is_ssl()` check); plain HTTP requests are rejected.
- The resulting session has the same **capabilities as the underlying WP
  user**. For read-only ingestion, create (or reuse) a low-privilege user with
  at least `read` capability; grant `edit_posts` / `moderate_comments` /
  `list_users` / `activate_plugins` only if the connector also needs
  non-public statuses (draft/private/trash content), full user email/roles,
  or the `plugins`/`themes`/`settings` endpoints.
- Example request:

```bash
curl -u "connector_user:xxxx xxxx xxxx xxxx xxxx xxxx" \
  "https://example.com/wp-json/wp/v2/posts?per_page=100&page=1"
```

  Or with an explicit `Authorization` header:

```bash
curl -H "Authorization: Basic $(printf '%s' 'connector_user:xxxxxxxxxxxxxxxxxxxxxxxx' | base64)" \
  "https://example.com/wp-json/wp/v2/posts?per_page=100"
```

**Alternatives (not chosen, noted for completeness):**

| Method | Suitability for a headless connector | Notes |
|---|---|---|
| Cookie + nonce auth | Poor | Requires a logged-in browser session; nonces expire; not usable server-to-server. |
| Basic Auth plugin (`WP-API/Basic-Auth`) | Not recommended | Explicitly documented by WordPress as "for development, not production" — sends raw user password on every request and has no official support. |
| OAuth1.0a / OAuth2 plugins (e.g. `WP OAuth Server`, WordPress.com OAuth2) | Viable, higher setup cost | Third-party plugin (self-hosted) or built-in (WordPress.com only). Requires registering a client app and running a token exchange; more moving parts than Application Passwords for a self-hosted site with no user-facing consent screen needed. |
| JWT plugins (e.g. `JWT Authentication for WP REST API`) | Viable, not core | Not part of WordPress core; requires installing/configuring a third-party plugin and a signing secret in `wp-config.php`. Application Passwords achieve the same headless goal without a plugin dependency. |

The connector's `dev_config.json` / connection parameters should hold:

```json
{
  "base_url": "https://example.com",
  "username": "connector_user",
  "application_password": "xxxx xxxx xxxx xxxx xxxx xxxx"
}
```

`base_url` is the site root (scheme + host, no trailing `/wp-json`); the
connector appends `/wp-json/wp/v2/<resource>` per request.

## **Object List**

The object list is **discoverable via API** (the REST index) but is also
stable/well-known enough to hardcode for `wp/v2`. Discovery request:

```bash
curl "https://example.com/wp-json/"
```

Response (abridged) includes a `namespaces` array (must contain `wp/v2`) and
a `routes` object keyed by path, e.g. `"/wp/v2/posts"`, each entry listing
supported HTTP methods and endpoint args. A namespace-scoped index is also
available at `GET /wp-json/wp/v2/` for just the core routes.

**Recommended objects for this connector** (cross-referenced against
Airbyte's `source-wordpress` low-code connector and general Fivetran/Airbyte
coverage of blog/CMS platforms — posts, pages, comments, taxonomy terms,
media, and users are the objects every major connector for a CMS-style
source exposes):

| Object (table) | Endpoint | Nesting | Notes |
|---|---|---|---|
| `posts` | `GET /wp/v2/posts` | top-level | Blog posts. |
| `pages` | `GET /wp/v2/pages` | top-level | Static pages (hierarchical via `parent`). |
| `comments` | `GET /wp/v2/comments` | top-level, filterable by `post` | Comments across all post types. |
| `categories` | `GET /wp/v2/categories` | top-level taxonomy | Hierarchical taxonomy terms (via `parent`). |
| `tags` | `GET /wp/v2/tags` | top-level taxonomy | Flat (non-hierarchical) taxonomy terms. |
| `media` | `GET /wp/v2/media` | top-level | Attachment/media library items; a `post` (attachment page's parent) subtype of the general "post" object. |
| `users` | `GET /wp/v2/users` | top-level | Authors/site users. |
| `taxonomies` | `GET /wp/v2/taxonomies` | top-level, metadata | Enumerates registered taxonomies (`category`, `post_tag`, plus custom ones). Small, mostly static. |

**Additional / lower-priority objects** (exist in core, documented lightly —
metadata/config-shaped, not row-oriented business data, several require
elevated capabilities):

| Object | Endpoint | Requires auth capability | Notes |
|---|---|---|---|
| `types` | `GET /wp/v2/types` | none (public types) | Registered post types (`post`, `page`, `attachment`, custom post types). Static-ish, small. |
| `statuses` | `GET /wp/v2/statuses` | none (public statuses) | Registered post statuses (`publish`, `future`, `draft`, ...). |
| `page_revisions` | `GET /wp/v2/pages/<id>/revisions` (also `posts/<id>/revisions`) | `edit_posts` on that item | Per-object sub-resource; must be paginated per parent page/post — expensive to enumerate at scale. |
| `plugins` | `GET /wp/v2/plugins` | `activate_plugins` | Site config, not content. |
| `themes` | `GET /wp/v2/themes` | `edit_themes` (or `switch_themes`) | Site config, not content. |
| `settings` | `GET /wp/v2/settings` | `manage_options` | Singleton object (site title, timezone, etc.), no list/pagination. |

Custom post types and custom taxonomies registered by themes/plugins appear
as additional routes under `wp/v2/<custom-type>` with the **same shape and
pagination model as `posts`/`categories`** — discoverable via the `types` /
`taxonomies` endpoints above. Not enumerated here since they are
installation-specific; the connector should treat any `wp/v2` route whose
schema resembles `posts` (has `id`, `date`, `modified`, `status`) the same
way it treats `posts`.

**Deferred / out of scope for this pass:** block-editor "reusable blocks"
(`wp/v2/blocks`) and custom-post-type discovery beyond the built-ins above —
both are optional, low-value, or installation-specific; add on request using
the same patterns documented for `posts`/`pages`.

## **Object Schema**

Schemas are **static per WordPress core** (defined by `WP_REST_Controller`
subclasses) but can also be introspected via `OPTIONS` on any route, e.g.
`OPTIONS /wp-json/wp/v2/posts`, which returns a JSON Schema under
`schema.properties`. Below are the field lists for the core objects (default
`context=view`, i.e. unauthenticated-safe fields; fields only present under
`context=edit` — which requires auth with edit capability on that object —
are marked **[edit-only]**).

### `posts`

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Primary key. |
| `date` | string (date-time, site TZ) | Publish date. `null` for scheduled/draft with no date. |
| `date_gmt` | string (date-time, UTC) | |
| `guid` | object `{ rendered }` | Not a stable dedup key across environments; use `id`. |
| `modified` | string (date-time, site TZ) | Last modification date — the incremental cursor field. |
| `modified_gmt` | string (date-time, UTC) | |
| `slug` | string | |
| `status` | string enum | `publish`, `future`, `draft`, `pending`, `private` (+ `trash` if requested explicitly, auth required). |
| `type` | string | Always `"post"` on this endpoint. |
| `link` | string (URI) | Public permalink. |
| `title` | object `{ rendered, raw [edit-only] }` | |
| `content` | object `{ rendered, raw [edit-only], protected (bool) }` | |
| `excerpt` | object `{ rendered, raw [edit-only], protected (bool) }` | |
| `author` | integer | FK → `users.id`. |
| `featured_media` | integer | FK → `media.id` (0 if none). |
| `comment_status` | string enum | `open` / `closed`. |
| `ping_status` | string enum | `open` / `closed`. |
| `sticky` | boolean | |
| `template` | string | Theme template file; often empty string. |
| `format` | string enum | `standard`, `aside`, `chat`, `gallery`, `link`, `image`, `quote`, `status`, `video`, `audio`. |
| `categories` | array\<integer\> | FKs → `categories.id`. |
| `tags` | array\<integer\> | FKs → `tags.id`. |
| `meta` | object | Custom fields registered with `show_in_rest`; shape varies per site. |
| `password` | string | **[edit-only]**, present only when set. |
| `_links` | object | HATEOAS links (`self`, `collection`, `author`, `replies`, `version-history`, `wp:featuredmedia`, `wp:term`, ...). Not typically materialized as columns. |

### `pages`

Same controller family as `posts` (`WP_REST_Posts_Controller` for the `page`
post type) with these differences:

| Field | Type | Notes |
|---|---|---|
| `id`, `date`, `date_gmt`, `guid`, `modified`, `modified_gmt`, `slug`, `status`, `type`, `link`, `title`, `content`, `excerpt`, `author`, `featured_media`, `comment_status`, `ping_status`, `template`, `meta` | — | Same semantics as `posts` above; `type` is `"page"`. |
| `parent` | integer | FK → `pages.id` (0 = top-level); pages are hierarchical, posts are not. |
| `menu_order` | integer | Manual ordering. |
| *(no `categories`, `tags`, `format`, `sticky`)* | — | Pages don't support those taxonomies/fields by default. |

### `comments`

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Primary key. |
| `post` | integer | FK → `posts.id` (or any commentable post type's id). |
| `parent` | integer | FK → `comments.id` for threaded replies (0 = top-level). |
| `author` | integer | FK → `users.id`; `0` for non-logged-in commenters. |
| `author_name` | string | Display name of commenter (used when `author` is 0). |
| `author_email` | string | **Requires auth** (`moderate_comments`) to view. |
| `author_url` | string | |
| `author_ip` | string | **Requires auth.** |
| `author_user_agent` | string | **[edit-only]**. |
| `date` | string (date-time, site TZ) | Comment creation date — the only cursor field available (see Known Quirks: no `modified`/`modified_gmt` field exists on comments). |
| `date_gmt` | string (date-time, UTC) | |
| `content` | object `{ rendered, raw [edit-only] }` | |
| `link` | string (URI) | |
| `status` | string | `approved`/`hold`/`spam`/`trash` internally; the REST field surfaces as `approve`/`hold` etc. and requires auth to see non-`approve` items. |
| `type` | string | `comment`, `pingback`, or `trackback`. |
| `author_avatar_urls` | object | Keyed by pixel size (`24`, `48`, `96`) → URL. |
| `meta` | object | Custom comment meta registered with `show_in_rest`. |

### `categories` / `tags`

Both are taxonomy-term objects (`WP_REST_Terms_Controller`), same shape,
under `wp/v2/categories` (`taxonomy=category`, hierarchical) and
`wp/v2/tags` (`taxonomy=post_tag`, flat):

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Primary key. |
| `count` | integer | Number of published posts using this term. |
| `description` | string | |
| `link` | string (URI) | |
| `name` | string | |
| `slug` | string | |
| `taxonomy` | string | `"category"` or `"post_tag"` respectively (constant per endpoint). |
| `parent` | integer | **Categories only** — FK → `categories.id`. Not present on `tags` (flat taxonomy). |
| `meta` | object | |

There is **no `modified`/`date` timestamp field on terms at all** — see
Known Quirks.

### `media`

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Primary key. |
| `date`, `date_gmt`, `modified`, `modified_gmt` | string (date-time) | `modified`/`modified_gmt` is the incremental cursor field. |
| `guid` | object `{ rendered }` | |
| `slug` | string | |
| `status` | string | Usually `inherit` (attachments inherit parent post's visibility). |
| `type` | string | Always `"attachment"`. |
| `link` | string (URI) | The attachment's own page, not the file URL. |
| `title` | object `{ rendered }` | |
| `author` | integer | FK → `users.id`. |
| `comment_status`, `ping_status` | string enum | |
| `template` | string | |
| `meta` | object | |
| `description` | object `{ rendered }` | |
| `caption` | object `{ rendered }` | |
| `alt_text` | string | |
| `media_type` | string enum | `image`, `file` (audio/video/other collapse to `file` at this level; refine with `mime_type`). |
| `mime_type` | string | E.g. `image/jpeg`, `application/pdf`. |
| `media_details` | object | Nested, shape varies by `media_type`: for images — `width`, `height`, `file`, `filesize`, `sizes` (map of size-name → `{file, width, height, mime_type, source_url}`), `image_meta` (EXIF-ish data); for other types — `length`/`length_formatted`, `file`. |
| `post` | integer\|null | FK → parent `posts.id`/`pages.id` this attachment belongs to (`null` if unattached). |
| `source_url` | string (URI) | Direct URL to the file itself. |
| `missing_image_sizes` | array\<string\> | **[edit-only]**. |

### `users`

`context=view` (unauthenticated / public) fields:

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | Display name. |
| `url` | string | |
| `description` | string | |
| `link` | string (URI) | Author archive page. |
| `slug` | string | |
| `avatar_urls` | object | Keyed by pixel size → URL. |
| `meta` | object | |

`context=edit` (requires `list_users` capability) adds:

| Field | Type | Notes |
|---|---|---|
| `username` | string | |
| `first_name`, `last_name`, `nickname` | string | |
| `email` | string | |
| `locale` | string | |
| `registered_date` | string (date-time) | |
| `roles` | array\<string\> | E.g. `["administrator"]`. |
| `capabilities` | object | Map of capability name → boolean. |
| `extra_capabilities` | object | |

There is **no `modified` field on users** — see Known Quirks.

### `taxonomies` / `types` / `statuses` (metadata, singleton-per-key)

These are not paginated lists of rows but a **dict of registered
definitions**, e.g. `GET /wp/v2/taxonomies` returns an object keyed by
taxonomy slug (`category`, `post_tag`, ...), each value shaped like:

| Field | Type | Notes |
|---|---|---|
| `name` | string | |
| `slug` | string | Also the dict key — treat as primary key when flattened to rows. |
| `description` | string | |
| `types` | array\<string\> | Post types this taxonomy applies to. |
| `hierarchical` | boolean | |
| `rest_base` | string | Path segment under `wp/v2/`. |

`types` and `statuses` follow the same "dict keyed by slug" shape with
type/status-specific fields (`types`: `hierarchical`, `rest_base`,
`supports`; `statuses`: `public`, `queryable`, `show_in_list`).

## **Get Object Primary Keys**

Primary keys are **static per object** (not separately discoverable via a
dedicated "keys" endpoint; inferred from the JSON Schema returned by
`OPTIONS <route>`, where `id` is marked `"readonly": true` and used in the
route's `args` as `{id}`):

| Object | Primary Key |
|---|---|
| `posts` | `id` |
| `pages` | `id` |
| `comments` | `id` |
| `categories` | `id` |
| `tags` | `id` |
| `media` | `id` |
| `users` | `id` |
| `taxonomies` | `slug` |
| `types` | `slug` |
| `statuses` | `slug` |
| `page_revisions` | `id` (unique only within a given parent page/post — compound key is `(parent_id, id)`) |
| `plugins` | `plugin` (the plugin file path, e.g. `akismet/akismet.php`) |
| `themes` | `stylesheet` |
| `settings` | none — singleton object, one row per site |

Example: `OPTIONS /wp-json/wp/v2/posts` → `schema.properties.id` includes
`"readonly": true`, confirming `id` as the immutable primary key.

## **Object's ingestion type**

| Object | Ingestion type | Rationale |
|---|---|---|
| `posts` | `cdc` | `modified`/`modified_gmt` supported as a real update cursor via `modified_after`. No native "deleted posts" feed for an unauthenticated/low-privilege connector — see Known Quirks for an optional `cdc_with_deletes` upgrade path using `status=trash`. |
| `pages` | `cdc` | Same as `posts`. |
| `media` | `cdc` | `modified_after` supported; attachments are rarely hard-deleted independent of their parent post in a way the API exposes. |
| `comments` | `append` | Cursor field is `date`/`date_gmt` (creation time only) via the `after` query param — there is **no `modified` field on comments**, so edits or moderation-status changes to already-synced comments are not re-surfaced by an incremental `after` read. New comments are captured; updates to old ones are not, without a full re-scan. |
| `categories` | `snapshot` | No date/modified field on terms at all; full re-read each sync (typically small volume). |
| `tags` | `snapshot` | Same as `categories`. |
| `users` | `snapshot` | No date/modified field on users; full re-read each sync (typically small volume). |
| `taxonomies` | `snapshot` | Metadata dict, effectively static per site. |
| `types` | `snapshot` | Metadata dict, effectively static per site. |
| `statuses` | `snapshot` | Metadata dict, effectively static per site. |
| `page_revisions` | `snapshot` | No practical incremental cursor exposed at the list level; low volume per parent. |
| `plugins` | `snapshot` | Config/inventory, not event data. |
| `themes` | `snapshot` | Config/inventory, not event data. |
| `settings` | `snapshot` | Singleton object. |

None of the core `wp/v2` endpoints expose a dedicated "list of deleted IDs"
feed. `cdc_with_deletes` is achievable only for `posts`/`pages`/`comments` if
the connector's user has the relevant edit/moderate capability, by also
polling `status=trash` (see Known Quirks) — documented as an optional
enhancement, not the default ingestion type, since it requires elevated
privileges beyond plain `read`.

## **Read API for Data Retrieval**

**Method:** `GET` against `https://<base_url>/wp-json/wp/v2/<resource>`.

### Pagination

- Query params: `page` (1-indexed, default `1`), `per_page` (default `10`,
  **max `100`** — enforced server-side), `offset` (alternative to `page`,
  rarely needed if using `page`).
- Response headers on every list response: `X-WP-Total` (total matching
  records) and `X-WP-TotalPages` (`ceil(X-WP-Total / per_page)`).
- Stop paging when `page > X-WP-TotalPages`, or when the returned array is
  shorter than `per_page`.
- Requesting a `page` beyond `X-WP-TotalPages` returns **HTTP 400** with
  `code: "rest_post_invalid_page_number"` (analogous `rest_*_invalid_page_number`
  codes for other resources) rather than an empty array — the connector must
  treat this as "no more pages," not a hard error.
- Recommended `per_page=100` (the max) to minimize request count.

### Incremental / cursor filters

| Object | Cursor param(s) | Cursor field in response | Notes |
|---|---|---|---|
| `posts` | `modified_after`, `modified_before` (ISO 8601, e.g. `2026-06-01T00:00:00`) | `modified_gmt` | Combine with `orderby=modified&order=asc` for a stable, resumable read order. |
| `pages` | `modified_after`, `modified_before` | `modified_gmt` | Same pattern as `posts`. |
| `media` | `modified_after`, `modified_before` | `modified_gmt` | Same pattern. |
| `comments` | `after`, `before` (ISO 8601, filters on `date_gmt`) | `date_gmt` | No `modified_after` equivalent exists for comments (see Known Quirks). |
| `categories`, `tags`, `users`, `taxonomies`, `types`, `statuses` | none | — | No incremental filter available; full page-through every sync. |

By default `modified_after`/`after` are compared against the **site's local
time** unless the value is explicitly UTC and the site is configured to
treat it as such — to avoid ambiguity, always pass **`_gmt`-equivalent UTC
timestamps** and use `orderby=modified&order=asc` (or `orderby=date` for
comments) so date-boundary records aren't skipped. Apply a small lookback
window (e.g. a few minutes to an hour) on the stored cursor to absorb clock
skew / DST edge cases, consistent with Airbyte's own `lookback_window`
config for this API.

By default, `status` defaults to `publish` for `posts`/`pages` (i.e.
draft/private/scheduled content is invisible unless the caller is
authenticated with edit rights and explicitly requests
`status=publish,future,draft,pending,private`). A read-only connector should
decide up front whether it wants "public content only" (`read` capability,
default status filter) or "all content the user manages"
(`edit_posts`/`edit_others_posts` capability + explicit `status=...`
including `trash`).

### Delete detection (optional, requires elevated auth)

There is no dedicated deleted-records endpoint. To approximate delete
detection for `posts`/`pages`/`comments`, an authenticated connector with
edit/moderate capability can additionally poll:

```
GET /wp/v2/posts?status=trash&modified_after=<cursor>&per_page=100&page=1
GET /wp/v2/comments?status=trash&after=<cursor>&per_page=100&page=1
```

and emit those IDs as deletes. This is a best-effort approximation (WP moves
items to `trash` before permanent deletion, and trashed items are
auto-purged after 30 days by default — a permanently, immediately deleted
item without passing through `trash` will never be observed). Not part of
the default `cdc` ingestion type above; document as an opt-in enhancement.

### Example requests

Full list (first page):

```bash
curl -u "connector_user:app_password" \
  "https://example.com/wp-json/wp/v2/posts?per_page=100&page=1&orderby=modified&order=asc"
```

Incremental read:

```bash
curl -u "connector_user:app_password" \
  "https://example.com/wp-json/wp/v2/posts?per_page=100&page=1&orderby=modified&order=asc&modified_after=2026-06-01T00:00:00"
```

Example response headers:

```
HTTP/1.1 200 OK
X-WP-Total: 428
X-WP-TotalPages: 5
Content-Type: application/json; charset=UTF-8
```

Single-object read (used for point lookups / backfill verification, not for
bulk paging):

```bash
curl -u "connector_user:app_password" \
  "https://example.com/wp-json/wp/v2/posts/123"
```

Reduce payload size with sparse fieldsets when full schema isn't needed:

```
GET /wp/v2/posts?_fields=id,date_gmt,modified_gmt,slug,status,title,author
```

### Rate limits

- **WordPress core ships no built-in REST API rate limiter.** Any throttling
  seen in practice comes from the hosting layer (e.g. managed hosts like
  WP Engine, Kinsta, WordPress VIP; or a WAF/CDN in front of the site such as
  Cloudflare) or from a rate-limiting plugin, and varies per installation.
  `TBD:` there is no universal numeric limit to document for self-hosted
  sites — the connector should implement conservative client-side pacing
  (e.g. a small fixed delay between pages, exponential backoff on `429`) and
  make it configurable, since the actual ceiling is site-specific and
  undocumented.
- **WordPress.com** (hosted) enforces its own, not-publicly-numbered rate
  limits under Automattic's "Guidelines for Responsible Use of Automattic's
  APIs"; exact request/hour thresholds are not published. `TBD:` treat any
  `429` from WordPress.com as authoritative and back off using
  `Retry-After` if present, else exponential backoff.
- Regardless of host, respond to HTTP `429 Too Many Requests` with
  exponential backoff (honor `Retry-After` header if present).

### Error handling

WordPress wraps internal `WP_Error` objects into a consistent REST error
shape:

```json
{
  "code": "rest_forbidden",
  "message": "Sorry, you are not allowed to do that.",
  "data": { "status": 403 }
}
```

Common codes/status the connector should special-case:

| HTTP Status | Example `code` | Meaning | Connector behavior |
|---|---|---|---|
| 400 | `rest_post_invalid_page_number` | Requested `page` beyond `X-WP-TotalPages` | Treat as end-of-pagination, not a failure. |
| 401 | `rest_not_logged_in` / `rest_cannot_view` | Missing/invalid Application Password, or resource needs auth | Fail fast with a credential error; do not retry. |
| 403 | `rest_forbidden` / `rest_cannot_edit` | Authenticated but lacking the required capability (e.g. requesting `status=trash` without `edit_posts`) | Fail with an actionable message naming the missing capability; do not retry. |
| 404 | `rest_post_invalid_id` / `rest_no_route` | Object ID does not exist, or `wp/v2` route/namespace not active (REST API disabled by a security plugin) | For point lookups, treat as "not found" (skip); for a whole route 404'ing, fail the table with a clear "endpoint unavailable" error. |
| 429 | (host/CDN-specific, not a core WP code) | Rate limited by hosting layer / WAF / WordPress.com | Exponential backoff, honor `Retry-After`. |
| 500/502/503 | — | Server error / site under load or in maintenance | Retry with backoff; treat sustained 5xx as a transient outage. |

## **Field Type Mapping**

| WordPress / REST JSON type | Connector logical type | Notes |
|---|---|---|
| `integer` | `long` / `integer` | IDs, foreign keys, counts. |
| `string` | `string` | |
| `string` (ISO 8601, no explicit `Z`, e.g. `2026-06-01T12:00:00`) | `timestamp` (site-local, naive) | `date`, `modified` fields — no timezone offset in the string; must be interpreted using the site's configured timezone (`GET /wp/v2/settings` → `timezone_string`/`timezone`, auth required) or treated as naive and paired with the `_gmt` sibling for a reliable UTC value. |
| `string` (ISO 8601 UTC, e.g. `2026-06-01T12:00:00`, from a `_gmt` field) | `timestamp` (UTC) | `date_gmt`, `modified_gmt` — prefer these over the non-GMT siblings for consistent UTC ingestion. |
| `boolean` | `boolean` | E.g. `sticky`, `protected`. |
| `object { rendered: string, raw?: string, protected?: boolean }` | `string` (flatten to `rendered`) or `struct` | Rendered HTML is always present; `raw` only under `context=edit`. Recommend ingesting `rendered` as the primary column and optionally a `protected` boolean column. |
| `object { href: string, ... }` under `_links` | (dropped) | HATEOAS navigation metadata; not business data — exclude from the ingested schema by default. |
| `array<integer>` (e.g. `categories`, `tags` on posts) | `array<long>` | Foreign-key arrays; can be flattened into a bridge/junction structure if the destination requires normalized relations. |
| `object` (e.g. `meta`, `media_details`, `capabilities`) | `struct` / `string` (JSON-serialized) | Shape is not fixed by core schema (`meta` varies per site/plugin); safest to ingest as a JSON string column unless a specific site's meta schema is known in advance. |
| `enum` string (e.g. `status`, `format`, `media_type`, `comment_status`) | `string` | Validate against the known enum server-side is optional; ingest as plain string and let the destination enforce constraints if desired. |
| dict-keyed-by-slug object (e.g. `taxonomies`, `types`, `statuses` top-level response) | `array<struct>` | Not natively an array — the connector must flatten `{slug: {...fields...}}` into rows with `slug` promoted to a column before emitting. |

Special behaviors:
- `id` is server-generated, immutable, and always present — safe as primary
  key across all row-shaped objects.
- `guid.rendered` looks like a stable URL but WordPress explicitly documents
  it as **not guaranteed unique or stable** (can change on migration) — do
  not use as a key.
- `meta` custom fields are opt-in per site (`register_post_meta` with
  `show_in_rest`); an installation may expose zero custom meta fields, so
  don't assume a fixed schema.

## **Known Quirks & Edge Cases**

- **Comments have no `modified`/`modified_gmt` field.** Only creation time
  (`date`/`date_gmt`) is available, and only `after`/`before` filters exist
  (no `modified_after`). Editing an existing comment's content or changing
  its moderation status does **not** change any field the API lets you
  filter on, so an `append`-only incremental strategy based on `after` will
  miss such edits. A full periodic re-snapshot of `comments` is the only way
  to reliably catch backdated moderation/content changes.
- **Terms (`categories`, `tags`) and `users` have no date/modified field at
  all.** These must be treated as `snapshot` objects with full re-reads;
  there is no way to ask "what changed since X" for these endpoints.
- **Non-`publish` statuses (draft/pending/private/trash) and full user
  PII (email, roles) are invisible unless authenticated with the matching
  capability.** A connector configured with a low-privilege ("read only")
  Application Password will silently see fewer rows than one configured
  with an editor/administrator account — document the configured user's
  role clearly since it directly affects row counts, not just field
  visibility.
- **`page` beyond `X-WP-TotalPages` returns HTTP 400**, not an empty page —
  treat `rest_post_invalid_page_number` (and its per-resource equivalents,
  e.g. `rest_comment_invalid_page_number`) as a normal "pagination
  exhausted" signal rather than an error to surface to the user.
- **Custom REST API–blocking security plugins** (e.g. some hardening
  plugins) disable `wp/v2` routes entirely, returning `404 rest_no_route`
  for every request including the root `wp-json/` discovery call. Treat a
  404 on the discovery/index call itself as "REST API disabled on this
  site," not as "site doesn't exist."
- **WordPress.com vs self-hosted:** WordPress.com sites are also reachable
  at `<site>/wp-json/wp/v2/...` for many read endpoints (Jetpack/WordPress.com
  ships core-compatible REST routes), **but** WordPress.com additionally
  exposes its own parallel, non-core API family at
  `https://public-api.wordpress.com/rest/v1.1/sites/$site/...` with a
  different pagination model (`number`/`offset`/`page`/`page_handle` cursor
  token instead of `X-WP-Total`/`X-WP-TotalPages` headers) and its own OAuth2
  flow. This doc targets the **core `wp/v2` shape**, which works against
  both self-hosted and WordPress.com sites that haven't disabled it; the
  `rest/v1.1` family is out of scope. `TBD:` confirm on a case-by-case basis
  whether a specific WordPress.com site has `wp/v2` enabled — some managed
  WordPress.com plans restrict it.
- **Timezone ambiguity on non-`_gmt` date fields**: always prefer
  `*_gmt` fields and `_after`/`_before` UTC values to avoid off-by-offset
  bugs around DST transitions (this mirrors the `lookback_window` config
  Airbyte's connector exposes specifically to paper over this issue).
- **`per_page` hard-capped at 100** server-side (core, not configurable via
  request) — requesting more silently clamps to 100, it does not error.
- **Custom post types/taxonomies** installed by themes/plugins reuse the
  exact same controller classes as `posts`/`categories`, so the same
  pagination, cursor, and schema rules in this document apply to them
  without modification — only the route path and post-type/taxonomy slug
  differ.

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://developer.wordpress.org/rest-api/reference/posts/ | 2026-07-02 | High | Posts query params (`modified_after`, `after`, pagination, filters) and full field schema. |
| Official Docs | https://developer.wordpress.org/rest-api/reference/comments/ | 2026-07-02 | High | Comments query params (`after`/`before` only, no `modified_after`) and field schema. |
| Official Docs | https://developer.wordpress.org/rest-api/reference/media/ | 2026-07-02 | High | Media query params (`modified_after` supported) and field schema incl. `media_details`. |
| Official Docs | https://developer.wordpress.org/rest-api/reference/categories/ | 2026-07-02 | High | Term (categories/tags) schema; confirmed tags lack `parent`. |
| Official Docs | https://developer.wordpress.org/rest-api/reference/users/ | 2026-07-02 | High | User schema split between `view` and `edit` context; confirmed no `modified` field. |
| Official Docs | https://developer.wordpress.org/rest-api/using-the-rest-api/pagination/ | 2026-07-02 | High | `page`/`per_page`/`offset` params, `per_page` cap of 100, `X-WP-Total`/`X-WP-TotalPages` headers. |
| Official Docs | https://developer.wordpress.org/rest-api/using-the-rest-api/authentication/ | 2026-07-02 | High | Comparison of cookie/nonce, Basic Auth plugin, Application Passwords, OAuth/JWT plugins; chose Application Passwords. |
| Official Docs | https://developer.wordpress.org/rest-api/using-the-rest-api/authentication/#basic-authentication-with-application-passwords | 2026-07-02 | High | Application Password generation location, Basic Auth format, HTTPS requirement, example curl. |
| Official Docs | https://developer.wordpress.org/rest-api/using-the-rest-api/discovery/ | 2026-07-02 | High | Root `wp-json/` discovery response shape (`namespaces`, `routes`). |
| Official Docs | https://developer.wordpress.org/rest-api/using-the-rest-api/global-parameters/ | 2026-07-02 | High | `_fields`, `_embed`, `_envelope` global query parameters. |
| Airbyte (manifest) | https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-integrations/connectors/source-wordpress/manifest.yaml | 2026-07-02 | High | Ground-truth stream list (`users`, `posts`, `categories`, `plugins`, `editor_blocks`, `comments`, `pages`, `tags`, `page_revisions`, `media`, `taxonomies`, `types`, `themes`, `statuses`, `settings`), pagination (`page`+`per_page`, size 100), Basic Auth (Application Passwords), and per-stream incremental cursor fields (`posts`: none/full-refresh in this connector; `pages`/`media`: `modified`/`modified_after`; `comments`: `date`/`after`). |
| Airbyte Docs | https://docs.airbyte.com/integrations/sources/wordpress | 2026-07-02 | Medium | Corroborated stream list and auth method (secondary confirmation of manifest). |
| GitHub Issue | https://github.com/airbytehq/airbyte/issues/46087 | 2026-07-02 | Medium | Confirmed the WordPress connector is a community-built, manifest-only (low-code) Airbyte connector, not an officially certified/maintained one — treated Airbyte as medium confidence accordingly, cross-checked every claim against official WP docs. |
| Community/Blog | https://wordpress.org/support/topic/invalid-page-number-when-set-per_page-wp-api/ and related search results | 2026-07-02 | Medium | Confirmed `rest_post_invalid_page_number` (HTTP 400) behavior when paging past `X-WP-TotalPages`. |
| Community/Search | rest_forbidden error shape (multiple WP-API/WordPress.org sources) | 2026-07-02 | Medium | Confirmed standard `{code, message, data.status}` error envelope. |
| Official Docs (WordPress.com) | https://developer.wordpress.com/docs/api/1.1/get/sites/%24site/posts/ | 2026-07-02 | High (for WordPress.com specifics) | Confirmed WordPress.com's separate `rest/v1.1` API family, its distinct pagination model (`number`/`offset`/`page`/`page_handle`), and that it differs from core `wp/v2`. |
| Community/Search | Fivetran WordPress connector page (nav-only content retrieved) | 2026-07-02 | Low | Could not retrieve Fivetran's specific table list (page returned only navigation chrome); Fivetran claims were **not** used as a factual source in this doc — Airbyte's manifest and official WP docs were used instead. |
| Community/Search | WordPress core/self-hosted rate limiting (multiple sources incl. github.com/cedaro/wprestcop) | 2026-07-02 | Medium | Confirmed core WordPress ships no built-in REST rate limiter; any limiting is host/plugin-dependent. |

## **Sources and References**

- Official WordPress REST API Handbook — Reference: https://developer.wordpress.org/rest-api/reference/ (posts, pages, comments, categories, tags, media, users, taxonomies, types, statuses sub-pages) — **highest confidence**.
- Official WordPress REST API Handbook — Using the REST API (pagination, authentication, discovery, global parameters): https://developer.wordpress.org/rest-api/using-the-rest-api/ — **highest confidence**.
- Airbyte `source-wordpress` low-code connector manifest (ground truth for stream/endpoint list and default incremental config): https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-wordpress — **high confidence**, but noted as a community/manifest-only connector, not Airbyte-certified, so every claim from it was cross-checked against official WP docs before inclusion.
- Airbyte WordPress connector docs page: https://docs.airbyte.com/integrations/sources/wordpress — **medium confidence**, secondary corroboration only.
- Fivetran WordPress connector page: https://fivetran.com/docs/connectors/applications/wordpress — attempted but the page only returned navigation chrome (no table/schema content extractable); **not used as a factual source** in this document. Note for future refresh: revisit with a tool that can render the interactive schema ERD.
- WordPress.com REST API docs (for the WordPress.com-vs-self-hosted comparison only): https://developer.wordpress.com/docs/api/ and https://developer.wordpress.com/docs/api/1.1/get/sites/%24site/posts/ — **high confidence** for WordPress.com-specific claims.
- Community/support-forum sources for error-code and rate-limit behavior not spelled out in the handbook (WordPress.org support forum threads, `cedaro/wprestcop` plugin README): **medium/low confidence**, used only to corroborate widely-observed behavior (400 on invalid page number, no core rate limiting), not for anything load-bearing to the connector's correctness.

**Conflict resolution notes:** Airbyte's manifest treats `posts` as
full-refresh only (no `incremental_sync` block), even though the official
WP docs confirm `posts` supports `modified_after` identically to `pages` and
`media`. This documentation follows the **official docs** (higher priority
per research methodology) and classifies `posts` as `cdc` using
`modified_after`, noting the discrepancy here rather than silently adopting
Airbyte's more conservative choice.
