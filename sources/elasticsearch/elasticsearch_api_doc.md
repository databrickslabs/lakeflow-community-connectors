# **Elasticsearch API Documentation**

## **Authorization**
- **Chosen methods (connector only supports these):**
  - API Key authentication
  - Bearer token authentication (Elasticsearch token service)
- **Base URL:** `https://<elasticsearch-host>:9200` (TLS recommended). Self-managed clusters may use self-signed certs; allow configuring certificate verification.
- **Auth placement in HTTP header**:
  - API key: `Authorization: ApiKey <BASE64(api_key_id:api_key)>` (base64 of the literal `id:api_key` pair returned by the Create API Key response). Source: [Create API key](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html).
  - Bearer token: `Authorization: Bearer <access_token>` (token returned by the token service). Source: [Get token API](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-token.html).
- **Other methods (not used by this connector):**
    - Basic auth Source: [Security APIs overview](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html).
- **Example (API key):**
  ```bash
  curl -X GET "https://<host>:9200/_cat/indices?format=json&expand_wildcards=all" \
    -H "Authorization: ApiKey <BASE64(api_key_id:api_key)>"
  ```
- **How to obtain credentials (out-of-band examples):**
  - Create API key (requires a user with `manage_api_key`):
    ```bash
    curl -X POST "https://<host>:9200/_security/api_key" \
      -u "<user>:<password>" \
      -H "Content-Type: application/json" \
      -d '{"name": "connector-key", "expiration": "7d", "role_descriptors": {}}'
    ```
    Response contains `id` and `api_key`; concatenate as `id:api_key` and base64-encode for the header.
  - Get bearer token (token service):
    ```bash
    curl -X POST "https://<host>:9200/_security/oauth2/token" \
      -u "<user>:<password>" \
      -H "Content-Type: application/json" \
      -d '{"grant_type": "password", "username": "<user>", "password": "<password>"}'
    ```
    Response contains `access_token`; use it in `Authorization: Bearer <access_token>`.

## **Object List**
- The connector supports **any index/alias** that is visible to the provided credentials — including system/hidden indices (dot-prefixed). Discovery requests include hidden/system indices by default.
- Discovery flow:
  - `GET /_cat/indices?format=json&expand_wildcards=all` to enumerate indices (includes hidden/system when permissions allow) with status and doc counts. Source: [cat indices](https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html).
  - `GET /_aliases` to resolve aliases → concrete indices (data streams resolve similarly). Source: [aliases API](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html).
- Supported Indices / object list (non-exhaustive):

| Object (index/alias) | Description | Notes |
|----------------------|-------------|-------|
| Any user index/alias | User-created index or alias (e.g., `logs-prod`, `orders`, `logs-2026-*`) | Available if credentials can read it |
| `.security-*` | Security model state (users, roles, API keys) | System index; requires elevated privileges |
| `.kibana_*` | Kibana saved objects/state | System index; requires elevated privileges |
| `.tasks` | Persistent task info | System index; requires elevated privileges |
| `.geoip_databases` | GeoIP database storage | System index; requires elevated privileges |
| `.transform-internal-*` | Transform internal state | System index; requires elevated privileges |

System indices vary by features enabled; table shows common ones documented for security/Kibana/geoip/transform. Source: [System indices](https://www.elastic.co/guide/en/elasticsearch/reference/current/system-indices.html).

## **Object Schema**
- Elasticsearch exposes schema via mappings. Fetch per index/alias:
  - `GET /{index}/_mapping`. Source: [get mapping](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html).
- Mappings include field names and types (`keyword`, `text`, `date`, `long`, `double`, `boolean`, `object`, `nested`, `geo_point`, `binary`, etc.) as a properties tree.
- Connector mapping rules:
  - Treat each index/alias as a table; derive columns from its mapping.
  - Multi-fields (e.g., `field` and `field.keyword`): connector will select explicit subfields as needed (typically `.keyword` for exact keys).
  - Nested/object fields map to struct / array<struct> in Spark.
- Example schema retrieval:
  ```bash
  curl -X GET "https://<host>:9200/logs-*/_mapping" \
    -H "Authorization: ApiKey <BASE64(api_key_id:api_key)>"
  ```
- System index schemas (not published; must be fetched at runtime):

| Index | How to fetch schema | Notes |
|-------|---------------------|-------|
| `.security-*` | `GET /.security-*/_mapping` | Security state (users/roles/API keys); mapping varies by version/features. |
| `.kibana_*` | `GET /.kibana_*/_mapping` | Kibana saved objects; mapping varies by Kibana version/plugins. |
| `.tasks` | `GET /.tasks/_mapping` | Persistent task info; mapping not published. |
| `.geoip_databases` | `GET /.geoip_databases/_mapping` | GeoIP database storage; mapping not published. |
| `.transform-internal-*` | `GET /.transform-internal-*/_mapping` | Transform internal state; mapping varies by version. |

Schema per system index is resolved live from `_mapping`; Elasticsearch does not publish fixed schemas for these indices. The connector will always derive columns at runtime from the retrieved mappings (applies to user indices/aliases as well).

## **Get Object Primary Keys**
- Elasticsearch uniquely identifies each document in an index with `_id`. Source: [Index docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html).
- There is no API to declare a custom primary key; the connector will use `_id` as the primary key column for every index it ingests.

## **Object's ingestion type**
- Elasticsearch has no delete/change feed; search returns current docs only.
- Supported ingestion types and prerequisites:
  - `cdc` (upserts): requires a sortable cursor field in documents (e.g., `timestamp`, `updated_at`, or `_seq_no`). Connector sorts by cursor + `_shard_doc` and uses `search_after` for pagination. If `cursor_field` is not provided, the connector auto-detects using `["timestamp", "updated_at", "_seq_no"]` in that order.
  - `append`: optional override for append-only streams; requires a cursor field (explicit or auto-detected). Uses the same PIT + cursor + `_shard_doc` pagination as `cdc`.
  - `snapshot`: if no reliable cursor is available; full reload via batch search (still uses PIT + `_shard_doc` for deterministic paging).
  - `cdc_with_deletes`: not supported (no delete stream). If a soft-delete flag exists in an index, this could be revisited per index.
- Application:
  - User indices/aliases: connector uses `cdc` when a cursor is available (or `append` if explicitly set); otherwise `snapshot`.
  - System indices: same rules apply; if no cursor, treat as `snapshot`.

## **Read API for Data Retrieval**
- **Search endpoint:** `POST /{index}/_search` (or `GET`). Source: [Search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html).
- **Pagination / consistency:**
  - **Point in time (PIT):** `POST /{index}/_pit?keep_alive=1m` → PIT ID. Source: [PIT API](https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html).
  - **search_after with PIT (preferred):**
    ```json
    {
      "pit": {"id": "<PIT_ID>", "keep_alive": "1m"},
      "size": 1000,
      "sort": [{"timestamp": "asc"}, {"_shard_doc": "asc"}],
      "search_after": ["<last_timestamp>", "<last_shard_doc>"],
      "_source": {"includes": ["field1", "field2", "..."]},
      "query": {"match_all": {}}
    }
    ```
    Source: [search_after](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after).
  - **Scroll (fallback/legacy):** `POST /{index}/_search?scroll=1m` then `POST /_search/scroll` with `_scroll_id` until no hits; clear via `DELETE /_search/scroll`. Source: [Scroll](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#scroll-search-results).
  - **Tiebreaker note:** `_shard_doc` is used instead of `_id` as the secondary sort key to avoid fielddata requirements on `_id`.
- **Filtering:** pass Query DSL in `query`; use `_source` includes/excludes to trim payload.
- **Count (optional):** `GET /{index}/_count`. Source: [Count API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html).
- **Deletes:** Search APIs return only existing docs; no delete stream is provided.

## **Field Type Mapping**
- Reference: [Field data types](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html).
- Typical mapping to connector types:
  - `keyword` → string
  - `text` → string (not ideal for exact keys)
  - `date`, `date_nanos` → datetime
  - `long` / `integer` / `double` / `float` / `half_float` / `scaled_float` → numeric
  - `boolean` → boolean
  - `binary` → bytes (base64)
  - `ip`, `version`, `completion` → string
  - `flattened` → map<string, string>
  - `object` → struct
  - `nested` → array\<struct\>
  - `geo_point` → struct/array (lat, lon)
  - `geo_shape` → string/struct (representation choice TBD per use case)
- Multi-fields: fields can have subfields (e.g., `field.keyword`); choose the subfield explicitly when needed.

## **Sources and References**
- Official docs (Elastic):
  - Security/API key auth: https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html
  - Security APIs overview: https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html
  - Configuring security/TLS: https://www.elastic.co/guide/en/elasticsearch/reference/current/configuring-stack-security.html
  - Cat indices: https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html
  - Aliases: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html
  - Get mapping: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html
  - Index docs: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
  - Search API: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
  - Pagination (search_after, scroll): https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
  - Point in time: https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
  - Count API: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
  - Field mapping types: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html

## **Connector defaults and options**
- Default cursor detection order: `timestamp`, `updated_at` (falls back to snapshot ingestion if no default cursor was found!)
- Per-table options:
  - `cursor_field`: override cursor detection.
  - `ingestion_type`: optional override (`append` or `cdc`); requires a cursor field. If none is available, the connector falls back to `snapshot`.
  - `page_size`: page size for search (default `1000`). Legacy `size` is still accepted.
  - `pit_keep_alive`: PIT keep-alive duration (default `1m`).

## **Restrictions**
- Wildcards or comma-separated patterns are not supported; each table must reference a single index or alias (no `logs-*` fanout).
- Cursor fields: Meta-fields starting with `_` are currently not allowed as cursors.
- Aliases or data streams resolving to multiple backing indices must have identical mappings; otherwise the connector raises an error to avoid schema drift.

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html | 2026-01-09 | High | Index listing endpoint and params |
| Official Docs | https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html | 2026-01-09 | High | Schema retrieval endpoint |
| Official Docs | https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html | 2026-01-09 | High | Core search endpoint and request structure |
| Official Docs | https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html | 2026-01-09 | High | Pagination options (search_after, scroll) |
| Official Docs | https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html | 2026-01-09 | High | PIT creation/usage |
| Official Docs | https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html | 2026-01-09 | High | API key auth header/usage |
| Official Docs | https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html | 2026-01-09 | High | Field type definitions |
