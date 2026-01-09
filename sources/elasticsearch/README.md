# Lakeflow Elasticsearch Community Connector

This documentation describes how to configure and use the **Elasticsearch** community connector to ingest any accessible index by its name or alias into Databricks.

## Prerequisites
- An Elasticsearch cluster reachable from your Databricks environment (TLS recommended).
- One of: **API key** (base64 encoded) or **Bearer token** from the token service.
- Permissions to read the indices/aliases you plan to ingest (including system indices if needed).
- Trust for the cluster certificate if using a private CA (set `verify_ssl=false` only when you explicitly accept that risk).

## Setup

### Required Connection Parameters

Provide these connector options when creating the Unity Catalog connection:

| Name | Type | Required | Description | Example |
| --- | --- | --- | --- | --- |
| `endpoint` | string | yes | Base URL for the cluster. | `https://es.example.com:9200` |
| `authentication` | string | yes | Auth method: `API_KEY` or `BEARER`. | `API_KEY` |
| `api_key` | string | conditional | Required when `authentication=API_KEY`; value is the base64 of `id:api_key`. | `ZXNfaWQ6ZXNfa2V5` |
| `token` | string | conditional | Required when `authentication=BEARER`; OAuth/token service bearer token. | `eyJhbGciOi...` |
| `verify_ssl` | boolean | no | Set to `false` to skip TLS verification (default: `true`). | `true` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of **table-level** options allowed to pass through. **Required** because this connector supports per-table options. | `cursor_field,ingestion_type,page_size,pit_keep_alive` |

Table-level option names (must appear in `externalOptionsAllowList`):
- `cursor_field`
- `ingestion_type`
- `page_size`
- `pit_keep_alive`

### Get the Required Parameters
- **Endpoint**: Use the HTTPS base URL of your cluster, e.g., `https://<elasticsearch-host>:9200`.
- **API key** (if using `API_KEY`):
  1. Use a user with `manage_api_key` (or similar) to call `POST /_security/api_key`.
  2. Concatenate the returned `id` and `api_key` as `id:api_key`, then base64-encode that string for the `Authorization: ApiKey ...` header.
- **Bearer token** (if using `BEARER`):
  1. Call `POST /_security/oauth2/token` (password grant or your org’s SSO flow).
  2. Use the `access_token` in `Authorization: Bearer <token>`.
- **TLS trust**: If your cluster uses a private CA, either import the CA cert into the environment trust store or temporarily set `verify_ssl=false`.

### Create a Unity Catalog Connection
1. In the Lakeflow Community Connector UI (`Add Data`), choose Elasticsearch.
2. Choose to create a new connection.
3. Enter the connection options above. Set `externalOptionsAllowList` to `cursor_field,ingestion_type,page_size,pit_keep_alive`.
3. Save the connection.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects
- Supports **any index or alias** visible to the provided credentials (including system/hidden indices if permitted).

| Object (index/alias) | Primary Key | Ingestion Type | Cursor | Notes |
| --- | --- | --- | --- | --- |
| Any accessible index/alias | `_id` | Defaults to `cdc` when a cursor field exists; otherwise `snapshot`. Override with `ingestion_type=cdc` or `append` when a cursor is present. | Auto-detected in order: `timestamp`, `updated_at` (or set `cursor_field`). Fields starting with `_` (meta fields) are currently not allowed as cursors. | Works with user and system indices; aliases and data streams resolve to concrete indices. Wildcards/patterns (e.g., `logs-*`) are not supported; specify a single index or alias per table. |

Table options (set per table in the pipeline):
- `cursor_field` (string, optional): Force the cursor field if auto-detection is not desired.
- `ingestion_type` (string, optional): `cdc` (default when cursor available) or `append`. If no cursor is available, the connector falls back to `snapshot`.
- `page_size` (integer, optional): Page size for search pagination (default `1000`).
- `pit_keep_alive` (string, optional): Point-in-time keep-alive duration (default `1m`).

Behavior notes:
- Reads use **point-in-time (PIT)** plus `search_after` with `_shard_doc` as a tiebreaker for deterministic paging.
- `_id` is always added as a column in the ingested data.
- Fields starting with `_` (meta fields such as `_seq_no`, `_version`, `_id`) are currently not accepted as cursors. If no suitable cursor is found, the connector performs a snapshot read.
- Wildcards or comma-separated patterns are not supported; each table must reference a single index or alias (no `logs-*` fanout).
- Aliases/data streams that resolve to multiple backing indices must share identical mappings; otherwise the connector raises an error to avoid schema drift.
- Network resiliency: HTTP calls use explicit timeouts (5s connect / 30s read) and a small retry with backoff (up to 3 attempts on 429/5xx, honoring `Retry-After`).

## Data Type Mapping

| Elasticsearch type | Connector type (Spark) | Notes |
| --- | --- | --- |
| `keyword`, `text` | string | |
| `date` | timestamp | |
| `boolean` | boolean | |
| `binary` | binary | Base64 payload preserved. |
| `long`, `integer`, `short`, `byte`, `unsigned_long` | long | |
| `double`, `float`, `half_float`, `scaled_float` | double | |
| `geo_point` | struct(lat: double, lon: double) | |
| `geo_shape` | string | Shape serialized to string. |
| `object` | struct | Nested properties mapped to struct. |
| `nested` | array<struct> | Nested type stays as array of structs. |
| other/unknown | string | Fallback type. |
| `_id` (added) | string | Added by the connector as primary key. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Use the Lakeflow Community Connector UI flow to copy or reference `sources/elasticsearch/elasticsearch.py` in your workspace.

### Step 2: Configure Your Pipeline
1. Point `connection_name` to the UC connection configured above.
2. List the indices/aliases you want to ingest as `source_table` entries. Add table options when needed.

Example pipeline snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "elasticsearch_connection",
    "object": [
      {
        "table": {
          "source_table": "logs-2026-*",
          "cursor_field": "timestamp",
          "page_size": 1000,
          "pit_keep_alive": "2m"
        }
      },
      {
        "table": {
          "source_table": "orders",
          "ingestion_type": "append",
          "cursor_field": "_seq_no"
        }
      }
    ]
  }
}
```

3. (Optional) Adjust `ingestion_type`, `page_size`, or `pit_keep_alive` per index based on size and latency needs.

### Step 3: Run and Schedule the Pipeline

#### Best Practices
- **Start small**: Begin with one index/alias to validate access and schema.
- **Pick a stable cursor**: Prefer monotonically increasing fields (`timestamp`, `updated_at`, `_seq_no`). Set `cursor_field` explicitly for reliability.
- **Tune pagination**: Use `page_size` to balance throughput vs. memory; extend `pit_keep_alive` for large scans.
- **Secure transport**: Keep `verify_ssl=true` unless you control the certificates and accept the risk.

#### Troubleshooting
- **401/403 auth errors**: Verify `authentication`, `api_key`/`token`, and user privileges on the target indices.
- **Index not accessible**: Ensure the index/alias exists and the credential can read it; discovery only lists accessible indices.
- **Cursor not found**: If the chosen `cursor_field` is missing, the connector falls back to snapshot or raises an error if explicitly set.
- **PIT expired**: Increase `pit_keep_alive` or reduce `page_size` for very large indices.
- **Mapping shape issues**: Connector expects typeless mappings (Elasticsearch 8.x+); unexpected structures may raise errors.

## References
- Connector implementation: `sources/elasticsearch/elasticsearch.py`
- Source research notes: `sources/elasticsearch/elasticsearch_api_doc.md`
- Elastic docs: Search API, pagination (`search_after`, scroll), point-in-time, API key auth, and mapping types.
