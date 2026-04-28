# Lakeflow Shopify Community Connector

Ingest data from the **Shopify Admin REST API** into Databricks Delta tables via Lakeflow Connect.

## Prerequisites

- **Shopify store**: any plan tier (Basic through Plus). The connector reads via the Admin API and works on every Shopify plan.
- **Admin API access token**: a token starting with `shpat_...` issued by an installed custom app on the shop. See "Setup" below for the exact steps.
- **Network access**: the environment running the connector must be able to reach `https://{shop}.myshopify.com`.
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `shop` | string | yes | Shopify shop subdomain — the part before `.myshopify.com` in the store URL. | `my-store` |
| `access_token` | string | yes | Admin API access token from a custom app installed on the shop. Format: `shpat_...`. | `shpat_abc123…` |
| `api_version` | string | no | Shopify Admin REST API version (e.g. `2026-04`). Defaults to the latest stable version supported by the connector. | `2026-04` |
| `externalOptionsAllowList` | string | no | Comma-separated list of allowed table-specific options. Most use cases don't need this; leave empty unless you set per-table options. | `since_id,status` |

### Obtaining the access token

There are two paths depending on when your store was created and how you intend to deploy:

**Path A — Custom app (legacy custom-app flow, simplest for testing & per-shop deployments)**

This path is available on dev stores under any Partner organization, and on merchant-owned stores created before Jan 1 2026.

1. Sign in to your Shopify admin (`https://admin.shopify.com/store/{shop}`).
2. Go to **Settings → Apps and sales channels → Develop apps**.
3. The first time only: click **Allow custom app development** and confirm.
4. Click **Create an app** → name it (e.g. `Lakeflow ingestion`).
5. Open the **Configuration** tab → under **Admin API integration**, click **Configure** and grant these scopes:
   - `read_products`
   - `read_customers`
   - `read_orders`
   - `read_inventory`
   - `read_locations`
   - *(Optional)* `read_all_orders` — only needed if your store has orders older than 60 days that you want to ingest. Without it, the API silently caps results to the last 60 days.
6. Save → switch to the **API credentials** tab → click **Install app**.
7. Copy the **Admin API access token** (`shpat_...`). You can only view this once — store it securely.

> **⚠️ Important — Protected Customer Data Access (PCDA)**
>
> If you're ingesting the `customers` table, the `email`, `first_name`, `last_name`, `phone`, and address-line fields are **silently redacted from the API response** unless the app has Protected Customer Data Access configured. This is a Shopify privacy policy, not a connector behavior — every record will come back with these fields as `null` until PCDA is granted.
>
> To enable: in the same app, **Configuration** tab → **Protected customer data access** section → **Configure** (or **Request access**) → declare the data fields you'll process (Name, Email, Phone, Address) and the reason. After saving, the API returns the full PII fields without any token reissue.
>
> PCDA configuration UI is available on Shopify Advanced and Plus plans (and on dev stores when the simulated plan tier is set to Advanced or higher). On lower-tier stores, the PII fields will remain null.
>
> See: [shopify.dev/docs/apps/launch/protected-customer-data](https://shopify.dev/docs/apps/launch/protected-customer-data).

**Path B — Dev Dashboard / OAuth (newer stores, public app distribution)**

For stores transferred to merchants after Jan 1 2026, custom apps must be created via the Shopify Dev Dashboard with an OAuth flow. The OAuth flow exchanges a code for the same kind of `shpat_...` token, which is what the connector accepts.

The connector itself does not run interactive OAuth flows. Provision the token out-of-band (e.g. via your existing partner-hosted OAuth app or via Shopify CLI) and supply it as `access_token` to the Unity Catalog connection.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide the required connection parameters (`shop`, `access_token`, optionally `api_version`).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Shopify connector exposes **7 tables** in v1, covering the core e-commerce data model:

**Catalog & customers:**
- `products` — product catalog with variants, options, and images
- `customers` — customer accounts with addresses

**Orders & order lifecycle:**
- `orders` — orders with line items, shipping/billing addresses, and embedded customer reference
- `refunds` — refunds against orders
- `fulfillments` — shipping/fulfillment events

**Operations:**
- `locations` — physical / virtual stores and warehouses
- `inventory_levels` — current stock per (inventory item × location)

### Ingestion modes & primary keys

| Table | Ingestion type | Primary key | Cursor |
|-------|----------------|-------------|--------|
| `products` | `cdc` | `id` | `updated_at` |
| `customers` | `cdc` | `id` | `updated_at` |
| `orders` | `cdc` | `id` | `updated_at` |
| `fulfillments` | `cdc` | `id` | `updated_at` |
| `inventory_levels` | `cdc` | `(inventory_item_id, location_id)` | `updated_at` |
| `refunds` | `append` | `id` | `created_at` |
| `locations` | `snapshot` | `id` | n/a |

### Required and optional table options

All tables work with no per-table options. The connector uses connection-level credentials and global watermarks. Per-table options are not required for any of the 7 tables — they're reserved for future filtering features (e.g. status filter on orders).

### Schema highlights

All tables are enriched with a non-null `shop` field carrying the shop subdomain so multi-shop ingests can be unambiguous in the destination tables.

- **Money fields** are returned as `string` (Shopify's API uses string-encoded decimals to preserve precision). Cast to `decimal(18,4)` downstream if numeric arithmetic is needed.
- **Timestamps** are returned as `string` in ISO 8601 format with timezone offset (e.g. `2026-04-21T10:00:00-04:00`). Cast to `timestamp` downstream if needed.
- **Nested arrays** (e.g. `orders.line_items`, `customers.addresses`, `products.variants`) are preserved as Spark struct arrays — no flattening at ingest. Use Spark's struct/array functions in downstream queries.
- **Currency-mirror fields** like `total_price_set` are dropped from the orders schema — they duplicate the simpler scalar (`total_price`) with currency metadata. Use `currency` and `presentment_currency` for currency context.

### Incremental sync behavior

| Table | Strategy |
|-------|----------|
| `products`, `customers`, `orders` | Server-side `updated_at_min` filter on the list endpoint, watermark = max `updated_at`. |
| `fulfillments`, `refunds` | Parent-orders trick: discover orders updated since the watermark via the `orders` endpoint, then fetch the sub-resource per order. Client-side filter on the cursor field. |
| `inventory_levels` | Full re-fetch each run (the inventory endpoint has no date filter). Framework CDC merge handles deltas via the composite primary key. Watermark advances when any item's `updated_at` advances. |
| `locations` | Full refresh each run — small set, rarely changes. |

### Data type mapping

| Shopify field | Connector Spark type | Notes |
|---------------|----------------------|-------|
| Numeric IDs (`id`, `product_id`, …) | `LongType` | All Shopify identifiers fit in 64 bits. |
| String IDs (`admin_graphql_api_id`) | `StringType` | GraphQL global IDs (`gid://...`) are strings. |
| Money / amount | `StringType` | API returns string-encoded decimals; preserve as-is to avoid float drift. |
| Timestamps (`created_at`, `updated_at`, `published_at`, …) | `StringType` | ISO 8601 with offset. |
| Booleans | `BooleanType` | `null` for missing values. |
| Tags | `StringType` | Comma-separated string per Shopify's response shape. |
| Addresses, marketing-consent dicts, line items | `StructType` (or `ArrayType<StructType>`) | Nested without flattening. |

## How to Run

### Step 1: Reference the connector in your workspace

Use the Lakeflow Community Connector UI to copy or reference the Shopify connector source in your workspace.

### Step 2: Configure your pipeline

In your `ingest.py` (or equivalent), point at the Unity Catalog connection and list the tables to ingest:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
register(spark, "shopify")

pipeline_spec = {
    "connection_name": "my_shopify_connection",
    "objects": [
        {"table": {"source_table": "products"}},
        {"table": {"source_table": "customers"}},
        {"table": {"source_table": "orders"}},
        {"table": {"source_table": "refunds"}},
        {"table": {"source_table": "fulfillments"}},
        {"table": {"source_table": "locations"}},
        {"table": {"source_table": "inventory_levels"}},
    ],
}

ingest(spark, pipeline_spec)
```

### Step 3: Run the pipeline

The first run does a full backfill across all tables (CDC tables also fetch all historical records, since they have no checkpoint to resume from). Subsequent runs ingest only changes since the last sync, using each table's saved watermark.

## Troubleshooting

### Authentication errors (401 / 403)

**Symptoms:** `Shopify API error for {table}: 401 ...` or `403 ...`.

**Causes:**
- The `access_token` expired, was revoked, or has the wrong format. Tokens issued by custom apps don't expire on their own but can be regenerated by uninstalling and reinstalling the app — that invalidates the old token.
- The app was uninstalled. Tokens are tied to an active installation.
- The shop subdomain in `shop` doesn't match the shop the token was issued for.

**Fix:**
- Verify the token is still valid by calling `GET /admin/api/{api_version}/shop.json` with the `X-Shopify-Access-Token` header. A 200 confirms auth works.
- If the token is invalid, follow the Setup → Path A steps to install a new app and obtain a fresh token.

### `customers` table has null `email`, `first_name`, `last_name`, `phone`

**Cause:** Shopify's Protected Customer Data Access (PCDA) policy redacts customer PII from API responses unless the app has been granted PCDA. The records exist in Shopify with full data — Shopify is filtering it out at the API layer.

**Fix:** Configure PCDA in the app — see the **"Important — Protected Customer Data Access"** callout in the Setup section above. After enabling, no token reissue is needed; the next sync returns the full fields.

### Missing tables / "Insufficient access" on a specific table

**Symptoms:** `403` only on certain tables (e.g. `orders` works but `inventory_levels` returns 403).

**Cause:** The custom app wasn't granted the required scope for that resource.

**Fix:** Open the app in the Shopify admin → **Configuration** → **Admin API integration** → **Configure** → tick the missing scope from the Setup section above → **Save** → re-install the app to apply (which generates a new token).

### Orders older than 60 days are missing

**Symptom:** the destination `orders` table looks like it caps at 60 days of history.

**Cause:** Without the `read_all_orders` scope, Shopify silently caps the orders endpoint to the last 60 days. The 60-day window slides — older orders previously ingested won't be returned again.

**Fix:** Add `read_all_orders` to the app's scopes (`Configure` → tick the box → `Save` → re-install). For public apps you may need to request approval from Shopify Partners — for custom apps it's available immediately.

### Rate limiting (429 with `Retry-After`)

**Symptom:** Slow runs, log warnings about retries, occasional `429 Too Many Requests`.

**Why:** Shopify uses a leaky-bucket rate limit. Standard / Basic plans get 40 requests in the bucket leaking at 2/sec. Higher tiers (Shopify Plus, Enterprise) get larger buckets.

**What the connector does:** Retries with exponential backoff (1, 2, 4, 8, 16 seconds) for up to 5 attempts. If Shopify returns a `Retry-After` header, the connector honors that value instead of the default backoff. After 5 failed attempts the call propagates the error.

**Fix if you keep hitting the limit:**
- Reduce the frequency of pipeline runs.
- For very large tables (millions of orders), consider running incremental syncs more frequently with smaller windows rather than fewer larger ones.

### `inventory_levels` looks empty for some items

**Cause:** Shopify treats `null` and `0` differently for `available`. A `null` means the item is not tracked at that location (Shopify isn't keeping a count). A `0` means it's tracked but has zero stock.

**Fix:** This is expected. Filter `WHERE available IS NOT NULL` to exclude untracked items.

### Schema validation errors

**Cause:** Shopify occasionally adds new fields in a quarterly API release. The connector's schema is pinned to the version specified in `api_version` — newer fields aren't surfaced unless the schema is updated.

**Fix:**
- Pin `api_version` to a tested version (e.g. `2026-04`) for predictable behavior.
- For new fields, file an issue or PR against this connector with the Shopify field name and the docs link.

## Limitations

- **REST-only** — uses Shopify's Admin REST API. Shopify formally classified the REST Admin API as "legacy" on 2024-10-01 and requires GraphQL for new public apps as of 2025-04-01. The REST API continues to function for custom apps and is sufficient for read-only ingestion. A future version may switch to GraphQL.
- **No write-back** — connector is read-only.
- **No partitioned reads** — single sequential reader; high-volume tables (large orders history, very large product catalogs) sync linearly.
- **No Customer Privacy / GDPR sync** — when a merchant deletes a customer for compliance, the API simply omits that customer; the connector treats this as "no record" rather than emitting a tombstone. Use the framework's full-refresh option to drop deleted customers from the destination.
- **`inventory_levels` re-fetches all records each run** — the inventory endpoint has no incremental filter. Framework CDC merge handles dedup via the composite PK; this is correct but not the most efficient for stores with millions of inventory items × locations.
- **Custom-app token only in v1** — interactive OAuth is not implemented. Tokens must be provisioned out-of-band.

## References

- [Shopify Admin REST API Reference (2026-04)](https://shopify.dev/docs/api/admin-rest/2026-04)
- [REST Admin API Rate Limits](https://shopify.dev/docs/api/admin-rest/usage/rate-limits)
- [REST Admin API Pagination](https://shopify.dev/docs/api/usage/pagination-rest)
- [Access Scopes Reference](https://shopify.dev/docs/api/admin-rest/usage/access-scopes)
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Shopify Admin REST API (`https://{shop}.myshopify.com/admin/api/`)
- **Supported Objects**: 7 tables (products, customers, orders, refunds, fulfillments, locations, inventory_levels)
- **API Version**: 2026-04 (default, configurable)
- **Authentication**: Custom-app access token (`X-Shopify-Access-Token` header)
- **Supported Ingestion Types**: snapshot, append, cdc
