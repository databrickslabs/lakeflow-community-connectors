# Lakeflow PayPal Community Connector

This documentation provides setup instructions and reference information for the **PayPal** source connector. The connector ingests data from the PayPal REST APIs (Transaction Search v1, Customer Disputes v1, Invoicing v2 and Catalog Products v1) into Databricks.

## Prerequisites

- **PayPal Business account**: A Business account is required to use the APIs in production (and to test outside the US with sandbox accounts).
- **PayPal REST API app**: An app created in the [PayPal Developer Dashboard](https://developer.paypal.com/dashboard/) that provides a **Client ID** and **Client Secret**.
- **Transaction Search enabled**: The **Transaction search** feature must be explicitly enabled on the app to read the `transactions` and `balances` tables. After enabling it, PayPal can take **up to 9 hours** before the feature becomes active.
- **App features / scopes** for the tables you plan to read (enabled by default for most Business apps):
  - Transaction Search (`transactions`, `balances`): `https://uri.paypal.com/services/reporting/search/read`
  - Disputes (`disputes`): `https://uri.paypal.com/services/disputes/read-seller`
  - Invoicing (`invoices`): `https://uri.paypal.com/services/invoicing`
  - Catalog Products (`products`, `product_details`): `https://uri.paypal.com/services/subscriptions`
- **Network access**: The environment running the connector must be able to reach `https://api-m.paypal.com` (production) or `https://api-m.sandbox.paypal.com` (sandbox).
- **Lakeflow / Databricks environment**: A workspace where you can create a Unity Catalog connection and run Lakeflow ingestion pipelines.

## Setup

### Required Connection Parameters

The connector authenticates with **OAuth 2.0 client credentials**. You supply the Client ID and Client Secret of your PayPal app; the connection exchanges them for an access token automatically and refreshes it before it expires. You never paste an access token, and there is no browser login or redirect URI to register — authentication is machine-to-machine.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `client_id` | string | Yes | Client ID of your PayPal REST API app. | `AbCdEf123...` |
| `client_secret` | string | Yes | Client Secret issued alongside the Client ID. Treat it as a secret. | `EFgHiJ456...` |
| `environment` | string | No | PayPal API environment: `production` (default) or `sandbox`. Must match the environment the app credentials belong to. | `sandbox` |
| `base_url` | string | No | Custom base URL for the PayPal API. Overrides `environment`. Only needed for non-standard endpoints. | `https://api-m.paypal.com` |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of table-specific option names that may be passed through to the connector. Must be set to the full list below. | `start_date,window_seconds,page_size,max_records_per_batch,lookback_seconds,currency_code,as_of_time` |

The full list of supported table-specific options for `externalOptionsAllowList` is:

`start_date,window_seconds,page_size,max_records_per_batch,lookback_seconds,currency_code,as_of_time`

> **Note**: Table-specific options such as `start_date` or `currency_code` are **not** connection parameters. They are set per table under `table_configuration` in the pipeline spec, and their names must be included in `externalOptionsAllowList` for the connection to accept them.

### Obtaining the Client ID and Client Secret

1. Sign in to the [PayPal Developer Dashboard](https://developer.paypal.com/dashboard/) with your PayPal Business account.
2. Use the **Sandbox / Live** toggle at the top of the dashboard to select the environment. Sandbox and live apps have different credentials.
3. Go to **Apps & Credentials** and click **Create App** (or open an existing app).
4. Give the app a name, choose the **Merchant** app type, and create it.
5. On the app's page, copy the **Client ID** and the **Secret** (click **Show** to reveal it). These are the `client_id` and `client_secret` connection parameters.
6. Under the app's **Features** (or **Live/Sandbox app settings** in older dashboards), make sure the following are enabled for the tables you need:
   - **Transaction search** — required for `transactions` and `balances`. Allow up to 9 hours for this to take effect after enabling.
   - **Disputes** — required for `disputes`.
   - **Invoicing** — required for `invoices`.
   - **Subscriptions** (Catalog Products) — required for `products` and `product_details`.
7. Save the changes. If you are using sandbox credentials, set `environment` to `sandbox` on the connection.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide `client_id` and `client_secret` (and optionally `environment` / `base_url`), and set `externalOptionsAllowList` to `start_date,window_seconds,page_size,max_records_per_batch,lookback_seconds,currency_code,as_of_time` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The PayPal connector exposes a **static list** of tables. Table names must be used in lowercase exactly as shown:

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|
| `transactions` | Merchant account transactions from Transaction Search (payments, refunds, fees, transfers, etc.) | `cdc` | `transaction_id` | `transaction_updated_date` |
| `disputes` | Customer disputes, inquiries and chargebacks | `cdc` | `dispute_id` | `update_time` |
| `balances` | Current account balance per currency | `snapshot` | `currency` | n/a |
| `invoices` | Invoices created with PayPal Invoicing, including line items, payments and refunds | `snapshot` | `id` | n/a |
| `products` | Catalog products (summary fields from the list endpoint) | `snapshot` | `id` | n/a |
| `product_details` | Full catalog product records (type, category, URLs, update time) | `snapshot` | `id` | n/a |

### Incremental tables

- **`transactions`**
  - Read in consecutive time windows (7 days by default, at most 31 days — the PayPal limit). Every window is read completely before the cursor advances to the window end.
  - On the first run, reading starts at `start_date` if set; otherwise it starts **3 years** back, which is the maximum history PayPal Transaction Search provides.
  - On subsequent runs, the connector re-reads a lookback period (1 day by default) before the stored cursor. PayPal can take up to 3 hours to make a transaction searchable, so the lookback catches late-arriving and recently updated transactions. Duplicates are merged on `transaction_id`.
  - If a window contains more than 10,000 transactions (PayPal returns `RESULTSET_TOO_LARGE`), the connector automatically shrinks the window and retries.
  - `transaction_id` and `transaction_updated_date` are copied to the top level of each row from the nested `transaction_info` struct so they can be used as the primary key and cursor.
- **`disputes`**
  - Filtered server-side by update time. On the first run, all disputes are read unless `start_date` is set (PayPal returns at most 180 days of dispute history).
  - On subsequent runs, a lookback period (7 days by default) is applied before the stored cursor, because disputes can be updated retroactively while evidence is under review.
  - Records are ordered by `update_time`; when `max_records_per_batch` is reached, disputes sharing the same `update_time` are kept in the same batch.

Each pipeline run reads data only up to the time the run started; newer data is picked up by the next run.

### Snapshot tables

`balances`, `invoices`, `products` and `product_details` are fully re-read on every run.

- `balances` returns one row per currency held in the account. Account-level fields (`account_id`, `as_of_time`, `last_refresh_time`) are added to every row.
- `invoices` includes the optional `items`, `payments` and `refunds` sections.
- `product_details` lists all products and then requests the full record for each product individually. Products removed between the two calls are skipped.

### Deletes

The connector does not synchronize deletes. PayPal does not delete transactions or disputes: reversed transactions appear with `transaction_info.transaction_status = 'V'`, and closed disputes keep a terminal `status` such as `RESOLVED`.

### Schema highlights

- Nested PayPal objects are preserved as structs and arrays (for example `transactions.payer_info`, `transactions.cart_info.item_details`, `invoices.detail`, `invoices.primary_recipients`).
- All monetary amounts are structs of `{currency_code, value}`, where `value` is a **string** (for example `"49.99"`). Cast to `DECIMAL` downstream for arithmetic.
- `transaction_info.transaction_status` is a single-character code: `D` (denied), `P` (pending), `S` (success), `V` (reversed).
- `transaction_info.protection_eligibility` is a two-digit code: `01` (eligible), `02` (not eligible), `03` (partially eligible).
- Empty nested objects returned by PayPal (for example an empty `shipping_info`) are stored as `null`.

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
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

All options are optional. Numeric values above the maximum are capped at the maximum.

| Option | Applies to | Default | Description |
|---|---|---|---|
| `start_date` | `transactions`, `disputes`, `invoices` | `transactions`: 3 years ago; `disputes` / `invoices`: no limit | RFC 3339 timestamp (e.g. `2025-01-01T00:00:00Z`). For `transactions` and `disputes`, the starting point of the first run (ignored once a cursor is stored). For `invoices`, only invoices **created** on or after this time are read on every run. |
| `window_seconds` | `transactions` | `604800` (7 days) | Size of each Transaction Search time window, in seconds. Maximum `2678400` (31 days). |
| `page_size` | `transactions`, `disputes`, `invoices` | Maximum for each table | Records per API page. Maximums: `transactions` 500, `disputes` 50, `invoices` 100. |
| `max_records_per_batch` | `transactions`, `disputes` | `1000` | Target number of records per micro-batch. For `transactions` this is best-effort (the current window is always finished). For `disputes`, records are truncated at the limit, keeping records with the same `update_time` together. |
| `lookback_seconds` | `transactions`, `disputes` | `transactions`: `86400` (1 day); `disputes`: `604800` (7 days) | How far before the stored cursor to start re-reading on each run, to catch late or retroactive updates. |
| `currency_code` | `balances` | All currencies | ISO-4217 currency code (e.g. `USD`) to return the balance for a single currency. |
| `as_of_time` | `balances` | Most recent refresh | RFC 3339 timestamp to return balances as of a point in time (up to 3 years back). |

`products` and `product_details` take no source-specific options.

## Data Type Mapping

| PayPal API Type | Example Fields | Databricks Type | Notes |
|---|---|---|---|
| string | `transaction_id`, `dispute_id`, `status`, `reason` | `STRING` | IDs, codes and enums are stored as strings. |
| money object `{currency_code, value}` | `transaction_amount`, `dispute_amount`, `total_balance` | `STRUCT<currency_code: STRING, value: STRING>` | `value` is a decimal string; cast to `DECIMAL` downstream. |
| datetime (RFC 3339) | `transaction_updated_date`, `update_time`, `create_time`, `as_of_time` | `TIMESTAMP` | Stored in UTC. Inputs with or without milliseconds and with compact offsets (e.g. `+0000`) are supported. |
| date (`YYYY-MM-DD`) | `detail.invoice_date`, `detail.payment_term.due_date`, `item_date` | `DATE` | |
| boolean | `primary`, `cart_info.tax_inclusive` | `BOOLEAN` | |
| quantity / percentage | `quantity`, `item_quantity`, `percent` | `STRING` | PayPal returns these as strings. |
| object | `payer_info`, `shipping_info`, `detail`, `invoicer` | `STRUCT` | Nested objects are preserved; empty objects become `null`. |
| array | `links`, `items`, `primary_recipients`, `cart_info.item_details` | `ARRAY` | Arrays of primitives or structs. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Add a `table` entry for each PayPal table you want to ingest, with optional source-specific options under `table_configuration`:

```json
{
  "pipeline_spec": {
    "connection_name": "paypal_connection",
    "object": [
      {
        "table": {
          "source_table": "transactions",
          "table_configuration": {
            "start_date": "2025-01-01T00:00:00Z",
            "window_seconds": "604800",
            "lookback_seconds": "86400"
          }
        }
      },
      {
        "table": {
          "source_table": "disputes",
          "table_configuration": {
            "start_date": "2025-06-01T00:00:00Z"
          }
        }
      },
      {
        "table": {
          "source_table": "balances",
          "table_configuration": {
            "currency_code": "USD"
          }
        }
      },
      {
        "table": {
          "source_table": "invoices",
          "table_configuration": {
            "start_date": "2024-01-01T00:00:00Z"
          }
        }
      },
      {
        "table": {
          "source_table": "products"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the Unity Catalog connection configured with your PayPal `client_id` and `client_secret`.
- `source_table` must be one of the supported table names listed above.

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

Run the pipeline with your standard Databricks orchestration (for example, a scheduled job). For `transactions` and `disputes`, the first run backfills from `start_date` (or the default history) and later runs read only new and updated records plus the lookback period.

#### Best Practices

- **Start Small**: Begin with one or two tables (for example `balances` and `transactions` with a recent `start_date`) to validate credentials and data shape before backfilling the full 3 years of transactions.
- **Use Incremental Sync**: `transactions` and `disputes` are incremental; prefer them over repeatedly re-reading snapshot tables.
- **Set Appropriate Schedules**: PayPal makes transactions searchable up to 3 hours after they occur, so syncing `transactions` more often than hourly brings little benefit. Snapshot tables such as `invoices` and `product_details` re-read everything on each run; schedule them less frequently.
- **Large product catalogs**: `products` reads 20 records per request, and `product_details` makes one request per product. For catalogs with tens of thousands of products, these tables can take a long time; run them off-peak or less often.
- **Rate limits**: PayPal does not publish fixed rate limits and throttles dynamically (HTTP `429`). The connector retries throttled and transient server errors (`429`, `500`, `502`, `503`, `504`) with exponential backoff, honoring `Retry-After` when present. Avoid running many pipelines against the same PayPal account concurrently.
- **Tune windows for high-volume accounts**: If an account has very many transactions per week, lower `window_seconds` to reduce automatic window splitting.

#### Troubleshooting

**Common Issues:**

- **`PayPal OAuth token request failed (401)`**: The `client_id` / `client_secret` pair is invalid, or the credentials belong to a different environment. Sandbox credentials require `environment` set to `sandbox`; live credentials require `production` (the default).
- **`403` / `NOT_AUTHORIZED` on `transactions` or `balances`**: Transaction Search is not enabled on the app, or was enabled less than 9 hours ago. Enable **Transaction search** in the Developer Dashboard and wait for it to take effect.
- **`403` on `disputes`, `invoices`, or `products`**: The corresponding feature (Disputes, Invoicing, Subscriptions) is not enabled for the app. Enable it in the app settings.
- **`transactions` is empty for recent activity**: PayPal can take up to 3 hours to make a transaction searchable. The default 1-day lookback picks these up on the next run.
- **`RESULTSET_TOO_LARGE ... 1-second window`**: More than 10,000 transactions were recorded in a single second. This is extremely rare; contact PayPal support or narrow the data using a different approach.
- **Missing older disputes**: PayPal only returns up to 180 days of dispute history.
- **Missing invoice updates**: `invoices` with `start_date` filters by **creation** date. Invoices created before `start_date` are not read even if updated later; remove `start_date` to read all invoices.
- **Rate limiting (`429`)**: The connector retries automatically. If failures persist, reduce how often pipelines run or how many run in parallel against the same account.

## References

- [PayPal REST API overview and authentication](https://developer.paypal.com/api/rest/)
- [PayPal Developer Dashboard](https://developer.paypal.com/dashboard/)
- [Transaction Search API v1](https://developer.paypal.com/api/transaction-search/v1/)
- [Customer Disputes API v1](https://developer.paypal.com/api/customer-disputes/v1/)
- [Invoicing API v2](https://developer.paypal.com/api/invoicing/v2/)
- [Catalog Products API v1](https://developer.paypal.com/api/catalog-products/v1/)
- [PayPal API rate limiting](https://developer.paypal.com/api/rest/reference/rate-limiting/)
