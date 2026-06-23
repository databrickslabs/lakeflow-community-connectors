# Lakeflow Tabs Platform Community Connector

This documentation describes how to configure and use the **Tabs Platform** Lakeflow community connector to ingest billing data from the [Tabs Platform API v3](https://integrators.prod.api.tabsplatform.com) into Databricks.

Tabs is a B2B billing and revenue platform that turns contracts into invoices, tracks payments, and reconciles revenue against ERP/CRM systems. This connector syncs the core billing objects — invoices and their line items, payments, customers, contracts, obligations, items, and revenue categories — so you can analyze billing, AR, and revenue data alongside the rest of your warehouse.

## Prerequisites

- **Tabs Platform account**: You need an account on the Tabs application for the organization whose billing data you want to read.
- **API key**:
  - Created in the Tabs application under the **Developers** section (admin privileges required).
  - Supplied to the connector as the `api_key` connection option.
  - The key is passed **raw** in the `Authorization` header — it is **not** a Bearer token. The connector handles the header for you; you only need to provide the key value.
- **Network access**: The environment running the connector must be able to reach `https://integrators.prod.api.tabsplatform.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector.

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_key` | string | yes | The raw Tabs Platform API key created in the Tabs Developers section. Passed directly in the `Authorization` header (not as a Bearer token). Treat it like a password. | `tabs_live_xxx...` |
| `base_url` | string | no | Base URL for the Tabs API. Override only if Tabs directs you to a non-default host; otherwise defaults to `https://integrators.prod.api.tabsplatform.com`. | `https://integrators.prod.api.tabsplatform.com` |

> **Note**: `start_date`, `window_days`, `lookback_days`, and `limit` are **table-specific options**, not connection parameters. They are provided per table via `table_configuration` in the pipeline spec (see [Table Configurations](#table-configurations)). For these to be passed through, their names must be listed in `externalOptionsAllowList`.

This connector supports table-specific options, so `externalOptionsAllowList` **must** be set as a connection option. The full, definitive list of supported table-specific options is:

```
window_days,lookback_days,start_date,limit,base_url
```

### Obtaining the API Key

1. Log in to the Tabs application.
2. Navigate to the **Developers** section (requires admin privileges). If you lack admin privileges, contact a Tabs administrator.
3. Create a new API key and copy it.
4. Store the key securely and supply it as the `api_key` connection option.

To rotate a compromised key, contact Tabs Support. Never expose the key in client-side code or public repositories.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `window_days,lookback_days,start_date,limit,base_url` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Tabs connector exposes a **static list** of eight tables (the Tabs API has no discovery endpoint). Use the exact lowercase, snake_case names below as the `source_table`:

- `invoices`
- `invoice_line_items`
- `payments`
- `customers`
- `contracts`
- `obligations`
- `items`
- `categories`

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|-------|-------------|----------------|-------------|--------------------|
| `invoices` | Itemized payment requests sent to customers | `cdc` | `id` | `lastUpdatedAt` |
| `invoice_line_items` | Individual line items exploded out of each invoice's `lineItems` array | `cdc` | `["invoiceId", "id"]` | `invoiceLastUpdatedAt` |
| `payments` | Transaction receipts settling invoices | `cdc` | `id` | `receivedAt` |
| `customers` | Businesses or individuals being billed | `cdc` | `id` | `lastUpdatedAt` |
| `contracts` | Billing agreements defining terms and obligations | `cdc_with_deletes` | `id` | `lastUpdatedAt` |
| `obligations` | Customer commitments tied to billing schedules | `snapshot` | `id` | n/a |
| `items` | Products/services appearing on invoices and obligations | `snapshot` | `id` | n/a |
| `categories` | Revenue category groupings for products/services | `snapshot` | `id` | n/a |

#### Notable columns and relationships

- **`invoice_line_items` is a derived child table.** Tabs does not expose line items on their own endpoint; the connector reads `/v3/invoices` and explodes each invoice's `lineItems` array into one row per line item. Each child row carries `invoiceId` (foreign key back to the parent invoice) and `invoiceLastUpdatedAt` (the parent's cursor), so the child table advances incrementally in lockstep with `invoices`. The composite primary key is `(invoiceId, id)` because a line-item `id` is only unique within its invoice.
- **Money fields** (`total`, `balanceRemaining`, `unitPrice`, line-item `total`, pricing-tier `amount`) are typed as `Decimal(18, 2)` to avoid floating-point rounding on currency values. Amounts are denominated in each customer's `defaultCurrency`.
- **`externalIds`** is present on most objects as an array of `{externalId, sourceType, metadata}` structs linking records to external ERP/CRM systems (QuickBooks, NetSuite, Salesforce, HubSpot, Stripe, etc.). It is preserved as a nested array rather than flattened.
- **Nested structs** such as `billingAddress` / `shippingAddress` (customers), `billingSchedule` and its `pricingTiers` (obligations), and the embedded `item` reference (line items) are preserved as Spark `StructType` / `ArrayType` rather than flattened.

### Delete synchronization

Only `contracts` supports delete detection (`cdc_with_deletes`). Tabs uses a **soft delete**: a deleted contract has a non-null `deletedAt` timestamp. The connector detects these via the Tabs filter `deletedAt:isnotnull`, scoped by the `lastUpdatedAt` date range so the scan stays bounded, and reports them to the pipeline as deletes.

No other object exposes a documented soft-delete or hard-delete signal, so deletions on other tables are not detected. For small dimension tables (`obligations`, `items`, `categories`), the `snapshot` ingestion mode performs a full refresh on every run, which naturally drops records that no longer exist at the source.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (one of the supported objects above) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Applicable to CDC and SNAPSHOT tables. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

All of the following are optional and must be listed in the connection's `externalOptionsAllowList` to be passed through.

| Option | Type | Applies to | Default | Description |
|--------|------|-----------|---------|-------------|
| `start_date` | string (`YYYY-MM-DD`) | incremental tables | 365 days before first run | The earliest date to read on the **first** run when there is no stored offset yet. Bounds the initial backfill. Ignored once a cursor has been committed. |
| `window_days` | integer | incremental tables | `7` | Size, in days, of each partition window. The cursor range is sliced into `window_days`-sized windows that Spark executors read in parallel. Smaller windows increase parallelism and reduce per-request payloads. |
| `lookback_days` | integer | incremental tables | `2` | Number of days subtracted from the read start to re-scan recently-updated records. Catches edits that land on a day already partly synced. Re-read rows are deduplicated by the primary key on upsert. |
| `limit` | integer | all tables | `100` | Page size for offset pagination. Capped at the API maximum of `100`. |
| `base_url` | string | all tables | connection default | Per-table override of the API base URL. Normally set only at the connection level. |

> Snapshot tables (`obligations`, `items`, `categories`) ignore `start_date`, `window_days`, and `lookback_days`; they are fully re-read on every run.

## Data Type Mapping

Tabs API JSON fields are mapped to Spark types as follows:

| Tabs API Type | Example Fields | Spark Type | Notes |
|---------------|----------------|------------|-------|
| `string` / `string (UUID)` / `string enum` | `id`, `status`, `method`, `defaultCurrency` | `StringType` | Enum values (e.g. invoice `status`) are stored as plain strings. |
| `string (YYYY-MM-DD)` | `issueDate`, `dueDate`, `serviceStartDate` | `DateType` | Pure date fields, no time component. |
| `string (ISO 8601 datetime)` | `createdAt`, `lastUpdatedAt`, `receivedAt`, `deletedAt` | `TimestampType` | Full timestamps. Note the incremental cursor is tracked at **date** granularity (see below). |
| `number` (money) | `total`, `balanceRemaining`, `unitPrice`, `amount` | `DecimalType(18, 2)` | Decimal to avoid float rounding on currency. |
| `number` (quantity / count) | `quantity`, `intervalFrequency`, `tierMinimum` | `DecimalType(18, 4)` | Higher scale for fractional quantities. |
| `number` (integer) | `netPaymentTerms`, `tierNumber` | `LongType` | |
| `boolean` | `isArrears`, `isRecurring` | `BooleanType` | |
| `object` | `billingAddress`, `billingSchedule`, line-item `item` | `StructType` | Nested objects preserved, not flattened. |
| `array` | `externalIds`, `lineItems`, `pricingTiers` | `ArrayType` | Preserved as nested collections. `lineItems` is also exploded into the `invoice_line_items` table. |

## Incremental Sync Behavior

The five incremental tables (`invoices`, `invoice_line_items`, `payments`, `customers`, `contracts`) sync using a **date-cursor** strategy backed by partitioned, parallel reads.

- **Date-only cursors.** The Tabs `filter` parameter only accepts the date part (`YYYY-MM-DD`) of the cursor field, even though the underlying field (`lastUpdatedAt` / `receivedAt`) is a full ISO timestamp. The connector therefore tracks the committed offset as a `YYYY-MM-DD` string. The cursor filter is an inclusive range built as `field:gte:"<since>",field:lte:"<until>"`.
- **Lookback window.** At read time the connector subtracts `lookback_days` (default `2`) from the start of the range and re-scans those days. This catches records edited later on a day that was already partly synced. The committed offset itself is **not** moved back, so the watermark always advances. Re-read rows are deduplicated against the primary key on upsert.
- **Partitioned, parallel reads.** The connector implements `SupportsPartitionedStream`. The `(last committed cursor, now]` date range is sliced into `window_days`-sized windows (default `7`), and Spark executors read each window in parallel — each window paginates through `/v3/...?filter=...&page=N&limit=...` independently. Snapshot tables read in a single batch on the driver.
- **First run.** With no stored offset, the read starts from `start_date` if provided, otherwise 365 days before the run. Use `start_date` to bound a heavy initial backfill on large accounts.
- **Termination.** The high-water mark is capped at the connector's initialization date, so successive offset reads stabilize and a `Trigger.AvailableNow` run terminates cleanly. Records that arrive after a run starts are picked up by the next run.

### Limitations

- **No sub-day cursor precision.** Because the Tabs filter is date-only, the smallest incremental granularity is one day. Any record updated on the boundary date can be re-read; this is intended and is the reason re-read rows are deduplicated on the primary key. Keep `lookback_days >= 1` so same-day edits are not missed between runs.
- **Offset pagination.** Tabs uses page/limit offset pagination with no documented stable sort. If records are inserted mid-pagination on a very active account, a page boundary can shift; the date-range filter plus lookback window minimize the impact, and primary-key upserts reconcile any duplicates.
- **Delete detection limited to contracts.** See [Delete synchronization](#delete-synchronization).
- **No published rate limits.** Tabs does not publish explicit rate limits, though `429` responses have been observed. The connector retries `429`/`5xx` with exponential backoff (honoring `Retry-After`); widen schedule intervals if you see sustained throttling.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Tabs connector source in your workspace. This will place the connector code (for example, `tabs.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g. `ingestion_pipeline.py` or a similar entrypoint), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Tabs connector (with `api_key` and `externalOptionsAllowList` set).
- One or more **tables** to ingest, each with optional `table_configuration` options.

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "tabs_connection",
    "object": [
      {
        "table": {
          "source_table": "invoices",
          "table_configuration": {
            "start_date": "2024-01-01",
            "window_days": "7",
            "lookback_days": "2"
          }
        }
      },
      {
        "table": {
          "source_table": "invoice_line_items",
          "table_configuration": {
            "start_date": "2024-01-01"
          }
        }
      },
      {
        "table": {
          "source_table": "payments",
          "table_configuration": {
            "start_date": "2024-01-01"
          }
        }
      },
      {
        "table": {
          "source_table": "contracts"
        }
      },
      {
        "table": {
          "source_table": "items"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Tabs `api_key` and an `externalOptionsAllowList` of `window_days,lookback_days,start_date,limit,base_url`.
- For each `table`, `source_table` must be one of the supported table names listed above.
- Table options such as `start_date`, `window_days`, and `lookback_days` are placed under `table_configuration`. Numeric options may be supplied as strings.

You can ingest additional tables (`customers`, `obligations`, `categories`) by adding more `table` entries with the appropriate options.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow). For incremental tables:

- On the **first run**, either:
  - Omit `start_date` to backfill the last 365 days, or
  - Set `start_date` to a recent cutoff to limit history.
- On **subsequent runs**, the connector uses the stored date cursor plus `lookback_days` to pick up late updates safely.

#### Best Practices

- **Start small**: Begin with one or two tables (e.g. `invoices`, `customers`) to validate configuration and data shape before adding the rest.
- **Use incremental sync**: For `invoices`, `invoice_line_items`, `payments`, `customers`, and `contracts`, rely on the date-cursor CDC pattern to minimize API calls.
- **Tune window and lookback**: Reduce `window_days` to increase parallelism on large accounts; keep `lookback_days` at `2` (or higher if updates frequently land late) to avoid gaps.
- **Bound the first run**: Set `start_date` to avoid an unbounded backfill on long-lived accounts.
- **Respect throttling**: Tabs does not publish rate limits. If you encounter sustained `429`s, widen schedule intervals; the connector already retries with exponential backoff.

#### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`401` / `403`)**:
  - Verify the `api_key` is correct and has not been rotated or revoked.
  - Ensure the key is supplied as the raw value — do **not** prepend `Bearer `. The connector sends it raw automatically.
- **`Tabs connector requires connection option 'api_key'`**:
  - The connection is missing the `api_key` parameter. Set it on the UC connection.
- **Table-specific options appear to be ignored**:
  - Confirm `externalOptionsAllowList` on the connection includes `window_days,lookback_days,start_date,limit,base_url`. Options not on the allowlist are dropped before reaching the connector.
- **Rate limiting (`429`)**:
  - The connector retries automatically with exponential backoff. For persistent throttling, widen schedule intervals or contact Tabs Support to confirm limits.
- **Missing same-day updates**:
  - Because cursors are date-only, ensure `lookback_days >= 1` so records edited on the boundary date are re-read.
- **Duplicate rows during a run**:
  - Expected for the lookback overlap and offset-pagination edge cases; primary-key upserts reconcile them. Confirm `primary_keys` are correct, especially the composite `(invoiceId, id)` for `invoice_line_items`.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/tabs/tabs.py`
- Connector schemas and metadata: `src/databricks/labs/community_connector/sources/tabs/tabs_schemas.py`
- Connector API research notes: `src/databricks/labs/community_connector/sources/tabs/tabs_api_doc.md`
- Connector spec: `src/databricks/labs/community_connector/sources/tabs/connector_spec.yaml`
- Official Tabs Platform documentation:
  - `https://docs.tabsplatform.com`
  - `https://docs.tabsplatform.com/docs/authentication`
  - `https://docs.tabsplatform.com/docs/filter-rules`
