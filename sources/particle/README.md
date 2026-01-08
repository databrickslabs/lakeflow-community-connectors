# Lakeflow Particle.io Community Connector

This documentation describes how to configure and use the **Particle.io** Lakeflow community connector to ingest data from the Particle Device Cloud REST API into Databricks.

## Prerequisites

- **Particle account**: You need a Particle.io account with access to the devices, products, and SIM cards you want to read.
- **Authentication credentials** (one of the following):
  - **Access Token**: A Particle access token from the Particle Console (Settings > Access Tokens)
  - **Username/Password**: Your Particle account email and password for automatic OAuth token generation
- **Network access**: The environment running the connector must be able to reach `https://api.particle.io`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. Choose one of the two authentication methods:

#### Authentication Method 1: Access Token

| Name            | Type   | Required | Description                                                                  | Example                      |
|-----------------|--------|----------|------------------------------------------------------------------------------|------------------------------|
| `access_token`  | string | yes      | Particle access token for API authentication.                                | `abc123...`                  |

#### Authentication Method 2: OAuth Password

| Name       | Type   | Required | Description                              | Example                  |
|------------|--------|----------|------------------------------------------|--------------------------|
| `username` | string | yes      | Particle account email address.          | `user@example.com`       |
| `password` | string | yes      | Particle account password.               | `SuperSecret123`         |

#### Common Parameters (both methods)

| Name               | Type   | Required | Description                                                                                       | Example                         |
|--------------------|--------|----------|---------------------------------------------------------------------------------------------------|---------------------------------|
| `base_url`         | string | no       | Base URL for the Particle API. Override if using a custom instance. Defaults to `https://api.particle.io`. | `https://api.particle.io`       |
| `token_cache_file` | string | no       | File path to cache generated OAuth tokens (improves performance).                                  | `/tmp/particle_token.txt`       |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector requires table-specific options, so this parameter must be set. | See below |

The full list of supported table-specific options for `externalOptionsAllowList` is:

```
product_id_or_slug,device_id,iccid,groups,device_name,serial_number,sort_attr,sort_dir,per_page,max_pages,start_date,end_date
```

> **Note**: Table-specific options such as `product_id_or_slug`, `device_id`, or `iccid` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **Particle Access Token**:
  1. Sign in to the Particle Console at https://console.particle.io/
  2. Navigate to **Settings → Access Tokens**
  3. Create a new access token or use an existing one
  4. Copy the token and store it securely. Use this as the `access_token` connection option.

- **Username/Password Authentication**:
  - Use your Particle account email and password
  - The connector will automatically generate an access token via OAuth password grant
  - Optionally set `token_cache_file` to cache the generated token for improved performance

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `product_id_or_slug,device_id,iccid,groups,device_name,serial_number,sort_attr,sort_dir,per_page,max_pages,start_date,end_date` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Particle connector exposes the following **static list** of tables:

| Table              | Description                                        | Ingestion Type | Primary Key | Required Table Options   |
|--------------------|----------------------------------------------------|----------------|-------------|--------------------------|
| `devices`          | All devices owned by the authenticated user        | `snapshot`     | `id`        | None                     |
| `products`         | All products accessible to the user                | `snapshot`     | `id`        | None                     |
| `sims`             | All SIM cards owned by the user                    | `snapshot`     | `iccid`     | None                     |
| `user`             | Authenticated user information                     | `snapshot`     | `username`  | None                     |
| `oauth_clients`    | OAuth clients for the authenticated user           | `snapshot`     | `id`        | None                     |
| `product_devices`  | Devices within a specific product                  | `snapshot`     | `id`        | `product_id_or_slug`     |
| `product_sims`     | SIM cards within a specific product                | `snapshot`     | `iccid`     | `product_id_or_slug`     |
| `diagnostics`      | Historical device vitals/diagnostics               | `append`       | `updated_at`| `device_id`              |
| `sim_data_usage`   | SIM card data usage for current billing period     | `snapshot`     | `iccid`     | `iccid`                  |
| `fleet_data_usage` | Fleet-wide SIM data usage for a product            | `snapshot`     | N/A         | `product_id_or_slug`     |

### Required and Optional Table Options

Table-specific options are passed via the pipeline spec under `table` in `objects`. The options per table are:

#### product_devices

| Option              | Required | Description                                                                 |
|---------------------|----------|-----------------------------------------------------------------------------|
| `product_id_or_slug`| Yes      | Product ID or slug to list devices for                                      |
| `groups`            | No       | Filter by group names (comma-separated)                                     |
| `device_name`       | No       | Filter by device name (partial match)                                       |
| `device_id`         | No       | Filter by device ID (partial match)                                         |
| `serial_number`     | No       | Filter by serial number (partial match)                                     |
| `sort_attr`         | No       | Sort by: `deviceName`, `deviceId`, `firmwareVersion`, `lastConnection`      |
| `sort_dir`          | No       | Sort direction: `asc` or `desc`                                             |
| `per_page`          | No       | Records per page (default: 100, max: 100)                                   |
| `max_pages`         | No       | Maximum pages to fetch (default: 100)                                       |

#### product_sims

| Option              | Required | Description                                                                 |
|---------------------|----------|-----------------------------------------------------------------------------|
| `product_id_or_slug`| Yes      | Product ID or slug to list SIMs for                                         |
| `iccid`             | No       | Filter by ICCID (partial match)                                             |
| `device_id`         | No       | Filter by associated device ID                                              |
| `device_name`       | No       | Filter by associated device name                                            |
| `per_page`          | No       | Records per page (default: 100, max: 100)                                   |
| `max_pages`         | No       | Maximum pages to fetch (default: 100)                                       |

#### diagnostics

| Option       | Required | Description                                           |
|--------------|----------|-------------------------------------------------------|
| `device_id`  | Yes      | Device ID to get diagnostics for                      |
| `start_date` | No       | Oldest diagnostic to return (ISO8601 format)          |
| `end_date`   | No       | Newest diagnostic to return (ISO8601 format)          |

#### sim_data_usage

| Option  | Required | Description                      |
|---------|----------|----------------------------------|
| `iccid` | Yes      | SIM ICCID to get data usage for  |

#### fleet_data_usage

| Option              | Required | Description                                    |
|---------------------|----------|------------------------------------------------|
| `product_id_or_slug`| Yes      | Product ID or slug to get fleet data usage for |

### Schema Highlights

Full schemas are defined by the connector and align with the Particle API documentation:

- **devices**: Includes device metadata such as `id`, `name`, `platform_id`, `connected`, `last_heard`, and nested structures for `network` information.
- **products**: Product metadata including `id`, `name`, `slug`, `platform_id`, and device counts.
- **sims**: SIM card details including `iccid`, `status`, `data_limit`, and usage information.
- **diagnostics**: Historical device vitals including power, network, memory, and connection metrics.

## Data Type Mapping

Particle API JSON fields are mapped to Spark types as follows:

| Particle JSON Type  | Example Fields                                | Spark Type      | Notes                                              |
|---------------------|-----------------------------------------------|-----------------|---------------------------------------------------|
| integer             | `id`, `platform_id`, `device_count`           | `LongType`      | All numeric IDs stored as `LongType`               |
| string              | `name`, `status`, `iccid`                     | `StringType`    |                                                    |
| boolean             | `connected`, `online`, `cellular`             | `BooleanType`   |                                                    |
| ISO 8601 datetime   | `last_heard`, `created_at`, `updated_at`      | `StringType`    | Stored as UTC strings; cast downstream if needed   |
| object              | `network`, `vitals`, `service`                | `StructType`    | Nested objects preserved as structs                |
| array               | `functions`, `variables`, `groups`            | `ArrayType`     | Arrays preserved as nested collections             |
| nullable fields     | `notes`, `firmware_version`                   | Same + `null`   | Missing fields surfaced as `null`                  |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Particle connector source in your workspace. This will typically place the connector code (e.g., `particle.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingestion_pipeline.py`), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Particle connector.
- One or more **tables** to ingest, each with required `table_options`.

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "particle_connection",
    "object": [
      {
        "table": {
          "source_table": "devices"
        }
      },
      {
        "table": {
          "source_table": "products"
        }
      },
      {
        "table": {
          "source_table": "product_devices",
          "product_id_or_slug": "my-product-slug"
        }
      },
      {
        "table": {
          "source_table": "diagnostics",
          "device_id": "0123456789abcdef01234567",
          "start_date": "2024-01-01T00:00:00Z"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Particle credentials.
- For each `table`:
  - `source_table` must be one of the supported table names listed above.
  - Required table options (e.g., `product_id_or_slug`, `device_id`, `iccid`) must be provided for tables that need them.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

#### Best Practices

- **Start small**:
  - Begin with simple tables like `devices`, `products`, or `user` to validate configuration.
- **Add product-scoped tables**:
  - Once basic tables work, add `product_devices` and `product_sims` with your `product_id_or_slug`.
- **Enable diagnostics carefully**:
  - The `diagnostics` table can return large amounts of historical data. Use `start_date` and `end_date` to limit the range.
- **Respect rate limits**:
  - Particle allows 10,000 requests per 5 minutes per IP. Stagger syncs or reduce `max_pages` if you hit limits.

#### Troubleshooting

**Common Issues:**

- **Authentication failures (`401`)**:
  - Verify that the `access_token` is valid and not expired.
  - If using username/password, ensure credentials are correct.
  - Check that the token has not been revoked in the Particle Console.

- **`404 Not Found` for product tables**:
  - Verify that `product_id_or_slug` matches an actual product you have access to.
  - Check that the product has devices/SIMs associated with it.

- **Empty results for `diagnostics`**:
  - Ensure the `device_id` is correct and the device has diagnostic data.
  - Try expanding the date range with `start_date` and `end_date`.

- **Rate limiting (`429`)**:
  - Reduce `max_pages` or add delays between table syncs.
  - Consider running syncs during off-peak hours.

## Rate Limits

The Particle API has the following rate limits:

| Scope                   | Limit                           |
|-------------------------|---------------------------------|
| General API calls       | 10,000 requests / 5 minutes per IP |
| SSE connections         | 100 simultaneous connections per IP |
| Serial number lookup    | 50 requests / hour per user     |

## References

- Connector implementation: `sources/particle/particle.py`
- Official Particle Cloud API documentation:
  - https://docs.particle.io/reference/cloud-apis/api/
  - https://docs.particle.io/getting-started/cloud/cloud-api/
