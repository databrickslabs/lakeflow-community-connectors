# Lakeflow Terna Community Connector

This documentation describes how to configure and use the **Terna** Lakeflow community connector to ingest Italian electricity system data (load, generation, and transmission) from the [Terna Public API](https://developer.terna.it) into Databricks.


## Overview

The connector pulls time-series data from Terna’s public APIs:

- **Load** – Total demand (and forecast) by bidding zone  
- **Generation** – Actual and renewable generation by primary source  
- **Transmission** – Physical cross-border electricity flows  

Data is requested by date range; the connector uses OAuth 2.0 Client Credentials for most endpoints and optionally an API key for the Physical Foreign Flow (transmission) endpoint.


## Prerequisites

- **Terna developer account**: Register at the [Terna developer portal](https://developer.terna.it/member/register) and [create an application](https://developer.terna.it/docs/read/How_to_register_an_application) to obtain `client_id` and `client_secret`.
- **Network access**: The environment running the connector must reach `https://api.terna.it` (and, if used, the transmission API base URL).
- **Lakeflow / Databricks**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.


## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name             | Type   | Required | Description                                                                 | Example                    |
|------------------|--------|----------|-----------------------------------------------------------------------------|----------------------------|
| `client_id`      | string | yes      | OAuth 2.0 Client Credentials application key (API Key).                     | From Terna developer portal |
| `client_secret`  | string | yes      | OAuth 2.0 Client Credentials secret.                                         | From Terna developer portal |
| `base_url`       | string | no       | Base URL for the Terna API. Defaults to `https://api.terna.it`.              | `https://api.terna.it`     |
| `x_api_key`      | string | no       | Optional API key for the Physical Foreign Flow endpoint when it uses `x-api-key` instead of the OAuth Bearer token. Leave empty if that endpoint accepts the same OAuth token. | From Terna if required     |
| `externalOptionsAllowList` | string | no* | Comma-separated list of table-specific option names allowed to be passed through. Required only if you use table options. | `date_from,date_to,chunk_days,biddingZone,bidding_zone,type` |

\* Set `externalOptionsAllowList` if you use any table-specific options (e.g. `date_from`, `date_to`, `chunk_days`, `biddingZone`, `type`). Full list: `date_from,date_to,chunk_days,biddingZone,bidding_zone,type`.

### Obtaining Credentials

- **OAuth (`client_id`, `client_secret`)**:
  1. Register at [Terna developer portal](https://developer.terna.it/member/register).
  2. Create an application and obtain the application key (API Key) and secret as described in [How to register an application](https://developer.terna.it/docs/read/How_to_register_an_application).
  3. Use these as `client_id` and `client_secret` in the connection. The connector obtains access tokens at runtime from `https://api.terna.it/public-api/access-token`.
- **Transmission API (`x_api_key`)**: If the Physical Foreign Flow endpoint is configured to use an API key instead of the OAuth token, obtain the key from Terna and set `x_api_key` in the connection. Otherwise leave it unset.

### Create a Unity Catalog Connection

1. Use the **Lakeflow Community Connector** flow from the **Add Data** page, or select/create a connection for this source.
2. Set `client_id` and `client_secret` (required).
3. Optionally set `base_url` and `x_api_key`.
4. If you use table options such as `date_from`, `date_to`, or `biddingZone`, set `externalOptionsAllowList` to include those option names (e.g. `date_from,date_to,chunk_days,biddingZone,bidding_zone,type`).

The connection can also be created via the Unity Catalog API.


## Supported Objects

The Terna connector exposes a **static list** of tables:

| Table                    | Description                                                                 | Ingestion Type | Primary Key                    | Cursor   |
|--------------------------|-----------------------------------------------------------------------------|----------------|--------------------------------|----------|
| `total_load`             | Total demand trend (and forecast) by bidding zone                          | append         | `date`, `bidding_zone`         | `date`   |
| `actual_generation`      | Net hourly generation by primary source (Thermal, Wind, Hydro, etc.)        | append         | `date`, `primary_source`       | `date`   |
| `renewable_generation`   | Hourly renewable generation by energy source                               | append         | `date`, `energy_source`        | `date`   |
| `physical_foreign_flow`  | Physical electricity exchange between Italy and neighbouring TSOs          | append         | `date`, `country`              | `date`   |

- All tables are **append-only**; the connector reads by date range and advances a cursor (`date`, format `yyyy-mm-dd hh:mm:ss`).
- No schema discovery API; schemas are fixed and match the API response structures in `terna_api_doc.md`.


## Table Configurations

### Source & Destination

| Option                  | Required | Description |
|--------------------------|----------|-------------|
| `source_table`           | Yes      | Table name in the source system |
| `destination_catalog`    | No       | Target catalog (defaults to pipeline default) |
| `destination_schema`     | No       | Target schema (defaults to pipeline default) |
| `destination_table`     | No       | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

| Option           | Required | Description |
|------------------|----------|-------------|
| `scd_type`       | No       | Not applicable; all tables are append-only. |
| `primary_keys`   | No       | Override the connector’s default primary keys. |
| `sequence_by`    | No       | Not used for append-only tables. |

### Source-specific `table_configuration` options

Optional table options (must be listed in the connection’s `externalOptionsAllowList` if used):

| Option        | Tables                    | Required | Description |
|---------------|---------------------------|----------|-------------|
| `date_from`   | All                       | Yes*     | Range start, format `dd/mm/yyyy`. *Required for the intended contract; if omitted, connector falls back to last 30 days before yesterday. |
| `date_to`     | All                       | No       | Range end, format `dd/mm/yyyy`. Optional; when omitted, the connector uses **current execution time** as the end. |
| `chunk_days`  | All                       | No       | Number of days per API request chunk (integer, minimum 1, max 60). Default: 1. Ranges longer than 60 days are split into multiple API calls. |
| `biddingZone` or `bidding_zone` | `total_load` only | No | Bidding zone filter. Allowed values: `North`, `Centre-North`, `South`, `Centre-South`, `Sardinia`, `Sicily`, `Calabria`, `Italy`. |
| `type`        | `actual_generation`, `renewable_generation` | No | Primary source / energy source filter. Allowed (e.g.): `Thermal`, `Wind`, `Geothermal`, `Photovoltaic`, `Self-consumption`, `Hydro`. |

- **First run**: Provide `date_from` (required). Optionally provide `date_to`; if omitted, data is read up to **current execution time**. Ranges longer than 60 days are chunked into multiple API calls of at most 60 days each; the last chunk may end at current time (e.g. not midnight).
- **Subsequent runs (CDC when `date_to` is omitted)**: Each run fetches from the **last stored cursor** (from the previous run) up to **current execution time**. If that span is more than 60 days, the connector makes multiple 60-day API calls automatically. The stored `start_offset` (cursor) is used so you always resume from where you left off.
- **Subsequent runs (bounded when `date_to` is set)**: The connector uses the stored cursor and does not go past the requested `date_to` (persisted as `range_end` in the offset).


## Schema Highlights

- **Dates**: `date` is `yyyy-mm-dd hh:mm:ss`; `date_tz` and `date_offset` are strings (e.g. `Europe/Rome`, `+02:00`).
- **Numeric fields**: The API returns values as strings (e.g. `total_load_MW`, `actual_generation_GWh`, `physical_foreign_flow_MW`, `import`, `export`). Downstream you can cast to numeric types as needed.
- **physical_foreign_flow**: Column name `import` is reserved in some contexts; the schema keeps the API field name `import` as a string column.


## Data Type Mapping

| API / response pattern        | Connector type | Notes |
|-------------------------------|----------------|--------|
| `date`                        | string         | `yyyy-mm-dd hh:mm:ss`; use for cursor and time ordering. |
| `date_tz`, `date_offset`       | string         | For interpretation only. |
| `*_MW`, `*_GWh`, `import`, `export` | string   | API returns strings; cast to decimal/double in SQL or Spark as needed. |
| `bidding_zone`, `country`, `primary_source`, `energy_source` | string | Category/enum values. |


## How to Run

### Step 1: Use the connector in your pipeline

Use the Lakeflow Community Connector UI to add or reference the Terna connector in your workspace so Lakeflow can load it.

### Step 2: Configure the pipeline

In your pipeline spec, reference the Unity Catalog connection and the tables to ingest. Example:

```json
{
  "pipeline_spec": {
    "connection_name": "terna_connection",
    "object": [
      {
        "table": {
          "source_table": "total_load",
          "table_configuration": {
            "date_from": "01/01/2024",
            "date_to": "31/01/2024",
            "chunk_days": "7",
            "biddingZone": "Italy"
          }
        }
      },
      {
        "table": {
          "source_table": "actual_generation",
          "table_configuration": {
            "date_from": "01/01/2024",
            "date_to": "31/01/2024"
          }
        }
      },
      {
        "table": {
          "source_table": "renewable_generation"
        }
      },
      {
        "table": {
          "source_table": "physical_foreign_flow"
        }
      }
    ]
  }
}
```

- Omit `date_from`/`date_to` to use the default initial range (last 30 days).
- Omit table-specific options (e.g. `biddingZone`, `type`) to fetch all zones/sources.

### Step 3: Run and schedule

Run the pipeline with your usual Lakeflow/Databricks orchestration. The connector advances the cursor after each run; subsequent runs continue from the last `date`.

#### Best practices

- **Start small**: Ingest one table and a short date range to validate credentials and data shape.
- **Chunk size**: Use `chunk_days` (e.g. 7 or 30) to balance response size and number of requests; default is 1 day.
- **Rate limits**: Terna tokens expire in 300 seconds; the connector refreshes automatically. If you see 403 with “Over Qps”, the connector backs off and retries; consider spacing scheduled runs if needed.
- **Transmission API**: If `physical_foreign_flow` fails with auth errors, confirm whether that endpoint uses OAuth or `x-api-key` and set `x_api_key` only if required.

#### Troubleshooting

| Issue | What to check |
|-------|-------------------------------|
| **Authentication (401/403)** | Verify `client_id` and `client_secret`; ensure the application is active on the Terna developer portal. |
| **Token errors** | Connector uses `https://api.terna.it/public-api/access-token`. Ensure `base_url` is correct and reachable. |
| **Empty or no data** | Confirm `date_from`/`date_to` format is `dd/mm/yyyy`. For `total_load`, check `biddingZone` values (e.g. `Italy`, `North`). For generation tables, check `type` values. |
| **physical_foreign_flow 401/403** | If the transmission API uses a different base URL or API key, set `base_url` and/or `x_api_key` as provided by Terna. |
| **Rate limit (403 “Over Qps”)** | Connector retries with backoff; reduce frequency of runs or increase `chunk_days` to reduce request count. |


## References

- Connector implementation: `src/databricks/labs/community_connector/sources/terna/terna.py`
- Connector schemas: `src/databricks/labs/community_connector/sources/terna/terna_schemas.py`
- API summary and endpoints: `src/databricks/labs/community_connector/sources/terna/terna_api_doc.md`
- Terna developer portal: [https://developer.terna.it](https://developer.terna.it)
- APIs catalog: [https://developer.terna.it/docs/read/apis_catalog](https://developer.terna.it/docs/read/apis_catalog)
