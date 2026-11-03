# Lakeflow Rootly Community Connector

Ingest Rootly incident and on-call configuration data into Databricks using read-only API requests.
This initial version supports full refreshes of seven objects. It has been tested with synthetic
responses; live Rootly and Databricks pipeline validation are still pending.

## Prerequisites

- A Rootly account and API key permitted to read the objects you select.
- A Databricks workspace supporting Lakeflow Community Connectors and VARIANT columns.
- Network access from the ingestion environment to `https://api.rootly.com`.

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `api_token` | string (secret) | Yes | Rootly API key with read access to the selected resources | `<ROOTLY_API_KEY>` |
| `externalOptionsAllowList` | string | Yes | Complete list of supported source table options | `page_size` |

### Obtain a Rootly API Key

In Rootly, open the organization dropdown, choose **Organization Settings**, then **API Keys**.
Create a key with the minimum roles needed to read your selected objects. A global key is suitable
for organization-wide automation; personal and team keys restrict the records you can access.
Store the key as a secret in the Unity Catalog connection. Do not put it in notebooks or pipeline
specifications. See [Rootly API authentication](https://docs.rootly.com/api-reference/overview)
for current key types and permissions.

### Create a Unity Catalog Connection

Follow the Lakeflow Community Connector flow from **Add Data**, select the Rootly connector code,
and create or select a connection. Set `api_token` and set `externalOptionsAllowList` to `page_size`.
The standard Unity Catalog API can also create the connection. While this connector is awaiting
upstream review, use the code from this branch rather than assuming Rootly appears in the catalog.

## Supported Objects

Names are case sensitive. All objects use full-refresh (SNAPSHOT) ingestion with primary key `id`.
There are no incremental cursors, deletion-event streams, or write-back operations.

| Object | Data |
|---|---|
| `incidents` | Incidents visible to the API key, including status, timing and related metadata |
| `services` | Service configuration |
| `users` | User profiles returned by the users list endpoint |
| `teams` | Team configuration; the JSON:API resource `type` is `groups` |
| `severities` | Severity configuration |
| `escalation_policies` | Escalation policy configuration |
| `schedules` | Schedule configuration, not expanded shifts or current on-call assignments |

Rows preserve `id`, `type`, nested `attributes`, and any `relationships` declared by the resource
schema. For example, access incident titles with
`attributes.title`. The schema includes fields declared in Rootly's public list-response schemas;
unknown object shapes use VARIANT. See the [complete field reference](schema_reference.md).
Response-level `links`, `meta`, and `included` are not ingested as rows. No `include` expansion is
requested. A field absent from an API response becomes null; new fields require a schema update.

## Table Configurations

### Source & Destination

These options sit directly under each `table` in the pipeline specification.

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | One of the object names above |
| `destination_catalog` | No | Target catalog; defaults to the pipeline default |
| `destination_schema` | No | Target schema; defaults to the pipeline default |
| `destination_table` | No | Target table; defaults to `source_table` |

### Common `table_configuration` Options

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2` for snapshot change tracking |
| `primary_keys` | No | Override the default primary key `id` |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | Destination columns for Liquid Clustering |

### Rootly Options

| Option | Required | Default | Description |
|---|---|---|---|
| `page_size` | No | `100` | Integer from 1 through 100; records requested per HTTP page. This is a connector limit, not a claim about the API maximum. |

`page_size` does not cap the total refresh size. Every refresh reads every visible record of each
selected object. There are no date-range or incremental options in this version.

## Data Type Mapping

| Rootly/OpenAPI type | Databricks type |
|---|---|
| string, including timestamps and IDs declared as strings | STRING |
| integer | BIGINT |
| boolean | BOOLEAN |
| object with declared properties | STRUCT |
| object without a declared shape | VARIANT |
| array | ARRAY of the mapped element type |

All schema fields are nullable to tolerate optional fields and permission-dependent omissions.
Dates remain strings as returned by Rootly; convert them explicitly when querying.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use this connector's code and its prebuilt wheel with the Lakeflow Community Connector setup flow.
PySpark is supplied by the Databricks runtime and is not installed as a runtime dependency of this wheel.

### Step 2: Configure Your Pipeline

Set the connection name and selected objects in the main pipeline file:

```json
{
  "pipeline_spec": {
    "connection_name": "rootly_connection",
    "object": [
      {
        "table": {
          "source_table": "incidents",
          "table_configuration": {"page_size": "100"}
        }
      },
      {"table": {"source_table": "services"}}
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

Start with a small object, compare the result with Rootly, then add other objects. Schedule full
refreshes at a frequency appropriate to the volume. Pagination is not a point-in-time snapshot:
concurrent source changes can affect results, particularly with numbered pages. A later refresh
reconciles the current visible state; this connector does not preserve every intermediate change.

Rootly documents a default read limit of 3,000 requests per minute per key, which may vary by
configuration. HTTP 429 and transient server/network failures are retried up to five total attempts.
Server retry hints are honored; delays above 120 seconds fail the refresh so it can be retried later.

#### Troubleshooting

- **HTTP 401/403:** Check the key, its roles, and access to the selected object.
- **Missing records:** Check the key's organization/team scope and private-incident visibility.
  An empty successful list means no records were visible to that key, not necessarily that none exist.
- **HTTP 429 or exhausted retries:** Reduce concurrent refreshes or schedule less frequently.
- **Invalid pagination or response:** The refresh fails rather than accepting a partial snapshot.
  Check for API contract changes and update the connector before retrying.
- **Large accounts:** Full refreshes may be expensive. Lowering `page_size` reduces each response's
  size but increases request count; it does not reduce the total number of records read.

## References

- [Rootly API overview](https://docs.rootly.com/api-reference/overview)
- [List incidents](https://docs.rootly.com/api-reference/incidents/list-incidents)
- [Rootly public OpenAPI definition](https://rootly-heroku.s3.amazonaws.com/swagger/v1/swagger.json)
- [Connector API research](rootly_api_doc.md)
- [Schema field reference](schema_reference.md)
