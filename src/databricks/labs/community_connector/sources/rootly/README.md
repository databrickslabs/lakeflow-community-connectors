# Lakeflow Rootly Community Connector

Ingest Rootly incident and on-call configuration data into Databricks using read-only API requests.
This initial version supports incremental incidents and full refreshes of six configuration objects.
The November 3, 2026 sampled live run validated the earlier snapshot implementation (five records
per table), including numbered and incident cursor pagination. On November 11, targeted read-only live checks also passed for incremental incident paging,
the inclusive update filter, and `page_size=1000` on all seven endpoints. These sampled checks do not
establish full-account completeness; Databricks pipeline validation remains pending. See the
[review follow-up validation](../../../../../../tests/unit/sources/rootly/REVIEW_VALIDATION.md).

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

Names are case sensitive. All objects use primary key `id`. Incidents use incremental CDC upserts
ordered by top-level `updated_at` (`attributes.updated_at` in UTC); the other six objects use
full-refresh (SNAPSHOT) ingestion.
There are no deletion-event streams or write-back operations.

| Object | Data |
|---|---|
| `incidents` | Incidents visible to the API key, including status, timing and related metadata; incremental updates |
| `services` | Service configuration |
| `users` | User profiles returned by the users list endpoint |
| `teams` | Team configuration; the JSON:API resource `type` is `groups` |
| `severities` | Severity configuration |
| `escalation_policies` | Escalation policy configuration |
| `schedules` | Schedule configuration, not expanded shifts or current on-call assignments |

Rows preserve `id`, `type`, nested `attributes`, and any `relationships` declared by the resource
schema. For example, access incident titles with
`attributes.title`. The schema includes fields declared in Rootly's public list-response schemas;
unknown object shapes use VARIANT. See the [complete field reference](schema_reference.md) and
[relationship keys and SQL join examples](relationships.md), including teams → escalation policies.
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
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2` for change tracking |
| `primary_keys` | No | Override the default primary key `id` |
| `sequence_by` | No | Ordering column; incidents default to `updated_at` |
| `cluster_by` | No | Destination columns for Liquid Clustering |

### Rootly Options

| Option | Required | Default | Description |
|---|---|---|---|
| `page_size` | No | `100` | Integer from 1 through 1,000; records requested per HTTP page. Incidents checkpoint after each page, so this also bounds each incident batch. |

`page_size` does not cap the total run size. The six snapshot tables read every visible record;
incidents fetch changes since the previous completed window. The default stays at 100 to keep
individual responses modest; larger values reduce request overhead. Rootly documents a maximum
of 1,000 for these list APIs in its [pagination overview](https://docs.rootly.com/api-reference/overview).

### Incremental incident behavior

- The first run reads incidents updated before the connector's initialization time (UTC, whole
  seconds). Later runs use `filter[updated_at][gte]` with the saved watermark and
  `filter[updated_at][lt]` with the new cutoff, always with `sort=updated_at`.
- Windows include their lower bound and exclude their upper bound. An incident exactly at the
  cutoff is fetched on the next run. Opaque `page[after]` cursors handle ties across pages;
  filters, sort, cutoff and page size stay fixed while resuming a partial window.
- Each batch reads one page. Its checkpoint stores the window and next cursor; the timestamp
  watermark advances only after the final page succeeds. A retry can replay rows, which CDC
  upserts reconcile by `id` and `updated_at`. The same completed offset ends the run.
- Incidents deleted in Rootly, newly hidden by permissions, or made visible without an `updated_at`
  change cannot be reconciled by this feed. Run a full refresh to rebuild from the visible set.
  The API is not a historical change log or a point-in-time snapshot; it does not preserve every
  intermediate update. Delayed/backdated source updates can require a full refresh as well.
- Anyone testing the earlier snapshot-only PR revision must fully refresh/recreate their incident
  pipeline when adopting this revision; its ingestion mode and checkpoint contract have changed.

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

Start with a small object, compare the result with Rootly, then add other objects. Schedule pipeline
runs at a frequency appropriate to the volume. Incidents resume incrementally; configuration objects
refresh in full. Concurrent source changes can affect pagination, particularly numbered pages.

Rootly documents a default read limit of 3,000 requests per minute per key, which may vary by
configuration. HTTP 429 and transient server/network failures are retried up to five total attempts.
Server retry hints are honored; delays above 120 seconds fail the refresh so it can be retried later.

#### Troubleshooting

- **HTTP 401/403:** Check the key, its roles, and access to the selected object.
- **Missing records:** Check the key's organization/team scope and private-incident visibility.
  An empty successful list means no records were visible to that key, not necessarily that none exist.
- **HTTP 429 or exhausted retries:** Reduce concurrent refreshes or schedule less frequently.
- **Invalid pagination or response:** The read fails rather than advancing an incomplete checkpoint.
  Check for API contract changes and update the connector before retrying.
- **Large accounts:** The initial incident load and configuration snapshots can be expensive. Increasing
  `page_size` (up to 1,000) reduces request count; lowering it reduces response/batch size.

## References

- [Rootly API overview](https://docs.rootly.com/api-reference/overview)
- [List incidents](https://docs.rootly.com/api-reference/incidents/list-incidents)
- [Rootly public OpenAPI definition](https://rootly-heroku.s3.amazonaws.com/swagger/v1/swagger.json)
- [Connector API research](rootly_api_doc.md)
- [Schema field reference](schema_reference.md)
