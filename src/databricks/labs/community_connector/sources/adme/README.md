# Lakeflow ADME (Azure Data Manager for Energy) Community Connector

Ingest OSDU master-data records from **Azure Data Manager for Energy (ADME)** into Databricks Delta tables via Lakeflow Connect.

ADME is Microsoft's managed implementation of the OSDU Data Platform. This connector reads from the **OSDU Search Service** (`/api/search/v2/query_with_cursor`) and currently exposes three master-data record kinds: **Wellbore**, **Reservoir**, and **Rock_and_Fluid** (mapped to OSDU's `master-data--Sample`).

## Live validation status

**`service_principal` (original):** validated end-to-end against a live ADME instance by deploying the connector as a Databricks App. All three tables (**Wellbore**, **Reservoir**, **Rock_and_Fluid**) read cleanly through the connector's `LakeflowConnect.list_tables()` / `get_partitions()` / `read_partition()` paths against the live OSDU Search Service, using the Azure AD client-credentials auth flow against `login.microsoftonline.com` and the `data-partition-id` header pattern.

**`managed_identity` + `static_token` (this PR, v1.2.0):** validated against the same live ADME instance (`admesbxscusins1.energy.azure.com`, partition `opendes`) from an Azure Databricks cluster in the same Entra tenant. Both modes successfully obtain a bearer (the static-token run mints its bearer via `ManagedIdentityCredential` first, then plugs it into the connector under `auth_mode=static_token`), instantiate `ADMELakeflowConnect`, and complete `list_tables()` + `get_partitions()` + the search-with-cursor POST round-trip end-to-end. Cross-mode consistency checks for `list_tables()` output and partition shape are green. The actual record yield during the live run was 0 (the source partition is currently empty for `Wellbore`); this is a data-availability state, not an auth-mode behaviour.

**`federated_identity` (this PR, v1.2.0):** unit-test coverage only at this time (`tests/unit/sources/adme/test_adme_auth_modes.py`) — the dispatch through `azure.identity.ClientAssertionCredential` is exercised offline. Live validation pending a Workload Identity Federation setup on the target service principal.

Offline coverage is in place across all four modes: the connector's simulate-mode suite runs against an in-process source simulator that replays a corpus seeded from the OSDU master-data schemas; the simulator handler matches the request/response shape observed against live ADME.

The Search Service contract used by this connector is independently exercised end-to-end against live ADME by a sister Databricks reference codebase, [`databricks-industry-solutions/energy-sandbox/osdu-app-with-connector`](https://github.com/databricks-industry-solutions/energy-sandbox/tree/main/osdu-app-with-connector) (see `notebooks/00_smoke_test.py`).

## Prerequisites

- **An ADME instance**, provisioned in your Azure subscription, with at least one data partition (e.g. `opendes`). The connector reads from one data partition per Unity Catalog connection.
- **An Azure AD app registration (service principal)** that the connector uses to authenticate via the OAuth2 client-credentials flow. You'll need the app's **tenant ID**, **client ID**, and a **client secret**.
- **The `users.datalake.viewers@<data-partition-id>.dataservices.energy` entitlement** granted to the service principal in the ADME Entitlements service. Without this role, the Search Service returns `403 Forbidden`.
- **Network access** from the environment running the connector to:
  - `https://login.microsoftonline.com` (token endpoint)
  - `https://<your-instance>.energy.azure.com` (ADME data plane)
- **A Databricks workspace** where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your Unity Catalog connection options. The connector supports four auth modes; the table below lists the union, with required-by-mode notes in the description.

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `base_url` | string | yes | ADME instance base URL — `https://<instance-name>.energy.azure.com` (no trailing slash; the connector strips it if present). Alias: `instance_url` (still accepted for backwards compatibility). | `https://admetest.energy.azure.com` |
| `data_partition_id` | string | yes | OSDU data partition within the ADME instance. Sent as the `data-partition-id` header on every API call. Found in the ADME instance overview on the Azure portal under **Data Partitions**. | `opendes` |
| `auth_mode` | string | no | One of `service_principal` (default), `managed_identity`, `federated_identity`, `static_token`. | `federated_identity` |
| `adme_api_client_id` | string | no | App registration ID of the ADME API app. Used as the OAuth2 scope audience (`<id>/.default`). Defaults to `client_id` (legacy behaviour). | `22222222-2222-2222-2222-222222222222` |
| `tenant_id` | string | conditional | Azure AD tenant (Directory) ID. Required for `service_principal` and `federated_identity` modes. | `00000000-0000-0000-0000-000000000000` |
| `client_id` | string | conditional | Azure AD app registration client ID. Required for `service_principal` and `federated_identity` modes. The SP must be granted `users.datalake.viewers` on the data partition. | `11111111-1111-1111-1111-111111111111` |
| `client_secret` | string (secret) | conditional | App registration client secret. Required for `service_principal` mode only. | `Abc~1234…` |
| `managed_identity_client_id` | string | no | Client ID of a user-assigned Managed Identity. Omit for system-assigned MI. Used only when `auth_mode=managed_identity`. | `33333333-…` |
| `federated_token_file` | string | no | Path to a file holding the OIDC assertion. Used when `auth_mode=federated_identity`. | `/var/run/secrets/azure/token` |
| `federated_token` | string (secret) | no | Inline OIDC assertion (alternative to `federated_token_file`). Used when `auth_mode=federated_identity`. | `<jwt-string>` |
| `access_token` | string (secret) | no | Pre-issued bearer token. Required when `auth_mode=static_token`. CI/testing only. | `<jwt-string>` |
| `page_size` | string | no | Search Service page size, between 1 and 1000. Defaults to `1000` (the API maximum). Lowering the value increases the number of API calls but can reduce executor memory pressure. | `500` |

`managed_identity` and `federated_identity` require the `azure-identity` package on the cluster (`pip install "azure-identity>=1.15.0"`). `service_principal` and `static_token` have no extra dependencies.

This connector supports per-table options (`window_days`, `lookback_minutes`, and `kind_query_<table>` overrides — see **Table Configurations** below). Because of that, **`externalOptionsAllowList` must be set as a connection parameter** with the exact value:

```
window_days,lookback_minutes,kind_query_wellbore,kind_query_reservoir,kind_query_rock_and_fluid
```

### Obtaining the connection parameters

#### 1. Register an Azure AD app and create a client secret

1. In the Azure portal, go to **Microsoft Entra ID → App registrations → New registration**.
2. Name the app (e.g. `lakeflow-adme-ingest`), leave **Supported account types** as **Single tenant**, and click **Register**.
3. From the app overview, copy the **Application (client) ID** → this is `client_id`.
4. Copy the **Directory (tenant) ID** → this is `tenant_id`.
5. Open **Certificates & secrets → New client secret**, set an expiry, and copy the secret **Value** immediately (it is shown only once) → this is `client_secret`.

#### 2. Find the ADME instance URL and data partition

1. In the Azure portal, open your ADME instance.
2. The **Overview** blade shows the **URI** field — that's `base_url` (e.g. `https://admetest.energy.azure.com`). The legacy alias `instance_url` is still accepted for backwards compatibility.
3. The same blade lists **Data Partitions** for the instance. Pick one — that's `data_partition_id` (e.g. `opendes`).

#### 3. Grant the service principal the `users.datalake.viewers` role

The Entitlements service controls which records the service principal can read. Without the right group membership, every Search call will return `403`.

The simplest path is via the Azure portal:

1. In the ADME instance, open **Data Partitions → \<your partition\> → Entitlements**.
2. Find the group `users.datalake.viewers@<data-partition-id>.dataservices.energy`.
3. Add the service principal (by client ID or app display name) as a member.

You can also do this via the Entitlements API directly. See the [ADME entitlements docs](https://learn.microsoft.com/en-us/azure/energy-data-services/how-to-manage-users) for the full reference.

#### 4. Verify the credentials work (optional)

A quick smoke test with `curl`:

```bash
TOKEN=$(curl -s -X POST \
  "https://login.microsoftonline.com/$TENANT_ID/oauth2/v2.0/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode "client_secret=$CLIENT_SECRET" \
  --data-urlencode "scope=$CLIENT_ID/.default" \
  | jq -r .access_token)

curl -s -X POST \
  "$INSTANCE_URL/api/search/v2/query_with_cursor" \
  -H "Authorization: Bearer $TOKEN" \
  -H "data-partition-id: $DATA_PARTITION_ID" \
  -H "Content-Type: application/json" \
  -d '{"kind":"osdu:wks:master-data--Wellbore:*","query":"*","limit":1}'
```

A `200 OK` with a `results` field confirms auth, partition routing, and entitlements are all wired correctly.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide the required connection parameters. The required set depends on the chosen `auth_mode`:
   - **`service_principal` (default):** `base_url`, `data_partition_id`, `tenant_id`, `client_id`, `client_secret`. Optionally `adme_api_client_id`, `page_size`.
   - **`managed_identity`:** `base_url`, `data_partition_id`, `auth_mode=managed_identity`. Optionally `managed_identity_client_id` (user-assigned MI), `adme_api_client_id`, `page_size`.
   - **`federated_identity`:** `base_url`, `data_partition_id`, `auth_mode=federated_identity`, `tenant_id`, `client_id`, plus one of `federated_token_file` or `federated_token`. Optionally `adme_api_client_id`, `page_size`.
   - **`static_token`:** `base_url`, `data_partition_id`, `auth_mode=static_token`, `access_token`. Optionally `page_size`. CI/testing only.
4. **Set `externalOptionsAllowList` to `window_days,lookback_minutes,kind_query_wellbore,kind_query_reservoir,kind_query_rock_and_fluid`** so the per-table options below can be passed through.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The ADME connector exposes **3 tables** in v1, all of which are OSDU master-data record kinds:

| Connector table | OSDU kind (canonical) | OSDU kind (search wildcard) |
|------|------|------|
| `Wellbore` | `osdu:wks:master-data--Wellbore:1.0.0` | `osdu:wks:master-data--Wellbore:*` |
| `Reservoir` | `osdu:wks:master-data--Reservoir:1.2.0` | `osdu:wks:master-data--Reservoir:*` |
| `Rock_and_Fluid` | `osdu:wks:master-data--Sample:2.1.0` | `osdu:wks:master-data--Sample:*` |

Table names are case-sensitive — use exactly `Wellbore`, `Reservoir`, `Rock_and_Fluid`.

> **Note on `Rock_and_Fluid`:** OSDU does not define a kind named `master-data--RockAndFluid`. The OSDU Rock & Fluid Sample domain is served by the RAFS DDMS, where the primary master-data entity is `master-data--Sample` — the physical rock or fluid sample (core plug, sidewall core, downhole fluid sample, cutting, etc.) from which petrophysical and fluid properties are measured. The connector therefore maps `Rock_and_Fluid` to `osdu:wks:master-data--Sample`. See `adme_api_doc.md` for the full mapping rationale.

The connector queries with the wildcard kind form (`...:*`) so minor schema-version updates (e.g. `1.0.0 → 1.0.1`) are picked up without code changes.

### Ingestion modes & primary keys

| Table | Ingestion type | Primary key | Cursor field |
|-------|----------------|-------------|--------------|
| `Wellbore` | `cdc` | `id` | `modifyTime` |
| `Reservoir` | `cdc` | `id` | `modifyTime` |
| `Rock_and_Fluid` | `cdc` | `id` | `modifyTime` |

All three tables run as upsert-style CDC keyed on the OSDU record `id` (a string of the form `<partition>:<entity-type>:<natural-key>`). The cursor is the top-level OSDU envelope field `modifyTime` (ISO-8601 string, e.g. `2024-11-01T08:14:32.000Z`); for never-modified records the connector falls back to `createTime`.

### Delete handling

OSDU deletes are removed from the Search index but are **not surfaced** through any feed the Search Service exposes. The connector therefore uses ingestion type `cdc` (upserts only), not `cdc_with_deletes`. If a record is deleted in OSDU, it stops being returned by the Search Service — but no tombstone is emitted to the destination table. Use a periodic full refresh if you need to reconcile deletes downstream.

### Partitioned reads

All three tables implement `SupportsPartitionedStream`. Each ingestion run splits the `modifyTime` range into `window_days`-sized windows and Spark executors fetch the windows in parallel against the OSDU Search Service. The window size is tunable per table — see the next section.

### Schema highlights

Every record is projected into a flat row with three layers of fields:

1. **Typed envelope columns** common to all three tables: `id`, `kind`, `version` (long, microsecond-precision Unix-epoch surrogate), `createTime`, `createUser`, `modifyTime`, `modifyUser`, `acl_owners`, `acl_viewers`, `legal_legaltags`, `legal_status`, `legal_otherRelevantDataCountries`. ACL and legal arrays are typed `array<string>` so they are queryable without a JSON parse.
2. **Typed kind-specific columns** for the most useful `data.*` scalars:
   - `Wellbore` — `FacilityID`, `FacilityName`, `FacilityTypeID`, `WellID`, `KickOffWellboreID`, `StatusSummary`, `TargetFormation`.
   - `Reservoir` — `ReservoirID`, `ReservoirName`, `ReservoirType`, `ReservoirDescription`, `FieldID`, `BasinID`, `FormationID`, `FluidTypeID`, plus typed `double` columns for depths, thicknesses, porosity, water saturation, permeability, pressure, temperature.
   - `Rock_and_Fluid` — `SampleAcquisitionJobID`, `SampleAcquisitionTypeID`, `SampleAcquisitionContainerID`, `AcquisitionStartDate`, `AcquisitionEndDate`, `CollectionServiceCompanyID`, `HandlingServiceCompanyID`, `WellboreID`, `ToolKind`, `RunNumber`, `TopDepth`, `BaseDepth`, `FormationPressure`, `FormationTemperature` (lifted from the nested `data.SampleAcquisition.SampleAcquisitionDetail` / `FormationCondition` objects).
3. **JSON string columns** for nested complex objects whose shape is too variable to commit to a fixed `StructType` across OSDU versions — for example `SpatialLocation_json`, `VerticalMeasurements_json`, `FacilityNameAliases_json`, `SampleAcquisition_json`, `GeoContexts_json`, `ExtensionProperties_json`, `tags_json`, `meta_json`. Each `*_json` column contains a JSON-encoded string of the corresponding sub-object — parse with `from_json` downstream when you need the structured values.

There is also an **escape-hatch column `data_json`** on every table containing the entire original `data` envelope as a JSON string. Use it when you need a field the connector hasn't lifted to a typed column. The full per-table column lists are defined in `adme_schemas.py`.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | One of `Wellbore`, `Reservoir`, `Rock_and_Fluid` (case-sensitive). |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default). |
| `destination_schema` | No | Target schema (defaults to pipeline's default). |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. All three tables use CDC ingestion, so both are applicable. |
| `primary_keys` | No | List of columns to override the connector's default primary key (`["id"]`). |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking — recommended value is `version` (monotonic int64) or `modifyTime`. |

### Source-specific `table_configuration` options

These options must appear in the connection's `externalOptionsAllowList` (see Setup) before they can be passed at the table level. They apply to all three tables:

| Option | Required | Default | Description |
|---|---|---|---|
| `window_days` | No | `1` | Size, in days, of each `modifyTime` partition window. The (start, end] range is split into windows of this size and executors fetch them in parallel. Larger windows mean fewer partitions and fewer cursor pages per partition; smaller windows give finer parallelism. Minimum `1`. |
| `lookback_minutes` | No | `5` | Lookback applied to the lower bound of the watermark on incremental runs to catch records whose `modifyTime` was set just after the previous run sealed its watermark (clock skew / indexer lag). Set to `0` to disable. Does not apply to the very first run, which always starts at the epoch. |
| `kind_query_wellbore` | No | `osdu:wks:master-data--Wellbore:*` | Override the OSDU kind pattern for the Wellbore table. Useful when an instance exposes wellbore records under a non-standard kind. |
| `kind_query_reservoir` | No | `osdu:wks:master-data--Reservoir:*` | Override the OSDU kind pattern for the Reservoir table. |
| `kind_query_rock_and_fluid` | No | `osdu:wks:master-data--Sample:*` | Override the OSDU kind pattern for the Rock_and_Fluid table. Use `osdu:wks:work-product-component--RockSampleAnalysis:*` for instances that expose sample data under the work-product family. Confirm via the Schema Service before overriding — envelope-typed columns still apply but `data.*` flattening leaves Sample-specific scalars NULL on non-Sample kinds. |

### First-run behaviour

On the first run there is no committed watermark. To avoid generating tens of thousands of empty daily windows from `1970-01-01` forward, the connector emits a single open-ended partition (`modifyTime:[* TO <init-time>]`) which the OSDU Search index handles efficiently. From the second run onward, the connector splits each `(previous_watermark, init_time]` range into `window_days`-sized partitions normally.

## Data Type Mapping

| OSDU field | Example | Connector Spark type | Notes |
|---|---|---|---|
| `string` | `id`, `kind`, `FacilityName`, `ReservoirName` | `StringType` | Default for most text fields. |
| `integer (int64)` | `version` | `LongType` | OSDU `version` is a microsecond-precision Unix epoch surrogate; monotonically increases on every write. |
| `number (float/double)` | `PorosityAverage`, `DepthTopMD`, `FormationPressure` | `DoubleType` | Units are carried in the record's `meta` array (Frame of Reference). The connector preserves raw values as-is. |
| `string (ISO-8601)` | `createTime`, `modifyTime`, `AcquisitionStartDate` | `StringType` | Kept as ISO-8601 strings; cast with `to_timestamp` downstream if needed. |
| `array of strings` | `acl.owners`, `acl.viewers`, `legal.legaltags` | `ArrayType(StringType)` | Lifted from the envelope to typed array columns. |
| `array of objects` | `FacilityNameAliases`, `VerticalMeasurements`, `FacilityOperators` | `StringType` (JSON) | Stored as JSON strings in a `*_json` column to tolerate OSDU version drift. Parse with `from_json` when needed. |
| `nested object` | `SpatialLocation`, `SampleAcquisition`, `InitialCompletion` | `StringType` (JSON) | Same JSON-string strategy. Key scalar fields inside `SampleAcquisition.SampleAcquisitionDetail` and `FormationCondition` are also lifted to typed columns for convenience. |
| `reference string` (trailing colon) | `WellID = "opendes:master-data--Well:123:"` | `StringType` | OSDU IDs ending in `:` denote a reference to another OSDU record at "latest version". Strip the trailing colon and call `GET /api/storage/v2/records/{id}` to resolve. |

### Special field notes

- **`id` format** is always `<data-partition-id>:<entity-type>:<natural-key>`, e.g. `opendes:master-data--Wellbore:WELL-001-MAIN`. It is the connector's primary key.
- **`version` (int64)** is a microsecond-precision Unix timestamp. The tuple `(id, version)` uniquely identifies an immutable point-in-time snapshot of a record.
- **`SpatialLocation_json`** holds GeoJSON for the wellbore surface location: `{"Wgs84Coordinates": {"type": "Point", "coordinates": [lon, lat]}}`. Note GeoJSON ordering — longitude first, latitude second.
- **`data_json`** is the full original `data` payload as a JSON string. Use it when you need a field the connector hasn't lifted to a typed column — preferred over modifying the connector for one-off downstream use cases.

## How to Run

### Step 1: Reference the connector in your workspace

Use the **Lakeflow Community Connector** UI to copy or reference the `adme` connector source in your workspace.

### Step 2: Configure your pipeline

In your `ingest.py` (or equivalent), point at the Unity Catalog connection and list the tables to ingest. A minimal three-table ingest:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
register(spark, "adme")

pipeline_spec = {
    "connection_name": "my_adme_connection",
    "objects": [
        {"table": {"source_table": "Wellbore"}},
        {"table": {"source_table": "Reservoir"}},
        {"table": {"source_table": "Rock_and_Fluid"}},
    ],
}

ingest(spark, pipeline_spec)
```

To override the per-table partition window or lookback, use `table_configuration`:

```python
pipeline_spec = {
    "connection_name": "my_adme_connection",
    "objects": [
        {
            "table": {
                "source_table": "Wellbore",
                "table_configuration": {
                    "window_days": "7",
                    "lookback_minutes": "10",
                    "scd_type": "SCD_TYPE_2",
                    "sequence_by": "version",
                },
            }
        },
        {"table": {"source_table": "Reservoir"}},
        {"table": {"source_table": "Rock_and_Fluid"}},
    ],
}
```

### Step 3: Run and schedule the pipeline

The first run does a full backfill across all selected tables (a single open-ended partition per table). Subsequent runs read only records modified since the previous watermark, split into `window_days`-sized partitions and processed in parallel.

#### Best Practices

- **Start small** — sync `Wellbore` first to validate auth, entitlements, and partition routing before fanning out to all three tables.
- **Use incremental sync** — leave `cdc` ingestion on (the default). The Search Service handles `modifyTime` range filters efficiently.
- **Tune `window_days` for your corpus size** — the default of `1` day is conservative. For partitions with millions of rarely-updated records, increase it to `7` or `30` to reduce per-partition overhead. For high-write partitions where each daily window already returns many pages, leave it at `1`.
- **Avoid concurrent pipelines on the same partition** — the OSDU Search Service has a default cap of 500 concurrent cursors per data partition. Each table opens one cursor per partition window, so a heavily fanned-out pipeline can come close to that limit on full backfill.
- **Prefer concrete kind queries over open wildcards** — the connector already does this (`osdu:wks:master-data--Wellbore:*` rather than `osdu:wks:*:*`), avoiding the ADME wildcard-query token-bucket rate limit (~12 wildcard queries / minute on the most permissive form).
- **`max_records_per_batch` is intentionally not consumed.** Work is bounded by `window_days` (each `(since, until]` partition is fetched as one unit) and OSDU `page_size` (cursor-pagination depth per partition). The framework's `max_records_per_batch` cap would cut a partition mid-window, which conflicts with the partition-equals-watermark-step contract that `SupportsPartitionedStream` relies on for offset commits. Tune `window_days` down if you need smaller per-batch volume.

#### Troubleshooting

##### Authentication errors (401 / 403)

**Symptoms:** `ADME auth failed: 401 ...` from the token endpoint, or `ADME search failed for kind=... 401/403 ...` from the data plane.

**Causes & fixes:**
- `401` from the **token endpoint** — wrong `tenant_id`, `client_id`, or `client_secret`, or the secret has expired. Re-issue the secret in **App registrations → Certificates & secrets** and update the connection.
- `401` from the **search endpoint** with a valid token — the connector retries once after refreshing the bearer token, then surfaces the error. If it still fails, the bearer token is being rejected by the data plane; verify the app registration's API permissions and that you used the correct app's `client_id` for both `client_id` and the `resource` field.
- `403` from the search endpoint — the service principal has a valid token but is missing the entitlement. Add it to `users.datalake.viewers@<data-partition-id>.dataservices.energy` in the ADME Entitlements service. Wrong `data-partition-id` also produces `403`; double-check the value matches a partition listed in the ADME instance overview.

##### Empty result sets / `404`-like behavior on a kind

**Symptoms:** the table runs, but every partition returns zero rows even though you expect data.

**Causes & fixes:**
- The kind has not been registered in this data partition. Verify with the OSDU Schema Service: `GET /api/schema-service/v1/schema?authority=osdu&source=wks&entityType=master-data--Wellbore`.
- The service principal does not have read access to the records' ACLs. ACLs in OSDU are per-record — even with `users.datalake.viewers`, records owned by a different group are filtered out at the index level. Confirm the records' `acl.viewers` includes a group your service principal is a member of.
- The wildcard kind query matched no records because no version of that kind has been deployed (e.g. an instance with `Reservoir:1.0.0` only when you expected `1.2.0` — the wildcard handles this; an instance with no `Reservoir` records at all does not).

##### `429 Too Many Requests` / cursor expired

**Symptoms:** Slow runs, log messages about retries, or `ADME search failed: 400 ... cursor ... expired`.

**Causes & fixes:**
- **`429`**: ADME is throttling. The connector retries on `429`, `500`, `502`, `503`, and `504` with exponential backoff (starting at 1s, doubling, up to 5 attempts) and honors the `Retry-After` header when present. If you keep hitting `429`, lower `page_size` or reduce parallelism by increasing `window_days`.
- **Cursor expired (`400`)**: the OSDU Search cursor has a 1-minute inactivity timeout (pre-M25 instances). If executor work is slow enough between page fetches that the cursor expires, restart the partition. ADME instances on M25.1+ use stateless `search_after` internally and do not expire — same API signature, no code change. If you see persistent cursor-expiry errors on a recent ADME instance, check that you're not running into the rare aggregate-cursor cap (500 concurrent per partition) by reducing pipeline concurrency.

##### `500 Internal Server Error` from the Search Service

**Cause:** transient ADME backend error.

**Fix:** the connector retries up to 5 times. If it still fails, re-run the pipeline; if it persists, file an Azure support ticket. Lucene syntax errors return `400`, not `500`, so a `500` is server-side.

##### Schema validation errors after an OSDU upgrade

**Cause:** the kind's schema has been registered with a new version (e.g. `Wellbore:1.5.1`). The connector's wildcard query matches the new version too, but new fields on `data.*` will not appear as typed columns — they're preserved inside `data_json`.

**Fix:** read the field from `data_json` with `from_json`. If the new field is widely useful, file an issue or PR against this connector to lift it to a typed column.

## Limitations

- **Read-only** — the connector does not write back to ADME or Storage Service.
- **Three tables only in v1** — `Wellbore`, `Reservoir`, `Rock_and_Fluid`. Other OSDU master-data kinds (Well, Field, Basin, SampleAcquisitionJob, GenericFacility, etc.) are reachable via the same Search Service but not exposed by this connector.
- **No delete sync** — the OSDU Search index drops deleted records but the Search Service does not provide a deletion feed. Tombstones are not emitted; use a periodic full refresh to reconcile.
- **No DDMS endpoints** — the connector reads only from `/api/search/v2/`. The DDMS-specific endpoints (Wellbore DDMS, Reservoir DDMS, RAFS DDMS) are out of scope; their RESQML/well-log payloads are not surfaced.
- **No interactive auth flows** — device-code and other user-facing OAuth grants are not supported. The four supported modes are `service_principal` (client-credentials), `managed_identity` (Azure system/user MI), `federated_identity` (OIDC / Workload Identity), and `static_token` (pre-issued bearer; CI/testing only).
- **Soft-bounded `init_time` snapshot** — `latest_offset` returns the connector's process-init time rather than probing the source for the actual maximum `modifyTime`. This guarantees `Trigger.AvailableNow` termination but means records modified after init time will not appear until the next trigger. This is the intended tradeoff.
- **No spatial / domain-specific filters as first-class options** — the OSDU Search API supports `spatialFilter`, `returnedFields`, and `sort` but those are not surfaced as connector options. To pre-filter by geography or domain attribute, post-process the destination tables.
- **One data partition per connection** — to ingest from multiple OSDU partitions, create one Unity Catalog connection per partition.

## References

- [Azure Data Manager for Energy — overview](https://learn.microsoft.com/en-us/azure/energy-data-services/overview-microsoft-energy-data-services)
- [Generate an authentication token (ADME)](https://learn.microsoft.com/en-us/azure/energy-data-services/how-to-generate-auth-token)
- [OSDU services on ADME](https://learn.microsoft.com/en-us/azure/energy-data-services/osdu-services-on-adme)
- [OSDU Search Service API reference](https://osdu.pages.opengroup.org/platform/system/search-service/api/)
- [OSDU Storage Service API reference](https://osdu.pages.opengroup.org/platform/system/storage/api/)
- [Manage users and entitlements in ADME](https://learn.microsoft.com/en-us/azure/energy-data-services/how-to-manage-users)
- [ADME Rock & Fluid Sample DDMS tutorial](https://learn.microsoft.com/en-us/azure/energy-data-services/tutorial-rock-and-fluid-samples-ddms)
- [Lakeflow Community Connectors documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Azure Data Manager for Energy (ADME) — OSDU Search Service (`/api/search/v2/query_with_cursor`)
- **Supported Objects**: 3 tables (`Wellbore`, `Reservoir`, `Rock_and_Fluid`)
- **Authentication**: Azure AD — four modes selected via `auth_mode` (`service_principal` / `managed_identity` / `federated_identity` / `static_token`). All call ADME with a Bearer token + `data-partition-id` header.
- **Supported Ingestion Types**: `cdc` (upserts; no deletes)
- **Partitioned Reads**: yes — `SupportsPartitionedStream` over `modifyTime` windows
