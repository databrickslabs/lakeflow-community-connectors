# Lakeflow OneStream Community Connector

This documentation provides setup instructions and reference information for the OneStream source connector.

The connector ingests OneStream (OneStream Software CPM: financial consolidation, planning, and reporting) application data into Databricks. It covers stored cube data (the financial numbers) and the dimension metadata needed to interpret it. It reads through the OneStream REST API, backfills each table on its first run, and then loads `cube_data` incrementally, re-reading only the data units that changed.

## Prerequisites

- A OneStream **cloud** tenant (`https://<tenant>.onestreamcloud.com`). The connector was validated on platform 9.3.3. On other versions, the metadata tables adapt to columns that don't exist, and a missing key column fails with a clear error. Self-hosted OneStream is not supported (see *Limitations*).
- A **dedicated non-interactive service user** for the connector instead of a person's account. The user needs:
  - the **Administrator** role. OneStream requires that role for the SQL data provider endpoint this connector uses. The connector only ever sends read-only `SELECT` statements that it generates itself (see *Security*).
  - the `AccessAsNonInteractiveUser` IAM role, which is required to create and use personal access tokens.
- A OneStream **personal access token (PAT)** for that user. OneStream does not offer PATs to government-cloud customers.
- The application must use OneStream's **standard monthly time profile**. The connector checks this and fails with a clear error otherwise (see *Limitations*).
- **Network access**: the Databricks compute that runs the pipeline must be able to reach `https://<tenant>.onestreamcloud.com`. The connector only connects over HTTPS.
- A Databricks workspace with Unity Catalog, where you can create connections and Lakeflow pipelines.

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connection options:

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `base_url` | string | yes | Tenant root URL. Must start with `https://`. A trailing `/`, `/OneStreamWeb`, or `/OneStreamApi` is removed automatically | `https://<tenant>.onestreamcloud.com` |
| `application_name` | string | yes | OneStream application name exactly as used by the REST API | `<Application>` |
| `personal_access_token` | string (secret) | yes | OneStream PAT of the connector's service user | `<PAT>` |
| `api_version` | string | no | DataProvider API version. Default `5.2.0` | `5.2.0` |
| `request_timeout_seconds` | string | no | Per-request HTTP timeout in seconds. Default `900`, because cold reads of large partitions can take several minutes | `900` |
| `page_size` | string | no | Rows per SQL page. Default `50000`. Values above `200000` are capped at `200000`. Each table can override it (see *Table Configurations*) | `50000` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of the table-specific options that the connection passes through to the connector. This connector has table-specific options, so this parameter is required | `years,scenarios,entities,include_descendants,cube,refresh_mode,lookback_seconds,hold_in_flight_data_units,in_flight_timeout_seconds,max_data_units_per_batch,max_partitions,page_size` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`years,scenarios,entities,include_descendants,cube,refresh_mode,lookback_seconds,hold_in_flight_data_units,in_flight_timeout_seconds,max_data_units_per_batch,max_partitions,page_size`

> **Note**: Table-specific options such as `years` or `scenarios` are **not** connection parameters. You set them on each table in the pipeline spec (see *Table Configurations*). The connection's `externalOptionsAllowList` must include them so that they are passed through. The `community-connector` CLI sets the allowlist automatically from `connector_spec.yaml`.

### Obtaining the Required Parameters

1. **Base URL**: the root of the address you use to open OneStream, e.g. `https://<tenant>.onestreamcloud.com`. If you paste a URL that ends in `/OneStreamWeb` or `/OneStreamApi`, the connector strips that suffix.
2. **Application name**: the application you select at OneStream logon. It is also visible under *System → Applications*. Use the application name, not a display label.
3. **Service user**: create (or ask your OneStream administrator to create) a dedicated user for the connector with the **Administrator** role and the `AccessAsNonInteractiveUser` IAM role.
4. **PAT**: sign in to OneStream as that user, open the **Identity & Access Management** portal, go to **Personal Access Tokens → Create**, and copy the token. It is shown only once. PATs expire. An expired PAT is revoked automatically and cannot be re-enabled, so create a new one and update the connection before it expires.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `years,scenarios,entities,include_descendants,cube,refresh_mode,lookback_seconds,hold_in_flight_data_units,in_flight_timeout_seconds,max_data_units_per_batch,max_partitions,page_size` (required for this connector to receive table-specific options).

The connection can also be created using the standard Unity Catalog API, or with the `community-connector` CLI (see `tools/community_connector/README.md`). The CLI validates the options against `connector_spec.yaml` and sets `externalOptionsAllowList` for you:

```bash
community-connector create_connection onestream <CONNECTION_NAME> \
  -o '{"base_url":"https://<tenant>.onestreamcloud.com","application_name":"<Application>","personal_access_token":"<PAT>"}'
```

To rotate the PAT, run `community-connector update_connection onestream <CONNECTION_NAME> -o '<json>'` with the same options and the new token.

## Supported Objects

The connector exposes a static list of nine tables:

| Table | Description | OneStream source | Ingestion type | Primary key | Cursor |
|---|---|---|---|---|---|
| `cube_data` | Stored cube cell data (the financial numbers), one row per intersection per month | `DataRecord{YYYY}` + `DataUnitCacheTimestamp` | `cdc` (or `snapshot` with `refresh_mode=snapshot`) | `CubeId, EntityId, ParentId, ConsId, ScenarioId, TimeId, AccountId, FlowId, OriginId, ICId, UD1Id..UD8Id` | `_du_timestamp_ticks` |
| `dimensions` | Dimension definitions | `Dim` | `snapshot` | `DimTypeId, DimId` | n/a |
| `members` | Dimension members (entities, scenarios, accounts, flows, UD1–UD8) | `Member` | `snapshot` | `DimTypeId, MemberId` | n/a |
| `member_relationships` | Parent-child hierarchy edges, with aggregation weight | `Relationship` | `snapshot` | `DimTypeId, ParentId, ChildId` | n/a |
| `member_properties` | Member property values, which can vary by cube type, scenario type, and time | `MemberProperty` | `snapshot` | `DimTypeId, MemberId, PropertyId, VaryByCubeTypeId, VaryByScenarioTypeId, VaryByTimeId` | n/a |
| `cubes` | Cube definitions | `Cube` | `snapshot` | `CubeId` | n/a |
| `cube_dimensions` | The dimension assigned to each cube and dimension type, by scenario type | `CubeDim` | `snapshot` | `CubeId, DimTypeId, ScenarioTypeId` | n/a |
| `fx_rates` | Exchange rates by rate type, period, and currency pair | `FxRate` | `snapshot` | `FxRateTypeID, TimeId, SourceCurrencyId, DestCurrencyId` | n/a |
| `fx_rate_types` | Exchange rate types | `FxRateType` | `snapshot` | `UniqueID` | n/a |

The metadata tables (everything except `cube_data`) are re-read in full on every run, so rows deleted in OneStream also disappear from them on the next run. They take no table-specific options except `page_size`.

No table uses delete synchronization (`cdc_with_deletes`). For deletes in `cube_data`, see *Deletes* below.

### `cube_data` shape

There is one row per stored intersection **per month** (long format). Columns:

- the data unit: `CubeId, EntityId, ParentId, ConsId, ScenarioId, YearId`
- time: `TimeId` (OneStream time key, e.g. `2026003000` = 2026M1) and `Period` (1–12)
- the intersection: `AccountId, FlowId, OriginId, ICId, UD1Id..UD8Id`
- values: `Amount` (`decimal(28,9)`) and `CellStatus` (OneStream cell status flags)
- change tracking: `_du_timestamp_ticks` and `_du_modified_at` (UTC)

All 12 months of every stored row are emitted, including zeros, so that a cleared cell overwrites its previous value. Filter `Amount <> 0` downstream if needed.

Dimension values are **member ids**. Join them to `members` (on `MemberId`, with `DimTypeId` 0 = Entity, 2 = Scenario, 5 = Account, 6 = Flow, 9–16 = UD1–UD8) and walk hierarchies with `member_relationships`. `-999` means *None*.

### Schema highlights

- `Amount` in `cube_data` is an exact `decimal(28,9)`. The connector parses API numbers losslessly, so values are never rounded through floating point.
- `XmlData` columns in `dimensions`, `cubes`, and `fx_rate_types` hold OneStream definitions as XML text.
- The metadata table schemas are fixed by the connector. If a OneStream version lacks one of those columns, the column is left out. Columns that a newer version adds are not ingested.

### How incremental loading works

OneStream records a last-modified tick per *data unit* (Cube × Entity × Parent × Cons × Scenario × Year). On each run the connector:

1. finds the data units changed since the last checkpoint, within the configured scope,
2. re-reads those data units with indexed seeks (`PartitionId` + data-unit key) in parallel Spark partitions. When very many data units of one entity changed within a cube, scenario, and year, it re-reads all of that entity's data for that cube, scenario, and year in one pass instead, and
3. upserts them by primary key.

Each run is capped at its start time, so `Trigger.AvailableNow` always terminates. The first run of a table is the backfill. Every later run is incremental. You don't need to switch modes.

**Checkpoints are kept per year × scenario × cube.** This makes it safe to change the scope of an existing table:

- **Adding years, scenarios, or cubes**: the new slices are backfilled from the beginning, and existing slices continue from their checkpoint.
- **Adding entities**: added explicitly or picked up through `include_descendants` after a hierarchy change. The history of just those entities is backfilled, and nothing else is re-read.
- **Removing years, scenarios, or cubes**: those rows stop updating but stay in the table. Their checkpoints are kept, so adding them back later resumes from where they stopped.
- **Removing entities**: those rows stop updating but stay in the table. If you add an entity back later, its history is backfilled again, like any newly added entity.

Delete rows that left the scope downstream if needed.

**Confirmation re-read (`lookback_seconds`, default 900).** A data unit changed shortly before a run is read by that run. It is then read once more by the first run that starts more than `lookback_seconds` after the change. That catches changes that became visible late (commit-order races, clock differences between OneStream app servers) without re-reading quiet data on every run. Set it to `0` to disable.

**In-flight hold-back (`hold_in_flight_data_units`, default off).** When enabled, data units flagged as being updated (`IsInCellUpdateLoop`) are skipped, and their slice's checkpoint waits below them until the flag clears. A flag still set after `in_flight_timeout_seconds` (default 3600) is treated as stale, and the unit is read anyway. This is off by default because OneStream does not document the flag's meaning. In testing, a data unit's stamp was written when its calculation finished, so calculations still running are normally restamped and re-read anyway.

**Deletes.** OneStream typically keeps cleared cells as zero-valued rows with a NoData status, so they arrive as updates. Rows that OneStream physically removes are **not** propagated. Do **not** switch an existing incremental table to `refresh_mode=snapshot` to reconcile them. Snapshot ingestion replaces the table with the snapshot's contents, so every row outside the snapshot's scope would be deleted, and changing the ingestion type of an existing table forces a full refresh. Use `refresh_mode=snapshot` only on its own, separate destination table.

### Table layout and clustering

Load `cube_data` into **one** bronze table across years, cubes, and business units, and don't split it per year or per operating company. Cube data compresses to a few bytes per row in Delta, and billions of rows are routine. Splitting would multiply pipelines and OneStream load, and it would break when the entity hierarchy is reorganized.

- **Clustering**: managed ingestion pipelines currently do **not** apply clustering to community-connector tables. `clustering_columns` in `table_configuration` is dropped by the pipelines API, and `ALTER TABLE ... CLUSTER BY` is not allowed on streaming tables. Cluster the **silver** table instead, e.g. `CLUSTER BY (EntityId, ScenarioId, TimeId)`. `TimeId` encodes year and month, so it prunes more finely than `YearId`. (Legacy workspace-mode pipelines accept `cluster_by` in `table_configuration`.)
- **Business units**: derive operating company or segment in silver by joining `EntityId` to `members` / `member_relationships`. Restrict access with Unity Catalog row filters or views rather than separate tables.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (one of the tables in *Supported Objects*) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

In a full managed-ingestion pipeline spec (one with an `ingestion_definition` block, as in *How to Run*), set `destination_catalog` and `destination_schema` on every table, and use `source_schema: default`.

### Common `table_configuration` options

These are set inside the `table_configuration` map:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. Only legacy workspace-mode pipelines apply it today. Managed ingestion pipelines drop clustering settings for community-connector tables, so cluster in silver instead (see *Table layout and clustering*) |

### Source-specific options

In a managed ingestion pipeline (the CLI default), put these options under `connector_options.community_connector_options.options` on the table, not in `table_configuration`. In a legacy workspace-mode pipeline (`ingest.py`), put them in `table_configuration`. Write every value as a string, e.g. `"true"` or `"900"`.

All options apply to `cube_data`. The metadata tables accept only `page_size`.

| Option | Required | Default | Description |
|---|---|---|---|
| `years` | **yes** | | Years to load, as a comma-separated list and/or ranges, e.g. `2025,2026` or `2024-2026`. Each year must be between 1900 and 2100. OneStream stores each year in its own `DataRecord{YYYY}` table, and they all land in the one `cube_data` table |
| `scenarios` | no | all | Scenario member names, comma-separated (e.g. `Actual,Budget`). Names are matched case-insensitively |
| `entities` | no | all | Entity member names, comma-separated. Names are matched case-insensitively |
| `include_descendants` | no | `false` | When `entities` is set, also load all descendants of those entities in the entity hierarchy (`true`/`false`). The hierarchy is re-read on every run, so entities added under a listed parent are picked up |
| `cube` | no | all cubes | A single cube name, matched exactly (case-sensitive). Each cube keeps its own checkpoints, so if you remove this option later to load all cubes, only the new cubes are backfilled |
| `refresh_mode` | no | `incremental` | `incremental` or `snapshot`. `snapshot` reloads the whole scope on every run. Only use it on a dedicated destination table, never to reconcile an incremental table (see *Deletes*) |
| `lookback_seconds` | no | `900` | Confirmation re-read window for recent changes. `0` disables it. Incremental mode only |
| `hold_in_flight_data_units` | no | `false` | Skip data units flagged as mid-update until the flag clears (`true`/`false`). Fails if this OneStream version has no in-flight flag. Incremental mode only |
| `in_flight_timeout_seconds` | no | `3600` | After this long, a still-set in-flight flag is treated as stale. Incremental mode only |
| `max_data_units_per_batch` | no | `5000` | Admission control: approximate number of data units per micro-batch. Incremental mode only. This replaces the framework's usual `max_records_per_batch`: OneStream doesn't expose a data unit's row count without reading it, so batches are bounded by data units (each is read and upserted whole) |
| `max_partitions` | no | `4` | Parallel read partitions. This bounds the concurrent load on OneStream |
| `page_size` | no | connection value | Rows per SQL page for this table (capped at `200000`) |

Boolean options accept `true`/`false` (also `1`/`0` and `yes`/`no`). Any other value fails instead of being read as `false`. `lookback_seconds` and `in_flight_timeout_seconds` must be `0` or more.

**Scope your loads.** A large OneStream application can hold hundreds of millions of stored cube rows, and an unscoped backfill through the REST API takes many hours. Start with the scenarios and entities you actually report on. You can widen the scope later on the same table (see *How incremental loading works*), e.g. most recent years first, then older history.

## Data Type Mapping

| OneStream (SQL Server) | Databricks | Notes |
|---|---|---|
| `int`, `smallint`, `tinyint`, `bigint` | `BIGINT` | All integer ids are widened to `BIGINT`, so joins across tables line up |
| `decimal(p,s)` | `DECIMAL(p,s)` | OneStream amounts are `decimal(28,9)`. They are parsed without float rounding |
| `bit` | `BOOLEAN` | |
| `uniqueidentifier` | `STRING` | GUID text |
| `nvarchar` | `STRING` | Includes the `XmlData` definition columns |
| .NET ticks (`_du_timestamp_ticks`) | `BIGINT` | Data-unit change stamp, also exposed as `_du_modified_at TIMESTAMP` (UTC) |

The `cube_data` columns `TimeId` and `Period` are `BIGINT`, derived from the year table and the month.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the OneStream connector code. With the `community-connector` CLI, `create_pipeline` builds the connector package from the source tree and uploads it for the pipeline, so you don't need to copy any code by hand.

### Step 2: Configure Your Pipeline

Write a pipeline spec. Put the `cube_data` scope options under `connector_options.community_connector_options.options`. Metadata tables need no options.

```yaml
name: <PIPELINE_NAME>
catalog: <CATALOG>
schema: <SCHEMA>
serverless: true
channel: PREVIEW
ingestion_definition:
  connection_name: <CONNECTION_NAME>
  objects:
    - table:
        source_schema: default
        source_table: cube_data
        destination_catalog: <CATALOG>
        destination_schema: <SCHEMA>
        destination_table: onestream_cube_data
        connector_options:
          community_connector_options:
            options:
              years: "2025,2026"
              scenarios: "Actual,Budget"
              # entities: "<Entity>"
              # include_descendants: "true"
    - table:
        source_schema: default
        source_table: members
        destination_catalog: <CATALOG>
        destination_schema: <SCHEMA>
        destination_table: onestream_members
    - table:
        source_schema: default
        source_table: member_relationships
        destination_catalog: <CATALOG>
        destination_schema: <SCHEMA>
        destination_table: onestream_member_relationships
```

Create the pipeline:

```bash
community-connector create_pipeline onestream <PIPELINE_NAME> -ps <spec.yaml>
```

To widen the scope later (for example, to add older years), edit the options and run `community-connector update_pipeline <PIPELINE_NAME> -ps <spec.yaml>`. Only the newly added years, scenarios, cubes, or entities are backfilled.

### Step 3: Run and Schedule the Pipeline

Run the pipeline once to backfill (`community-connector run_pipeline <PIPELINE_NAME>`), then schedule it, e.g. hourly or around the close calendar. Each run reads only the data units changed since the previous run. A full refresh (`--full-refresh`) resets the checkpoints and backfills the whole scope again.

#### Best Practices

- **Start small**: begin with `cube_data` scoped to the recent years and the scenarios and entities you report on, plus `members` and `member_relationships` to interpret the ids. Widen the scope once the data looks right.
- **Use incremental sync**: keep `cube_data` in the default incremental mode. Each run re-reads only changed data units.
- **Set appropriate schedules**: run backfills outside business hours. OneStream app servers are shared with interactive finance users.
- **Keep `max_partitions` small** (the default is 4). Reads of cold partitions can take minutes, and that is normal.
- **Expect a per-run scan**: a run that finds changes scans each affected year table once to locate entity partitions. On large year tables this takes a minute or more.
- **Rate limits**: OneStream publishes no rate limits. The connector retries timeouts, connection errors, and HTTP 408, 429, 502, 503, and 504 with exponential backoff (up to 5 attempts). Other errors fail immediately: SQL errors are deterministic, and each failed call is also written to the application's Error Log.

#### Troubleshooting

**Common Issues:**

| Symptom | Cause / fix |
|---|---|
| `OneStream rejected the personal access token (HTTP 401)` | The PAT is invalid, expired, or revoked, or belongs to another tenant. Create a new PAT and update the connection |
| `OneStream API error HTTP 400` about an unsupported API version | Leave `api_version` at `5.2.0` |
| Other `OneStream API error HTTP <status>: <detail>` | The detail is OneStream's own message. Check that `application_name` is correct and that the PAT user has the Administrator role |
| `base_url must start with https://` | Use the `https://<tenant>.onestreamcloud.com` form |
| `cube_data requires the table option 'years'` | Set `years` on the `cube_data` table |
| `unknown member name(s)` | Check the spelling in `scenarios`/`entities` against the `members` table |
| `member name(s) ... exist in more than one dimension of this type` | The name is used in two dimensions of the same type. Rename one in OneStream, or narrow the scope |
| `Unknown cube '<name>'` | `cube` must match a cube name exactly, including case. The error lists the available cubes |
| `not standard monthly periods` | The application uses a non-standard time profile, which `cube_data` doesn't support yet |
| `OneStream returned only N rows for a M-row page` | The OneStream server caps rows per request. Set `page_size` to `N` or less |
| `OneStream request failed after 5 attempts` | OneStream was unreachable or kept timing out. Check network access, and raise `request_timeout_seconds` or lower `page_size` and `max_partitions` if the tenant is under load |
| A run is slow on first touch | Cold OneStream storage. Lower `page_size` or `max_partitions` if the tenant is under load |
| `hold_in_flight_data_units requires DataUnitCacheTimestamp.IsInCellUpdateLoop` | This OneStream version has no in-flight flag. Leave `hold_in_flight_data_units` off |
| `Table dbo.<name> was not found` or `is missing key column(s)` | This OneStream version doesn't support that metadata table |
| `This cube_data checkpoint was written by an earlier version of the OneStream connector` | Run a full refresh of the `cube_data` table |

## Security

OneStream's SQL endpoint runs any statement with the PAT owner's rights. This connector only sends single `SELECT` statements that it builds itself from validated identifiers and integer literals (`onestream_sql.py`). Member and cube names you configure are resolved to ids in Python and never embedded in SQL. It calls only the `DataProvider/GetAdoDataSetForSqlCommand` endpoint, never the `DataManagement` endpoints that run jobs, and it only connects over HTTPS. Keep the PAT only in the Unity Catalog connection, and create it for a dedicated service user rather than a person's account.

## Limitations

- **OneStream cloud with PAT auth only.** Self-hosted OneStream uses OAuth client credentials against your identity provider, which the connector doesn't support yet. PATs are not available to government-cloud customers.
- **Standard monthly time profile only.** `TimeId` and `Period` are derived from OneStream's standard encoding. The connector checks the application's period keys and fails rather than writing wrong periods.
- **Physical deletes are not propagated** (see *Deletes*).
- **Clocks are assumed to be UTC.** That's true for OneStream cloud. The confirmation re-read window (`lookback_seconds`) absorbs normal clock differences.
- **Stored data only.** `cube_data` reads what OneStream stores in `DataRecord`. Dynamically calculated members (computed on the fly in cube views) aren't stored, so they aren't included.
- **One cube or all cubes.** The `cube` option takes a single cube name. Omit it to load every cube.

## References

- OneStream REST API endpoints: https://documentation.onestream.com/docs/Content/REST%20API/OneStream%20WebAPI%20Endpoints.html
- Personal access tokens: https://documentation.onestream.com/docs/Content/IDM/Manage%20Personal%20Access%20Tokens.html
- Requirements for using personal access tokens: https://documentation.onestream.com/docs/Content/IDM/Requirements%20for%20Using%20Personal%20Access%20Tokens.html
- Connector CLI: `tools/community_connector/README.md`
- Detailed API notes: [onestream_api_doc.md](onestream_api_doc.md)
