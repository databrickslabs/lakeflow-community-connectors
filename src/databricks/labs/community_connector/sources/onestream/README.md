# Lakeflow OneStream Community Connector

Ingests OneStream (OneStream Software CPM / financial consolidation, planning and reporting) application data into Databricks. The connector covers stored cube data (the financial numbers) and the dimension metadata needed to interpret it. It reads through the OneStream REST API, backfills each table, and then loads `cube_data` incrementally, re-reading only the data units that changed.

## Prerequisites

- A OneStream **cloud** tenant (`https://<tenant>.onestreamcloud.com`). The connector was validated on platform 9.3.3. On other versions, the metadata tables adapt to columns that don't exist, and a missing key column fails with a clear error.
- A OneStream **personal access token (PAT)** for a user with the **Administrator** role. OneStream requires that role for the SQL data provider endpoint this connector uses. Create a **dedicated non-interactive service user** for the connector instead of using a person's account. The connector only ever sends read-only `SELECT` statements that it generates itself (see *Security*).
- The PAT user needs the `AccessAsNonInteractiveUser` IAM role, which is required to create and use PATs.
- The application must use OneStream's **standard monthly time profile**. The connector checks this and fails with a clear error otherwise (see *Limitations*).

## Setup

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `base_url` | string | yes | Tenant root URL. A trailing `/OneStreamWeb` is stripped | `https://acme.onestreamcloud.com` |
| `application_name` | string | yes | OneStream application name as used by the REST API | `Finance_Prod` |
| `personal_access_token` | string (secret) | yes | OneStream PAT | |
| `api_version` | string | no | DataProvider API version (default `5.2.0`) | `5.2.0` |
| `request_timeout_seconds` | string | no | Per-request timeout (default `900`) | `900` |
| `page_size` | string | no | Rows per SQL page (default `50000`, max `200000`) | `50000` |

The table-specific options listed under *Table Configurations* must be in the connection's `externalOptionsAllowList`. The CLI adds them automatically from `connector_spec.yaml`.

### Obtaining the Required Parameters

1. **PAT**: sign in to OneStream, open the **Identity & Access Management** portal, go to **Personal Access Tokens → Create**, and copy the token. It is shown only once. An expired PAT is revoked automatically and must be recreated.
2. **Application name**: the application you select at OneStream logon. It is also visible under *System → Applications*.

### Create a Unity Catalog Connection

```bash
community-connector create_connection onestream <CONNECTION_NAME> \
  -o '{"base_url":"https://<tenant>.onestreamcloud.com","application_name":"<Application>","personal_access_token":"<PAT>"}'
```

## Supported Objects

| Table | Source | Ingestion | Primary key | Cursor |
|---|---|---|---|---|
| `cube_data` | `DataRecord{YYYY}` + `DataUnitCacheTimestamp` | `cdc` (or `snapshot` with `refresh_mode=snapshot`) | `CubeId, EntityId, ParentId, ConsId, ScenarioId, TimeId, AccountId, FlowId, OriginId, ICId, UD1Id..UD8Id` | `_du_timestamp_ticks` |
| `members` | `Member` | snapshot | `DimTypeId, MemberId` | |
| `member_relationships` | `Relationship` | snapshot | `DimTypeId, ParentId, ChildId` | |
| `member_properties` | `MemberProperty` | snapshot | `DimTypeId, MemberId, PropertyId, VaryByCubeTypeId, VaryByScenarioTypeId, VaryByTimeId` | |
| `dimensions` | `Dim` | snapshot | `DimTypeId, DimId` | |
| `cubes` | `Cube` | snapshot | `CubeId` | |
| `cube_dimensions` | `CubeDim` | snapshot | `CubeId, DimTypeId, ScenarioTypeId` | |
| `fx_rates` | `FxRate` | snapshot | `FxRateTypeID, TimeId, SourceCurrencyId, DestCurrencyId` | |
| `fx_rate_types` | `FxRateType` | snapshot | `UniqueID` | |

### `cube_data` shape
There is one row per stored intersection **per month** (long format). Columns:
- the data unit: `CubeId, EntityId, ParentId, ConsId, ScenarioId, YearId`
- time: `TimeId` (OneStream time key, e.g. `2026003000` = 2026M1) and `Period` (1–12)
- the intersection: `AccountId, FlowId, OriginId, ICId, UD1Id..UD8Id`
- values: `Amount` (`decimal(28,9)`) and `CellStatus` (OneStream cell status flags)
- change tracking: `_du_timestamp_ticks` and `_du_modified_at` (UTC)

All 12 months of every stored row are emitted, including zeros, so that a cleared cell overwrites its previous value. Filter `Amount <> 0` downstream if needed.

Dimension values are **member ids**. Join them to `members` (on `MemberId`, with `DimTypeId` 0 = Entity, 2 = Scenario, 5 = Account, 6 = Flow, 9–16 = UD1–UD8) and walk hierarchies with `member_relationships`. `-999` means *None*.

### How incremental loading works
OneStream records a last-modified tick per *data unit* (Cube × Entity × Parent × Cons × Scenario × Year). On each run the connector:
1. finds the data units changed since the last checkpoint, within the configured scope,
2. re-reads exactly those data units with indexed seeks (`PartitionId` + data-unit key) in parallel Spark partitions, and
3. upserts them by primary key.

Each run is capped at its start time, so `Trigger.AvailableNow` always terminates. The first run of a table is the backfill. Every later run is incremental. You don't need to switch modes.

**Checkpoints are kept per year × scenario × cube.** This makes it safe to grow the scope of an existing table:
- **Adding years, scenarios, or cubes**: the new slices are backfilled from the beginning, and existing slices continue from their checkpoint.
- **Adding entities**: added explicitly or picked up through `include_descendants` after a hierarchy change. The history of just those entities is backfilled, and nothing else is re-read.
- **Removing years, scenarios, cubes, or entities**: those rows stop updating but stay in the table. Their checkpoints are kept, so adding them back later resumes correctly. Delete the rows downstream if needed.

**Confirmation re-read (`lookback_seconds`, default 900).** A data unit changed shortly before a run is read by that run. It is then read once more by the first run that starts more than `lookback_seconds` after the change. That catches changes that became visible late (commit-order races, clock differences between OneStream app servers) without re-reading quiet data on every run. Set it to `0` to disable.

**In-flight hold-back (`hold_in_flight_data_units`, default off).** When enabled, data units flagged as being updated (`IsInCellUpdateLoop`) are skipped, and their slice's checkpoint waits below them until the flag clears. A flag still set after `in_flight_timeout_seconds` (default 3600) is treated as stale and the unit is read anyway. This is off by default because OneStream does not document the flag's meaning. In testing, a data unit's stamp was written when its calculation finished, so calculations still running are normally restamped and re-read anyway.

**Deletes.** OneStream typically keeps cleared cells as zero-valued rows with a NoData status, so they arrive as updates. Rows that OneStream physically removes are **not** propagated. Do **not** switch an existing incremental table to `refresh_mode=snapshot` to reconcile them. Snapshot ingestion replaces the table with the snapshot's contents, so every row outside the snapshot's scope would be deleted, and changing the ingestion type of an existing table forces a full refresh. Use `refresh_mode=snapshot` only on its own, separate destination table.

### Table layout: one table
Load `cube_data` into **one** bronze table across years, cubes, and business units, and don't split it per year or per operating company. Cube data compresses to a few bytes per row in Delta. Billions of rows are routine. Splitting would multiply pipelines and OneStream load and break when the entity hierarchy is reorganized.
- **Clustering**: managed ingestion pipelines currently do **not** apply clustering to community-connector tables. `clustering_columns` in `table_configuration` is dropped by the pipelines API, and `ALTER TABLE ... CLUSTER BY` is not allowed on streaming tables. Cluster the **silver** table instead, e.g. `CLUSTER BY (EntityId, ScenarioId, TimeId)`. `TimeId` encodes year and month, so it prunes more finely than `YearId`. (Legacy workspace-mode pipelines accept `cluster_by` in `table_configuration`.)
- **Business units**: derive operating company or segment in silver by joining `EntityId` to `members` / `member_relationships`. Restrict access with Unity Catalog row filters or views rather than separate tables.

## Table Configurations

### Source & Destination
| Option | Description |
|---|---|
| `source_table` | One of the table names above |
| `destination_catalog` / `destination_schema` / `destination_table` | Target location (defaults to the pipeline catalog/schema and the source table name) |

### Source-specific options (`cube_data`)
| Option | Required | Default | Description |
|---|---|---|---|
| `years` | **yes** | | Years to load, e.g. `2025,2026` or `2024-2026` (OneStream stores each year in its own `DataRecord{YYYY}` table; they all land in the one `cube_data` table) |
| `scenarios` | no | all | Scenario member names, comma-separated (e.g. `Actual,Budget`) |
| `entities` | no | all | Entity member names, comma-separated |
| `include_descendants` | no | `false` | Also load all descendants of `entities` in the entity hierarchy (`true`/`false`) |
| `cube` | no | all | Cube name. Each cube keeps its own checkpoints, so cubes can be added later |
| `refresh_mode` | no | `incremental` | `snapshot` reloads the whole scope on every run. Only use it on a dedicated destination table, never to reconcile an incremental table (see *Deletes*) |
| `lookback_seconds` | no | `900` | Confirmation re-read window for recent changes. `0` disables it |
| `hold_in_flight_data_units` | no | `false` | Skip data units flagged as mid-update until the flag clears (`true`/`false`) |
| `in_flight_timeout_seconds` | no | `3600` | After this long, a still-set in-flight flag is treated as stale |
| `max_data_units_per_batch` | no | `5000` | Admission control: data units per micro-batch |
| `max_partitions` | no | `4` | Parallel read partitions. This bounds the concurrent load on OneStream |
| `page_size` | no | connection value | Rows per SQL page |

**Scope your loads.** A large application can hold more than 500M cube rows per pair of years, and an unscoped backfill through the REST API takes many hours. Start with the scenarios and entities you actually report on. You can widen the scope later on the same table (see *How incremental loading works*), e.g. most recent years first, then older history.

### Schema highlights
- `Amount` is exact `decimal(28,9)`. The connector parses API numbers losslessly.
- `XmlData` columns in `dimensions`, `cubes`, and `fx_rate_types` hold OneStream definitions as XML text.

## Data Type Mapping

| OneStream (SQL Server) | Databricks |
|---|---|
| `int`, `smallint`, `tinyint`, `bigint` | `BIGINT` |
| `decimal(p,s)` | `DECIMAL(p,s)` |
| `bit` | `BOOLEAN` |
| `uniqueidentifier`, `nvarchar` | `STRING` |
| .NET ticks (`_du_timestamp_ticks`) | `BIGINT`, plus `_du_modified_at TIMESTAMP` (UTC) |

## How to Run

### Step 1: Create the connection
See *Create a Unity Catalog Connection* above.

### Step 2: Configure your pipeline
```yaml
name: onestream_ingest
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
    - table:
        source_schema: default
        source_table: members
        destination_catalog: <CATALOG>
        destination_schema: <SCHEMA>
        destination_table: onestream_members
```

```bash
community-connector create_pipeline onestream onestream_ingest -ps <spec.yaml>
```

### Step 3: Run and schedule
Run the pipeline once to backfill, then schedule it, e.g. hourly or after the close calendar. Each run reads only the data units changed since the previous run.

#### Best Practices
- Run backfills outside business hours. OneStream app servers are shared with interactive finance users.
- Keep `max_partitions` small (the default is 4). Reads of cold partitions can take minutes, and that is normal.
- Each run scans each year table in scope once (about 1 minute per year on a 50M-row table) to locate entity partitions.

#### Troubleshooting
| Symptom | Cause / fix |
|---|---|
| `HTTP 401 ... token is not authenticated` | The PAT is invalid, expired, or revoked. Create a new PAT and update the connection |
| `HTTP 400 Unsupported API version` | Leave `api_version` at `5.2.0` |
| `unknown member name(s)` | Check the spelling in `scenarios`/`entities` against the `members` table |
| A run is slow on first touch | Cold OneStream storage. Lower `page_size` or `max_partitions` if the tenant is under load |
| `returned only N rows for a M-row page` | The OneStream server caps rows per request. Set `page_size` to `N` or less |
| `not standard monthly periods` | The application uses a non-standard time profile, which `cube_data` doesn't support yet |
| `member name(s) ... exist in more than one dimension` | The name is used in two dimensions of the same type. Narrow the scope |

## Security
OneStream's SQL endpoint runs any statement with the PAT owner's rights. This connector only builds single `SELECT` statements from validated identifiers and integer literals (`onestream_sql.py`). User-supplied member names are resolved to ids in Python and never embedded in SQL. It never calls `DataManagement` endpoints, and it only connects over HTTPS.

## Limitations
- **OneStream cloud with PAT auth only.** Self-hosted OneStream uses OAuth client credentials against your identity provider, which the connector doesn't support yet.
- **Standard monthly time profile only.** `TimeId` and `Period` are derived from OneStream's standard encoding. The connector checks the application's period keys and fails rather than writing wrong periods.
- **Physical deletes are not propagated** (see *Deletes*).
- **Clocks are assumed to be UTC.** That's true for OneStream cloud. The confirmation re-read window (`lookback_seconds`) absorbs normal clock differences.
- **Stored data only.** `cube_data` reads what OneStream stores in `DataRecord`. Dynamically calculated members (computed on the fly in cube views) aren't stored, so they aren't included.

## References
- OneStream REST API endpoints: https://documentation.onestream.com/docs/Content/REST%20API/OneStream%20WebAPI%20Endpoints.html
- Personal access tokens: https://documentation.onestream.com/docs/Content/IDM/Manage%20Personal%20Access%20Tokens.html
- Detailed API notes: [onestream_api_doc.md](onestream_api_doc.md)
