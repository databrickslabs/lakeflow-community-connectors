# **OneStream API Documentation**

> Scope: read-only ingestion of OneStream (OneStream Software CPM platform, "XF") application data into Databricks.
>
> Evidence labels used throughout:
> - **[Docs]**: official OneStream documentation (see *Sources and References*).
> - **[Observed]**: verified by live probing of a OneStream **cloud** tenant on platform **9.3.3** (`xf-version 9.3.3.18929`), Application DB on **Azure SQL**, on 2026-11-10.
> - **[Inferred]**: concluded from observed data patterns but not stated by OneStream. Re-verify before relying on it.
> - **TBD**: unknown, with the reason.
>
> No OpenAPI/Swagger spec is published or served. `/OneStreamApi/swagger*` and unknown paths return the Blazor web-UI shell (HTTP 200 HTML) **[Observed]**.

---

## **Authorization**

### Preferred method: Personal Access Token (PAT), Bearer header

| Item | Value |
|---|---|
| Header | `Authorization: Bearer <PAT>` |
| Also send | `Content-Type: application/json`, `Accept: application/json` |
| Where PATs are created | OneStream Identity & Access Management (IAM) portal. A token is shown **once**. If it is lost, revoke it and create a new one **[Docs]** |
| Identity / permissions | A PAT runs as the user who created it, with that user's security roles. PATs have no scopes **[Docs]** |
| Required roles | `AccessAsNonInteractiveUser` (use own PATs). `AdministerNonInteractiveUser` manages all PATs **[Docs]** |
| Role needed for the SQL endpoint | **Administrator** role is required for `GetAdoDataSetForSqlCommand` **[Docs]**. The PAT used in testing worked against it **[Observed]** |
| Expiry | Has an expiry. An expired PAT is auto-revoked and cannot be re-enabled. The lifetime value is **TBD** (not in public docs) **[Docs]** |
| Applicability | OneStream-hosted (cloud) tenants. PATs are not available for government-cloud customers **[Docs]** |

Bad or expired token response **[Observed]**:
```http
HTTP/1.1 401
{"Type":"https://tools.ietf.org/html/rfc9110#section-15.5.2","Title":"Unauthorized","Status":401,
 "Detail":"External Provider token is not authenticated.", ...}
```

### Alternatives (Known Quirks, not used by the connector)
- **Self-hosted OneStream**: OAuth 2.0 `client_credentials` against the customer's IdP (Entra ID / Okta / PingFederate). The short-lived access token is sent the same way (`Authorization: Bearer`) **[Docs]**. The connector can support this later by exchanging `client_id`/`client_secret` at runtime. That is out of scope for v1.
- **Session logon**: `Authentication/Logon` and `Authentication/LogonAndReturnCookie` return a `SessionInfo.XfBytes` blob. A Bearer PAT is accepted directly on every DataProvider call, so the connector does **not** need a logon step **[Observed]**.

### Connection parameters the connector needs
| Parameter | Example | Notes |
|---|---|---|
| `base_url` | `https://<tenant>.onestreamcloud.com` | Host root. Strip a trailing `/OneStreamWeb` if the user pastes it. The connector derives `<base>/OneStreamApi/api/...` and `BaseWebServerUrl = <base>/OneStreamWeb` |
| `application_name` | `<Application>` | The OneStream **application** name as used in API bodies. It can differ from the display name returned under `Authorized applications` by `Authentication/Logon` **[Observed]** |
| `personal_access_token` | secret | |
| `api_version` | `5.2.0` | Optional, default `5.2.0`. See *API versions* |

### Example authenticated request **[Observed]**
```bash
curl -s -X POST "https://<tenant>.onestreamcloud.com/OneStreamApi/api/DataProvider/GetAdoDataSetForSqlCommand?api-version=5.2.0" \
  -H "Authorization: Bearer $ONESTREAM_PAT" -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{"BaseWebServerUrl":"https://<tenant>.onestreamcloud.com/OneStreamWeb",
       "ApplicationName":"<Application>",
       "SqlQuery":"SELECT 1 AS one",
       "DbLocation":"Application"}'
# -> {"Table":[{"one":1}]}
```

### API versions **[Observed]**
`api-version` is a required query parameter, and each action accepts only specific values. Wrong values return HTTP 400 `"Unsupported API version"` (`https://docs.api-versioning.org/problems#unsupported`).

| Endpoint group | Accepted `api-version` | Rejected (tested) |
|---|---|---|
| `DataProvider/*` | `5.2.0` | 1.0, 2.0, 3.0, 4.0, 5.0.0, 5.1.0, 5.3.0, 6.0.0, 7.0.0, 7.2.0, 8.0.0, 9.0.0 |
| `Authentication/Logon` | `7.2.0` | everything above except 7.2.0 |
| `Authentication/LogonAndReturnCookie`, `DataManagement/*` | `5.2.0` (routing and validation confirmed) | n/a |

No `api-supported-versions` response header is emitted.

---

## **Endpoint Inventory**

Base: `POST https://<host>/OneStreamApi/api/<Controller>/<Action>?api-version=<v>`. Every action is a POST with a JSON body. These are the only controllers found, both by probing and in the docs **[Docs][Observed]**. Probed and **not present** (HTTP 405): `Application/*`, `Workflow/*`, `Session/*`, `Metadata/*`, `Cube/*`, `FileExplorer/*`, `Authentication/ValidateToken`.

| Controller/Action | Read-only? | Used by connector | Required body fields (server validation) **[Observed]** | Optional fields **[Docs]** |
|---|---|---|---|---|
| `DataProvider/GetAdoDataSetForSqlCommand` | Read by intent, **not enforced** (see quirks) | **Primary** | `BaseWebServerUrl`, `ApplicationName`, `SqlQuery` | `DbLocation` (`"Application"` default, or `"External"`), `XFExternalDBConnectionName` (when External), `ResultDataTableName`, `CustomSubstVarsAsCommaSeparatedPairs` |
| `DataProvider/GetAdoDataSetForMethodCommand` | Yes | Optional (workflow status tables) | `BaseWebServerUrl`, `ApplicationName`, `XFCommandMethodType`, `MethodQuery` | `ResultDataTableName`, `CustomSubstVarsAsCommaSeparatedPairs` |
| `DataProvider/GetAdoDataSetForCubeViewCommand` | Yes | Optional (user-defined report extracts) | `BaseWebServerUrl`, `ApplicationName`, `CubeViewName` | `DataTablePerCubeViewRow` (bool), `ResultDataTableName`, `CubeViewDataTableOptions` (`IncludePovCube`, `IncludeMemberDetails`, `IncludeHasDataStatus`, `IncludeAnnotation`, ...), `CustomSubstVarsAsCommaSeparatedPairs` |
| `DataProvider/GetAdoDataSetForAdapter` | Yes, if the adapter is read-only | Optional | `BaseWebServerUrl`, `ApplicationName`, `AdapterName` | `WorkspaceName`, `ResultDataTableName`, `CustomSubstVarsAsCommaSeparatedPairs` |
| `Authentication/Logon` (`7.2.0`) | Yes | No | (none, PAT only) | `BaseWebServerUrl` |
| `Authentication/LogonAndReturnCookie` | Yes | No | `BaseWebServerUrl`, `ApplicationName` | |
| `DataManagement/ExecuteSequence` | **NO: runs jobs** | **Never** | `BaseWebServerUrl`, `ApplicationName`, `SequenceName` | `CustomSubstVarsAsCommaSeparatedPairs` |
| `DataManagement/ExecuteStep` | **NO: runs jobs** | **Never** | `BaseWebServerUrl`, `ApplicationName`, `DataManagementGroupName`, `StepName` | |

Notes:
- **Body field name conflict**: the docs name the method field `XFCommandMethodTypeId`, but the server validates **`XFCommandMethodType`** (string) **[Observed]**.
- **`DbLocation`**: the docs list `Application` and `External`. `Framework` (which some community notebooks use) is **not documented**. Use `Application` **[Docs]**.
- **`Parameters` array**: some community code sends `"Parameters": []`. It is accepted but undocumented. Parameterization is documented only through `CustomSubstVarsAsCommaSeparatedPairs` (`"Var1=[v1],Var2=[v2]"`) **[Docs]**. The connector should build fully literal, validated SQL (integers only, quoted identifiers) instead of relying on substitution variables.

### Response format (all DataProvider actions) **[Observed]**
A serialized ADO.NET `DataSet`: a JSON object whose keys are result-table names and whose values are arrays of row objects.
```json
{"Table": [{"MemberId": 12345, "Name": "...", "Description": "..."}]}
```
- The key is `"Table"` by default. It is the value of `ResultDataTableName` when that is provided (e.g. `{"Members":[...]}`).
- **Empty result = `{"Table": []}`**, which contains no column information. **Schemas must come from `INFORMATION_SCHEMA`, never from responses.**
- **Multiple statements are executed**, but only the first result table was observed in the payload.
- `NULL` becomes JSON `null`. `bit` becomes `true/false`. `uniqueidentifier` becomes a string. `datetime` becomes a naive ISO string without an offset (e.g. `"2026-01-08T18:57:03.56"`). The DB server time zone is **UTC** (`CURRENT_TIMEZONE()` = `(UTC) Coordinated Universal Time`).
- **`decimal(28,9)` values are emitted as full-precision JSON numbers** (e.g. `12345678901234567.123456789`). Default `json.loads` would round them to float. **The connector must parse with `parse_float=decimal.Decimal` (or `str`)**.
- `varbinary` is serialized as a base64 string **[Inferred]**. The connector should exclude binary columns by default.
- There is no server-side paging, cursor, or continuation token. One call returns one complete result set. Paging is done in SQL (see *Read API*).

### Error format **[Observed]**
- Validation: HTTP 400 `{"errors":{"<Field>":["The <Field> field is required."]},"title":"One or more validation errors occurred.",...}`
- Execution errors (bad SQL, bad method query): HTTP **500** `{"Type":...,"Title":"An error occurred while processing your request.","Status":500,"Detail":"<OneStream message>"}`. Each one is also written to the OneStream **Error Log** in the application, so the connector should avoid speculative or failing calls.
- Unknown action: HTTP 405. Wrong version: HTTP 400 `Unsupported API version`. Bad token: HTTP 401.

---

## **Object List**

OneStream does not expose a "list objects" REST endpoint. Objects are discovered through the SQL endpoint against the **Application database** (SQL Server / Azure SQL):

```sql
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES ORDER BY 1, 2
```

Observed shape of an application DB: several hundred base tables and a few dozen views across the schemas `dbo` (platform), `rcm` (Account Reconciliations solution), `txm` (Transaction Matching), `jem` / `jemcon` (Journal Entry Manager). Solution schemas exist only if those MarketPlace solutions are installed. Many tables are empty in a given application **[Observed]**.

The connector exposes a **curated, static table list** with known semantics (below), plus an **opt-in generic passthrough** for any other application table (see *Implementation plan*).

### Curated objects

v1 of the connector implements `cube_data` and the eight metadata tables (`members`, `member_relationships`, `member_properties`, `dimensions`, `cubes`, `cube_dimensions`, `fx_rates`, `fx_rate_types`). The other rows are researched candidates for later versions.

| Connector table | Source | What it is | Grain |
|---|---|---|---|
| `cube_data` | `dbo.DataRecord{YYYY}` (one physical table per year, e.g. `DataRecord2025`) + `dbo.DataUnitCacheTimestamp` | **Stored cube cell data** (the financial numbers). One row per stored intersection per year, with 12 monthly value/status column pairs | Cube × Entity × Parent × Cons × Scenario × Year × Account × Flow × Origin × IC × UD1..UD8 |
| `dimensions` | `dbo.Dim` | Dimension definitions by dimension type | DimId |
| `members` | `dbo.Member` | Dimension members (entities, accounts, scenarios, flows, UD1–UD8) | DimTypeId × MemberId |
| `member_relationships` | `dbo.Relationship` | Parent→child hierarchy edges per dimension type, with aggregation weight | DimTypeId × ParentId × ChildId |
| `member_properties` | `dbo.MemberProperty` | Member property values (vary by cube type / scenario type / time) | DimTypeId × MemberId × PropertyId × VaryBy* |
| `cubes` | `dbo.Cube` | Cube definitions | CubeId |
| `cube_dimensions` | `dbo.CubeDim` | Which dimension is assigned to each cube and dim type (by scenario type) | CubeId × DimTypeId × ScenarioTypeId |
| `time_dim_profiles` | `dbo.TimeDimProfile` | Time dimension profile (XML) | UniqueID |
| `fx_rates` | `dbo.FxRate` | Exchange rates by rate type, period, and currency pair | FxRateTypeID × TimeId × Source × Dest |
| `fx_rate_types` | `dbo.FxRateType` | Rate type names | UniqueID |
| `journal_headers` | `dbo.JournalHeader` | Journal headers, with status and lifecycle timestamps | UniqueID |
| `journal_line_items` | `dbo.JournalLineItem` | Journal lines (amount + full POV) | UniqueID |
| `stage_summary_target_data` | `dbo.StageSummaryTargetDataSlim` | Import/staging data after transformation (target member **names**, amounts), per workflow unit | Wfk × Wsk × Wtk × SummaryRowID |
| `workflow_profiles` | `dbo.WorkflowProfileHierarchy` | Workflow profile tree | ProfileKey |
| `data_unit_timestamps` | `dbo.DataUnitCacheTimestamp` | Last-modified ticks per data unit (also used internally as the change cursor for `cube_data`) | Cube × Entity × Parent × Cons × Scenario × Year |
| `calc_status` | `dbo.CalcStatus` | Per-period data-unit calc/translate/consolidate timestamps | Partition × Cube × Entity × Parent × Cons × Scenario × TimeId |
| `data_entry_audit_cells` | `dbo.DataEntryAuditCell` | Cell-level audit of data entry (forms, Excel, data management), with amounts | UniqueID |
| `data_entry_audit_sources` | `dbo.DataEntryAuditSource` | Audit header: who, how (`DataEntryType`), when | UniqueID |
| `audit_members` / `audit_relationships` / `audit_fx_rates` | `dbo.AuditMember`, `dbo.AuditRelationship`, `dbo.AuditFxRate` | Metadata change audit logs with `AuditTime` | per table |

The method-command workflow-status objects are listed under *Read API → Method commands*.

### Dimension type ids (`DimTypeId`) **[Observed + Inferred]**

| DimTypeId | Dimension | Stored in `dbo.Member`? | Evidence |
|---|---|---|---|
| 0 | Entity | yes | observed |
| 1 | Consolidation (Cons) | **no** (system members; currency ids such as `176`, plus negative system ids) | gap in sequence **[Inferred]** |
| 2 | Scenario | yes | observed |
| 3 | Time | **no** (encoded `TimeId`, see below) | **[Inferred]** |
| 4 | View | **no** (system) | **[Inferred]** |
| 5 | Account | yes | observed |
| 6 | Flow | yes | observed |
| 7 | Origin | **no** (system members, e.g. `OriginId = -30`) | **[Inferred]** |
| 8 | IC (Intercompany) | **no** (IC partners reuse Entity members; `-999` = None) | **[Inferred]** |
| 9–16 | UD1 … UD8 | yes | observed (one top-level dimension per UD type) |

Member ids are globally unique and allocated in per-dimension-type numeric ranges. `-999` = "None" and `-1` = "no parent" **[Observed]**. The names of system members (Cons, Origin, View) and their id-to-name mapping are **TBD**. They are not in any application table found. The connector should expose the raw ids and document known values.

### Time encoding (`TimeId`, `YearId`) **[Observed]**
`TimeId = Year × 1,000,000 + PeriodIndex × 1,000`. The monthly period indices were verified against stage-table time names (`2025003000` = `2025M1`):

| M1 | M2 | M3 | M4 | M5 | M6 | M7 | M8 | M9 | M10 | M11 | M12 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 4 | 5 | 7 | 8 | 9 | 12 | 13 | 14 | 16 | 17 | 18 |

The skipped indices (0–2, 6, 10, 11, 15, 19, 20) are year, half-year, and quarter roll-ups **[Inferred]**. This applies to standard monthly time profiles. Weekly or custom time profiles are **TBD**.

`DataRecord{YYYY}` is per year: `YearId` = YYYY and `M1Value…M12Value` = the 12 months.

### .NET ticks
`TimestampTicks`, `TimeModified`, `TimeStartCalc` etc. are .NET `DateTime.Ticks` (100 ns since 0001-01-01). `datetime_utc = 0001-01-01 + ticks/10` microseconds. In SQL: `DATEADD(second, (ticks - 621355968000000000)/10000000, '1970-01-01')`. The sentinel `599266080000000000` (= 1900-01-01) means "never" **[Observed]**. Ticks are assumed to be UTC because the server is UTC **[Inferred]**.

---

## **Object Schema**

Schemas are retrieved from SQL Server metadata. Column order and types are stable per platform version. The **connector must always read live metadata** because applications differ (solution schemas, UD usage):

```sql
SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION, c.COLUMN_NAME, c.DATA_TYPE,
       c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE,
       ku.ORDINAL_POSITION AS PK_POSITION
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
       ON tc.TABLE_SCHEMA = c.TABLE_SCHEMA AND tc.TABLE_NAME = c.TABLE_NAME AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
       ON ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND ku.TABLE_SCHEMA = tc.TABLE_SCHEMA
      AND ku.TABLE_NAME = c.TABLE_NAME AND ku.COLUMN_NAME = c.COLUMN_NAME
WHERE c.TABLE_SCHEMA = 'dbo' AND c.TABLE_NAME = 'Member'
ORDER BY c.ORDINAL_POSITION
```
The whole-DB variant (about 14k columns) returns in about 2 s **[Observed]**.

Fast row counts and sizes. These read metadata only, with no table scan:
```sql
SELECT s.name AS table_schema, t.name AS table_name,
       (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) AS row_count
FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
```
(Adding `sys.allocation_units` for MB sizes makes this take about 3.5 min on a large DB **[Observed]**. Avoid it at runtime.)

### Static schemas of the curated tables **[Observed, platform 9.3.3]**

`*` marks a primary-key column. Spark type is the recommended mapping (see *Field Type Mapping*).

<!-- SCHEMAS:BEGIN (generated from INFORMATION_SCHEMA on 2026-11-10) -->
#### `dbo.Dim`

Primary key: `DimTypeId`, `DimId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `DimTypeId` | int | N | LongType |
| 2 | `DimId` | int | N | LongType |
| 3 | `Name` | nvarchar(100) | N | StringType |
| 4 | `Description` | nvarchar(200) | N | StringType |
| 5 | `AccessGroupUniqueID` | uniqueidentifier | N | StringType |
| 6 | `MaintenanceGroupUniqueID` | uniqueidentifier | N | StringType |
| 7 | `InheritedDimId` | int | N | LongType |
| 8 | `DimMemberSourceTypeId` | int | N | LongType |
| 9 | `DimMemberSourcePath` | nvarchar(max) | N | StringType |
| 10 | `DimMemberSourceNVPairs` | nvarchar(max) | N | StringType |
| 11 | `XmlData` | nvarchar(max) | N | StringType |

#### `dbo.Member`

Primary key: `DimTypeId`, `MemberId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `DimTypeId` | int | N | LongType |
| 2 | `MemberId` | int | N | LongType |
| 3 | `Name` | nvarchar(500) | N | StringType |
| 4 | `Alias` | nvarchar(500) | N | StringType |
| 5 | `Description` | nvarchar(200) | N | StringType |
| 6 | `DimId` | int | N | LongType |
| 7 | `DisplayMemberGroupUniqueID` | uniqueidentifier | N | StringType |
| 8 | `ReadDataGroupUniqueID` | uniqueidentifier | N | StringType |
| 9 | `ReadDataGroupUniqueID2` | uniqueidentifier | N | StringType |
| 10 | `ReadWriteDataGroupUniqueID` | uniqueidentifier | N | StringType |
| 11 | `ReadWriteDataGroupUniqueID2` | uniqueidentifier | N | StringType |
| 12 | `UseCubeDataAccessSecurity` | bit | N | BooleanType |
| 13 | `DataCellAccessCategories` | nvarchar(max) | N | StringType |
| 14 | `ConditionalInputCategories` | nvarchar(max) | N | StringType |
| 15 | `DataMgmtAccessCategories` | nvarchar(max) | N | StringType |

#### `dbo.Relationship`

Primary key: `DimTypeId`, `ParentId`, `ChildId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `DimTypeId` | int | N | LongType |
| 2 | `ParentId` | int | N | LongType |
| 3 | `ChildId` | int | N | LongType |
| 4 | `DimId` | int | N | LongType |
| 5 | `SiblingSortOrder` | bigint | N | LongType |
| 6 | `UDAggWeight` | decimal(28,9) | N | DecimalType(28,9) |

#### `dbo.MemberProperty`

Primary key: `DimTypeId`, `MemberId`, `PropertyId`, `VaryByCubeTypeId`, `VaryByScenarioTypeId`, `VaryByTimeId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `DimTypeId` | int | N | LongType |
| 2 | `MemberId` | int | N | LongType |
| 3 | `PropertyId` | int | N | LongType |
| 4 | `VaryByCubeTypeId` | int | N | LongType |
| 5 | `VaryByScenarioTypeId` | int | N | LongType |
| 6 | `VaryByTimeId` | int | N | LongType |
| 7 | `RevertToDefaultCubeType` | bit | N | BooleanType |
| 8 | `RevertToDefaultScenarioType` | bit | N | BooleanType |
| 9 | `PropStorageTypeId` | int | N | LongType |
| 10 | `DecimalValue` | decimal(28,9) | N | DecimalType(28,9) |
| 11 | `TextValue` | nvarchar(max) | Y | StringType |

#### `dbo.Cube`

Primary key: `CubeId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `CubeId` | int | N | LongType |
| 2 | `Name` | nvarchar(100) | N | StringType |
| 3 | `Description` | nvarchar(200) | N | StringType |
| 4 | `CubeType` | int | N | LongType |
| 5 | `NumberType` | int | N | LongType |
| 6 | `StorageType` | int | N | LongType |
| 7 | `IsTopLevelCube` | bit | N | BooleanType |
| 8 | `TimeDimProfileID` | uniqueidentifier | N | StringType |
| 9 | `AccessGroupUniqueID` | uniqueidentifier | N | StringType |
| 10 | `MaintenanceGroupUniqueID` | uniqueidentifier | N | StringType |
| 11 | `ConsAlgorithmType` | int | N | LongType |
| 12 | `TransAlgorithmType` | int | N | LongType |
| 13 | `CalcNoneConsIfNoData` | bit | N | BooleanType |
| 14 | `CalcLocalCurrIfNoData` | bit | N | BooleanType |
| 15 | `CalcTransCurrsIfNoData` | bit | N | BooleanType |
| 16 | `CalcOwnerPreAdjIfNoData` | bit | N | BooleanType |
| 17 | `CalcShareIfNoData` | bit | N | BooleanType |
| 18 | `CalcElimIfNoData` | bit | N | BooleanType |
| 19 | `CalcOwnerPostAdjIfNoData` | bit | N | BooleanType |
| 20 | `BR1Name` | nvarchar(100) | N | StringType |
| 21 | `BR2Name` | nvarchar(100) | N | StringType |
| 22 | `BR3Name` | nvarchar(100) | N | StringType |
| 23 | `BR4Name` | nvarchar(100) | N | StringType |
| 24 | `BR5Name` | nvarchar(100) | N | StringType |
| 25 | `BR6Name` | nvarchar(100) | N | StringType |
| 26 | `BR7Name` | nvarchar(100) | N | StringType |
| 27 | `BR8Name` | nvarchar(100) | N | StringType |
| 28 | `DefaultCurrencyId` | int | N | LongType |
| 29 | `FxRateTypeIDForRevExp` | uniqueidentifier | N | StringType |
| 30 | `FxRuleTypeIdForRevExp` | int | N | LongType |
| 31 | `FxRateTypeIDForAssetLiab` | uniqueidentifier | N | StringType |
| 32 | `FxRuleTypeIdForAssetLiab` | int | N | LongType |
| 33 | `XmlData` | nvarchar(max) | N | StringType |

#### `dbo.CubeDim`

Primary key: `CubeId`, `DimTypeId`, `ScenarioTypeId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `CubeId` | int | N | LongType |
| 2 | `DimTypeId` | int | N | LongType |
| 3 | `ScenarioTypeId` | int | N | LongType |
| 4 | `DimId` | int | N | LongType |

#### `dbo.DataRecord{YYYY}`

Primary key: `PartitionId`, `CubeId`, `EntityId`, `ParentId`, `ConsId`, `ScenarioId`, `YearId`, `AccountId`, `FlowId`, `OriginId`, `ICId`, `UD1Id`, `UD2Id`, `UD34Id`, `UD56Id`, `UD78Id`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `PartitionId` | smallint | N | LongType |
| 2 | `CubeId` | smallint | N | LongType |
| 3 | `EntityId` | int | N | LongType |
| 4 | `ParentId` | int | N | LongType |
| 5 | `ConsId` | smallint | N | LongType |
| 6 | `ScenarioId` | int | N | LongType |
| 7 | `YearId` | smallint | N | LongType |
| 8 | `AccountId` | int | N | LongType |
| 9 | `FlowId` | int | N | LongType |
| 10 | `OriginId` | smallint | N | LongType |
| 11 | `ICId` | int | N | LongType |
| 12 | `UD1Id` | int | N | LongType |
| 13 | `UD2Id` | int | N | LongType |
| 14 | `UD34Id` | bigint | N | LongType |
| 15 | `UD56Id` | bigint | N | LongType |
| 16 | `UD78Id` | bigint | N | LongType |
| 17 | `UD3Id` | int | N | LongType |
| 18 | `UD4Id` | int | N | LongType |
| 19 | `UD5Id` | int | N | LongType |
| 20 | `UD6Id` | int | N | LongType |
| 21 | `UD7Id` | int | N | LongType |
| 22 | `UD8Id` | int | N | LongType |
| 23 | `M1Value` | decimal(28,9) | N | DecimalType(28,9) |
| 24 | `M1Status` | tinyint | N | LongType |
| 25 | `M2Value` | decimal(28,9) | N | DecimalType(28,9) |
| 26 | `M2Status` | tinyint | N | LongType |
| 27 | `M3Value` | decimal(28,9) | N | DecimalType(28,9) |
| 28 | `M3Status` | tinyint | N | LongType |
| 29 | `M4Value` | decimal(28,9) | N | DecimalType(28,9) |
| 30 | `M4Status` | tinyint | N | LongType |
| 31 | `M5Value` | decimal(28,9) | N | DecimalType(28,9) |
| 32 | `M5Status` | tinyint | N | LongType |
| 33 | `M6Value` | decimal(28,9) | N | DecimalType(28,9) |
| 34 | `M6Status` | tinyint | N | LongType |
| 35 | `M7Value` | decimal(28,9) | N | DecimalType(28,9) |
| 36 | `M7Status` | tinyint | N | LongType |
| 37 | `M8Value` | decimal(28,9) | N | DecimalType(28,9) |
| 38 | `M8Status` | tinyint | N | LongType |
| 39 | `M9Value` | decimal(28,9) | N | DecimalType(28,9) |
| 40 | `M9Status` | tinyint | N | LongType |
| 41 | `M10Value` | decimal(28,9) | N | DecimalType(28,9) |
| 42 | `M10Status` | tinyint | N | LongType |
| 43 | `M11Value` | decimal(28,9) | N | DecimalType(28,9) |
| 44 | `M11Status` | tinyint | N | LongType |
| 45 | `M12Value` | decimal(28,9) | N | DecimalType(28,9) |
| 46 | `M12Status` | tinyint | N | LongType |

#### `dbo.DataUnitCacheTimestamp`

Primary key: `PartitionId`, `ScenarioId`, `YearId`, `CubeId`, `EntityId`, `ParentId`, `ConsId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `PartitionId` | smallint | N | LongType |
| 2 | `ScenarioId` | int | N | LongType |
| 3 | `YearId` | int | N | LongType |
| 4 | `CubeId` | int | N | LongType |
| 5 | `EntityId` | int | N | LongType |
| 6 | `ParentId` | int | N | LongType |
| 7 | `ConsId` | int | N | LongType |
| 8 | `TimestampTicks` | bigint | N | LongType |
| 9 | `IsInCellUpdateLoop` | bit | N | BooleanType |

#### `dbo.CalcStatus`

Primary key: `PartitionId`, `CubeId`, `EntityId`, `ParentId`, `ConsId`, `ScenarioId`, `TimeId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `PartitionId` | smallint | N | LongType |
| 2 | `CubeId` | int | N | LongType |
| 3 | `EntityId` | int | N | LongType |
| 4 | `ParentId` | int | N | LongType |
| 5 | `ConsId` | int | N | LongType |
| 6 | `ScenarioId` | int | N | LongType |
| 7 | `TimeId` | int | N | LongType |
| 8 | `TimeModified` | bigint | N | LongType |
| 9 | `TimeStartCalc` | bigint | N | LongType |
| 10 | `GuidStartCalc` | uniqueidentifier | N | StringType |
| 11 | `TimeEndCalc` | bigint | N | LongType |
| 12 | `GuidEndCalc` | uniqueidentifier | N | StringType |
| 13 | `TimeStartTrans` | bigint | N | LongType |
| 14 | `GuidStartTrans` | uniqueidentifier | N | StringType |
| 15 | `TimeEndTrans` | bigint | N | LongType |
| 16 | `GuidEndTrans` | uniqueidentifier | N | StringType |
| 17 | `TimeStartCons` | bigint | N | LongType |
| 18 | `GuidStartCons` | uniqueidentifier | N | StringType |
| 19 | `TimeEndCons` | bigint | N | LongType |
| 20 | `GuidEndCons` | uniqueidentifier | N | StringType |

#### `dbo.DataEntryAuditCell`

Primary key: `UniqueID`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `UniqueID` | uniqueidentifier | N | StringType |
| 2 | `Timestamp` | datetime | N | TimestampNTZType |
| 3 | `CubeId` | int | N | LongType |
| 4 | `EntityId` | int | N | LongType |
| 5 | `ParentId` | int | N | LongType |
| 6 | `ConsId` | int | N | LongType |
| 7 | `ScenarioId` | int | N | LongType |
| 8 | `TimeId` | int | N | LongType |
| 9 | `AccountId` | int | N | LongType |
| 10 | `FlowId` | int | N | LongType |
| 11 | `OriginId` | int | N | LongType |
| 12 | `ICId` | int | N | LongType |
| 13 | `UD1Id` | int | N | LongType |
| 14 | `UD2Id` | int | N | LongType |
| 15 | `UD3Id` | int | N | LongType |
| 16 | `UD4Id` | int | N | LongType |
| 17 | `UD5Id` | int | N | LongType |
| 18 | `UD6Id` | int | N | LongType |
| 19 | `UD7Id` | int | N | LongType |
| 20 | `UD8Id` | int | N | LongType |
| 21 | `IsViewYTD` | bit | N | BooleanType |
| 22 | `Amount` | decimal(28,9) | N | DecimalType(28,9) |
| 23 | `IsNoData` | bit | N | BooleanType |
| 24 | `AuditSourceID` | uniqueidentifier | N | StringType |

#### `dbo.DataEntryAuditSource`

Primary key: `UniqueID`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `UniqueID` | uniqueidentifier | N | StringType |
| 2 | `Timestamp` | datetime | N | TimestampNTZType |
| 3 | `UserID` | uniqueidentifier | N | StringType |
| 4 | `DataEntryType` | nvarchar(50) | N | StringType |
| 5 | `TaskActivityID` | uniqueidentifier | N | StringType |
| 6 | `CubeViewOrFileName` | nvarchar(2000) | N | StringType |
| 7 | `FileName` | nvarchar(100) | N | StringType |
| 8 | `FileType` | nvarchar(50) | N | StringType |
| 9 | `FileBytes` | varbinary(max) | Y | StringType |

#### `dbo.JournalHeader`

Primary key: `UniqueID`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `UniqueID` | uniqueidentifier | N | StringType |
| 2 | `RelatedJournalID` | uniqueidentifier | N | StringType |
| 3 | `Name` | nvarchar(100) | N | StringType |
| 4 | `Description` | nvarchar(max) | N | StringType |
| 5 | `IsTemplate` | bit | N | BooleanType |
| 6 | `OriginatingTemplateID` | uniqueidentifier | N | StringType |
| 7 | `TemplateType` | int | N | LongType |
| 8 | `TemplateGroupID` | uniqueidentifier | N | StringType |
| 9 | `JournalReqtLevel` | int | N | LongType |
| 10 | `JournalReqtFrequency` | int | N | LongType |
| 11 | `TimeFilterForReqtFreq` | nvarchar(max) | N | StringType |
| 12 | `JournalRecurringFrequency` | int | N | LongType |
| 13 | `FinalPeriodForRecurringJournal` | int | N | LongType |
| 14 | `CanChangeJournalSettings` | bit | N | BooleanType |
| 15 | `CanChangeJournalPov` | bit | N | BooleanType |
| 16 | `CanChangeJournalLineItems` | bit | N | BooleanType |
| 17 | `CanChangeJournalAmounts` | bit | N | BooleanType |
| 18 | `XmlData` | nvarchar(max) | N | StringType |
| 19 | `JournalType` | int | N | LongType |
| 20 | `JournalBalanceType` | int | N | LongType |
| 21 | `IsSingleEntity` | bit | N | BooleanType |
| 22 | `EntityMemberFilter` | nvarchar(max) | N | StringType |
| 23 | `JournalStatus` | int | N | LongType |
| 24 | `WorkflowProfileID` | uniqueidentifier | N | StringType |
| 25 | `CubeId` | int | N | LongType |
| 26 | `EntityId` | int | N | LongType |
| 27 | `ParentId` | int | N | LongType |
| 28 | `ConsId` | int | N | LongType |
| 29 | `ScenarioId` | int | N | LongType |
| 30 | `TimeId` | int | N | LongType |
| 31 | `AccountId` | int | N | LongType |
| 32 | `FlowId` | int | N | LongType |
| 33 | `ICId` | int | N | LongType |
| 34 | `UD1Id` | int | N | LongType |
| 35 | `UD2Id` | int | N | LongType |
| 36 | `UD3Id` | int | N | LongType |
| 37 | `UD4Id` | int | N | LongType |
| 38 | `UD5Id` | int | N | LongType |
| 39 | `UD6Id` | int | N | LongType |
| 40 | `UD7Id` | int | N | LongType |
| 41 | `UD8Id` | int | N | LongType |
| 42 | `CreatedUserID` | uniqueidentifier | N | StringType |
| 43 | `CreatedTimestamp` | datetime | N | TimestampNTZType |
| 44 | `LastEditedUserID` | uniqueidentifier | N | StringType |
| 45 | `LastEditedTimestamp` | datetime | N | TimestampNTZType |
| 46 | `LastAppliedTemplateUserID` | uniqueidentifier | N | StringType |
| 47 | `LastAppliedTemplateTimestamp` | datetime | N | TimestampNTZType |
| 48 | `SubmittedUserID` | uniqueidentifier | N | StringType |
| 49 | `SubmittedTimestamp` | datetime | N | TimestampNTZType |
| 50 | `ApprovedUserID` | uniqueidentifier | N | StringType |
| 51 | `ApprovedTimestamp` | datetime | N | TimestampNTZType |
| 52 | `RejectedUserID` | uniqueidentifier | N | StringType |
| 53 | `RejectedTimestamp` | datetime | N | TimestampNTZType |
| 54 | `PostedUserID` | uniqueidentifier | N | StringType |
| 55 | `PostedTimestamp` | datetime | N | TimestampNTZType |
| 56 | `UnpostedUserID` | uniqueidentifier | N | StringType |
| 57 | `UnpostedTimestamp` | datetime | N | TimestampNTZType |

#### `dbo.JournalLineItem`

Primary key: `UniqueID`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `UniqueID` | uniqueidentifier | N | StringType |
| 2 | `JournalID` | uniqueidentifier | N | StringType |
| 3 | `SortOrder` | int | N | LongType |
| 4 | `CubeId` | int | N | LongType |
| 5 | `EntityId` | int | N | LongType |
| 6 | `ParentId` | int | N | LongType |
| 7 | `ConsId` | int | N | LongType |
| 8 | `ScenarioId` | int | N | LongType |
| 9 | `TimeId` | int | N | LongType |
| 10 | `AccountId` | int | N | LongType |
| 11 | `FlowId` | int | N | LongType |
| 12 | `ICId` | int | N | LongType |
| 13 | `UD1Id` | int | N | LongType |
| 14 | `UD2Id` | int | N | LongType |
| 15 | `UD3Id` | int | N | LongType |
| 16 | `UD4Id` | int | N | LongType |
| 17 | `UD5Id` | int | N | LongType |
| 18 | `UD6Id` | int | N | LongType |
| 19 | `UD7Id` | int | N | LongType |
| 20 | `UD8Id` | int | N | LongType |
| 21 | `Description` | nvarchar(max) | N | StringType |
| 22 | `LineItemType` | int | N | LongType |
| 23 | `Amount` | decimal(28,9) | N | DecimalType(28,9) |

#### `dbo.FxRate`

Primary key: `FxRateTypeID`, `TimeId`, `SourceCurrencyId`, `DestCurrencyId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `FxRateTypeID` | uniqueidentifier | N | StringType |
| 2 | `TimeId` | int | N | LongType |
| 3 | `SourceCurrencyId` | int | N | LongType |
| 4 | `DestCurrencyId` | int | N | LongType |
| 5 | `Amount` | decimal(28,9) | N | DecimalType(28,9) |

#### `dbo.FxRateType`

Primary key: `UniqueID`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `UniqueID` | uniqueidentifier | N | StringType |
| 2 | `Name` | nvarchar(100) | N | StringType |
| 3 | `Description` | nvarchar(200) | N | StringType |
| 4 | `AccessGroupUniqueID` | uniqueidentifier | N | StringType |
| 5 | `MaintenanceGroupUniqueID` | uniqueidentifier | N | StringType |
| 6 | `XmlData` | nvarchar(max) | N | StringType |

#### `dbo.StageSummaryTargetDataSlim`

Primary key: `Wfk`, `Wsk`, `Wtk`, `SummaryRowID`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `Wfk` | uniqueidentifier | N | StringType |
| 2 | `Wsk` | int | N | LongType |
| 3 | `Wtk` | int | N | LongType |
| 4 | `SummaryRowID` | uniqueidentifier | N | StringType |
| 5 | `Cube` | nvarchar(250) | N | StringType |
| 6 | `EtT` | nvarchar(250) | N | StringType |
| 7 | `PrTId` | int | N | LongType |
| 8 | `CnTId` | int | N | LongType |
| 9 | `SnT` | nvarchar(250) | N | StringType |
| 10 | `TmT` | nvarchar(50) | N | StringType |
| 11 | `VwT` | nvarchar(50) | N | StringType |
| 12 | `AcT` | nvarchar(250) | N | StringType |
| 13 | `FwTId` | int | N | LongType |
| 14 | `OgTId` | int | N | LongType |
| 15 | `IcTId` | int | N | LongType |
| 16 | `U1TId` | int | N | LongType |
| 17 | `U2TId` | int | N | LongType |
| 18 | `U3TId` | int | N | LongType |
| 19 | `U4TId` | int | N | LongType |
| 20 | `U5TId` | int | N | LongType |
| 21 | `U6TId` | int | N | LongType |
| 22 | `U7TId` | int | N | LongType |
| 23 | `U8TId` | int | N | LongType |
| 24 | `Am` | decimal(28,9) | N | DecimalType(28,9) |
| 25 | `Tv` | nvarchar(max) | N | StringType |

#### `dbo.WorkflowProfileHierarchy`

Primary key: `ProfileKey`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `ProfileKey` | uniqueidentifier | N | StringType |
| 2 | `ParentProfileKey` | uniqueidentifier | N | StringType |
| 3 | `HierarchyLevel` | int | N | LongType |
| 4 | `HierarchyIndex` | decimal(28,9) | N | DecimalType(28,9) |
| 5 | `ProfileName` | nvarchar(100) | N | StringType |
| 6 | `ProfileType` | int | N | LongType |
| 7 | `CubeName` | nvarchar(100) | N | StringType |
| 8 | `AccessGroupUniqueID` | uniqueidentifier | N | StringType |
| 9 | `MaintenanceGroupUniqueID` | uniqueidentifier | N | StringType |
| 10 | `IsTemplate` | bit | N | BooleanType |

#### `dbo.TimeDimProfile`

Primary key: `UniqueID`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `UniqueID` | uniqueidentifier | N | StringType |
| 2 | `Name` | nvarchar(100) | N | StringType |
| 3 | `Description` | nvarchar(200) | N | StringType |
| 4 | `XmlData` | nvarchar(max) | N | StringType |

#### `dbo.AuditMember`

Primary key: `AuditGuid`, `AuditSeqId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `AuditGuid` | uniqueidentifier | N | StringType |
| 2 | `AuditSeqId` | int | N | LongType |
| 3 | `AuditUser` | nvarchar(100) | N | StringType |
| 4 | `AuditTime` | datetime | N | TimestampNTZType |
| 5 | `AuditInsUpdateDel` | int | N | LongType |
| 6 | `DimTypeId` | int | N | LongType |
| 7 | `MemberId` | int | N | LongType |
| 8 | `Name` | nvarchar(500) | N | StringType |
| 9 | `Alias` | nvarchar(500) | N | StringType |
| 10 | `Description` | nvarchar(200) | N | StringType |
| 11 | `DimId` | int | N | LongType |
| 12 | `DisplayMemberGroupUniqueID` | uniqueidentifier | N | StringType |
| 13 | `ReadDataGroupUniqueID` | uniqueidentifier | N | StringType |
| 14 | `ReadDataGroupUniqueID2` | uniqueidentifier | N | StringType |
| 15 | `ReadWriteDataGroupUniqueID` | uniqueidentifier | N | StringType |
| 16 | `ReadWriteDataGroupUniqueID2` | uniqueidentifier | N | StringType |
| 17 | `UseCubeDataAccessSecurity` | bit | N | BooleanType |
| 18 | `DataCellAccessCategories` | nvarchar(max) | N | StringType |
| 19 | `ConditionalInputCategories` | nvarchar(max) | N | StringType |
| 20 | `DataMgmtAccessCategories` | nvarchar(max) | N | StringType |

#### `dbo.AuditRelationship`

Primary key: `AuditGuid`, `AuditSeqId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `AuditGuid` | uniqueidentifier | N | StringType |
| 2 | `AuditSeqId` | int | N | LongType |
| 3 | `AuditUser` | nvarchar(100) | N | StringType |
| 4 | `AuditTime` | datetime | N | TimestampNTZType |
| 5 | `AuditInsUpdateDel` | int | N | LongType |
| 6 | `DimTypeId` | int | N | LongType |
| 7 | `ParentId` | int | N | LongType |
| 8 | `ChildId` | int | N | LongType |
| 9 | `DimId` | int | N | LongType |
| 10 | `SiblingSortOrder` | bigint | N | LongType |
| 11 | `UDAggWeight` | decimal(28,9) | N | DecimalType(28,9) |

#### `dbo.AuditFxRate`

Primary key: `AuditGuid`, `AuditSeqId`

| # | Column | SQL Server type | Nullable | Spark type |
|---|---|---|---|---|
| 1 | `AuditGuid` | uniqueidentifier | N | StringType |
| 2 | `AuditSeqId` | int | N | LongType |
| 3 | `AuditUser` | nvarchar(100) | N | StringType |
| 4 | `AuditTime` | datetime | N | TimestampNTZType |
| 5 | `AuditInsUpdateDel` | int | N | LongType |
| 6 | `FxRateTypeID` | uniqueidentifier | N | StringType |
| 7 | `TimeId` | int | N | LongType |
| 8 | `SourceCurrencyId` | int | N | LongType |
| 9 | `DestCurrencyId` | int | N | LongType |
| 10 | `Amount` | decimal(28,9) | N | DecimalType(28,9) |
<!-- SCHEMAS:END -->

### `DataRecord{YYYY}` semantics **[Observed]**
- **105 physical tables** `DataRecord1996` … `DataRecord2075` exist in every application. Most are empty. The connector reads only the year tables named in the user's `years` option.
- **Data unit** = `CubeId, EntityId, ParentId, ConsId, ScenarioId, YearId`. This is OneStream's unit of calculation, clearing, and locking, and the leading part of the PK after `PartitionId`.
- `PartitionId` (smallint, leading clustered-key column): in the tenant tested, each entity was stored in a single PartitionId per year table. **Filtering on `PartitionId` turns a 7.9 s scan into a 0.6 s seek** for the same data unit. Mapping entities to partitions requires `SELECT DISTINCT PartitionId, EntityId FROM dbo.DataRecord{YYYY}` (a full scan, about 60 s on a 50M-row table), cached per run. The connector does **not** assume one partition per entity: it reads every partition an entity appears in.
- `UD34Id`, `UD56Id`, `UD78Id` (bigint) are packed pairs of `UD3Id/UD4Id` etc. They are in the PK for index width. The unpacked `UD3Id…UD8Id` columns are also present.
- `MnValue decimal(28,9)` + `MnStatus tinyint` (a bit-flag cell status). Observed status values: `16, 32, 33, 49, 64, 65, 96, 97, 145`. In a 2-partition sample of `M1`, **every row with status 16 had value 0** (status 16 is likely "NoData/cleared"). Bit meanings are **TBD**. Expose the raw status.
- **Cleared cells largely remain as rows** with zero value and a NoData-type status, rather than being physically deleted. Physical row deletion on "Clear Data" / data-unit recalculation is **TBD** (it needs a controlled write test, which the connector itself never performs).
- `dbo.vDataRecordAll` (view) unions all year tables. **Do not use it**: it cannot be seeked by year efficiently.

---

## **Get Object Primary Keys**

Primary keys come from `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` + `KEY_COLUMN_USAGE` (query above). The curated PKs are listed in the static schemas. Connector-level PKs:

| Connector table | Primary key |
|---|---|
| `cube_data` | `CubeId, EntityId, ParentId, ConsId, ScenarioId, TimeId, AccountId, FlowId, OriginId, ICId, UD1Id, UD2Id, UD3Id, UD4Id, UD5Id, UD6Id, UD7Id, UD8Id`: the source PK minus `PartitionId` and the packed `UD34/56/78` columns (redundant), with `YearId` replaced by the monthly `TimeId` of the long format |
| everything else | the source table's declared PK |
| tables with no declared PK | snapshot only (the generic passthrough refuses incremental reads without a PK) |

---

## **Object's ingestion type**

There is **no** SQL Server Change Tracking, **no** CDC (`is_cdc_enabled = 0`), and **no** `rowversion` column anywhere in the application DB **[Observed]**. Incremental reads therefore rely on OneStream's own timestamps. v1 implements `cube_data` and the eight metadata tables listed in the README. The other rows below are proposed designs for later versions.

| Connector table | Ingestion type | Cursor | Rationale |
|---|---|---|---|
| `cube_data` | `cdc` | `_du_timestamp_ticks` (the data unit's `DataUnitCacheTimestamp.TimestampTicks`, attached to each row) | OneStream updates this per-data-unit tick whenever the data unit's data changes (it is the platform's cache-invalidation stamp). **100% of data units present in `DataRecord2026` had a matching row** (matched without PartitionId, since the cache table stores `PartitionId = 0`), and the stamp matched the data unit's last calc end time to within about 1 ms **[Observed]**. On change, the **whole data unit is re-read** and upserted. Deletes: see *Read API → Deletes* |
| `members`, `member_relationships`, `member_properties`, `dimensions`, `cubes`, `cube_dimensions`, `time_dim_profiles`, `fx_rate_types`, `workflow_profiles`, `calc_status`, `data_unit_timestamps` | `snapshot` | none | small (≤ a few k rows, except `calc_status` at about 2M), with no reliable modified column |
| `fx_rates` | `snapshot` | none | about 60k rows. `AuditFxRate.AuditTime` could drive cdc later |
| `journal_headers` | `cdc` | `LastEditedTimestamp` (falls back to `CreatedTimestamp`) | lifecycle timestamps exist (`Created/LastEdited/Submitted/Approved/Rejected/Posted/UnpostedTimestamp`) |
| `journal_line_items` | `cdc` | the parent header's `LastEditedTimestamp` (joined via `JournalID`) | lines have no timestamp of their own. Re-read all lines of changed headers |
| `stage_summary_target_data` | `snapshot` (scoped by workflow time/scenario options) | none | no timestamp. TBD: a cursor via the workflow load history (`StageToFinanceLoadResult`, `WorkflowLockHistory.Timestamp`) |
| `data_entry_audit_cells`, `data_entry_audit_sources`, `audit_*` | `append` | `Timestamp` / `AuditTime` | insert-only audit logs |

---

## **Read API for Data Retrieval**

All reads go through `POST DataProvider/GetAdoDataSetForSqlCommand?api-version=5.2.0` with `DbLocation: "Application"`.

### Pagination (connector-side, in SQL)
There is no server paging, so every page is its own bounded SQL statement.
- **Keyset pagination is preferred:** `SELECT TOP (@page) <cols> FROM [s].[t] WHERE (<pk>) > (<last pk>) ORDER BY <pk>`. T-SQL has no row-value comparison, so expand it to the `(a > x) OR (a = x AND b > y) ...` form, or page by a leading integer key.
- **Plain `OFFSET n ROWS FETCH NEXT m ROWS ONLY`** works, but its cost grows with the offset: a 50k page at offset 0 took 4.3 s, and at **offset 5M it took 19.2 s**. A keyset-style seek to the same position took 6.1 s **[Observed]**. Use OFFSET only inside small, already-seeked slices (one `PartitionId` + data unit). Use keyset paging everywhere else.
- **Page size** **[Observed, single stream, warm cache]**:

  | Rows/call (46-col `DataRecord`) | Time | Response size |
  |---|---|---|
  | 50,000 | 4.3 s | 41.8 MB |
  | 100,000 | 6.7 s | 83.3 MB |
  | 200,000 | 12.9 s | 167.3 MB |

  Responses are **not compressed** (no `Content-Encoding`, even with `Accept-Encoding: gzip`), at about 0.84 KB of JSON per row. Default `page_size = 50000` (about 42 MB per response) to bound executor memory. 100k is a reasonable maximum.

### `cube_data` read algorithm (backfill + incremental)
Table options: `years` (required), `scenarios`, `entities` (+ `include_descendants`), `cube`, plus the checkpoint options in the README. The implementation is in `onestream.py`, and the checkpoint rules are in `onestream_offsets.py`.

1. **Resolve scope** to ids: names are looked up in `Member`, and an ambiguous name is an error. Descendants come from `Relationship`, walked in Python. The connector then checks that every period key in `CalcStatus` for the scoped years is a standard monthly `TimeId`, and fails loudly otherwise.
2. **Per-slice high-water marks** (one cheap `GROUP BY` on the stamp table, with slice = year × scenario × cube):
   ```sql
   SELECT [YearId], [ScenarioId], [CubeId], MAX([TimestampTicks]) AS [max_value]
   FROM [dbo].[DataUnitCacheTimestamp]
   WHERE [YearId] IN (...) [AND [ScenarioId] IN (...)] [AND [EntityId] IN (...)] AND [TimestampTicks] <= @run_start
   GROUP BY [YearId], [ScenarioId], [CubeId]
   ```
   Every read is capped at the run's start time so `Trigger.AvailableNow` terminates.
3. **Changed data units** per slice, in `(slice cursor, high-water]`, plus one confirmation re-read of recently stamped units and a catch-up for entities that newly joined the scope. Admission control is `max_data_units_per_batch`. The stamp rows are fetched once per run and reused across micro-batches.
4. **Entity → PartitionId map** per year (`SELECT DISTINCT PartitionId, EntityId FROM dbo.DataRecord{YYYY}`, about 60 s per 50M rows, once per run per year). It covers every partition an entity occupies, so all reads are index seeks.
5. **Read each changed data unit**: one read unit per (year, partition, cube, entity, scenario) with the changed (Parent, Cons) pairs. The pages are spread over `max_partitions` Spark partitions:
   ```sql
   SELECT <DataRecord columns> FROM [dbo].[DataRecord2026]
   WHERE [PartitionId] = @p AND [CubeId] = @c AND [EntityId] = @e AND [ScenarioId] = @s AND [YearId] = 2026
     AND (([ParentId] = @parent AND [ConsId] = @cons) OR ...)
   ORDER BY <clustered key> OFFSET @o ROWS FETCH NEXT @page ROWS ONLY
   ```
   Each row is unpivoted into 12 monthly records, tagged with its data unit's stamp.
6. **Offset**: per-slice cursors, the entity scope, and a per-slice "verified" watermark for confirmation re-reads (format documented in `onestream_offsets.py`). Restart-safe: batch ranges are derived from the two offsets alone, and upserts are idempotent.

Before paging, the connector checks once that the server returns full pages (a `sys.all_columns` row probe), so a server-side row cap fails loudly instead of truncating reads.

Sizing guidance: a large application can hold hundreds of millions of `DataRecord` rows per pair of years. A scoped slice of about 1M rows loads in a few minutes. **Users should scope `years`/`scenarios`/`entities`.** An unscoped full backfill of a large app takes many hours through the REST API.

### Deletes
- **Cleared data**: rows usually persist with NoData status and zero values, so they arrive as **updates**. All 12 months are emitted, including zeros, so a cleared cell overwrites its old value.
- **Physically removed rows** (TBD whether OneStream does this) are **not** propagated by `cdc`. The framework applies deletes by exact primary key (`apply_as_deletes`), and the connector has no record of previously emitted keys, so "replace a data unit" is not expressible. Do **not** reconcile by switching the table to snapshot: `apply_changes_from_snapshot` deletes every row outside the snapshot scope. If deletes turn out to matter, reconcile through a separate table.
- Members, relationships, and other small tables are `snapshot`, so their deletes are handled by the full refresh.

### Method commands (workflow status objects; optional)
`POST DataProvider/GetAdoDataSetForMethodCommand` with body `{BaseWebServerUrl, ApplicationName, XFCommandMethodType, MethodQuery, ResultDataTableName}`. `MethodQuery` is a sequence of curly-brace-delimited arguments. Signatures **[Observed]** (from server validation messages):

| XFCommandMethodType | MethodQuery arguments |
|---|---|
| `WorkflowProfiles` | `{WorkflowProfileType}{Filter}` |
| `WorkflowProfileRelatives` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Workflow Profile Type}{Relative Type}{Include Requesting Profile}{Filter}` |
| `WorkflowStatus` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Workflow Profile Type}{Relative Type}{Filter}` |
| `WorkflowStatusTwelvePeriod` | `{Workflow Profile Name}{Scenario Name}{Year Name}{Workflow Profile Type}{Relative Type}{Filter}` |
| `WorkflowAndEntityStatus` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Workflow Profile Type}{Relative Type}{Filter}` |
| `JournalsForWorkflowUnit` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Journal Status}{Filter}` |
| `FormsStatusForWorkflowUnit` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Form Status}{Filter}` |
| `ConfirmationForWorkflowUnit` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Include Descendants}{Filter}` |
| `CertificationForWorkflowUnit` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Include Descendants}{Filter}` (doc example: `{Houston}{Actual}{2018M1}{true}{}`) |
| `ICMatchingForWorkflowUnit` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Parameter Set Plug Account}{Currency Filter Override}{View Filter Override}{Plug Account Override}{Suppress Matches Override}{Tolerance Override}{Entity Filter Override}{Partner Filter Override}{DetailDims Filter Override}{Filter}` |
| `ICMatchingForWorkflowUnitMultiPlug` | `{Workflow Profile Name}{Scenario Name}{Time Name}{Empty String (All Plug Accounts), Single Plug Account or List of Plug Accounts for Parameter Set}{Currency Filter Override}{View Filter Override}{Suppress Matches Override}{Tolerance Override}{Entity Filter Override}{Partner Filter Override}{DetailDims Filter Override}{Filter}` |
| `ICMatchingForWorkflowUnitMultiPeriod` | `{Workflow Profile Name}{Scenario Name}{Time Name, List or Filter}{Parameter Set Plug Account}{Currency Filter Override}{View Filter Override}{Plug Account Override}{Suppress Matches Override}{Tolerance Override}{Entity Filter Override}{Partner Filter Override}{DetailDims Filter Override}{Filter}` |
| `ICMatchingPlugAccountsForWorkflowUnit` | `{Workflow Profile Name}{Scenario Name}{Time Name}` |
| `WorkflowCalculationEntities`, `WorkflowConfirmationEntities`, `WorkflowProfileAndDependentProfileEntities`, `WorkflowProfileEntities` | no parameters (`"Parameters are not used for method type ..."`). They likely depend on the session's current workflow POV, which is **TBD** for PAT calls |

The output columns of the method commands are **TBD** (they need valid workflow arguments). The doc example for `CertificationForWorkflowUnit` returns `ProfileName, ScenarioName, TimeKey, TimeName, QuestionResponse, ...` **[Docs]**. Treat these as **v2** tables. Ingestion type: `snapshot` per (profile, scenario, time) argument set.

### Cube views (optional)
`GetAdoDataSetForCubeViewCommand` returns a report grid as rows. It executes the OneStream calc engine, so results include **dynamic/derived** members (e.g. parent rollups, translated currencies) that are not stored in `DataRecord`. Cube views commonly depend on POV/prompt substitution variables. A call without them returned `{"<ResultDataTableName>": []}` **[Observed]**. Use case: "give me this exact report as a table", ingested as `snapshot`. The list of cube views comes from `SELECT Name, Description FROM dbo.CubeViewItem`. v2.

### Rate limits, timeouts, concurrency
| Limit | Value |
|---|---|
| Documented rate limits | **None published** **[Docs]** |
| HTTP 429 observed | none, up to 8 concurrent requests **[Observed]** |
| Request timeout (gateway) | **None hit at 250 s**: a `WAITFOR DELAY '00:04:10'` call returned 200 after 250.9 s **[Observed]**. The upper bound is TBD. Client timeout `request_timeout_seconds = 900` |
| Response size | 167 MB (200k rows) succeeded. The upper bound is TBD **[Observed]** |
| Concurrency | 8 parallel 50k-row reads on warm partitions: about 12 s each, **about 29k rows/s total** (about 2× a single stream) **[Observed]**. **Cold-cache variance**: with 4 parallel first-touch reads of large partitions, 2 requests took **263–266 s** while the other 2 took 6 s. Latency can spike by about 40× on cold data. The connector defaults to `max_partitions = 4` parallel read partitions (the app servers are shared with interactive finance users). Treat slow responses as normal, not as failures |

Connector behavior: retry with exponential backoff (up to 5 attempts) on HTTP 408/429/502/503/504, timeouts and connection errors. Do **not** retry HTTP 500 SQL errors (they are deterministic and each one writes to the OneStream Error Log). Back off when latency degrades.

---

## **Field Type Mapping**

| SQL Server | Spark | Notes |
|---|---|---|
| `int`, `smallint`, `tinyint` | `LongType` | Map all integer types to `LongType` for consistent joins across tables (member ids fit in int; `YearId` is smallint in `DataRecord` but int in `DataUnitCacheTimestamp`) |
| `bigint` | `LongType` | .NET ticks, packed UD pairs |
| `bit` | `BooleanType` | JSON `true/false` |
| `decimal(p,s)` / `numeric` | `DecimalType(p,s)` | `DataRecord` values are `decimal(28,9)`. **Parse JSON with Decimal** to avoid float rounding |
| `money`, `smallmoney` | `DecimalType(19,4)` | not observed |
| `float`, `real` | `DoubleType` | |
| `datetime`, `datetime2`, `smalldatetime` | `TimestampType` | Naive ISO strings, server is UTC. Interpret as UTC |
| `date` | `DateType` | |
| `uniqueidentifier` | `StringType` | lowercase GUID string |
| `nvarchar`, `varchar`, `nchar`, `char` | `StringType` | `XmlData` columns hold large XML definitions. They are kept in the metadata tables |
| `varbinary` | `BinaryType` (base64-decoded) **or excluded** | Exclude by default: attachment tables (`FileContents`, `ReconSupportDoc`) can hold hundreds of GB |

`cube_data` output schema (long format, one row per stored intersection per month):

| Column | Type | Source |
|---|---|---|
| `CubeId`, `EntityId`, `ParentId`, `ConsId`, `ScenarioId`, `YearId` | LongType | data-unit key columns of `DataRecord{YYYY}` |
| `TimeId` | LongType | derived: `YearId * 1,000,000 + period index * 1,000` (standard monthly profile) |
| `Period` | LongType | derived: month 1–12, from the `M{n}` column the row was unpivoted from |
| `AccountId`, `FlowId`, `OriginId`, `ICId`, `UD1Id` … `UD8Id` | LongType | intersection columns of `DataRecord{YYYY}` |
| `Amount` | DecimalType(28,9) | `M{n}Value` |
| `CellStatus` | LongType | `M{n}Status` (raw OneStream cell-status flags) |
| `_du_timestamp_ticks` | LongType | the data unit's `DataUnitCacheTimestamp.TimestampTicks` (the cursor) |
| `_du_modified_at` | TimestampType | `_du_timestamp_ticks` converted to UTC |

---

## **Known Quirks & Safety Rules**
1. **The SQL endpoint is not read-only.** It requires the Administrator role, and nothing documented blocks DML. Multi-statement batches execute. **The connector must only ever send SQL it generates itself** from validated identifiers (`[schema].[table]` / `[column]` that exist in `INFORMATION_SCHEMA`) and integer literals. Never interpolate free text. If a "custom query" feature is ever added, reject anything that is not a single `SELECT`/`WITH` statement.
2. **Never call `DataManagement/*`**. Those endpoints run jobs.
3. Avoid any failing call (500). Each one writes to the customer's OneStream Error Log.
4. Never scan `DataRecord{YYYY}` without a `PartitionId` predicate (except the once-per-run partition map). Unseeked queries take 50–80 s on a 50M-row year table.
5. Empty results carry no columns. Always use the metadata schema.
6. A dev/test OneStream application is often a **point-in-time copy of production** (the DB name carries a date suffix). Incremental tests there may see no new changes.
7. The application DB is shared by all workflows. Schedule backfills off-hours. The connector defaults to modest concurrency.

---

## **Sources and References**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Live probe | OneStream cloud tenant, platform 9.3.3, `api-version` 5.2.0 / 7.2.0 | 2026-11-10 | **Highest** (observed behavior) | Endpoints, required body fields, versions, response/error formats, all schemas, PKs, indexes, change-tracking absence, data-unit cursor, TimeId encoding, partition behavior, method-command signatures |
| Official Docs | https://documentation.onestream.com/docs/Content/REST%20API/OneStream%20WebAPI%20Endpoints.html | 2026-11-10 | High | Endpoint list, body fields, `DbLocation` values, Administrator requirement, `CustomSubstVarsAsCommaSeparatedPairs` |
| Official Docs | https://documentation.onestream.com/docs/Content/REST%20API/Authentication2.html | 2026-11-10 | High | Logon at 7.2.0, OAuth client_credentials for self-hosted |
| Official Docs | https://documentation.onestream.com/docs/Content/REST%20API/Configure%20OneStream%20API%20for%20External.html | 2026-11-10 | High | Self-hosted IdP configuration |
| Official Docs | https://documentation.onestream.com/docs/Content/IDM/Manage%20Personal%20Access%20Tokens.html | 2026-11-10 | High | PAT creation, one-time display, auto-revoke on expiry |
| Official Docs | https://documentation.onestream.com/docs/Content/IDM/Personal%20Access%20Tokens.html | 2026-11-10 | High | PAT identity = creator. Reference tokens only |
| Official Docs | https://documentation.onestream.com/docs/Content/IDM/Requirements%20for%20Using%20Personal%20Access%20Tokens.html | 2026-11-10 | High | Required roles |
| Official Docs | https://documentation.onestream.com/docs/Content/IDM/About%20Using%20Personal%20Access%20Tokens%20in%20APIs.html | 2026-11-10 | High | PATs not for government customers |
| Community OSS | https://github.com/CyTo78/OSRESTAPI (`api/src/lib/onestreamClient.ts`) | 2026-11-10 | Medium | Body key `XFCommandMethodType` (confirmed live), version defaults |
| Practitioner test notebook | private, not published | 2026-11-10 | Medium | 50k rows/call ≈ 4 s throughput, PartitionId-chunked load pattern |
| Airbyte / Fivetran | https://docs.airbyte.com/integrations/sources/onestream, https://fivetran.com/docs/connectors/applications/onestream | 2026-11-10 | n/a | **No existing connector** (404) |

Conflicts resolved: `XFCommandMethodTypeId` (docs) vs `XFCommandMethodType`: live validation wins. `DbLocation: "Framework"` (community notebook) vs `Application|External` (docs): docs win. `Framework` was not tested.

Not found publicly: rate limits, timeouts, PAT lifetime, data-model / table reference (the OneStream *Design and Reference Guide* is behind login), cell-status bit meanings, system member (Cons/Origin/View) id-to-name mapping.
