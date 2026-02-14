# Succession Management API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **NomineeHistory** - Tracks history of nominee changes in succession planning
2. **TalentGraphicOption** - Configuration options for talent graphics display
3. **TalentPool** - Talent pool definitions and configurations
4. **Successor** - Successor nominations and details
5. **NominationTarget** - Targets for succession nominations
6. **LegacyPositionEntity** - Legacy position data for succession planning

## Object Schema

### NomineeHistory

| Field | Type | Description |
|-------|------|-------------|
| id | Int64 | Primary key - Unique identifier for the nominee history record |
| changeType | Int32 | Type of change made |
| changeTypeLabel | String (nullable) | Label for the change type |
| modifiedBy | String (max 100) | User who modified the record |
| modifiedDateTime | DateTime | Timestamp of modification |
| nomineeId | Int64 | Reference to the nominee |
| note | String (nullable, max 4000) | Notes about the change |
| rank | Int32 (nullable) | Ranking of the nominee |
| readiness | Double (nullable) | Readiness score |
| readinessLabel | String (nullable) | Label for readiness level |
| status | Int32 | Current status |
| statusLabel | String (nullable) | Label for status |

### TalentGraphicOption

| Field | Type | Description |
|-------|------|-------------|
| dataIndex | String | **Primary Key** - Index for the data |
| optionKey | String | **Primary Key** - Unique key for the option |
| dataImage | String | Image data |
| dataLabel | String | Display label |
| dataShortLabel | String | Short display label |
| dataValue | String | Data value |
| gradientBackgroundColor | String | Background gradient color |
| optionIndex | Int32 | Index position of the option |
| optionLabel | String | Option display label |
| optionName | String | Option name |
| optionScaleId | String | Scale identifier |
| optionTarget | String | Target reference |
| optionType | String | Type of option |

### TalentPool

| Field | Type | Description |
|-------|------|-------------|
| code | String (max 128) | **Primary Key** - Unique code for the talent pool |
| effectiveStartDate | DateTime | **Primary Key** - Start date for effective dating |
| effectiveEndDate | DateTime (nullable) | End date for effective dating |
| effectiveStatus | String (nullable, max 255) | Status of the effective record |
| createdBy | String (nullable, max 255) | User who created the record |
| createdDateTime | DateTime (nullable) | Creation timestamp |
| lastModifiedBy | String (nullable, max 255) | User who last modified |
| lastModifiedDateTime | DateTime (nullable) | Last modification timestamp |
| description_defaultValue | String (nullable, max 255) | Default description |
| description_localized | String (nullable, max 255) | Localized description |
| description_de_DE | String (nullable, max 255) | German description |
| description_en_GB | String (nullable, max 255) | British English description |
| description_en_US | String (nullable, max 255) | US English description |
| description_es_ES | String (nullable, max 255) | Spanish description |
| description_fr_FR | String (nullable, max 255) | French description |
| description_ja_JP | String (nullable, max 255) | Japanese description |
| description_ko_KR | String (nullable, max 255) | Korean description |
| description_nl_NL | String (nullable, max 255) | Dutch description |
| description_pt_BR | String (nullable, max 255) | Brazilian Portuguese description |
| description_pt_PT | String (nullable, max 255) | Portuguese description |
| description_ru_RU | String (nullable, max 255) | Russian description |
| description_zh_CN | String (nullable, max 255) | Simplified Chinese description |
| description_zh_TW | String (nullable, max 255) | Traditional Chinese description |
| name_defaultValue | String (nullable, max 255) | Default name |
| name_localized | String (nullable, max 255) | Localized name |
| name_de_DE | String (nullable, max 255) | German name |
| name_en_GB | String (nullable, max 255) | British English name |
| name_en_US | String (nullable, max 255) | US English name |
| name_es_ES | String (nullable, max 255) | Spanish name |
| name_fr_FR | String (nullable, max 255) | French name |
| name_ja_JP | String (nullable, max 255) | Japanese name |
| name_ko_KR | String (nullable, max 255) | Korean name |
| name_nl_NL | String (nullable, max 255) | Dutch name |
| name_pt_BR | String (nullable, max 255) | Brazilian Portuguese name |
| name_pt_PT | String (nullable, max 255) | Portuguese name |
| name_ru_RU | String (nullable, max 255) | Russian name |
| name_zh_CN | String (nullable, max 255) | Simplified Chinese name |
| name_zh_TW | String (nullable, max 255) | Traditional Chinese name |
| enableReadiness | Boolean (nullable) | Whether readiness tracking is enabled |
| owner | String (nullable, max 100) | Owner of the talent pool |
| type | String (nullable, max 128) | Type of talent pool |
| transactionSequence | Int64 (nullable) | Transaction sequence number |
| mdfSystemCreatedBy | String (nullable, max 255) | MDF system creator |
| mdfSystemCreatedDate | DateTime (nullable) | MDF system creation date |
| mdfSystemEntityId | String (nullable, max 255) | MDF entity identifier |
| mdfSystemLastModifiedBy | String (nullable, max 255) | MDF last modifier |
| mdfSystemLastModifiedDate | DateTime (nullable) | MDF last modification date |
| mdfSystemLastModifiedDateWithTZ | DateTime (nullable) | MDF last modification with timezone |
| mdfSystemObjectType | String (nullable, max 255) | MDF object type |
| mdfSystemRecordId | String (nullable, max 255) | MDF record identifier |
| mdfSystemRecordStatus | String (nullable, max 255) | MDF record status |
| mdfSystemVersionId | Int64 (nullable) | MDF version identifier |

**Navigation Properties:**
- `successorNav` - Collection of related Successor entities

### Successor

| Field | Type | Description |
|-------|------|-------------|
| id | Int64 | **Primary Key** - Unique identifier |
| lastModifiedBy | String (nullable, max 100) | User who last modified |
| lastModifiedDateTime | DateTime (nullable) | Last modification timestamp |
| nomineeUserId | String (nullable, max 100) | User ID of the nominee |
| note | String (nullable, max 4000) | Notes about the successor |
| rank | Int32 (nullable) | Ranking of the successor |
| readiness | Double (nullable) | Readiness score |
| readinessLabel | String (nullable) | Label for readiness level |
| status | Int32 (nullable) | Current status |
| statusLabel | String (nullable) | Label for status |

**Navigation Properties:**
- `nomineeHistoryNav` - Collection of related NomineeHistory entities

### NominationTarget

| Field | Type | Description |
|-------|------|-------------|
| nominationId | Int64 | **Primary Key** - Unique identifier for the nomination |
| nominationType | Int32 | Type of nomination |

**Navigation Properties:**
- `successorNav` - Collection of related Successor entities
- `talentPoolNav` - Related TalentPool entity

### LegacyPositionEntity

| Field | Type | Description |
|-------|------|-------------|
| positionId | Int64 | **Primary Key** - Unique identifier for the position |
| positionCode | String (nullable, max 4000) | Position code |
| title | String (nullable, max 256) | Position title |
| incumbent | String (nullable, max 100) | Current incumbent |
| createDate | DateTime (nullable) | Creation date |

**Navigation Properties:**
- `parentNav` - Parent LegacyPositionEntity (self-reference)
- `successorNav` - Collection of related Successor entities

## Get Object Primary Keys

| Entity | Primary Key(s) |
|--------|----------------|
| NomineeHistory | `id` (Int64) |
| TalentGraphicOption | `dataIndex` (String), `optionKey` (String) |
| TalentPool | `code` (String), `effectiveStartDate` (DateTime) |
| Successor | `id` (Int64) |
| NominationTarget | `nominationId` (Int64) |
| LegacyPositionEntity | `positionId` (Int64) |

## Object's Ingestion Type

| Entity | Ingestion Type | Rationale |
|--------|----------------|-----------|
| NomineeHistory | `cdc` | Has `modifiedDateTime` field that supports filtering and ordering for incremental reads |
| TalentGraphicOption | `snapshot` | No timestamp fields available for incremental tracking |
| TalentPool | `cdc` | Has `lastModifiedDateTime` and `mdfSystemLastModifiedDate` fields for incremental tracking |
| Successor | `cdc` | Has `lastModifiedDateTime` field for incremental tracking |
| NominationTarget | `snapshot` | No timestamp fields available for incremental tracking |
| LegacyPositionEntity | `snapshot` | Only has `createDate`, no modification timestamp for change tracking |

## Read API for Data Retrieval

### Base URL Pattern
```
https://{api-server}/odata/v2/{EntitySet}
```

### Supported Query Parameters

| Parameter | Description |
|-----------|-------------|
| `$top` | Limit the number of results returned (default: 20) |
| `$skip` | Skip the first n results for pagination |
| `$filter` | Filter results by property values |
| `$orderby` | Order results by property values |
| `$select` | Select specific properties to return |
| `$expand` | Expand related entities |
| `$search` | Search items by search phrases |
| `$count` | Include count of items |

### Pagination Approach

Use `$top` and `$skip` for offset-based pagination:
- First page: `$top=100&$skip=0`
- Second page: `$top=100&$skip=100`
- Continue incrementing `$skip` until no more results

### Example Requests

**Get all NomineeHistory records:**
```http
GET https://{api-server}/odata/v2/NomineeHistory?$top=100
```

**Get NomineeHistory with specific fields:**
```http
GET https://{api-server}/odata/v2/NomineeHistory?$select=id,nomineeId,modifiedDateTime,status
```

**Filter NomineeHistory by modification date (incremental read):**
```http
GET https://{api-server}/odata/v2/NomineeHistory?$filter=modifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=modifiedDateTime asc
```

**Get TalentPool with expanded Successors:**
```http
GET https://{api-server}/odata/v2/TalentPool?$expand=successorNav
```

**Get single TalentPool by composite key:**
```http
GET https://{api-server}/odata/v2/TalentPool(code='POOL001',effectiveStartDate=datetime'2024-01-01T00:00:00')
```

**Get Successor records ordered by last modified:**
```http
GET https://{api-server}/odata/v2/Successor?$orderby=lastModifiedDateTime desc&$top=50
```

**Get LegacyPositionEntity with parent and successors:**
```http
GET https://{api-server}/odata/v2/LegacyPositionEntity?$expand=parentNav,successorNav
```

**Get NominationTarget with all related entities:**
```http
GET https://{api-server}/odata/v2/NominationTarget?$expand=successorNav,talentPoolNav
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| Edm.String | StringType |
| Edm.Int32 | IntegerType |
| Edm.Int64 | LongType |
| Edm.Boolean | BooleanType |
| Edm.DateTime | TimestampType |
| Edm.DateTimeOffset | TimestampType |
| Edm.Decimal | DecimalType |
| Edm.Double | DoubleType |

### JSON Type to Spark Type Mapping (from schema)

| JSON Schema Type | Format | Spark Type |
|------------------|--------|------------|
| string | - | StringType |
| string | int64 | LongType |
| integer | int32 | IntegerType |
| integer | int64 | LongType |
| boolean | - | BooleanType |
| number/string | double | DoubleType |
| string (DateTime pattern) | - | TimestampType |

## Sources and References

- SAP SuccessFactors API Spec: `SDSuccessionManagement.json`
- SAP Help Portal: [Succession Management APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/6465bcdd52d948898ecfd923092437bf.html)
- API Server List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
