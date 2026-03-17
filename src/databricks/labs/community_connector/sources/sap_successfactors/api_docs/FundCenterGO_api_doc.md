# Fund Center API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- **FundCenterGO** - Fund Center entity representing one of the standard accounting objects supported for employees in an organization

## Object Schema

### FundCenterGO

| Field | Type | Max Length | Required | Read Only | Description |
|-------|------|------------|----------|-----------|-------------|
| effectiveStartDate | Edm.DateTime | - | Yes | No | Effective start date of the fund center |
| effectiveStatus | Edm.String | 128 | Yes | No | Status (A=Active, I=Inactive) |
| financialManagementArea | Edm.String | 4 | Yes | No | Financial management area code |
| fundCenterCode | Edm.String | 16 | Yes | No | Fund center code |
| createdBy | Edm.String | 100 | No | Yes | User who created the record |
| createdDateTime | Edm.DateTime | - | No | Yes | Creation timestamp |
| effectiveEndDate | Edm.DateTime | - | No | Yes | Effective end date |
| entityOID | Edm.String | 70 | No | Yes | Entity object ID |
| entityUUID | Edm.String | 70 | No | Yes | Entity UUID |
| externalCode | Edm.String | 128 | No | No | External code (format: fundCenterCode/financialManagementArea) |
| fundCenterDescription_defaultValue | Edm.String | 255 | No | No | Default description |
| fundCenterDescription_en_US | Edm.String | 255 | No | No | English (US) description |
| fundCenterDescription_localized | Edm.String | 255 | No | No | Localized description |
| lastModifiedBy | Edm.String | 100 | No | Yes | User who last modified the record |
| lastModifiedDateTime | Edm.DateTime | - | No | Yes | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Yes | Record status (C=Correction, D=Soft deleted, F=Full Purge, N=Normal, P=Pending, PH=Pending history) |

## Get Object Primary Keys
- **effectiveStartDate** (Edm.DateTime) - Effective start date
- **externalCode** (Edm.String) - External code (format: fundCenterCode/financialManagementArea)

Composite key format: `FundCenterGO(effectiveStartDate=datetime'{effectiveStartDate}',externalCode='{externalCode}')`

Example: `FundCenterGO(effectiveStartDate=datetime'2021-01-01T00:00:00',externalCode='FC001/FMA1')`

## Object's Ingestion Type

**Recommended Ingestion Type: `cdc`**

Rationale:
- The entity supports `$filter` on `lastModifiedDateTime` field
- The entity supports `$orderby` on `lastModifiedDateTime` for incremental ordering
- The `mdfSystemRecordStatus` field can indicate soft-deleted records (D status)
- Records can be tracked incrementally using the lastModifiedDateTime cursor field

**Cursor Field:** `lastModifiedDateTime`

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2/FundCenterGO
```

### Get All Fund Centers

**Request:**
```http
GET https://{api-server}/odata/v2/FundCenterGO
    ?$top={pageSize}
    &$skip={offset}
    &$filter=lastModifiedDateTime gt datetime'{lastSyncTime}'
    &$orderby=lastModifiedDateTime asc
    &$select=createdBy,createdDateTime,effectiveEndDate,effectiveStartDate,effectiveStatus,entityOID,entityUUID,externalCode,financialManagementArea,fundCenterCode,fundCenterDescription_defaultValue,fundCenterDescription_en_US,fundCenterDescription_localized,lastModifiedBy,lastModifiedDateTime,mdfSystemRecordStatus
```

### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| $top | integer | 20 | Number of records to return |
| $skip | integer | 0 | Number of records to skip |
| $filter | string | - | OData filter expression |
| $count | boolean | false | Include total count in response |
| $orderby | string | - | Sort order for results |
| $select | string | - | Fields to include in response |

### Supported $orderby Fields
- createdBy
- createdDateTime
- effectiveEndDate
- effectiveStartDate
- effectiveStatus
- entityOID
- entityUUID
- externalCode
- financialManagementArea
- fundCenterCode
- fundCenterDescription_defaultValue
- fundCenterDescription_en_US
- lastModifiedBy
- lastModifiedDateTime
- mdfSystemRecordStatus

### Get Single Fund Center by Key

**Request:**
```http
GET https://{api-server}/odata/v2/FundCenterGO(effectiveStartDate=datetime'2021-01-01T00:00:00',externalCode='FC001/FMA1')
```

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- Default page size is 20 records
- For incremental sync, filter by `lastModifiedDateTime gt datetime'{lastSyncTime}'`
- Order by `lastModifiedDateTime asc` for consistent incremental processing

### Example: Incremental Data Retrieval
```http
GET https://{api-server}/odata/v2/FundCenterGO
    ?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'
    &$orderby=lastModifiedDateTime asc
    &$top=1000
    &$skip=0
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

## Sources and References
- SAP SuccessFactors API Spec: FundCenterGO.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/b2b06831c2cb4d5facd1dfde49a7aab5/a058ad5e80b14e6280b3733337cfc014.html
