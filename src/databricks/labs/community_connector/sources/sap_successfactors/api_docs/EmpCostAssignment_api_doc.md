# Employee Cost Assignment API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- **EmpCostAssignment** - Employee Cost Assignment entity for managing cost center assignments, fund, grant, functional area, budget period, and funds center for employees

## Object Schema

### EmpCostAssignment

| Field | Type | Max Length | Required | Read Only | Description |
|-------|------|------------|----------|-----------|-------------|
| effectiveStartDate | Edm.DateTime | - | Yes | No | Effective start date of the cost assignment |
| worker | Edm.String | 100 | Yes | No | Worker identifier |
| companyCode | Edm.String | 4 | No | No | Company code |
| createdBy | Edm.String | 100 | No | Yes | User who created the record |
| createdDateTime | Edm.DateTime | - | No | Yes | Creation timestamp |
| doNotSyncFromPositionCostAssignment | Edm.Boolean | - | No | No | Flag to prevent sync from position cost assignment |
| effectiveEndDate | Edm.DateTime | - | No | Yes | Effective end date |
| lastModifiedBy | Edm.String | 100 | No | Yes | User who last modified the record |
| lastModifiedDateTime | Edm.DateTime | - | No | Yes | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Yes | Record status (C=Correction, D=Soft deleted, F=Full Purge, N=Normal, P=Pending, PH=Pending history) |
| mdfSystemStatus | Edm.String | 128 | No | No | System status (A=Active, I=Inactive) |
| skipValidationDerivation | Edm.Boolean | - | No | No | Flag to skip validation derivation |

### EmpCostAssignmentItem (Nested)

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| defaultAssignment | Edm.Boolean | - | Yes | Indicates if this is the default assignment |
| costCenter | Edm.String | 128 | Yes | Cost center code |
| percentage | Edm.Decimal | - | Yes | Percentage allocation |
| budgetPeriod | Edm.String | 128 | No | Budget period |
| functionalArea | Edm.String | 128 | No | Functional area code |
| fund | Edm.String | 128 | No | Fund code |
| fundCenter | Edm.String | 128 | No | Fund center code |
| grant | Edm.String | 128 | No | Grant code |
| workBreakdownStructure | Edm.String | 128 | No | Work breakdown structure |

## Get Object Primary Keys
- **effectiveStartDate** (Edm.DateTime) - Effective start date
- **worker** (Edm.String) - Worker identifier

Composite key format: `EmpCostAssignment(effectiveStartDate=datetime'{effectiveStartDate}',worker='{worker}')`

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
https://{api-server}/odata/v2/EmpCostAssignment
```

### Get All Employee Cost Assignments

**Request:**
```http
GET https://{api-server}/odata/v2/EmpCostAssignment
    ?$top={pageSize}
    &$skip={offset}
    &$filter=lastModifiedDateTime gt datetime'{lastSyncTime}'
    &$orderby=lastModifiedDateTime asc
    &$select=companyCode,createdBy,createdDateTime,doNotSyncFromPositionCostAssignment,effectiveEndDate,effectiveStartDate,lastModifiedBy,lastModifiedDateTime,skipValidationDerivation,worker
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
- companyCode
- createdBy
- createdDateTime
- doNotSyncFromPositionCostAssignment
- effectiveEndDate
- effectiveStartDate
- lastModifiedBy
- lastModifiedDateTime
- skipValidationDerivation
- worker

### Get Single Employee Cost Assignment by Key

**Request:**
```http
GET https://{api-server}/odata/v2/EmpCostAssignment(effectiveStartDate=datetime'2021-01-01T00:00:00',worker='EMP001')
```

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- Default page size is 20 records
- For incremental sync, filter by `lastModifiedDateTime gt datetime'{lastSyncTime}'`
- Order by `lastModifiedDateTime asc` for consistent incremental processing

### Example: Incremental Data Retrieval
```http
GET https://{api-server}/odata/v2/EmpCostAssignment
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
- SAP SuccessFactors API Spec: EmpCostAssignment.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/b2b06831c2cb4d5facd1dfde49a7aab5/a60fb8ac528044f191bc9735ac174c72.html
