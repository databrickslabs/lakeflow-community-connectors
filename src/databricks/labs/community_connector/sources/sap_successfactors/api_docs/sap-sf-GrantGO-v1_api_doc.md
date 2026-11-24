# Grant API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Object Name | Description |
|-------------|-------------|
| GrantGO | Grant is one of the standard accounting objects supported for employees in an organization |

## Object Schema

### GrantGO

| Field Name | Type | Required | Max Length | Description |
|------------|------|----------|------------|-------------|
| companyCode | string | No | 4 | Company code |
| createdBy | string | No | 100 | User who created the record (read-only) |
| createdDateTime | string (date-time) | No | - | Creation timestamp (read-only) |
| effectiveEndDate | string (date-time) | No | - | Effective end date (read-only) |
| effectiveStartDate | string | Yes | - | Effective start date |
| effectiveStatus | string (enum: A, I) | Yes | 128 | Status: A=Active, I=Inactive |
| entityOID | string | No | 70 | Entity OID (read-only) |
| entityUUID | string | No | 70 | Entity UUID (read-only) |
| grantCode | string | Yes | 128 | Grant code |
| grantDesc_defaultValue | string | No | 255 | Grant description (default value) |
| grantDesc_en_US | string | No | 255 | Grant description (English US) |
| grantDesc_localized | string | No | 255 | Grant description (localized) |
| grantNotRelevent | boolean | No | - | Flag indicating if grant is not relevant |
| lastModifiedBy | string | No | 100 | User who last modified the record (read-only) |
| lastModifiedDateTime | string (date-time) | No | - | Last modification timestamp (read-only) |
| mdfSystemRecordStatus | string (enum) | No | 255 | Record status: C=Correction, D=Soft deleted, F=Full Purge, N=Normal, P=Pending, PH=Pending history (read-only) |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| GrantGO | effectiveStartDate, grantCode |

**Key Format**: `GrantGO(effectiveStartDate=datetime'{effectiveStartDate}',grantCode='{grantCode}')`

**Example**: `GrantGO(effectiveStartDate=datetime'2021-01-01T00:00:00',grantCode='string')`

## Object's Ingestion Type

| Object | Ingestion Type | Rationale |
|--------|---------------|-----------|
| GrantGO | `cdc` | The `lastModifiedDateTime` field is available and supports filtering/ordering. Use `$filter=lastModifiedDateTime gt datetime'{timestamp}'` for incremental loads. Soft deletes can be tracked via `mdfSystemRecordStatus eq 'D'`. |

**CDC Strategy**:
- Filter by `lastModifiedDateTime` for incremental data extraction
- Track soft deletes using `mdfSystemRecordStatus eq 'D'`
- Order by `lastModifiedDateTime` to ensure consistent pagination

## Read API for Data Retrieval

### Get All Grants

**Endpoint**: `GET https://{api-server}/odata/v2/GrantGO`

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| $filter | string | Filter items by property values |
| $select | string | Select properties to be returned |
| $orderby | string | Order items by property values |
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $count | boolean | Include count of items |

**Selectable Fields**:
- companyCode, createdBy, createdDateTime, effectiveEndDate, effectiveStartDate
- effectiveStatus, entityOID, entityUUID, grantCode
- grantDesc_defaultValue, grantDesc_en_US, grantDesc_localized
- grantNotRelevent, lastModifiedBy, lastModifiedDateTime, mdfSystemRecordStatus

**Orderable Fields**:
- All selectable fields support ascending and descending order

**Example Requests**:

```http
# Get all grants
GET https://{api-server}/odata/v2/GrantGO

# Get with pagination
GET https://{api-server}/odata/v2/GrantGO?$top=100&$skip=0

# Incremental load (CDC) - get records modified after a timestamp
GET https://{api-server}/odata/v2/GrantGO?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc

# Get active grants only
GET https://{api-server}/odata/v2/GrantGO?$filter=effectiveStatus eq 'A'

# Get soft-deleted records
GET https://{api-server}/odata/v2/GrantGO?$filter=mdfSystemRecordStatus eq 'D'

# Filter by company code
GET https://{api-server}/odata/v2/GrantGO?$filter=companyCode eq 'COMP'

# Select specific fields
GET https://{api-server}/odata/v2/GrantGO?$select=grantCode,companyCode,effectiveStatus,lastModifiedDateTime

# Get with count
GET https://{api-server}/odata/v2/GrantGO?$count=true&$top=100
```

### Get Grant by Key

**Endpoint**: `GET https://{api-server}/odata/v2/GrantGO(effectiveStartDate=datetime'{effectiveStartDate}',grantCode='{grantCode}')`

**Example**:
```http
GET https://{api-server}/odata/v2/GrantGO(effectiveStartDate=datetime'2021-01-01T00:00:00',grantCode='GRANT001')
```

**Response Structure**:
```json
{
  "d": {
    "results": [
      {
        "companyCode": "COMP",
        "createdBy": "admin",
        "createdDateTime": "/Date(1492098664000)/",
        "effectiveStartDate": "/Date(1492098664000)/",
        "effectiveStatus": "A",
        "grantCode": "GRANT001",
        "grantDesc_defaultValue": "Research Grant",
        "grantNotRelevent": false,
        "lastModifiedBy": "admin",
        "lastModifiedDateTime": "/Date(1492098664000)/",
        "mdfSystemRecordStatus": "N"
      }
    ]
  }
}
```

**Pagination Approach**:
- Use `$top` and `$skip` for offset-based pagination
- Default page size is 20 records
- For large datasets, combine with `$orderby=lastModifiedDateTime` for consistent ordering
- Continue fetching until results array is empty or less than $top

**HTTP Response Codes**:

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request - invalid property or JSON format |
| 401 | Unauthorized |
| 403 | Forbidden - insufficient permissions |
| 404 | Not Found |
| 500 | Internal Server Error |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| string (date-time) | TimestampType |
| string (enum) | StringType |
| boolean | BooleanType |

## Sources and References
- SAP SuccessFactors API Spec: sap-sf-GrantGO-v1.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/b2b06831c2cb4d5facd1dfde49a7aab5/5ae7fb4faddd4c408032bf6ba00bcf38.html
