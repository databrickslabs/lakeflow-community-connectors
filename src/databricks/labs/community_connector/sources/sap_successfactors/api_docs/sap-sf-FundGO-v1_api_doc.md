# Fund API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Object Name | Description |
|-------------|-------------|
| FundGO | Fund is one of the standard accounting objects supported for employees in an organization |

## Object Schema

### FundGO

| Field Name | Type | Required | Max Length | Description |
|------------|------|----------|------------|-------------|
| createdBy | string | No | 100 | User who created the record (read-only) |
| createdDateTime | string (date-time) | No | - | Creation timestamp (read-only) |
| effectiveEndDate | string (date-time) | No | - | Effective end date (read-only) |
| effectiveStartDate | string | Yes | - | Effective start date |
| effectiveStatus | string (enum: A, I) | Yes | 128 | Status: A=Active, I=Inactive |
| entityOID | string | No | 70 | Entity OID (read-only) |
| entityUUID | string | No | 70 | Entity UUID (read-only) |
| externalCode | string | No | 128 | External code (format: fundCode/fmArea) |
| fmArea | string | Yes | 4 | Financial Management Area |
| fundCode | string | Yes | 10 | Fund code |
| fundDescription_defaultValue | string | No | 255 | Fund description (default value) |
| fundDescription_en_US | string | No | 255 | Fund description (English US) |
| fundDescription_localized | string | No | 255 | Fund description (localized) |
| fundExpirationDate | string (date-time) | No | - | Fund expiration date |
| fundPeriodicity | string | No | 255 | Fund periodicity |
| lastModifiedBy | string | No | 100 | User who last modified the record (read-only) |
| lastModifiedDateTime | string (date-time) | No | - | Last modification timestamp (read-only) |
| mdfSystemRecordStatus | string (enum) | No | 255 | Record status: C=Correction, D=Soft deleted, F=Full Purge, N=Normal, P=Pending, PH=Pending history (read-only) |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| FundGO | effectiveStartDate, externalCode |

**Key Format**: `FundGO(effectiveStartDate=datetime'{effectiveStartDate}',externalCode='{externalCode}')`

**Example**: `FundGO(effectiveStartDate=datetime'2021-01-01T00:00:00',externalCode='string/strg')`

## Object's Ingestion Type

| Object | Ingestion Type | Rationale |
|--------|---------------|-----------|
| FundGO | `cdc` | The `lastModifiedDateTime` field is available and supports filtering/ordering. Use `$filter=lastModifiedDateTime gt datetime'{timestamp}'` for incremental loads. Soft deletes can be tracked via `mdfSystemRecordStatus eq 'D'`. |

**CDC Strategy**:
- Filter by `lastModifiedDateTime` for incremental data extraction
- Track soft deletes using `mdfSystemRecordStatus eq 'D'`
- Order by `lastModifiedDateTime` to ensure consistent pagination

## Read API for Data Retrieval

### Get All Funds

**Endpoint**: `GET https://{api-server}/odata/v2/FundGO`

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
- createdBy, createdDateTime, effectiveEndDate, effectiveStartDate, effectiveStatus
- entityOID, entityUUID, externalCode, fmArea, fundCode
- fundDescription_defaultValue, fundDescription_en_US, fundDescription_localized
- fundExpirationDate, fundPeriodicity, lastModifiedBy, lastModifiedDateTime, mdfSystemRecordStatus

**Orderable Fields**:
- All selectable fields support ascending and descending order

**Example Requests**:

```http
# Get all funds
GET https://{api-server}/odata/v2/FundGO

# Get with pagination
GET https://{api-server}/odata/v2/FundGO?$top=100&$skip=0

# Incremental load (CDC) - get records modified after a timestamp
GET https://{api-server}/odata/v2/FundGO?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc

# Get active funds only
GET https://{api-server}/odata/v2/FundGO?$filter=effectiveStatus eq 'A'

# Get soft-deleted records
GET https://{api-server}/odata/v2/FundGO?$filter=mdfSystemRecordStatus eq 'D'

# Select specific fields
GET https://{api-server}/odata/v2/FundGO?$select=externalCode,fundCode,fmArea,effectiveStatus,lastModifiedDateTime

# Get with count
GET https://{api-server}/odata/v2/FundGO?$count=true&$top=100
```

### Get Fund by Key

**Endpoint**: `GET https://{api-server}/odata/v2/FundGO(effectiveStartDate=datetime'{effectiveStartDate}',externalCode='{externalCode}')`

**Example**:
```http
GET https://{api-server}/odata/v2/FundGO(effectiveStartDate=datetime'2021-01-01T00:00:00',externalCode='string/strg')
```

**Response Structure**:
```json
{
  "d": {
    "results": [
      {
        "createdBy": "admin",
        "createdDateTime": "/Date(1492098664000)/",
        "effectiveStartDate": "/Date(1492098664000)/",
        "effectiveStatus": "A",
        "externalCode": "FUND001/FM01",
        "fmArea": "FM01",
        "fundCode": "FUND001",
        "fundDescription_defaultValue": "General Fund",
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
- SAP SuccessFactors API Spec: sap-sf-FundGO-v1.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/b2b06831c2cb4d5facd1dfde49a7aab5/c9a76802aed7420dbde352d788731070.html
