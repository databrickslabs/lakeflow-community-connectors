# Dismissal Protection API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- EmployeeDismissalProtectionDetail
- EmployeeDismissalProtection

## Object Schema

### EmployeeDismissalProtectionDetail

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| EmployeeDismissalProtection_workerId | string | 100 | Parent worker ID (key) |
| externalCode | string | 128 | External code (key) |
| createdBy | string | 100 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| dismissalProtectionType | string | 128 | Type of dismissal protection |
| lastModifiedBy | string | 100 | Last modified by user |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| mdfSystemRecordStatus | string | 255 | MDF record status |
| protectionEndDate | datetime | - | Protection end date |
| protectionStartDate | datetime | - | Protection start date |

### EmployeeDismissalProtection

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| workerId | string | 100 | Worker ID (key) |
| createdBy | string | 100 | Created by user |
| createdDateTime | datetime | - | Creation timestamp |
| lastModifiedBy | string | 100 | Last modified by user |
| lastModifiedDateTime | datetime | - | Last modification timestamp |
| mdfSystemRecordStatus | string | 255 | MDF record status |

**Related Entities:**
- empDismissalProtectionDetails -> EmployeeDismissalProtectionDetail

## Get Object Primary Keys

### EmployeeDismissalProtectionDetail
- `EmployeeDismissalProtection_workerId` (string)
- `externalCode` (string)

### EmployeeDismissalProtection
- `workerId` (string)

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| EmployeeDismissalProtectionDetail | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| EmployeeDismissalProtection | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Entity Endpoints

#### EmployeeDismissalProtectionDetail
```http
GET /EmployeeDismissalProtectionDetail
GET /EmployeeDismissalProtectionDetail(EmployeeDismissalProtection_workerId='{workerId}',externalCode='{externalCode}')
```

#### EmployeeDismissalProtection
```http
GET /EmployeeDismissalProtection
GET /EmployeeDismissalProtection('{workerId}')
```

### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| $top | integer | 5 | Maximum number of records to return |
| $skip | integer | - | Number of records to skip |
| $filter | string | - | OData filter expression |
| $orderby | string[] | - | Sort order for results |
| $select | string[] | - | Fields to include in response |
| $count | boolean | - | Include total count of records |
| $search | string | - | Search items by phrases |
| $expand | string[] | - | Expand related entities (EmployeeDismissalProtection only) |

### Available $orderby Fields

**EmployeeDismissalProtectionDetail:**
- EmployeeDismissalProtection_workerId
- createdBy, createdDateTime
- dismissalProtectionType
- externalCode
- lastModifiedBy, lastModifiedDateTime
- mdfSystemRecordStatus
- protectionEndDate, protectionStartDate

**EmployeeDismissalProtection:**
- createdBy, createdDateTime
- lastModifiedBy, lastModifiedDateTime
- mdfSystemRecordStatus
- workerId

### Example Requests

**Get all dismissal protection records with pagination:**
```http
GET /EmployeeDismissalProtection?$top=100&$skip=0
```

**Get dismissal protection modified after a specific date (CDC):**
```http
GET /EmployeeDismissalProtection?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get dismissal protection with details:**
```http
GET /EmployeeDismissalProtection?$expand=empDismissalProtectionDetails
```

**Get specific worker's dismissal protection:**
```http
GET /EmployeeDismissalProtection('{workerId}')?$expand=empDismissalProtectionDetails
```

**Get active protection records:**
```http
GET /EmployeeDismissalProtectionDetail?$filter=protectionEndDate ge datetime'2024-01-01T00:00:00'
```

**Get records by protection type:**
```http
GET /EmployeeDismissalProtectionDetail?$filter=dismissalProtectionType eq 'MATERNITY'
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "workerId": "USER001",
        "createdBy": "ADMIN",
        "createdDateTime": "/Date(1609459200000)/",
        "lastModifiedBy": "ADMIN",
        "lastModifiedDateTime": "/Date(1609459200000)/",
        "mdfSystemRecordStatus": "N",
        "empDismissalProtectionDetails": {
          "results": [
            {
              "EmployeeDismissalProtection_workerId": "USER001",
              "externalCode": "DP001",
              "dismissalProtectionType": "MATERNITY",
              "protectionStartDate": "/Date(1609459200000)/",
              "protectionEndDate": "/Date(1617235200000)/"
            }
          ]
        }
      }
    ]
  }
}
```

### Pagination
- Default page size: 5 records
- Use `$top` and `$skip` for pagination
- Iterate with increasing `$skip` until no more results

## Field Type Mapping

| API Type | Spark Type | Notes |
|----------|------------|-------|
| string | StringType | Basic string fields |
| string (maxLength) | StringType | String with length constraint |
| datetime (/Date(...)/) | TimestampType | SAP OData datetime format |

## Sources and References
- SAP SuccessFactors API Spec: ECDismissalProtection.json
- SAP Help Portal: [Dismissal Protection on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/c6340025421a4704aba4f9f899e63f2f.html)
