# Project Controlling Object API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Entity Name | Description |
|-------------|-------------|
| ProjectControllingObject | Project controlling object for public sector cost management (WBS elements) |

## Object Schema

### ProjectControllingObject

| Field Name | Type | Max Length | Required | Description |
|------------|------|------------|----------|-------------|
| externalCode | String | 50 | Yes | Primary key component - external code identifier |
| effectiveStartDate | DateTime | - | Yes | Primary key component - effective start date |
| effectiveStatus | String (Enum) | 128 | Yes | Status: A (Active), I (Inactive) |
| company | String | 128 | No | Company code |
| createdBy | String | 100 | No | User who created the record (read-only) |
| createdDateTime | DateTime (nullable) | - | No | Creation timestamp (read-only) |
| description_defaultValue | String (nullable) | 128 | No | Default description |
| description_en_US | String (nullable) | 128 | No | English (US) description |
| description_localized | String (nullable) | 128 | No | Localized description |
| effectiveEndDate | DateTime (nullable) | - | No | Effective end date (read-only) |
| entityOID | String (nullable) | 32 | No | Entity OID (read-only) |
| isStatistical | Boolean (nullable) | - | No | Statistical indicator |
| lastModifiedBy | String (nullable) | 100 | No | Last modified by user (read-only) |
| lastModifiedDateTime | DateTime (nullable) | - | No | Last modification timestamp (read-only) |
| mdfSystemRecordStatus | String (Enum) | 255 | No | Record status: C (Correction), D (Soft deleted), F (Full Purge Import in Progress), N (Normal), P (Pending), PH (Pending history) (read-only) |
| name_defaultValue | String (nullable) | 60 | No | Default name |
| name_en_US | String (nullable) | 60 | No | English (US) name |
| name_localized | String (nullable) | 32 | No | Localized name |
| projectControllingObjectType | String (nullable) | 128 | No | Object type - only WBS element (PRN) is supported |

## Get Object Primary Keys

### ProjectControllingObject
- **Composite Primary Key**:
  - `effectiveStartDate` (DateTime, format: YYYY-MM-DDTHH:MM:SS)
  - `externalCode` (String)

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|----------------|--------|
| ProjectControllingObject | `cdc` | Has `lastModifiedDateTime` field for incremental tracking; supports `$filter` and `$orderby` |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All Project Controlling Objects
```http
GET /ProjectControllingObject
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| $top | Integer | 20 | Number of records to return |
| $skip | Integer | - | Number of records to skip for pagination |
| $filter | String | - | OData filter expression |
| $orderby | Array | - | Fields to sort by |
| $select | Array | - | Fields to return |
| $count | Boolean | - | Include total count |

**Sortable Fields:**
- company (asc/desc)
- createdBy (asc/desc)
- createdDateTime (asc/desc)
- description_defaultValue (asc/desc)
- description_en_US (asc/desc)
- effectiveEndDate (asc/desc)
- effectiveStartDate (asc/desc)
- effectiveStatus (asc/desc)
- entityOID (asc/desc)
- externalCode (asc/desc)
- isStatistical (asc/desc)
- lastModifiedBy (asc/desc)
- lastModifiedDateTime (asc/desc)
- mdfSystemRecordStatus (asc/desc)
- name_defaultValue (asc/desc)
- projectControllingObjectType (asc/desc)

**Selectable Fields:**
- company
- createdBy
- createdDateTime
- description_defaultValue
- description_en_US
- description_localized
- effectiveEndDate
- effectiveStartDate
- effectiveStatus
- entityOID
- externalCode
- isStatistical
- lastModifiedBy
- lastModifiedDateTime
- mdfSystemRecordStatus
- name_defaultValue
- name_en_US
- name_localized
- projectControllingObjectType

**Example Request - Full Sync:**
```http
GET https://{api-server}/odata/v2/ProjectControllingObject?$top=1000&$select=externalCode,effectiveStartDate,name_defaultValue,lastModifiedDateTime
```

**Example Request - Incremental (CDC):**
```http
GET https://{api-server}/odata/v2/ProjectControllingObject?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc&$top=1000
```

**Example Request - Filter by Status:**
```http
GET https://{api-server}/odata/v2/ProjectControllingObject?$filter=effectiveStatus eq 'A'&$top=1000
```

### Get Single Project Controlling Object by Key
```http
GET /ProjectControllingObject(effectiveStartDate=datetime'{effectiveStartDate}',externalCode='{externalCode}')
```

**Path Parameters:**
| Parameter | Type | Format | Description |
|-----------|------|--------|-------------|
| effectiveStartDate | DateTime | YYYY-MM-DDTHH:MM:SS | Effective start date |
| externalCode | String | - | External code identifier |

**Example:**
```http
GET https://{api-server}/odata/v2/ProjectControllingObject(effectiveStartDate=datetime'2024-01-01T00:00:00',externalCode='WBS001')
```

### Create Project Controlling Object
```http
POST /ProjectControllingObject
```

**Request Body:**
```json
{
  "externalCode": "WBS001",
  "effectiveStartDate": "/Date(1704067200000)/",
  "effectiveStatus": "A",
  "name_defaultValue": "Project Alpha WBS",
  "description_defaultValue": "Work breakdown structure for Project Alpha",
  "company": "COMPANY01",
  "projectControllingObjectType": "PRN",
  "isStatistical": false
}
```

### Update Project Controlling Object
```http
PUT /ProjectControllingObject(effectiveStartDate=datetime'{effectiveStartDate}',externalCode='{externalCode}')
```

### Delete Project Controlling Object
```http
DELETE /ProjectControllingObject(effectiveStartDate=datetime'{effectiveStartDate}',externalCode='{externalCode}')
```

### Pagination Strategy
- Use `$top` and `$skip` for offset-based pagination
- Default page size: 20 records
- Recommended page size: 1000 records for bulk operations
- For CDC: Order by `lastModifiedDateTime` ascending and use cursor-based pagination

### Response Format

**Success Response (GET Collection):**
```json
{
  "d": {
    "results": [
      {
        "externalCode": "WBS001",
        "effectiveStartDate": "/Date(1704067200000)/",
        "effectiveStatus": "A",
        "name_defaultValue": "Project Alpha WBS",
        "description_defaultValue": "Work breakdown structure for Project Alpha",
        "company": "COMPANY01",
        "lastModifiedDateTime": "/Date(1704153600000)/",
        "projectControllingObjectType": "PRN"
      }
    ]
  }
}
```

**Success Response (GET Single):**
```json
{
  "d": {
    "externalCode": "WBS001",
    "effectiveStartDate": "/Date(1704067200000)/",
    "effectiveStatus": "A",
    "name_defaultValue": "Project Alpha WBS"
  }
}
```

**Error Response:**
```json
{
  "error": {
    "code": "NotFoundException",
    "message": {
      "lang": "en-US",
      "value": "Entity ProjectControllingObject with the given key effectiveStartDate=datetime'2020-01-01T00:00:00',externalCode='string', is not found."
    }
  }
}
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

## Status Code Mappings

### effectiveStatus
| Code | Description |
|------|-------------|
| A | Active |
| I | Inactive |

### mdfSystemRecordStatus
| Code | Description |
|------|-------------|
| C | Correction |
| D | Soft deleted record |
| F | Full Purge Import in Progress |
| N | Normal record |
| P | Pending record |
| PH | Pending history record |

## Sources and References
- SAP SuccessFactors API Spec: ProjectControllingObject.json
- SAP Help Portal: [Project Controlling Object](https://help.sap.com/viewer/b2b06831c2cb4d5facd1dfde49a7aab5/latest)
