# Succession and Development API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **DevLearningCertifications** - Learning certifications earned by employees
2. **DevLearning_4201** - Development learning activities and records

## Object Schema

### DevLearningCertifications

| Field | Type | Description |
|-------|------|-------------|
| certificateId | Int64 | **Primary Key** - Unique identifier for the certificate |
| learningId | Int64 | **Primary Key** - Reference to the learning activity |
| certificateName | String (nullable) | Name of the certificate |
| certificateCreatedBy | String (nullable) | User who created the certificate record |
| certificateCreatedDate | DateTime (nullable) | Date the certificate was created |
| certificateIssueDate | DateTime (nullable) | Date the certificate was issued |
| certificateExpiredDate | DateTime (nullable) | Expiration date of the certificate |
| certificateLastModifiedBy | String (nullable) | User who last modified the record |
| certificateLastModifiedDate | DateTime (nullable) | Last modification timestamp |

### DevLearning_4201

| Field | Type | Description |
|-------|------|-------------|
| learningId | Int64 | **Primary Key** - Unique identifier for the learning activity |
| userId | String (nullable) | User ID of the learner |
| assignee | String (nullable) | Person assigned to the learning |
| name | String (nullable) | Name of the learning activity |
| description | String (nullable) | Description of the learning activity |
| guid | String (nullable) | Global unique identifier |
| sourceType | String (nullable) | Source type of the learning |
| status | String (nullable) | Current status of the learning |
| startDate | DateTime (nullable) | Start date of the learning activity |
| completedDate | DateTime (nullable) | Completion date of the learning activity |

**Navigation Properties:**
- `learningCertNavigation` - Collection of related DevLearningCertifications entities

## Get Object Primary Keys

| Entity | Primary Key(s) |
|--------|----------------|
| DevLearningCertifications | `certificateId` (Int64), `learningId` (Int64) |
| DevLearning_4201 | `learningId` (Int64) |

## Object's Ingestion Type

| Entity | Ingestion Type | Rationale |
|--------|----------------|-----------|
| DevLearningCertifications | `cdc` | Has `certificateLastModifiedDate` field that supports filtering and ordering for incremental reads |
| DevLearning_4201 | `snapshot` | No modification timestamp field available; has `startDate` and `completedDate` but no `lastModifiedDate` for reliable change tracking |

**Note:** For DevLearning_4201, while `startDate` and `completedDate` exist, these represent business dates rather than record modification timestamps, making them unsuitable for reliable CDC. Consider using `snapshot` ingestion with periodic full refreshes or coordinate with SAP for audit trail capabilities.

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
| `$expand` | Expand related entities (DevLearning_4201 only) |
| `$search` | Search items by search phrases |
| `$count` | Include count of items |

### Pagination Approach

Use `$top` and `$skip` for offset-based pagination:
- First page: `$top=100&$skip=0`
- Second page: `$top=100&$skip=100`
- Continue incrementing `$skip` until no more results

### Example Requests

**Get all DevLearningCertifications:**
```http
GET https://{api-server}/odata/v2/DevLearningCertifications?$top=100
```

**Get DevLearningCertifications with specific fields:**
```http
GET https://{api-server}/odata/v2/DevLearningCertifications?$select=certificateId,learningId,certificateName,certificateIssueDate,certificateExpiredDate
```

**Filter DevLearningCertifications by modification date (incremental read):**
```http
GET https://{api-server}/odata/v2/DevLearningCertifications?$filter=certificateLastModifiedDate gt datetime'2024-01-01T00:00:00'&$orderby=certificateLastModifiedDate asc
```

**Get single DevLearningCertification by composite key:**
```http
GET https://{api-server}/odata/v2/DevLearningCertifications(certificateId=12345L,learningId=67890L)
```

**Get all DevLearning_4201 records:**
```http
GET https://{api-server}/odata/v2/DevLearning_4201?$top=100
```

**Get DevLearning_4201 with specific fields:**
```http
GET https://{api-server}/odata/v2/DevLearning_4201?$select=learningId,userId,name,status,startDate,completedDate
```

**Get DevLearning_4201 with related certifications:**
```http
GET https://{api-server}/odata/v2/DevLearning_4201?$expand=learningCertNavigation
```

**Filter DevLearning_4201 by status:**
```http
GET https://{api-server}/odata/v2/DevLearning_4201?$filter=status eq 'Completed'
```

**Get DevLearning_4201 ordered by completion date:**
```http
GET https://{api-server}/odata/v2/DevLearning_4201?$orderby=completedDate desc&$top=50
```

**Get single DevLearning_4201 by key:**
```http
GET https://{api-server}/odata/v2/DevLearning_4201(12345L)
```

**Get DevLearning_4201 for a specific user:**
```http
GET https://{api-server}/odata/v2/DevLearning_4201?$filter=userId eq 'user123'&$expand=learningCertNavigation
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

### JSON Type to Spark Type Mapping (from schema)

| JSON Schema Type | Format | Spark Type |
|------------------|--------|------------|
| string | - | StringType |
| string | int64 | LongType |
| integer | int32 | IntegerType |
| integer | int64 | LongType |
| boolean | - | BooleanType |
| string (DateTime pattern) | - | TimestampType |

## Sources and References

- SAP SuccessFactors API Spec: `SuccessionandDevelopmentSD.json`
- SAP Help Portal: [Succession and Development APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/7e0cb1d8434d46a4acba3d6e0b25519a.html)
- API Server List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
