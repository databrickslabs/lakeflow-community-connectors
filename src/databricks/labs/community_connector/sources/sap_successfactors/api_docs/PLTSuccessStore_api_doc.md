# Success Store API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **SuccessStoreContent** - Success Store Content
2. **SuccessStoreContentBlob** - Success Store Content Blob

## Object Schema

### SuccessStoreContent
| Field | Type | Description |
|-------|------|-------------|
| contentId | String (int64) | Primary key - Content identifier |
| bestPractice | Boolean (nullable) | Whether content is a best practice |
| comments | String (nullable) | Content comments |
| contentType | String (nullable) | Type of content |
| customField | String (nullable) | Custom field data |
| defaultContent | Boolean (nullable) | Whether content is default |
| defaultContentName | String (nullable) | Default content name |
| domain | String (nullable) | Content domain |
| expiryOnDate | DateTime (nullable) | Expiration date |
| publishOnDate | DateTime (nullable) | Publication date |
| revisionNo | String (nullable) | Revision number |
| status | String (nullable) | Content status |
| wizardable | Boolean (nullable) | Whether content is wizardable |
| contentData | SuccessStoreContentBlob | Related content blob |

### SuccessStoreContentBlob
| Field | Type | Description |
|-------|------|-------------|
| contentId | String | Primary key - Content identifier |

## Get Object Primary Keys

| Entity | Primary Key Field(s) | Type |
|--------|---------------------|------|
| SuccessStoreContent | contentId | int64 |
| SuccessStoreContentBlob | contentId | String |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| SuccessStoreContent | `snapshot` | No lastModifiedDateTime field available; has `publishOnDate` and `expiryOnDate` but no modification tracking field |
| SuccessStoreContentBlob | `snapshot` | No lastModifiedDateTime field available for incremental filtering |

**Note:** While `SuccessStoreContent` has date fields (`publishOnDate`, `expiryOnDate`), these represent content lifecycle dates rather than modification timestamps. For reliable change tracking, snapshot ingestion is recommended.

## Read API for Data Retrieval

### Base URL Pattern
```
https://{api-server}/odata/v2/{EntitySet}
```

### Supported Query Parameters
| Parameter | Description |
|-----------|-------------|
| $select | Select specific properties to return |
| $filter | Filter items by property values |
| $top | Limit number of items returned (default: 20) |
| $skip | Skip first n items for pagination |
| $orderby | Order items by property values |
| $expand | Expand related entities (SuccessStoreContent only) |
| $count | Include count of items |
| $search | Search items by search phrases |

### Available $orderby Fields

**SuccessStoreContent:**
- bestPractice, comments, contentId, contentType, customField
- defaultContent, defaultContentName, domain
- expiryOnDate, publishOnDate, revisionNo, status, wizardable

**SuccessStoreContentBlob:**
- contentId

### Available $expand Options

**SuccessStoreContent:**
- contentData (expands to SuccessStoreContentBlob)

### Example Requests

**Get all Success Store Content with pagination:**
```http
GET https://{api-server}/odata/v2/SuccessStoreContent?$top=100&$skip=0
```

**Get Success Store Content ordered by publish date:**
```http
GET https://{api-server}/odata/v2/SuccessStoreContent?$orderby=publishOnDate desc
```

**Get Success Store Content with expanded blob data:**
```http
GET https://{api-server}/odata/v2/SuccessStoreContent?$expand=contentData
```

**Get active content (not expired):**
```http
GET https://{api-server}/odata/v2/SuccessStoreContent?$filter=status eq 'active'
```

**Get a specific content by ID:**
```http
GET https://{api-server}/odata/v2/SuccessStoreContent(12345)
```

**Get content with selected fields:**
```http
GET https://{api-server}/odata/v2/SuccessStoreContent?$select=contentId,contentType,domain,status
```

**Get Success Store Content Blob by ID:**
```http
GET https://{api-server}/odata/v2/SuccessStoreContentBlob('content-id-123')
```

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- Default page size is 20 items
- Increment `$skip` by page size for subsequent requests
- Example: Page 1: `$top=100&$skip=0`, Page 2: `$top=100&$skip=100`

### Response Structure
```json
{
  "d": {
    "results": [
      {
        "contentId": "12345",
        "contentType": "template",
        "domain": "performance",
        "status": "active",
        "publishOnDate": "/Date(1492098664000)/",
        "expiryOnDate": null,
        ...
      }
    ]
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

## Sources and References
- SAP SuccessFactors API Spec: PLTSuccessStore.json
- SAP Help Portal: [Success Store on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/7e0721d63aa9446bb3edfa2c80252ac7.html)
