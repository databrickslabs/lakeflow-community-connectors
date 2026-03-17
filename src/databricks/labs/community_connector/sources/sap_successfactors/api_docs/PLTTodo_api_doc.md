# Todo API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **Todo** - Legacy Todo (tasks assigned to logged-in user only)
2. **TodoEntryV2** - Enhanced Todo Entry (supports querying all users with proper permissions)

## Object Schema

### Todo (Legacy)
| Field | Type | Description |
|-------|------|-------------|
| categoryId | String | Primary key - Category identifier |
| categoryLabel | String (nullable) | Display label for category |
| displayOrder | Integer (int32, nullable) | Order for display |
| status | Integer (int32, nullable) | Todo status |

### TodoEntryV2
| Field | Type | Description |
|-------|------|-------------|
| todoEntryId | Decimal | Primary key - Todo entry identifier |
| categoryId | String | Category identifier |
| categoryLabel | String (nullable) | Display label for category |
| completedDateTime | DateTime (nullable) | Completion timestamp |
| createdDate | DateTime | Creation timestamp |
| dueDate | DateTime | Due date |
| formDataId | Integer (int32, nullable) | Form data identifier |
| lastModifiedDateTime | DateTime | Last modification timestamp |
| linkUrl | String (max 2000, nullable) | Associated URL link |
| status | Integer (int32) | Todo status |
| subjectId | String (max 100, nullable) | Subject user identifier |
| todoEntryName | String | Name/title of the todo entry |

## Get Object Primary Keys

| Entity | Primary Key Field(s) | Type |
|--------|---------------------|------|
| Todo | categoryId | String |
| TodoEntryV2 | todoEntryId | Decimal |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| Todo | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| TodoEntryV2 | `cdc` | Supports $filter and $orderby on `lastModifiedDateTime` field |

**Note:** The `TodoEntryV2` entity is the recommended entity for data ingestion as it:
- Supports querying todos for all users (with proper permissions)
- Has a `lastModifiedDateTime` field enabling CDC (Change Data Capture)
- Provides more comprehensive fields than the legacy `Todo` entity

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
| $count | Include count of items |
| $search | Search items by search phrases |

### Available $orderby Fields

**Todo:**
- categoryId, categoryLabel, displayOrder, status

**TodoEntryV2:**
- categoryId, categoryLabel, completedDateTime, createdDate, dueDate
- formDataId, lastModifiedDateTime, linkUrl, status, subjectId
- todoEntryId, todoEntryName

### Example Requests

**Get all TodoEntryV2 items with pagination:**
```http
GET https://{api-server}/odata/v2/TodoEntryV2?$top=100&$skip=0
```

**Get TodoEntryV2 items modified after a specific date (CDC):**
```http
GET https://{api-server}/odata/v2/TodoEntryV2?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get pending todos ordered by due date:**
```http
GET https://{api-server}/odata/v2/TodoEntryV2?$filter=status eq 0&$orderby=dueDate asc
```

**Get todos for a specific subject/user:**
```http
GET https://{api-server}/odata/v2/TodoEntryV2?$filter=subjectId eq 'user123'
```

**Get a specific todo entry by ID:**
```http
GET https://{api-server}/odata/v2/TodoEntryV2('12345')
```

**Get todos with selected fields:**
```http
GET https://{api-server}/odata/v2/TodoEntryV2?$select=todoEntryId,todoEntryName,status,dueDate,lastModifiedDateTime
```

**Get legacy Todo categories:**
```http
GET https://{api-server}/odata/v2/Todo?$orderby=displayOrder
```

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- Default page size is 20 items
- Increment `$skip` by page size for subsequent requests
- For CDC: filter by `lastModifiedDateTime` and order ascending to get incremental changes

### CRUD Operations (TodoEntryV2 only)

**Create a new todo:**
```http
POST https://{api-server}/odata/v2/TodoEntryV2
Content-Type: application/json

{
  "todoEntryName": "Complete Performance Review",
  "categoryId": "performance",
  "dueDate": "/Date(1735689600000)/",
  "status": 0
}
```

**Update a todo:**
```http
PUT https://{api-server}/odata/v2/TodoEntryV2('12345')
Content-Type: application/json

{
  "d": {
    "status": 1,
    "completedDateTime": "/Date(1704067200000)/"
  }
}
```

**Delete a todo:**
```http
DELETE https://{api-server}/odata/v2/TodoEntryV2('12345')
If-Match: *
```

### Response Structure
```json
{
  "d": {
    "results": [
      {
        "todoEntryId": "12345",
        "todoEntryName": "Complete Annual Review",
        "categoryId": "performance",
        "categoryLabel": "Performance",
        "status": 0,
        "dueDate": "/Date(1735689600000)/",
        "createdDate": "/Date(1704067200000)/",
        "lastModifiedDateTime": "/Date(1704153600000)/",
        "subjectId": "user123",
        ...
      }
    ]
  }
}
```

### Status Values
| Status Code | Meaning |
|-------------|---------|
| 0 | Pending/Open |
| 1 | Completed |
| 2 | Dismissed/Cancelled |

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
- SAP SuccessFactors API Spec: PLTTodo.json
- SAP Help Portal: [Todo APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/cb29d40f60594c559dd1251e084cdcf7.html)
