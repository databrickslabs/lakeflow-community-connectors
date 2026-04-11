# Forms Management API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Entity Name | Description |
|-------------|-------------|
| FormCustomizedWeightedRatingSection | Customized weighted rating sections in performance forms |
| FormHeader | Header information for performance management forms |
| FormPerfPotSummarySection | Performance/Potential summary sections |
| FormFolder | Form folder organization structure |
| FormTemplate | Form template definitions |

## Object Schema

### FormCustomizedWeightedRatingSection

| Field Name | Type | Description |
|------------|------|-------------|
| formContentId | Int64 | Composite key - form content identifier |
| formDataId | Int64 | Composite key - form data identifier |
| sectionDescription | String (nullable) | Description of the section |
| sectionIndex | Int32 | Index/order of the section |
| sectionName | String (nullable) | Name of the section |

### FormHeader

| Field Name | Type | Description |
|------------|------|-------------|
| formDataId | Int64 | Primary key - form data identifier |
| creationDate | DateTime | Date the form was created |
| currentStep | String (nullable) | Current workflow step |
| dateAssigned | DateTime (nullable) | Date form was assigned |
| formDataStatus | Int64 | Form status code |
| formLastModifiedDate | DateTime | Last modification date |
| formOriginator | String | User who created the form |
| formReviewDueDate | DateTime | Review due date |
| formReviewEndDate | DateTime | Review end date |
| formReviewStartDate | DateTime (nullable) | Review start date |
| formSubjectId | String | Subject user ID |
| formTemplateId | Int64 | Associated template ID |
| formTemplateType | String | Type of form template |
| formTitle | String | Form title |
| isRated | Boolean | Whether form has been rated |
| rating | Decimal | Form rating value |
| sender | String (nullable) | Form sender |
| stepDueDate | DateTime (nullable) | Current step due date |

### FormPerfPotSummarySection

| Field Name | Type | Description |
|------------|------|-------------|
| formContentId | Int64 | Composite key - form content identifier |
| formDataId | Int64 | Composite key - form data identifier |
| sectionDescription | String (nullable) | Description of the section |
| sectionIndex | Int32 | Index/order of the section |
| sectionName | String (nullable) | Name of the section |

### FormFolder

| Field Name | Type | Description |
|------------|------|-------------|
| folderId | Int64 | Primary key - folder identifier |
| folderName | String | Name of the folder |
| userId | String | User who owns the folder |

### FormTemplate

| Field Name | Type | Description |
|------------|------|-------------|
| formTemplateId | Int64 | Primary key - template identifier |
| formTemplateName | String | Name of the template |
| formTemplateType | String | Type of template (PM, 360, etc.) |

## Get Object Primary Keys

### FormCustomizedWeightedRatingSection
- **Composite Primary Key**: `formContentId` + `formDataId` (both Int64)

### FormHeader
- **Primary Key**: `formDataId` (Int64)

### FormPerfPotSummarySection
- **Composite Primary Key**: `formContentId` + `formDataId` (both Int64)

### FormFolder
- **Primary Key**: `folderId` (Int64)

### FormTemplate
- **Primary Key**: `formTemplateId` (Int64)

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|----------------|--------|
| FormHeader | `cdc` | Has `formLastModifiedDate` field for incremental tracking |
| FormCustomizedWeightedRatingSection | `snapshot` | No modification timestamp available |
| FormPerfPotSummarySection | `snapshot` | No modification timestamp available |
| FormFolder | `snapshot` | No modification timestamp available |
| FormTemplate | `snapshot` | No modification timestamp available |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All Form Headers
```http
GET /FormHeader
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | Integer | Number of records to return (default: 20) |
| $skip | Integer | Number of records to skip for pagination |
| $filter | String | OData filter expression |
| $orderby | Array | Fields to sort by |
| $select | Array | Fields to return |
| $expand | Array | Related entities to expand (formTemplate) |
| $search | String | Search phrases |
| $count | Boolean | Include total count |

**Sortable Fields:**
- creationDate
- currentStep
- dateAssigned
- formDataId
- formDataStatus
- formLastModifiedDate
- formOriginator
- formReviewDueDate
- formReviewEndDate
- formReviewStartDate
- formSubjectId
- formTemplateId
- formTemplateType
- formTitle
- isRated
- rating
- sender
- stepDueDate

**Example Request - Full Sync:**
```http
GET https://{api-server}/odata/v2/FormHeader?$top=1000&$select=formDataId,formTitle,formSubjectId,formLastModifiedDate
```

**Example Request - Incremental (CDC):**
```http
GET https://{api-server}/odata/v2/FormHeader?$filter=formLastModifiedDate gt datetime'2024-01-01T00:00:00'&$orderby=formLastModifiedDate asc&$top=1000
```

### Get Single Form Header by Key
```http
GET /FormHeader({formDataId})
```

**Example:**
```http
GET https://{api-server}/odata/v2/FormHeader(12345)?$expand=formTemplate
```

### Get All Customized Weighted Rating Sections
```http
GET /FormCustomizedWeightedRatingSection
```

### Get Single Customized Weighted Rating Section by Key
```http
GET /FormCustomizedWeightedRatingSection(formContentId={formContentId},formDataId={formDataId})
```

**Example:**
```http
GET https://{api-server}/odata/v2/FormCustomizedWeightedRatingSection(formContentId=100,formDataId=200)
```

### Get All Performance/Potential Summary Sections
```http
GET /FormPerfPotSummarySection
```

### Get Single Performance/Potential Summary Section by Key
```http
GET /FormPerfPotSummarySection(formContentId={formContentId},formDataId={formDataId})
```

### Get All Form Folders
```http
GET /FormFolder
```

### Get Single Form Folder by Key
```http
GET /FormFolder({folderId})
```

### Get All Form Templates
```http
GET /FormTemplate
```

**Expandable Relations:**
- associatedForms (collection of FormHeader)

**Example:**
```http
GET https://{api-server}/odata/v2/FormTemplate?$expand=associatedForms
```

### Get Single Form Template by Key
```http
GET /FormTemplate({formTemplateId})
```

### Pagination Strategy
- Use `$top` and `$skip` for offset-based pagination
- Default page size: 20 records
- Recommended page size: 1000 records for bulk operations
- For incremental sync on FormHeader: Order by `formLastModifiedDate`

### Response Format
```json
{
  "d": {
    "results": [
      {
        "formDataId": "12345",
        "formTitle": "Annual Performance Review 2024",
        "formSubjectId": "employee001",
        "formLastModifiedDate": "/Date(1492098664000)/",
        "formDataStatus": "2"
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
- SAP SuccessFactors API Spec: PMFormsManagement.json
- SAP Help Portal: [Performance Management Forms](https://help.sap.com/viewer/28bc3c8e3f214ab487ec51b1b8709adc/latest)
