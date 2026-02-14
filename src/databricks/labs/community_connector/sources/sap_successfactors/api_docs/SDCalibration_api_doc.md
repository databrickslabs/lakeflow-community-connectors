# Calibration API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Object Name | Description |
|-------------|-------------|
| TalentRatings | Talent rating feedback data linked to calibration sessions |
| CalibrationSession | Calibration session information including status and date |
| CalibrationSessionSubject | Subject (employee) records within a calibration session |
| CalibrationTemplate | Template definitions for calibration sessions |
| CalibrationSubjectRank | Ranking data for calibration subjects |

## Object Schema

### TalentRatings

| Field Name | Type | Description |
|------------|------|-------------|
| employeeId | string | Employee identifier |
| feedbackId | int64 | Feedback identifier (primary key) |
| feedbackModule | int32 | Feedback module identifier |
| feedbackName | string | Name of the feedback |
| feedbackRating | double | Rating value |
| feedbackRatingLabel | string | Label for the rating |
| feedbackScaleId | int64 | Scale identifier |
| feedbackScaleMaximum | double | Maximum value on the scale |
| feedbackScaleMinimum | double | Minimum value on the scale |
| feedbackSource | int32 | Source of feedback |
| feedbackType | int32 | Type of feedback |
| feedbackWeight | double | Weight of the feedback |
| formContentId | int64 | Form content identifier |
| formDataId | int64 | Form data identifier |
| calSession | Navigation | Link to CalibrationSession |

### CalibrationSession

| Field Name | Type | Description |
|------------|------|-------------|
| activationDate | date-time | Date when session was activated |
| lastModifiedBy | string | User who last modified the session |
| lastModifiedDateTime | date-time | Last modification timestamp |
| sessionDate | date-time | Date of the calibration session |
| sessionId | int64 | Session identifier (primary key) |
| sessionLocation | string | Location of the session |
| sessionName | string | Name of the session |
| status | int32 | Session status |
| calTemplate | Navigation | Link to CalibrationTemplate |
| subjectList | Navigation | Collection of CalibrationSessionSubject |

### CalibrationSessionSubject

| Field Name | Type | Description |
|------------|------|-------------|
| authorizedBy | string | User who authorized the subject |
| calSessionId | int64 | Calibration session identifier |
| calibratedFlag | boolean | Flag indicating if subject was calibrated |
| comments | string | Internal comments |
| createdBy | string | User who created the record |
| createdDateTime | date-time | Creation timestamp |
| externalComments | string | External comments |
| lastModifiedBy | string | User who last modified the record |
| lastModifiedDateTime | date-time | Last modification timestamp |
| pmFolderMapId | int64 | Performance Management folder map ID |
| pmFormDataId | int64 | Performance Management form data ID |
| pmFormOwnerId | string | Performance Management form owner ID |
| reason | string | Reason text |
| reasonId | int64 | Reason identifier |
| sessionSubjectId | int64 | Session subject identifier (primary key) |
| status | int32 | Subject status |
| userId | string | User identifier for the subject |
| calSession | Navigation | Link to CalibrationSession |
| pmRatingList | Navigation | Collection of TalentRatings |
| rankList | Navigation | Collection of CalibrationSubjectRank |

### CalibrationTemplate

| Field Name | Type | Description |
|------------|------|-------------|
| createdBy | string | User who created the template |
| createdDateTime | date-time | Creation timestamp |
| endDate | date-time | Template end date |
| lastModifiedBy | string | User who last modified the template |
| lastModifiedDateTime | date-time | Last modification timestamp |
| startDate | date-time | Template start date |
| status | int32 | Template status |
| templateId | int64 | Template identifier (primary key) |
| templateName | string | Name of the template |

### CalibrationSubjectRank

| Field Name | Type | Description |
|------------|------|-------------|
| dataType | int32 | Data type identifier |
| rank | int32 | Rank position |
| ratingValue | double | Rating value |
| subjectRankId | int64 | Subject rank identifier (primary key) |
| calSessionSubject | Navigation | Link to CalibrationSessionSubject |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| TalentRatings | feedbackId |
| CalibrationSession | sessionId |
| CalibrationSessionSubject | sessionSubjectId |
| CalibrationTemplate | templateId |
| CalibrationSubjectRank | subjectRankId |

## Object's Ingestion Type

| Object | Ingestion Type | Rationale |
|--------|---------------|-----------|
| TalentRatings | `snapshot` | No lastModifiedDateTime field available on the entity itself |
| CalibrationSession | `cdc` | Has `lastModifiedDateTime` field supporting filtering and ordering |
| CalibrationSessionSubject | `cdc` | Has `lastModifiedDateTime` field supporting filtering and ordering |
| CalibrationTemplate | `cdc` | Has `lastModifiedDateTime` field supporting filtering and ordering |
| CalibrationSubjectRank | `snapshot` | No lastModifiedDateTime field available |

## Read API for Data Retrieval

### TalentRatings

**Endpoint**: `GET https://{api-server}/odata/v2/TalentRatings`

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| $filter | string | Filter items by property values |
| $select | string | Select properties to be returned |
| $orderby | string | Order items by property values |
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $count | boolean | Include count of items |
| $search | string | Search items by search phrases |
| $expand | string | Expand related entities (calSession) |

**Example Requests**:

```http
# Get all talent ratings
GET https://{api-server}/odata/v2/TalentRatings

# Get with pagination
GET https://{api-server}/odata/v2/TalentRatings?$top=100&$skip=0

# Filter by employee
GET https://{api-server}/odata/v2/TalentRatings?$filter=employeeId eq 'EMP001'

# Expand related session
GET https://{api-server}/odata/v2/TalentRatings?$expand=calSession

# Get by key
GET https://{api-server}/odata/v2/TalentRatings(12345)
```

### CalibrationSession

**Endpoint**: `GET https://{api-server}/odata/v2/CalibrationSession`

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| $filter | string | Filter items by property values |
| $select | string | Select properties to be returned |
| $orderby | string | Order items by property values |
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $count | boolean | Include count of items |
| $search | string | Search items by search phrases |
| $expand | string | Expand related entities (calTemplate, subjectList) |

**Example Requests**:

```http
# Get all calibration sessions
GET https://{api-server}/odata/v2/CalibrationSession

# Incremental load (CDC)
GET https://{api-server}/odata/v2/CalibrationSession?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc

# Expand template and subjects
GET https://{api-server}/odata/v2/CalibrationSession?$expand=calTemplate,subjectList

# Filter by status
GET https://{api-server}/odata/v2/CalibrationSession?$filter=status eq 1

# Get by key
GET https://{api-server}/odata/v2/CalibrationSession(12345)
```

### CalibrationSessionSubject

**Endpoint**: `GET https://{api-server}/odata/v2/CalibrationSessionSubject`

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| $filter | string | Filter items by property values |
| $select | string | Select properties to be returned |
| $orderby | string | Order items by property values |
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $count | boolean | Include count of items |
| $search | string | Search items by search phrases |
| $expand | string | Expand related entities (calSession, pmRatingList, rankList) |

**Example Requests**:

```http
# Get all session subjects
GET https://{api-server}/odata/v2/CalibrationSessionSubject

# Incremental load (CDC)
GET https://{api-server}/odata/v2/CalibrationSessionSubject?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc

# Filter by calibration session
GET https://{api-server}/odata/v2/CalibrationSessionSubject?$filter=calSessionId eq 12345

# Expand ratings and ranks
GET https://{api-server}/odata/v2/CalibrationSessionSubject?$expand=pmRatingList,rankList

# Get by key
GET https://{api-server}/odata/v2/CalibrationSessionSubject(67890)
```

### CalibrationTemplate

**Endpoint**: `GET https://{api-server}/odata/v2/CalibrationTemplate`

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| $filter | string | Filter items by property values |
| $select | string | Select properties to be returned |
| $orderby | string | Order items by property values |
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $count | boolean | Include count of items |
| $search | string | Search items by search phrases |

**Example Requests**:

```http
# Get all templates
GET https://{api-server}/odata/v2/CalibrationTemplate

# Incremental load (CDC)
GET https://{api-server}/odata/v2/CalibrationTemplate?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc

# Get by key
GET https://{api-server}/odata/v2/CalibrationTemplate(11111)
```

### CalibrationSubjectRank

**Endpoint**: `GET https://{api-server}/odata/v2/CalibrationSubjectRank`

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| $filter | string | Filter items by property values |
| $select | string | Select properties to be returned |
| $orderby | string | Order items by property values |
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $count | boolean | Include count of items |
| $search | string | Search items by search phrases |
| $expand | string | Expand related entities (calSessionSubject) |

**Example Requests**:

```http
# Get all subject ranks
GET https://{api-server}/odata/v2/CalibrationSubjectRank

# Expand session subject
GET https://{api-server}/odata/v2/CalibrationSubjectRank?$expand=calSessionSubject

# Get by key
GET https://{api-server}/odata/v2/CalibrationSubjectRank(22222)
```

**Response Structure** (example for CalibrationSession):
```json
{
  "d": {
    "results": [
      {
        "activationDate": "/Date(1492098664000)/",
        "lastModifiedBy": "admin",
        "lastModifiedDateTime": "/Date(1492098664000)/",
        "sessionDate": "/Date(1492098664000)/",
        "sessionId": "12345",
        "sessionLocation": "Virtual",
        "sessionName": "Q4 2024 Calibration",
        "status": 1
      }
    ]
  }
}
```

**Pagination Approach**:
- Use `$top` and `$skip` for offset-based pagination
- Default page size is 20 records
- For CDC-enabled entities, combine with `$orderby=lastModifiedDateTime` for consistent ordering
- Continue fetching until results array is empty or less than $top

**HTTP Response Codes**:

| Code | Description |
|------|-------------|
| 200 | Success - Retrieved entities |
| default | Error response with error details |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| int32 | IntegerType |
| int64 | LongType |
| double | DoubleType |
| boolean | BooleanType |
| date-time (/Date(...)/) | TimestampType |

## Sources and References
- SAP SuccessFactors API Spec: SDCalibration.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/ddeacbd0ffc5442882b494cbda599eb1.html
