# Calibration Session API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
- CalibrationSession
- CalibrationSubject
- CalibrationSessionReviewer
- CalibrationExecutiveReviewer
- CalibrationSubjectComment
- CalibrationCompetencyRating
- CalibrationRating
- CalibrationRatingOption

## Object Schema

### CalibrationSession
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| id | int64 | No | Yes | Yes | Unique identifier for the session |
| sessionName | string | No | No | Yes | Name of the calibration session |
| sessionDate | date | Yes | No | Yes | Date of the session (format: YYYY-MM-DD) |
| sessionLocation | string | Yes | No | Yes | Location of the session |
| status | int32 | No | No | Yes | Status code of the session |
| activationDate | date | Yes | No | Yes | Date when session was activated |
| approvalDateTime | date-time | Yes | No | Yes | DateTime when session was approved |
| createdByWorkAssignmentLegacyId | string | No | No | Yes | Work assignment ID of the creator |
| lastModifiedDateTime | date-time | No | No | Yes | Last modification timestamp |
| executiveReviewerList | array | No | No | Yes | List of executive reviewers |
| reviewerList | array | No | No | Yes | List of session reviewers |
| subjectList | array | No | No | Yes | List of subjects being calibrated |

### CalibrationSubject
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| id | int64 | No | Yes | Yes | Unique identifier for the subject |
| workAssignmentLegacyId | string | No | No | Yes | Work assignment ID of the subject |
| calibrated | boolean | Yes | No | Yes | Whether the subject has been calibrated |
| pmFormDataId | int64 | Yes | No | Yes | Performance management form data ID |
| pmuDeeplink | string | Yes | No | Yes | Deep link to PMU |
| commentList | array | No | No | Yes | List of comments on this subject |
| competencyRatingList | array | No | No | Yes | List of competency ratings |
| ratingList | array | No | No | Yes | List of ratings |

### CalibrationSessionReviewer
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| id | int64 | No | Yes | Yes | Unique identifier |
| workAssignmentLegacyId | string | No | No | Yes | Work assignment ID of the reviewer |
| reviewerType | string | No | No | Yes | Type of reviewer (facilitator, participant, owner) |

### CalibrationExecutiveReviewer
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| id | int64 | No | Yes | Yes | Unique identifier |
| workAssignmentLegacyId | string | No | No | Yes | Work assignment ID of the executive reviewer |

### CalibrationSubjectComment
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| id | int64 | No | Yes | Yes | Unique identifier |
| comment | string | Yes | No | Yes | Comment text |
| createdByWorkAssignmentLegacyId | string | No | No | Yes | Creator's work assignment ID |
| createdDateTime | date-time | No | No | Yes | Creation timestamp |
| lastModifiedDateTime | date-time | Yes | No | Yes | Last modification timestamp |
| modified | boolean | No | No | Yes | Whether the comment has been modified |
| authorizedByWorkAssignmentLegacyId | string | Yes | No | Yes | Authorizer's work assignment ID |
| reasonId | int64 | Yes | No | Yes | Reason identifier |
| reasonLabel | string | Yes | No | Yes | Reason label text |

### CalibrationCompetencyRating
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| competencyId | int64 | No | Yes | Yes | Unique identifier for the competency |
| competencyName | string | Yes | No | Yes | Name of the competency |
| competencySectionId | int64 | Yes | No | Yes | Section ID for the competency |
| competencySectionName | string | Yes | No | Yes | Section name |
| rating | decimal | Yes | No | Yes | Rating value |
| ratingLabel | string | Yes | No | Yes | Rating label text |
| ratingOptions | array | No | No | Yes | Available rating options |

### CalibrationRating
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| ratingType | int32 | No | Yes | Yes | Type of rating |
| rating | decimal | Yes | No | Yes | Rating value |
| ratingLabel | string | Yes | No | Yes | Rating label |
| ratingTypeLabel | string | Yes | No | Yes | Rating type label |

### CalibrationRatingOption
| Field | Type | Nullable | Key | Filterable | Description |
|-------|------|----------|-----|------------|-------------|
| value | double | No | Yes | Yes | Option value |
| label | string | Yes | No | Yes | Option label |

## Get Object Primary Keys
| Entity | Primary Key Field(s) |
|--------|---------------------|
| CalibrationSession | id |
| CalibrationSubject | id |
| CalibrationSessionReviewer | id |
| CalibrationExecutiveReviewer | id |
| CalibrationSubjectComment | id |
| CalibrationCompetencyRating | competencyId |
| CalibrationRating | ratingType |
| CalibrationRatingOption | value |

## Object's Ingestion Type
| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| CalibrationSession | `cdc` | lastModifiedDateTime | Supports $filter and $orderby on lastModifiedDateTime |
| CalibrationSubject | `snapshot` | N/A | Accessed via parent session, no direct lastModifiedDateTime filter |
| CalibrationSessionReviewer | `snapshot` | N/A | No timestamp field for incremental reads |
| CalibrationExecutiveReviewer | `snapshot` | N/A | No timestamp field for incremental reads |
| CalibrationSubjectComment | `cdc` | lastModifiedDateTime | Has lastModifiedDateTime field |
| CalibrationCompetencyRating | `snapshot` | N/A | No timestamp field for incremental reads |
| CalibrationRating | `snapshot` | N/A | No timestamp field for incremental reads |

## Read API for Data Retrieval

### List Calibration Sessions

**Endpoint**: `GET /CalibrationSession`

**Base URL**: `https://{api-server}/odatav4/talent/calibration/CalSession.svc/v1`

**Description**: Query all the calibration sessions that a user can access.

**Query Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | integer | Limit number of results (e.g., 50) |
| $skip | integer | Skip first n items for pagination |
| $filter | string | Filter expression (e.g., `lastModifiedDateTime gt 2024-01-01T00:00:00Z`) |
| $orderby | string | Sort order (e.g., `lastModifiedDateTime desc`) |
| $select | string | Select specific fields |
| $expand | string | Expand related entities (executiveReviewerList, reviewerList, subjectList) |
| $count | boolean | Include total count |
| $search | string | Search items by phrases |

**Supported $orderby Values**:
- activationDate, activationDate desc
- approvalDateTime, approvalDateTime desc
- createdByWorkAssignmentLegacyId, createdByWorkAssignmentLegacyId desc
- id, id desc
- lastModifiedDateTime, lastModifiedDateTime desc
- sessionDate, sessionDate desc
- sessionLocation, sessionLocation desc
- sessionName, sessionName desc
- status, status desc

**Supported $expand Values**:
- `*` (all)
- executiveReviewerList
- reviewerList
- subjectList

**Example Request**:
```http
GET https://apisalesdemo8.successfactors.com/odatav4/talent/calibration/CalSession.svc/v1/CalibrationSession?$top=50&$filter=lastModifiedDateTime gt 2024-01-01T00:00:00Z&$orderby=lastModifiedDateTime asc&$expand=subjectList
Authorization: Basic base64(username@companyId:password)
Accept: application/json
```

**Example Response**:
```json
{
  "@odata.count": 100,
  "value": [
    {
      "id": 42,
      "sessionName": "Q4 2024 Performance Calibration",
      "sessionDate": "2024-10-15",
      "sessionLocation": "Conference Room A",
      "status": 1,
      "activationDate": "2024-10-01",
      "approvalDateTime": null,
      "createdByWorkAssignmentLegacyId": "MGR001",
      "lastModifiedDateTime": "2024-10-05T10:30:00Z",
      "subjectList": [
        {
          "id": 101,
          "workAssignmentLegacyId": "EMP001",
          "calibrated": false,
          "pmFormDataId": 5001,
          "pmuDeeplink": "/sf/liveprofile?user_id=EMP001"
        }
      ]
    }
  ]
}
```

### Get Single Calibration Session

**Endpoint**: `GET /CalibrationSession({id})`

**Example Request**:
```http
GET https://apisalesdemo8.successfactors.com/odatav4/talent/calibration/CalSession.svc/v1/CalibrationSession(42)?$expand=*
Authorization: Basic base64(username@companyId:password)
Accept: application/json
```

### Get Session Reviewers

**Endpoint**: `GET /CalibrationSession({id})/reviewerList`

**Description**: Query all reviewers of the calibration session, including facilitators, participants, and owners.

### Get Session Subjects

**Endpoint**: `GET /CalibrationSession({id})/subjectList`

**Description**: Query all subjects of the calibration session.

**Supported $expand Values for Subjects**:
- `*` (all)
- commentList
- competencyRatingList
- ratingList

### Approve Session (Action)

**Endpoint**: `POST /CalibrationSession({id})/CalSession.svc.approveSession`

**Description**: Finalize a calibration session.

**Response Schema (ApproveSessionResponse)**:
| Field | Type | Description |
|-------|------|-------------|
| isSuccess | boolean | Whether the approval was successful |
| errorCode | string | Error code if failed |
| errorMessage | string | Error message if failed |

## Pagination
- Use `$top` and `$skip` for pagination
- Use `$count=true` to get total record count
- Recommended page size: 50-100 records

**Example Pagination**:
```http
# First page
GET /CalibrationSession?$top=50&$skip=0&$count=true

# Second page
GET /CalibrationSession?$top=50&$skip=50
```

## Incremental Data Retrieval (CDC)
For CalibrationSession, use `lastModifiedDateTime` for incremental data extraction:

```http
GET /CalibrationSession?$filter=lastModifiedDateTime gt 2024-01-01T00:00:00Z&$orderby=lastModifiedDateTime asc&$top=100
```

## Field Type Mapping
| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer/int32 | IntegerType |
| int64 | LongType |
| boolean | BooleanType |
| date | DateType |
| date-time | TimestampType |
| decimal | DecimalType(38, 3) |
| double | DoubleType |
| array | ArrayType |

## Sources and References
- SAP SuccessFactors API Spec: CalSession.json
- SAP Help Portal: [List of API Servers in SAP SuccessFactors](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/bdf1755a26754ab5a1c59e237d0e54f3/af2b8d5437494b12be88fe374eba75b6.html)
