# Continuous Performance Management API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Entity Name | Description |
|-------------|-------------|
| FormReviewFeedbackList | List of review feedback for forms |
| SupporterFeedback | Feedback from supporters/colleagues |
| FormSignatureSection | Signature sections in forms |
| Form360Rater | 360 review rater information |
| Form360ReviewContentDetail | Detailed content of 360 review forms |
| FormRatingScaleValue | Rating scale values used in forms |
| FormPMReviewContentDetail | Performance management review content details |
| FormSummarySection | Form summary sections |
| FormIntroductionSection | Form introduction sections |
| DevGoalAchievements | Development goal achievements |
| FormAuditTrail | Audit trail for form actions |
| FormObjectiveOtherDetailsItem | Additional details items for objectives |
| FormObjectiveSection | Objective sections in forms |
| ContinuousPerformanceUserPermission | User permissions for continuous performance |
| Achievement | Achievement records |
| FormObjectiveOtherDetailsItemValueCell | Value cells for objective details |
| Form360RaterSection | 360 rater sections |
| FormSignature | Individual signatures |
| FormObjective | Individual objectives |
| AchievementDevGoalDetail | Development goal details for achievements |
| FormUserInformationSection | User information sections |
| FormObjectiveComment | Comments on objectives |
| FormCompetency | Competency items |
| AchievementGoalDetail | Goal details for achievements |
| FormReviewFeedback | Review feedback items |
| FormObjectiveOtherDetailsItemCol | Column definitions for objective details |
| FormObjectiveOtherDetails | Other details for objectives |
| GoalAchievementsList | List of goal achievements |
| FormBehaviorRatingComment | Behavior rating comments |
| ActivityStatus | Activity status information |
| FormUserRatingComment | User rating comments |
| FormContent | Form content |
| FormObjectiveDetails | Objective details |
| Activity | Activity records |
| FormRatingScale | Rating scale definitions |
| DevGoalAchievementsList | Development goal achievements list |
| FormCustomElementListValue | Custom element list values |
| GoalDetail | Goal details |
| FormObjCompSummarySection | Objective/Competency summary sections |
| FormCompetencySection | Competency sections |
| ActivityFeedback | Feedback on activities |
| FormItemConfiguration | Form item configurations |
| FormRaterListSection | Rater list sections |
| FormCustomElement | Custom form elements |
| FormSectionConfiguration | Section configurations |
| FormCompetencyBehavior | Competency behaviors |
| GoalAchievements | Goal achievement records |
| FeedbackFlag | Feedback flags |
| FormCustomSection | Custom sections |

## Object Schema

### Achievement

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| achievementId | Int64 | - | Primary key - achievement identifier |
| achievementDate | DateTime (nullable) | - | Date of achievement |
| achievementName | String (nullable) | 4000 | Name of the achievement |
| createdBy | String (nullable) | 255 | User who created the record |
| createdDate | DateTime (nullable) | - | Creation date |
| createdDateTime | DateTime (nullable) | - | Creation date/time |
| lastModifiedBy | String (nullable) | 255 | Last modifier |
| lastModifiedDate | DateTime (nullable) | - | Last modification date |
| lastModifiedDateTime | DateTime (nullable) | - | Last modification date/time |
| lastModifiedDateWithTZ | DateTime (nullable) | - | Last modification with timezone |
| mdfSystemRecordStatus | String (nullable) | 255 | System record status |
| parentExternalId | String (nullable) | 255 | Parent external identifier |
| parentTypeEnum | String (nullable) | 255 | Parent type enumeration |
| reviewed | Boolean (nullable) | - | Whether reviewed |
| snapshot | Boolean (nullable) | - | Snapshot flag |
| subjectUserId | String (nullable) | 100 | Subject user ID |

### Activity

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| activityId | Int64 | - | Primary key - activity identifier |
| activityName | String (nullable) | 4000 | Name of the activity |
| createdBy | String (nullable) | 255 | User who created the record |
| createdDateTime | DateTime (nullable) | - | Creation date/time |
| lastModifiedBy | String (nullable) | 255 | Last modifier |
| lastModifiedDateTime | DateTime (nullable) | - | Last modification date/time |
| mdfSystemRecordStatus | String (nullable) | 255 | System record status |
| subjectUserId | String (nullable) | 100 | Subject user ID |

### SupporterFeedback

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| feedbackId | Int64 | - | Primary key - feedback identifier |
| createdBy | String (nullable) | 255 | User who created the record |
| createdDate | DateTime (nullable) | - | Creation date |
| createdDateTime | DateTime (nullable) | - | Creation date/time |
| lastModifiedBy | String (nullable) | 255 | Last modifier |
| lastModifiedDate | DateTime (nullable) | - | Last modification date |
| lastModifiedDateTime | DateTime (nullable) | - | Last modification date/time |
| lastModifiedDateWithTZ | DateTime (nullable) | - | Last modification with timezone |
| mdfSystemRecordStatus | String (nullable) | 255 | System record status |
| recipientId | String (nullable) | 100 | Recipient user ID |
| requesterId | String (nullable) | 100 | Requester user ID |
| requestDateTime | DateTime (nullable) | - | Request date/time |
| requestText_defaultValue | String (nullable) | 4000 | Request text (default) |
| requestText_localized | String (nullable) | 4000 | Localized request text |
| responseDateTime | DateTime (nullable) | - | Response date/time |
| responseText | String (nullable) | 4000 | Response text |
| subjectUserId | String (nullable) | 100 | Subject user ID |

### FormReviewFeedback

| Field Name | Type | Description |
|------------|------|-------------|
| feedbackId | Int64 | Primary key - feedback identifier |
| appraiserUserEmail | String (nullable) | Appraiser's email |
| appraiserUserId | String (nullable) | Appraiser's user ID |
| companyId | String (nullable) | Company identifier |
| digiCode | String (nullable) | Digital code |
| formDataId | Int64 (nullable) | Associated form data ID |
| pmFeedback | String (nullable) | Performance management feedback |
| requestDate | DateTime (nullable) | Request date |
| requestUserEmail | String (nullable) | Requester's email |
| requestUserId | String (nullable) | Requester's user ID |
| requestUserRole | String (nullable) | Requester's role |
| responseDate | DateTime (nullable) | Response date |
| responseId | String (nullable) | Response identifier |
| subjectUserEmail | String (nullable) | Subject's email |
| subjectUserId | String (nullable) | Subject's user ID |

### FormObjective

| Field Name | Type | Description |
|------------|------|-------------|
| formContentId | Int64 | Composite key - form content ID |
| formDataId | Int64 | Composite key - form data ID |
| itemId | Int64 | Composite key - item ID |
| sectionIndex | Int32 | Composite key - section index |
| category | String (nullable) | Objective category |
| done | String (nullable) | Completion status |
| itemOrder | Int32 (nullable) | Order of the item |
| metric | String (nullable) | Associated metric |
| name | String (nullable) | Objective name |
| state | String (nullable) | Current state |
| stateColor | String (nullable) | State color indicator |
| useMLTRating | Boolean (nullable) | Use MLT rating flag |
| weight | String (nullable) | Objective weight |
| weightKey | String (nullable) | Weight key |

### FormCompetency

| Field Name | Type | Description |
|------------|------|-------------|
| formContentId | Int64 | Composite key - form content ID |
| formDataId | Int64 | Composite key - form data ID |
| itemId | Int64 | Composite key - item ID |
| sectionIndex | Int32 | Composite key - section index |
| category | String (nullable) | Competency category |
| description | String (nullable) | Competency description |
| expectedRating | String (nullable) | Expected rating |
| itemIndex | Int64 (nullable) | Item index |
| name | String (nullable) | Competency name |
| source | String (nullable) | Source of competency |
| weight | String (nullable) | Competency weight |
| weightKey | String (nullable) | Weight key |

### ContinuousPerformanceUserPermission

| Field Name | Type | Description |
|------------|------|-------------|
| permType | String | Primary key component - permission type |
| targetUserId | String | Primary key component - target user ID |
| permStringValue | String | Permission value string |
| hasPermission | Boolean | Whether permission is granted |

## Get Object Primary Keys

### Achievement
- **Primary Key**: `achievementId` (Int64)

### Activity
- **Primary Key**: `activityId` (Int64)

### SupporterFeedback
- **Primary Key**: `feedbackId` (Int64)

### FormReviewFeedback
- **Primary Key**: `feedbackId` (Int64)

### FormObjective
- **Composite Primary Key**: `formContentId` + `formDataId` + `itemId` + `sectionIndex`

### FormCompetency
- **Composite Primary Key**: `formContentId` + `formDataId` + `itemId` + `sectionIndex`

### ContinuousPerformanceUserPermission
- **Composite Primary Key**: `permType` + `targetUserId`

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|----------------|--------|
| Achievement | `cdc` | Has `lastModifiedDateTime` field for incremental tracking |
| Activity | `cdc` | Has `lastModifiedDateTime` field for incremental tracking |
| SupporterFeedback | `cdc` | Has `lastModifiedDateTime` field for incremental tracking |
| FormReviewFeedback | `cdc` | Has `requestDate` and `responseDate` for tracking |
| FormObjective | `snapshot` | No direct modification timestamp |
| FormCompetency | `snapshot` | No direct modification timestamp |
| ContinuousPerformanceUserPermission | `snapshot` | No modification timestamp |
| FormContent | `snapshot` | No modification timestamp |
| FormAuditTrail | `cdc` | Has `auditTrailLastModified` field |
| GoalAchievements | `snapshot` | No modification timestamp |
| DevGoalAchievements | `snapshot` | No modification timestamp |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All Achievements
```http
GET /Achievement
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | Integer | Number of records to return (default: 5) |
| $skip | Integer | Number of records to skip for pagination |
| $filter | String | OData filter expression |
| $orderby | Array | Fields to sort by |
| $select | Array | Fields to return |
| $expand | Array | Related entities to expand |
| $search | String | Search phrases |
| $count | Boolean | Include total count |

**Example Request - Incremental (CDC):**
```http
GET https://{api-server}/odata/v2/Achievement?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc&$top=1000
```

### Get All Activities
```http
GET /Activity
```

**Example Request - Incremental (CDC):**
```http
GET https://{api-server}/odata/v2/Activity?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc&$top=1000
```

### Get All Supporter Feedback
```http
GET /SupporterFeedback
```

### Get All Form Review Feedback
```http
GET /FormReviewFeedback
```

### Get Form Review Feedback List by Form
```http
GET /FormReviewFeedbackList({formDataId})
```

### Get All Form Objectives
```http
GET /FormObjective
```

### Get All Form Competencies
```http
GET /FormCompetency
```

### Get Continuous Performance User Permissions
```http
GET /ContinuousPerformanceUserPermission
```

### Get Form Content
```http
GET /FormContent
```

### Get Form Audit Trail
```http
GET /FormAuditTrail
```

**Example Request:**
```http
GET https://{api-server}/odata/v2/FormAuditTrail?$filter=auditTrailLastModified gt datetime'2024-01-01T00:00:00'
```

### Get Goal Achievements
```http
GET /GoalAchievements(goalId='{goalId}',subjectUserId='{subjectUserId}')
```

### Get Development Goal Achievements
```http
GET /DevGoalAchievements(goalId='{goalId}',subjectUserId='{subjectUserId}')
```

### Pagination Strategy
- Use `$top` and `$skip` for offset-based pagination
- Default page size: 5 records
- Recommended page size: 1000 records for bulk operations
- For CDC entities: Order by modification timestamp fields

### Response Format
```json
{
  "d": {
    "results": [
      {
        "achievementId": "12345",
        "achievementName": "Q1 Sales Target Met",
        "subjectUserId": "employee001",
        "lastModifiedDateTime": "/Date(1492098664000)/"
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
| Edm.Float | FloatType |

## Sources and References
- SAP SuccessFactors API Spec: PMGMContinuousPerformanceManagement.json
- SAP Help Portal: [Continuous Performance Management](https://help.sap.com/viewer/28bc3c8e3f214ab487ec51b1b8709adc/latest)
