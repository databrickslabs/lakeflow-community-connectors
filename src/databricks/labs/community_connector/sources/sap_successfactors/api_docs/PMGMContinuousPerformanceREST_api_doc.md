# Continuous Performance Management API Documentation

## Authorization

- **Method**: HTTP Bearer Token Authentication
- **Security Scheme**: BearerAuth
- **Headers**:
  - `Authorization`: Bearer <token>
  - Alternative: `Basic <Base64 encoded (user@company:password)>`

## Object List

| Resource | Tag | Description |
|----------|-----|-------------|
| ActivityStatuses | ActivityStatuses | Activity status definitions |
| ActivityUpdates | ActivityUpdates | Comments and updates on activities |
| Activities | Activities | Performance activities linked to goals |
| Achievements | Achievements | Completed activities marked as achievements |
| UserPermissions | UserPermissions | CPM permissions for users |

### Endpoints

#### Activity Statuses
| Path | Methods | Description |
|------|---------|-------------|
| `/activityStatuses` | GET | Get all activity statuses |
| `/activityStatuses/{statusRecordId}` | GET | Get single activity status |

#### Activity Updates
| Path | Methods | Description |
|------|---------|-------------|
| `/activityUpdates/{activityUpdateRecordId}` | DELETE, PATCH | Delete or edit an activity update |
| `/activities/{activityRecordId}/activityUpdates` | GET, POST | Get or create activity updates |

#### Activities
| Path | Methods | Description |
|------|---------|-------------|
| `/activities` | GET, POST | List or create activities |
| `/activities/{activityRecordId}` | GET, PATCH, DELETE | Get, update, or delete an activity |

#### Achievements
| Path | Methods | Description |
|------|---------|-------------|
| `/achievements` | GET | List achievements |

#### User Permissions
| Path | Methods | Description |
|------|---------|-------------|
| `/userPermissions` | GET | Get CPM permissions |

## Object Schema

### activityStatus

| Field | Type | Key | Description |
|-------|------|-----|-------------|
| activityStatusRecordId | string | Yes (x-key) | Status record identifier |
| statusName | string | No | Name of the status (e.g., "On Target", "Completed") |
| colorRGBCode | string | No | Color code for status (e.g., "#ffffff") |
| createAchievement | boolean | No | Whether this status creates an achievement |
| defaultStatus | boolean | No | Whether this is the default status |
| deleted | boolean | No | Whether the status is deleted |
| priority | string | No | Priority of the status |
| createdBy | string | No | User who created the status |
| createdDate | string (date-time) | No | Creation timestamp |
| lastModifiedBy | string | No | User who last modified |
| lastModifiedDateTime | string (date-time) | No | Last modification timestamp |

### activities

| Field | Type | Key | Insertable | Description |
|-------|------|-----|------------|-------------|
| activityRecordId | string | Yes (x-key) | No | Activity record identifier (max 255 chars) |
| name | string | No | Yes | Activity name (max 4000 chars) |
| goalId | integer (int64) | No | Yes | Linked Performance Goal ID |
| devGoalId | integer (int64) | No | Yes | Linked Development Goal ID |
| statusRecordId | string | No | Yes | Activity Status record ID (max 38 chars) |
| subjectUserAssignmentUuid | string | No | No | Subject user's assignment UUID (max 32 chars) |
| meetingOwnerAssignmentUuid | string | No | No | Meeting owner's assignment UUID |
| isAchievement | boolean | No | Yes | Whether activity is an achievement |
| achievementDate | string (date) | No | Yes | Date marked as achievement |
| createdDate | string (date-time) | No | No | Creation timestamp |
| lastModifiedDate | string (date-time) | No | No | Last modification timestamp |
| createdBy | string | No | No | User who created |
| lastModifiedBy | string | No | No | User who last modified |
| status | object (activityStatus) | No | No | Nested status object |

### activityUpdates

| Field | Type | Key | Insertable | Description |
|-------|------|-----|------------|-------------|
| activityUpdateRecordId | string | Yes (x-key) | No | Update record identifier |
| commentContent | string | No | Yes | Comment text |
| commenterAssignmentUUID | string | No | No | Commenter's assignment UUID |
| createdDate | string (date-time) | No | No | Creation timestamp |
| lastModifiedDate | string (date-time) | No | No | Last modification timestamp |
| createdBy | string | No | No | User who created |
| lastModifiedBy | string | No | No | User who last modified |

### achievements

| Field | Type | Key | Insertable | Description |
|-------|------|-----|------------|-------------|
| achievementRecordId | string | Yes (x-key) | No | Achievement record identifier |
| activityRecordId | string | Yes (x-key) | No | Linked activity record ID |
| name | string | No | Yes | Achievement name (max 4000 chars) |
| subjectUserAssignmentUuid | string | No | No | Subject user's assignment UUID |
| achievementDate | string (date) | No | Yes | Achievement date |
| goalId | integer (int64) | No | Yes | Linked Performance Goal ID |
| devGoalId | integer (int64) | No | Yes | Linked Development Goal ID |
| createdDate | string (date-time) | No | No | Creation timestamp |
| lastModifiedDate | string (date-time) | No | No | Last modification timestamp |
| createdBy | string | No | No | User who created |
| lastModifiedBy | string | No | No | User who last modified |

### userPermissions

| Field | Type | Key | Description |
|-------|------|-----|-------------|
| targetUserAssignmentUUID | string | Yes (x-key) | Target user's assignment UUID |
| hasPermission | boolean | No | Whether permission is granted |
| permissionType | string | No | Entity type: ACTIVITY or ACHIEVEMENT |
| permisssionValue | string | No | Permission level: VIEW or EDIT |

## Get Object Primary Keys

| Entity | Primary Key Field(s) |
|--------|---------------------|
| activityStatus | activityStatusRecordId |
| activities | activityRecordId |
| activityUpdates | activityUpdateRecordId |
| achievements | achievementRecordId, activityRecordId |
| userPermissions | targetUserAssignmentUUID |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| activityStatuses | `snapshot` | Reference data, includes `lastModifiedDate` but no filter support |
| activities | `cdc` | Has `lastModifiedDate` field for tracking changes |
| activityUpdates | `cdc` | Has `lastModifiedDate` field for tracking changes |
| achievements | `cdc` | Has `lastModifiedDate` field for tracking changes |
| userPermissions | `snapshot` | Permission check API, no timestamp filtering |

**Note**: While entities have `lastModifiedDate` fields, the API does not explicitly document filter parameters for timestamp-based queries. Implementation should verify CDC capability during testing.

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/rest/talent/continuousperformance/v1
```

### Get Activity Statuses
```
GET /activityStatuses
```

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| includeDeleted | boolean | No | false | Include deleted statuses |

**Example Request:**
```http
GET https://api.successfactors.com/rest/talent/continuousperformance/v1/activityStatuses?includeDeleted=false
Authorization: Bearer <token>
```

### Get Activities
```
GET /activities
```

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| subjectUserAssignmentUuid | string | No | - | Filter by user UUID |
| $top | integer | No | 200 | Page size |
| $skip | integer | No | 0 | Records to skip |

**Example Request:**
```http
GET https://api.successfactors.com/rest/talent/continuousperformance/v1/activities?subjectUserAssignmentUuid=E46A4DEEFEB74B9593272DC8BA1E54A5&$top=100&$skip=0
Authorization: Bearer <token>
```

### Get Single Activity
```
GET /activities/{activityRecordId}
```

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| activityRecordId | string | Yes | Activity record identifier |

### Get Activity Updates
```
GET /activities/{activityRecordId}/activityUpdates
```

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| activityRecordId | string | Yes | Activity record identifier |

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| $top | integer | No | 10 | Page size |
| $skip | integer | No | 0 | Records to skip |

### Get Achievements
```
GET /achievements
```

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| targetUserAssignmentUuid | string | No | - | Filter by user UUID |
| $top | integer | No | 200 | Page size |
| $skip | integer | No | 0 | Records to skip |

### Get User Permissions
```
GET /userPermissions
```

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| targetUserAssignmentUuid | string | No | Check permissions for target user |

### Pagination

Uses `$top` and `$skip` for offset-based pagination:
- `$top`: Number of records to return (default varies by endpoint)
- `$skip`: Number of records to skip (0-based)

**Example:**
```
GET /activities?$top=100&$skip=0    # First page
GET /activities?$top=100&$skip=100  # Second page
```

### Example Response (Activities)
```json
{
  "value": [
    {
      "activityRecordId": "5C3B8904AF5E451EB79EF8770A7245F4",
      "goalId": 2515,
      "devGoalId": 2444,
      "statusRecordId": "36938E41800F49049410FF8D9C1F189E",
      "name": "Test Activity",
      "meetingOwnerAssignmentUuid": null,
      "subjectUserAssignmentUuid": "E46A4DEEFEB74B9593272DC8BA1E54A5",
      "createdDate": "2023-06-20 14:34:13",
      "lastModifiedDate": "2023-06-20 14:34:38",
      "status": {
        "statusName": "On Target",
        "activityStatusRecordId": "36938E41800F49049410FF8D9C1F189E",
        "colorRGBCode": "#78f73d"
      },
      "isAchievement": true,
      "achievementDate": "2023-06-21"
    }
  ]
}
```

### Response Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 201 | Created (POST) |
| 204 | No Content (DELETE) |
| 400 | Bad Request |
| 403 | Forbidden - No permission |
| 404 | Not Found |
| 500 | Internal Server Error |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer (int32) | IntegerType |
| integer (int64) | LongType |
| boolean | BooleanType |
| string (date) | DateType |
| string (date-time) | TimestampType |
| array | ArrayType |
| object | StructType |

## Sources and References

- **SAP SuccessFactors API Spec**: PMGMContinuousPerformanceREST.json
- **SAP Help Portal**: [Continuous Performance Management REST API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PERFORMANCE_AND_GOALS/80db66fe42a34ee4b248f9f46df6156c/31e570c514fd48db985685758091f027.html)
