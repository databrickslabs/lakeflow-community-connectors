# Goal Plan API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
The following entities are available in this API (grouped by category):

### Goal Entities
| Entity Name | Description |
|------------|-------------|
| Goal_1 through Goal_8 | Goal records for different plan types |
| Goal_101 | Goal records for plan type 101 |
| SimpleGoal | Simplified goal entity |
| TeamGoal_1, TeamGoal_5, TeamGoal_7 | Team goals for different plan types |
| DevGoal_2001, DevGoal_2002 | Development goals |
| SimpleDevGoal | Simplified development goal |

### Goal Plan Entities
| Entity Name | Description |
|------------|-------------|
| GoalPlanTemplate | Goal plan template definitions |
| DevGoalPlanTemplate | Development goal plan templates |
| GoalPlanState | State of goal plans |

### Permission Entities
| Entity Name | Description |
|------------|-------------|
| GoalPermission_1 through GoalPermission_8 | Goal permissions for different plan types |
| GoalPermission_101 | Goal permissions for plan type 101 |
| GoalTaskPermission_1 through GoalTaskPermission_8 | Task permissions |
| GoalMilestonePermission_1 through GoalMilestonePermission_7 | Milestone permissions |
| DevGoalPermission_2001, DevGoalPermission_2002 | Development goal permissions |

### Task & Milestone Entities
| Entity Name | Description |
|------------|-------------|
| GoalTask_1 through GoalTask_8 | Goal tasks |
| GoalTask_101 | Goal tasks for plan type 101 |
| GoalMilestone_1 through GoalMilestone_7 | Goal milestones |

### Other Entities
| Entity Name | Description |
|------------|-------------|
| GoalComment_1, GoalComment_5 | Goal comments |
| GoalEnum, DevGoalEnum | Enumeration values |
| GoalWeight | Goal weight configuration |
| GoalTarget_7 | Goal targets |
| GoalMetricLookup_7 | Metric lookup values |
| DevGoalDetail | Development goal details |
| DevGoalCompetency | Development goal competencies |
| AssignTeamGoal | Team goal assignments |
| Form360ParticipantDetail | 360 form participant details |
| Form360ParticipantSection | 360 form sections |
| Form360ParticipantColumn | 360 form columns |
| Form360Participant | 360 form participants |
| Form360ParticipantCategory | 360 participant categories |
| Form360ParticipantConfig | 360 participant configurations |
| FormRouteStep | Form routing steps |
| FormRouteMap | Form route mappings |
| FormRouteSubStep | Form routing sub-steps |

## Object Schema

### GoalWeight
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| planId | string | int64 | No | Plan ID (Key) |
| type | string | - | No | Weight type (Key) |
| maxValue | number | double | Yes | Maximum weight value |
| minValue | number | double | Yes | Minimum weight value |

### Form360ParticipantDetail
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| formContentId | string | int64 | No | Form content ID (Key) |
| formDataId | string | int64 | No | Form data ID (Key) |
| participantId | string | - | No | Participant ID (Key) |
| columnKey | string | - | No | Column key (Key) |
| columnValue | string | - | Yes | Column value |

### GoalPermission (Common Fields across versions)
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| id | string | int64 | No | Permission ID (Key) |
| category | integer | uint8 | Yes | Category permission level |
| create | boolean | - | Yes | Create permission |
| delete | boolean | - | Yes | Delete permission |
| edit | boolean | - | Yes | Edit permission |
| view | boolean | - | Yes | View permission |
| due | integer | uint8 | Yes | Due date permission level |
| flag | integer | int32 | Yes | Flag value |
| metric | integer | uint8 | Yes | Metric permission level |
| name | integer | uint8 | Yes | Name permission level |
| privateAccess | boolean | - | Yes | Private access flag |
| start | integer | uint8 | Yes | Start date permission level |
| state | integer | uint8 | Yes | State permission level |
| tasks | integer | uint8 | Yes | Tasks permission level |
| type | string | - | Yes | Permission type |
| userId | string | - | Yes | User ID |
| weight | integer | uint8 | Yes | Weight permission level |

### GoalTask (Common Fields across versions)
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| id | string | int64 | No | Task ID (Key) |
| date | string | DateTime | Yes | Task date |
| flag | string | int64 | Yes | Flag value |
| index | integer | int32 | Yes | Task index/order |
| lastModified | string | DateTime | Yes | Last modification timestamp |
| modifier | string | - | Yes | User who last modified |
| objId | string | int64 | Yes | Related object ID |
| target | string | - | Yes | Task target |

### GoalComment (Common Fields)
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| id | string | int64 | No | Comment ID (Key) |
| commentator | string | - | Yes | User who made the comment |
| content | string | - | Yes | Comment content |
| lastModified | string | DateTime | Yes | Last modification timestamp |
| objId | string | int64 | Yes | Related object ID |

## Get Object Primary Keys

| Entity | Primary Key Field(s) |
|--------|---------------------|
| GoalWeight | planId, type |
| Form360ParticipantDetail | formContentId, formDataId, participantId, columnKey |
| GoalPermission_* | id |
| GoalTask_* | id |
| GoalComment_* | id |
| GoalMilestone_* | id |
| Goal_* | id |
| DevGoal_* | id |
| GoalPlanTemplate | id |
| GoalPlanState | planId, stateId, userId |

## Object's Ingestion Type

| Entity Category | Ingestion Type | Reason |
|----------------|---------------|--------|
| Goal entities | `cdc` | Have lastModified/lastModifiedDateTime for incremental filtering |
| GoalTask entities | `cdc` | Have lastModified field for incremental filtering |
| GoalComment entities | `cdc` | Have lastModified field for incremental filtering |
| GoalPermission entities | `snapshot` | Permission data typically requires full refresh |
| GoalMilestone entities | `cdc` | Have lastModified field for incremental filtering |
| GoalPlanTemplate | `snapshot` | Template data typically requires full refresh |
| Form360 entities | `snapshot` | Form data typically requires full refresh |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All Entities Examples
```http
GET /GoalWeight
GET /GoalPlanTemplate
GET /Goal_1
GET /GoalTask_1
GET /GoalComment_1
GET /GoalPermission_1
GET /DevGoal_2001
GET /TeamGoal_1
GET /Form360ParticipantDetail
```

**Common Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $filter | string | Filter items by property values |
| $search | string | Search items by search phrases |
| $count | boolean | Include count of items |
| $orderby | array | Order items by property values |
| $select | array | Select properties to be returned |
| $expand | array | Expand related entities |

### Get Single Entity by Key Examples

**GoalWeight:**
```http
GET /GoalWeight(planId={planId},type='{type}')
```

**Goal by ID:**
```http
GET /Goal_1({id})
```

**GoalTask by ID:**
```http
GET /GoalTask_1({id})
```

### $expand Options by Entity
| Entity | Expand Options |
|--------|---------------|
| Goal_1 | permissionNav, tasksNav, milestonesNav, commentsNav |
| GoalPermission_1 | milestonesPermissionNav, tasksPermissionNav |
| DevGoal_2001 | permissionNav, competenciesNav |
| TeamGoal_1 | assignedGoalsNav |

### Example Requests

**List all goals for a user:**
```http
GET https://{api-server}/odata/v2/Goal_1?$filter=userId eq 'user123'
```

**Filter by lastModified for incremental sync:**
```http
GET https://{api-server}/odata/v2/Goal_1?$filter=lastModified gt datetime'2024-01-01T00:00:00'&$orderby=lastModified asc
```

**Get goals with expanded permissions:**
```http
GET https://{api-server}/odata/v2/Goal_1?$expand=permissionNav
```

**Get goal plan templates:**
```http
GET https://{api-server}/odata/v2/GoalPlanTemplate?$select=id,name,description
```

**Get comments for a specific goal:**
```http
GET https://{api-server}/odata/v2/GoalComment_1?$filter=objId eq 12345L
```

### Pagination
Use `$top` and `$skip` for pagination:
```http
GET /Goal_1?$top=100&$skip=0
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "id": "12345",
        "name": "Q1 Sales Target",
        "category": "Sales",
        "state": "Active",
        "due": "/Date(1704067200000)/",
        "lastModified": "/Date(1492098664000)/"
      }
    ]
  }
}
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| string (int64) | LongType |
| string (DateTime) | TimestampType |
| boolean | BooleanType |
| integer (int32) | IntegerType |
| integer (uint8) | ShortType |
| number (double) | DoubleType |

## Sources and References
- SAP SuccessFactors API Spec: PerformanceandGoalsPMGM.json
- SAP Help Portal: https://help.sap.com/viewer/28bc3c8e3f214ab487ec51b1b8709adc/latest
