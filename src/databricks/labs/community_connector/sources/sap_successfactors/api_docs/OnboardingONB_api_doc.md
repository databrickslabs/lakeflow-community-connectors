# Onboarding 1.0 API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
The following entities are available in this API:

| Entity Name | Description |
|------------|-------------|
| OnboardingMeetingActivity | Meeting activities for onboarding process |
| OnboardingEquipmentActivity | Equipment selection activities |
| OnboardingGoal | Individual onboarding goals |
| OnboardingGoalCategory | Goal categories for organizing goals |
| OnboardingNewHireActivitiesStep | Steps in the new hire activities process |
| OnboardingEquipmentTypeValue | Equipment type values/options |
| OnboardingMeetingEvent | Meeting events for candidates |
| OnboardingGoalActivity | Goal-related activities |
| OnboardingCandidateInfo | Candidate information details |
| OnboardingEquipmentType | Types of equipment available |
| OnboardingProcess | Main onboarding process entity |
| OnboardingEquipment | Equipment items selected for candidates |

## Object Schema

### OnboardingMeetingActivity
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| OnboardingNewHireActivitiesStep_processStepId | string | int64 | No | Process step ID (Key) |
| OnboardingProcess_onboardingProcessId | string | int64 | No | Onboarding process ID (Key) |
| activityId | string | int64 | No | Activity ID (Key) |
| activityStatus | string | - | Yes | Status of the activity |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| eventTriggered | boolean | - | Yes | Flag if event was triggered |
| externalName | string | - | Yes | External name |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| optional | boolean | - | Yes | Flag if activity is optional |
| meetingEvent | collection | - | - | Related meeting events |

### OnboardingEquipmentActivity
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| OnboardingNewHireActivitiesStep_processStepId | string | int64 | No | Process step ID (Key) |
| OnboardingProcess_onboardingProcessId | string | int64 | No | Onboarding process ID (Key) |
| activityId | string | int64 | No | Activity ID (Key) |
| activityStatus | string | - | Yes | Status of the activity |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| eventTriggered | boolean | - | Yes | Flag if event was triggered |
| externalName | string | - | Yes | External name |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| optional | boolean | - | Yes | Flag if activity is optional |
| equipment | collection | - | - | Related equipment items |

### OnboardingGoal
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| OnboardingGoalActivity_activityId | string | int64 | No | Goal activity ID (Key) |
| OnboardingGoalCategory_externalCode | string | int64 | No | Goal category external code (Key) |
| OnboardingNewHireActivitiesStep_processStepId | string | int64 | No | Process step ID (Key) |
| OnboardingProcess_onboardingProcessId | string | int64 | No | Onboarding process ID (Key) |
| goalId | string | int64 | No | Goal ID (Key) |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| text | string | - | Yes | Goal text content |

### OnboardingGoalCategory
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| OnboardingGoalActivity_activityId | string | int64 | No | Goal activity ID (Key) |
| OnboardingNewHireActivitiesStep_processStepId | string | int64 | No | Process step ID (Key) |
| OnboardingProcess_onboardingProcessId | string | int64 | No | Onboarding process ID (Key) |
| externalCode | string | int64 | No | External code (Key) |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| dueDaysAfterStart | string | int64 | Yes | Days after start for due date |
| goalCategoryId | string | - | Yes | Goal category ID |
| goalCategoryName | string | - | Yes | Goal category name |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| goals | collection | - | - | Related goals |

### OnboardingCandidateInfo
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| applicantId | string | - | No | Applicant ID (Key) |
| candidateId | string | - | Yes | Candidate ID |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| department | string | - | Yes | Department |
| division | string | - | Yes | Division |
| email | string | - | Yes | Email address |
| fName | string | - | Yes | First name |
| lName | string | - | Yes | Last name |
| hireDate | string | - | Yes | Hire date |
| hired | boolean | - | Yes | Hired flag |
| hrManagerId | string | - | Yes | HR manager ID |
| internalHire | boolean | - | Yes | Internal hire flag |
| jobReqId | string | - | Yes | Job requisition ID |
| jobTitle | string | - | Yes | Job title |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| location | string | - | Yes | Location |
| managerId | string | - | Yes | Manager ID |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| userId | string | - | Yes | User ID |
| workCountry | string | - | Yes | Work country |

### OnboardingProcess
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| onboardingProcessId | string | int64 | No | Onboarding process ID (Key) |
| candidateInfo | string | - | Yes | Candidate info reference |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| externalName | string | - | Yes | External name |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| processConfig | string | - | Yes | Process configuration |
| processStatus | string | - | Yes | Process status |
| candidateInfoNav | navigation | - | - | Related candidate info |
| newHireActivitiesStep | navigation | - | - | Related activities step |

### OnboardingMeetingEvent
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| OnboardingMeetingActivity_activityId | string | int64 | No | Meeting activity ID (Key) |
| OnboardingNewHireActivitiesStep_processStepId | string | int64 | No | Process step ID (Key) |
| OnboardingProcess_onboardingProcessId | string | int64 | No | Onboarding process ID (Key) |
| externalCode | string | - | No | External code (Key) |
| agenda | string | - | Yes | Meeting agenda |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| endDateTime | string | DateTime | Yes | Meeting end time |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| location | string | - | Yes | Meeting location |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| participantUserId1-5 | string | - | Yes | Participant user IDs |
| send | boolean | - | Yes | Send notification flag |
| startDateTime | string | DateTime | Yes | Meeting start time |
| subject | string | - | Yes | Meeting subject |

## Get Object Primary Keys

| Entity | Primary Key Field(s) |
|--------|---------------------|
| OnboardingMeetingActivity | OnboardingNewHireActivitiesStep_processStepId, OnboardingProcess_onboardingProcessId, activityId |
| OnboardingEquipmentActivity | OnboardingNewHireActivitiesStep_processStepId, OnboardingProcess_onboardingProcessId, activityId |
| OnboardingGoal | OnboardingGoalActivity_activityId, OnboardingGoalCategory_externalCode, OnboardingNewHireActivitiesStep_processStepId, OnboardingProcess_onboardingProcessId, goalId |
| OnboardingGoalCategory | OnboardingGoalActivity_activityId, OnboardingNewHireActivitiesStep_processStepId, OnboardingProcess_onboardingProcessId, externalCode |
| OnboardingNewHireActivitiesStep | OnboardingProcess_onboardingProcessId, processStepId |
| OnboardingEquipmentTypeValue | code |
| OnboardingMeetingEvent | OnboardingMeetingActivity_activityId, OnboardingNewHireActivitiesStep_processStepId, OnboardingProcess_onboardingProcessId, externalCode |
| OnboardingGoalActivity | OnboardingNewHireActivitiesStep_processStepId, OnboardingProcess_onboardingProcessId, activityId |
| OnboardingCandidateInfo | applicantId |
| OnboardingEquipmentType | code |
| OnboardingProcess | onboardingProcessId |
| OnboardingEquipment | OnboardingEquipmentActivity_activityId, OnboardingNewHireActivitiesStep_processStepId, OnboardingProcess_onboardingProcessId, equipmentId |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| OnboardingMeetingActivity | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingEquipmentActivity | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingGoal | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingGoalCategory | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingNewHireActivitiesStep | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingEquipmentTypeValue | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingMeetingEvent | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingGoalActivity | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingCandidateInfo | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingEquipmentType | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingProcess | `cdc` | Has lastModifiedDateTime for incremental filtering |
| OnboardingEquipment | `cdc` | Has lastModifiedDateTime for incremental filtering |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All Entities
```http
GET /OnboardingMeetingActivity
GET /OnboardingEquipmentActivity
GET /OnboardingGoal
GET /OnboardingGoalCategory
GET /OnboardingNewHireActivitiesStep
GET /OnboardingEquipmentTypeValue
GET /OnboardingMeetingEvent
GET /OnboardingGoalActivity
GET /OnboardingCandidateInfo
GET /OnboardingEquipmentType
GET /OnboardingProcess
GET /OnboardingEquipment
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

**OnboardingProcess:**
```http
GET /OnboardingProcess({onboardingProcessId})
```

**OnboardingCandidateInfo:**
```http
GET /OnboardingCandidateInfo('{applicantId}')
```

**OnboardingMeetingActivity:**
```http
GET /OnboardingMeetingActivity(OnboardingNewHireActivitiesStep_processStepId={processStepId},OnboardingProcess_onboardingProcessId={processId},activityId={activityId})
```

### $expand Options by Entity
| Entity | Expand Options |
|--------|---------------|
| OnboardingMeetingActivity | meetingEvent |
| OnboardingEquipmentActivity | equipment |
| OnboardingGoalCategory | goals |
| OnboardingNewHireActivitiesStep | equipmentActivity, goalActivity, meetingActivity |
| OnboardingEquipmentTypeValue | typeNav |
| OnboardingGoalActivity | goalCategories |
| OnboardingProcess | candidateInfoNav, newHireActivitiesStep |
| OnboardingEquipment | typeNav, valueNav |

### Example Requests

**List all onboarding processes with expanded candidate info:**
```http
GET https://{api-server}/odata/v2/OnboardingProcess?$expand=candidateInfoNav
```

**Filter by lastModifiedDateTime for incremental sync:**
```http
GET https://{api-server}/odata/v2/OnboardingProcess?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get candidate info with specific fields:**
```http
GET https://{api-server}/odata/v2/OnboardingCandidateInfo?$select=applicantId,fName,lName,email,hireDate
```

**Get meeting activities for a specific process:**
```http
GET https://{api-server}/odata/v2/OnboardingMeetingActivity?$filter=OnboardingProcess_onboardingProcessId eq 12345
```

### Pagination
Use `$top` and `$skip` for pagination:
```http
GET /OnboardingProcess?$top=100&$skip=0
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "onboardingProcessId": "12345",
        "candidateInfo": "APP001",
        "processStatus": "InProgress",
        "createdDateTime": "/Date(1492098664000)/",
        "lastModifiedDateTime": "/Date(1492098664000)/"
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
| string (maxLength) | StringType |

## Sources and References
- SAP SuccessFactors API Spec: OnboardingONB.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/a1415b9fb54a497eb7898ff385994faa.html
