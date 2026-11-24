# Onboarding API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
1. **ONB2EquipmentTypeValue** - Equipment Type Values for onboarding
2. **ComplianceFormDataFieldValue** - Compliance Form Data Field Values
3. **ONB2Process** - Onboarding Process
4. **ONB2DataCollectionUserConfig** - Data Collection User Configuration
5. **ONB2ProcessTask** - Onboarding Process Tasks
6. **ONB2EquipmentType** - Equipment Types
7. **ONB2ProcessTrigger** - Process Triggers
8. **ComplianceUserFormData** - Compliance User Form Data
9. **ONB2EquipmentActivity** - Equipment Activities
10. **ComplianceProcess** - Compliance Process
11. **ComplianceProcessTask** - Compliance Process Tasks
12. **AssignedComplianceForm** - Assigned Compliance Forms
13. **ComplianceDocumentFlow** - Compliance Document Flow
14. **ComplianceFormSignature** - Compliance Form Signatures
15. **ComplianceFormData** - Compliance Form Data

## Object Schema

### ONB2Process

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| processId | Edm.String | 128 | Yes | Process ID (Primary Key) |
| activitiesConfig | Edm.String | 128 | No | Activities configuration |
| activitiesStatus | Edm.String | 255 | No | Activities status |
| bpeProcessInstanceId | Edm.String | 128 | No | BPE process instance ID |
| cancelEventReason | Edm.String | 32 | No | Cancel event reason |
| cancelOffboardingReason | Edm.String | 128 | No | Cancel offboarding reason |
| cancelOnboardingReason | Edm.String | 128 | No | Cancel onboarding reason |
| cancellationComment | Edm.String | 255 | No | Cancellation comment |
| cancellationDate | Edm.DateTime | - | No | Cancellation date |
| cancelledDueToRestart | Edm.Boolean | - | No | Cancelled due to restart flag |
| createdBy | Edm.String | 100 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| employeePersonId | Edm.Int64 | - | No | Employee person ID |
| endDate | Edm.DateTime | - | No | End date |
| lastModifiedBy | Edm.String | 100 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| locale | Edm.String | 128 | No | Locale |
| manager | Edm.String | 100 | No | Manager |
| managerPersonId | Edm.Int64 | - | No | Manager person ID |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| onboardingHireStatus | Edm.String | 255 | No | Hire status |
| onboardingHiredDate | Edm.DateTime | - | No | Hired date |
| onboardingInternalHire | Edm.Boolean | - | No | Internal hire flag |
| processRestarted | Edm.Boolean | - | No | Process restarted flag |
| processStatus | Edm.String | 255 | No | Process status |
| processTrigger | Edm.String | 128 | No | Process trigger |
| processType | Edm.String | 255 | No | Process type |
| processVariant | Edm.String | 128 | No | Process variant |
| startDate | Edm.DateTime | - | No | Start date |
| targetDate | Edm.DateTime | - | No | Target date |
| targetSystem | Edm.String | 128 | No | Target system |
| user | Edm.String | 100 | No | User |

### ONB2ProcessTask

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| ONB2Process_processId | Edm.String | 128 | Yes | Parent process ID (Part of composite key) |
| taskId | Edm.String | 128 | Yes | Task ID (Part of composite key) |
| completedBy | Edm.String | 100 | No | Completed by |
| createdBy | Edm.String | 100 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| endDate | Edm.DateTime | - | No | End date |
| lastModifiedBy | Edm.String | 100 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| responsibilityConfig | Edm.String | 128 | No | Responsibility configuration |
| startDate | Edm.DateTime | - | No | Start date |
| status | Edm.String | 128 | No | Status |
| type | Edm.String | 128 | No | Type |

### ONB2ProcessTrigger

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| triggerId | Edm.String | 38 | Yes | Trigger ID (Primary Key) |
| atsApplicationId | Edm.String | 255 | No | ATS application ID |
| atsUserId | Edm.String | 100 | No | ATS user ID |
| bpeProcessInstanceId | Edm.String | 128 | No | BPE process instance ID |
| createdBy | Edm.String | 100 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| hireType | Edm.String | 128 | No | Hire type |
| lastModifiedBy | Edm.String | 100 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| rcmApplicationId | Edm.Int64 | - | No | RCM application ID |
| rcmCandidateId | Edm.Int64 | - | No | RCM candidate ID |
| rcmCompany | Edm.String | 255 | No | RCM company |
| rcmHiringMgr | Edm.String | 255 | No | RCM hiring manager |
| rcmJobReqId | Edm.Int64 | - | No | RCM job requisition ID |
| rcmOfferId | Edm.Int64 | - | No | RCM offer ID |
| rcmPrimaryEmail | Edm.String | 255 | No | RCM primary email |
| rcmStartDate | Edm.String | 255 | No | RCM start date |
| rehireUser | Edm.String | 100 | No | Rehire user |
| triggerStatus | Edm.String | 128 | No | Trigger status |
| triggerType | Edm.String | 128 | No | Trigger type |

### ONB2EquipmentType

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| code | Edm.String | 128 | Yes | Equipment type code (Primary Key) |
| createdBy | Edm.String | 100 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| description_defaultValue | Edm.String | 300 | No | Default description |
| description_en_US | Edm.String | 300 | No | English (US) description |
| description_localized | Edm.String | 300 | No | Localized description |
| inUse | Edm.String | 128 | No | In use flag |
| lastModifiedBy | Edm.String | 100 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| ui5StandardIconId | Edm.String | 255 | No | UI5 standard icon ID |

### ONB2EquipmentTypeValue

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| code | Edm.String | 128 | Yes | Equipment type value code (Primary Key) |
| createdBy | Edm.String | 100 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| description_defaultValue | Edm.String | 300 | No | Default description |
| description_en_US | Edm.String | 300 | No | English (US) description |
| description_localized | Edm.String | 300 | No | Localized description |
| inUse | Edm.String | 128 | No | In use flag |
| lastModifiedBy | Edm.String | 100 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| type | Edm.String | 128 | No | Parent equipment type |

### ONB2EquipmentActivity

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| activityId | Edm.String | 128 | Yes | Activity ID (Primary Key) |
| activityStatus | Edm.String | 255 | No | Activity status |
| activityStatusDate | Edm.DateTime | - | No | Activity status date |
| activityTitle | Edm.String | 255 | No | Activity title |
| activityType | Edm.String | 128 | No | Activity type |
| createdBy | Edm.String | 100 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| dueDate | Edm.DateTime | - | No | Due date |
| equipmentComment | Edm.String | 2000 | No | Equipment comment |
| equipmentStatus | Edm.String | 255 | No | Equipment status |
| equipmentType | Edm.String | 128 | No | Equipment type |
| equipmentValue | Edm.String | 128 | No | Equipment value |
| lastModifiedBy | Edm.String | 100 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| optional | Edm.Boolean | - | No | Optional flag |
| process | Edm.String | 128 | No | Parent process |
| responsibleUsers | Edm.String | 255 | No | Responsible users |
| subjectUser | Edm.String | 100 | No | Subject user |

### ComplianceProcess

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| processId | Edm.String | 128 | Yes | Process ID (Primary Key) |
| complianceMasterId | Edm.String | 128 | No | Compliance master ID |
| correctDataTriggered | Edm.Boolean | - | No | Correct data triggered flag |
| createdBy | Edm.String | 100 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| lastModifiedBy | Edm.String | 100 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| onboardingProcess | Edm.String | 128 | No | Parent onboarding process |
| processInitiatorId | Edm.String | 128 | No | Process initiator ID |
| processInitiatorType | Edm.String | 128 | No | Process initiator type |
| processStatus | Edm.String | 128 | No | Process status |
| processType | Edm.String | 128 | No | Process type |
| startDate | Edm.DateTime | - | No | Start date |
| user | Edm.String | 100 | No | User |

### ComplianceFormData

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| externalCode | Edm.String | 128 | Yes | External code (Primary Key) |
| createdBy | Edm.String | 100 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| documentAttachmentId | Edm.Int64 | - | No | Document attachment ID |
| documentFlow | Edm.String | 128 | No | Document flow |
| envelopeId | Edm.String | 128 | No | Envelope ID |
| form | Edm.String | 128 | No | Form |
| lastModifiedBy | Edm.String | 100 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| pdfId | Edm.String | 255 | No | PDF ID |
| process | Edm.String | 128 | No | Process |
| sensitiveDataIncluded | Edm.Boolean | - | No | Sensitive data included flag |
| subjectUser | Edm.String | 100 | No | Subject user |

## Get Object Primary Keys

| Entity | Primary Keys |
|--------|--------------|
| ONB2Process | processId |
| ONB2ProcessTask | ONB2Process_processId, taskId |
| ONB2ProcessTrigger | triggerId |
| ONB2EquipmentType | code |
| ONB2EquipmentTypeValue | code |
| ONB2EquipmentActivity | activityId |
| ComplianceProcess | processId |
| ComplianceProcessTask | taskId |
| ComplianceFormData | externalCode |
| ComplianceDocumentFlow | documentFlowCode |
| ComplianceFormSignature | ComplianceFormData_externalCode, externalCode |
| AssignedComplianceForm | id |

## Object's Ingestion Type

**Recommended Ingestion Type by Entity:**

| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| ONB2Process | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ONB2ProcessTask | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ONB2ProcessTrigger | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ONB2EquipmentType | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ONB2EquipmentTypeValue | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ONB2EquipmentActivity | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ComplianceProcess | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ComplianceProcessTask | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ComplianceFormData | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ComplianceDocumentFlow | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| ComplianceFormSignature | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| AssignedComplianceForm | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2/{EntitySet}
```

### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| $top | integer | - | Number of records to return |
| $skip | integer | 0 | Number of records to skip |
| $filter | string | - | OData filter expression |
| $count | boolean | false | Include total count in response |
| $orderby | string | - | Sort order for results |
| $select | string | - | Fields to include in response |
| $expand | string | - | Related entities to expand |
| $search | string | - | Free text search |

### Example Requests

**Get Onboarding Processes:**
```http
GET https://{api-server}/odata/v2/ONB2Process
    ?$top=100
    &$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'
    &$orderby=lastModifiedDateTime asc
    &$expand=processTasks
```

**Get Process Tasks:**
```http
GET https://{api-server}/odata/v2/ONB2ProcessTask
    ?$top=100
    &$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'
    &$orderby=lastModifiedDateTime asc
```

**Get Equipment Types with Values:**
```http
GET https://{api-server}/odata/v2/ONB2EquipmentType
    ?$expand=toEquipmentTypeValue
    &$top=100
```

**Get Compliance Processes with Related Data:**
```http
GET https://{api-server}/odata/v2/ComplianceProcess
    ?$expand=processTasks,toFormData
    &$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'
    &$orderby=lastModifiedDateTime asc
```

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- For incremental sync, filter by `lastModifiedDateTime gt datetime'{lastSyncTime}'`
- Order by `lastModifiedDateTime asc` for consistent incremental processing

### Navigation Properties

| Entity | Navigation Properties |
|--------|----------------------|
| ONB2Process | processTasks, processTriggerNav, toDataCollectionUserConfig, toEquipmentActivity |
| ONB2EquipmentType | toEquipmentTypeValue |
| ONB2EquipmentTypeValue | typeNav |
| ONB2EquipmentActivity | equipmentTypeNav, equipmentValueNav, processNav |
| ONB2ProcessTrigger | toProcess |
| ComplianceProcess | onboardingProcessNav, processTasks, toDocumentFlow, toFormData |
| ComplianceFormData | documentFlowNav, formSignatures, processNav |

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
- SAP SuccessFactors API Spec: OnboardingOBX.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/2435145dfc84462db9bdf2bb536dc5aa.html
