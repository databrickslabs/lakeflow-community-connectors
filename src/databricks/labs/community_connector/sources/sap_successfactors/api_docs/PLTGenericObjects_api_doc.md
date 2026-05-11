# Generic Objects API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
The following entities are available in this API:

| Entity Name | API Entity | Description |
|------------|-----------|-------------|
| Voluntary Separation Requests | cust_voluntarySeparationRequest | Employee voluntary separation/resignation requests |
| Commuting Allowance | cust_CommutingAllowance | Employee commuting allowance information |
| Progressive Disciplinary Actions | cust_ProgressiveDisciplinaryAction | Employee disciplinary action records |
| Recruit Interview JP | cust_RecruitInterviewJP | Japanese recruitment interview data |
| MDF Enum Values | MDFEnumValue | Metadata framework enumeration values |
| Group Medical Insurance IND | cust_groupMedicalInsuranceIND | Group medical insurance claims (India) |
| Grievances | cust_grievances | Employee grievance records |
| Auth Sign | cust_auth_sign | Authorization signatures |
| MDF Localized Values | MDFLocalizedValue | Metadata framework localized values |
| Group Insurance Details IND | cust_groupInsuranceDetailsIND | Group insurance details (India) |

**Note:** The APIs exposed through the Generic Objects framework are based on custom configurations within the specific SuccessFactors instance. Entity names starting with `cust_` are customer-defined custom objects.

## Object Schema

### cust_voluntarySeparationRequest
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| effectiveStartDate | string | DateTime | No | Effective start date (Key) |
| externalCode | string | - | No | External code (Key) |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| cust_eecomments | string | - | Yes | Employee comments |
| cust_hrcomments | string | - | Yes | HR comments |
| cust_lastday | string | DateTime | Yes | Last working day |
| cust_lastdayppolicy | string | DateTime | Yes | Last day per policy |
| cust_mgrcomments | string | - | Yes | Manager comments |
| cust_noticePeriodMonths | string | - | Yes | Notice period in months |
| cust_noticerecovery | boolean | - | Yes | Notice recovery flag |
| cust_reason | string | - | Yes | Separation reason |
| cust_requestAsOfDate | string | DateTime | Yes | Request as of date |
| cust_shortfallinnperiod | string | int64 | Yes | Shortfall in notice period |
| externalName | string | - | Yes | External name |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemEffectiveEndDate | string | DateTime | Yes | Effective end date |
| mdfSystemRecordStatus | string | - | Yes | Record status |

### cust_CommutingAllowance
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| effectiveStartDate | string | DateTime | No | Effective start date (Key) |
| externalCode | string | - | No | External code (Key) |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| cust_Amount | string | int64 | Yes | Allowance amount |
| cust_ConnectionPoint | string | - | Yes | Connection point |
| cust_DestinationStation | string | - | Yes | Destination station |
| cust_OriginStation | string | - | Yes | Origin station |
| cust_PaymentInterval | string | int64 | Yes | Payment interval |
| cust_TrainLinename1 | string | - | Yes | First train line name |
| cust_TrainLinename2 | string | - | Yes | Second train line name |
| externalName | string | - | Yes | External name |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemEffectiveEndDate | string | DateTime | Yes | Effective end date |
| mdfSystemRecordStatus | string | - | Yes | Record status |

### cust_ProgressiveDisciplinaryAction
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| effectiveStartDate | string | DateTime | No | Effective start date (Key) |
| externalCode | string | - | No | External code (Key) |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| cust_EEAcknowledgement | boolean | - | Yes | Employee acknowledgement |
| cust_FacetoFaceMtg | boolean | - | Yes | Face to face meeting flag |
| cust_IncidentDetails | string | - | Yes | Incident details |
| cust_IncidentStatus | string | - | Yes | Incident status |
| cust_InitiateVerbal | boolean | - | Yes | Initiate verbal warning flag |
| cust_Reason | string | - | Yes | Reason for action |
| cust_Severity | string | - | Yes | Severity level |
| cust_VerbalComments | string | - | Yes | Verbal warning comments |
| cust_dateofincident | string | DateTime | Yes | Date of incident |
| cust_step | string | - | Yes | Disciplinary step |
| cust_unionRep | string | - | Yes | Union representative |
| externalName | string | - | Yes | External name |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| mdfSystemStatus | string | - | Yes | System status |

### cust_grievances
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| effectiveStartDate | string | DateTime | No | Effective start date (Key) |
| externalCode | string | - | No | External code (Key) |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| cust_attendGrievanceComm | string | - | Yes | Grievance committee attendees |
| cust_coResponse | string | - | Yes | Company response |
| cust_coResponseDue | string | DateTime | Yes | Company response due date |
| cust_comments | string | - | Yes | Comments |
| cust_dateSubmitted | string | DateTime | Yes | Date submitted |
| cust_description | string | - | Yes | Description |
| cust_grievanceNumber | string | int64 | Yes | Grievance number |
| cust_grievanceStatus | string | - | Yes | Grievance status |
| cust_outcome | string | - | Yes | Outcome |
| cust_union | string | - | Yes | Union |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemRecordStatus | string | - | Yes | Record status |
| mdfSystemStatus | string | - | Yes | System status |

### MDFEnumValue
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| key | string | - | No | Enum key (Key) |
| value | string | - | No | Enum value |
| de_DE | string | - | Yes | German label |
| en_GB | string | - | Yes | British English label |
| en_US | string | - | Yes | US English label |
| es_ES | string | - | Yes | Spanish label |
| fr_FR | string | - | Yes | French label |
| ja_JP | string | - | Yes | Japanese label |
| ko_KR | string | - | Yes | Korean label |
| nl_NL | string | - | Yes | Dutch label |
| pt_BR | string | - | Yes | Brazilian Portuguese label |
| pt_PT | string | - | Yes | Portuguese label |
| ru_RU | string | - | Yes | Russian label |
| zh_CN | string | - | Yes | Simplified Chinese label |
| zh_TW | string | - | Yes | Traditional Chinese label |

### MDFLocalizedValue
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| locale | string | - | No | Locale code (Key) |
| value | string | - | Yes | Localized value |

### cust_auth_sign
| Field Name | Type | Format | Nullable | Description |
|-----------|------|--------|----------|-------------|
| externalCode | string | - | No | External code (Key) |
| createdBy | string | - | Yes | User who created the record |
| createdDateTime | string | DateTime | Yes | Creation timestamp |
| cust_asofdate | string | DateTime | Yes | As of date |
| cust_signature | string | - | Yes | Signature |
| externalName | string | - | Yes | External name |
| lastModifiedBy | string | - | Yes | User who last modified |
| lastModifiedDateTime | string | DateTime | Yes | Last modification timestamp |
| mdfSystemRecordStatus | string | - | Yes | Record status |

## Get Object Primary Keys

| Entity | Primary Key Field(s) |
|--------|---------------------|
| cust_voluntarySeparationRequest | effectiveStartDate, externalCode |
| cust_CommutingAllowance | effectiveStartDate, externalCode |
| cust_ProgressiveDisciplinaryAction | effectiveStartDate, externalCode |
| cust_RecruitInterviewJP | effectiveStartDate, externalCode |
| MDFEnumValue | key |
| cust_groupMedicalInsuranceIND | BenefitEmployeeClaim_id, externalCode |
| cust_grievances | effectiveStartDate, externalCode |
| cust_auth_sign | externalCode |
| MDFLocalizedValue | locale |
| cust_groupInsuranceDetailsIND | BenefitEmployeeClaim_id, externalCode |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| cust_voluntarySeparationRequest | `cdc` | Has lastModifiedDateTime for incremental filtering |
| cust_CommutingAllowance | `cdc` | Has lastModifiedDateTime for incremental filtering |
| cust_ProgressiveDisciplinaryAction | `cdc` | Has lastModifiedDateTime for incremental filtering |
| cust_RecruitInterviewJP | `cdc` | Has lastModifiedDateTime for incremental filtering |
| MDFEnumValue | `snapshot` | Reference data without timestamp fields |
| cust_groupMedicalInsuranceIND | `cdc` | Has lastModifiedDateTime for incremental filtering |
| cust_grievances | `cdc` | Has lastModifiedDateTime for incremental filtering |
| cust_auth_sign | `cdc` | Has lastModifiedDateTime for incremental filtering |
| MDFLocalizedValue | `snapshot` | Reference data without timestamp fields |
| cust_groupInsuranceDetailsIND | `cdc` | Has lastModifiedDateTime for incremental filtering |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All Entities
```http
GET /cust_voluntarySeparationRequest
GET /cust_CommutingAllowance
GET /cust_ProgressiveDisciplinaryAction
GET /cust_RecruitInterviewJP
GET /MDFEnumValue
GET /cust_groupMedicalInsuranceIND
GET /cust_grievances
GET /cust_auth_sign
GET /MDFLocalizedValue
GET /cust_groupInsuranceDetailsIND
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

**cust_voluntarySeparationRequest:**
```http
GET /cust_voluntarySeparationRequest(effectiveStartDate={effectiveStartDate},externalCode='{externalCode}')
```

**cust_CommutingAllowance:**
```http
GET /cust_CommutingAllowance(effectiveStartDate={effectiveStartDate},externalCode='{externalCode}')
```

**MDFEnumValue:**
```http
GET /MDFEnumValue('{key}')
```

### $expand Options by Entity
| Entity | Expand Options |
|--------|---------------|
| cust_voluntarySeparationRequest | mdfSystemRecordStatusNav |
| cust_CommutingAllowance | mdfSystemRecordStatusNav |
| cust_ProgressiveDisciplinaryAction | mdfSystemRecordStatusNav, mdfSystemStatusNav |
| cust_grievances | mdfSystemRecordStatusNav, mdfSystemStatusNav |
| cust_auth_sign | mdfSystemRecordStatusNav |

### Example Requests

**List all voluntary separation requests:**
```http
GET https://{api-server}/odata/v2/cust_voluntarySeparationRequest?$top=100
```

**Filter by lastModifiedDateTime for incremental sync:**
```http
GET https://{api-server}/odata/v2/cust_voluntarySeparationRequest?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get commuting allowances with expanded status:**
```http
GET https://{api-server}/odata/v2/cust_CommutingAllowance?$expand=mdfSystemRecordStatusNav
```

**Get grievances by status:**
```http
GET https://{api-server}/odata/v2/cust_grievances?$filter=cust_grievanceStatus eq 'Open'
```

**Get disciplinary actions for a date range:**
```http
GET https://{api-server}/odata/v2/cust_ProgressiveDisciplinaryAction?$filter=cust_dateofincident ge datetime'2024-01-01T00:00:00' and cust_dateofincident le datetime'2024-12-31T23:59:59'
```

**Get enum values for a specific locale:**
```http
GET https://{api-server}/odata/v2/MDFEnumValue?$select=key,value,en_US
```

### Pagination
Use `$top` and `$skip` for pagination:
```http
GET /cust_voluntarySeparationRequest?$top=100&$skip=0
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "effectiveStartDate": "/Date(1704067200000)/",
        "externalCode": "VSR001",
        "cust_reason": "Personal",
        "cust_lastday": "/Date(1706745600000)/",
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
- SAP SuccessFactors API Spec: PLTGenericObjects.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/03e1fc3791684367a6a76a614a2916de.html
