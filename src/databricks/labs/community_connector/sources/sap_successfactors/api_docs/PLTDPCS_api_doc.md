# Data Privacy Consent Statement API Documentation

## Authorization

- **Method**: HTTP Basic Authentication
- **Security Scheme**: BasicAuth
- **Headers**:
  - `Authorization`: Basic base64(username:password)

## Object List

| Resource | Tag | Description |
|----------|-----|-------------|
| Statements | Statements | Data privacy consent statement definitions |
| Acknowledgements | Acknowledgements | User acknowledgement status for consent statements |

### Endpoints

| Path | Methods | Description |
|------|---------|-------------|
| `/statements` | GET | View details of data privacy consent statements |
| `/acknowledgements` | GET | Get the status of a data privacy consent statement |
| `/acknowledgements` | POST | Update the status of a data privacy consent statement |

## Object Schema

### dpcsVersion (Statement)

| Field | Type | Key | Nullable | Description |
|-------|------|-----|----------|-------------|
| id | integer (int64) | Yes (x-key) | No | Identifier of the DPCS |
| version | integer (int32) | No | No | Version number of the DPCS |
| customFieldType | string | No | Yes | Type of customized fields (max 200 chars) |
| template | object (dpcs) | No | No | DPCS template information |
| contents | array (dpcsContent) | No | Yes | List of DPCS content |
| attributes | array (dpcsAttribute) | No | Yes | List of DPCS attributes |

### dpcs (Template)

| Field | Type | Key | Description |
|-------|------|-----|-------------|
| id | integer (int64) | Yes (x-key) | Identifier of the DPCS template |
| name | string | No | Name of the DPCS template (max 100 chars) |
| type | integer (int32) | No | Type: 0-8 (2=Recruiting external, 8=Profile photo) |
| status | integer (int32) | No | Status: 0=disabled, 1=enabled, 2=deleted |
| forceAccept | integer (int32) | No | Force display: 0=false, 1=true |

### dpcsContent

| Field | Type | Key | Nullable | Description |
|-------|------|-----|----------|-------------|
| id | integer (int64) | Yes (x-key) | No | Identifier of the DPCS content |
| statementId | integer (int64) | No | Yes | Version ID of the DPCS |
| locale | string | No | No | Language locale (max 32 chars, e.g., en_US) |
| title | string | No | Yes | Title of the DPCS content (max 100 chars) |
| content | string | No | Yes | Content of the DPCS (max 50000 chars) |

### dpcsAttribute

| Field | Type | Key | Nullable | Description |
|-------|------|-----|----------|-------------|
| id | integer (int64) | Yes (x-key) | No | Identifier of the DPCS attribute |
| statementId | integer (int64) | No | Yes | Version ID of the DPCS |
| attributeName | string | No | No | Name of the attribute (max 100 chars) |
| attributeValue | string | No | No | Value of the attribute (max 100 chars) |

### dpcsStatus (Acknowledgement)

| Field | Type | Description |
|-------|------|-------------|
| status | integer (int32) | Status: 0=DECLINE, 1=ACCEPT, 2=REVOKE, 3=NOT PRESENTED |
| statement | object (dpcsVersion) | Statement details |

### acknowledgementResult

| Field | Type | Description |
|-------|------|-------------|
| status | integer (int32) | Status: 0=DECLINE, 1=ACCEPT, 2=REVOKE |
| statementId | integer (int64) | Version ID of the DPCS |

## Get Object Primary Keys

| Entity | Primary Key Field |
|--------|-------------------|
| dpcsVersion | id |
| dpcs (template) | id |
| dpcsContent | id |
| dpcsAttribute | id |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| Statements | `snapshot` | No timestamp-based filtering available. Returns current statement configuration. |
| Acknowledgements | `snapshot` | Returns current acknowledgement status for a subject. No incremental sync capability. |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/rest/security/privacy/v1
```

### Get Statements
```
GET /statements
```

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| type | string | Yes | DPCS type: RECRUITING_EXTERNAL or PROFILE_PHOTO |
| country | string | Yes | Alpha-3 country code (e.g., CHN, USA) |
| customFieldValue | string | No | Custom field value |
| language | string | No | Locale filter (e.g., en_US, zh_CN_SF) |

**Example Request:**
```http
GET https://apisalesdemo8.successfactors.com/rest/security/privacy/v1/statements?type=RECRUITING_EXTERNAL&country=USA&language=en_US
Authorization: Basic <credentials>
```

### Get Acknowledgements
```
GET /acknowledgements
```

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| type | string | Yes | DPCS type: RECRUITING_EXTERNAL or PROFILE_PHOTO |
| country | string | Conditional | Required for RECRUITING_EXTERNAL type |
| subjectId | string | Yes | Identifier (userId, assignmentId, or candidateId) |
| customFieldValue | string | No | Custom field values |
| language | string | No | Locale filter (default: en_US) |

**Subject ID Types:**
- For PROFILE_PHOTO: Use `assignmentId`
- For RECRUITING_EXTERNAL: Use `candidateId`

**Example Request:**
```http
GET https://apisalesdemo8.successfactors.com/rest/security/privacy/v1/acknowledgements?type=RECRUITING_EXTERNAL&country=USA&subjectId=65548733
Authorization: Basic <credentials>
```

### Update Acknowledgement
```
POST /acknowledgements
```

**Request Body:**
```json
{
  "type": "RECRUITING_EXTERNAL",
  "statementId": 1,
  "subjectId": "65548733",
  "action": 1
}
```

**Action Values:**
- 0: DECLINE
- 1: ACCEPT
- 2: REVOKE

### Response Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request - Invalid type or parameters |
| 403 | Forbidden - No permission |
| 404 | Not Found - Statement not found |
| 500 | Internal Server Error |

## Pagination

This API does not support pagination. Each request returns the complete result for the specified parameters.

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer (int32) | IntegerType |
| integer (int64) | LongType |
| boolean | BooleanType |
| array | ArrayType |
| object | StructType |

## Sources and References

- **SAP SuccessFactors API Spec**: PLTDPCS.json
- **SAP Help Portal**: [Implementing and Managing Data Protection and Privacy](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/2becac773fcf4f84a993f0556160d3de/13ebd268d4dd4686b8162794305c2984.html)
