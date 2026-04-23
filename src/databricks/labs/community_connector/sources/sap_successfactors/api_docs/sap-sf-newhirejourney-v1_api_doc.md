# Onboarding New Hire Journey API Documentation

## Authorization
- **Method**: Basic Authentication
- **Security Scheme**: BasicAuth (HTTP Basic)
- **Headers**:
  - `Authorization: Basic <base64_encoded_credentials>`
- **Credentials**: Username and password encoded in Base64

## Object List
The API provides access to the following resources:

| Resource | Description |
|----------|-------------|
| Journeys | Onboarding journeys for new hires and rehires |

## Object Schema

### journeyDetails
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| journeyId | string | Yes | Onboarding Journey ID for the Onboardee |
| status | string | No | Status of the Journey (e.g., "INITIATED") |
| userId | string | No | Onboarding user ID |
| startDate | string (date) | No | Start Date |
| manager | string | No | Manager user ID |
| journeyType | string | No | Type of Journey |
| journeySubType | string | No | Sub type of journey |
| hireStatus | string | No | Hire status of journey |
| userFullName | string | No | Full name of new hire |
| managerFullName | string | No | Full name of the manager |

### newHireCreateJourneyRequest
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| firstName | string | No | First Name of the new hire |
| lastName | string | No | Last Name of the new hire |
| email | string | No | Personal email of the new hire |
| userName | string | Yes | Preferred user name |
| startDate | string (date) | No | Start date (e.g., "2025-03-01") |
| manager | string | No | Manager ID |
| company | string | No | Company ID |
| eventReason | string | No | Hire Event reason |
| status | string | No | Journey status |
| userId | string | Yes | Preferred User ID |
| assignmentId | string | No | Assignment ID (nullable) |
| personId | string | No | Person ID (nullable) |
| journeyType | string | No | Only "ONBOARDING" is supported |
| journeySubType | string | No | "NewHire" or "RehireWithNewEmployment" (default: NewHire) |
| terminatedLegacyUserId | string | No | Terminated user ID for Rehire case (nullable) |
| initiatorKeys | object (initiatorKeys) | No | Initiator business keys |

### initiatorKeys
| Field | Type | Description |
|-------|------|-------------|
| atsApplicationId | string | Applicant Tracking System Requisition ID (max 255 chars) |

### newHireCreateJourneyResponse
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| id | string | Yes | Email ID (nullable) |
| createJourneyDetails | object (newHireCreateJourneyDetails) | No | Created journey details |
| errors | string | No | Error details (nullable) |

### newHireCreateJourneyDetails
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| journeyId | string | Yes | Journey ID |
| responseStatus | string | No | Response status (e.g., "INITIATED") |
| userId | string | No | User ID |

### newHireUpdateJourneyRequest
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| journeyId | string | Yes | ID of the Onboarding Process Journey |
| status | string | No | Type of update operation (only "CANCEL" supported) |
| cancelReason | string | No | Reason for cancellation (e.g., "No Show") |
| cancelEventReason | string | No | Event Reason for cancellation |

### newHireUpdateJourneyResponse
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| id | string | Yes | Journey ID (nullable) |
| updatedJourneyDetails | object (newHireUpdateJourneyDetails) | No | Updated journey details |
| errors | string | No | Error details (nullable) |

### newHireUpdateJourneyDetails
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| journeyId | string | Yes | Journey ID |
| status | string | No | Status of update (e.g., "CANCELLED") |

## Get Object Primary Keys
| Object | Primary Key Field(s) |
|--------|---------------------|
| journeyDetails | journeyId |
| newHireCreateJourneyRequest | userName, userId |
| newHireUpdateJourneyRequest | journeyId |

## Object's Ingestion Type
| Object | Ingestion Type | Reasoning |
|--------|----------------|-----------|
| journeyDetails | `snapshot` | Single record retrieval by journeyId. No list endpoint with filtering/pagination. No timestamp-based incremental query support. |

**Notes**:
- GET endpoint only retrieves a single journey by ID
- No batch retrieval or list endpoint available
- No filtering by timestamp or status for incremental loading
- For comprehensive journey data, consider using OData APIs or event-based integration

## Read API for Data Retrieval

### Get Onboarding Journey by ID
- **Endpoint**: `GET /onboarding/newhire/v1/journeys/{journeyId}`
- **Base URL**: `https://{api-server}`
- **Full URL**: `https://{api-server}/onboarding/newhire/v1/journeys/{journeyId}`

#### Path Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| journeyId | string | Yes | Journey ID |

#### Example Request
```http
GET https://apisalesdemo2.successfactors.com/onboarding/newhire/v1/journeys/journey123
Authorization: Basic <base64_credentials>
```

#### Response Structure
```json
{
  "value": [
    {
      "journeyId": "journey123",
      "status": "INITIATED",
      "userId": "newhire001",
      "startDate": "2025-03-01",
      "manager": "mgr001",
      "journeyType": "ONBOARDING",
      "journeySubType": "NewHire",
      "hireStatus": "Active",
      "userFullName": "John Doe",
      "managerFullName": "Jane Smith"
    }
  ]
}
```

### Write Operations (For Reference)

#### Mass Initiate Onboarding Journeys
- **Endpoint**: `POST /onboarding/newhire/v1/journeys`
- **Description**: Mass initiate onboarding journeys for new hires

```http
POST https://apisalesdemo2.successfactors.com/onboarding/newhire/v1/journeys
Authorization: Basic <base64_credentials>
Content-Type: application/json

[
  {
    "firstName": "John",
    "lastName": "Doe",
    "email": "john.doe@example.com",
    "userName": "jdoe",
    "startDate": "2025-03-01",
    "manager": "mgr001",
    "company": "company001",
    "eventReason": "New Hire",
    "userId": "newhire001",
    "journeyType": "ONBOARDING",
    "journeySubType": "NewHire",
    "initiatorKeys": {
      "atsApplicationId": "ats001"
    }
  }
]
```

#### Mass Cancel Onboarding Journeys
- **Endpoint**: `PATCH /onboarding/newhire/v1/journeys`
- **Description**: Bulk cancel onboarding journeys

```http
PATCH https://apisalesdemo2.successfactors.com/onboarding/newhire/v1/journeys
Authorization: Basic <base64_credentials>
Content-Type: application/json

[
  {
    "journeyId": "journey123",
    "status": "CANCEL",
    "cancelReason": "No Show",
    "cancelEventReason": "Candidate declined offer"
  }
]
```

#### Pagination
- **Pagination**: Not supported
- GET endpoint retrieves a single journey by ID
- POST/PATCH operations process arrays of journeys in bulk

## Field Type Mapping
| API Type | Spark Type |
|----------|------------|
| string | StringType |
| string (date) | DateType |
| boolean | BooleanType |
| array | ArrayType |
| object | StructType |

## Sources and References
- **SAP SuccessFactors API Spec**: sap-sf-newhirejourney-v1.json
- **SAP Help Portal**: [Mass Initiation of Onboarding API](https://help.sap.com/docs/successfactors-release-information/8974cf00008b4e209398478ca43bcba7/64b8f20a98f74118bf7cc8a0325e1d7f.html)
