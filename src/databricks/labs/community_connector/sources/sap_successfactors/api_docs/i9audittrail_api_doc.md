# Form I-9 Audit Trail API Documentation

## Authorization

- **Method**: X.509 Certificate Authentication
- **Headers**:
  - `Authorization`: Authorization token (Bearer token or certificate-based)
  - `Prefer`: Controls output detail level
    - `return=minimal`: Returns only `seqNo` and `externalId`
    - `return=representation`: Returns full set of fields
- **Note**: According to the API specification, x509 certificate authentication is required for this API.

## Object List

| Resource | Description |
|----------|-------------|
| I9 Audit Trail Records | I-9 reverification and compliance data records |

### Endpoints

| Path | Methods | Description |
|------|---------|-------------|
| `/i9AuditTrailRecords` | POST | Import I-9 reverification data |
| `/i9AuditTrailRecords/{externalCode}` | GET | Get audit trail record by external code |
| `/user/{assignmentId}/i9AuditTrailRecords` | GET | Get audit trail records for a user |

## Object Schema

### i9AuditTrailRecord

The main entity composed of three sections plus core fields:

#### Core Fields
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| seqNo | integer (int64) | No | Sequence number |
| externalCode | string | Yes (x-key) | External code identifier |
| userId | string | No | User ID (e.g., "2011034567") |
| sourceOfRecord | string (enum) | No | Source: COMPLIANCE, IMPORT, API |
| complianceProcess.processId | string | No | Compliance process identifier |
| status | string (enum) | No | Status of the I-9 form |

#### Status Values
- SECTION1_INITIATED, SECTION1_SAVED, SECTION1_SUBMITTED, SECTION1_SIGNED, SECTION1_DECLINED
- SECTION2_SAVED, SECTION2_SUBMITTED, SECTION2_SIGNED, SECTION2_DECLINED
- SECTION3_INITIATED, SECTION3_SAVED, SECTION3_SUBMITTED, SECTION3_SIGNED
- ONBOARDING_CANCELLED, ONBOARDING_RESTARTED, USER_TERMINATED, CANCELLED, COMPLETED, VOIDED

#### Section 1 Fields (Employee Information)
| Field | Type | Description |
|-------|------|-------------|
| firstName | string | First name |
| lastName | string | Last name |
| middleName | string | Middle name |
| otherName | string | Other names used |
| address | string | Street address |
| apartmentNumber | string | Apartment number |
| city | string | City |
| state | string (enum) | US state code (AL, AK, AZ, etc.) |
| zipCode | string | ZIP code |
| dateOfBirth | string (date) | Date of birth |
| ssn | string | Social Security Number |
| emailAddress | string | Email address |
| phoneNumber | string | Phone number |
| citizenshipType | string (enum) | US_CITIZEN, US_NATIONAL, LAWFUL_PERMANENT_RESIDENT, ALIEN_AUTHORIZED_TO_WORK |
| alienRegistrationNumber | string | Alien registration number |
| uscisNumber | string | USCIS number |
| i94AdministrationNum | string | I-94 admission number |
| passportNumber | string | Passport number |
| countryOfIssue.code | string | Country code |
| visaType | string (enum) | Visa type (F1, J1, H1B, etc.) |
| visaNum | string | Visa number |
| visaExpirationDate | string (date) | Visa expiration date |
| protectedStatus | string (enum) | REFUGEE, ASYLEE, OTHERPROTECTED, NONE |
| translator1-5 | string | Translator information |

#### Section 2 Fields (Employer Verification)
| Field | Type | Description |
|-------|------|-------------|
| employerTitle | string | Employer title |
| employerBusiness | string | Employer business name |
| employerFirstName | string | Employer first name |
| employerLastName | string | Employer last name |
| employerAddress | string | Employer address |
| employerCity | string | Employer city |
| employerState | string | Employer state |
| employerZipCode | string | Employer ZIP code |
| listADocument1-3Number | string | List A document numbers |
| listADocument1-3Name | string | List A document names |
| listADocument1-3Issuer | string | List A document issuers |
| listADocument1-3Expiry | string | List A document expiry dates |
| listBDocumentNumber | string | List B document number |
| listCDocumentNumber | string | List C document number |
| additionalInfo1-10 | string | Additional information fields |

#### Section 3 Fields (Reverification)
| Field | Type | Description |
|-------|------|-------------|
| verificationDate | string | Reverification date |
| verificationDocument | string | Reverification document |
| verifierFirstName | string | Verifier first name |
| verifierLastName | string | Verifier last name |
| dateOfRehire | string | Date of rehire |
| reverificationListADoc1-3 | string | Reverification List A documents |
| reverificationListCDoc | string | Reverification List C document |
| newFirstName | string | New first name (if changed) |
| newLastName | string | New last name (if changed) |

## Get Object Primary Keys

| Entity | Primary Key Field |
|--------|-------------------|
| i9AuditTrailRecord | externalCode |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| i9AuditTrailRecord | `snapshot` | No timestamp-based filtering or ordering available. The API provides pagination via `$top` and `$skip` but no incremental/CDC capabilities. |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/rest/onboarding/compliance/i9audittrail/v1
```

### Get Records by External Code
```
GET /i9AuditTrailRecords/{externalCode}
```

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| externalCode | string | Yes | External code identifier |

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| $top | integer | No | Number of items to retrieve |
| $skip | integer | No | Number of items to skip |

**Headers:**
| Header | Value | Description |
|--------|-------|-------------|
| Prefer | return=minimal or return=representation | Controls response detail level |
| Authorization | Bearer token | Authorization token |

### Get Records by User Assignment ID
```
GET /user/{assignmentId}/i9AuditTrailRecords
```

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| assignmentId | string | Yes | User assignment ID |

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| $top | integer | No | Number of items to retrieve |
| $skip | integer | No | Number of items to skip |

### Pagination

- Uses `$top` and `$skip` for offset-based pagination
- Default page size: Not specified in API
- Example: `?$top=100&$skip=0`

### Example Request
```http
GET https://apisalesdemo8.successfactors.com/rest/onboarding/compliance/i9audittrail/v1/user/12345/i9AuditTrailRecords?$top=100&$skip=0
Authorization: Bearer <token>
Prefer: return=representation
```

### Example Response
```json
{
  "value": [
    {
      "seqNo": 1,
      "externalCode": "EXT001",
      "userId": "2011034567",
      "sourceOfRecord": "COMPLIANCE",
      "status": "COMPLETED",
      "firstName": "John",
      "lastName": "Doe",
      "citizenshipType": "US_CITIZEN"
    }
  ]
}
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer (int64) | LongType |
| integer (int32) | IntegerType |
| string (date) | DateType |
| string (enum) | StringType |
| boolean | BooleanType |
| array | ArrayType |
| object | StructType |

## Sources and References

- **SAP SuccessFactors API Spec**: i9audittrail.json
- **SAP Help Portal**: [Migrating Form I-9 Data from a Third-Party System](https://help.sap.com/docs/SAP_SUCCESSFACTORS_ONBOARDING/c94ed5fcb5fe4e0281f396556743812c/28aeb719c101486aabbd0df01a35af46.html)
