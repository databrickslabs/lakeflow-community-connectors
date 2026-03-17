# Candidate API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **Candidate** - Main candidate profile with personal information
2. **CandidateLight** - Lightweight version of candidate data
3. **CandidateComments** - Comments associated with candidates
4. **CandidateBackground_Education** - Candidate education history
5. **CandidateBackground_InsideWorkExperience** - Internal work experience
6. **CandidateBackground_OutsideWorkExperience** - External work experience
7. **CandidateBackground_Certificates** - Certifications and licenses
8. **CandidateBackground_Languages** - Language proficiencies
9. **CandidateBackground_TalentPool** - Talent pool associations
10. **CandidateBackground_TalentPoolcorp** - Corporate talent pool associations
11. **CandidateProfileExtension** - Extended profile fields
12. **CandidateTags** - Tags associated with candidates
13. **JobApplicationSnapshot_Education** - Snapshot of education at application time
14. **JobApplicationSnapshot_InsideWorkExperience** - Snapshot of internal work experience
15. **JobApplicationSnapshot_OutsideWorkExperience** - Snapshot of external work experience
16. **JobApplicationSnapshot_Certificates** - Snapshot of certificates
17. **JobApplicationSnapshot_Languages** - Snapshot of languages
18. **JobApplicationSnapshot_TalentPool** - Snapshot of talent pool
19. **JobApplicationSnapshot_TalentPoolcorp** - Snapshot of corporate talent pool

## Object Schema

### Candidate / CandidateLight
| Field | Type | Description |
|-------|------|-------------|
| candidateId | int64 | Primary key |
| firstName | string (nullable) | First name |
| lastName | string (nullable) | Last name |
| middleName | string (nullable) | Middle name |
| primaryEmail | string | Primary email address |
| contactEmail | string (nullable) | Contact email |
| cellPhone | string (nullable) | Cell phone number |
| homePhone | string (nullable) | Home phone number |
| address | string (nullable) | Address line 1 |
| address2 | string (nullable) | Address line 2 |
| city | string (nullable) | City |
| country | string (nullable) | Country |
| zip | string (nullable) | Postal/ZIP code |
| currentTitle | string (nullable) | Current job title |
| candidateLocale | string | Candidate's locale |
| anonymized | string | Anonymization status |
| anonymizedDateTime | datetime | When anonymized |
| externalCandidate | boolean | External candidate flag |
| publicIntranet | boolean | Public intranet visibility |
| visibilityOption | boolean | Visibility option |
| shareProfile | string | Profile sharing setting |
| agreeToPrivacyStatement | string (nullable) | Privacy statement agreement |
| dataPrivacyId | int64 | Data privacy identifier |
| usersSysId | string | System user ID |
| partnerMemberId | string | Partner member ID |
| partnerSource | int64 | Partner source ID |
| creationDateTime | datetime | Creation timestamp |
| lastModifiedDateTime | datetime | Last modification timestamp |
| lastLoginDateTime | datetime | Last login timestamp |
| dateofAvailability | datetime | Date of availability |
| privacyAcceptDateTime | datetime | Privacy acceptance timestamp |
| password | string (nullable) | Password (hashed) |

### CandidateComments
| Field | Type | Description |
|-------|------|-------------|
| commentId | int64 | Primary key |
| candidateId | int64 (nullable) | Reference to candidate |
| associatedId | int64 (nullable) | Associated entity ID |
| associatedCommentId | int64 (nullable) | Associated comment ID |
| commentator | string (nullable) | User who commented |
| content | string (nullable) | Comment content |
| hasAssociatedComment | string (nullable) | Has associated comment flag |
| refType | string (nullable) | Reference type |
| migratedCommentatorUserName | string (nullable) | Migrated commentator |

### CandidateBackground_Education
| Field | Type | Description |
|-------|------|-------------|
| backgroundElementId | int64 | Primary key |
| candidateId | int64 | Reference to candidate |
| bgOrderPos | int64 | Order position |
| school | string (nullable) | School name |
| degree | string (nullable) | Degree type |
| major | string (nullable) | Major/Field of study |
| degreeDate | datetime (nullable) | Degree date |
| startDate | datetime (nullable) | Start date |
| endDate | datetime (nullable) | End date |
| schoolType | string (nullable) | Type of school |
| schoolAddress | string (nullable) | School address |
| schoolCity | string (nullable) | School city |
| schoolState | string (nullable) | School state |
| schoolCountry | string (nullable) | School country |
| schoolZip | string (nullable) | School ZIP code |
| schoolPhone | string (nullable) | School phone |
| presentStudent | int32 (nullable) | Currently a student |
| lastModifiedDateTime | datetime | Last modification timestamp |

### CandidateBackground_InsideWorkExperience
| Field | Type | Description |
|-------|------|-------------|
| backgroundElementId | int64 | Primary key |
| candidateId | int64 | Reference to candidate |
| bgOrderPos | int64 | Order position |
| title | string (nullable) | Job title |
| department | string (nullable) | Department |
| startDate | datetime (nullable) | Start date |
| endDate | datetime (nullable) | End date |
| lastModifiedDateTime | datetime | Last modification timestamp |

### CandidateBackground_OutsideWorkExperience
| Field | Type | Description |
|-------|------|-------------|
| backgroundElementId | int64 | Primary key |
| candidateId | int64 | Reference to candidate |
| bgOrderPos | int64 | Order position |
| employer | string (nullable) | Employer name |
| startTitle | string (nullable) | Starting title |
| businessType | string (nullable) | Type of business |
| startDate | datetime (nullable) | Start date |
| endDate | datetime (nullable) | End date |
| presentEmployer | string (nullable) | Current employer flag |
| employerAddress | string (nullable) | Employer address |
| employerCity | string (nullable) | Employer city |
| employerState | string (nullable) | Employer state |
| employerCountry | string (nullable) | Employer country |
| employerZip | string (nullable) | Employer ZIP code |
| employerPhone | string (nullable) | Employer phone |
| employerEmail | string (nullable) | Employer email |
| employerContact | string (nullable) | Employer contact |
| lastModifiedDateTime | datetime | Last modification timestamp |

### CandidateBackground_Certificates
| Field | Type | Description |
|-------|------|-------------|
| backgroundElementId | int64 | Primary key |
| candidateId | int64 | Reference to candidate |
| bgOrderPos | int64 | Order position |
| name | string (nullable) | Certificate name |
| description | string (nullable) | Description |
| institution | string (nullable) | Issuing institution |
| licenseType | string (nullable) | License type |
| licenseName | string (nullable) | License name |
| licenseNumber | string (nullable) | License number |
| licenseState | string (nullable) | License state |
| licenseCountry | string (nullable) | License country |
| startDate | datetime (nullable) | Issue date |
| endDate | datetime (nullable) | Expiration date |
| lastModifiedDateTime | datetime | Last modification timestamp |

### CandidateBackground_Languages
| Field | Type | Description |
|-------|------|-------------|
| backgroundElementId | int64 | Primary key |
| candidateId | int64 | Reference to candidate |
| bgOrderPos | int64 | Order position |
| language | string (nullable) | Language name |
| readingProf | string (nullable) | Reading proficiency |
| speakingProf | string (nullable) | Speaking proficiency |
| writingProf | string (nullable) | Writing proficiency |
| lastModifiedDateTime | datetime | Last modification timestamp |

## Get Object Primary Keys

| Entity | Primary Key(s) |
|--------|----------------|
| Candidate | candidateId |
| CandidateLight | candidateId |
| CandidateComments | commentId |
| CandidateBackground_Education | backgroundElementId |
| CandidateBackground_InsideWorkExperience | backgroundElementId |
| CandidateBackground_OutsideWorkExperience | backgroundElementId |
| CandidateBackground_Certificates | backgroundElementId |
| CandidateBackground_Languages | backgroundElementId |
| CandidateBackground_TalentPool | backgroundElementId |
| CandidateBackground_TalentPoolcorp | backgroundElementId |
| JobApplicationSnapshot_Education | applicationId, backgroundElementId |
| JobApplicationSnapshot_InsideWorkExperience | applicationId, backgroundElementId |
| JobApplicationSnapshot_OutsideWorkExperience | applicationId, backgroundElementId |
| JobApplicationSnapshot_Certificates | applicationId, backgroundElementId |
| JobApplicationSnapshot_Languages | applicationId, backgroundElementId |
| JobApplicationSnapshot_TalentPool | applicationId, backgroundElementId |
| JobApplicationSnapshot_TalentPoolcorp | applicationId, backgroundElementId |

## Object's Ingestion Type

| Entity | Ingestion Type | Cursor Field | Notes |
|--------|---------------|--------------|-------|
| Candidate | cdc | lastModifiedDateTime | Supports $filter on lastModifiedDateTime |
| CandidateLight | cdc | lastModifiedDateTime | Supports $filter on lastModifiedDateTime |
| CandidateBackground_Education | cdc | lastModifiedDateTime | Supports $filter on lastModifiedDateTime |
| CandidateBackground_InsideWorkExperience | cdc | lastModifiedDateTime | Supports $filter on lastModifiedDateTime |
| CandidateBackground_OutsideWorkExperience | cdc | lastModifiedDateTime | Supports $filter on lastModifiedDateTime |
| CandidateBackground_Certificates | cdc | lastModifiedDateTime | Supports $filter on lastModifiedDateTime |
| CandidateBackground_Languages | cdc | lastModifiedDateTime | Supports $filter on lastModifiedDateTime |
| CandidateComments | snapshot | N/A | No timestamp field available |
| JobApplicationSnapshot_* | cdc | lastModifiedDateTime | Supports $filter on lastModifiedDateTime |

## Read API for Data Retrieval

### Base URL Pattern
```
https://{api-server}/odata/v2/{EntitySet}
```

### Supported Query Parameters
| Parameter | Description |
|-----------|-------------|
| $select | Select specific properties to return |
| $filter | Filter results by property values |
| $top | Limit number of results (default: 20) |
| $skip | Skip first n results for pagination |
| $orderby | Order results by property |
| $expand | Expand related entities |
| $count | Include total count of items |
| $search | Search items by search phrases |

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- Default page size: 20 records
- Recommended: Use `$orderby` with `$top` and `$skip` for consistent pagination

### Example Requests

**Get all candidates:**
```http
GET https://{api-server}/odata/v2/Candidate?$top=100
```

**Get candidate by ID:**
```http
GET https://{api-server}/odata/v2/Candidate({candidateId})
```

**Get candidates modified after a date (incremental sync):**
```http
GET https://{api-server}/odata/v2/Candidate?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc
```

**Get candidate with expanded education:**
```http
GET https://{api-server}/odata/v2/Candidate({candidateId})?$expand=education
```

**Get candidate background education:**
```http
GET https://{api-server}/odata/v2/CandidateBackground_Education?$filter=candidateId eq {candidateId}
```

**Select specific fields:**
```http
GET https://{api-server}/odata/v2/Candidate?$select=candidateId,firstName,lastName,primaryEmail,lastModifiedDateTime
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
| Edm.Double | DoubleType |
| Edm.Binary | BinaryType |

## Sources and References

- SAP SuccessFactors API Spec: RCMCandidate.json
- SAP Help Portal: [Candidate APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/33ccaf71c7df4e8c810899581452b544.html)
- API Server List: [List of API Servers in SAP SuccessFactors](https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html)
