# SCIM (System for Cross-domain Identity Management) API Documentation

## Authorization

- **Method**: HTTP Basic Authentication or X.509 Certificate
- **Security Schemes**:
  - BasicAuth: HTTP Basic authentication
  - Certificate: X.509 certificate authentication
- **Headers**:
  - `Authorization`: Basic base64(username:password) or certificate-based

### Rate Limiting
- **Limit**: 20 calls per second per user account
- Exceeding this limit may result in degraded service or unavailability

## Object List

| Resource | Tag | Description |
|----------|-----|-------------|
| Users | Users | User identity management (CRUD operations) |
| Groups | Groups | Permission group management |

### Endpoints

#### Users
| Path | Methods | Description |
|------|---------|-------------|
| `/rest/iam/scim/v2/Users` | GET, POST | List/search users or create a user |
| `/rest/iam/scim/v2/Users/{id}` | GET, PUT, PATCH, DELETE | Get, replace, modify, or delete a user |
| `/rest/iam/scim/v2/Users/.search` | POST | Search users with POST request |

#### Groups
| Path | Methods | Description |
|------|---------|-------------|
| `/rest/iam/scim/v2/Groups` | GET | Query permission groups |
| `/rest/iam/scim/v2/Groups/{id}` | GET, PUT, PATCH | Get or modify permission group members |

## Object Schema

### User

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | No (auto-generated) | Unique identifier for the user |
| externalId | string | No | External identifier for the user |
| userName | string | Yes | Unique username |
| name | object (Name) | No | User's name components |
| displayName | string | No | Display name |
| nickName | string | No | Nickname |
| title | string | No | Job title |
| userType | string | No | User type: EMPLOYEE or ONBOARDEE |
| preferredLanguage | string | No | Preferred language (e.g., en_US) |
| locale | string | No | Default locale |
| active | boolean | No | Active status |
| emails | array (Emails) | No | Email addresses |
| phoneNumbers | array (PhoneNumbers) | No | Phone numbers |
| groups | array (Groups) | No | Group memberships |
| schemas | array (string) | Yes | SCIM schema URIs |
| meta | object (Meta) | No | Resource metadata |

### Name

| Field | Type | Description |
|-------|------|-------------|
| formatted | string | Full formatted name |
| familyName | string | Family/last name |
| givenName | string | Given/first name |
| middleName | string | Middle name |
| honorificPrefix | string | Prefix (e.g., Mr., Dr.) |
| honorificSuffix | string | Suffix |

### Meta

| Field | Type | Description |
|-------|------|-------------|
| resourceType | string | Resource type (User, Group) |
| created | string (date-time) | Creation timestamp |
| lastModified | string (date-time) | Last modification timestamp |
| location | string | Resource URI |
| version | string | API version |

### Extension Schemas

#### SuccessFactors Extension (urn:ietf:params:scim:schemas:extension:successfactors:2.0:User)
| Field | Type | Description |
|-------|------|-------------|
| perPersonUuid | string | Person UUID |
| loginMethod | string | Login method (e.g., sso) |
| personIdExternal | string | Person ID in Employee Central |
| customFields | array | Custom fields (custom01-custom15) |

#### Enterprise Extension (urn:ietf:params:scim:schemas:extension:enterprise:2.0:User)
| Field | Type | Description |
|-------|------|-------------|
| department | string | Department name |
| division | string | Division name |
| manager | object | Manager information |
| employeeNumber | string | Employee number |

#### SAP Extension (urn:ietf:params:scim:schemas:extension:sap:2.0:User)
| Field | Type | Description |
|-------|------|-------------|
| userUuid | string | Global User ID (cross-SAP applications) |
| groupDomains | array | Group domains for filtering |
| sourceSystem | string | Source system identifier |

#### SAP Workforce Extension (urn:ietf:params:scim:schemas:extension:sap.workforce:2.0:User)
| Field | Type | Description |
|-------|------|-------------|
| lastDeactivated | string (date-time) | Last deactivation timestamp |

#### SAP ODM Extension (urn:ietf:params:scim:schemas:extension:sap.odm:2.0:User)
| Field | Type | Description |
|-------|------|-------------|
| workforcePersonId | string | Workforce Person UUID |

### Group

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | No | Group identifier |
| displayName | string | No | Group name |
| members | array (Members) | Yes | Group members |
| schemas | array (string) | Yes | SCIM schema URIs |
| meta | object (GroupMeta) | No | Resource metadata |

### GroupMeta

| Field | Type | Description |
|-------|------|-------------|
| resourceType | string | Always "Group" |
| created | string (date-time) | Creation timestamp |
| lastModified | string (date-time) | Last modification timestamp |
| location | string | Resource URI |
| version | string | API version |
| members.nextId | string | Next member ID for pagination |
| members.cnt | integer | Total member count |

### Group SAP Extension (urn:ietf:params:scim:schemas:extension:sap:2.0:Group)
| Field | Type | Description |
|-------|------|-------------|
| type | string | Group type: userGroup, deepLinkActivationPermission, embeddedAnalyticsAccessPermission, mobileAccessPermission |
| supportedOperations | string | readOnly or readWrite |

## Get Object Primary Keys

| Entity | Primary Key Field | Description |
|--------|-------------------|-------------|
| User | id | System-generated UUID |
| Group | id | Group identifier |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| Users | `cdc` | Supports filtering by `meta.lastModified` for incremental sync |
| Groups | `cdc` | Supports filtering by `meta.lastModified` for incremental sync |

### CDC Filtering Support

The API supports the following filter operators on `meta.lastModified`:
- `gt` (greater than)
- `ge` (greater than or equal)
- `lt` (less than)
- `le` (less than or equal)

Example filter: `meta.lastModified ge "2021-03-22T07:19:38"`

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/rest/iam/scim/v2
```

### Get Users
```
GET /Users
```

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| filter | string | No | SCIM filter expression |
| startIndex | integer | No | Start index (1-based, default: 1) |
| startId | string | No | ID-based pagination (initial or user ID) |
| count | integer | No | Items per page (default: 100) |
| attributes | string | No | Attributes to include |
| excludedAttributes | string | No | Attributes to exclude |

**Supported Filter Attributes:**
- Core: `id`, `userName`, `userType`, `active`
- Meta: `lastModified`
- SAP Extension: `userUuid`, `groupDomains`
- SuccessFactors Extension: `perPersonUuid`
- Workforce Extension: `lastDeactivated`

**Filter Operators:**
| Operator | Description |
|----------|-------------|
| eq | Equal |
| ne | Not equal |
| gt | Greater than |
| co | Contains |
| sw | Starts with |
| ew | Ends with |
| and | Logical AND |
| or | Logical OR |

**Example Request:**
```http
GET https://apisalesdemo4.successfactors.com/rest/iam/scim/v2/Users?filter=userType eq "EMPLOYEE" and active eq true&count=100&startId=initial
Authorization: Basic <credentials>
Accept: application/scim+json
```

### Get User by ID
```
GET /Users/{id}
```

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| id | string | Yes | User ID (UUID) |

### Get Groups
```
GET /Groups
```

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| filter | string | No | SCIM filter expression |
| startIndex | integer | No | Start index (1-based) |
| startId | string | No | ID-based pagination |
| count | integer | No | Items per page (default/max: 100) |
| attributes | string | No | Member pagination (e.g., members[startId=initial&count=100]) |

**Supported Filter Attributes:**
- Core: `displayName`
- Meta: `lastModified`
- SAP Extension: `type`

### Get Group by ID
```
GET /Groups/{id}
```

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| id | string | Yes | Group ID |

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| attributes | string | No | Member pagination (max count: 1000) |

### Pagination

#### Index-Based Pagination
Response includes:
- `totalResults`: Total matching records
- `itemsPerPage`: Records per page
- `startIndex`: Current start index

#### ID-Based Pagination
Response includes:
- `startId`: Current start ID
- `nextId`: Next page start ID (or "end" if last page)

**Example for ID-based pagination:**
```http
GET /Users?startId=initial&count=100
```
Then use `nextId` from response for subsequent requests.

### Example Response (Users)
```json
{
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
  "totalResults": 100,
  "itemsPerPage": 1,
  "startIndex": 1,
  "Resources": [
    {
      "schemas": [
        "urn:ietf:params:scim:schemas:core:2.0:User",
        "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"
      ],
      "id": "34c182f3-ba70-4d84-915a-33081c4cf7a1",
      "userName": "jrsap",
      "name": {
        "familyName": "Thompson",
        "givenName": "Alexander"
      },
      "active": true,
      "userType": "EMPLOYEE",
      "meta": {
        "resourceType": "User",
        "created": "2022-05-19T07:41:39Z",
        "lastModified": "2022-06-01T03:43:17Z",
        "location": "/rest/iam/scim/v2/Users/34c182f3-ba70-4d84-915a-33081c4cf7a1",
        "version": "2.0.0"
      }
    }
  ]
}
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer | IntegerType |
| boolean | BooleanType |
| string (date-time) | TimestampType |
| array | ArrayType |
| object | StructType |

## Sources and References

- **SAP SuccessFactors API Spec**: PLTScim.json
- **SAP Help Portal**: [SCIM APIs](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/534356acc0ab4b0e8977ebfb2eb432f7/895a0d10d4984152b9f6d0cd9f9f850c.html)
- **SCIM Protocol**: [RFC 7643](https://tools.ietf.org/html/rfc7644)
