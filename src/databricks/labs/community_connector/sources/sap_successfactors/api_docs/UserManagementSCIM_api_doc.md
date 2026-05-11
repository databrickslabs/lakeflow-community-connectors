# User Management (SCIM) for Employee Central Payroll API Documentation

## Authorization
- **Method**: HTTP Basic Authentication with X.509 Certificate
- **Headers**:
  - `Authorization`: Basic {base64_encoded_credentials}
  - `X-CSRF-Token`: Required for write operations (obtain via GET with "Fetch" value)
- **Security Scheme**: BasicAuth (HTTP Basic)
- **Authentication Methods**: X.509 certificate-based authentication
- **Reference**: https://help.sap.com/docs/SAP_SUCCESSFACTORS_EMPLOYEE_CENTRAL_PAYROLL/185f14fbe60d4bbb8d7d5e4f8d89b24b/1ae57884eb4a43e5a7fd3bbeb229c5c1.html

## Object List

| Tag/Resource | Description |
|--------------|-------------|
| Users | SCIM 2.0 user management operations |
| Groups | SCIM 2.0 group management operations |

## Object Schema

### User (SCIM User)
| Field | Type | Required | Filterable | Description |
|-------|------|----------|------------|-------------|
| id | string | Yes | Yes | Unique user identifier |
| userUuid | string | No | No | User UUID |
| userName | string | Yes | Yes | User name |
| name | object (Name) | No | No | Name components |
| displayName | string | No | No | Display name |
| nickName | string | No | No | Nickname |
| title | string | No | No | Title |
| userType | string | No | Yes | User type |
| active | boolean | Yes | Yes | Active status |
| meta | object (Meta) | No | No | Resource metadata |
| Schemas | array[string] | Yes | No | SCIM schemas |
| emails | array[Emails] | No | No | Email addresses |
| phoneNumbers | array[PhoneNumbers] | No | No | Phone numbers |
| groups | object (Groups) | No | No | Group memberships |
| externalId | string | No | No | External identifier |
| type | string | No | No | Type |

### Name
| Field | Type | Description |
|-------|------|-------------|
| formatted | string | Formatted full name |
| familyName | string | Family/last name |
| givenName | string | Given/first name |
| middleName | string | Middle name |
| honorificPrefix | string | Prefix (e.g., Mr., Dr.) |
| honorificSuffix | string | Suffix (e.g., Jr., III) |

### Emails
| Field | Type | Description |
|-------|------|-------------|
| value | string | Email address |
| type | string | Email type (work, home, etc.) |
| primary | boolean | Whether this is the primary email |

### PhoneNumbers
| Field | Type | Description |
|-------|------|-------------|
| value | string | Phone number |
| type | string | Phone type |
| display | string | Display format |
| primary | string | Primary indicator |

### Meta
| Field | Type | Description |
|-------|------|-------------|
| resourceType | string | Resource type (User/Group) |
| created | datetime | Creation timestamp |
| lastModified | datetime | Last modification timestamp |
| location | string | Resource location URL |
| version | string | Resource version |

### SCIM Group
| Field | Type | Filterable | Description |
|-------|------|------------|-------------|
| id | string | Yes | Group identifier |
| displayName | string | Yes | Display name |
| members | array[Member] | No | Group members |
| meta | object (Meta) | No | Resource metadata |
| schemas | array[string] | No | SCIM schemas |
| GroupExtensionVersion2 | object | No | Extension data |

### Member
| Field | Type | Description |
|-------|------|-------------|
| value | any | ID of a SCIM User |
| type | string | Member type (User) |
| display | string | Display name of user |
| $ref | string | Reference to the member |

### UserQueryResponse
| Field | Type | Description |
|-------|------|-------------|
| schemas | array[string] | Response schemas |
| totalResults | integer (int64) | Total number of results |
| itemsPerPage | integer (int64) | Items per page |
| startIndex | integer (int64) | Current start index |
| Resources | array[User] | List of users |

### ScimGroupQueryResponse
| Field | Type | Description |
|-------|------|-------------|
| schemas | array[string] | Response schemas |
| totalResults | integer (int32) | Total number of results |
| itemsPerPage | integer (int32) | Items per page |
| startIndex | integer (int32) | Current start index |
| resources | array[SCIM.Group] | List of groups |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| User | id |
| Group | id |

## Object's Ingestion Type

| Object | Ingestion Type | Reasoning |
|--------|---------------|-----------|
| Users | `cdc` | Supports `meta.lastModified` filtering with operators (gt, ge, lt, le) |
| Groups | `cdc` | Supports `meta.lastModified` filtering with operators (gt, ge, lt, le) |

**Filter Support**: Both Users and Groups support filtering on `meta.lastModified` which enables change data capture:
- Operators: eq (equals), gt (greater than), lt (lower than), ne (not equals), ge (greater than or equals), le (lower than or equals)
- Boolean operators: and, or

## Read API for Data Retrieval

### Get All Users
- **Method**: GET
- **URL**: `https://{hosturl}/sap/payroll/ecp/scim/v1/Users`
- **Query Parameters**:
  - `sap-client` (optional): ECP tenant identifier
  - `startIndex` (optional): Index of first response page
  - `count` (optional): Maximum results per page (default: 20)
  - `attributes` (optional): Attributes to return (e.g., `groups[startIndex=1&count=100]`)
  - `filter` (optional): Search criteria (e.g., `meta.lastModified gt "2024-01-01T00:00:00Z"`)
- **Headers**:
  - `X-CSRF-Token`: Set to "Fetch" to retrieve token for write operations
- **Example Request**:
```
GET https://host.com/sap/payroll/ecp/scim/v1/Users?startIndex=1&count=100&filter=active eq true
```
- **Example Response**:
```json
{
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
  "totalResults": 150,
  "itemsPerPage": 100,
  "startIndex": 1,
  "Resources": [
    {
      "id": "user123",
      "userName": "jsmith",
      "displayName": "John Smith",
      "active": true,
      "Schemas": [
        "urn:ietf:params:scim:schemas:core:2.0:User",
        "urn:successfactors:scim:schemas:extension:2.0:User"
      ]
    }
  ]
}
```

### Get Specific User
- **Method**: GET
- **URL**: `https://{hosturl}/sap/payroll/ecp/scim/v1/Users/{id}`
- **Path Parameters**:
  - `id` (required): User ID
- **Query Parameters**:
  - `sap-client` (optional): ECP tenant
  - `attributes` (optional): Attributes to return

### Get All Groups
- **Method**: GET
- **URL**: `https://{hosturl}/sap/payroll/ecp/scim/v1/Groups`
- **Query Parameters**:
  - `sap-client` (optional): ECP tenant
  - `startIndex` (optional): Index of first response page
  - `count` (optional): Maximum results per page (default: 20)
  - `filter` (optional): Search criteria (displayName, meta.lastModified)
  - `attributes` (optional): Attributes to return (e.g., `members[startIndex=initial&count=100]`)
- **Headers**:
  - `X-CSRF-Token`: Set to "Fetch" to retrieve token
- **Example Request**:
```
GET https://host.com/sap/payroll/ecp/scim/v1/Groups?startIndex=1&count=50&filter=displayName eq "Admins"
```

### Get Specific Group
- **Method**: GET
- **URL**: `https://{hosturl}/sap/payroll/ecp/scim/v1/Groups/{id}`
- **Path Parameters**:
  - `id` (required): Group ID
- **Query Parameters**:
  - `sap-client` (optional): ECP tenant
  - `attributes` (optional): Attributes to return

### Pagination
- **SCIM-style pagination** using `startIndex` and `count`:
  - `startIndex`: 1-based index of first result
  - `count`: Number of results per page (default: 20)
- Response includes:
  - `totalResults`: Total count of matching resources
  - `itemsPerPage`: Number of results in current page
  - `startIndex`: Current start index
- For next page: increment `startIndex` by `count`

### Filtering
- **Supported attributes**: displayName, meta.lastModified, userName (Users), active (Users), userType (Users)
- **Operators**: eq, gt, lt, ne, ge, le, and, or
- **Examples**:
  - `filter=active eq true`
  - `filter=meta.lastModified gt "2024-01-01T00:00:00Z"`
  - `filter=displayName eq "John" and active eq true`

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer (int32) | IntegerType |
| integer (int64) | LongType |
| boolean | BooleanType |
| date-time | TimestampType |
| array | ArrayType |
| object | StructType |

## Sources and References
- SAP SuccessFactors API Spec: UserManagementSCIM.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_EMPLOYEE_CENTRAL_PAYROLL/185f14fbe60d4bbb8d7d5e4f8d89b24b/1ae57884eb4a43e5a7fd3bbeb229c5c1.html
- SCIM 2.0 RFC 7643: https://datatracker.ietf.org/doc/html/rfc7643
- SCIM 2.0 RFC 7644: https://datatracker.ietf.org/doc/html/rfc7644
