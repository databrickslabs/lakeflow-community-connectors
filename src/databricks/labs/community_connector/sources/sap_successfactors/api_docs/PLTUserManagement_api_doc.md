# User Management API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Entity Name | Description |
|-------------|-------------|
| User | System user information including login credentials, personal and organizational data |
| UserPermissions | Permission settings for user data field access |

## Object Schema

### User

| Field Name | Type | Max Length | Description |
|------------|------|------------|-------------|
| userId | String | 100 | Primary key - unique user identifier |
| username | String | 100 | Login username |
| password | String | 128 | User password (encrypted) |
| firstName | String | 128 | User's first name |
| lastName | String | 128 | User's last name |
| mi | String | 128 | Middle initial |
| email | String | - | Email address |
| gender | String | 2 | Gender code |
| dateOfBirth | DateTime | - | Date of birth |
| hireDate | DateTime | - | Hire date |
| origHireDate | DateTime | - | Original hire date |
| addressLine1 | String | 255 | Address line 1 |
| addressLine2 | String | 255 | Address line 2 |
| addressLine3 | String | - | Address line 3 |
| city | String | 255 | City |
| state | String | 20 | State/Province |
| country | String | 255 | Country |
| zipCode | String | 10 | Postal/ZIP code |
| businessPhone | String | 20 | Business phone number |
| cellPhone | String | - | Cell phone number |
| homePhone | String | - | Home phone number |
| fax | String | 20 | Fax number |
| department | String | 128 | Department |
| division | String | 128 | Division |
| location | String | 128 | Location |
| jobCode | String | 128 | Job code |
| jobTitle | String | - | Job title |
| jobFamily | String | - | Job family |
| jobLevel | String | - | Job level |
| jobRole | String | - | Job role |
| empId | String | 255 | Employee ID |
| employeeClass | String | - | Employee class |
| status | String | 17 | Employment status |
| salary | Decimal | - | Base salary |
| salaryLocal | Decimal | - | Local currency salary |
| localCurrencyCode | String | - | Local currency code |
| timeZone | String | 16 | User timezone |
| defaultLocale | String | 100 | Default locale |
| lastModified | DateTime | - | Last modification date |
| lastModifiedDateTime | DateTime | - | Last modification date/time |
| lastModifiedWithTZ | DateTime | - | Last modification with timezone |
| custom01-custom15 | String | 255 | Custom fields 1-15 |
| title | String | 255 | Title |
| salutation | String | 128 | Salutation |
| nickname | String | 128 | Nickname |
| suffix | String | 128 | Name suffix |
| defaultFullName | String | - | Default full name |
| benchStrength | String | - | Bench strength indicator |
| competency | Float | - | Competency rating |
| performance | Float | - | Performance rating |
| potential | Float | - | Potential rating |
| objective | Float | - | Objective rating |
| riskOfLoss | String | - | Risk of loss indicator |
| impactOfLoss | String | - | Impact of loss indicator |
| futureLeader | Boolean | - | Future leader flag |
| keyPosition | Boolean | - | Key position flag |
| married | Boolean | - | Married status |
| minority | Boolean | - | Minority status |
| ethnicity | String | - | Ethnicity |
| citizenship | String | - | Citizenship |
| nationality | String | - | Nationality |
| veteranDisabled | Boolean | - | Disabled veteran flag |
| veteranMedal | Boolean | - | Medal veteran flag |
| veteranProtected | Boolean | - | Protected veteran flag |
| veteranSeparated | Boolean | - | Separated veteran flag |
| payGrade | String | - | Pay grade |
| bonusTarget | Decimal | - | Bonus target amount |
| bonusBudgetAmount | Decimal | - | Bonus budget amount |
| meritTarget | DateTime | - | Merit target |
| meritEffectiveDate | DateTime | - | Merit effective date |
| dateOfCurrentPosition | DateTime | - | Date of current position |
| dateOfPosition | DateTime | - | Date of position |
| serviceDate | DateTime | - | Service date |
| totalTeamSize | Int64 | - | Total team size |

### UserPermissions

| Field Name | Type | Description |
|------------|------|-------------|
| userId | Integer (uint8) | Primary key - user identifier |
| All User fields | Integer (uint8) | Permission level for each corresponding User field |

Note: UserPermissions contains permission flags (integer values) for each field in the User entity, indicating access levels.

## Get Object Primary Keys

### User
- **Primary Key**: `userId` (String, max 100 characters)

### UserPermissions
- **Primary Key**: `userId` (Integer)

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|----------------|--------|
| User | `cdc` | Supports `$filter` on `lastModifiedDateTime` field; supports `$orderby` on `lastModifiedDateTime` |
| UserPermissions | `snapshot` | No modification timestamp field available; full snapshot required |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All Users
```http
GET /User
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | Integer | Number of records to return (default: 20) |
| $skip | Integer | Number of records to skip for pagination |
| $filter | String | OData filter expression |
| $orderby | Array | Fields to sort by |
| $select | Array | Fields to return |
| $expand | Array | Related entities to expand |
| $search | String | Search phrases |
| $count | Boolean | Include total count |

**Expandable Relations:**
- customManager
- customReports
- directReports
- hr
- hrReports
- manager
- matrixManager
- matrixReports
- proxy
- secondManager
- secondReports
- userPermissionsNav

**Example Request - Full Sync:**
```http
GET https://{api-server}/odata/v2/User?$top=1000&$select=userId,firstName,lastName,email,department,lastModifiedDateTime
```

**Example Request - Incremental (CDC):**
```http
GET https://{api-server}/odata/v2/User?$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDateTime asc&$top=1000
```

### Get Single User by Key
```http
GET /User('{userId}')
```

**Example:**
```http
GET https://{api-server}/odata/v2/User('admin')?$select=userId,firstName,lastName,email
```

### Get All User Permissions
```http
GET /UserPermissions
```

### Get Single User Permissions by Key
```http
GET /UserPermissions({userId})
```

### Service Operations

**Get User Name Format:**
```http
GET /getUserNameFormat?locale={locale}
```

**Get Password Policy:**
```http
GET /getPasswordPolicy?locale={locale}
```

### Pagination Strategy
- Use `$top` and `$skip` for offset-based pagination
- Default page size: 20 records
- Recommended page size: 1000 records for bulk operations
- For incremental sync: Order by `lastModifiedDateTime` and use cursor-based pagination

### Response Format
```json
{
  "d": {
    "results": [
      {
        "userId": "admin",
        "firstName": "John",
        "lastName": "Doe",
        "email": "john.doe@company.com",
        "lastModifiedDateTime": "/Date(1492098664000)/"
      }
    ]
  }
}
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
| Edm.Float | FloatType |
| Edm.Byte (uint8) | IntegerType |

## Sources and References
- SAP SuccessFactors API Spec: PLTUserManagement.json
- SAP Help Portal: [User Management APIs](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/b39fde677c254b68ba7704f7c0129071.html)
