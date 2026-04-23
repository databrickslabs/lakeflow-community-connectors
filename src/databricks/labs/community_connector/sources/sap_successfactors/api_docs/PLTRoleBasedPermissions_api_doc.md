# Role Based Permissions API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **DGExpression** - Dynamic Group Expressions
2. **DynamicGroup** - Dynamic Group
3. **RBPRule** - RBP Rules
4. **DGField** - Dynamic Group Fields
5. **DGFieldValue** - Dynamic Group Field Values
6. **DGFilter** - Dynamic Group Filters
7. **RBPRole** - RBP Roles
8. **DGPeoplePool** - Dynamic Group People Pools
9. **RBPBasicPermission** - RBP Basic Permissions
10. **DGFieldOperator** - Dynamic Group Field Operators
11. **DynamicGroupDefinition** - Dynamic Group Definition

## Object Schema

### DGExpression
| Field | Type | Description |
|-------|------|-------------|
| expressionID | String (max 200) | Primary key - Expression identifier |
| operator | DGFieldOperator | Related operator object |
| values | Collection(DGFieldValue) | Related field values |

### DynamicGroup
| Field | Type | Description |
|-------|------|-------------|
| groupID | String (int64) | Primary key - Group identifier |
| groupName | String (max 100) | Name of the group |
| groupType | String (max 32) | Type of the group |
| activeMembershipCount | Integer (int32, nullable) | Count of active members |
| lastModifiedDate | DateTime (nullable) | Last modification timestamp |
| staticGroup | Boolean | Whether group is static |
| userType | String (max 32, nullable) | Type of user |
| dgExcludePools | Collection(DGPeoplePool) | Excluded people pools |
| dgIncludePools | Collection(DGPeoplePool) | Included people pools |

### RBPRule
| Field | Type | Description |
|-------|------|-------------|
| ruleId | String (int64) | Primary key - Rule identifier |
| accessGroupLevel | Integer (int32, nullable) | Access group level |
| accessUserType | String (max 32, nullable) | Access user type |
| excludeSelf | Boolean | Exclude self flag |
| includeSelf | Boolean (nullable) | Include self flag |
| myFilter | String (max 100, nullable) | Custom filter |
| relationRole | String (max 20, nullable) | Relation role |
| status | Integer (int32, nullable) | Rule status |
| targetGroupLevel | Integer (int32, nullable) | Target group level |
| targetUserType | String (max 32, nullable) | Target user type |
| accessGroups | Collection(DynamicGroup) | Access groups |
| roles | RBPRole | Related role |
| targetGroups | Collection(DynamicGroup) | Target groups |

### DGField
| Field | Type | Description |
|-------|------|-------------|
| name | String (max 200) | Primary key - Field name |
| dataType | String (max 200) | Data type of the field |
| label | String (max 200) | Display label |
| picklistId | String (max 200) | Picklist identifier |
| allowedOperators | Collection(DGFieldOperator) | Allowed operators |

### DGFieldValue
| Field | Type | Description |
|-------|------|-------------|
| fieldValue | String (max 200) | Primary key - Field value |

### DGFilter
| Field | Type | Description |
|-------|------|-------------|
| filterId | String (max 200) | Primary key - Filter identifier |
| expressions | Collection(DGExpression) | Filter expressions |
| field | DGField | Related field |

### RBPRole
| Field | Type | Description |
|-------|------|-------------|
| roleId | String (int64) | Primary key - Role identifier |
| roleName | String (max 256) | Role name |
| roleDesc | String (max 4000, nullable) | Role description |
| lastModifiedBy | String (max 100) | Last modifier |
| lastModifiedDate | DateTime | Last modification timestamp |
| userType | String (max 32, nullable) | User type |
| permissions | Collection(RBPBasicPermission) | Associated permissions |
| rules | Collection(RBPRule) | Associated rules |

### DGPeoplePool
| Field | Type | Description |
|-------|------|-------------|
| peoplePoolId | String (max 200) | Primary key - People pool identifier |
| filters | Collection(DGFilter) | Associated filters |

### RBPBasicPermission
| Field | Type | Description |
|-------|------|-------------|
| permissionId | String (int64) | Primary key - Permission identifier |
| permissionType | String (max 100) | Type of permission |
| permissionStringValue | String (max 256) | String value of permission |
| permissionLongValue | String (int64, nullable) | Long value of permission |

### DGFieldOperator
| Field | Type | Description |
|-------|------|-------------|
| token | String (max 200) | Primary key - Operator token |
| label | String (max 200) | Display label |

### DynamicGroupDefinition
| Field | Type | Description |
|-------|------|-------------|
| groupID | String (int64) | Primary key - Group identifier |
| excludedPeoplePool1 | String (nullable) | First excluded people pool |
| excludedPeoplePool2 | String (nullable) | Second excluded people pool |
| excludedPeoplePool3 | String (nullable) | Third excluded people pool |
| includedPeoplePool1 | String (nullable) | First included people pool |
| includedPeoplePool2 | String (nullable) | Second included people pool |
| includedPeoplePool3 | String (nullable) | Third included people pool |
| group | DynamicGroup | Related dynamic group |

## Get Object Primary Keys

| Entity | Primary Key Field(s) | Type |
|--------|---------------------|------|
| DGExpression | expressionID | String |
| DynamicGroup | groupID | int64 |
| RBPRule | ruleId | int64 |
| DGField | name | String |
| DGFieldValue | fieldValue | String |
| DGFilter | filterId | String |
| RBPRole | roleId | int64 |
| DGPeoplePool | peoplePoolId | String |
| RBPBasicPermission | permissionId | int64 |
| DGFieldOperator | token | String |
| DynamicGroupDefinition | groupID | int64 |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| DGExpression | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| DynamicGroup | `cdc` | Supports $filter and $orderby on `lastModifiedDate` field |
| RBPRule | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| DGField | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| DGFieldValue | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| DGFilter | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| RBPRole | `cdc` | Supports $filter and $orderby on `lastModifiedDate` field |
| DGPeoplePool | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| RBPBasicPermission | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| DGFieldOperator | `snapshot` | No lastModifiedDateTime field available for incremental filtering |
| DynamicGroupDefinition | `snapshot` | No lastModifiedDateTime field available for incremental filtering |

## Read API for Data Retrieval

### Base URL Pattern
```
https://{api-server}/odata/v2/{EntitySet}
```

### Supported Query Parameters
| Parameter | Description |
|-----------|-------------|
| $select | Select specific properties to return |
| $filter | Filter items by property values |
| $top | Limit number of items returned (default: 20) |
| $skip | Skip first n items for pagination |
| $orderby | Order items by property values |
| $expand | Expand related entities |
| $count | Include count of items |
| $search | Search items by search phrases |

### Example Requests

**Get all RBP Roles with pagination:**
```http
GET https://{api-server}/odata/v2/RBPRole?$top=100&$skip=0&$orderby=lastModifiedDate desc
```

**Get RBP Roles modified after a specific date (CDC):**
```http
GET https://{api-server}/odata/v2/RBPRole?$filter=lastModifiedDate gt datetime'2024-01-01T00:00:00'&$orderby=lastModifiedDate asc
```

**Get Dynamic Groups with expanded pools:**
```http
GET https://{api-server}/odata/v2/DynamicGroup?$expand=dgIncludePools,dgExcludePools
```

**Get a specific RBP Role by ID:**
```http
GET https://{api-server}/odata/v2/RBPRole(12345)
```

**Get RBP Role with permissions and rules:**
```http
GET https://{api-server}/odata/v2/RBPRole(12345)?$expand=permissions,rules
```

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- Default page size is 20 items
- Increment `$skip` by page size for subsequent requests
- For CDC: filter by `lastModifiedDate` and order ascending to get incremental changes

### Service Operations (Function Imports)
The API also provides the following service operations:

| Operation | Method | Description |
|-----------|--------|-------------|
| checkUserPermission | GET | Check if a user has a specific permission |
| updateStaticGroup | GET | Update members of a static group |
| getDynamicGroupsByUser | GET | Get dynamic groups for a user |
| getUsersByDynamicGroup | GET | Get users in a dynamic group |
| getUserRolesReport | GET | Get roles report for users |
| getPermissionMetadata | GET | Get permission metadata |
| getUsersPermissions | GET | Get permissions for users |
| getRolesPermissions | GET | Get permissions for roles |
| getUserRolesByUserId | GET | Get roles for a specific user |
| checkUserPermissions | POST | Check multiple permissions for a user |

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
- SAP SuccessFactors API Spec: PLTRoleBasedPermissions.json
- SAP Help Portal: [Role Based Permissions on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/1411316651974a7dae4b09a104172dc2.html)
