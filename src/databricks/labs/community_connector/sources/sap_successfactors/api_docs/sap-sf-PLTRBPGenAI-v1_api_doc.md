# Role-Based Permissions Generative AI Troubleshooting API Documentation

## Authorization
- **Method**: HTTP Basic Authentication
- **Headers**:
  - `Authorization`: Basic {base64_encoded_credentials}
- **Security Scheme**: BasicAuth (HTTP Basic)

## Object List

| Tag/Resource | Description |
|--------------|-------------|
| Permissions | Query RBP permissions for a user |
| Groups | Query permission groups (static and dynamic) |
| Roles | Query role names for access groups |
| Static Groups | Modify static group members |

## Object Schema

### PermissionsRequest
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| userName | string | Yes | The input user name used to query permissions |
| keyWords | string | Yes | Keywords used to search for RBP permissions |

### PermissionsResponse
| Field | Type | Description |
|-------|------|-------------|
| permissions | array | List of permission details |
| permissions[].permissionId | integer | The RBP permission ID |
| permissions[].permissionLabel | string | The label of the permission |
| permissions[].granted | boolean | Indicates whether the permission is granted |
| hasMore | boolean | Indicates whether there are more permissions available |

### GroupsRequest
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| permissionId | integer | Yes | The RBP permission ID |
| isPermissionBased | boolean | Yes | Indicates whether it is a permission group |

### GroupsResponse
| Field | Type | Description |
|-------|------|-------------|
| staticGroups | array | List of top 5 static groups |
| staticGroups[].groupId | integer | The ID of a permission group |
| staticGroups[].groupName | string | The name of the static group |
| dynamicGroups | array | List of top 5 dynamic groups |
| dynamicGroups[].groupId | integer | The unique identifier of the dynamic group |
| dynamicGroups[].groupName | string | The name of the dynamic group |

### RoleRequest
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| groupId | integer | Yes | The ID of a permission group |

### RoleResponse
| Field | Type | Description |
|-------|------|-------------|
| value | array[string] | List of role names associated with the given group ID |

### StaticGroupRequest
| Field | Type | Description |
|-------|------|-------------|
| members | array | Array of member objects |
| members[].id | string | Member ID |

### StaticGroupResponse
| Field | Type | Description |
|-------|------|-------------|
| (root) | boolean | Success indicator |

### ErrorResponse
| Field | Type | Description |
|-------|------|-------------|
| errorCode | integer | The HTTP error code |
| message | string | A description of the error |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| Permissions | permissionId |
| Groups | groupId |
| Roles | groupId (path parameter) |
| Static Groups | groupId (path parameter) |

## Object's Ingestion Type

| Object | Ingestion Type | Reasoning |
|--------|---------------|-----------|
| Permissions | `snapshot` | No timestamp filtering or incremental support; returns up to 5 results per query |
| Groups | `snapshot` | No timestamp filtering; returns up to 5 groups per query |
| Roles | `snapshot` | Simple query by groupId; no incremental support |
| Static Groups | N/A | Write-only endpoint (PATCH) |

## Read API for Data Retrieval

### Query Permissions
- **Method**: POST
- **URL**: `https://{api-server}/rest/iam/authorization/genai/v1/permissions`
- **Query Parameters**:
  - `userName` (required): The name of a user
  - `keyWords` (required): Keywords used to search for RBP permissions
- **Headers**:
  - `Accept-Language` (optional): Preferred language (e.g., 'en_US')
- **Request Body** (application/json):
```json
{
  "userName": "cgrant",
  "keyWords": "user_login"
}
```
- **Example Response**:
```json
{
  "permissions": [
    {
      "permissionId": 55,
      "permissionLabel": "user_login",
      "granted": true
    }
  ],
  "hasMore": false
}
```

### Query Groups
- **Method**: POST
- **URL**: `https://{api-server}/rest/iam/authorization/genai/v1/groups`
- **Query Parameters**:
  - `permissionId` (required): The RBP permission ID
  - `isPermissionBased` (required): Whether it is a permission group
- **Request Body** (application/json):
```json
{
  "permissionId": 123,
  "isPermissionBased": true
}
```
- **Example Response**:
```json
{
  "staticGroups": [
    {
      "groupId": 1,
      "groupName": "StaticGroupWithAllKindsOfUsers"
    }
  ],
  "dynamicGroups": [
    {
      "groupId": 2,
      "groupName": "DynamicGroup"
    }
  ]
}
```

### Query Roles
- **Method**: GET
- **URL**: `https://{api-server}/rest/iam/authorization/genai/v1/roles`
- **Query Parameters**:
  - `groupId` (required): The ID of a permission group
- **Example Response**:
```json
{
  "value": ["roleA", "roleB", "roleC", "roleD"]
}
```

### Pagination
- No standard pagination parameters available
- Permissions endpoint returns up to 5 results with `hasMore` flag
- Groups endpoint returns up to 5 static and dynamic groups

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer | IntegerType |
| boolean | BooleanType |
| array | ArrayType |
| object | StructType |

## Sources and References
- SAP SuccessFactors API Spec: sap-sf-PLTRBPGenAI-v1.json
- SAP Help Portal: https://help.sap.com/docs/successfactors-platform/implementing-role-based-permissions/role-based-permissions-troubleshooting-tool
- API Server List: https://help.sap.com/viewer/d599f15995d348a1b45ba5603e2aba9b/LATEST/en-US/af2b8d5437494b12be88fe374eba75b6.html
