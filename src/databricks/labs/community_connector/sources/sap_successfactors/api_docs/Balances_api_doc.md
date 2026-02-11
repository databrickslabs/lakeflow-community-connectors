# Time Off Balances API Documentation

## Authorization

- **Method**: OAuth 2.0 (Client Credentials Flow)
- **Security Scheme**: `sfOauth`
- **Token Endpoint**: `https://{api-server}/oauth/token`
- **Headers**:
  - `Authorization`: Bearer {access_token}
- **More Information**: [SAP SuccessFactors OAuth 2.0 Documentation](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/d9a9545305004187986c866de2b66987.html?locale=en-US)

## Object List

| Object Name | Description |
|-------------|-------------|
| Time Account Balances | Time accounts with their respective balances for a user |
| Time Type Balances | Time types with their respective balances for the active user |
| Termination Time Account Balances | Time accounts with termination balances for a user |

## Object Schema

### TimeAccountBalanceResponse

| Field | Type | Description |
|-------|------|-------------|
| timeAccount | TimeAccountResponse | Time account details |
| balances | DetailedBalanceResponse | Detailed balance information |

### TerminationTimeAccountBalanceResponse

| Field | Type | Description |
|-------|------|-------------|
| timeAccount | TerminationTimeAccountResponse | Time account details |
| balance | AvailableBalanceResponse | Available balance with payable indicator |
| accrualSeparatedBalance | AvailableBalanceResponse | Accrual balance not yet transferred (only for 'Entitled as Transferred' method) |

### TimeAccountResponse

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| timeAccountType | TimeAccountTypeResponse | Associated time account type | - |
| externalCode | string | External code of the time account | "92f29d6bfafb40ac8cbca033744f50ad" |
| bookableStartDate | string | Start date for booking | "2023-01-01" |
| bookableEndDate | string | End date for booking | "2024-06-30" |
| nextTransferDate | string | Next transfer date | "2023-02-01" |

### TerminationTimeAccountResponse

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| timeAccountType | TimeAccountTypeResponse | Associated time account type | - |
| externalCode | string | External code of the time account | "92f29d6bfafb40ac8cbca033744f50ad" |
| bookableStartDate | string | Start date for booking | "2023-01-01" |
| bookableEndDate | string | End date for booking | "2024-06-30" |

### TimeAccountTypeResponse

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| externalCode | string | External code of the time account type | "VAC-DAYS" |
| externalName | string | Display name | "Vacation (Days)" |
| unit | string | Unit of measurement | "DAYS" |

### DetailedBalanceResponse

| Field | Type | Description |
|-------|------|-------------|
| available | SimpleBalanceResponse | Available balance |
| accrued | SimpleBalanceResponse | Accrued balance |
| earned | SimpleBalanceResponse | Earned balance |
| paidOut | SimpleBalanceResponse | Paid out balance |
| planned | SimpleBalanceResponse | Planned balance |
| taken | SimpleBalanceResponse | Taken balance |

### SimpleBalanceResponse

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| value | number | Numeric balance value | 23.533 |
| formattedWithUnit | string | Balance with unit in user's language format | "23.533 days" |
| formattedWithUnitRoundedDown | string | Balance rounded to 2 decimal places | "23.53 days" |

### AvailableBalanceResponse

Extends SimpleBalanceResponse with:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| payable | boolean | Whether balance can be paid out | false |

### TimeTypeBalanceResponse

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| mainAbsenceTimeType | boolean | Is main absence time type | true |
| favoriteTimeTypeBalance | boolean | Is favorite time type balance | false |
| timeType | TimeTypeResponse | Time type details | - |
| balance | SimpleBalanceResponse | Balance information | - |

### TimeTypeResponse

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| externalCode | string | External code | "VAC" |
| externalName | string | Display name | "Vacation (Days)" |
| unit | string | Unit of measurement | "DAYS" |
| leaveOfAbsence | boolean | Is leave of absence | false |
| undeterminedEndDateAllowed | boolean | Undetermined end date allowed | false |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| TimeAccountBalanceResponse | timeAccount.externalCode |
| TerminationTimeAccountBalanceResponse | timeAccount.externalCode |
| TimeTypeBalanceResponse | timeType.externalCode |

## Object's Ingestion Type

**Recommended Ingestion Type**: `snapshot`

**Analysis**:
- All endpoints return point-in-time balance data based on a specific date (`$at` parameter)
- No timestamp-based filtering for incremental change detection
- No modification tracking or audit fields
- Balances are calculated values, not historical records
- No delete tracking capability

## Read API for Data Retrieval

### 1. Get Time Account Balances

**Endpoint**: `GET /rest/timemanagement/absence/v1/timeAccountBalances`

**Base URL**: `https://{api-server}`

**Query Parameters**:

| Parameter | Required | Type | Description | Example |
|-----------|----------|------|-------------|---------|
| $at | Yes | string | Date for balances (YYYY-MM-DD) | "2020-03-17" |
| assignmentId | No | string | AssignmentUUID of user (defaults to logged-in user) | "08F824D550FC40C9AACA7D4760C5AA3C" |

**Example Request**:
```http
GET https://apisalesdemo2.successfactors.eu/rest/timemanagement/absence/v1/timeAccountBalances?$at=2020-03-17
Authorization: Bearer {access_token}
Content-Type: application/json
```

### 2. Get Time Type Balances

**Endpoint**: `GET /rest/timemanagement/absence/v1/timeTypeBalances`

**Base URL**: `https://{api-server}`

**Query Parameters**:

| Parameter | Required | Type | Description | Example |
|-----------|----------|------|-------------|---------|
| $at | Yes | string | Date for balances (YYYY-MM-DD) | "2020-03-17" |
| mainAbsenceTimeTypeBalanceOnly | No | boolean | Return only main absence time type balances | false |
| favoriteTimeTypeBalances | No | boolean | true=favorites only, false=non-favorites, null=all | false |

**Example Request**:
```http
GET https://apisalesdemo2.successfactors.eu/rest/timemanagement/absence/v1/timeTypeBalances?$at=2020-03-17&mainAbsenceTimeTypeBalanceOnly=false
Authorization: Bearer {access_token}
Content-Type: application/json
```

### 3. Get Termination Time Account Balances

**Endpoint**: `GET /rest/timemanagement/absence/v1/terminationTimeAccountBalances`

**Base URL**: `https://{api-server}`

**Query Parameters**:

| Parameter | Required | Type | Description | Example |
|-----------|----------|------|-------------|---------|
| $at | No | string | Termination date (YYYY-MM-DD). Required if preview=true | "2020-03-17" |
| assignmentId | No | string | AssignmentUUID of user (defaults to logged-in user) | "08F824D550FC40C9AACA7D4760C5AA3C" |
| preview | No | boolean | Simulate termination if user not terminated | true |

**Example Request**:
```http
GET https://apisalesdemo2.successfactors.eu/rest/timemanagement/absence/v1/terminationTimeAccountBalances?$at=2020-03-17&preview=true
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Example Response (Time Account Balances)**:
```json
{
  "value": [
    {
      "timeAccount": {
        "timeAccountType": {
          "externalCode": "VAC-DAYS",
          "externalName": "Leave Account for Vacation",
          "unit": "DAYS"
        },
        "externalCode": "92f29d6bfafb40ac8cbca033744f50ad",
        "bookableStartDate": "2023-01-01",
        "bookableEndDate": "2024-06-30",
        "nextTransferDate": null
      },
      "balances": {
        "available": {
          "value": 23.533,
          "formattedWithUnit": "23.533 days",
          "formattedWithUnitRoundedDown": "23.53 days"
        },
        "accrued": {
          "value": 0,
          "formattedWithUnit": "0 days",
          "formattedWithUnitRoundedDown": "0 days"
        },
        "earned": {
          "value": 25.033,
          "formattedWithUnit": "25.033 days",
          "formattedWithUnitRoundedDown": "25.03 days"
        },
        "paidOut": {
          "value": 0,
          "formattedWithUnit": "0 days",
          "formattedWithUnitRoundedDown": "0 days"
        },
        "planned": {
          "value": 0,
          "formattedWithUnit": "0 days",
          "formattedWithUnitRoundedDown": "0 days"
        },
        "taken": {
          "value": 1.5,
          "formattedWithUnit": "1.5 days",
          "formattedWithUnitRoundedDown": "1.5 days"
        }
      }
    }
  ]
}
```

**Pagination**: No pagination support - returns all balances in a single response.

**Response Codes**:

| Code | Description |
|------|-------------|
| 200 | Success - Returns balance data |
| 400 | Bad Request - Invalid parameters or validation error |
| 403 | Forbidden - User lacks permission |
| 404 | Not Found - Resource not found |
| 500 | Internal Server Error |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| number | DoubleType |
| boolean | BooleanType |
| object | StructType |
| array | ArrayType |

## Sources and References

- **SAP SuccessFactors API Spec**: Balances.json
- **SAP Help Portal**: [Time Off Balances API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_EMPLOYEE_CENTRAL/68de09dff990417b9f0acf6ccc13a14d/9872a67cc9be4066a9cc2731f9c542a8.html?state=LATEST)
- **API Type**: REST
- **API Version**: 1.0.0
