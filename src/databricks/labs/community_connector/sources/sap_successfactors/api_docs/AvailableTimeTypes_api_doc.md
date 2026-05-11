# Time Off Available Time Types API Documentation

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
| Available Time Types | Time types available to the login user based on their Time Profile |

## Object Schema

### AvailableTimeType

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| externalCode | string | External code identifier for the time type | "VAC-DAYS" |
| externalName | string | Display name of the time type | "Vacation (Days)" |
| unit | string | Unit of measurement for the time type | "Day(s)" |
| undeterminedEndDateAllowed | boolean | Whether undetermined end dates are allowed | false |
| leaveOfAbsence | boolean | Whether this is a leave of absence type | true |
| mainAbsenceTimeType | boolean | Whether this is the main absence time type | true |
| flexibleRequestingAllowed | boolean | Whether flexible requesting is allowed | false |
| allowedFractions | AllowedFraction | Allowed fraction values for the time type | "FULL_DAY" |

### AllowedFraction (Enum)

| Value | Description |
|-------|-------------|
| FULL_DAY | Full day fractions only |
| HALF_DAY | Half day fractions allowed |
| QUARTER_DAY | Quarter day fractions allowed |
| FULL_HOURS | Full hours only |
| HOURS_MINUTES | Hours and minutes allowed |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| AvailableTimeType | externalCode |

## Object's Ingestion Type

**Recommended Ingestion Type**: `snapshot`

**Analysis**:
- The API returns available time types for a specific date (`$at` parameter)
- No timestamp-based filtering for incremental updates
- No modification tracking or audit fields available
- No delete tracking capability
- Data represents point-in-time configuration based on user's Time Profile

## Read API for Data Retrieval

### Get Available Time Types

**Endpoint**: `GET /rest/timemanagement/absence/v1/availableTimeTypes`

**Base URL**: `https://{api-server}`

**Query Parameters**:

| Parameter | Required | Type | Description | Example |
|-----------|----------|------|-------------|---------|
| $at | Yes | string | Date for which available time types are requested (YYYY-MM-DD format) | "2020-03-17" |
| absenceRequestCode | No | string | External code of an existing absence to filter the list | "b80d6c95369f415dafe13c5f7527513c" |

**Example Request**:
```http
GET https://apisalesdemo2.successfactors.eu/rest/timemanagement/absence/v1/availableTimeTypes?$at=2020-03-17
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Example Response**:
```json
{
  "value": [
    {
      "externalCode": "VAC-DAYS",
      "externalName": "Vacation (Days)",
      "unit": "Day(s)",
      "undeterminedEndDateAllowed": false,
      "leaveOfAbsence": false,
      "mainAbsenceTimeType": true,
      "flexibleRequestingAllowed": false,
      "allowedFractions": "QUARTER_DAY"
    },
    {
      "externalCode": "SICKNESS",
      "externalName": "Sick Leave",
      "unit": "Day(s)",
      "undeterminedEndDateAllowed": false,
      "leaveOfAbsence": false,
      "mainAbsenceTimeType": false,
      "flexibleRequestingAllowed": false,
      "allowedFractions": "HALF_DAY"
    }
  ]
}
```

**Pagination**: No pagination support - returns all available time types for the user in a single response.

**Response Codes**:

| Code | Description |
|------|-------------|
| 200 | Success - Returns list of available time types |
| 400 | Bad Request - Invalid date format or validation error |
| 403 | Forbidden - User lacks permission to access resource |
| 404 | Not Found - Resource not found |
| 500 | Internal Server Error |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| boolean | BooleanType |
| enum (AllowedFraction) | StringType |

## Sources and References

- **SAP SuccessFactors API Spec**: AvailableTimeTypes.json
- **SAP Help Portal**: [Time Off Available Time Types API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_EMPLOYEE_CENTRAL/68de09dff990417b9f0acf6ccc13a14d/4db5c8fb7d644fd187d556ea712927f6.html?state=LATEST)
- **API Type**: REST
- **API Version**: 1.0.0
