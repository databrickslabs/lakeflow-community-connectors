# Time Off Events API Documentation

## Authorization
- **Method**: OAuth 2.0 (Client Credentials Flow)
- **Token Endpoint**: `https://{api-server}/oauth/token`
- **Headers**:
  - `Authorization`: Bearer {access_token}
- **Security Scheme**: sfOauth (OAuth 2.0 Client Credentials)
- **Reference**: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/d9a9545305004187986c866de2b66987.html

## Object List

| Tag/Resource | Description |
|--------------|-------------|
| Time Off Events | List of absences, public holidays, and non-working days |

## Object Schema

### TimeOffEventResponse
| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| externalCode | string | No | Unique identifier for the event |
| title | string | No | Title/name of the event |
| startDate | date | No | Start date of the event |
| endDate | date | No | End date of the event |
| startTime | string | Yes | Start time (for partial day events) |
| endTime | string | Yes | End time (for partial day events) |
| duration | number | No | Duration value |
| timeUnit | string | No | Unit of duration (e.g., "DAYS") |
| durationFormatted | string | No | Human-readable duration (e.g., "2 days") |
| crossMidnight | boolean | No | Whether the event crosses midnight |
| type | string | No | Event type: ABSENCE, PUBLIC_HOLIDAY, NON_WORKING_DAY |
| typeFormatted | string | No | Human-readable type |
| status | string | Yes | Status: PENDING, CANCELLED, APPROVED, REJECTED, PENDING_CANCELLATION |
| statusFormatted | string | Yes | Human-readable status |
| absenceDurationCategory | string | Yes | Category: MULTI_DAY, SINGLE_FULL_DAY, etc. |

### Event Types
| Type | Description |
|------|-------------|
| ABSENCE | Employee absence/time off |
| PUBLIC_HOLIDAY | Public/bank holiday |
| NON_WORKING_DAY | Non-working day (weekend, etc.) |

### Absence Status Values
| Status | Description |
|--------|-------------|
| PENDING | Awaiting approval |
| APPROVED | Approved absence |
| REJECTED | Rejected absence request |
| CANCELLED | Cancelled absence |
| PENDING_CANCELLATION | Pending cancellation approval |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| TimeOffEvent | externalCode |

## Object's Ingestion Type

| Object | Ingestion Type | Reasoning |
|--------|---------------|-----------|
| Time Off Events | `snapshot` | Requires date range filtering (startDate/endDate); no cursor-based incremental support; data is inherently time-bounded |

**Note**: While date range filtering is available, the API design is oriented toward retrieving events within a specific period rather than tracking changes over time. Each query returns events active within the specified date range.

## Read API for Data Retrieval

### Get Time Off Events
- **Method**: GET
- **URL**: `https://{api-server}/rest/timemanagement/absence/v1/events`
- **Required Query Parameters**:
  - `assignmentId`: The assignmentUUID or userId of the user
  - `types`: Event types to retrieve (comma-separated: ABSENCE, PUBLIC_HOLIDAY, NON_WORKING_DAY)
  - `startDate`: Start of the date range (YYYY-MM-DD format)
  - `endDate`: End of the date range (YYYY-MM-DD format, max 12 months from startDate)
- **Optional Query Parameters**:
  - `includePartialDayAbsences`: Include partial-day absences (default: false)
  - `excludeAbsencesStartingBeforeSelectionPeriod`: Exclude absences starting before startDate (default: false)
  - `absenceStatus`: Filter by status (comma-separated). Default: PENDING, APPROVED, PENDING_CANCELLATION
  - `$top`: Maximum number of events to return
  - `$skip`: Number of events to skip

### Example Request
```
GET https://api.successfactors.com/rest/timemanagement/absence/v1/events?assignmentId=23a8043c-e50d-4076-b0bc-8d59657fa97a&types=ABSENCE,PUBLIC_HOLIDAY&startDate=2024-02-01&endDate=2024-05-05&$top=10&$skip=0
```

### Example Response (Absence)
```json
{
  "value": [
    {
      "externalCode": "6273b315aafa4e3bb4ca2d8b01997ed6",
      "title": "VACATION",
      "startDate": "2024-02-01",
      "endDate": "2024-02-02",
      "startTime": null,
      "endTime": null,
      "duration": 2,
      "timeUnit": "DAYS",
      "durationFormatted": "2 days",
      "crossMidnight": false,
      "type": "ABSENCE",
      "typeFormatted": "Absence",
      "status": "APPROVED",
      "statusFormatted": "Approved",
      "absenceDurationCategory": "MULTI_DAY"
    }
  ]
}
```

### Example Response (Public Holiday)
```json
{
  "value": [
    {
      "externalCode": "f4b7a018788c438687a9787f4b76bb0c",
      "title": "Good Friday",
      "startDate": "2024-03-29",
      "endDate": "2024-03-29",
      "startTime": null,
      "endTime": null,
      "duration": 1,
      "timeUnit": "DAYS",
      "durationFormatted": "1 day",
      "crossMidnight": false,
      "type": "PUBLIC_HOLIDAY",
      "typeFormatted": "Public Holiday",
      "status": null,
      "statusFormatted": null,
      "absenceDurationCategory": null
    }
  ]
}
```

### Pagination
- Uses OData-style pagination with `$top` and `$skip` parameters
- Increment `$skip` by `$top` value for next page
- No explicit total count provided in response

### Date Range Limitation
- Maximum date range: 12 months between startDate and endDate

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| number | DoubleType |
| boolean | BooleanType |
| date | DateType |
| array | ArrayType |
| object | StructType |

## Sources and References
- SAP SuccessFactors API Spec: TimeOffEvents.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_EMPLOYEE_CENTRAL/68de09dff990417b9f0acf6ccc13a14d/b72aaaf5747f4fca99f68972f9ec41a2.html
- OAuth Authentication: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/d9a9545305004187986c866de2b66987.html
