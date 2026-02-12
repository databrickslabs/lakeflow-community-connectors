# Family and Medical Leave Act (FMLA) Request API Documentation

## Authorization
- **Method**: OAuth 2.0 (Client Credentials Flow)
- **Security Scheme**: sfOauth
- **Token Endpoint**: `https://{api-server}/oauth/token`
- **Headers**:
  - `Authorization: Bearer <access_token>`
- **Documentation**: [SAP SuccessFactors OAuth 2.0](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/d9a9545305004187986c866de2b66987.html?locale=en-US)

## Object List
The API provides access to the following resources:

| Resource | Description |
|----------|-------------|
| FMLA Request | Family and Medical Leave Act requests for third-party integration |

## Object Schema

### FMLARequest
| Field | Type | Key | Description |
|-------|------|-----|-------------|
| Assignment Id | string | Yes | Assignment ID |
| Id | string | Yes | Request ID |
| Provider Name | string | No | Name of the FMLA provider |
| FMLA Start date | string | No | Start date of FMLA leave |
| FMLA End date | string | No | End date of FMLA leave |
| FMLA reason | string | No | Reason for FMLA leave |
| Absence Type | string | No | Type of absence |
| Request Status | string | No | Status of the request |
| Return to work date | string | No | Expected return to work date |
| Absence Specific Information | array[IntermitentRequestDetail] | No | Details for intermittent absences |

### IntermitentRequestDetail
| Field | Type | Description |
|-------|------|-------------|
| Absence Start date | string | Start date of absence |
| Absence End Date | string | End date of absence |
| Absence start time | string | Start time of absence |
| Absence end time | string | End time of absence |
| duration days | string | Duration in days |
| duration hours | string | Duration in hours |

### BadRequest (Error Response)
| Field | Type | Description |
|-------|------|-------------|
| error.code | string | Error code (e.g., "BadRequest") |
| error.message | string | Detailed error message |

### InternalServerError (Error Response)
| Field | Type | Description |
|-------|------|-------------|
| error.code | string | Error code (e.g., "InternalError") |
| error.message | string | Detailed error message |

## Get Object Primary Keys
| Object | Primary Key Field(s) |
|--------|---------------------|
| FMLARequest | Assignment Id, Id (composite key) |

## Object's Ingestion Type
| Object | Ingestion Type | Reasoning |
|--------|----------------|-----------|
| FMLARequest | N/A (Write-only API) | This API only supports POST (create) and PATCH (update) operations. No GET endpoint is available for data retrieval. |

**Notes**:
- This is a **write-only API** designed for third-party providers to create and update FMLA requests in SAP SuccessFactors
- No read/query operations are supported
- Data ingestion from this API is not possible as there are no retrieval endpoints
- To retrieve FMLA-related data, use OData APIs for Employee Central Time Management

## Read API for Data Retrieval

### Important Notice
**This API does not support data retrieval operations.**

The FMLA Request API is designed for inbound integration only:
- **POST** `/rest/workforce/localization/v1/fmlaRequests` - Create FMLA request(s)
- **PATCH** `/rest/workforce/localization/v1/fmlaRequests` - Update FMLA request(s)

### Available Endpoints (Write Operations Only)

#### Create FMLA Request
- **Endpoint**: `POST /rest/workforce/localization/v1/fmlaRequests`
- **Base URL**: `https://{api-server}`
- **Description**: Creates new FMLA request(s) in SAP SuccessFactors

```http
POST https://apisalesdemo2.successfactors.eu/rest/workforce/localization/v1/fmlaRequests
Authorization: Bearer <access_token>
Content-Type: application/json

{
  "Assignment Id": "assign001",
  "Id": "fmla001",
  "Provider Name": "FMLA Provider Inc.",
  "FMLA Start date": "2024-01-15",
  "FMLA End date": "2024-03-15",
  "FMLA reason": "Medical Leave",
  "Absence Type": "FMLA",
  "Request Status": "Approved",
  "Return to work date": "2024-03-16",
  "Absence Specific Information": [
    {
      "Absence Start date": "2024-01-15",
      "Absence End Date": "2024-01-20",
      "Absence start time": "08:00",
      "Absence end time": "17:00",
      "duration days": "5",
      "duration hours": "40"
    }
  ]
}
```

#### Update FMLA Request
- **Endpoint**: `PATCH /rest/workforce/localization/v1/fmlaRequests`
- **Base URL**: `https://{api-server}`
- **Description**: Updates existing FMLA request(s) in SAP SuccessFactors

### Alternative for Data Retrieval
To retrieve FMLA-related absence data, consider using:
- SAP SuccessFactors OData APIs for Employee Central Time Off
- Time Account and Time Off Balance APIs

## Field Type Mapping
| API Type | Spark Type |
|----------|------------|
| string | StringType |
| array | ArrayType |
| object | StructType |

**Note**: All date/time fields in this API are represented as strings rather than typed date formats.

## Sources and References
- **SAP SuccessFactors API Spec**: sap-sf-FMLARequest-v1.json
- **SAP Help Portal**: [Family and Medical Leave Act Documentation](https://help.sap.com/docs/successfactors-employee-central/implementing-time-management-in-sap-successfactors/family-and-medical-leave-act?state=DRAFT)
