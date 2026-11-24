# 360 Reviews Management API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

| Entity Name | Description |
|-------------|-------------|
| CORouteFormStatusBean | Status bean for form routing actions (sign/reject/send) |

Note: This API primarily provides service operations for completing 360 review forms rather than entity queries.

## Object Schema

### CORouteFormStatusBean

| Field Name | Type | Description |
|------------|------|-------------|
| redirectUrl | String (nullable) | URL to redirect after action |
| status | String (nullable) | Status of the form routing action |

This entity is a response bean that indicates whether a form was successfully signed, rejected, or sent to the next step in the 360 review workflow.

## Get Object Primary Keys

### CORouteFormStatusBean
- This is a response object, not a persisted entity
- No primary key - returned as result of service operations

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|----------------|--------|
| CORouteFormStatusBean | N/A | Response-only object from service operations; not suitable for data ingestion |

Note: This API is primarily for 360 review workflow operations (completing reviews) rather than data extraction. For 360 review data, refer to the Continuous Performance Management API.

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Service Operations

#### Complete 360 Review
This service operation completes a 360 review form.

```http
GET /complete360?formDataId={formDataId}&comment={comment}
```

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| formDataId | Int64 | Yes | The form data identifier |
| comment | String | No | Optional comment for the completion |

**Example Request:**
```http
GET https://{api-server}/odata/v2/complete360?formDataId=12345&comment=Review%20completed
```

**Response:**
```json
{
  "d": {
    "redirectUrl": "/sf/form/12345",
    "status": "completed"
  }
}
```

### Pagination Strategy
- Not applicable - this API provides service operations, not entity collections
- For querying 360 review data, use the Continuous Performance Management API

### OData Query Parameters
Standard OData query parameters are defined but not commonly used with this API:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| $top | Integer | 5 | Number of records to return |
| $skip | Integer | - | Number of records to skip |
| $filter | String | - | OData filter expression |
| $search | String | - | Search phrases |
| $count | Boolean | - | Include total count |

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| Edm.String | StringType |
| Edm.Int64 | LongType |

## Related APIs

For comprehensive 360 review data extraction, use these related APIs:

1. **Continuous Performance Management API** - Contains:
   - Form360Rater
   - Form360ReviewContentDetail
   - Form360RaterSection
   - FormRaterListSection

2. **Forms Management API** - Contains:
   - FormHeader (for 360 form headers)
   - FormTemplate (for 360 form templates)

## Sources and References
- SAP SuccessFactors API Spec: PMGMMultirater.json
- SAP Help Portal: [360 Reviews Management](https://help.sap.com/viewer/28bc3c8e3f214ab487ec51b1b8709adc/2105/en-US/5b323cd6b4c44743bed8c1dc9692998b.html)
