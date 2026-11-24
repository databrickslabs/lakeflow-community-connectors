# Single Sign On (SSO) API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

This API provides service operations for SSO configuration rather than standard entity sets:

1. **SPMetadataGenerator** - Service Provider Metadata Generator (response type for getSPMetadata function)

## Object Schema

### SPMetadataGenerator
| Field | Type | Description |
|-------|------|-------------|
| errorMessage | String (nullable) | Error message if operation fails |
| result | String (nullable) | SP metadata result |
| status | String (nullable) | Operation status |

## Get Object Primary Keys

This API does not expose standard entity sets with primary keys. It provides a function import (`getSPMetadata`) that returns SP metadata based on input parameters.

| Function | Required Parameters | Type |
|----------|---------------------|------|
| getSPMetadata | dcDomain | String (Data Center domain name) |
| getSPMetadata | companyId | String (Customer company ID) |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| SPMetadataGenerator | `snapshot` | This is a service operation response, not a data entity. It returns configuration metadata on demand and does not support incremental loading. |

**Note:** This API is primarily designed for configuration retrieval rather than data ingestion. It provides SP metadata for SSO setup and is typically called once during initial configuration or when SSO settings need to be verified.

## Read API for Data Retrieval

### Base URL Pattern
```
https://{api-server}/odata/v2/getSPMetadata
```

### Supported Query Parameters
| Parameter | Description | Required |
|-----------|-------------|----------|
| dcDomain | Data Center domain name (e.g., 'https://pmsalesdemo8.successfactors.com') | Yes |
| companyId | The ID for the customer company | Yes |

### Example Requests

**Get SP Metadata for a company:**
```http
GET https://{api-server}/odata/v2/getSPMetadata?dcDomain='https://pmsalesdemo8.successfactors.com'&companyId='COMPANY123'
```

### Response Structure
```json
{
  "d": {
    "errorMessage": null,
    "result": "<SP Metadata XML content>",
    "status": "SUCCESS"
  }
}
```

### Pagination Approach
- This API does not support pagination as it returns a single configuration response
- Each call returns the complete SP metadata for the specified company and data center

### Error Handling
- Check the `status` field in the response
- If status indicates failure, check the `errorMessage` field for details
- Ensure the `dcDomain` matches your actual SuccessFactors data center

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
- SAP SuccessFactors API Spec: PLTSSO.json
- SAP Help Portal: [Single Sign On (SSO) APIs on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/12bb66d126764ec382629b67e329aacf.html)
