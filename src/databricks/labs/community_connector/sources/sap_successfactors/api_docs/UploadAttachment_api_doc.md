# Time Off Upload Attachment API Documentation

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
| Attachment | Upload files for use as content in custom fields on absence requests |

## Object Schema

### AttachmentUploadRequest (JSON)
| Field | Type | Format | Description |
|-------|------|--------|-------------|
| fileName | string | text | The name of the file to be uploaded |
| attachmentContent | string | byte (base64) | Attachment content encoded as base64 string |

### MultipartUploadRequest (Multipart Form Data)
| Field | Type | Format | Description |
|-------|------|--------|-------------|
| attachment | string | binary | The file to be uploaded |

### SuccessfulUploadResponse
| Field | Type | Description |
|-------|------|-------------|
| attachmentId | integer | The ID of the uploaded attachment |

### Error Responses

#### BadRequest
| Field | Type | Description |
|-------|------|-------------|
| error.code | string | Error code (e.g., "BadRequest") |
| error.message | string | Error message describing the issue |

#### Forbidden
| Field | Type | Description |
|-------|------|-------------|
| error.code | string | Error code (e.g., "Forbidden") |
| error.message | string | Access denied message |

#### NotFound
| Field | Type | Description |
|-------|------|-------------|
| error.code | string | Error code (e.g., "NotFound") |
| error.message | string | Resource not found message |

#### InternalServerError
| Field | Type | Description |
|-------|------|-------------|
| error.code | string | Error code (e.g., "InternalError") |
| error.message | string | Internal error message |

## Get Object Primary Keys

| Object | Primary Key Fields |
|--------|-------------------|
| Attachment | attachmentId (returned after upload) |

## Object's Ingestion Type

| Object | Ingestion Type | Reasoning |
|--------|---------------|-----------|
| Attachment | N/A | Write-only endpoint (POST); no read/list operations available |

**Note**: This API is designed for uploading attachments only. It does not provide endpoints for listing or retrieving attachments, making it unsuitable for data ingestion purposes.

## Write API for Data Upload

### Upload Attachment (JSON)
- **Method**: POST
- **URL**: `https://{api-server}/rest/timemanagement/absence/v1/attachment/upload`
- **Content-Type**: `application/json`
- **Request Body**:
```json
{
  "fileName": "test.txt",
  "attachmentContent": "SGVsbG8hCg=="
}
```
- **Example Response**:
```json
{
  "attachmentId": 140
}
```

### Upload Attachment (Multipart)
- **Method**: POST
- **URL**: `https://{api-server}/rest/timemanagement/absence/v1/attachment/upload`
- **Content-Type**: `multipart/form-data`
- **Form Field**:
  - `attachment`: Binary file content

### HTTP Status Codes
| Code | Description |
|------|-------------|
| 200 | Successful upload; returns attachment ID |
| 400 | Bad request (validation error) |
| 403 | Forbidden (insufficient permissions) |
| 404 | Not found |
| 500 | Internal server error |

## Read API for Data Retrieval

**Not Available**: This API does not provide any read endpoints for retrieving attachment data. It is a write-only API for uploading files.

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| integer | IntegerType |
| byte (base64) | BinaryType |
| binary | BinaryType |
| object | StructType |

## Sources and References
- SAP SuccessFactors API Spec: UploadAttachment.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_EMPLOYEE_CENTRAL/68de09dff990417b9f0acf6ccc13a14d/42ea8806a2e441438c7b2bce87c268e8.html
- OAuth Authentication: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/d9a9545305004187986c866de2b66987.html
