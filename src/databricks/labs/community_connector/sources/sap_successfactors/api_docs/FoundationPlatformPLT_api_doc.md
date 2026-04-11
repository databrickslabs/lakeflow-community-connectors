# Common Platform APIs Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
1. **ExternalUser** - Onboarding Users
2. **PicklistOption** - Legacy Picklist Options
3. **Attachment** - Attachments
4. **PickListValueV2** - MDF PickList Value
5. **ExternalLearnerPersonalInfo** - Personal Info for Learning Users
6. **PickListV2** - MDF PickList
7. **CompanyProvisioner** - Company Provisioner
8. **ExtAddressInfo** - Address Info for Onboarding Users
9. **CurrencyConversion** - Currency Conversion
10. **InitiativeAlignmentBean** - Initiative Alignment
11. **PicklistLabel** - Legacy Picklist Labels
12. **Country** - Countries
13. **Photo** - Photos
14. **WorkOrder** - Work Orders
15. **CompetencyRating** - Competency Rating
16. **ExtEmailInfo** - Email Info for Onboarding Users
17. **VendorInfo** - Vendor Info
18. **Picklist** - Legacy Picklist
19. **ExtPersonalInfo** - Personal Info for Onboarding Users
20. **ExtPhoneInfo** - Phone Info for Learning Users
21. **TimeZone** - Time Zone
22. **ExternalLearner** - External Learners
23. **Currency** - Currencies
24. **TeamGoalOwner** - Team Goal Owner

## Object Schema

### ExternalUser

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| userId | Edm.String | 100 | Yes | User identifier (Primary Key) |
| defaultLocale | Edm.String | 32 | No | Default locale |
| is_deleted | Edm.Boolean | - | No | Deletion flag |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| loginMethod | Edm.String | 8 | No | Login method |
| password | Edm.String | 128 | No | Password |
| personGUID | Edm.String | 32 | No | Person GUID |
| personId | Edm.Decimal | - | No | Person ID |
| personIdExternal | Edm.String | 100 | No | External person ID |
| productName | Edm.String | 32 | No | Product name |
| status | Edm.String | 2 | No | Status |
| timeZone | Edm.String | 64 | No | Time zone |
| userName | Edm.String | 100 | No | User name |

### PicklistOption

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| id | Edm.Int64 | - | Yes | Option ID (Primary Key) |
| externalCode | Edm.String | 256 | No | External code |
| maxValue | Edm.Double | - | No | Maximum value |
| minValue | Edm.Double | - | No | Minimum value |
| optionValue | Edm.Double | - | No | Option value |
| sortOrder | Edm.Int32 | - | No | Sort order |
| status | Edm.String | 9 | No | Status |

### Attachment

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| attachmentId | Edm.Int64 | - | Yes | Attachment ID (Primary Key) |
| createdDate | Edm.DateTime | - | No | Creation date |
| deletable | Edm.Boolean | - | No | Can be deleted flag |
| deprecable | Edm.Boolean | - | No | Can be deprecated flag |
| description | Edm.String | 4000 | No | Description |
| documentCategory | Edm.String | 256 | No | Document category |
| documentEntityId | Edm.String | 256 | No | Document entity ID |
| documentEntityType | Edm.String | 256 | No | Document entity type |
| documentType | Edm.String | 256 | No | Document type |
| fileContent | Edm.Binary | - | No | File content (Base64) |
| fileExtension | Edm.String | 5 | No | File extension |
| fileName | Edm.String | 256 | No | File name |
| fileSize | Edm.Int32 | - | No | File size |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mimeType | Edm.String | 256 | No | MIME type |
| module | Edm.String | 100 | No | Module |
| userId | Edm.String | 100 | No | User ID |

### PickListValueV2

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| PickListV2_effectiveStartDate | Edm.DateTime | - | Yes | Effective start date (Part of composite key) |
| PickListV2_id | Edm.String | 128 | Yes | PickList ID (Part of composite key) |
| externalCode | Edm.String | 128 | Yes | External code (Part of composite key) |
| createdBy | Edm.String | 255 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| label_defaultValue | Edm.String | 255 | No | Default label |
| label_en_US | Edm.String | 255 | No | English (US) label |
| label_localized | Edm.String | 255 | No | Localized label |
| lastModifiedBy | Edm.String | 255 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mdfSystemRecordStatus | Edm.String | 255 | No | Record status |
| status | Edm.String | 255 | No | Status |

### PickListV2

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| effectiveStartDate | Edm.DateTime | - | Yes | Effective start date (Part of composite key) |
| id | Edm.String | 128 | Yes | PickList ID (Part of composite key) |
| createdBy | Edm.String | 255 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| displayOrder | Edm.String | 255 | No | Display order |
| effectiveEndDate | Edm.DateTime | - | No | Effective end date |
| lastModifiedBy | Edm.String | 255 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| name | Edm.String | 128 | No | Name |
| status | Edm.String | 255 | No | Status |

### Country

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| code | Edm.String | 128 | Yes | Country code (Part of composite key) |
| effectiveStartDate | Edm.DateTime | - | Yes | Effective start date (Part of composite key) |
| createdBy | Edm.String | 255 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| currency | Edm.String | 128 | No | Currency |
| externalName_defaultValue | Edm.String | 255 | No | Default name |
| externalName_en_US | Edm.String | 255 | No | English (US) name |
| lastModifiedBy | Edm.String | 255 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| numericCountryCode | Edm.Int64 | - | No | Numeric country code |
| status | Edm.String | 255 | No | Status |
| twoCharCountryCode | Edm.String | 255 | No | Two character country code |

### CurrencyConversion

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| code | Edm.String | 128 | Yes | Code (Part of composite key) |
| effectiveStartDate | Edm.DateTime | - | Yes | Effective start date (Part of composite key) |
| baseCurrency | Edm.String | 128 | No | Base currency |
| conversionRate | Edm.Decimal | - | No | Conversion rate |
| createdBy | Edm.String | 255 | No | Created by |
| createdDateTime | Edm.DateTime | - | No | Creation timestamp |
| effectiveEndDate | Edm.DateTime | - | No | Effective end date |
| effectiveStatus | Edm.String | 255 | No | Effective status |
| exchangeRateType | Edm.String | 128 | No | Exchange rate type |
| lastModifiedBy | Edm.String | 255 | No | Last modified by |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| toCurrency | Edm.String | 128 | No | Target currency |

### Photo

| Field | Type | Max Length | Required | Description |
|-------|------|------------|----------|-------------|
| photoId | Edm.Int64 | - | Yes | Photo ID (Primary Key) |
| userId | Edm.String | 100 | Yes | User ID |
| photoType | Edm.Int32 | - | Yes | Photo type |
| height | Edm.Int32 | - | No | Height in pixels |
| lastModified | Edm.DateTime | - | No | Last modified date |
| lastModifiedDateTime | Edm.DateTime | - | No | Last modification timestamp |
| mimeType | Edm.String | 32 | No | MIME type |
| photo | Edm.Binary | - | No | Photo content (Base64) |
| photoName | Edm.String | 128 | No | Photo name |
| width | Edm.Int32 | - | No | Width in pixels |

## Get Object Primary Keys

| Entity | Primary Keys |
|--------|--------------|
| ExternalUser | userId |
| PicklistOption | id |
| Attachment | attachmentId |
| PickListValueV2 | PickListV2_effectiveStartDate, PickListV2_id, externalCode |
| PickListV2 | effectiveStartDate, id |
| Country | code, effectiveStartDate |
| CurrencyConversion | code, effectiveStartDate |
| Photo | photoId |

## Object's Ingestion Type

**Recommended Ingestion Type by Entity:**

| Entity | Ingestion Type | Cursor Field | Rationale |
|--------|---------------|--------------|-----------|
| ExternalUser | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| PicklistOption | snapshot | - | No timestamp field for incremental |
| Attachment | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| PickListValueV2 | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| PickListV2 | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| Country | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| CurrencyConversion | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |
| Photo | cdc | lastModifiedDateTime | Supports filtering on lastModifiedDateTime |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2/{EntitySet}
```

### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| $top | integer | - | Number of records to return |
| $skip | integer | 0 | Number of records to skip |
| $filter | string | - | OData filter expression |
| $count | boolean | false | Include total count in response |
| $orderby | string | - | Sort order for results |
| $select | string | - | Fields to include in response |
| $expand | string | - | Related entities to expand |
| $search | string | - | Free text search |

### Example Requests

**Get External Users:**
```http
GET https://{api-server}/odata/v2/ExternalUser
    ?$top=100
    &$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'
    &$orderby=lastModifiedDateTime asc
```

**Get PickList Values:**
```http
GET https://{api-server}/odata/v2/PickListValueV2
    ?$top=100
    &$filter=lastModifiedDateTime gt datetime'2024-01-01T00:00:00'
    &$orderby=lastModifiedDateTime asc
```

**Get Countries with Currency:**
```http
GET https://{api-server}/odata/v2/Country
    ?$expand=currencyNav
    &$top=100
```

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- For incremental sync, filter by `lastModifiedDateTime gt datetime'{lastSyncTime}'`
- Order by `lastModifiedDateTime asc` for consistent incremental processing

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
| Edm.Binary | BinaryType |

## Sources and References
- SAP SuccessFactors API Spec: FoundationPlatformPLT.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/bf3876ea9fc641848e85677d27995ed3.html
