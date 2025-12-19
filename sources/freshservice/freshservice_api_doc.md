# **Freshservice API Documentation**

## **Authorization**

Freshservice API uses **HTTP Basic Authentication**. The API key should be used as the username with a dummy password (or 'X' as password).

**Authentication Method:** Basic Auth
- **Username:** Your Freshservice API Key
- **Password:** X (any dummy password)
- **Header Format:** `Authorization: Basic base64(api_key:X)`

**Example API Request with Authentication:**

```bash
curl -u YOUR_API_KEY:X \
  -H "Content-Type: application/json" \
  -X GET 'https://YOUR_DOMAIN.freshservice.com/api/v2/tickets'
```

**Note:** Replace `YOUR_API_KEY` with your actual Freshservice API key and `YOUR_DOMAIN` with your Freshservice subdomain.

**Obtaining API Key:**
1. Log in to your Freshservice account as an admin
2. Navigate to Profile Settings
3. Your API key will be available in the profile page under the "API Key" section

## **Object List**

The Freshservice API provides access to the following objects. The object list is **static** and defined below:

### Supported Objects:

1. **Tickets** - Support tickets and service requests (Incremental)
2. **Problems** - Problem records for root cause analysis (Incremental)
3. **Releases** - Release management records (Incremental)
4. **Locations** - Physical or virtual locations (Snapshot)
5. **Products** - Product catalog items (Snapshot)
6. **Vendors** - Vendor information (Snapshot)
7. **Assets** - Hardware and software assets (Snapshot)
8. **PurchaseOrders** - Purchase orders for assets (Snapshot)
9. **Software** - Software application records (Snapshot)
10. **Requested Items** - Service catalog requested items (Snapshot)

**API Endpoints:**
- Tickets: `/api/v2/tickets`
- Problems: `/api/v2/problems`
- Releases: `/api/v2/releases`
- Locations: `/api/v2/locations`
- Products: `/api/v2/products`
- Vendors: `/api/v2/vendors`
- Assets: `/api/v2/assets`
- Purchase Orders: `/api/v2/purchase_orders`
- Software: `/api/v2/applications`
- Requested Items: Nested under service requests - `/api/v2/tickets/{ticket_id}/requested_items`

## **Object Schema**

The Freshservice API does not provide a dynamic schema discovery endpoint. Schemas are **static** and defined by the API documentation. Below are the schemas for each supported object:

### **Tickets Schema**

```json
{
  "id": 123,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "subject": "Cannot access email",
  "description": "User unable to login to email",
  "description_text": "Plain text version of description",
  "priority": 1,
  "status": 2,
  "source": 2,
  "ticket_type": "Incident",
  "due_by": "2024-01-03T10:00:00Z",
  "fr_due_by": "2024-01-02T10:00:00Z",
  "is_escalated": false,
  "requester_id": 456,
  "responder_id": 789,
  "email": "user@example.com",
  "cc_emails": ["manager@example.com"],
  "fwd_emails": [],
  "spam": false,
  "deleted": false,
  "to_emails": null,
  "department_id": 101,
  "group_id": 202,
  "category": "Hardware",
  "sub_category": "Laptop",
  "item_category": "Computer",
  "tags": ["urgent", "hardware"],
  "custom_fields": {},
  "attachments": [],
  "nr_due_by": "2024-01-01T12:00:00Z",
  "nr_escalated": false,
  "urgency": 1,
  "impact": 1,
  "association_type": null,
  "associated_tickets_count": 0,
  "workspace_id": 1
}
```

### **Problems Schema**

```json
{
  "id": 123,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "agent_id": 789,
  "description": "Root cause analysis of email outage",
  "description_text": "Plain text description",
  "requester_id": 456,
  "subject": "Email service outage",
  "due_by": "2024-01-05T10:00:00Z",
  "priority": 2,
  "status": 1,
  "impact": 2,
  "known_error": false,
  "department_id": 101,
  "group_id": 202,
  "category": "Network",
  "sub_category": "Email",
  "item_category": "Service",
  "custom_fields": {},
  "tags": [],
  "analysis_fields": {
    "problem_cause": {},
    "problem_symptom": {},
    "problem_impact": {}
  },
  "assets": [],
  "workspace_id": 1
}
```

### **Releases Schema**

```json
{
  "id": 123,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "agent_id": 789,
  "group_id": 202,
  "priority": 2,
  "status": 1,
  "release_type": 1,
  "subject": "Q1 Application Release",
  "description": "Quarterly application release",
  "description_text": "Plain text description",
  "planned_start_date": "2024-02-01T08:00:00Z",
  "planned_end_date": "2024-02-01T18:00:00Z",
  "work_start_date": "2024-01-15T08:00:00Z",
  "work_end_date": "2024-01-31T18:00:00Z",
  "department_id": 101,
  "category": "Software",
  "sub_category": "Application",
  "item_category": "Release",
  "custom_fields": {},
  "planning_fields": {
    "release_notes": {},
    "deployment_plan": {}
  },
  "tags": [],
  "assets": [],
  "workspace_id": 1
}
```

### **Locations Schema**

```json
{
  "id": 301,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "name": "Headquarters",
  "parent_location_id": null,
  "primary_contact_id": 789,
  "address": {
    "line1": "123 Business Blvd",
    "line2": "Suite 100",
    "city": "San Francisco",
    "state": "CA",
    "country": "USA",
    "zipcode": "94102"
  },
  "contact_name": "Jane Smith",
  "email": "hq@example.com",
  "phone": "+1-555-0000"
}
```

### **Products Schema**

```json
{
  "id": 501,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "name": "MacBook Pro 16\"",
  "asset_type_id": 601,
  "manufacturer": "Apple Inc.",
  "status": "In Production",
  "mode_of_procurement": "Buy",
  "depreciation_type_id": null,
  "description": "16-inch MacBook Pro with M3 chip",
  "description_text": "16-inch MacBook Pro with M3 chip"
}
```

### **Vendors Schema**

```json
{
  "id": 401,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "name": "Tech Supplies Inc.",
  "description": "IT hardware and software supplier",
  "description_text": "IT hardware and software supplier",
  "primary_contact_id": null,
  "address": {
    "line1": "789 Vendor St",
    "line2": "",
    "city": "New York",
    "state": "NY",
    "country": "USA",
    "zipcode": "10001"
  },
  "contact_name": "Vendor Manager",
  "email": "contact@techsupplies.com",
  "mobile": "+1-555-7777",
  "phone": "+1-555-6666"
}
```

### **Assets Schema**

```json
{
  "id": 701,
  "display_id": 701,
  "name": "MacBook-John-Doe",
  "description": "MacBook Pro assigned to John Doe",
  "asset_type_id": 601,
  "asset_tag": "ASSET-12345",
  "impact": "low",
  "usage_type": "permanent",
  "asset_state": "In Use",
  "user_id": 456,
  "location_id": 301,
  "department_id": 101,
  "agent_id": null,
  "group_id": null,
  "assigned_on": "2024-01-01T10:00:00Z",
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "author_type": "User",
  "end_of_life": "2027-01-01T00:00:00Z",
  "discovery_enabled": false,
  "type_fields": {
    "product_601": 501,
    "vendor_601": 401,
    "cost_601": "2500.00",
    "warranty_601": "2027-01-01",
    "acquisition_date_601": "2024-01-01",
    "serial_number_601": "ABC123DEF456"
  }
}
```

### **PurchaseOrders Schema**

```json
{
  "id": 801,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "vendor_id": 401,
  "name": "Q1 Hardware Purchase",
  "po_number": "PO-2024-001",
  "vendor_details": "Tech Supplies Inc.",
  "expected_delivery_date": "2024-02-01T00:00:00Z",
  "created_by": 789,
  "status": "Open",
  "shipping_address": "123 Business Blvd, San Francisco, CA 94102",
  "billing_same_as_shipping": true,
  "billing_address": "123 Business Blvd, San Francisco, CA 94102",
  "currency_code": "USD",
  "conversion_rate": null,
  "department_id": 101,
  "discount_percentage": 0,
  "tax_percentage": 8.5,
  "shipping_cost": 0,
  "custom_fields": {},
  "purchase_items": [
    {
      "item_type": 1,
      "item_id": 501,
      "item_name": "MacBook Pro 16\"",
      "description": "MacBook Pro for new employee",
      "cost": 2500.00,
      "quantity": 5,
      "tax_percentage": 8.5,
      "total_cost": 12500.00
    }
  ]
}
```

### **Software Schema**

```json
{
  "id": 901,
  "user_count": 50,
  "installation_count": 50,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "application_type": "desktop",
  "name": "Microsoft Office 365",
  "description": "Office productivity suite",
  "notes": "Enterprise license",
  "publisher_id": 401,
  "managed_by_id": 789,
  "category": "Productivity",
  "status": "managed"
}
```

### **Requested Items Schema**

Requested Items are nested under service requests (tickets with type "Service Request"). They represent catalog items requested by users.

```json
{
  "id": 1101,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "service_item_id": 201,
  "quantity": 1,
  "stage": 1,
  "loaned": false,
  "cost_per_request": 2500.00,
  "remarks": "Urgent request for new employee",
  "delivery_time": null,
  "is_parent": false,
  "custom_fields": {},
  "fulfilled_at": null,
  "cancelled_at": null,
  "fulfillment_notes": null
}
```

## **Get Object Primary Keys**

Primary keys for Freshservice objects are **static** and defined as follows:

| Object | Primary Key Column |
|--------|-------------------|
| Tickets | `id` |
| Problems | `id` |
| Releases | `id` |
| Locations | `id` |
| Products | `id` |
| Vendors | `id` |
| Assets | `id` |
| PurchaseOrders | `id` |
| Software | `id` |
| Requested Items | `id` |

All objects use `id` as the unique identifier (integer/long type).

## **Object's Ingestion Type**

The ingestion type for each object determines how data should be synchronized:

| Object | Ingestion Type | Cursor Field | Description |
|--------|---------------|--------------|-------------|
| Tickets | `cdc` | `updated_at` | Supports incremental sync with updates and deletes |
| Problems | `cdc` | `updated_at` | Supports incremental sync with updates and deletes |
| Releases | `cdc` | `updated_at` | Supports incremental sync with updates and deletes |
| Locations | `snapshot` | N/A | Full snapshot only |
| Products | `snapshot` | N/A | Full snapshot only |
| Vendors | `snapshot` | N/A | Full snapshot only |
| Assets | `snapshot` | N/A | Full snapshot only |
| PurchaseOrders | `snapshot` | N/A | Full snapshot only |
| Software | `snapshot` | N/A | Full snapshot only |
| Requested Items | `snapshot` | N/A | Full snapshot only |

**Notes:**
- `cdc` (Change Data Capture): Supports incremental reads using `updated_at` timestamp, can track updates and soft deletes
- `snapshot`: Requires full table reads, no incremental capability
- `append`: Not applicable for Freshservice objects

## **Read API for Data Retrieval**

### **Base URL Format**
```
https://{domain}.freshservice.com/api/v2/{resource}
```

### **HTTP Method**
`GET` for all list operations

### **Common Query Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `page` | integer | No | Page number for pagination (default: 1) |
| `per_page` | integer | No | Number of records per page (default: 30, max: 100) |
| `updated_since` | string (ISO 8601) | No | Filter records updated after this timestamp (for incremental objects) |
| `include` | string | No | Include additional data (e.g., "requester", "stats") |

### **Pagination**

Freshservice uses **page-based pagination**:
- Default: 30 records per page
- Maximum: 100 records per page
- Page numbers start at 1
- Response includes `Link` header with pagination links (rel="next", rel="prev", rel="first", rel="last")

**Example Pagination Request:**
```bash
curl -u YOUR_API_KEY:X \
  -H "Content-Type: application/json" \
  -X GET 'https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?page=1&per_page=100'
```

**Response Headers:**
```
Link: <https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?page=2&per_page=100>; rel="next",
      <https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?page=1&per_page=100>; rel="first",
      <https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?page=10&per_page=100>; rel="last"
```

### **Incremental Data Retrieval**

For objects that support incremental sync (Tickets, Problems, Changes, Releases), use the `updated_since` parameter:

**Format:** ISO 8601 datetime string (e.g., `2024-01-01T00:00:00Z`)

**Example Incremental Request:**
```bash
curl -u YOUR_API_KEY:X \
  -H "Content-Type: application/json" \
  -X GET 'https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?updated_since=2024-01-01T00:00:00Z&per_page=100'
```

**Response Format:**
```json
{
  "tickets": [
    {
      "id": 123,
      "subject": "Cannot access email",
      "status": 2,
      "priority": 1,
      "created_at": "2024-01-01T10:00:00Z",
      "updated_at": "2024-01-02T15:30:00Z",
      "deleted": false
    }
  ]
}
```

### **Deleted Records**

Freshservice uses **soft deletes** for most objects. Deleted records are indicated by the `deleted` field set to `true` in the response.

**Deleted Records in Incremental Sync:**
- Deleted records appear in the `updated_since` query results
- The `deleted` field will be `true`
- The `updated_at` timestamp reflects when the record was deleted
- No separate API endpoint needed for deleted records

**Example Response with Deleted Record:**
```json
{
  "tickets": [
    {
      "id": 124,
      "subject": "Old ticket",
      "deleted": true,
      "updated_at": "2024-01-02T14:00:00Z"
    }
  ]
}
```

### **Object-Specific Read Examples**

#### **Tickets**
```bash
# List all tickets
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets

# List tickets with incremental sync
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?updated_since=2024-01-01T00:00:00Z&per_page=100

# Get specific ticket
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets/123
```

#### **Problems**
```bash
# List all problems
GET https://YOUR_DOMAIN.freshservice.com/api/v2/problems

# List problems with incremental sync
GET https://YOUR_DOMAIN.freshservice.com/api/v2/problems?updated_since=2024-01-01T00:00:00Z&per_page=100
```

#### **Releases**
```bash
# List all releases
GET https://YOUR_DOMAIN.freshservice.com/api/v2/releases

# List releases with incremental sync
GET https://YOUR_DOMAIN.freshservice.com/api/v2/releases?updated_since=2024-01-01T00:00:00Z&per_page=100
```

#### **Locations**
```bash
# List all locations
GET https://YOUR_DOMAIN.freshservice.com/api/v2/locations?per_page=100
```

#### **Products**
```bash
# List all products
GET https://YOUR_DOMAIN.freshservice.com/api/v2/products?per_page=100
```

#### **Vendors**
```bash
# List all vendors
GET https://YOUR_DOMAIN.freshservice.com/api/v2/vendors?per_page=100
```

#### **Assets**
```bash
# List all assets
GET https://YOUR_DOMAIN.freshservice.com/api/v2/assets?per_page=100

# Get specific asset
GET https://YOUR_DOMAIN.freshservice.com/api/v2/assets/701
```

#### **Purchase Orders**
```bash
# List all purchase orders
GET https://YOUR_DOMAIN.freshservice.com/api/v2/purchase_orders?per_page=100
```

#### **Software**
```bash
# List all software applications
GET https://YOUR_DOMAIN.freshservice.com/api/v2/applications?per_page=100
```

#### **Requested Items**
```bash
# List requested items for a specific service request (ticket)
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets/{ticket_id}/requested_items
```

**Note for Requested Items:** This object requires a parent `ticket_id` parameter. To retrieve all requested items across all service requests, you must:
1. First retrieve all tickets with `ticket_type = "Service Request"`
2. For each service request ticket, call the requested_items endpoint
3. Combine results from all service requests

### **Rate Limits**

**Freshservice API Rate Limits:**
- **Default Limit:** 1000 requests per hour per API key (across all endpoints)
- **Rate Limit Headers:** The API returns rate limit information in response headers:
  - `X-RateLimit-Total`: Total requests allowed per hour
  - `X-RateLimit-Remaining`: Remaining requests in current window
  - `X-RateLimit-Used-CurrentRequest`: Requests consumed by current call
- **429 Response:** When rate limit is exceeded, the API returns HTTP 429 (Too Many Requests)
- **Retry-After Header:** Indicates seconds to wait before retrying

**Best Practices:**
- Implement exponential backoff when receiving 429 responses
- Monitor rate limit headers to avoid hitting limits
- Use `per_page=100` to minimize number of requests
- For large datasets, space out requests to stay under the hourly limit

**Example Rate Limit Headers:**
```
X-RateLimit-Total: 1000
X-RateLimit-Remaining: 950
X-RateLimit-Used-CurrentRequest: 1
```

### **Error Handling**

**Common HTTP Status Codes:**

| Status Code | Meaning | Description |
|------------|---------|-------------|
| 200 | OK | Request successful |
| 400 | Bad Request | Invalid request parameters |
| 401 | Unauthorized | Invalid or missing API key |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Resource not found |
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Internal Server Error | Server-side error |

**Error Response Format:**
```json
{
  "code": "invalid_credentials",
  "message": "Authentication failed. Please check your API key."
}
```

## **Field Type Mapping**

Mapping of Freshservice API field types to standard data types for use in data pipelines:

| Freshservice Type | Standard Type | PySpark Type | Description |
|------------------|---------------|--------------|-------------|
| Integer ID fields (id, *_id) | Long | `LongType()` | Unique identifiers and foreign keys |
| String | String | `StringType()` | Text fields |
| Text (description) | String | `StringType()` | Long text fields |
| Boolean | Boolean | `BooleanType()` | True/false fields |
| Datetime (ISO 8601) | Timestamp | `TimestampType()` | Date and time fields (created_at, updated_at, etc.) |
| Integer (priority, status) | Integer | `IntegerType()` | Enumerated values |
| Float/Decimal (cost) | Decimal | `DecimalType(10,2)` | Currency and decimal fields |
| Array | Array | `ArrayType(StringType())` | Lists (tags, cc_emails, etc.) |
| Object/JSON | Struct | `StructType([...])` | Nested objects (custom_fields, address, etc.) |

**Special Field Notes:**

1. **Timestamps:** All datetime fields are in ISO 8601 format with timezone (e.g., `2024-01-01T10:00:00Z`)
2. **IDs:** All ID fields should be treated as Long (not Integer) to avoid overflow
3. **Custom Fields:** The `custom_fields` object contains user-defined fields with dynamic schemas. Use `MapType(StringType(), StringType())` or parse as JSON string
4. **Enumerated Fields:** Fields like `status`, `priority`, `impact` use integer codes. Refer to API documentation for code meanings
5. **Nested Objects:** Objects like `address`, `planning_fields`, `analysis_fields` should be mapped to StructType
6. **Arrays:** Fields like `tags`, `cc_emails` are arrays of strings

**Priority Values (Common across Tickets, Problems, Changes, Releases):**
- 1: Low
- 2: Medium
- 3: High
- 4: Urgent

**Ticket Status Values:**
- 2: Open
- 3: Pending
- 4: Resolved
- 5: Closed

**Asset State Values:**
- In Stock
- In Use
- Loaned
- Lost
- Damaged
- In Repair
- Retired

## **Write API**

The Freshservice API supports write operations (Create, Update, Delete) for most objects. Write operations use POST, PUT, and DELETE HTTP methods.

### **Create Operations (POST)**

**Example: Create a Ticket**
```bash
curl -u YOUR_API_KEY:X \
  -H "Content-Type: application/json" \
  -X POST \
  -d '{
    "subject": "Unable to login",
    "description": "User cannot access their account",
    "email": "user@example.com",
    "priority": 2,
    "status": 2
  }' \
  'https://YOUR_DOMAIN.freshservice.com/api/v2/tickets'
```

**Response:**
```json
{
  "ticket": {
    "id": 125,
    "subject": "Unable to login",
    "description": "User cannot access their account",
    "status": 2,
    "priority": 2,
    "created_at": "2024-01-03T10:00:00Z",
    "updated_at": "2024-01-03T10:00:00Z"
  }
}
```

### **Update Operations (PUT)**

**Example: Update a Ticket**
```bash
curl -u YOUR_API_KEY:X \
  -H "Content-Type: application/json" \
  -X PUT \
  -d '{
    "status": 4,
    "priority": 1
  }' \
  'https://YOUR_DOMAIN.freshservice.com/api/v2/tickets/125'
```

**Response:**
```json
{
  "ticket": {
    "id": 125,
    "subject": "Unable to login",
    "status": 4,
    "priority": 1,
    "updated_at": "2024-01-03T11:00:00Z"
  }
}
```

### **Delete Operations (DELETE)**

**Example: Delete a Ticket**
```bash
curl -u YOUR_API_KEY:X \
  -X DELETE \
  'https://YOUR_DOMAIN.freshservice.com/api/v2/tickets/125'
```

**Response:**
```
HTTP 204 No Content
```

**Note:** Deleting a record typically performs a soft delete (sets `deleted=true`) rather than permanently removing it from the database.

### **Validation**

After write operations, you can validate the changes using the Read API:

```bash
# Read back the updated ticket
curl -u YOUR_API_KEY:X \
  -H "Content-Type: application/json" \
  -X GET 'https://YOUR_DOMAIN.freshservice.com/api/v2/tickets/125'
```

### **Write Operations Support by Object**

| Object | Create (POST) | Update (PUT) | Delete (DELETE) |
|--------|--------------|-------------|----------------|
| Tickets | ✓ | ✓ | ✓ |
| Problems | ✓ | ✓ | ✓ |
| Releases | ✓ | ✓ | ✓ |
| Locations | ✓ | ✓ | ✓ |
| Products | ✓ | ✓ | ✓ |
| Vendors | ✓ | ✓ | ✓ |
| Assets | ✓ | ✓ | ✓ |
| PurchaseOrders | ✓ | ✓ | ✓ |
| Software | ✓ | ✓ | ✓ |
| Requested Items | ✓ | ✓ | ✓ |

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|-----------|-------------------|
| Official API Docs | https://api.freshservice.com/v1 | 2024-12-19 | High | Base endpoints, authentication method, all object schemas |
| Official Developer Portal | https://developers.freshservice.com | 2024-12-19 | High | API structure, SDK examples, rate limits |
| Official API Docs | https://developers.freshworks.com/api-sdk/freshservice/tickets.html | 2024-12-19 | High | Ticket schema, pagination, incremental sync with updated_since |
| Integration Docs (Skyvia) | https://docs.skyvia.com/connectors/cloud-sources/freshservice_connections.html | 2024-12-19 | Medium | Object list confirmation, endpoint paths |
| Integration Docs (CData) | https://cdn.cdata.com/help/FAJ/api/freshservice/pg_alltablesapi.htm | 2024-12-19 | Medium | Complete object list, table descriptions |

## **Sources and References**

1. **Official Freshservice API Documentation**
   - URL: https://api.freshservice.com/v1
   - Confidence: High (Official documentation)
   - Used for: Authentication, all endpoint definitions, request/response formats, schemas

2. **Freshservice Developers Portal**
   - URL: https://developers.freshservice.com
   - Confidence: High (Official developer resources)
   - Used for: API SDK examples, best practices, rate limiting policies

3. **Freshworks API SDK Documentation - Tickets**
   - URL: https://developers.freshworks.com/api-sdk/freshservice/tickets.html
   - Confidence: High (Official SDK documentation)
   - Used for: Ticket schema details, pagination mechanics, incremental sync patterns

4. **Skyvia Freshservice Integration Documentation**
   - URL: https://docs.skyvia.com/connectors/cloud-sources/freshservice_connections.html
   - Confidence: Medium (Third-party integration platform)
   - Used for: Cross-reference of object list, endpoint validation

5. **CData Freshservice API Tables Reference**
   - URL: https://cdn.cdata.com/help/FAJ/api/freshservice/pg_alltablesapi.htm
   - Confidence: Medium (Third-party data connectivity platform)
   - Used for: Complete object list validation, field descriptions

**Conflict Resolution:**
- All information prioritized official Freshservice documentation over third-party sources
- Where multiple official sources existed, the most recent API v2 documentation was used
- No significant conflicts were found between sources

**Known Gaps:**
- Detailed custom field schemas are instance-specific and cannot be statically defined
- Some advanced features like webhooks and app-specific endpoints were not included as they are not relevant for basic data connector implementation
- The exact behavior of workspace_id field in multi-workspace accounts may vary

