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
3. **Changes** - Change management records (Incremental)
4. **Releases** - Release management records (Incremental)
5. **Agents** - Support agents and their information (Snapshot)
6. **Requesters** - End users and requesters (Snapshot)
7. **Locations** - Physical or virtual locations (Snapshot)
8. **Products** - Product catalog items (Snapshot)
9. **Vendors** - Vendor information (Snapshot)
10. **Assets** - Hardware and software assets (Snapshot)
11. **AssetTypes** - Asset type definitions and configurations (Snapshot)
12. **PurchaseOrders** - Purchase orders for assets (Snapshot)
13. **Software** - Software application records (Snapshot)
14. **ServiceCatalog** - Service catalog items available for requests (Snapshot)
15. **Requested Items** - Service catalog requested items (Snapshot)
16. **Conversations** - Ticket conversations, notes, and replies (Snapshot, nested under tickets)

**API Endpoints:**
- Tickets: `/api/v2/tickets`
- Problems: `/api/v2/problems`
- Changes: `/api/v2/changes`
- Releases: `/api/v2/releases`
- Agents: `/api/v2/agents`
- Requesters: `/api/v2/requesters`
- Locations: `/api/v2/locations`
- Products: `/api/v2/products`
- Vendors: `/api/v2/vendors`
- Assets: `/api/v2/assets`
- Asset Types: `/api/v2/asset_types`
- Purchase Orders: `/api/v2/purchase_orders`
- Software: `/api/v2/applications`
- Service Catalog: `/api/v2/service_catalog/items`
- Requested Items: Nested under service requests - `/api/v2/tickets/{ticket_id}/requested_items`
- Conversations: Nested under tickets - `/api/v2/tickets/{ticket_id}/conversations`

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

### **Changes Schema**

```json
{
  "id": 123,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "agent_id": 789,
  "description": "Upgrade production database",
  "description_text": "Plain text description",
  "requester_id": 456,
  "subject": "Database upgrade to v14",
  "group_id": 202,
  "priority": 2,
  "impact": 2,
  "status": 1,
  "risk": 2,
  "change_type": 1,
  "approval_status": 1,
  "planned_start_date": "2024-02-01T08:00:00Z",
  "planned_end_date": "2024-02-01T18:00:00Z",
  "department_id": 101,
  "category": "Software",
  "sub_category": "Database",
  "item_category": "Infrastructure",
  "custom_fields": {},
  "planning_fields": {
    "maintenance_window": {},
    "rollout_plan": {},
    "backout_plan": {}
  },
  "tags": [],
  "assets": [],
  "workspace_id": 1
}
```

### **Agents Schema**

```json
{
  "id": 789,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "first_name": "Jane",
  "last_name": "Smith",
  "occasional": false,
  "job_title": "IT Support Engineer",
  "email": "jane.smith@example.com",
  "work_phone_number": "+1-555-1234",
  "mobile_phone_number": "+1-555-5678",
  "department_ids": [101, 102],
  "can_see_all_tickets_from_associated_departments": true,
  "reporting_manager_id": 750,
  "address": "123 Office St, San Francisco, CA",
  "time_zone": "Pacific Time (US & Canada)",
  "time_format": "12h",
  "language": "en",
  "location_id": 301,
  "background_information": "5 years IT support experience",
  "scoreboard_level_id": 3,
  "member_of": [202, 203],
  "observer_of": [204],
  "roles": {
    "role_ids": [1, 2]
  },
  "signature": "<p>Best regards,<br>Jane Smith</p>",
  "custom_fields": {},
  "active": true,
  "has_logged_in": true,
  "workspace_id": 1
}
```

### **Requesters Schema**

```json
{
  "id": 456,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "first_name": "John",
  "last_name": "Doe",
  "job_title": "Software Engineer",
  "primary_email": "john.doe@example.com",
  "secondary_emails": ["john.d@example.com"],
  "work_phone_number": "+1-555-9876",
  "mobile_phone_number": "+1-555-5432",
  "department_ids": [101],
  "can_see_all_tickets_from_associated_departments": false,
  "reporting_manager_id": 789,
  "address": "456 Remote Way, Austin, TX",
  "time_zone": "Central Time (US & Canada)",
  "time_format": "24h",
  "language": "en",
  "location_id": 302,
  "background_information": "Engineering team member",
  "custom_fields": {},
  "is_agent": false,
  "has_logged_in": true,
  "active": true,
  "workspace_id": 1
}
```

### **AssetTypes Schema**

```json
{
  "id": 601,
  "name": "Laptop",
  "description": "Laptop computers",
  "parent_asset_type_id": null,
  "visible": true,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z"
}
```

### **ServiceCatalog Schema**

Service Catalog Items represent services available in the service catalog that users can request.

```json
{
  "id": 201,
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-02T15:30:00Z",
  "name": "New Laptop Request",
  "delivery_time": 3,
  "display_id": 201,
  "category_id": 10,
  "product_id": null,
  "quantity": 1,
  "deleted": false,
  "icon_name": "laptop",
  "group_visibility": 1,
  "item_type": 1,
  "ci_type_id": 601,
  "product_provider": null,
  "delivery_time_type": "days",
  "botified": false,
  "visibility": 1,
  "allow_attachments": true,
  "allow_quantity": true,
  "is_bundle": false,
  "create_child": false,
  "description": "Request a new laptop for employees",
  "short_description": "New laptop request"
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

### **Conversations Schema**

Conversations include notes, replies, and comments on tickets. They are nested under tickets.

```json
{
  "id": 9001,
  "created_at": "2024-01-01T12:00:00Z",
  "updated_at": "2024-01-01T12:00:00Z",
  "body": "<p>We are investigating this issue.</p>",
  "body_text": "We are investigating this issue.",
  "incoming": false,
  "private": false,
  "user_id": 789,
  "support_email": "support@example.com",
  "ticket_id": 123,
  "to_emails": ["user@example.com"],
  "from_email": "agent@example.com",
  "cc_emails": [],
  "bcc_emails": [],
  "attachments": []
}
```

## **Get Object Primary Keys**

Primary keys for Freshservice objects are **static** and defined as follows:

| Object | Primary Key Column |
|--------|-------------------|
| Tickets | `id` |
| Problems | `id` |
| Changes | `id` |
| Releases | `id` |
| Agents | `id` |
| Requesters | `id` |
| Locations | `id` |
| Products | `id` |
| Vendors | `id` |
| Assets | `id` |
| AssetTypes | `id` |
| PurchaseOrders | `id` |
| Software | `id` |
| ServiceCatalog | `id` |
| Requested Items | `id` |
| Conversations | `id` |

All objects use `id` as the unique identifier (integer/long type).

## **Object's Ingestion Type**

The ingestion type for each object determines how data should be synchronized:

| Object | Ingestion Type | Cursor Field | Description |
|--------|---------------|--------------|-------------|
| Tickets | `cdc` | `updated_at` | Supports incremental sync with updates and deletes |
| Problems | `cdc` | `updated_at` | Supports incremental sync with updates and deletes |
| Changes | `cdc` | `updated_at` | Supports incremental sync with updates and deletes |
| Releases | `cdc` | `updated_at` | Supports incremental sync with updates and deletes |
| Agents | `snapshot` | N/A | Full snapshot only |
| Requesters | `snapshot` | N/A | Full snapshot only |
| Locations | `snapshot` | N/A | Full snapshot only |
| Products | `snapshot` | N/A | Full snapshot only |
| Vendors | `snapshot` | N/A | Full snapshot only |
| Assets | `snapshot` | N/A | Full snapshot only |
| AssetTypes | `snapshot` | N/A | Full snapshot only |
| PurchaseOrders | `snapshot` | N/A | Full snapshot only |
| Software | `snapshot` | N/A | Full snapshot only |
| ServiceCatalog | `snapshot` | N/A | Full snapshot only |
| Requested Items | `snapshot` | N/A | Full snapshot only |
| Conversations | `snapshot` | N/A | Full snapshot only (nested under tickets) |

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
| `include` | string | No | Comma-separated list of related entities to include (e.g., "requester,stats") |

### **Include Parameter for Tickets**

The `include` parameter allows you to retrieve additional related data when fetching tickets. This enhances the ticket data by including related entities in a single API call, reducing the need for multiple requests.

**Supported Include Values:**

| Include Value | Description | Fields Added |
|--------------|-------------|--------------|
| `stats` | Ticket statistics and timestamps | `closed_at`, `resolved_at`, `first_responded_at` |
| `requester` | Requester information | `requester.email`, `requester.id`, `requester.mobile`, `requester.name`, `requester.phone` |
| `requested_for` | User for whom ticket was requested | `requested_for` object with user details |
| `onboarding_context` | Onboarding-related information | `onboarding_context` object |
| `offboarding_context` | Offboarding-related information | `offboarding_context` object |

**Usage Examples:**

```bash
# Include stats only
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?include=stats

# Include multiple entities
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?include=stats,requester,requested_for

# Include all supported entities
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?include=stats,requester,requested_for,onboarding_context,offboarding_context

# Combine with other parameters
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets?updated_since=2024-01-01T00:00:00Z&include=stats,requester&per_page=100
```

**Example Response with Include:**

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
      "stats": {
        "closed_at": "2024-01-03T10:00:00Z",
        "resolved_at": "2024-01-03T09:30:00Z",
        "first_responded_at": "2024-01-01T10:15:00Z"
      },
      "requester": {
        "id": 456,
        "name": "John Doe",
        "email": "john.doe@example.com",
        "phone": "+1-555-9876",
        "mobile": "+1-555-5432"
      }
    }
  ]
}
```

**Databricks Pipeline Configuration:**

The `include` parameter should be configured as a table option in your Databricks pipeline to control which related data is imported during ticket ingestion.

**In `pipeline-spec/ingest.py` or your pipeline configuration:**

```python
# Example table configuration for tickets
table_configuration = {
    "tickets": {
        "table_options": {
            "include": "stats,requester,requested_for,onboarding_context,offboarding_context"
        }
    }
}
```

This allows users to specify which related entities to include when importing tickets, providing flexibility based on their data requirements. The connector implementation will automatically append the `include` parameter to API requests when the `include` option is present in `table_options`.

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

#### **Changes**
```bash
# List all changes
GET https://YOUR_DOMAIN.freshservice.com/api/v2/changes

# List changes with incremental sync
GET https://YOUR_DOMAIN.freshservice.com/api/v2/changes?updated_since=2024-01-01T00:00:00Z&per_page=100

# Get specific change
GET https://YOUR_DOMAIN.freshservice.com/api/v2/changes/123
```

#### **Agents**
```bash
# List all agents
GET https://YOUR_DOMAIN.freshservice.com/api/v2/agents?per_page=100

# Get specific agent
GET https://YOUR_DOMAIN.freshservice.com/api/v2/agents/789
```

#### **Requesters**
```bash
# List all requesters
GET https://YOUR_DOMAIN.freshservice.com/api/v2/requesters?per_page=100

# Get specific requester
GET https://YOUR_DOMAIN.freshservice.com/api/v2/requesters/456
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

#### **Asset Types**
```bash
# List all asset types
GET https://YOUR_DOMAIN.freshservice.com/api/v2/asset_types?per_page=100

# Get specific asset type
GET https://YOUR_DOMAIN.freshservice.com/api/v2/asset_types/601
```

#### **Service Catalog**
```bash
# List all service catalog items
GET https://YOUR_DOMAIN.freshservice.com/api/v2/service_catalog/items?per_page=100

# Get specific service catalog item
GET https://YOUR_DOMAIN.freshservice.com/api/v2/service_catalog/items/201
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

#### **Conversations**
```bash
# List all conversations for a specific ticket
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets/{ticket_id}/conversations?per_page=100

# Get specific conversation
GET https://YOUR_DOMAIN.freshservice.com/api/v2/tickets/{ticket_id}/conversations/9001
```

**Note for Conversations:** This object requires a parent `ticket_id` parameter. To retrieve all conversations across all tickets, you must:
1. First retrieve all tickets
2. For each ticket, call the conversations endpoint
3. Combine results from all tickets
4. Add the `ticket_id` field to each conversation record for reference

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
| Object/JSON (well-defined) | Struct | `StructType([...])` | Well-defined nested objects (address) |
| Object/JSON (dynamic) | JSON String | `StringType()` | Dynamic objects (custom_fields, planning_fields, type_fields, etc.) |

**Special Field Notes:**

1. **Timestamps:** All datetime fields are in ISO 8601 format with timezone (e.g., `2024-01-01T10:00:00Z`)
2. **IDs:** All ID fields should be treated as Long (not Integer) to avoid overflow
3. **Custom Fields:** The `custom_fields` object contains user-defined fields with dynamic schemas. **In the connector implementation, these are stored as JSON strings** (not structs) for compatibility with Spark/Databricks. Use Spark's `from_json()` or `get_json_object()` functions to parse them.
4. **Enumerated Fields:** Fields like `status`, `priority`, `impact` use integer codes. Refer to API documentation for code meanings
5. **Well-Defined Nested Objects:** Objects like `address` (in locations, vendors) have known schemas and are mapped to `StructType`
6. **Dynamic Nested Objects:** Objects like `planning_fields`, `analysis_fields`, `type_fields`, `roles`, `ratings`, `purchase_items` have variable schemas and are stored as JSON strings in the connector implementation
7. **Arrays:** Fields like `tags`, `cc_emails` are arrays of strings

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
| Official API Docs | https://developers.freshworks.com/api-sdk/freshservice/tickets.html | 2024-12-30 | High | Tickets include parameter, stats, requester, requested_for fields |
| Freshservice Support | https://support.freshservice.com/support/solutions/articles/233753 | 2024-12-30 | High | Asset types management, configuration |
| Freshservice Support | https://msp.support.freshservice.com/support/solutions/articles/50000011683 | 2024-12-30 | High | Service catalog configuration, structure |
| Developer Docs (Knit) | https://developers.getknit.dev/docs/freshservice-usecases | 2024-12-30 | Medium | Agents API usage, conversations API structure |
| Freshservice Support | https://support.freshservice.com/support/solutions/articles/154762 | 2024-12-30 | High | Requesters management, fields |

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
   - Used for: Ticket schema details, pagination mechanics, incremental sync patterns, include parameter usage

4. **Freshservice Support - Asset Management**
   - URL: https://support.freshservice.com/support/solutions/articles/233753
   - Confidence: High (Official support documentation)
   - Used for: Asset types management, configuration, structure

5. **Freshservice MSP Support - Service Catalog**
   - URL: https://msp.support.freshservice.com/support/solutions/articles/50000011683
   - Confidence: High (Official support documentation)
   - Used for: Service catalog configuration, item structure, visibility rules

6. **Freshservice Support - Managing Requesters**
   - URL: https://support.freshservice.com/support/solutions/articles/154762
   - Confidence: High (Official support documentation)
   - Used for: Requesters management, field definitions, user attributes

7. **Knit Developer Documentation - Freshservice Use Cases**
   - URL: https://developers.getknit.dev/docs/freshservice-usecases
   - Confidence: Medium (Third-party integration documentation)
   - Used for: Agents API usage examples, conversations API structure validation

8. **Skyvia Freshservice Integration Documentation**
   - URL: https://docs.skyvia.com/connectors/cloud-sources/freshservice_connections.html
   - Confidence: Medium (Third-party integration platform)
   - Used for: Cross-reference of object list, endpoint validation

9. **CData Freshservice API Tables Reference**
   - URL: https://cdn.cdata.com/help/FAJ/api/freshservice/pg_alltablesapi.htm
   - Confidence: Medium (Third-party data connectivity platform)
   - Used for: Complete object list validation, field descriptions

**Conflict Resolution:**
- All information prioritized official Freshservice documentation over third-party sources
- Where multiple official sources existed, the most recent API v2 documentation was used
- No significant conflicts were found between sources
- Include parameter usage confirmed across multiple sources (official Freshworks docs and implementation examples)

**Known Gaps:**
- Detailed custom field schemas are instance-specific and cannot be statically defined
- Some advanced features like webhooks and app-specific endpoints were not included as they are not relevant for basic data connector implementation
- The exact behavior of workspace_id field in multi-workspace accounts may vary
- Service catalog item custom fields and child fields vary by configuration and are stored as JSON strings
- Conversation attachments structure may vary based on attachment type

