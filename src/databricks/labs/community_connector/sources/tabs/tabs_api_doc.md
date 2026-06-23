# Tabs Platform API Documentation

## Authorization

Tabs uses **API key authentication** via a single HTTP header. There is no OAuth flow — the key is passed directly on every request.

**Header format:**
```
Authorization: <YOUR_API_KEY>
```

**How to obtain an API key:**
1. Log in to the Tabs application.
2. Navigate to the **Developers** section (admin privileges required).
3. Create a new API key and copy it.
4. If you lack admin privileges, contact a Tabs administrator.

**Security notes:**
- Treat the API key like a password — never expose it in client-side code or public repositories.
- To rotate a compromised key, contact Tabs Support.

**Example authenticated request:**
```http
GET https://integrators.prod.api.tabsplatform.com/v3/invoices?page=1&limit=50
Authorization: <YOUR_API_KEY>
```

**Base URL:**
```
https://integrators.prod.api.tabsplatform.com
```

All endpoints use API version `v3`.

---

## Object List

The Tabs Platform API exposes eight core objects relevant to analytics ingestion. The object list is **static** (not retrievable via an API call):

| Object | Description | Analytics Priority |
|--------|-------------|-------------------|
| **Invoices** | Itemized payment requests sent to customers | High |
| **Payments** | Transaction receipts settling invoices | High |
| **Customers** | Businesses or individuals being billed | High |
| **Contracts** | Billing agreements defining terms and obligations | High |
| **Obligations** | Customer commitments tied to billing schedules | High |
| **Items** | Products/services appearing on invoices and obligations | Medium |
| **Categories** | Revenue category groupings for products/services | Medium |
| **Events** (Legacy) | Usage consumption tracking | Medium |

Objects are not nested under one another at the API level — each has its own top-level list endpoint. However, logically:
- Invoices derive from Obligations.
- Obligations are attached to Contracts.
- Contracts are associated with Customers.
- Invoice line items reference Items.

---

## Object Schema

### Invoices

**List endpoint:** `GET /v3/invoices`

**Get single invoice:** `GET /v3/invoices/{id}` (path parameter: `id`)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique invoice identifier |
| `issueDate` | string (YYYY-MM-DD) | Yes | Date the invoice was created/issued |
| `dueDate` | string (YYYY-MM-DD) | Yes | Payment deadline |
| `originalIssueDate` | string (YYYY-MM-DD) | Yes | Initial issue date before any modifications |
| `originalDueDate` | string (YYYY-MM-DD) | Yes | Initial due date before any modifications |
| `sentDate` | string (YYYY-MM-DD) or null | No | Date the invoice was sent to the customer |
| `createdAt` | string (ISO 8601 datetime) | Yes | Precise timestamp of record creation |
| `lastUpdatedAt` | string (ISO 8601 datetime) | Yes | Timestamp of last modification (used for incremental sync) |
| `status` | string enum | Yes | Invoice state: `DRAFT`, `OPEN`, `PAID`, `VOID`, `UNCOLLECTIBLE` |
| `customerId` | string | Yes | Foreign key to the Customers object |
| `contractId` | string | No | Foreign key to the Contracts object |
| `obligationId` | string | No | Foreign key to the Obligations object |
| `source` | string | No | Origin of the invoice (e.g., system that generated it) |
| `total` | number | Yes | Total invoice amount in customer currency |
| `balanceRemaining` | number | Yes | Outstanding balance still owed |
| `externalIds` | ExternalId[] | No | References to identifiers in external systems (ERP, CRM) |
| `lineItems` | InvoiceItem[] | Yes | Array of billing line items |

**InvoiceItem (nested under `lineItems`):**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique line item identifier |
| `name` | string | Display name (e.g., "Subscription Fee") |
| `description` | string | Additional descriptive details |
| `quantity` | number | Unit count |
| `unitPrice` | number | Per-unit cost |
| `total` | number | Line total (`quantity × unitPrice`) |
| `item` | ItemV3Dto | Reference to the underlying Item object (id, name, sku) |

**ExternalId (nested under `externalIds`):**

| Field | Type | Description |
|-------|------|-------------|
| `externalId` | string | ID in the external system |
| `sourceType` | string enum | System name: `QUICKBOOKS`, `NETSUITE`, `SALESFORCE`, `HUBSPOT`, `STRIPE`, etc. |
| `metadata` | object | Additional key-value context |

---

### Payments

**List endpoint:** `GET /v3/payments`

**Get single payment:** `GET /v3/payments/{id}` (TBD: endpoint URL pattern inferred from REST convention; not explicitly confirmed in docs)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string (UUID) | Yes | Unique payment identifier |
| `invoiceId` | string | Yes | Foreign key to the Invoices object |
| `customerId` | string | No | Foreign key to the Customers object |
| `total` | number | Yes | Payment amount |
| `receivedAt` | string (ISO 8601 datetime) | Yes | Timestamp of when funds were received |
| `status` | string enum | Yes | `COMPLETED`, `PENDING`, `FAILED` |
| `type` | string enum | Yes | `MANUAL`, `AUTOMATED` |
| `method` | string enum | Yes | Payment mechanism: `CREDIT_CARD`, `ACH`, `CHECK`, `WIRE` |
| `reference` | string | No | Tracking reference (e.g., check number) |
| `stripeId` | string | No | Stripe transaction identifier when applicable |
| `externalIds` | ExternalId[] | No | Third-party system identifiers (same structure as Invoices) |

---

### Customers

**List endpoint:** `GET /v3/customers`

**Get single customer:** `GET /v3/customers/{id}` (TBD: URL pattern inferred from REST convention)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique customer identifier |
| `name` | string | Yes | Customer business or individual name |
| `defaultCurrency` | string | Yes | ISO currency code (e.g., `USD`) |
| `primaryBillingContactName` | string | Yes | Name of the main billing contact |
| `primaryBillingContactEmail` | string | Yes | Email of the main billing contact |
| `secondaryBillingContacts` | string[] | No | Additional billing contact emails |
| `billingAddress` | Address | Yes | Primary billing address |
| `shippingAddress` | Address | No | Shipping address if different |
| `externalIds` | ExternalId[] | No | Cross-system identifiers |
| `lastUpdatedAt` | string (ISO 8601 datetime) | Yes | Last modification timestamp (used for incremental sync) |

**Address (nested under `billingAddress` / `shippingAddress`):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `address1` | string | Yes | Primary street address line |
| `address2` | string | No | Secondary street address line |
| `city` | string | Yes | City name |
| `state` | string | Yes | State or province |
| `country` | string | Yes | Country code |
| `zip` | string | Yes | Postal/ZIP code |
| `addressee` | string | No | Recipient name at address |
| `externalId` | string | No | External identifier for this address |

---

### Contracts

**List endpoint:** `GET /v3/contracts`

**Get single contract:** `GET /v3/contracts/{id}` (TBD: URL pattern inferred from REST convention)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique contract identifier |
| `name` | string | Yes | Display name visible in the UI |
| `fileName` | string | Yes | Contract file name (PDF, DOCX, etc.) |
| `status` | string enum | Yes | `ACTIVE`, `DRAFT`, `EXPIRED` |
| `customerId` | string | Yes | Foreign key to the Customers object |
| `customerName` | string | No | Denormalized customer name |
| `uploaderId` | string | No | User ID of the person who uploaded the contract |
| `source` | string | No | Origin of the contract record |
| `createdAt` | string (ISO 8601 datetime) | No | Creation timestamp |
| `lastUpdatedAt` | string (ISO 8601 datetime) | No | Last modification timestamp (used for incremental sync) |
| `deletedAt` | string (ISO 8601 datetime) or null | No | Soft-delete timestamp; present if the contract has been deleted |
| `externalIds` | ExternalIdDto[] | No | Third-party system references |

**ExternalIdDto (nested under `externalIds`):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `externalId` | string | Yes | External system identifier |
| `sourceType` | string enum | Yes | External system name (e.g., Salesforce, NetSuite) |
| `metadata` | object | No | Additional contextual information |

---

### Obligations

**List endpoint:** `GET /v3/obligations`

**Get single obligation:** `GET /v3/obligations/{id}` (TBD: URL pattern inferred from REST convention)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique obligation identifier |
| `contractId` | string | No | Foreign key to Contracts |
| `customerId` | string | No | Foreign key to Customers |
| `categoryId` | string | Yes | Foreign key to Revenue Categories |
| `serviceStartDate` | string (YYYY-MM-DD) | Yes | Billing commencement date |
| `serviceEndDate` | string (YYYY-MM-DD) | Yes | Billing termination date |
| `startDate` | string (YYYY-MM-DD) | No | Billing schedule start date |
| `endDate` | string (YYYY-MM-DD) | No | Billing schedule end date |
| `billingSchedule` | BillingSchedule | Yes | Nested billing mechanics object |
| `erpItemId` | string | No | ERP system item reference |
| `eventTypeId` | string | No | Usage-based billing event type reference |

**BillingSchedule (nested under `billingSchedule`):**

| Field | Type | Description |
|-------|------|-------------|
| `interval` | string enum | Billing frequency unit: `DAY`, `WEEK`, `MONTH`, `YEAR` |
| `intervalFrequency` | number | Number of intervals between billing cycles |
| `billingType` | string enum | `UNIT` (per-unit) or `FLAT` (flat fee) |
| `pricingType` | string enum | `SIMPLE` or `TIERED` |
| `netPaymentTerms` | number | Days until payment is due |
| `isArrears` | boolean | Whether billing occurs after the service period |
| `isRecurring` | boolean | Whether obligation recurs |
| `name` | string | Line item name on invoices |
| `description` | string | Line item description on invoices |
| `quantity` | number | Units per billing cycle |
| `invoiceType` | string | Invoice classification |

**PricingTier (nested under billing schedule for TIERED pricing):**

| Field | Type | Description |
|-------|------|-------------|
| `tierNumber` | number | Tier ordering index |
| `amount` | number | Cost for this tier |
| `amountType` | string enum | `PER_ITEM` or `TOTAL_INVOICE` |
| `tierMinimum` | number | Minimum quantity to enter this tier |

---

### Items

**List endpoint:** `GET /v3/items`

**Get single item:** `GET /v3/items/{id}` (TBD: URL pattern inferred from REST convention)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique item identifier |
| `name` | string | Yes | Display name (e.g., "Premium Subscription") |
| `sku` | string | No | Stock-keeping unit for inventory tracking |
| `externalIds` | ExternalIdV3Dto[] | No | References to external systems |
| `lastUpdatedAt` | string (ISO 8601 datetime) | Yes | Last modification timestamp |

**ExternalIdV3Dto (nested under `externalIds`):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `externalId` | string | Yes | ID in the external system |
| `sourceType` | string | Yes | System name: `QUICKBOOKS`, `NETSUITE`, `RILLET`, `SAGE_INTACCT`, `SALESFORCE`, `HUBSPOT`, `AVALARA`, `ANROK`, `STRIPE` |
| `metadata` | any | No | Additional key-value context |

---

### Categories (Revenue Categories)

**List endpoint:** `GET /v3/categories`

**Get single category:** `GET /v3/categories/{id}` (TBD: URL pattern inferred from REST convention)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique category identifier |
| `name` | string | Yes | Category display name |
| `lastUpdatedAt` | string (ISO 8601 datetime) | TBD | Last modification timestamp (TBD: not explicitly confirmed in docs) |

TBD: Full schema for Categories is not detailed in official documentation. Only `id` and `name` are mentioned as filterable fields.

---

### Events (Legacy Usage Events)

**List endpoint:** `GET /v3/events`

**Get single event:** `GET /v3/events/{id}` (TBD: URL pattern inferred)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Unique event identifier |
| `eventTypeId` | string | Yes | Foreign key to event type |
| `customerId` | string | Yes | Foreign key to Customers |
| `differentiator` | string | No | Sub-identifier for distinguishing event variants |
| `datetime` | string (YYYY-MM-DD or ISO 8601) | Yes | When the usage event occurred |

TBD: Additional fields for the Events object (quantity, value, metadata) are not explicitly listed in official documentation.

---

## Get Object Primary Keys

All objects use a single `id` field (string/UUID) as the primary key:

| Object | Primary Key | Notes |
|--------|-------------|-------|
| Invoices | `id` | UUID string |
| Payments | `id` | UUID string |
| Customers | `id` | UUID string |
| Contracts | `id` | UUID string |
| Obligations | `id` | UUID string |
| Items | `id` | UUID string |
| Categories | `id` | UUID string |
| Events | `id` | UUID string |

There are no composite primary keys. Each record is uniquely identified by its `id` field returned in the API response.

---

## Object Ingestion Type

| Object | Ingestion Type | Cursor Field | Notes |
|--------|---------------|--------------|-------|
| Invoices | `cdc` | `lastUpdatedAt` | Filterable by `lastUpdatedAt`; status changes are upserts |
| Payments | `cdc` | `receivedAt` | Filterable by `receivedAt`; status may change (PENDING → COMPLETED/FAILED) |
| Customers | `cdc` | `lastUpdatedAt` | Filterable by `lastUpdatedAt` |
| Contracts | `cdc` | `lastUpdatedAt` | Filterable by `lastUpdatedAt`; `deletedAt` field enables soft-delete detection |
| Obligations | `snapshot` | N/A | Filter fields are `startDate`, `endDate`, `serviceStartDate`, `serviceEndDate` — no `lastUpdatedAt` confirmed in filter docs |
| Items | `cdc` | `lastUpdatedAt` | Filterable by `lastUpdatedAt` (TBD: `lastUpdatedAt` filter not explicitly confirmed for items) |
| Categories | `snapshot` | N/A | TBD: No `lastUpdatedAt` filter documented; likely a small dimension table, full refresh is safe |
| Events | `append` | `datetime` | Filterable by `datetime`; usage events are immutable once created |

**Delete handling:**
- Contracts: `deletedAt` field is present in the schema. Records where `deletedAt` is non-null have been soft-deleted. Filter `deletedAt:isnotnull` to detect deletions.
- All other objects: TBD — no documented soft-delete field or hard-delete endpoint. Recommend full snapshot comparison for delete detection on small objects; for large objects, assume `cdc` without deletes unless Tabs Support confirms otherwise.

---

## Read API for Data Retrieval

### Common Pagination Pattern

All list endpoints use **offset-based pagination** with two query parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | number | 1 | Page number (1-indexed) |
| `limit` | number | 50 | Records per page |

There is no cursor-based or keyset pagination. To paginate through all records, increment `page` until a page returns fewer records than `limit` (or an empty array).

**Example — fetch page 3 with 100 records per page:**
```http
GET https://integrators.prod.api.tabsplatform.com/v3/invoices?page=3&limit=100
Authorization: <YOUR_API_KEY>
```

TBD: Maximum allowed `limit` value is not documented. Recommended safe value is `100` or `200`; test empirically.

---

### Filter Syntax

All list endpoints accept an optional `filter` query parameter with a structured syntax:

```
property:operator:"value"
```

Multiple filters are combined with AND logic using commas as separators. Values containing commas must be wrapped in double quotes.

**Supported operators:**

| Operator | Meaning |
|----------|---------|
| `eq` | Equals |
| `neq` | Not equals |
| `gt` | Greater than |
| `gte` | Greater than or equal |
| `lt` | Less than |
| `lte` | Less than or equal |
| `like` | Case-insensitive substring match |
| `nlike` | Does not contain substring |
| `in` | Matches any value in list (use `\|` as separator) |
| `nin` | Does not match any value in list |
| `isnull` | Field is null |
| `isnotnull` | Field is not null |

**Example filters:**

```http
# Invoices updated after a date (incremental sync cursor)
GET /v3/invoices?filter=lastUpdatedAt:gte:"2024-01-01"&page=1&limit=100

# Open invoices only
GET /v3/invoices?filter=status:eq:"OPEN"

# Payments by status and type
GET /v3/payments?filter=status:eq:"COMPLETED",type:eq:"AUTOMATED"

# Payments received after a date (incremental sync cursor)
GET /v3/payments?filter=receivedAt:gte:"2024-01-01"&page=1&limit=100

# Customers updated after a date
GET /v3/customers?filter=lastUpdatedAt:gte:"2024-01-01"&page=1&limit=100

# Active contracts for a specific customer
GET /v3/contracts?filter=status:eq:"ACTIVE",customerId:eq:"cust_abc123"

# Contracts updated after a date
GET /v3/contracts?filter=lastUpdatedAt:gte:"2024-01-01"&page=1&limit=100

# Events in a date range
GET /v3/events?filter=datetime:gte:"2024-01-01",datetime:lt:"2024-02-01"&page=1&limit=100
```

---

### Per-Endpoint Read Details

#### Invoices — `GET /v3/invoices`

**Filterable fields:**
- `customerId` — filter by customer
- `contractId` — filter by contract
- `obligationId` — filter by obligation
- `total` — filter by amount (numeric)
- `source` — filter by invoice origin
- `status` — filter by status (`DRAFT`, `OPEN`, `PAID`, `VOID`, `UNCOLLECTIBLE`)
- `issueDate` — date in YYYY-MM-DD format
- `dueDate` — date in YYYY-MM-DD format
- `lastUpdatedAt` — date in YYYY-MM-DD format (primary incremental cursor)

**Incremental sync approach:**
1. On first run, fetch all pages: `GET /v3/invoices?page=1&limit=100`
2. On subsequent runs, use `lastUpdatedAt` filter with the max `lastUpdatedAt` from the previous run:
   `GET /v3/invoices?filter=lastUpdatedAt:gte:"YYYY-MM-DD"&page=1&limit=100`
3. Upsert records using `id` as the primary key.
4. Apply a lookback window (e.g., 7 days) to handle clock skew.

**Note on dates:** Filter values for `lastUpdatedAt` use YYYY-MM-DD format, not full ISO 8601 timestamps. This means any record updated on the same day may be caught by the filter.

---

#### Payments — `GET /v3/payments`

**Filterable fields:**
- `receivedAt` — primary incremental cursor (date in YYYY-MM-DD format)
- `status` — `PENDING`, `COMPLETED`, `FAILED`
- `type` — `MANUAL`, `AUTOMATED`
- `invoiceId` — filter by associated invoice
- `customerId` — filter by customer

**Incremental sync approach:**
Use `receivedAt` as the cursor field. Note that `status` can change after receipt (PENDING → COMPLETED or FAILED), so a lookback window is important.

---

#### Customers — `GET /v3/customers`

**Filterable fields:**
- `name` — customer name (supports `like` for substring search)
- `externalIds.externalId` — external system ID
- `lastUpdatedAt` — date in YYYY-MM-DD format (primary incremental cursor)

**Incremental sync approach:**
Use `lastUpdatedAt` as cursor. Customers are updated when contact or address details change.

---

#### Contracts — `GET /v3/contracts`

**Filterable fields:**
- `name` — contract name
- `externalIds.externalId` — external ID
- `externalIds.sourceType` — external system type
- `source` — contract origin
- `status` — `ACTIVE`, `DRAFT`, `EXPIRED`
- `customerId` — customer filter
- `createdAt` — date in YYYY-MM-DD format
- `lastUpdatedAt` — date in YYYY-MM-DD format (primary incremental cursor)

**Delete detection:**
`deletedAt` field is present in the schema. Poll for `filter=deletedAt:isnotnull` to identify deleted contracts, or include `deletedAt` in the upserted record and filter downstream.

---

#### Obligations — `GET /v3/obligations`

**Filterable fields:**
- `contractId` — filter by contract
- `customerId` — filter by customer
- `startDate` — billing schedule start date (YYYY-MM-DD)
- `endDate` — billing schedule end date (YYYY-MM-DD)
- `serviceStartDate` — service start date (YYYY-MM-DD)
- `serviceEndDate` — service end date (YYYY-MM-DD)

**Note:** No `lastUpdatedAt` filter is documented for Obligations. Recommended ingestion type is `snapshot` (full refresh). Obligations are typically created when contracts are signed and do not change frequently.

---

#### Items — `GET /v3/items`

**Filterable fields:**
- `name` — item name
- `externalId` — external system ID
- `externalIdType` — one of `QUICKBOOKS`, `NETSUITE`, `RILLET`, `SAGE_INTACCT`, `SALESFORCE`, `HUBSPOT`, `AVALARA`, `ANROK`, `STRIPE`

**Note:** `lastUpdatedAt` filter is not explicitly documented for Items. Recommended approach: snapshot (full refresh). Items are a small dimension table (catalog of products/services) that changes infrequently.

---

#### Categories — `GET /v3/categories`

**Filterable fields:**
- `name` — category name

**Note:** No `lastUpdatedAt` filter is documented. Recommended approach: snapshot (full refresh). Categories are a small, rarely-changing dimension table.

---

#### Events — `GET /v3/events` (Legacy)

**Filterable fields:**
- `eventTypeId` — filter by event type
- `customerId` — filter by customer
- `differentiator` — sub-event identifier
- `datetime` — date in YYYY-MM-DD format (primary incremental cursor)

**Incremental sync approach:**
Use `datetime:gte:"YYYY-MM-DD"` as the cursor. Usage events are append-only (immutable once recorded).

---

### Rate Limits

TBD: The Tabs Platform API documentation does not publish explicit rate limit figures (requests per minute/hour). During testing, this researcher encountered HTTP 429 (Too Many Requests) responses when making rapid successive fetches to `docs.tabsplatform.com`.

**Recommendations:**
- Implement exponential backoff on HTTP 429 responses.
- Add a conservative inter-request delay (e.g., 0.5–1 second between pages).
- Contact Tabs Support (`support@tabsplatform.com`) to confirm rate limits before production deployment.

**Uptime / SLA:**
- Tabs guarantees **99.9% monthly uptime**, excluding scheduled maintenance.
- Maintenance windows: 9 PM – 4 AM ET, typically on weekends, not on the 1st or last day of the month.

---

## Field Type Mapping

| API Type | Description | Python/Spark Type |
|----------|-------------|-------------------|
| `string` | Plain text | `StringType` |
| `string (UUID)` | UUID format string | `StringType` |
| `string (YYYY-MM-DD)` | Date string | `DateType` (parse with `datetime.strptime`) |
| `string (ISO 8601)` | Full datetime with timezone | `TimestampType` (parse with `datetime.fromisoformat`) |
| `number` | Numeric (int or float) | `DoubleType` (safe default; use `DecimalType` for money fields) |
| `boolean` | True/False | `BooleanType` |
| `string enum` | Categorical string | `StringType` |
| `object` | Nested JSON | `StructType` or `StringType` (JSON serialized) |
| `array` | List of objects/strings | `ArrayType` or JSON serialized |

**Special field behaviors:**
- **Money fields** (`total`, `balanceRemaining`, `unitPrice`, `amount`): Returned as `number`. Use `DecimalType(18, 2)` or `DoubleType` — currency denomination is in the customer's `defaultCurrency`.
- **`lastUpdatedAt` / `createdAt`**: ISO 8601 strings. Parse as `TimestampType`.
- **`issueDate`, `dueDate`, `serviceStartDate`**: YYYY-MM-DD date strings. Parse as `DateType`.
- **`status` fields**: String enums. Treat as `StringType`; document all known enum values in schema.
- **`externalIds`**: Array of objects. Recommend flattening into a separate child table or serializing as JSON string in the parent table.
- **`lineItems`**: Array of objects. Recommend exploding into a separate `invoice_line_items` child table with `invoice_id` as a foreign key.

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs - Main Nav | https://docs.tabsplatform.com | 2026-06-23 | High | Full list of 8 core objects; integration ecosystem |
| Official Docs - Authentication | https://docs.tabsplatform.com/docs/authentication | 2026-06-23 | High | API key auth via `Authorization` header; no OAuth |
| Official Docs - Filter Rules | https://docs.tabsplatform.com/docs/filter-rules | 2026-06-23 | High | Filter syntax, all 12 operators, AND combination via comma |
| Official Docs - LLMs Index | https://docs.tabsplatform.com/llms.txt | 2026-06-23 | High | Full endpoint inventory: all CRUD operations per object |
| Official Docs - Invoices Guide | https://docs.tabsplatform.com/docs/invoices | 2026-06-23 | High | Invoice schema: all fields, types, lineItems structure |
| Official Docs - Payments Guide | https://docs.tabsplatform.com/docs/payments | 2026-06-23 | High | Payment schema: all fields, status enum, method enum |
| Official Docs - Customers Guide | https://docs.tabsplatform.com/docs/customers | 2026-06-23 | High | Customer schema: all fields, Address struct, ExternalId |
| Official Docs - Contracts Guide | https://docs.tabsplatform.com/docs/contracts | 2026-06-23 | High | Contract schema: all fields including `deletedAt` for soft deletes |
| Official Docs - Obligations Guide | https://docs.tabsplatform.com/docs/obligations | 2026-06-23 | High | Obligation schema: BillingSchedule, PricingTier, filter fields |
| Official Docs - Items Guide | https://docs.tabsplatform.com/docs/items | 2026-06-23 | High | Item schema: id, name, sku, externalIds; external system types |
| Official Docs - Invoices API Ref | https://docs.tabsplatform.com/reference/integratorsapiinvoicescontroller_getinvoices | 2026-06-23 | High | Endpoint URL, pagination params, filter fields for invoices |
| Official Docs - Payments API Ref | https://docs.tabsplatform.com/reference/integratorsapipaymentscontroller_getpayments | 2026-06-23 | High | Endpoint URL, pagination params, filter fields for payments |
| Official Docs - Customers API Ref | https://docs.tabsplatform.com/reference/integratorsapicustomerscontroller_getcustomers | 2026-06-23 | High | Endpoint URL, pagination params, filter fields for customers |
| Official Docs - Contracts API Ref | https://docs.tabsplatform.com/reference/integratorsapicontractsv3controller_getcontracts | 2026-06-23 | High | Endpoint URL, pagination params, filter fields for contracts |
| Official Docs - Obligations API Ref | https://docs.tabsplatform.com/reference/integratorsapiobligationscontroller_getobligations | 2026-06-23 | High | Endpoint URL, pagination params, filter fields for obligations |
| Official Docs - Items API Ref | https://docs.tabsplatform.com/reference/integratorsapiitemscontroller_getitems | 2026-06-23 | High | Endpoint URL, pagination params, externalIdType enum values |
| Official Docs - Categories API Ref | https://docs.tabsplatform.com/reference/integratorsapicategoriescontroller_getcategories | 2026-06-23 | High | Endpoint URL, pagination params, filter by name |
| Official Docs - Events API Ref | https://docs.tabsplatform.com/reference/integratorsapieventscontroller_getevents | 2026-06-23 | High | Endpoint URL, pagination params, filter fields for events |
| Official Docs - SLA | https://docs.tabsplatform.com/docs/slas-and-uptime | 2026-06-23 | High | 99.9% uptime SLA; maintenance windows; no rate limits published |
| Airbyte - Invoiced Connector | https://docs.airbyte.com/integrations/sources/invoiced | 2026-06-23 | Medium | Reference: 16 streams for similar billing platform; core = customers, invoices, payments, items, events |

---

## Known Quirks and Implementation Notes

1. **Date filter granularity**: The `lastUpdatedAt`, `createdAt`, and `receivedAt` filter values use YYYY-MM-DD format, not ISO 8601 timestamps. This means filtering with `gte:"2024-01-15"` will include all records updated on or after midnight on January 15th — but records updated later on the same day as the previous sync run may be missed if you use the current date. Always apply a **lookback window of at least 1–3 days** to avoid gaps.

2. **Offset pagination only**: The API uses page/limit offset pagination, not cursor-based. For large datasets, this can cause missed or duplicated records if new data is inserted mid-pagination. Recommended mitigation: sort by a stable field (TBD: no sort parameter documented — contact Tabs Support) and use `lastUpdatedAt` filter to minimize the window.

3. **No documented rate limits**: Despite HTTP 429 responses observed during documentation research, no rate limit values are published. Implement automatic retry with exponential backoff.

4. **Contracts soft delete**: `deletedAt` is the only confirmed soft-delete field. Contracts show as deleted when `deletedAt` is non-null. Other objects may not support soft deletes — assume hard deletes are undetectable without full snapshot comparison.

5. **`lineItems` nesting**: Invoice line items are nested arrays within invoice records. For analytical use, these should be exploded into a separate `invoice_line_items` table at ingestion time.

6. **`externalIds` normalization**: All objects carry an `externalIds` array linking to ERP/CRM systems. For analytics, flatten these into a separate `{object}_external_ids` table or filter to a single `sourceType` relevant to the organization.

7. **Beta Usage Events**: A newer `Usage Events (Beta)` API exists alongside the legacy `Events` API. The beta endpoint URL (`/v3/usage-events` — TBD: exact path not confirmed) returns HTTP 404/429 in documentation research. Document when endpoint stabilizes; require Tabs account manager opt-in.

8. **Authentication**: API key is passed raw in the `Authorization` header — not as `Bearer <token>`. This is non-standard; ensure the connector does NOT prepend `Bearer `.
