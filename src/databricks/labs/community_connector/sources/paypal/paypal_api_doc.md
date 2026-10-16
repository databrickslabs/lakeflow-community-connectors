# PayPal API Documentation

> **Connector target source name:** `paypal`
> **API version:** REST APIs v1/v2 (Transaction Search v1, Disputes v1, Invoices v2, Catalog Products v1)
> **Last researched:** 2026-10-16

---

## **Authorization**

PayPal uses **OAuth 2.0 Client Credentials** flow. The connector stores `client_id` and `client_secret` and exchanges them for a short-lived Bearer access token at runtime. No user-facing OAuth redirect is required.

### Preferred method: OAuth 2.0 Client Credentials

**Token endpoint:**
```
POST https://api-m.paypal.com/v1/oauth2/token
```

**Headers:**
```
Authorization: Basic base64(CLIENT_ID:CLIENT_SECRET)
Content-Type: application/x-www-form-urlencoded
```

**Body:**
```
grant_type=client_credentials
```

**Example request (cURL):**
```bash
curl -v -X POST "https://api-m.paypal.com/v1/oauth2/token" \
  -u "CLIENT_ID:CLIENT_SECRET" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials"
```

**Example response:**
```json
{
  "scope": "https://uri.paypal.com/services/invoicing https://uri.paypal.com/services/disputes/read-seller https://uri.paypal.com/services/subscriptions ...",
  "access_token": "A21AAFEpH4PsADK7qSS7pSRsgz...",
  "token_type": "Bearer",
  "app_id": "APP-80W284485P519543T",
  "expires_in": 32400,
  "nonce": "2026-10-16T15:00:00ZaYZlGvEkV4..."
}
```

**Token lifetime:** Approximately 9 hours (`expires_in` seconds, typically ~32400). The connector must cache the token and re-exchange before expiry. Do not request a new token for every API call — PayPal will rate-limit repeated token requests.

**Using the token:**
```
Authorization: Bearer <access_token>
```

**Required OAuth scopes (enabled by default for Business accounts):**
- Transaction Search: `https://uri.paypal.com/services/reporting/search/read`
- Disputes: `https://uri.paypal.com/services/disputes/read-seller`
- Invoicing: `https://uri.paypal.com/services/invoicing`
- Catalog Products/Subscriptions: `https://uri.paypal.com/services/subscriptions`

**Base URLs:**
- Production: `https://api-m.paypal.com`
- Sandbox: `https://api-m.sandbox.paypal.com`

**Prerequisites:**
- PayPal Business account (required to go live and to test outside the US)
- Transaction Search must be **explicitly enabled** in the Developer Dashboard — after enabling, allow up to 9 hours for the feature to become active.

**Alternative method (not preferred):** Refresh Token flow (OAuth 2.0 Authorization Code). Connector stores `client_id`, `client_secret`, and a long-lived `refresh_token`. Not recommended for server-to-server integrations; use Client Credentials instead.

---

## **Object List**

The object list is **static** — it is determined by the PayPal REST API surface and does not change at runtime. The following tables/objects are documented in this connector (all read via REST GET/POST endpoints):

| Object Name | API Endpoint | Vendor Support |
|---|---|---|
| `transactions` | `GET /v1/reporting/transactions` | Airbyte ✅, Fivetran ✅ |
| `balances` | `GET /v1/reporting/balances` | Airbyte ✅, Fivetran ✅ |
| `disputes` | `GET /v1/customer/disputes` | Airbyte ✅, Fivetran ✅ |
| `invoices` | `POST /v2/invoicing/search-invoices` | Airbyte ✅, Fivetran ✅ |
| `products` | `GET /v1/catalogs/products` | Airbyte ✅, Fivetran ✅ |
| `product_details` | `GET /v1/catalogs/products/{product_id}` | Airbyte ✅ (child of products) |

**Deferred tables** are listed at the bottom of this document.

---

## **Object Schema**

Schemas are **static** — field names and types are defined by the PayPal OpenAPI 3.0 specifications. There is no runtime schema discovery API.

### `transactions` (Transaction Search v1)

Each element of `transaction_details[]` in the list response contains nested sub-objects:

**`transaction_info` sub-object:**

| Field | Type | Description |
|---|---|---|
| `transaction_id` | string | PayPal-generated transaction ID (primary key) |
| `paypal_account_id` | string | PayPal account ID of the counterparty |
| `paypal_reference_id` | string | PayPal-generated base ID (related pre-existing transaction) |
| `paypal_reference_id_type` | string (enum) | Reference type: `ODR`, `TXN`, `SUB`, `PAP` |
| `transaction_event_code` | string | 5-digit event code classifying transaction type (e.g. `T0001`) |
| `transaction_initiation_date` | datetime (RFC 3339) | When work on the transaction began |
| `transaction_updated_date` | datetime (RFC 3339) | When the transaction was last changed **(incremental cursor)** |
| `transaction_amount` | money `{currency_code, value}` | Gross transaction amount |
| `fee_amount` | money | PayPal fee amount |
| `discount_amount` | money | Price reduction |
| `insurance_amount` | money | Insurance charge |
| `sales_tax_amount` | money | Sales tax |
| `shipping_amount` | money | Shipping amount |
| `shipping_discount_amount` | money | Discount on shipping |
| `shipping_tax_amount` | money | Tax on shipping |
| `other_amount` | money | Special additional amount |
| `tip_amount` | money | Tip |
| `transaction_status` | string (enum) | `D` (denied), `P` (pending), `S` (success), `V` (reversed) |
| `transaction_subject` | string | Payment subject |
| `transaction_note` | string | Payer note to payee |
| `payment_tracking_id` | string | Unique partner-provided tracking ID |
| `bank_reference_id` | string | Bank reference ID (ACH) |
| `ending_balance` | money | Ending account balance |
| `available_balance` | money | Available balance during transaction |
| `invoice_id` | string | Merchant-provided invoice ID |
| `custom_field` | string | Merchant-provided custom text |
| `protection_eligibility` | string | `01`=Eligible, `02`=Not eligible, `03`=Partially |
| `credit_term` | string | Credit installment term |
| `instrument_type` | string | Financial instrument type (e.g. `PAYPAL`, `CREDIT_CARD`, `BANK`) |
| `instrument_sub_type` | string | Finer instrument classification (e.g. `Visa card`) |

**`payer_info` sub-object:**

| Field | Type | Description |
|---|---|---|
| `account_id` | string | PayPal customer account ID |
| `email_address` | string | Payer's email address |
| `phone_number` | object `{country_code, national_number}` | Primary phone number |
| `address_status` | string | Address verification status |
| `payer_name` | object | Payer's name |
| `country_code` | string | ISO-3166 two-character country code |
| `payer_status` | string | Whether payer is `Y` (verified) or `N` (unverified) |

**`shipping_info` sub-object:**

| Field | Type | Description |
|---|---|---|
| `name` | string | Recipient name |
| `method` | string | Shipping method |
| `address` | object | Primary shipping address |
| `secondary_shipping_address` | object | Secondary shipping address |

**`cart_info` sub-object:**

| Field | Type | Description |
|---|---|---|
| `item_details` | array | Line items, each with name, quantity, unit_price, tax_amounts, etc. |
| `tax_inclusive` | boolean | Whether item amounts include tax |
| `paypal_invoice_id` | string | PayPal-generated invoice ID |

**`store_info` sub-object:**

| Field | Type | Description |
|---|---|---|
| `store_id` | string | Merchant store ID |
| `terminal_id` | string | Checkout terminal ID |

**`auction_info` sub-object:**

| Field | Type | Description |
|---|---|---|
| `auction_site` | string | Auction site name |
| `auction_item_site` | string (URI) | Auction item URL |
| `auction_buyer_id` | string | Auction buyer ID |
| `auction_closing_date` | datetime | When auction closed |

**`incentive_info` sub-object:**

| Field | Type | Description |
|---|---|---|
| `incentive_details` | array | Each element: `incentive_type`, `incentive_code`, `incentive_amount`, `incentive_program_code` |

---

### `balances` (Transaction Search v1)

Each element of `balances[]` in the response:

| Field | Type | Description |
|---|---|---|
| `currency` | string (ISO-4217) | Currency code **(primary key component)** |
| `primary` | boolean | Whether this is the account's primary currency |
| `total_balance` | money `{currency_code, value}` | Total balance |
| `available_balance` | money | Available (disposable) balance |
| `withheld_balance` | money | Withheld balance |

**Top-level response fields:**

| Field | Type | Description |
|---|---|---|
| `account_id` | string | PayPal merchant account ID |
| `as_of_time` | datetime | Time the balance snapshot was taken **(incremental cursor)** |
| `last_refresh_time` | datetime | When balances were last refreshed |

---

### `disputes` (Customer Disputes v1)

**List response** (`items[]` array) — summary fields:

| Field | Type | Description |
|---|---|---|
| `dispute_id` | string | Dispute ID **(primary key)** |
| `create_time` | datetime | When dispute was created |
| `update_time` | datetime | When dispute was last updated **(incremental cursor)** |
| `reason` | string (enum) | e.g. `MERCHANDISE_OR_SERVICE_NOT_RECEIVED`, `UNAUTHORISED`, `DUPLICATE_TRANSACTION`, etc. |
| `status` | string (enum) | e.g. `OPEN`, `WAITING_FOR_BUYER_RESPONSE`, `WAITING_FOR_SELLER_RESPONSE`, `UNDER_REVIEW`, `RESOLVED`, `OTHER` |
| `dispute_state` | string | e.g. `OPEN_INQUIRIES`, `REQUIRED_ACTION`, `REQUIRED_OTHER_PARTY_ACTION` |
| `dispute_life_cycle_stage` | string | e.g. `INQUIRY`, `CHARGEBACK`, `PRE_ARBITRATION`, `ARBITRATION` |
| `dispute_channel` | string | e.g. `INTERNAL`, `EXTERNAL` |
| `dispute_amount` | money `{currency_code, value}` | Disputed amount |
| `buyer_response_due_date` | datetime | Deadline for buyer response |
| `seller_response_due_date` | datetime | Deadline for seller response |

**Detail response** (additional fields from `GET /v1/customer/disputes/{id}`):

| Field | Type | Description |
|---|---|---|
| `dispute_outcome` | object | `outcome_code`, `outcome_reason`, `amount_refunded` |
| `adjudications` | array | Each: `type`, `adjudication_time`, `reason`, `dispute_life_cycle_stage` |
| `money_movements` | array | Each: `affected_party`, `amount`, `initiated_time`, `type`, `reason` |
| `messages` | array | Messages between parties |
| `extensions` | object | Billing dispute properties, buyer/seller transaction details |

---

### `invoices` (Invoices v2)

Each element of `items[]` from POST `/v2/invoicing/search-invoices`:

| Field | Type | Description |
|---|---|---|
| `id` | string | Invoice ID (e.g. `INV2-Z56S-5LLA-Q52L-CPZ5`) **(primary key)** |
| `status` | string (enum) | `DRAFT`, `SENT`, `SCHEDULED`, `PAYMENT_PENDING`, `PARTIALLY_PAID`, `PAID`, `MARKED_AS_PAID`, `CANCELLED`, `REFUNDED`, `PARTIALLY_REFUNDED`, `MARKED_AS_REFUNDED`, `UNPAID`, `PAYMENT_INITIATED` |
| `detail.invoice_number` | string | Merchant-assigned invoice number |
| `detail.reference` | string | Purchase Order or reference number |
| `detail.invoice_date` | date | Invoice date |
| `detail.currency_code` | string (ISO-4217) | Invoice currency |
| `detail.note` | string | Note to recipient |
| `detail.term` | string | Payment terms text |
| `detail.memo` | string | Private bookkeeping memo |
| `detail.payment_term.term_type` | string | e.g. `NET_10`, `NET_30`, `DUE_ON_RECEIPT` |
| `detail.payment_term.due_date` | date | Due date |
| `detail.metadata.create_time` | datetime | When invoice was created |
| `detail.metadata.recipient_view_url` | string | Customer-facing payment URL |
| `detail.metadata.invoicer_view_url` | string | Merchant-facing invoice URL |
| `invoicer.email_address` | string | Merchant email |
| `primary_recipients[].billing_info.email_address` | string | Recipient email |
| `amount.currency_code` | string | Currency code |
| `amount.value` | string | Total invoice amount |
| `amount.breakdown.item_total` | money | Line items total |
| `amount.breakdown.discount` | money | Discount |
| `amount.breakdown.tax_total` | money | Tax total |
| `amount.breakdown.shipping` | money | Shipping |
| `amount.due_amount` | money | Amount still due |
| `items` | array | Line items (optional field; include by adding `fields=items` to request) |
| `payments` | array | Payment records (optional field) |
| `refunds` | array | Refund records (optional field) |

---

### `products` (Catalog Products v1 — list)

Each element of `products[]`:

| Field | Type | Description |
|---|---|---|
| `id` | string | Product ID **(primary key)** |
| `name` | string | Product name |
| `description` | string | Product description |
| `create_time` | datetime | When the product was created |
| `links` | array | HATEOAS links |

---

### `product_details` (Catalog Products v1 — detail, child of products)

Full product detail from `GET /v1/catalogs/products/{product_id}`:

| Field | Type | Description |
|---|---|---|
| `id` | string | Product ID **(primary key)** |
| `name` | string | Product name |
| `description` | string | Product description |
| `type` | string (enum) | `PHYSICAL`, `DIGITAL`, `SERVICE` |
| `category` | string (enum) | Product category (e.g. `AC_REFRIGERATION_REPAIR`, `CLOTHING`) |
| `image_url` | string (URI) | Product image URL |
| `home_url` | string (URI) | Product landing page URL |
| `create_time` | datetime | Creation timestamp |
| `update_time` | datetime | Last updated timestamp |

---

## **Get Object Primary Keys**

Primary keys are **static** — determined by schema inspection and vendor implementations:

| Object | Primary Key(s) | Notes |
|---|---|---|
| `transactions` | `transaction_id` | PayPal-generated, unique per transaction |
| `balances` | `currency` (+ `as_of_time`) | Balance snapshot per currency; treat `(currency, as_of_time)` as composite key for append mode |
| `disputes` | `dispute_id` | PayPal-generated unique ID |
| `invoices` | `id` | PayPal-generated invoice ID (format: `INV2-XXXX-...`) |
| `products` | `id` | Merchant-assigned or PayPal-generated product ID |
| `product_details` | `id` | Same as `products.id` |

---

## **Object's Ingestion Type**

| Object | Ingestion Type | Rationale |
|---|---|---|
| `transactions` | `cdc` | Incremental by `transaction_updated_date`; supports upserts. Deleted transactions appear as status `V` (reversed) — no separate delete API. |
| `balances` | `snapshot` | Point-in-time snapshot of account balance per currency. `as_of_time` query param can filter by date, but the response is always a full balance state — not a changelog. Best treated as full snapshot each sync. |
| `disputes` | `cdc` | Incremental by `update_time`. Disputes transition through statuses; `RESOLVED`/`CANCELLED` disputes are not deleted from the API. |
| `invoices` | `snapshot` | Airbyte does not support incremental sync for this stream. The search API uses `creation_date_range` but doesn't support `updated_after`-style filtering reliably. Full refresh each sync. |
| `products` | `snapshot` | No cursor field available; page-based list only. Full refresh each sync. Fivetran re-imports every sync. |
| `product_details` | `snapshot` | Child of `products`; re-synced for each product ID on every sync. |

---

## **Read API for Data Retrieval**

### `transactions` — `GET /v1/reporting/transactions`

**Method:** `GET`  
**Endpoint:** `https://api-m.paypal.com/v1/reporting/transactions`

**Required query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `start_date` | string (RFC 3339) | Start of date range. Seconds required, fractional optional. E.g. `2026-01-01T00:00:00Z` |
| `end_date` | string (RFC 3339) | End of date range. Max range: **31 days**. |

**Optional query parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `page_size` | integer | 100 | Records per page. Range: 1–500. |
| `page` | integer | 1 | Page number. Range: 1–2,147,483,647 |
| `fields` | string | `transaction_info` | Comma-separated: `transaction_info`, `payer_info`, `shipping_info`, `auction_info`, `cart_info`, `incentive_info`, `store_info`. Use `all` for all fields. |
| `balance_affecting_records_only` | string | — | `Y` to return only balance-affecting records. |
| `transaction_id` | string | — | Filter by specific transaction ID. |
| `transaction_status` | string | — | Filter by status: `D`, `P`, `S`, `V`. |
| `transaction_type` | string | — | Filter by event code. |
| `transaction_currency` | string | — | ISO-4217 currency code filter. |

**Pagination strategy:** Page-increment (numeric `page` parameter).  
**Max records per request:** 10,000 (API server restriction). If exceeded, PayPal returns `RESULTSET_TOO_LARGE` — narrow the date range.  
**Data availability:** Executed transactions appear in results within **3 hours** of execution.  
**Historical limit:** Maximum **3 years** of historical data.

**Example request:**
```bash
curl -X GET "https://api-m.paypal.com/v1/reporting/transactions?start_date=2026-10-01T00:00:00Z&end_date=2026-10-07T23:59:59Z&fields=all&page_size=500&page=1" \
  -H "Authorization: Bearer ACCESS_TOKEN" \
  -H "Content-Type: application/json"
```

**Example response (abbreviated):**
```json
{
  "transaction_details": [
    {
      "transaction_info": {
        "transaction_id": "5YD08598XK610192E",
        "transaction_event_code": "T0006",
        "transaction_initiation_date": "2026-10-01T12:00:00Z",
        "transaction_updated_date": "2026-10-01T12:01:00Z",
        "transaction_amount": {"currency_code": "USD", "value": "50.00"},
        "fee_amount": {"currency_code": "USD", "value": "-1.75"},
        "transaction_status": "S"
      },
      "payer_info": {
        "account_id": "XYZ1234567890",
        "email_address": "buyer@example.com"
      }
    }
  ],
  "account_number": "MERCHANTACCT001",
  "start_date": "2026-10-01T00:00:00Z",
  "end_date": "2026-10-07T23:59:59Z",
  "total_items": 250,
  "total_pages": 1,
  "page": 1,
  "links": [{"href": "...", "rel": "self", "method": "GET"}]
}
```

**Incremental sync strategy:**
- Cursor field: `transaction_info.transaction_updated_date`
- Slice requests in 7-day windows (configurable, max 31 days per request)
- On each sync: fetch from `last_transaction_updated_date` to `now`, paginating through all pages
- Apply a lookback of 1–3 days to catch delayed records (PayPal allows up to 3 hours, but network delays warrant 1-day lookback minimum)
- No delete handling: reversed/refunded transactions appear as status `V`; no separate delete stream

---

### `balances` — `GET /v1/reporting/balances`

**Method:** `GET`  
**Endpoint:** `https://api-m.paypal.com/v1/reporting/balances`

**Optional query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `as_of_time` | string (RFC 3339) | Returns balances at this point in time. If omitted, returns the most recent refresh. |
| `currency_code` | string (ISO-4217) | Filter to a single currency. |

**Example request:**
```bash
curl -X GET "https://api-m.paypal.com/v1/reporting/balances" \
  -H "Authorization: Bearer ACCESS_TOKEN"
```

**Example response:**
```json
{
  "balances": [
    {
      "currency": "USD",
      "primary": true,
      "total_balance": {"currency_code": "USD", "value": "1250.00"},
      "available_balance": {"currency_code": "USD", "value": "1000.00"},
      "withheld_balance": {"currency_code": "USD", "value": "250.00"}
    },
    {
      "currency": "EUR",
      "primary": false,
      "total_balance": {"currency_code": "EUR", "value": "300.00"},
      "available_balance": {"currency_code": "EUR", "value": "300.00"},
      "withheld_balance": {"currency_code": "EUR", "value": "0.00"}
    }
  ],
  "account_id": "MERCHANTACCT001",
  "as_of_time": "2026-10-16T15:00:00Z",
  "last_refresh_time": "2026-10-16T15:00:00Z"
}
```

**Sync strategy:** Full snapshot each sync. Balances appear in results within **3 hours**. Historical limit: up to 3 years via `as_of_time`.

---

### `disputes` — `GET /v1/customer/disputes`

**Method:** `GET`  
**Endpoint:** `https://api-m.paypal.com/v1/customer/disputes`

**Optional query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `start_time` | string (RFC 3339 with milliseconds) | Filter disputes updated after this time |
| `disputed_transaction_id` | string | Filter by transaction ID |
| `page_size` | integer | Records per page. Range: 1–50. Default: 10 |
| `next_page_token` | string | Cursor token for next page (from `links` in previous response) |
| `dispute_state` | string | Filter by state |

**Pagination strategy:** Page token (cursor-based). The response `links` array includes a `next` link with `next_page_token`.  
**Max page size:** 50  
**Historical limit:** Maximum **180 days** of dispute history.

**Example request:**
```bash
curl -X GET "https://api-m.paypal.com/v1/customer/disputes?start_time=2026-10-01T00:00:00.000Z&page_size=50" \
  -H "Authorization: Bearer ACCESS_TOKEN"
```

**Example response (abbreviated):**
```json
{
  "items": [
    {
      "dispute_id": "PP-D-12345",
      "create_time": "2026-10-01T08:00:00.000Z",
      "update_time": "2026-10-02T10:00:00.000Z",
      "reason": "MERCHANDISE_OR_SERVICE_NOT_RECEIVED",
      "status": "OPEN",
      "dispute_state": "REQUIRED_OTHER_PARTY_ACTION",
      "dispute_amount": {"currency_code": "USD", "value": "49.99"},
      "dispute_life_cycle_stage": "INQUIRY",
      "dispute_channel": "INTERNAL"
    }
  ],
  "links": [
    {"href": "https://api-m.paypal.com/v1/customer/disputes?next_page_token=PAGE2TOKEN", "rel": "next", "method": "GET"}
  ]
}
```

**Incremental sync strategy:**
- Cursor field: `update_time`
- Use `start_time` query parameter with millisecond precision (e.g. `2026-10-01T00:00:00.000Z`)
- Paginate via `next_page_token` in the `links` array until no `next` link is returned
- Lookback: 7-day lookback recommended (disputes may be updated retroactively when evidence is reviewed)

---

### `invoices` — `POST /v2/invoicing/search-invoices`

**Method:** `POST`  
**Endpoint:** `https://api-m.paypal.com/v2/invoicing/search-invoices`

**Query parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `page` | integer | 1 | Page number |
| `page_size` | integer | 10 | Records per page. Max: 100 |
| `total_required` | boolean | false | Include total_items and total_pages in response |

**Request body (JSON):**
```json
{
  "creation_date_range": {
    "start": "2026-01-01T00:00:00Z",
    "end": "2026-10-16T23:59:59Z"
  },
  "fields": ["items", "payments", "refunds"]
}
```

**Additional body filters available:**
- `status`: array of invoice statuses
- `currency_code`: ISO-4217 code
- `recipient_email`: filter by recipient
- `total_amount_range.lower_amount` / `upper_amount`
- `invoice_number`: exact invoice number

**Example request:**
```bash
curl -X POST "https://api-m.paypal.com/v2/invoicing/search-invoices?page=1&page_size=100&total_required=true" \
  -H "Authorization: Bearer ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "creation_date_range": {
      "start": "2026-01-01T00:00:00Z",
      "end": "2026-10-16T23:59:59Z"
    }
  }'
```

**Example response (abbreviated):**
```json
{
  "total_items": 6,
  "total_pages": 1,
  "items": [
    {
      "id": "INV2-Z56S-5LLA-Q52L-CPZ5",
      "status": "PAID",
      "detail": {
        "invoice_number": "#123",
        "invoice_date": "2026-09-01",
        "currency_code": "USD",
        "metadata": {
          "create_time": "2026-09-01T10:00:00Z"
        }
      },
      "amount": {"currency_code": "USD", "value": "74.21"},
      "invoicer": {"email_address": "merchant@example.com"},
      "primary_recipients": [{"billing_info": {"email_address": "customer@example.com"}}]
    }
  ],
  "links": [
    {"href": "https://api-m.paypal.com/v2/invoicing/invoices?page=2&page_size=100&total_required=true", "rel": "next", "method": "POST"}
  ]
}
```

**Sync strategy:** Full snapshot. Paginate by incrementing `page` until no `next` link in the response or `page >= total_pages`. Each page is a POST request with the same body.

**Note on incremental:** The search API supports `creation_date_range` but **not** an `updated_after` filter. Airbyte connector passes `start_date` to `creation_date_range.start` for filtering, but this only filters by creation date (not update date), so missed updates on old invoices would require a full re-scan.

---

### `products` — `GET /v1/catalogs/products`

**Method:** `GET`  
**Endpoint:** `https://api-m.paypal.com/v1/catalogs/products`

**Query parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `page_size` | integer | 10 | Records per page. Max: 20 |
| `page` | integer | 1 | Page number |
| `total_required` | boolean | false | Include totals in response |

**Example request:**
```bash
curl -X GET "https://api-m.paypal.com/v1/catalogs/products?page_size=20&page=1&total_required=true" \
  -H "Authorization: Bearer ACCESS_TOKEN"
```

**Example response:**
```json
{
  "products": [
    {
      "id": "RBW00001",
      "name": "Premium Widget",
      "description": "A high-quality widget",
      "create_time": "2026-01-15T08:00:00Z",
      "links": [{"href": "https://api-m.paypal.com/v1/catalogs/products/RBW00001", "rel": "self", "method": "GET"}]
    }
  ],
  "total_items": 1,
  "total_pages": 1,
  "links": []
}
```

**Warning:** Page size is fixed at max 20. For large catalogs (30K+ products), retrieval can take 10–15 minutes. PayPal API calls are not concurrent.

---

### `product_details` — `GET /v1/catalogs/products/{product_id}`

**Method:** `GET`  
**Endpoint:** `https://api-m.paypal.com/v1/catalogs/products/{product_id}`

This is a **child stream** of `products`. For each product returned by the list endpoint, make an individual GET request.

**Example request:**
```bash
curl -X GET "https://api-m.paypal.com/v1/catalogs/products/RBW00001" \
  -H "Authorization: Bearer ACCESS_TOKEN"
```

**Example response:**
```json
{
  "id": "RBW00001",
  "name": "Premium Widget",
  "description": "A high-quality widget for all your needs",
  "type": "PHYSICAL",
  "category": "ELECTRONICS",
  "image_url": "https://example.com/widget.jpg",
  "home_url": "https://example.com/widget",
  "create_time": "2026-01-15T08:00:00Z",
  "update_time": "2026-06-10T14:00:00Z",
  "links": [{"href": "https://api-m.paypal.com/v1/catalogs/products/RBW00001", "rel": "self", "method": "GET"}]
}
```

**Warning:** For large catalogs, fetching individual details for each product sequentially can take several hours. Consider running product sync at off-peak times.

---

## **Rate Limits**

PayPal does **not publish explicit rate limits**. Rate limits are applied dynamically based on traffic patterns.

| Behavior | Details |
|---|---|
| Rate limit signal | HTTP `429 Too Many Requests` with `RATE_LIMIT_REACHED` error code |
| Retry-After | May be included in response headers |
| Backoff strategy | Implement **exponential backoff with jitter**: `delay = 2^attempt * 1000ms + random(0, 500ms)` |
| Max retries | 4 attempts recommended |
| Token refresh | Do NOT request a new token per API call. Cache and reuse until ~60 seconds before `expires_in`. |
| Polling avoidance | Prefer date-range filters over polling; do not make continuous requests |
| Known limit (Payouts API) | 400 POST requests/minute (only documented limit for any PayPal API) |
| Airbyte observed | ~50 requests/minute from IP address across all endpoints (unconfirmed by PayPal, observed empirically) |

**Handling `RESULTSET_TOO_LARGE` (transactions):** The Transaction Search API rejects requests with more than 10,000 records. Automatically retry with a smaller date range (halving until down to 1-second windows before failing).

---

## **Field Type Mapping**

| PayPal API Type | Spark/Python Type | Notes |
|---|---|---|
| `string` | `StringType` | Default for IDs, codes, names |
| `number` (amount `value`) | `StringType` | PayPal amounts are strings (e.g. `"50.00"`); parse to `DecimalType(19,4)` in Spark |
| `string (currency_code)` | `StringType` | ISO-4217 3-character code |
| `string (RFC 3339 datetime)` | `TimestampType` | Parse with timezone support; milliseconds may or may not be present |
| `string (date)` | `DateType` | ISO-8601 date `YYYY-MM-DD` |
| `boolean` | `BooleanType` | e.g. `primary`, `tax_inclusive` |
| `array` | `ArrayType` | Nested arrays; flatten in Spark as needed |
| `object (money)` | `StructType` with `currency_code: StringType, value: StringType` | Represent as struct or flatten |
| `enum` | `StringType` | Validate against known enum values |
| `integer` | `LongType` | e.g. `total_items`, `total_pages` |

**Special behaviors:**
- All monetary `value` fields are **strings** (e.g. `"49.99"`), not numbers — always parse explicitly.
- Datetime fields do not always include milliseconds; use a flexible parser.
- The `transaction_status` field is a **single character** (`D`, `P`, `S`, `V`), not a descriptive string.
- The `protection_eligibility` field is a **2-digit string code**, not a boolean.

---

## **Deferred Tables**

The following PayPal API objects were identified but deferred to future implementation batches due to different API patterns, scope requirements, or low cross-vendor coverage:

| Table | API | Reason for Deferral |
|---|---|---|
| `billing_plans` | `GET /v1/billing/plans` | Subscriptions API requires separate OAuth scope (`https://uri.paypal.com/services/subscriptions`). Fivetran-only; not in Airbyte. Different pagination (cursor-based). |
| `billing_cycles` | Child of billing_plans | Nested within billing plan detail responses; requires billing plan sync first. |
| `payments` (v1) | `GET /v1/payments/payment` | Uses deprecated Payments v1 API. Airbyte marks this stream as deprecated. Recommend using `transactions` stream for payment data instead. |
| `subscriptions` | `GET /v1/billing/subscriptions/{id}` | ID-only lookup (no list API for subscriptions); would need subscription IDs from another source. Complex. |
| `webhooks` | `GET /v1/notifications/webhooks` | Management/configuration data, not transactional. Low value for analytics use cases. |
| `payouts` | `GET /v1/payments/payouts/{id}` | ID-based lookup only; no list-all API. Requires payout batch IDs from another source. |

---

## **Sources and References**

### Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://developer.paypal.com/api/rest | 2026-10-16 | High | OAuth 2.0 auth flow, base URLs, token structure |
| Official Docs | https://developer.paypal.com/api/rest/requests | 2026-10-16 | High | HTTP headers, query parameter format |
| Official Docs | https://developer.paypal.com/api/transaction-search/v1 | 2026-10-16 | High | Transaction Search and Balances endpoints |
| Official Docs | https://developer.paypal.com/api/transaction-search/v1/balances-get | 2026-10-16 | High | Balances endpoint params and response |
| Official Docs | https://developer.paypal.com/api/customer-disputes/v1 | 2026-10-16 | High | Disputes endpoint list |
| Official Docs | https://developer.paypal.com/api/customer-disputes/v1/disputes-list | 2026-10-16 | High | Disputes list fields, pagination token |
| Official Docs | https://developer.paypal.com/api/customer-disputes/v1/disputes-get | 2026-10-16 | High | Disputes detail response fields |
| Official Docs | https://developer.paypal.com/api/invoicing/v2 | 2026-10-16 | High | Invoices API endpoints |
| Official Docs | https://developer.paypal.com/api/invoicing/v2/invoices-search-invoices | 2026-10-16 | High | Invoice search request/response |
| Official Docs | https://developer.paypal.com/api/catalog-products/v1/products-list | 2026-10-16 | High | Products list endpoint and response |
| Official Docs | https://developer.paypal.com/api/catalog-products/v1/products-get | 2026-10-16 | High | Product detail endpoint and response |
| Official Docs | https://developer.paypal.com/api/rate-limiting | 2026-10-16 | High | Rate limiting behavior and best practices |
| Official Docs | https://developer.paypal.com/api/rest/reference/rate-limiting | 2026-10-16 | High | Official rate limiting guidance |
| Official Docs | https://developer.paypal.com/invoicing/troubleshooting | 2026-10-16 | High | Token expiry (~9 hrs), backoff strategies |
| Official OpenAPI Spec | https://github.com/paypal/paypal-rest-api-specifications | 2026-10-16 | High | Available API surface (list of all APIs) |
| Official OpenAPI Spec (raw) | https://raw.githubusercontent.com/api-evangelist/paypal/refs/heads/main/openapi/paypal-transactions-api-openapi.yml | 2026-10-16 | High | Full transaction_info, payer_info, cart_info, shipping_info schemas |
| Official OpenAPI Spec (raw) | https://raw.githubusercontent.com/api-evangelist/paypal/refs/heads/main/openapi/paypal-products-api-openapi.yml | 2026-10-16 | High | Products API schema (type, category enums) |
| Official OpenAPI Spec (raw) | https://raw.githubusercontent.com/api-evangelist/paypal/refs/heads/main/openapi/paypal-balances-api-openapi.yml | 2026-10-16 | High | Balances response schema |
| Airbyte Connector Docs | https://docs.airbyte.com/integrations/sources/paypal-transaction | 2026-10-16 | High | 7 supported streams, pagination strategies, sync modes, performance considerations |
| Airbyte GitHub PR | https://github.com/airbytehq/airbyte/pull/76177 | 2026-10-16 | High | Deprecation of payments v1, token delay note, Business account requirement |
| Fivetran Connector Docs | https://fivetran.com/docs/connectors/applications/paypal | 2026-10-16 | High | Incremental vs snapshot tables, BILLING_PLAN, DISPUTE, TRANSACTION sync modes |
| Fivetran Schema ERD | https://fivetran.com/docs/connectors/applications/paypal/connector-schema | 2026-10-16 | High | Full table/column list for all Fivetran-synced tables |
| PayPal TypeScript SDK | https://github.com/paypal/PayPal-TypeScript-Server-SDK/blob/2.2.0/doc/controllers/transaction-search.md | 2026-10-16 | Medium | Transaction search parameters cross-reference |
