# Shopify Admin REST API — Connector Reference

**API Version Researched**: 2026-04 (latest stable as of 2026-04-19)
**Deprecation Notice**: Shopify formally designated the REST Admin API as "legacy" on 2024-10-01. New public apps must use GraphQL as of 2025-04-01. However, the REST API continues to function and is the basis for this connector. Key resources — especially `products` and `variants` — have been deprecated for mutation operations since REST API 2024-04, but the list/read endpoints remain operational. See "Deprecation Status" subsections per table.

---

## API Overview

### Authentication

**Method**: Admin API access token passed via HTTP header.

| Property | Value |
|----------|-------|
| Header name | `X-Shopify-Access-Token` |
| Token format | `shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` |
| **NOT** | `Authorization: Bearer ...` — the Authorization header is NOT used |

**How to obtain a token**: For private/custom apps, generate the token in the Shopify Admin under "Apps > Develop apps > [App] > API credentials > Admin API access token". For public apps, exchange the OAuth code for an access token (connector stores `client_id`, `client_secret`, `refresh_token` and exchanges at runtime — the connector does not run interactive OAuth flows).

**Required scopes** for the 7 tables in this connector:

| Scope handle | Resources granted |
|---|---|
| `read_products` | Product, Product Variant, Product Image, Collect, Custom Collection, Smart Collection |
| `read_customers` | Customer, Customer Address |
| `read_orders` | Order, Refund, Fulfillment, Transaction (last 60 days by default) |
| `read_all_orders` | Orders older than 60 days — must be explicitly requested alongside `read_orders` |
| `read_inventory` | Inventory Level, Inventory Item |
| `read_locations` | Location |
| `read_fulfillments` | Fulfillment Service |

**Example request**:
```
GET https://{shop}.myshopify.com/admin/api/2026-04/orders.json?status=any&limit=250
X-Shopify-Access-Token: shpat_abc123...
```

### Base URL Pattern

```
https://{shop}.myshopify.com/admin/api/{api_version}/{resource}.json
```

- `{shop}` is the myshopify subdomain (e.g., `my-store`)
- `{api_version}` is the quarterly release string (e.g., `2026-04`)
- All responses are JSON

### API Versioning Policy

Versions follow a quarterly cadence: `YYYY-01`, `YYYY-04`, `YYYY-07`, `YYYY-10`. Each stable version is supported for **at least 12 months**, providing at least 9 months of overlap between consecutive versions. After end-of-life, requests to a deprecated version are silently routed to the oldest currently supported version (no error is thrown — this is a silent compatibility shim). Deprecation warnings appear in the `X-Shopify-API-Deprecated-Reason` response header. Version is encoded directly in the URL path; there is no `api-version` header or query param alternative.

### Rate Limits — Leaky Bucket Algorithm

Shopify uses a **leaky bucket** model for REST Admin API rate limits:

| Plan tier | Bucket size | Leak rate |
|---|---|---|
| Basic / Standard | 40 requests per app per store | 2 req/sec |
| Advanced Shopify | 80 requests per app per store | 4 req/sec |
| Shopify Plus | 400 requests per app per store | 20 req/sec |
| Enterprise (Commerce Components) | 800 requests per app per store | 40 req/sec |

**Behavior**: Each API request consumes one "marble" from the bucket. The bucket drains at the leak rate regardless. If the bucket fills, the API returns **HTTP 429 Too Many Requests**.

**Response headers** on every REST response:

```
X-Shopify-Shop-Api-Call-Limit: 32/40
```

- Format: `{current_usage}/{bucket_size}`
- The count decreases automatically as the bucket drains

**When throttled** (HTTP 429):

```
HTTP/1.1 429 Too Many Requests
Retry-After: 2.0
```

- `Retry-After` contains the number of seconds to wait before retrying
- The connector should respect this header and sleep for at least that duration before retrying

**Connector strategy**: Monitor `X-Shopify-Shop-Api-Call-Limit` and back off proactively when usage reaches ~80% of bucket capacity. Always honor `Retry-After` on 429.

### GraphQL note

Shopify's GraphQL Admin API uses a different rate limiting model (calculated query cost, in points per second). For the 7 tables in this connector, we use REST exclusively. However, for future scale or for stores with very large product catalogs (>100 variants per product), the GraphQL Bulk Operations API is recommended by Shopify and is used by Airbyte's Shopify connector for `products` and `inventory_levels`. See individual table notes.

---

## Pagination

All Shopify REST Admin API list endpoints use **cursor-based pagination via the `Link` response header**. Offset-based pagination (page number) is not supported.

### How it works

1. Make an initial request with filters and `limit` (max 250).
2. If more results exist, the response includes a `Link` header.
3. Parse the `Link` header to extract the `page_info` cursor for the next page.
4. Repeat using only `page_info` and optionally `limit` — no other parameters.

### Link header format

The `Link` header may contain one or both of `rel=next` and `rel=previous`, separated by a comma:

```
link: "<https://{shop}.myshopify.com/admin/api/2026-04/orders.json?page_info=eyJsYXN0X2lkIjo0NTEwNjQ3NjcwNDQsImxhc3RfdmFsdWUiOiIyMDIzLTA0LTEwVDEyOjAwOjAwLTA0OjAwIiwiZGlyZWN0aW9uIjoibmV4dCJ9&limit=250>; rel=next, <https://{shop}.myshopify.com/admin/api/2026-04/orders.json?page_info=eyJsYXN0X2lkIjo0NTEwNjQ3NjcwNDQsImxhc3RfdmFsdWUiOiIyMDIzLTA0LTEwVDEyOjAwOjAwLTA0OjAwIiwiZGlyZWN0aW9uIjoicHJldmlvdXMifQ&limit=250>; rel=previous"
```

**Extraction logic** (Python pseudocode):

```python
import re

def parse_link_header(link_header: str) -> dict:
    """Returns dict with keys 'next' and/or 'previous', values are page_info strings."""
    result = {}
    for part in link_header.split(","):
        url_match = re.search(r'<([^>]+)>', part)
        rel_match = re.search(r'rel="([^"]+)"', part)
        if url_match and rel_match:
            url = url_match.group(1)
            page_info_match = re.search(r'page_info=([^&>]+)', url)
            if page_info_match:
                result[rel_match.group(1)] = page_info_match.group(1)
    return result
```

### Rules for page_info requests

- A request that includes `page_info` **cannot include any other filter parameters** (no `updated_at_min`, `status`, etc.)
- Only `limit` and `fields` may be combined with `page_info`
- `page_info` tokens are **temporary** — do not persist them across connector runs; only use them within the same sync session
- If there is no `Link` header (or no `rel=next` in the header), the current page is the last page

### Limit parameter

| Default | Maximum |
|---------|---------|
| 50 | 250 |

Always set `limit=250` for connector efficiency.

---

## Timestamp Format

All timestamps are returned in **ISO 8601 with timezone offset**:

```
2026-04-22T10:00:00-04:00
```

Shopify stores all timestamps internally in UTC but surfaces them with the store's configured timezone offset in REST responses. For robust incremental sync, always compare in UTC. When using `updated_at_min` and similar filter parameters, send UTC timestamps with the `+00:00` or `Z` suffix.

---

## Tables

---

### products

**Deprecation note**: The REST products list/read endpoint (`GET /products.json`) is deprecated as of REST API version 2024-04. Shopify recommends migrating to the GraphQL `products` query with Bulk Operations. The endpoint continues to work but may be removed in a future version. For new implementations, prefer GraphQL. This connector documents the REST endpoint for backward compatibility; a GraphQL migration path should be planned.

#### Endpoint

```
GET https://{shop}.myshopify.com/admin/api/2026-04/products.json
```

#### Required scopes
`read_products`

#### Query parameters

| Parameter | Type | Description |
|---|---|---|
| `limit` | integer | Max results per page. Default: `50`, max: `250` |
| `since_id` | integer | Return only products with an ID **greater than** this value. Exclusive (strictly >). |
| `ids` | string | Comma-separated list of product IDs to return |
| `handle` | string | Comma-separated list of product handles to return |
| `product_type` | string | Filter by product type |
| `collection_id` | integer | Filter by collection ID |
| `created_at_min` | ISO 8601 | Products created on or after this date |
| `created_at_max` | ISO 8601 | Products created on or before this date |
| `updated_at_min` | ISO 8601 | Products updated on or after this date (inclusive, >=) |
| `updated_at_max` | ISO 8601 | Products updated on or before this date |
| `published_at_min` | ISO 8601 | Products published on or after this date |
| `published_at_max` | ISO 8601 | Products published on or before this date |
| `published_scope` | string | `web` or `global` |
| `fields` | string | Comma-separated field names to include in response |
| `presentment_currencies` | string | ISO 4217 currency codes (comma-separated) for presentment prices |

#### Incremental sync parameters

| Parameter | Behavior | Inclusive? |
|---|---|---|
| `updated_at_min` | Returns products where `updated_at >= value` | Yes (inclusive, >=) |
| `since_id` | Returns products where `id > value` (strictly greater) | No (exclusive, >) |

**Recommended cursor**: `updated_at` — use `updated_at_min` with a small lookback window (e.g., minus 1 minute) on each incremental run to handle late-arriving updates.

#### Response shape

```json
{
  "products": [
    { ... }
  ]
}
```

Wrapper key: `"products"` (array)

#### Product record fields

**Root-level fields**:

| Field | Type | Read-only | Notes |
|---|---|---|---|
| `id` | integer (int64) | yes | Unique product ID across Shopify |
| `title` | string | no | Product name; required on create |
| `body_html` | string | no | Product description; supports HTML |
| `vendor` | string | no | Vendor/brand name |
| `product_type` | string | no | Merchant-defined category |
| `handle` | string | yes | URL-friendly slug; auto-generated from title |
| `created_at` | ISO 8601 datetime | yes | Creation timestamp |
| `updated_at` | ISO 8601 datetime | yes | Last modification timestamp; use as cursor |
| `published_at` | ISO 8601 datetime | no | Publication timestamp; `null` if unpublished |
| `published_scope` | string | no | `"web"` (online store only) or `"global"` (incl. POS) |
| `status` | string | no | `"active"`, `"archived"`, or `"draft"` |
| `tags` | string | no | Comma-separated tags; max 250 tags, 255 chars each |
| `template_suffix` | string | no | Custom Liquid template suffix; `null` if default |
| `admin_graphql_api_id` | string | yes | GraphQL GID, e.g. `gid://shopify/Product/123` |
| `variants` | array | — | See nested structure below |
| `options` | array | — | See nested structure below |
| `images` | array | — | See nested structure below |

**Nested: `variants` array** (each element):

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Unique variant ID |
| `product_id` | integer | Parent product ID |
| `title` | string | Variant label (e.g., "Blue / Large") |
| `price` | string (decimal) | Current selling price |
| `compare_at_price` | string / null | Crossed-out original price |
| `position` | integer | Display order |
| `inventory_policy` | string | `"deny"` or `"continue"` (oversell behavior) |
| `inventory_management` | string / null | `"shopify"` or `null` (untracked) |
| `inventory_quantity` | integer | Current stock at all locations |
| `old_inventory_quantity` | integer | Previous inventory count |
| `sku` | string / null | Stock keeping unit |
| `barcode` | string / null | Product barcode (EAN, UPC, ISBN) |
| `option1` | string | First option value |
| `option2` | string | Second option value |
| `option3` | string | Third option value |
| `taxable` | boolean | Whether tax is applied |
| `requires_shipping` | boolean | Whether shipping is required |
| `fulfillment_service` | string | `"manual"` or fulfillment service handle |
| `grams` | integer | Weight in grams |
| `weight` | decimal | Weight value |
| `weight_unit` | string | `"lb"`, `"kg"`, `"oz"`, `"g"` |
| `inventory_item_id` | integer | Associated InventoryItem ID (link to inventory_levels) |
| `image_id` | integer / null | Associated product image |
| `created_at` | ISO 8601 datetime | Variant creation timestamp |
| `updated_at` | ISO 8601 datetime | Variant last updated timestamp |
| `presentment_prices` | array | Currency-specific pricing objects |
| `admin_graphql_api_id` | string | GraphQL GID |

**Nested: `options` array** (each element):

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Option ID |
| `product_id` | integer | Parent product ID |
| `name` | string | Option name (e.g., "Color", "Size") |
| `position` | integer | Display order |
| `values` | array of strings | Possible values for this option |

Max 3 options per product; each value up to 255 characters.

**Nested: `images` array** (each element):

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Image ID |
| `product_id` | integer | Parent product ID |
| `position` | integer | Display order in gallery |
| `created_at` | ISO 8601 datetime | — |
| `updated_at` | ISO 8601 datetime | — |
| `width` | integer | Pixel width |
| `height` | integer | Pixel height |
| `src` | string | Shopify CDN URL |
| `alt` | string / null | Alt text |
| `variant_ids` | array of integers | Associated variant IDs |
| `admin_graphql_api_id` | string | GraphQL GID |

#### Ingestion type
`cdc` — Products can be read incrementally using `updated_at_min`. There is no delete feed via REST; deleted products disappear from the list response silently. To detect deletes, a periodic full snapshot comparison is required.

#### Edge cases
- **Archived products** (`status: "archived"`) are returned by the endpoint but are not visible to customers on sales channels. Include them in the sync.
- **Draft products** (`status: "draft"`) are returned by the endpoint. When a product is duplicated or unarchived, it defaults to draft status.
- **Deleted products** are not returned — they simply disappear with no tombstone record. The REST endpoint has no `deleted_at` field.
- The product REST endpoint supports a maximum of **100 variants** per product. Stores with >100 variants must use the GraphQL API (up to 2048 variants). REST may truncate variant lists for high-variant products without error.
- `updated_at` on the root product is NOT updated when inventory quantities change — only when product metadata changes. Use `inventory_levels` for inventory change tracking.

---

### customers

#### Endpoint

```
GET https://{shop}.myshopify.com/admin/api/2026-04/customers.json
```

#### Required scopes
`read_customers` (also requires protected customer data approval for production apps)

#### Query parameters

| Parameter | Type | Description |
|---|---|---|
| `limit` | integer | Max results per page. Default: `50`, max: `250` |
| `since_id` | integer | Return only customers with ID **greater than** this value (exclusive, >) |
| `ids` | string | Comma-separated list of customer IDs |
| `created_at_min` | ISO 8601 | Customers created on or after this date |
| `created_at_max` | ISO 8601 | Customers created on or before this date |
| `updated_at_min` | ISO 8601 | Customers updated on or after this date (inclusive, >=) |
| `updated_at_max` | ISO 8601 | Customers updated on or before this date |
| `fields` | string | Comma-separated field names |

#### Incremental sync parameters

| Parameter | Behavior | Inclusive? |
|---|---|---|
| `updated_at_min` | Returns customers where `updated_at >= value` | Yes (>=) |
| `since_id` | Returns customers where `id > value` | No (exclusive, >) |

**Recommended cursor**: `updated_at`

#### Response shape

```json
{
  "customers": [
    { ... }
  ]
}
```

Wrapper key: `"customers"` (array)

#### Customer record fields

**Root-level fields**:

| Field | Type | Read-only | Notes |
|---|---|---|---|
| `id` | integer | yes | Unique customer ID |
| `email` | string | no | Unique email address; max one per shop |
| `first_name` | string | no | Given name |
| `last_name` | string | no | Family name |
| `phone` | string | no | E.164 format (e.g., `+16135551111`) |
| `verified_email` | boolean | yes | Whether email has been verified |
| `orders_count` | integer | yes | Total orders placed |
| `state` | string | no | Account state: `"enabled"`, `"disabled"`, `"invited"`, or `"declined"` |
| `total_spent` | string (decimal) | yes | Lifetime purchase total |
| `last_order_id` | integer | yes | ID of most recent order |
| `last_order_name` | string | yes | Name of most recent order (e.g., `"#1169"`) |
| `currency` | string | yes | **Deprecated** — 3-letter ISO 4217 code from last purchase |
| `note` | string | no | Internal merchant notes |
| `tags` | string | no | Comma-separated customer labels |
| `tax_exempt` | boolean | no | Whether customer is exempt from taxes |
| `tax_exemptions` | array of strings | no | Jurisdiction-specific exemption codes |
| `multipass_identifier` | string | no | Multipass login identifier |
| `password` | string | no | **Deprecated** |
| `password_confirmation` | string | no | **Deprecated** |
| `created_at` | ISO 8601 datetime | yes | Customer creation timestamp |
| `updated_at` | ISO 8601 datetime | yes | Last modification timestamp; use as cursor |
| `admin_graphql_api_id` | string | yes | GraphQL GID |
| `addresses` | array | — | See nested structure below |
| `default_address` | object | — | Same fields as address entry |
| `email_marketing_consent` | object | — | See nested structure below |
| `sms_marketing_consent` | object | — | See nested structure below |

**Nested: `addresses` array** — contains "the ten most recently updated" addresses:

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Address record ID |
| `customer_id` | integer | Parent customer ID |
| `first_name` | string | — |
| `last_name` | string | — |
| `name` | string | Combined first + last name |
| `company` | string | — |
| `address1` | string | Primary street line |
| `address2` | string | Secondary address line |
| `city` | string | — |
| `province` | string | Region name (state, province, prefecture) |
| `province_code` | string | Two-character alphanumeric code |
| `country` | string | Country name |
| `country_code` | string | Two-letter ISO 3166-1 alpha-2 code |
| `country_name` | string | Normalized country name |
| `zip` | string | Postal code |
| `phone` | string | Address-specific phone |
| `default` | boolean | Whether this is the default address |

**Nested: `email_marketing_consent`**:

| Field | Type | Notes |
|---|---|---|
| `state` | string | `"subscribed"`, `"not_subscribed"`, `"pending"`, or `"invalid"` |
| `opt_in_level` | string | Consent type per M3AAWG guidelines |
| `consent_updated_at` | ISO 8601 datetime | When consent was last changed |

**Nested: `sms_marketing_consent`**:

| Field | Type | Notes |
|---|---|---|
| `state` | string | Subscription status |
| `opt_in_level` | string | Opt-in classification |
| `consent_updated_at` | ISO 8601 datetime | Consent date |
| `consent_collected_from` | string | Collection source (e.g., `"OTHER"`) |

#### Ingestion type
`cdc` — Customers can be read incrementally using `updated_at_min`. No delete feed via REST; GDPR erasure will cause a customer record to disappear silently.

#### Edge cases
- A customer email must be unique per store. Attempting to assign the same email to two customers returns an error (relevant if deduplicating during load).
- GDPR data erasure: deleted/erased customer records disappear from the API with no tombstone. The connector cannot detect these deletes without a full snapshot comparison.
- The `addresses` array returns only the **ten most recently updated** addresses — not all historical addresses.
- `currency` field is deprecated; do not rely on it. Use the order-level `currency` instead.
- Customer creation requires at minimum one of: name, phone, or email.

---

### orders

#### Endpoint

```
GET https://{shop}.myshopify.com/admin/api/2026-04/orders.json
```

#### Required scopes
`read_orders` (for last 60 days); `read_orders` + `read_all_orders` (for complete history)

**60-day restriction**: By default, only orders from the last 60 days are accessible. To access all historical orders, the `read_all_orders` scope must be requested and approved. This is a separate scope that requires justification.

#### Query parameters

| Parameter | Type | Description |
|---|---|---|
| `limit` | integer | Max results per page. Default: `50`, max: `250` |
| `since_id` | integer | Return only orders with ID **greater than** this value (exclusive, >) |
| `status` | string | `"open"` (default), `"closed"`, `"cancelled"`, `"any"` |
| `financial_status` | string | `"pending"`, `"authorized"`, `"partially_paid"`, `"paid"`, `"partially_refunded"`, `"refunded"`, `"voided"` |
| `fulfillment_status` | string | `"fulfilled"`, `"partial"`, `"unshipped"`, `"any"`, `"restocked"`, or null |
| `created_at_min` | ISO 8601 | Orders created on or after this date |
| `created_at_max` | ISO 8601 | Orders created on or before this date |
| `updated_at_min` | ISO 8601 | Orders updated on or after this date (inclusive, >=) |
| `updated_at_max` | ISO 8601 | Orders updated on or before this date |
| `processed_at_min` | ISO 8601 | Orders processed on or after this date |
| `processed_at_max` | ISO 8601 | Orders processed on or before this date |
| `attribution_app_id` | string | Filter by sales channel attribution |
| `ids` | string | Comma-separated order IDs |
| `fields` | string | Comma-separated field names |

**Important**: The `status` parameter **defaults to `"open"`** when not specified. To retrieve all orders regardless of status, always pass `status=any`. Connectors doing incremental sync must use `status=any` to avoid missing closed/cancelled orders.

#### Incremental sync parameters

| Parameter | Behavior | Inclusive? |
|---|---|---|
| `updated_at_min` | Returns orders where `updated_at >= value` | Yes (>=) |
| `since_id` | Returns orders where `id > value` | No (exclusive, >) |

**Recommended cursor**: `updated_at` — always combine with `status=any` to capture updates to orders in any status.

**Lookback note**: There are known cases where `updated_at_min` with `updated_at_max` can return incomplete data due to timestamp precision or concurrent writes. Recommended approach: use `updated_at_min = last_sync_time - 1 minute` to avoid missing records near boundaries.

#### Response shape

```json
{
  "orders": [
    { ... }
  ]
}
```

Wrapper key: `"orders"` (array)

#### Order record fields

**Root-level fields**:

| Field | Type | Read-only | Notes |
|---|---|---|---|
| `id` | integer (int64) | yes | Unique order ID |
| `admin_graphql_api_id` | string | yes | GraphQL GID |
| `name` | string | yes | Human-readable order reference (e.g., `"#1001"`) |
| `number` | integer | yes | Sequential order number within the store |
| `confirmation_number` | string | yes | Alphanumeric confirmation shown to customer |
| `token` | string | yes | Unique token for the order |
| `cart_token` | string | yes | Cart token; links to abandoned checkout |
| `checkout_token` | string | yes | Checkout session token |
| `email` | string | no | Customer email |
| `phone` | string | no | Customer phone in E.164 format |
| `created_at` | ISO 8601 datetime | yes | Order creation timestamp |
| `updated_at` | ISO 8601 datetime | yes | Last modification timestamp; use as cursor |
| `processed_at` | ISO 8601 datetime | yes | When payment was processed |
| `closed_at` | ISO 8601 datetime / null | yes | When order was closed; null if open |
| `cancelled_at` | ISO 8601 datetime / null | yes | When order was cancelled; null if not cancelled |
| `cancel_reason` | string / null | yes | `"customer"`, `"fraud"`, `"inventory"`, `"declined"`, `"other"`, or null |
| `financial_status` | string | yes | `"pending"`, `"authorized"`, `"partially_paid"`, `"paid"`, `"partially_refunded"`, `"refunded"`, `"voided"` |
| `fulfillment_status` | string / null | yes | `"fulfilled"`, `"partial"`, `"restocked"`, or null (unfulfilled) |
| `currency` | string | yes | Presentment currency (ISO 4217) |
| `total_price` | string (decimal) | yes | Total charged to customer, incl. taxes & shipping |
| `subtotal_price` | string (decimal) | yes | Sum of line item prices after discounts, before taxes/shipping |
| `total_tax` | string (decimal) | yes | Total tax amount |
| `total_discounts` | string (decimal) | yes | Total discount amount applied |
| `total_line_items_price` | string (decimal) | yes | Sum of all line items before discounts |
| `total_price_usd` | string (decimal) | yes | Total in USD (for shops not using USD) |
| `total_weight` | integer | yes | Total weight in grams |
| `taxes_included` | boolean | yes | Whether taxes are included in line item prices |
| `test` | boolean | yes | Whether order is a test (from Shopify Payments test mode) |
| `confirmed` | boolean | yes | Whether order inventory has been reserved |
| `buyer_accepts_marketing` | boolean | yes | Email marketing consent at time of order |
| `gateway` | string | yes | Payment gateway handle (e.g., `"shopify_payments"`) |
| `app_id` | integer | yes | ID of app that created the order |
| `location_id` | integer / null | yes | POS location ID |
| `user_id` | integer / null | yes | Staff user who placed the order (POS) |
| `source_identifier` | string | yes | Order source identifier |
| `source_url` | string | yes | URL where order originated |
| `referring_site` | string | yes | Referral URL |
| `landing_site` | string | yes | Landing page URL |
| `browser_ip` | string | yes | Customer IP (IPv4 or IPv6) |
| `customer_locale` | string | yes | BCP 47 locale code (e.g., `"en-US"`) |
| `note` | string | no | Merchant order notes |
| `reference` | string | yes | Order reference from external system |
| `customer` | object | yes | Abbreviated customer object |
| `billing_address` | object | no | See address structure below |
| `shipping_address` | object | no | See address structure below |
| `line_items` | array | yes | See nested structure below |
| `shipping_lines` | array | yes | See nested structure below |
| `tax_lines` | array | yes | See nested structure below |
| `discount_codes` | array | yes | See nested structure below |
| `fulfillments` | array | yes | Embedded fulfillment objects |
| `refunds` | array | yes | Embedded refund summaries |
| `payment_details` | object | yes | **Deprecated** — partial card data |

**Nested: address object** (shared by `billing_address` and `shipping_address`):

| Field | Type |
|---|---|
| `first_name` | string |
| `last_name` | string |
| `name` | string |
| `company` | string |
| `address1` | string |
| `address2` | string |
| `city` | string |
| `province` | string |
| `province_code` | string |
| `country` | string |
| `country_code` | string |
| `zip` | string |
| `phone` | string |
| `latitude` | decimal |
| `longitude` | decimal |

**Nested: `line_items` array** (each element):

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Line item ID |
| `variant_id` | integer / null | Product variant ID |
| `product_id` | integer / null | Product ID |
| `title` | string | Product title at time of order |
| `name` | string | Variant name at time of order |
| `quantity` | integer | Quantity ordered |
| `price` | string (decimal) | Unit price |
| `total_discount` | string (decimal) | Total discount on this line |
| `sku` | string | SKU at time of order |
| `variant_title` | string | Variant title |
| `vendor` | string | Vendor name |
| `grams` | integer | Weight in grams |
| `requires_shipping` | boolean | — |
| `taxable` | boolean | — |
| `gift_card` | boolean | Whether this is a gift card |
| `fulfillment_status` | string / null | `"fulfilled"`, `"partial"`, or null |
| `fulfillable_quantity` | integer | Remaining quantity to fulfill |
| `fulfillment_service` | string | Fulfillment service handle |
| `product_exists` | boolean | Whether product still exists |
| `price_set` | object | `{shop_money: {amount, currency_code}, presentment_money: {amount, currency_code}}` |
| `total_discount_set` | object | Same shape as `price_set` |
| `discount_allocations` | array | Per-discount allocation breakdown |
| `tax_lines` | array | Tax breakdown for this line item |
| `duties` | array | Duty charges for international orders |
| `properties` | array | Custom line item properties (key/value pairs) |

**Nested: `shipping_lines` array** (each element):

| Field | Type |
|---|---|
| `id` | integer |
| `carrier_identifier` | string |
| `code` | string |
| `delivery_category` | string |
| `discounted_price` | string (decimal) |
| `discounted_price_set` | object |
| `phone` | string |
| `price` | string (decimal) |
| `price_set` | object |
| `requested_fulfillment_service_id` | string |
| `source` | string |
| `title` | string |
| `tax_lines` | array |
| `discount_allocations` | array |

**Nested: `tax_lines` array** (each element):

| Field | Type |
|---|---|
| `title` | string |
| `price` | string (decimal) |
| `rate` | decimal |
| `price_set` | object |
| `channel_liable` | boolean |

**Nested: `discount_codes` array** (each element):

| Field | Type |
|---|---|
| `code` | string |
| `amount` | string (decimal) |
| `type` | string (`"percentage"`, `"fixed_amount"`, `"shipping"`) |

#### Ingestion type
`cdc` — Orders can be read incrementally using `updated_at_min` + `status=any`. Orders are updated (and `updated_at` bumped) when fulfillments, refunds, cancellations, or payment status changes occur.

#### Edge cases
- **Default status is `"open"`** — always pass `status=any` to capture all order lifecycle states.
- **60-day restriction**: Without `read_all_orders` scope, orders older than 60 days are inaccessible. Initial historical load will be incomplete without this scope.
- Orders have `updated_at` bumped when child records (refunds, fulfillments) are added, making the parent order the reliable change indicator.
- `payment_details` is deprecated and may not appear for all orders.
- Cancelled orders have both `cancelled_at` and `cancel_reason` set; `closed_at` is also set.
- Test orders (`"test": true`) are created by Shopify Payments test mode; include or exclude per use case.
- The embedded `fulfillments` and `refunds` arrays in the order response are summaries — use the dedicated `/orders/{id}/refunds.json` and `/orders/{id}/fulfillments.json` endpoints for complete data.

---

### refunds

#### Endpoint

```
GET https://{shop}.myshopify.com/admin/api/2026-04/orders/{order_id}/refunds.json
```

This is a **child endpoint** — it requires the parent `order_id`. There is no top-level `/refunds.json` endpoint. Each call returns all refunds for a single order.

#### Required scopes
`read_orders`

#### Query parameters

| Parameter | Type | Description |
|---|---|---|
| `limit` | integer | Max results per page. Default: `50`, max: `250` |
| `fields` | string | Comma-separated field names |
| `in_shop_currency` | boolean | If `true`, return amounts in shop currency. Default: `false` |

**No incremental filter parameters** — `updated_at_min`, `since_id`, and similar filters are not supported on this endpoint. Refunds must be fetched per-order.

#### Response shape

```json
{
  "refunds": [
    { ... }
  ]
}
```

Wrapper key: `"refunds"` (array)

#### Refund record fields

**Root-level fields**:

| Field | Type | Read-only | Notes |
|---|---|---|---|
| `id` | integer | yes | Unique refund ID |
| `order_id` | integer | yes | Parent order ID |
| `created_at` | ISO 8601 datetime | yes | When refund was created; use as cursor |
| `processed_at` | ISO 8601 datetime | — | When the refund transaction was processed |
| `note` | string | no | Internal reason for refund |
| `user_id` | integer | yes | Staff user who created the refund |
| `restock` | boolean | yes | **Deprecated** |
| `duties` | array | yes | Refunded duty amounts |
| `refund_duties` | array | — | Explicit duty refund objects |
| `refund_line_items` | array | — | See nested structure below |
| `refund_shipping_lines` | array | yes | Refunded shipping line details |
| `order_adjustments` | array | yes | See nested structure below |
| `transactions` | array | — | See nested structure below |
| `total_duties_set` | object | — | `{shop_money, presentment_money}` |
| `additional_fees` | array | — | Additional fee line items |
| `total_additional_fees_set` | object | — | `{shop_money, presentment_money}` |

**No `updated_at` field** — refunds are append-only; use `created_at` as the cursor field for append-style sync.

**Nested: `refund_line_items` array** (each element):

| Field | Type | Notes |
|---|---|---|
| `id` | integer | — |
| `line_item_id` | integer | Original order line item ID |
| `quantity` | integer | Quantity being refunded |
| `restock_type` | string | `"no_restock"`, `"cancel"`, `"return"`, `"legacy_restock"` |
| `location_id` | integer | Location where restock occurs |
| `subtotal` | decimal | Line subtotal before tax |
| `total_tax` | decimal | Tax amount |
| `subtotal_set` | object | `{shop_money, presentment_money}` |
| `total_tax_set` | object | `{shop_money, presentment_money}` |
| `line_item` | object | Full nested line item object (same structure as order `line_items`) |

**Nested: `order_adjustments` array** (each element):

| Field | Type |
|---|---|
| `id` | integer |
| `order_id` | integer |
| `refund_id` | integer |
| `amount` | decimal |
| `tax_amount` | decimal |
| `kind` | string (e.g., `"refund_discrepancy"`, `"shipping_refund"`) |
| `reason` | string |
| `amount_set` | object (`{shop_money, presentment_money}`) |
| `tax_amount_set` | object (`{shop_money, presentment_money}`) |

**Nested: `transactions` array** (each element):

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Transaction ID |
| `order_id` | integer | — |
| `amount` | string (decimal) | — |
| `kind` | string | Transaction type (e.g., `"refund"`) |
| `gateway` | string | Payment gateway handle |
| `status` | string | `"pending"`, `"failure"`, `"success"`, `"error"` |
| `message` | string | Gateway message |
| `created_at` | ISO 8601 datetime | — |
| `test` | boolean | Whether test transaction |
| `authorization` | string | Gateway authorization code |
| `currency` | string | ISO 4217 code |
| `location_id` | integer / null | — |
| `user_id` | integer / null | — |
| `parent_id` | integer / null | Parent transaction ID |
| `device_id` | integer / null | — |
| `receipt` | object | Gateway receipt object |
| `error_code` | string | Machine-readable error code |
| `source_name` | string | Origin channel (e.g., `"web"`) |
| `processed_at` | ISO 8601 datetime | — |

#### Ingestion type
`append` — Refunds are append-only (no `updated_at`). Use `created_at` as the cursor. Because this is a child endpoint, the connector must iterate over orders to collect refunds. Recommended approach: paginate through all orders using `updated_at_min` on the orders endpoint and fetch refunds for each modified order.

#### Edge cases
- No standalone incremental endpoint — refunds must be pulled per-order.
- No `updated_at` field; `created_at` is append-only. Refund records are not modified after creation.
- The `restock` field is deprecated; use `restock_type` on individual `refund_line_items` instead.
- `transactions` within refunds may be empty if the refund was a manual adjustment (no gateway transaction).

---

### fulfillments

#### Endpoint

```
GET https://{shop}.myshopify.com/admin/api/2026-04/orders/{order_id}/fulfillments.json
```

This is a **child endpoint** requiring the parent `order_id`. There is no top-level `/fulfillments.json` endpoint. Each call returns all fulfillments for a single order.

#### Required scopes
`read_orders` OR `read_fulfillments` (either scope grants access)

#### Query parameters

| Parameter | Type | Description |
|---|---|---|
| `limit` | integer | Max results per page. Default: `50`, max: `250` |
| `since_id` | integer | Return only fulfillments with ID **greater than** this value (exclusive, >) |
| `created_at_min` | ISO 8601 | Fulfillments created on or after this date |
| `created_at_max` | ISO 8601 | Fulfillments created on or before this date |
| `updated_at_min` | ISO 8601 | Fulfillments updated on or after this date (inclusive, >=) |
| `updated_at_max` | ISO 8601 | Fulfillments updated on or before this date |
| `fields` | string | Comma-separated field names |

#### Incremental sync parameters

| Parameter | Behavior | Inclusive? |
|---|---|---|
| `updated_at_min` | Returns fulfillments where `updated_at >= value` | Yes (>=) |
| `since_id` | Returns fulfillments where `id > value` | No (exclusive, >) |

#### Response shape

```json
{
  "fulfillments": [
    { ... }
  ]
}
```

Wrapper key: `"fulfillments"` (array)

#### Fulfillment record fields

| Field | Type | Read-only | Notes |
|---|---|---|---|
| `id` | integer | yes | Unique fulfillment ID |
| `order_id` | integer | yes | Parent order ID |
| `status` | string | yes | `"pending"`, `"open"`, `"success"`, `"cancelled"`, `"error"`, `"failure"` |
| `created_at` | ISO 8601 datetime | yes | Creation timestamp |
| `updated_at` | ISO 8601 datetime | yes | Last modification timestamp; use as cursor |
| `service` | string | yes | Fulfillment service provider handle |
| `tracking_company` | string | no | Carrier name (e.g., `"USPS"`, `"UPS"`, `"FedEx"`) |
| `tracking_number` | string | no | Primary tracking number |
| `tracking_numbers` | array of strings | no | All tracking numbers (multi-package shipments) |
| `tracking_url` | string | no | Primary tracking URL |
| `tracking_urls` | array of strings | no | All tracking URLs |
| `shipment_status` | string / null | yes | Carrier status: `"label_printed"`, `"label_purchased"`, `"attempted_delivery"`, `"ready_for_pickup"`, `"picked_up"`, `"confirmed"`, `"in_transit"`, `"out_for_delivery"`, `"delivered"`, `"failure"` |
| `location_id` | integer | yes | Location where fulfillment was processed |
| `name` | string | yes | Fulfillment identifier, format `#OrderName.N` (e.g., `#1001.1`) |
| `receipt` | object | yes | **Deprecated** — gateway authorization info |
| `line_items` | array | yes | See nested structure below |
| `origin_address` | object / null | no | See nested structure below |
| `admin_graphql_api_id` | string | yes | GraphQL GID |

**Nested: `line_items` array** (each element):

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Line item ID |
| `variant_id` | integer | Product variant ID |
| `product_id` | integer | Product ID |
| `title` | string | Product title |
| `name` | string | Variant name |
| `quantity` | integer | Quantity in this fulfillment |
| `sku` | string | SKU |
| `variant_title` | string | Variant title |
| `grams` | integer | Weight in grams |
| `price` | string (decimal) | Unit price |
| `price_set` | object | `{shop_money, presentment_money}` |
| `requires_shipping` | boolean | — |
| `taxable` | boolean | — |
| `gift_card` | boolean | — |
| `product_exists` | boolean | Whether product still exists |
| `fulfillable_quantity` | integer | Remaining quantity to fulfill |
| `fulfillment_status` | string / null | — |
| `fulfillment_line_item_id` | integer | — |
| `vendor` | string | — |
| `variant_inventory_management` | string | — |
| `fulfillment_service` | string | — |
| `properties` | array | Custom properties |
| `total_discount` | string (decimal) | — |
| `total_discount_set` | object | — |
| `discount_allocations` | array | — |
| `tax_lines` | array | — |
| `duties` | array | — |

**Nested: `origin_address`** (optional, for tax purposes):

| Field | Type |
|---|---|
| `address1` | string |
| `address2` | string |
| `city` | string |
| `zip` | string |
| `province_code` | string |
| `country_code` | string (ISO 3166-1 alpha-2, required) |

#### Ingestion type
`cdc` — Fulfillments have `updated_at` and can be read incrementally using `updated_at_min`. However, since this is a child endpoint of orders, the connector must iterate over orders. Recommended approach: pull orders changed since last sync (using orders `updated_at_min`), then for each changed order fetch its fulfillments.

#### Edge cases
- Fulfillment `shipment_status` is updated by carrier tracking webhooks — the record's `updated_at` is bumped each time a tracking event occurs.
- `receipt` is deprecated; do not rely on it.
- `tracking_number` and `tracking_url` are singular convenience fields; `tracking_numbers` and `tracking_urls` are arrays for multi-parcel shipments.

---

### locations

#### Endpoint

```
GET https://{shop}.myshopify.com/admin/api/2026-04/locations.json
```

#### Required scopes
`read_locations`

#### Query parameters

| Parameter | Type | Description |
|---|---|---|
| `limit` | integer | Max results per page. Default: `50`, max: `250` |

**No incremental filter parameters** — `updated_at_min`, `since_id`, and similar filters are not documented for this endpoint. Locations must be fetched in full.

#### Response shape

```json
{
  "locations": [
    { ... }
  ]
}
```

Wrapper key: `"locations"` (array)

#### Count endpoint

```
GET https://{shop}.myshopify.com/admin/api/2026-04/locations/count.json
```

Returns: `{"count": N}`

#### Location record fields

| Field | Type | Read-only | Notes |
|---|---|---|---|
| `id` | integer | yes | Unique location ID |
| `name` | string | no | Location name |
| `active` | boolean | no | Whether location is active (can sell, stock, fulfill) |
| `legacy` | boolean | yes | Whether this is a fulfillment service location (not a physical location) |
| `address1` | string | no | Primary street address |
| `address2` | string | no | Secondary address line |
| `city` | string | no | City |
| `province` | string | no | Province/state name |
| `province_code` | string | no | Two-character province/state code |
| `country` | string | no | Country name |
| `country_code` | string | no | Two-letter ISO 3166-1 alpha-2 code |
| `country_name` | string | yes | Normalized country name |
| `localized_country_name` | string | yes | Localized country name |
| `localized_province_name` | string | yes | Localized province/state name |
| `zip` | string | no | Postal code |
| `phone` | string | no | Contact number |
| `created_at` | ISO 8601 datetime | yes | Creation timestamp |
| `updated_at` | ISO 8601 datetime | yes | Last modification timestamp |
| `admin_graphql_api_id` | string | yes | GraphQL GID |

#### Ingestion type
`snapshot` — The locations endpoint has no documented incremental filter parameters. Most stores have a small, stable set of locations (typically fewer than 20). Full re-fetch on each sync is the appropriate strategy.

**Alternative**: Because locations have `updated_at`, a connector could pull all locations and filter client-side, or watch for changes via webhooks. For simplicity, full snapshot is recommended.

#### Edge cases
- `legacy: true` indicates a fulfillment service location (virtual, not a physical warehouse). These appear as locations but represent third-party fulfillment services.
- Inactive locations (`active: false`) are still returned by the API. Include them in the sync so downstream can filter.
- Location count for most stores is small (<50). The full snapshot cost is low.

---

### inventory_levels

#### Endpoint

```
GET https://{shop}.myshopify.com/admin/api/2026-04/inventory_levels.json
```

#### Required scopes
`read_inventory`

#### Query parameters

**At least one of `location_ids` or `inventory_item_ids` is required.**

| Parameter | Type | Description |
|---|---|---|
| `location_ids` | string | Comma-separated location IDs (max 50 per request) |
| `inventory_item_ids` | string | Comma-separated inventory item IDs (max 50 per request) |
| `limit` | integer | Max results per page. Default: `50`, max: `250` |
| `updated_at_min` | ISO 8601 | Filter by last updated timestamp (inclusive, >=) |

**Error behavior**: A request with neither `location_ids` nor `inventory_item_ids` returns **HTTP 422 Unprocessable Entity**.

#### Incremental sync parameters

| Parameter | Behavior | Inclusive? |
|---|---|---|
| `updated_at_min` | Returns levels where `updated_at >= value` | Yes (>=) |

#### Response shape

```json
{
  "inventory_levels": [
    { ... }
  ]
}
```

Wrapper key: `"inventory_levels"` (array)

#### Inventory level record fields

| Field | Type | Read-only | Notes |
|---|---|---|---|
| `inventory_item_id` | integer | yes | Associated inventory item ID (links to product variant via `inventory_item_id`) |
| `location_id` | integer | — | Associated location ID |
| `available` | integer / null | — | Available quantity at this location; `null` if inventory tracking is disabled for this item |
| `updated_at` | ISO 8601 datetime | yes | Last modification timestamp; use as cursor |
| `admin_graphql_api_id` | string | yes | GraphQL GID in format `gid://shopify/InventoryLevel/{location_id}?inventory_item_id={item_id}` |

**Note**: There is no `id` field on inventory_level records. The composite key is `(inventory_item_id, location_id)`.

#### Connector enumeration strategy

Because at least one filter is required, the connector must use one of these approaches:

**Approach 1 — Enumerate by location** (recommended):
1. Fetch all locations using `GET /locations.json`.
2. Batch location IDs (up to 50 per request) and query `inventory_levels.json?location_ids=...`.
3. Optionally add `updated_at_min` for incremental filtering.

**Approach 2 — Enumerate by inventory item**:
1. Fetch all product variants to collect `inventory_item_id` values.
2. Batch inventory item IDs (up to 50 per request) and query `inventory_levels.json?inventory_item_ids=...`.
3. Optionally add `updated_at_min`.

Approach 1 is preferred because location counts are small (<50 typically), making it a single request in most cases.

**Incremental example** (by location, with time filter):
```
GET /admin/api/2026-04/inventory_levels.json?location_ids=1,2,3&updated_at_min=2026-04-01T00:00:00Z&limit=250
```

#### Ingestion type
`cdc` — Inventory levels have `updated_at` and support `updated_at_min` filtering. Can be read incrementally by filtering on `updated_at_min` per location batch.

#### Edge cases
- `available` is `null` when inventory tracking is disabled for the item (`inventory_management: null` on the variant). Null means "untracked", not zero.
- Records are identified by composite key `(inventory_item_id, location_id)` — not a single `id` field.
- Inventory changes do NOT update the parent product's `updated_at`. The `inventory_levels.updated_at` must be tracked independently.
- Max 50 `location_ids` or `inventory_item_ids` per request — for stores with many locations, pagination over location batches is needed.
- Airbyte implements `inventory_levels` as a child stream of locations, using GraphQL Bulk Operations for large stores.

---

## Edge Cases and Gotchas

1. **`status=any` is mandatory for orders**: The orders endpoint defaults to `status=open`. Omitting `status=any` silently excludes closed, cancelled, and archived orders, which is almost always wrong for a data connector.

2. **60-day orders restriction**: Without `read_all_orders` scope, historical order data older than 60 days is inaccessible. Initial full loads will be incomplete. Request this scope as part of app setup.

3. **Product REST deprecation**: The products REST endpoint is deprecated since 2024-04. It continues to function but should be migrated to GraphQL for long-term viability. Products with >100 variants may return truncated variant lists via REST.

4. **REST Admin API legacy status**: As of 2024-10-01, the REST Admin API is officially "legacy". New public apps must use GraphQL from 2025-04-01. This connector uses REST but a GraphQL migration plan should exist.

5. **No delete tombstones in REST**: Deleted products, customers, and orders simply disappear from list endpoints. No `deleted_at` field. To detect deletes, compare a full snapshot to the previous sync. Airbyte's connector handles order deletes via webhooks.

6. **Refunds and fulfillments are child-only endpoints**: There is no top-level refunds or fulfillments list. The connector must iterate over orders and fetch children per order, which is expensive at scale. Consider tracking changed orders via `updated_at_min` and only re-fetching children for those orders.

7. **inventory_levels requires at least one filter**: Cannot enumerate all inventory levels without knowing location IDs or inventory item IDs. Use the locations endpoint to bootstrap.

8. **`available: null` vs `available: 0`**: `null` means inventory tracking is disabled for that item; `0` means tracked but out of stock. These must be treated differently downstream.

9. **Cursor fields and timezone**: `updated_at_min` values should be sent in UTC (e.g., `2026-04-01T00:00:00+00:00`). Shopify stores with non-UTC timezone settings may surface timestamps in local time in responses; normalize to UTC in the connector.

10. **Lookback window on incremental sync**: When using `updated_at_min`, apply a lookback of 1–5 minutes to the previous sync high-water mark to handle write races and timestamp imprecision. Deduplicate on primary key downstream.

11. **page_info tokens are temporary**: Do not persist `page_info` cursor values between connector runs. They expire and must not be reused. Only use them within a single sync session to paginate through a result set.

12. **`Link` header may be absent**: When a result set fits on one page, the `Link` header is not present. Always check for the header's existence before attempting to parse it.

13. **Addresses on customers are capped at 10**: The `addresses` array returns at most the 10 most recently updated addresses. Historical addresses beyond 10 are not accessible via this endpoint.

14. **`customer.currency` is deprecated**: Do not use `customer.currency`. Use `order.currency` or `order.presentment_currency` instead.

15. **Orders `updated_at` bumped by children**: When a refund or fulfillment is added to an order, the order's `updated_at` is updated. This means querying orders by `updated_at_min` will surface orders that have had fulfillment or refund activity, which is useful for change detection but means the connector will re-read a lot of orders.

---

## Field Type Mapping

| Shopify type | Python / PySpark type |
|---|---|
| integer (id, int64) | `LongType` |
| integer (count, quantity, grams) | `IntegerType` |
| string | `StringType` |
| string (decimal) — e.g. `"10.00"` | `DecimalType(20, 2)` or `StringType` (preserve precision) |
| boolean | `BooleanType` |
| ISO 8601 datetime | `TimestampType` |
| array | `ArrayType(...)` — schema depends on elements |
| object / nested struct | `StructType(...)` |
| null-possible fields | wrap in `NullType` / allow nullable |

**Note on decimal strings**: Shopify returns monetary amounts as strings (e.g., `"19.99"`) to preserve decimal precision. Cast to `DecimalType(20, 2)` in Spark, or keep as `StringType` and cast in downstream SQL.

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://shopify.dev/docs/api/admin-rest/2026-04/resources/product | 2026-04-19 | High | Products endpoint, all fields, variants/options/images schema, deprecation status |
| Official Docs | https://shopify.dev/docs/api/admin-rest/2026-04/resources/customer | 2026-04-19 | High | Customers endpoint, all fields, nested address/consent objects |
| Official Docs | https://shopify.dev/docs/api/admin-rest/2026-04/resources/order | 2026-04-19 | High | Orders endpoint fields, 60-day restriction, status=any requirement |
| Official Docs | https://shopify.dev/docs/api/admin-rest/2026-04/resources/refund | 2026-04-19 | High | Refunds endpoint, all fields, no updated_at, no incremental filter |
| Official Docs | https://shopify.dev/docs/api/admin-rest/2026-04/resources/fulfillment | 2026-04-19 | High | Fulfillments endpoint, all fields, incremental params (updated_at_min, since_id) |
| Official Docs | https://shopify.dev/docs/api/admin-rest/2026-04/resources/location | 2026-04-19 | High | Locations endpoint, all fields, no incremental filter |
| Official Docs | https://shopify.dev/docs/api/admin-rest/2026-04/resources/inventorylevel | 2026-04-19 | High | Inventory levels endpoint, required filters, composite key, updated_at |
| Official Docs | https://shopify.dev/docs/api/admin-rest/latest/resources/inventoryitem | 2026-04-19 | High | InventoryItem relationship to variants and levels, ids parameter |
| Official Docs | https://shopify.dev/docs/api/admin-rest/usage/rate-limits | 2026-04-19 | High | Bucket sizes by plan tier, leak rates, X-Shopify-Shop-Api-Call-Limit header, 429 behavior |
| Official Docs | https://shopify.dev/docs/api/usage/pagination-rest | 2026-04-19 | High | Link header format, page_info extraction, parameter restrictions |
| Official Docs | https://shopify.dev/docs/api/admin-rest/usage/versioning | 2026-04-19 | High | 12-month support window, quarterly release cadence, end-of-life fallback behavior |
| Official Docs | https://shopify.dev/docs/api/admin-rest/usage/access-scopes | 2026-04-19 | High | Scope handles for products, customers, orders, inventory, locations, fulfillments |
| Official Docs | https://shopify.dev/docs/api/release-notes/previous-versions/2024-04 | 2026-04-19 | High | Products REST deprecation since 2024-04 |
| Airbyte | https://docs.airbyte.com/integrations/sources/shopify | 2026-04-19 | High | Supported streams, cursor fields (updated_at for most tables), inventory levels via GraphQL BULK, refunds as child stream |
| Web search / community | https://community.shopify.com/t/orders-api-using-updated_at_min-and-updated_at_max-returning-incomplete-data/94934 | 2026-04-19 | Medium | Known issue: updated_at_min/max can return incomplete data; lookback window recommended |
| Web search / blog | https://kirillplatonov.com/posts/shopify-api-rate-limits/ | 2026-04-19 | Medium | Leaky bucket explanation, bucket sizes cross-referenced against official docs |

---

## References

- [Shopify REST Admin API Reference (2026-04)](https://shopify.dev/docs/api/admin-rest/2026-04)
- [Product Resource](https://shopify.dev/docs/api/admin-rest/2026-04/resources/product)
- [Customer Resource](https://shopify.dev/docs/api/admin-rest/2026-04/resources/customer)
- [Order Resource](https://shopify.dev/docs/api/admin-rest/2026-04/resources/order)
- [Refund Resource](https://shopify.dev/docs/api/admin-rest/2026-04/resources/refund)
- [Fulfillment Resource](https://shopify.dev/docs/api/admin-rest/2026-04/resources/fulfillment)
- [Location Resource](https://shopify.dev/docs/api/admin-rest/2026-04/resources/location)
- [InventoryLevel Resource](https://shopify.dev/docs/api/admin-rest/2026-04/resources/inventorylevel)
- [InventoryItem Resource](https://shopify.dev/docs/api/admin-rest/latest/resources/inventoryitem)
- [REST Admin API Rate Limits](https://shopify.dev/docs/api/admin-rest/usage/rate-limits)
- [REST Admin API Pagination](https://shopify.dev/docs/api/usage/pagination-rest)
- [REST Admin API Versioning](https://shopify.dev/docs/api/admin-rest/usage/versioning)
- [Access Scopes Reference](https://shopify.dev/docs/api/admin-rest/usage/access-scopes)
- [2024-04 Release Notes (Product deprecation)](https://shopify.dev/docs/api/release-notes/previous-versions/2024-04)
- [Airbyte Shopify Connector](https://docs.airbyte.com/integrations/sources/shopify)
- [Fivetran Shopify Connector](https://fivetran.com/docs/connectors/applications/shopify)
