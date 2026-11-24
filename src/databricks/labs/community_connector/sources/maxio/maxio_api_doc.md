# Maxio / SaaSOptics REST API Documentation

> **Scope note**: This connector targets the **SaaSOptics REST API v1.0** — Maxio's billing and revenue-recognition product (formerly SaaSOptics, distinct from Maxio Advanced Billing / Chargify). The base URL pattern is `https://{server_subdomain}.saasoptics.com/{account_name}/api/v1.0/`.

---

## Authorization

**Method**: Token Authentication (Django REST Framework style)

The API uses a pre-issued API token passed in every request via an HTTP header. This is **not** Bearer/OAuth — the header keyword is `Token`, not `Bearer`.

```
Authorization: Token <api_key>
```

**Full header set used by the production notebook:**
```
Authorization: Token <api_key>
Accept: application/json
Content-Type: application/json
User-Agent: spark-resource-client/1.0
```

**Obtaining the token:**
1. Log in to your SaaSOptics/Maxio Core instance.
2. Navigate to **Admin > API Tokens** in the settings section.
3. Create a new token with a descriptive name. Only account admins can generate tokens initially; once created, users can regenerate their own.
4. Store the token securely (e.g., in a key vault). It does not expire on a fixed schedule but can be revoked by an admin.

**Required configuration parameters for this connector:**
| Parameter | Description | Example |
|---|---|---|
| `server_subdomain` | Subdomain portion of the instance hostname | `s12` |
| `account_name` | Account/instance path segment | `example_co` |
| `api_key` | The API token value | `abc123...` |

The base URL is assembled as:
```
https://{server_subdomain}.saasoptics.com/{account_name}/api/v1.0/
```

Both `server_subdomain` and `account_name` are customer-specific and vary per tenant. Example: `https://acme.saasoptics.com/example_co/api/v1.0/`.

**Scope / permissions**: The API token inherits the permissions of the user account that generated it. No OAuth scopes are defined; access is all-or-nothing at the instance level. TBD: verify during live testing whether read-only API tokens are available as a separate permission level.

---

## Object List

The SaaSOptics API exposes a static set of REST resources at predictable paths under the base URL. The object list is not itself discoverable via an API endpoint — it is a fixed set.

**Tables in scope for this connector (7 tables):**

| Logical Table | API Endpoint Path | Notes |
|---|---|---|
| `customers` | `/customers/` | Top-level resource |
| `items` | `/items/` | Product catalog entries |
| `invoices` | `/invoices/` | Parent resource; `line_items[]` nested inside |
| `invoices_line_items` | Derived from `/invoices/` | Exploded from `line_items[]` array; no standalone endpoint |
| `contracts` | `/contracts/` | Top-level resource |
| `transactions` | `/transactions/` | Top-level resource |
| `revenue_entries` | `/transactions/{transaction_id}/revenue_entries/` | Sub-resource per transaction (see note below) |

**Note on `revenue_entries` endpoint discrepancy**: The production notebook fetches revenue entries as a sub-resource of each transaction: `GET /transactions/{transaction_id}/revenue_entries/`. The Singer tap-saasoptics implementation references a top-level `/revenue_entries/` endpoint with `modified__gte`/`modified__lte` filter support. **Both may exist.** During live validation against a real SaaSOptics tenant we confirmed the top-level `/revenue_entries/` endpoint returns records in bulk and supports `modified__gte` filtering. The connector now uses the top-level endpoint as the **primary read path** (cdc, `modified` cursor) — this matches Singer's approach and avoids fanning out one HTTP call per transaction. The sub-resource traversal lives in `_read_revenue_entries_sub_resource` as a fallback for tenants where the top-level endpoint is unavailable.

**Additional endpoints known to exist (not in scope for this connector):**
`/accounts/`, `/auto_renewal_profiles/`, `/billing_descriptions/`, `/billing_methods/`, `/country_codes/`, `/currency_codes/`, `/payment_terms/`, `/registers/`, `/revenue_recognition_methods/`, `/sales_orders/`, plus soft-delete shadow endpoints (`/deleted_contracts/`, `/deleted_invoices/`, `/deleted_transactions/`, `/deleted_revenue_entries/`).

---

## Object Schema

Schemas are static and documented per-table below. There is no schema-discovery API endpoint. Schemas are derived primarily from the production notebook's normalization functions (`normalize_customers_df`, `normalize_items_df`, `normalize_invoices_df`, `normalize_contracts_df`, `normalize_transactions_df`, `normalize_revenue_entries_df`, and the `write_invoice_lines_from_df` projection) cross-referenced against Singer tap JSON schemas.

**Nested struct handling conventions (apply globally):**
- `billing_profile.*` fields are flattened with a `billing_` prefix (e.g., `billing_profile.city` → `billing_city`). Exception: on `customers`, the shipping address sub-fields of `billing_profile` are flattened without a `billing_` prefix (e.g., `billing_profile.shipping_addr1` → `shipping_addr1`) to match the notebook's normalization.
- `auditentry.*` fields are flattened with an `audit_` prefix on `invoices` and `transactions` (e.g., `auditentry.created` → `audit_created`). On `customers`, `auditentry` is retained as a struct (sanitized to JSON string if needed by downstream readers).
- `line_items[]` on `invoices` is exploded into a separate `invoices_line_items` table keyed by `invoice_id`. The `line_items` column is **dropped** from the `invoices` table output to avoid struct-in-Parquet issues.

---

## Get Object Primary Keys

Primary keys are static integer `id` fields on all tables. There is no API for discovering primary keys.

| Table | Primary Key | Notes |
|---|---|---|
| `customers` | `id` | Integer |
| `items` | `id` | Integer |
| `invoices` | `id` | Integer |
| `invoices_line_items` | `line_id` (from `line_item.id`) | Composite uniqueness: `(invoice_id, line_id)` recommended |
| `contracts` | `id` | Integer |
| `transactions` | `id` | Integer |
| `revenue_entries` | `id` | Integer; `transaction` (FK) is also present on every row |

---

## Object Ingestion Type

| Table | Ingestion Type | Cursor Field | Filter Params | Notes |
|---|---|---|---|---|
| `customers` | `cdc` | `modified` | `modified__gte`, `modified__lte` | Confirmed incremental support via Singer tap |
| `items` | `cdc` | `modified` | `modified__gte`, `modified__lte` | Confirmed incremental support |
| `invoices` | `cdc` | `auditentry.modified` | `auditentry__modified__gte`, `auditentry__modified__lte` | Uses `auditentry` path for filter; replication key is `auditentry_modified` in Singer schema |
| `invoices_line_items` | `snapshot` | — | Derived from invoices; inherits parent sync cadence | No independent cursor; re-derived on each invoices sync |
| `contracts` | `cdc` | `modified` | `modified__gte`, `modified__lte` | Confirmed incremental support |
| `transactions` | `cdc` | `auditentry.modified` | `auditentry__modified__gte`, `auditentry__modified__lte` | Uses `auditentry` path for filter (Singer tap); notebook shows top-level `modified` field also present |
| `revenue_entries` | `cdc` | `modified` | `modified__gte`, `modified__lte` (top-level endpoint only) | Sub-resource traversal has no date filter — must enumerate all transaction IDs. If top-level endpoint is accessible, use `modified__gte`/`modified__lte` instead. **Verify during live testing.** |

**Known limitation — incremental filter confirmation**: The `modified__gte` / `modified__lte` and `auditentry__modified__gte` / `auditentry__modified__lte` parameters are confirmed by the Singer tap-saasoptics implementation (which runs in production via Stitch). The SaaSOptics public API docs are sparse and do not explicitly document these filter parameters. If they do not work on a given tenant/version, fall back to **snapshot** mode and document the finding.

**Delete handling**: The SaaSOptics API exposes soft-delete shadow endpoints (`/deleted_contracts/`, `/deleted_invoices/`, `/deleted_transactions/`, `/deleted_revenue_entries/`) with an incremental `deleted` timestamp. These are **not in scope** for the current connector but upgrading to `cdc_with_deletes` is possible by adding these endpoints.

---

## Read API for Data Retrieval

### Pagination

The API uses **DRF (Django REST Framework) cursor/next-page pagination**:

- **Response envelope**: `{"count": <int>, "next": "<url_or_null>", "previous": "<url_or_null>", "results": [...]}`
- **Page size control**: `page_size` query parameter (notebook uses 100 as default; DRF default is typically also 100).
- **Advancing pages**: Follow the `next` URL from the previous response. The `next` field is either an absolute URL or `null` when exhausted.
- **Relative vs. absolute `next`**: The notebook defensively handles relative `next` URLs by joining them against the original base URL via `urljoin`. In practice, SaaSOptics returns absolute URLs, but the defensive handling should be preserved.
- **Fallback**: If the response is a plain JSON list (not wrapped in `{"results": [...]}`) — which can happen on some endpoints or API versions — the notebook falls through to handle it directly.

**Example first-page request:**
```
GET https://acme.saasoptics.com/example_co/api/v1.0/customers/?page_size=100
Authorization: Token <api_key>
Accept: application/json
```

**Example response envelope:**
```json
{
  "count": 542,
  "next": "https://acme.saasoptics.com/example_co/api/v1.0/customers/?page=2&page_size=100",
  "previous": null,
  "results": [
    { "id": 1, "name": "Acme Corp", ... },
    ...
  ]
}
```

**Subsequent page**: Follow `next` URL directly. Do not re-send `page_size` after the first request — the `next` URL already encodes it.

**Maximum page size**: Not officially documented. The notebook uses 100. TBD: test whether larger values (e.g., 500) are accepted without error during live testing.

### Incremental Reads

For endpoints that support date filtering:

```
GET /customers/?page_size=100&modified__gte=2024-01-01T00:00:00Z&modified__lte=2024-02-01T00:00:00Z
GET /invoices/?page_size=100&auditentry__modified__gte=2024-01-01T00:00:00Z&auditentry__modified__lte=2024-02-01T00:00:00Z
```

The Singer tap uses **date windowing**: it splits the full time range from the last bookmark to now into fixed-size windows (default 60 days, configurable) to avoid timeouts on large result sets. This pattern is recommended for production use.

### Rate Limiting

- **HTTP 429 handling**: The API returns HTTP 429 with a `Retry-After` header when the rate limit is exceeded. The production notebook honors `Retry-After` and sleeps that many seconds before retrying.
- **Documented quota**: No official rate limit quota is published in SaaSOptics documentation. The notebook caps client-side throughput at approximately **5 requests per second** (`MAXIO_RPS = 5`) as a conservative self-imposed limit.
- **Retry strategy**: The notebook uses exponential backoff (`backoff_factor=0.5`, up to 5 retries) on 429, 500, 502, 503, and 504 status codes via `urllib3.Retry`.
- **Recommendation**: Stay at or below 5 RPS in the connector implementation. Honor the `Retry-After` value on 429 responses.

### HTTP Method

All data retrieval uses `GET`. No `POST`-based query endpoints are required for the tables in scope.

### Sub-resource Traversal (revenue_entries)

Revenue entries require a two-step fetch:

1. Enumerate all transaction IDs from the `transactions` table.
2. For each transaction ID, page through: `GET /transactions/{transaction_id}/revenue_entries/?page_size=100`

The notebook performs this in parallel across Spark partitions. In a sequential connector, process in batches and apply the same 5 RPS limit across all sub-resource calls.

**Back-fill of `transaction` foreign key**: The SaaSOptics API may omit the `transaction` field from revenue entry records returned via the sub-resource endpoint. The production notebook explicitly back-fills `entry["transaction"] = transaction_id` if the field is absent.

---

## Field Type Mapping

### customers

Derived from `normalize_customers_df` in the production notebook.

| Field | API Type | Connector Type | Notes |
|---|---|---|---|
| `id` | integer | `long` | Primary key |
| `name` | string | `string` | |
| `number` | string | `string` | Customer number (not integer) |
| `code` | string | `string` | |
| `domain` | string | `string` | |
| `notes` | string | `string` | |
| `is_active` | boolean | `boolean` | |
| `qb_id` | string | `string` | QuickBooks ID |
| `do_not_sync` | boolean | `boolean` | |
| `unbalanced_revenue_exception` | boolean | `boolean` | |
| `modified` | ISO 8601 datetime string | `timestamp` | Top-level modified; use as CDC cursor |
| `chargify_id` | string | `string` | |
| `sf_id` | string | `string` | Salesforce ID |
| `sf_owner_id` | string | `string` | |
| `einvoicing_id` | string | `string` | |
| `email` | string | `string` | |
| `cc_email` | string | `string` | |
| `escalation_email` | string | `string` | |
| `default_email_from_so` | boolean | `boolean` | |
| `default_enable_cc_payment` | boolean | `boolean` | |
| `default_enable_ach_payment` | boolean | `boolean` | |
| `autopay_enrollment` | string | `string` | |
| `autopay_status` | string | `string` | |
| `parent` | integer | `long` | FK to parent customer |
| `default_theme` | string | `string` | |
| `is_paying_customer` | boolean | `boolean` | |
| `text_field1` | string | `string` | Custom text field |
| `text_field2` | string | `string` | |
| `text_field3` | string | `string` | |
| `number_field1` | numeric string | `string` | Custom numeric field; returned as string |
| `number_field2` | numeric string | `string` | |
| `number_field3` | numeric string | `string` | |
| `industry` | string | `string` | |
| `segment` | string | `string` | |
| `market` | string | `string` | |
| `c_account_type` | string | `string` | Custom field |
| `c_po_reqd_for_invoicing` | string | `string` | |
| `c_salesforce_id` | string | `string` | |
| `c_subvertical` | string | `string` | |
| `c_vertical` | string | `string` | |
| `auditentry` | struct | `string` (JSON) | Retained as struct; serialized to JSON string for flat Parquet output |
| `billing_name` | string | `string` | Flattened from `billing_profile.name` |
| `billing_company_name` | string | `string` | |
| `billing_salutation` | string | `string` | |
| `billing_first_name` | string | `string` | |
| `billing_last_name` | string | `string` | |
| `billing_addr1` | string | `string` | |
| `billing_addr2` | string | `string` | |
| `billing_addr3` | string | `string` | |
| `billing_city` | string | `string` | |
| `billing_state` | string | `string` | |
| `billing_zip_code` | string | `string` | |
| `billing_country` | string | `string` | |
| `shipping_addr1` | string | `string` | Flattened from `billing_profile.shipping_addr1` (no `billing_` prefix) |
| `shipping_addr2` | string | `string` | |
| `shipping_addr3` | string | `string` | |
| `shipping_city` | string | `string` | |
| `shipping_state` | string | `string` | |
| `shipping_zip_code` | string | `string` | |
| `shipping_country` | string | `string` | |
| `billing_phone` | string | `string` | |
| `billing_alt_phone` | string | `string` | |
| `billing_fax` | string | `string` | |
| `billing_email` | string | `string` | |
| `billing_contact` | string | `string` | |
| `billing_alt_contact` | string | `string` | |
| `billing_sales_rep_fullname` | string | `string` | |
| `billing_resale_number` | string | `string` | |
| `billing_account_number` | string | `string` | |
| `billing_is_active` | boolean | `boolean` | |
| `billing_invoice_print_preference` | boolean | `boolean` | |
| `billing_invoice_email_preference` | boolean | `boolean` | |
| `billing_edit_sequence` | string | `string` | |
| `billing_last_updated_by_qb` | boolean | `boolean` | |
| `billing_avatax_address_validation_timestamp` | date string | `date` | |
| `billing_payment_terms` | integer | `long` | FK to payment_terms |
| `billing_payment_method` | string | `string` | |
| `billing_currency` | string | `string` | Currency code |
| `billing_sales_tax_code` | string | `string` | |
| `billing_item_sales_tax` | string | `string` | |
| `billing_sales_rep` | string | `string` | |
| `billing_customer_type` | string | `string` | |
| `billing_default_invoice_template` | string | `string` | |
| `billing_default_credit_memo_template` | string | `string` | |
| `billing_default_class` | string | `string` | |
| `billing_entity_use_code` | string | `string` | AvaTax entity use code |
| `billing_tax_exemption_reason` | string | `string` | |

### items

Derived from `normalize_items_df`.

| Field | API Type | Connector Type | Notes |
|---|---|---|---|
| `id` | integer | `long` | Primary key |
| `product_family` | string | `string` | |
| `name` | string | `string` | |
| `code` | string | `string` | |
| `source_system` | string | `string` | |
| `description` | string | `string` | |
| `recurring` | boolean | `boolean` | |
| `billing_description` | string | `string` | |
| `default_duration` | string | `string` | |
| `normalized_term` | string | `string` | |
| `is_active` | boolean | `boolean` | |
| `enable_create_transactions` | boolean | `boolean` | |
| `create_revenue` | boolean | `boolean` | |
| `use_chargify_start_and_end_dates` | boolean | `boolean` | |
| `needs_so_profile` | boolean | `boolean` | |
| `qb_id` | string | `string` | QuickBooks ID |
| `sf_do_not_sync` | boolean | `boolean` | |
| `wizard_enabled` | boolean | `boolean` | |
| `gl_name` | string | `string` | GL account name |
| `gl_description` | string | `string` | |
| `sync_invoices` | boolean | `boolean` | |
| `is_sales_tax` | boolean | `boolean` | |
| `is_discount` | boolean | `boolean` | |
| `is_taxable` | boolean | `boolean` | |
| `income_account` | integer | `long` | FK to GL account |
| `qb_account` | integer | `long` | QuickBooks account ID |
| `modified` | ISO 8601 datetime string | `timestamp` | CDC cursor field |
| `modified_by_name` | string | `string` | |

**Additional fields present in Singer schema but not in notebook normalization** (may exist in API response): `avatax_id`, `intacct_id`, `netsuite_id`, `recurly_id`, `stripe_id`, `intacct_recordno`, `asset_account`, `liability_account`, `gl_created`, `gl_modified`, `gl_is_active`, `qb_only`, `revenue_recognition_method`, `billing_method`, `avatax_sales_tax_code`, `qb_sales_tax_code`, `transaction_start_date`, `transaction_end_date`, `intacct_modified`, `netsuite_modified`. These are not included in the connector's normalized output but are present in the raw API response.

### invoices

Derived from `normalize_invoices_df`. Note: `line_items` is **excluded** from this table's output and written separately as `invoices_line_items`.

| Field | API Type | Connector Type | Notes |
|---|---|---|---|
| `id` | integer | `long` | Primary key |
| `number` | string | `string` | Invoice number |
| `date` | date string `yyyy-MM-dd` | `date` | Invoice date |
| `due_date` | date string `yyyy-MM-dd` | `date` | |
| `ship_date` | date string `yyyy-MM-dd` | `date` | |
| `po_number` | string | `string` | |
| `memo` | string | `string` | |
| `other` | string | `string` | |
| `to_be_printed` | boolean | `boolean` | |
| `to_be_emailed` | boolean | `boolean` | |
| `billing_addr1` | string | `string` | Top-level billing address (not from billing_profile) |
| `billing_addr2` | string | `string` | |
| `billing_addr3` | string | `string` | |
| `billing_city` | string | `string` | |
| `billing_state` | string | `string` | |
| `billing_zip_code` | string | `string` | |
| `billing_country` | string | `string` | |
| `shipping_addr1` | string | `string` | |
| `shipping_addr2` | string | `string` | |
| `shipping_addr3` | string | `string` | |
| `shipping_city` | string | `string` | |
| `shipping_state` | string | `string` | |
| `shipping_zip_code` | string | `string` | |
| `shipping_country` | string | `string` | |
| `exported_date` | ISO 8601 datetime | `timestamp` | |
| `do_not_sync` | boolean | `boolean` | |
| `ignore_date_when_syncing` | boolean | `boolean` | |
| `type` | string | `string` | Invoice type enum |
| `chargify_id` | string | `string` | |
| `qb_number` | string | `string` | QuickBooks invoice number |
| `qb_txn_id` | string | `string` | QuickBooks transaction ID |
| `deleted_in_qb` | boolean | `boolean` | |
| `sync_date` | ISO 8601 datetime | `timestamp` | |
| `foreign_exchange_rate` | numeric string | `double` | FX rate; see currency duality note below |
| `subtotal` | numeric string | `double` | Cast required — returned as string |
| `is_paid` | boolean | `boolean` | |
| `sales_tax` | numeric string | `double` | |
| `sales_tax_percentage` | numeric string | `double` | |
| `applied_amount` | numeric string | `double` | |
| `balance` | numeric string | `double` | |
| `email_from_so` | boolean | `boolean` | |
| `enable_cc_payment` | string | `string` | |
| `enable_ach_payment` | string | `string` | |
| `sf_id` | string | `string` | |
| `stripe_id` | string | `string` | |
| `contract` | integer | `long` | FK to contracts |
| `qb_payment_terms` | integer | `long` | |
| `qb_class` | integer | `long` | |
| `qb_template` | integer | `long` | |
| `qb_customer_message` | string | `string` | |
| `qb_sales_rep` | integer | `long` | |
| `qb_currency` | string | `string` | |
| `qb_ar_account` | integer | `long` | |
| `refund_of` | string | `string` | Reference to original invoice if refund |
| `external_id` | string | `string` | |
| `committed` | boolean | `boolean` | |
| `committed_timestamp` | ISO 8601 datetime | `timestamp` | |
| `audit_created` | ISO 8601 datetime string | `string` | Flattened from `auditentry.created` |
| `audit_created_by` | string | `string` | |
| `audit_created_by_name` | string | `string` | |
| `audit_modified` | ISO 8601 datetime string | `string` | CDC cursor (`auditentry__modified__gte` filter) |
| `audit_modified_by` | string | `string` | |
| `audit_modified_by_name` | string | `string` | |
| `local_amount` | numeric string | `double` | Amount in local (customer) currency |
| `home_amount` | numeric string | `double` | Amount in home (reporting) currency |
| `tax_lines` | array/struct | `string` (JSON) | Sanitized to JSON string |
| `last_emailed` | ISO 8601 datetime | `timestamp` | |
| `last_clicked` | ISO 8601 datetime | `timestamp` | |
| `last_opened` | ISO 8601 datetime | `timestamp` | |
| `einvoicing_url` | string | `string` | |
| `einvoicing_url_no_click_tracking` | string | `string` | |

### invoices_line_items

Derived from `write_invoice_lines_from_df`. Each row is one exploded `line_items` element from a parent invoice.

| Field | API Type | Connector Type | Notes |
|---|---|---|---|
| `invoice_id` | integer | `long` | FK to `invoices.id` — join key |
| `line_id` | integer | `long` | `line_item.id`; part of composite PK |
| `line_number` | string/integer | `string` | `line_item.number` |
| `home_amount` | numeric string | `double` | Cast from string |
| `local_amount` | numeric string | `double` | Cast from string |
| `quantity` | numeric string | `double` | |
| `notes` | string | `string` | |
| `no_transaction_permitted` | boolean | `boolean` | |
| `exported_date` | ISO 8601 datetime | `timestamp` | |
| `qb_txn_line_id` | string | `string` | QuickBooks line item ID |
| `sync_date` | ISO 8601 datetime | `timestamp` | |
| `deleted_in_qb` | boolean | `boolean` | |
| `qb_time_modified` | ISO 8601 datetime | `timestamp` | |
| `sf_id` | string | `string` | Salesforce line item ID |
| `item` | integer | `long` | FK to `items.id` |
| `transaction` | integer | `long` | FK to `transactions.id` |
| `qb_class` | integer | `long` | |
| `external_id` | string | `string` | |
| `modified` | ISO 8601 datetime | `timestamp` | Line item modified timestamp |
| `modified_by_name` | string | `string` | |

**Composite primary key recommendation**: `(invoice_id, line_id)`.

### contracts

Derived from `normalize_contracts_df`.

| Field | API Type | Connector Type | Notes |
|---|---|---|---|
| `id` | integer | `long` | Primary key |
| `number` | string | `string` | |
| `maxio_id` | string | `string` | Maxio/Advanced Billing subscription ID |
| `entry_date` | date string | `date` | |
| `is_active` | boolean | `boolean` | |
| `notes` | string | `string` | |
| `qb_job_id` | string | `string` | |
| `is_job` | boolean | `boolean` | |
| `unbalanced_revenue_exception` | boolean | `boolean` | |
| `email` | string | `string` | |
| `default_cadence_template` | string | `string` | |
| `sf_id` | string | `string` | |
| `register` | integer | `long` | FK to registers |
| `register_maxio_id` | string | `string` | |
| `customer` | integer | `long` | FK to customers |
| `created` | string | `string` | Creation timestamp (raw; not cast in notebook) |
| `created_by` | string | `string` | |
| `created_by_name` | string | `string` | |
| `modified` | ISO 8601 datetime string | `timestamp` | CDC cursor field |
| `modified_by` | string | `string` | |
| `modified_by_name` | string | `string` | |
| `text_field1` | string | `string` | |
| `text_field2` | string | `string` | |
| `number_field1` | numeric string | `string` | |
| `number_field2` | numeric string | `string` | |
| `channel` | string | `string` | |
| `lead_source` | string | `string` | |
| `lead_date` | date string | `date` | |
| `billing_parent_id` | string | `string` | Flattened from `billing_profile.parent_id` |
| `billing_name` | string | `string` | |
| `billing_company_name` | string | `string` | |
| `billing_salutation` | string | `string` | |
| `billing_first_name` | string | `string` | |
| `billing_last_name` | string | `string` | |
| `billing_addr1` | string | `string` | |
| `billing_addr2` | string | `string` | |
| `billing_addr3` | string | `string` | |
| `billing_city` | string | `string` | |
| `billing_state` | string | `string` | |
| `billing_zip_code` | string | `string` | |
| `billing_country` | string | `string` | |
| `billing_shipping_addr1` | string | `string` | Note: `billing_` prefix on shipping for contracts (differs from customers table) |
| `billing_shipping_addr2` | string | `string` | |
| `billing_shipping_addr3` | string | `string` | |
| `billing_shipping_city` | string | `string` | |
| `billing_shipping_state` | string | `string` | |
| `billing_shipping_zip_code` | string | `string` | |
| `billing_shipping_country` | string | `string` | |
| `billing_phone` | string | `string` | |
| `billing_alt_phone` | string | `string` | |
| `billing_fax` | string | `string` | |
| `billing_email` | string | `string` | |
| `billing_contact` | string | `string` | |
| `billing_alt_contact` | string | `string` | |
| `billing_customer_type_ref_fullname` | string | `string` | |
| `billing_resale_number` | string | `string` | |
| `billing_account_number` | string | `string` | |
| `billing_is_active` | boolean | `boolean` | |
| `billing_job_status` | string | `string` | |
| `billing_job_start_date` | date string | `date` | |
| `billing_job_projected_end_date` | date string | `date` | |
| `billing_job_end_date` | date string | `date` | |
| `billing_job_description` | string | `string` | |
| `billing_invoice_print_preference` | boolean | `boolean` | |
| `billing_invoice_email_preference` | boolean | `boolean` | |
| `billing_edit_sequence` | string | `string` | |
| `billing_payment_terms` | integer | `long` | |
| `billing_sales_rep` | integer | `long` | |
| `billing_job_type` | string | `string` | |
| `billing_default_invoice_template` | string | `string` | |
| `billing_default_credit_memo_template` | string | `string` | |
| `billing_entity_use_code` | string | `string` | |
| `billing_qbo_bill_parent_customer` | boolean | `boolean` | QuickBooks Online flag |
| `billing_tax_exemption_reason` | string | `string` | |

### transactions

Derived from `normalize_transactions_df`. Many numeric fields come back as **strings** from the API; explicit `.cast("double")` is required.

| Field | API Type | Connector Type | Notes |
|---|---|---|---|
| `id` | integer | `long` | Primary key |
| `number` | string | `string` | |
| `start_date` | date string `yyyy-MM-dd` | `date` | |
| `end_date` | date string `yyyy-MM-dd` | `date` | |
| `duration` | string | `string` | |
| `foreign_exchange_rate` | numeric string | `double` | Cast required |
| `home_amount` | numeric string | `double` | Amount in home (reporting) currency |
| `local_amount` | numeric string | `double` | Amount in local (customer) currency |
| `home_normalized_amount` | numeric string | `double` | Normalized for ARR/MRR calc |
| `local_normalized_amount` | numeric string | `double` | |
| `home_arr_amount` | numeric string | `double` | |
| `local_arr_amount` | numeric string | `double` | |
| `always_use_amount_as_normalized_amount` | boolean | `boolean` | |
| `quantity` | numeric string | `double` | |
| `term_number` | integer | `long` | |
| `unbalanced_revenue_exception` | boolean | `boolean` | |
| `different_invoice_item_permitted` | boolean | `boolean` | |
| `renewal_probability` | string | `string` | Enum/percentage; not cast to numeric |
| `renewal_amount_value` | numeric string | `double` | |
| `renewal_amount_percentage` | numeric string | `double` | |
| `renewal_amount` | numeric string | `double` | |
| `order_number` | string | `string` | |
| `order_date` | date string `yyyy-MM-dd` | `date` | |
| `recognize` | boolean | `boolean` | Whether to recognize revenue |
| `flagged` | string | `string` | |
| `notes` | string | `string` | |
| `invoice_description` | string | `string` | |
| `do_not_sync_invoices` | boolean | `boolean` | |
| `home_rate` | numeric string | `double` | Per-unit rate in home currency |
| `local_rate` | numeric string | `double` | Per-unit rate in local currency |
| `home_normalized_rate` | numeric string | `double` | |
| `local_normalized_rate` | numeric string | `double` | |
| `renewal_quantity` | numeric string | `double` | |
| `renewal_duration` | string | `string` | |
| `is_autorenewal` | boolean | `boolean` | |
| `reconcile_required` | boolean | `boolean` | |
| `revenue_notes` | string | `string` | |
| `revenue_amount` | numeric string | `double` | Total recognized revenue amount |
| `modified` | ISO 8601 datetime string | `timestamp` | Top-level; CDC cursor in Singer (but Singer uses `auditentry__modified__gte` as filter) |
| `sf_group_id` | string | `string` | |
| `sf_id` | string | `string` | |
| `sf_opportunity_id` | string | `string` | |
| `sf_opportunity_line_item_id` | string | `string` | |
| `sf_renewal_opportunity_id` | string | `string` | |
| `sf_renewal_opportunity_line_item_id` | string | `string` | |
| `cancelled` | boolean | `boolean` | |
| `contract` | integer | `long` | FK to contracts |
| `item` | integer | `long` | FK to items |
| `billing_method` | integer | `long` | FK to billing_methods |
| `ili_qb_class` | integer | `long` | |
| `renew_using_item` | integer | `long` | FK to items for renewal |
| `renewal_factor` | numeric string | `double` | |
| `autorenewal_profile` | integer | `long` | FK to auto_renewal_profiles |
| `sf_renewal_opportunity_rule` | integer | `long` | |
| `item_class` | integer | `long` | |
| `project` | string | `string` | |
| `renewal_of_set` | string | `string` | |
| `is_renewed` | string | `string` | |
| `renewal_billing_method` | integer | `long` | |
| `crm_opportunity_id` | string | `string` | |
| `crm_opportunity_line_item_id` | string | `string` | |
| `hubspot_renewal_deal_rule` | integer | `long` | |
| `advanced_billing_subscription_id` | string | `string` | |
| `advanced_billing_subscription_line_id` | string | `string` | |
| `text_field1` | string | `string` | |
| `text_field2` | string | `string` | |
| `number_field1` | string | `string` | |
| `number_field2` | string | `string` | |
| `sales_rep` | string | `string` | |
| `sales_manager` | string | `string` | |
| `conversion` | string | `string` | |
| `audit_created` | string | `string` | Flattened from `auditentry.created` |
| `audit_created_by` | string | `string` | |
| `audit_modified` | string | `string` | Flattened from `auditentry.modified`; used as CDC cursor via `auditentry__modified__gte` |
| `audit_modified_by` | string | `string` | |

### revenue_entries

Derived from `normalize_revenue_entries_df`.

| Field | API Type | Connector Type | Notes |
|---|---|---|---|
| `id` | integer | `long` | Primary key |
| `transaction` | integer | `long` | FK to transactions; back-filled if absent from API response |
| `start_date` | date string `yyyy-MM-dd` | `date` | Revenue period start |
| `end_date` | date string `yyyy-MM-dd` | `date` | Revenue period end |
| `home_amount` | numeric string | `double` | Revenue amount in home currency; cast required |
| `local_amount` | numeric string | `double` | Revenue amount in local currency; cast required |
| `modified` | ISO 8601 datetime string | `timestamp` | CDC cursor |

---

## Known Quirks

1. **Numeric fields returned as strings**: Many amount, rate, and quantity fields on `invoices`, `transactions`, and `revenue_entries` come from the API as JSON strings (e.g., `"123.45"`) rather than JSON numbers. Explicit type casts are required before writing to typed storage. The notebook uses `.cast("double")` universally for numeric fields.

2. **Currency duality (`home_*` vs `local_*`)**: Most financial tables carry both a `home_amount` and `local_amount` (and sometimes `home_rate`/`local_rate`). `home_*` reflects the tenant's base reporting currency; `local_*` reflects the customer's billing currency. `foreign_exchange_rate` bridges them. Always preserve both — downstream analytics choose which to use.

3. **`foreign_exchange_rate` semantics**: `home_amount = local_amount * foreign_exchange_rate` (rate is local→home). A rate of `1.0` means home and local currencies are the same.

4. **Nested structs in Parquet**: Downstream consumers (e.g., Azure Data Factory Data Flows) may fail to read Parquet files containing `StructType` or `ArrayType` columns. The notebook serializes all remaining complex columns to JSON strings via `to_json()` after normalization. Apply the same pattern in the connector.

5. **`line_items` excluded from `invoices` table**: The `line_items` array is intentionally omitted from the normalized `invoices` output and written as a separate `invoices_line_items` table. Do not include `line_items` in the `invoices` schema output.

6. **`auditentry` on `customers` kept as struct**: Unlike `invoices` and `transactions`, the `auditentry` on `customers` is not explicitly flattened in the notebook's normalization function — it is retained as a struct (which is then serialized to JSON string by the sanitizer). Consider flattening it consistently for query ergonomics.

7. **`revenue_entries` sub-resource vs top-level endpoint**: The notebook fetches revenue entries by iterating over every transaction ID and calling `GET /transactions/{id}/revenue_entries/`. The Singer tap references a top-level `/revenue_entries/` endpoint with `modified__gte` filter support. The top-level endpoint would be dramatically more efficient for incremental sync. Verify availability during live testing.

8. **`transaction` FK back-fill**: When fetching revenue entries via the sub-resource path, the `transaction` field may be absent from the API response. Always back-fill it from the parent transaction ID.

9. **`invoices_line_items` has no independent cursor**: Since it's derived from invoice payloads, line items can only be refreshed by re-fetching invoices. If incremental invoice sync is used, only new/modified invoices will have their line items updated. To handle deletes or retroactive line item changes, a periodic full snapshot of invoices (and their line items) is recommended.

10. **`page_size` max undocumented**: No official upper bound documented. The notebook uses 100. Higher values may improve throughput but could trigger timeouts. TBD: test 200–500 during live testing.

11. **Relative `next` URL edge case**: While the API typically returns absolute URLs in `next`, the notebook defensively handles relative URLs using `urljoin`. Include this handling in the connector.

12. **`deleted_*` endpoints not implemented**: SaaSOptics exposes `deleted_contracts`, `deleted_invoices`, `deleted_transactions`, `deleted_revenue_entries` endpoints with `deleted` timestamps for hard-delete tracking. Not in scope for the current connector but required for true `cdc_with_deletes` support.

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| User-provided notebook (primary) | `/sources/maxio/resources/PTB_Maxio.ipynb` (local) | 2026-05-22 | Highest | Auth header format, pagination, rate limiting, all field schemas, nested struct handling, revenue_entries sub-resource pattern, numeric-as-string quirk |
| Singer tap (OSS reference) | [github.com/singer-io/tap-saasoptics](https://github.com/singer-io/tap-saasoptics) | 2026-05-22 | High | All stream names, primary keys, `modified__gte`/`modified__lte` and `auditentry__modified__gte`/`auditentry__modified__lte` incremental filter params, top-level `/revenue_entries/` endpoint existence, base URL pattern |
| Singer tap — streams.py | [github.com/singer-io/tap-saasoptics/blob/master/tap_saasoptics/streams.py](https://github.com/singer-io/tap-saasoptics/blob/master/tap_saasoptics/streams.py) | 2026-05-22 | High | Per-stream bookmark query field names (`modified__gte`, `auditentry__modified__gte`) |
| Singer tap — schemas | [github.com/singer-io/tap-saasoptics/tree/master/tap_saasoptics/schemas](https://github.com/singer-io/tap-saasoptics/tree/master/tap_saasoptics/schemas) | 2026-05-22 | High | Field names and types for customers, items, invoices, contracts, transactions, revenue_entries |
| Stitch documentation | [stitchdata.com/docs/integrations/saas/saasoptics](https://www.stitchdata.com/docs/integrations/saas/saasoptics) | 2026-05-22 | High | Confirmed 25-table set, replication methods, replication keys per table |
| SaaSync help article | [support.saasync.com/article/176-getting-started-and-authenticating-with-maxio-core-formerly-saasoptics](https://support.saasync.com/article/176-getting-started-and-authenticating-with-maxio-core-formerly-saasoptics) | 2026-05-22 | Medium | Token auth, Admin > API Tokens path, instance URL format |
| Fivetran connector page | [fivetran.com/docs/connectors/applications/maxio-saasoptics](https://fivetran.com/docs/connectors/applications/maxio-saasoptics) | 2026-05-22 | Medium | Confirmed subdomain + account_name config params; no additional schema details accessible |

**Source priority note**: Where the production notebook and Singer tap disagree (e.g., revenue_entries endpoint path: sub-resource vs top-level), the notebook is treated as ground truth because it runs in production against a live tenant. The Singer tap's top-level endpoint should be verified during live testing.

Sources:
- [SaaSOptics API - API Tracker](https://apitracker.io/a/saasoptics)
- [SaaSOptics (v1) - Stitch Documentation](https://www.stitchdata.com/docs/integrations/saas/saasoptics)
- [Singer tap-saasoptics - GitHub](https://github.com/singer-io/tap-saasoptics)
- [tap-saasoptics streams.py](https://github.com/singer-io/tap-saasoptics/blob/master/tap_saasoptics/streams.py)
- [Getting Started with Maxio Core - SaaSync](https://support.saasync.com/article/176-getting-started-and-authenticating-with-maxio-core-formerly-saasoptics)
- [Maxio SaaSOptics connector - Fivetran](https://fivetran.com/docs/connectors/applications/maxio-saasoptics)
