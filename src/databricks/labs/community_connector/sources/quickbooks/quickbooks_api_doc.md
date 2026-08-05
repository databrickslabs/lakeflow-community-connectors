# QuickBooks Online Accounting API research notes

## Authentication

- OAuth 2.0 authorization-code flow.
- Accounting scope: `com.intuit.quickbooks.accounting`.
- Authorization endpoint:
  `https://appcenter.intuit.com/connect/oauth2`.
- Token endpoint:
  `https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer`.
- API calls require both a bearer access token and a QuickBooks company
  `realmId`.

The connector delegates token acquisition and refresh to the Unity Catalog
COMMUNITY connection. The connector treats the runtime `access_token` as
opaque.

## Query API

Entities are read with the QuickBooks SQL-like query endpoint:

```text
GET /v3/company/{realmId}/query
```

Queries use positional pagination:

```sql
SELECT * FROM Customer STARTPOSITION 1 MAXRESULTS 1000
```

The maximum response size is 1,000 records.

Customer incremental queries use inclusive time bounds:

```sql
SELECT * FROM Customer
WHERE Active IN (true, false)
  AND MetaData.LastUpdatedTime >= '2026-07-25T09:59:00Z'
  AND MetaData.LastUpdatedTime <= '2026-07-26T10:00:00Z'
STARTPOSITION 1 MAXRESULTS 1000
```

QuickBooks supports `AND` but not `OR`. Its `Id` filter supports equality and
`IN`, not ordering comparisons. The M3 cursor therefore uses a timestamp
watermark with an overlap rather than an unsupported `(timestamp, Id)` range
tie-breaker.

## Initial object set

| Lakeflow table | QuickBooks entity | Primary key | Initial mode |
|---|---|---|---|
| customers | Customer | realm_id + Id | cdc (inserts, updates, inactive rows) |
| vendors | Vendor | realm_id + Id | cdc (inserts, updates, inactive rows) |
| accounts | Account | realm_id + Id | cdc (inserts, updates, inactive rows) |
| items | Item | realm_id + Id | cdc (inserts, updates, inactive rows) |
| invoices | Invoice | realm_id + Id | cdc_with_deletes |
| bills | Bill | realm_id + Id | cdc_with_deletes |

## Destination columns

Every table has these typed core columns:

| Column | Spark type | Nullable | Meaning |
|---|---|---:|---|
| `realm_id` | string | no | QuickBooks company ID and tenant component of the primary key |
| `id` | string | no | QuickBooks entity `Id` and entity component of the primary key |
| `sync_token` | string | yes | QuickBooks optimistic-concurrency token |
| `created_at` | timestamp | yes | `MetaData.CreateTime` |
| `last_updated_at` | timestamp | yes | `MetaData.LastUpdatedTime` and CDC sequence cursor |
| `raw_json` | string | no | Lossless, canonical JSON representation of the source object |

Entity-specific columns are:

| Table | Additional typed columns |
|---|---|
| `customers` | `display_name` string, `company_name` string, `given_name` string, `family_name` string, `primary_email` string, `primary_phone` string, `balance` decimal(38,9), `currency_ref` string, `active` boolean |
| `vendors` | `display_name` string, `company_name` string, `given_name` string, `family_name` string, `primary_email` string, `primary_phone` string, `balance` decimal(38,9), `vendor_1099` boolean, `currency_ref` string, `active` boolean |
| `accounts` | `name` string, `fully_qualified_name` string, `account_type` string, `account_sub_type` string, `classification` string, `current_balance` decimal(38,9), `currency_ref` string, `active` boolean |
| `items` | `name` string, `fully_qualified_name` string, `item_type` string, `description` string, `unit_price` decimal(38,9), `purchase_cost` decimal(38,9), `quantity_on_hand` decimal(38,9), `income_account_ref` string, `expense_account_ref` string, `asset_account_ref` string, `active` boolean |
| `invoices` | `doc_number` string, `txn_date` date, `due_date` date, `customer_ref` string, `total_amount` decimal(38,9), `balance` decimal(38,9), `currency_ref` string, `email_status` string, `print_status` string, `line_json` string |
| `bills` | `doc_number` string, `txn_date` date, `due_date` date, `vendor_ref` string, `total_amount` decimal(38,9), `balance` decimal(38,9), `currency_ref` string, `ap_account_ref` string, `line_json` string |

Except for `realm_id`, `id`, and `raw_json`, fields are nullable because
QuickBooks returns sparse objects and entity shape varies by company
configuration.

## Incremental status

Inserts and updates for all six tables are implemented with an independent
versioned `updated_through` offset, a frozen per-run upper bound, bounded
update windows, replay overlap, and best-effort `max_records_per_batch`
admission control. Oversized windows are retried with smaller time bounds and
the checkpoint advances only after a complete window is drained. Each table's
initial batch is a complete snapshot.

QuickBooks list entities use soft deletion: `Active=false` is an update, and
the row remains in the destination. Query API calls explicitly include active
and inactive rows because QuickBooks otherwise defaults to active-only list
results.

QuickBooks transaction entities use hard deletion. Invoice and Bill delete
flows call:

```text
GET /v3/company/{realmId}/cdc?entities={entity}&changedSince={timestamp}
```

They filter the response to `status=Deleted` and emit tombstones containing
the source `Id` and `MetaData.LastUpdatedTime`. CDC only covers the previous
30 days and caps a response at 1,000 objects. Because the endpoint has no
`changedUntil`, saturated responses cannot be safely subdivided; the
connector fails without advancing its checkpoint and requires reconciliation.

## Source references

- https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0
- https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/data-queries
- https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/change-data-capture
