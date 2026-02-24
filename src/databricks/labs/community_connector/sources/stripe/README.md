# Lakeflow Stripe Connector

The Lakeflow Stripe Connector allows you to extract data from your Stripe account and load it into your data lake or warehouse. This connector supports incremental synchronization using Change Data Capture (CDC) patterns.

## Set up

### Prerequisites
- Access to a Stripe account with API permissions
- Stripe Secret API Key (test or live mode)

### Required Parameters

To configure the Stripe connector, you'll need to provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_key` | string | Yes | Stripe Secret API Key | `sk_test_51abc...` or `sk_live_51xyz...` |

### Table-Level Options

The following options can be set per table via `table_options`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_records_per_batch` | integer | `100000` | Maximum number of records per microbatch. Controls memory usage when streaming large Stripe objects (e.g., events, charges). |

To use table-level options, include them in the `externalOptionsAllowList` connection parameter:

```
max_records_per_batch
```

### Getting Your Stripe API Key

1. Log in to your Stripe Dashboard
2. Navigate to **Developers** � **API keys**
3. Copy your **Secret key** (starts with `sk_test_` for test mode or `sk_live_` for live mode)
4. **Important**: Keep your secret key secure - never share it publicly

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.

The connection can also be created using the standard Unity Catalog API.

## Objects Supported

The Stripe connector supports the following tables/objects with incremental synchronization:

| Table Name | Primary Key | Cursor Field | Supports Deletion |
|------------|-------------|--------------|-------------------|
| `customers` | `id` | `created` | Yes |
| `charges` | `id` | `created` | No |
| `payment_intents` | `id` | `created` | No |
| `payment_methods` | `id` | `created` | No |
| `refunds` | `id` | `created` | No |
| `disputes` | `id` | `created` | No |
| `subscriptions` | `id` | `created` | No |
| `invoices` | `id` | `created` | No |
| `invoice_items` | `id` | `created` | No |
| `products` | `id` | `created` | Yes |
| `prices` | `id` | `created` | No |
| `plans` | `id` | `created` | Yes |
| `coupons` | `id` | `created` | No |
| `balance_transactions` | `id` | `created` | No |
| `payouts` | `id` | `created` | No |
| `events` | `id` | `created` | No |

> **Note**: Table names are case-sensitive. Use the exact names shown above (lowercase with underscores).

### Object Details

### Core Customer & Account Objects

#### `customers`
- **Primary Key**: `id`
- **Ingestion Type**: CDC (Change Data Capture)
- **Cursor Field**: `created`
- **Supports Deletion Tracking**: Yes
- **Schema**: Complete customer information including:
  - Core fields: `id`, `object`, `created`, `livemode`
  - Contact information: `email`, `name`, `phone`, `description`
  - Address: All address fields flattened with `address_*` prefix
  - Shipping: All shipping fields flattened with `shipping_*` prefix
  - Financial: `balance`, `currency`, `delinquent`
  - Metadata: Custom key-value pairs (stored as JSON string)
  - Settings: `invoice_settings`, `tax_exempt`, `preferred_locales`
  - Deletion tracking: `deleted` flag for soft-deleted customers

### Payment Objects

#### `charges`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Payment transaction details including amounts, status, customer reference, payment method details, billing details, and outcome information

#### `payment_intents`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Modern payment flow tracking including amount fields, status, customer reference, payment method, charges, cancellation details, and metadata

#### `payment_methods`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Stored payment methods including type (card, bank account), customer reference, billing details, and card/bank account details (stored as JSON)

#### `refunds`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Refund transactions including amount, currency, charge reference, status, reason, receipt number, and failure information

#### `disputes`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Payment disputes including amount, currency, charge reference, status, reason, evidence, and refundability status

### Billing & Subscription Objects

#### `subscriptions`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Recurring billing subscriptions including status, period dates, customer reference, payment method, billing details, trial information, items (JSON), and cancellation details

#### `invoices`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Billing invoices including status, amount fields, customer/subscription references, billing details, dates, invoice URLs, line items (JSON), and metadata

#### `invoice_items`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Individual line items on invoices including amount, currency, customer/invoice references, description, quantity, unit amounts, period dates, and discounts

### Product Catalog Objects

#### `products`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Supports Deletion Tracking**: Yes
- **Schema**: Product catalog items including name, description, active status, type, images, metadata, tax code, and shippable flag

#### `prices`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Pricing information for products including currency, unit amount, billing scheme, type (one-time/recurring), recurring details (JSON), tiers (JSON), and metadata

#### `plans` (Legacy)
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Supports Deletion Tracking**: Yes
- **Schema**: Legacy pricing plans including amount, currency, interval, product reference, usage type, trial period, tiers (JSON), and metadata

#### `coupons`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Discount coupons including amount_off/percent_off, duration, redemption limits, validity, and applicable products

### Financial Objects

#### `balance_transactions`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Financial transactions affecting your Stripe balance including amount, currency, net amount, fees, transaction type, source, status, and availability date

#### `payouts`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Transfers to your bank account including amount, currency, arrival date, status, type, method, destination, and failure information

### Event Objects

#### `events`
- **Primary Key**: `id`
- **Ingestion Type**: CDC
- **Cursor Field**: `created`
- **Schema**: Webhook events and audit log including event type, affected object data (JSON), API version, request information, and pending webhooks count

### Schema Structure

#### Flattened Fields
Complex nested fields (address, shipping) are flattened with prefixes for easier querying:
- Address: `address_line1`, `address_city`, `address_state`, etc.
- Shipping: `shipping_name`, `shipping_address_line1`, etc.

#### JSON String Fields
Very complex nested objects are stored as JSON strings:
- `invoice_settings`: Customer invoice configuration
- `metadata`: Custom application data
- `discount`: Applied discount information (if any)

#### Timestamp Fields
- **`created`**: Unix timestamp (integer) - used as cursor for incremental sync
- **`updated`**: Unix timestamp (integer) - may not be present for all customers

## Incremental Sync Strategy

### Change Data Capture (CDC)
The connector uses the `created` timestamp as a cursor field to fetch only new or updated customers since the last sync:

1. **Initial Sync**: Fetches all customers from Stripe
2. **Subsequent Syncs**: Fetches only customers created/updated after the last checkpoint
3. **Cursor Tracking**: Automatically tracks the latest `created` timestamp for efficient incremental updates

### Deletion Tracking
Stripe supports soft deletion for customers. The `deleted` field will be `true` for deleted customers, allowing you to track deletions in your data warehouse.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

## Implementation Details

### API Endpoints Used
The connector uses the Stripe List API pattern for all objects:
- **Endpoint Pattern**: `GET /v1/{object_type}`
- **Pagination**: `starting_after` parameter for cursor-based pagination
- **Incremental Sync**: `created[gte]` parameter to fetch only new/updated records
- **Page Size**: Up to 100 records per page (Stripe's maximum)

Examples:
- Customers: `GET /v1/customers`
- Charges: `GET /v1/charges`
- Subscriptions: `GET /v1/subscriptions`
- Invoices: `GET /v1/invoices`

### Rate Limiting
- Implements 0.1-second delay between API requests
- Stripe's standard rate limits apply (varies by account type)
- Implement exponential backoff for rate limit errors if needed

### Data Types

| Stripe Type | Spark Type | Notes |
|-------------|-----------|-------|
| String ID | StringType | Customer IDs like `cus_abc123` |
| String | StringType | Text fields |
| Integer | LongType | Unix timestamps, amounts in cents |
| Boolean | BooleanType | Flags like `livemode`, `delinquent` |
| Object | StringType | Stored as JSON string |
| Array | ArrayType(StringType) | Lists like `preferred_locales` |


## Best Practices

### Performance Optimization
- **Incremental Sync**: Always use incremental sync for large customer bases
- **Checkpoint Frequency**: Balance data freshness needs with API rate limits
- **Monitor API Usage**: Track API calls in Stripe Dashboard

### Data Management
- **Start Small**: Test with a small subset before full production sync
- **Schema Evolution**: The schema is fixed; new Stripe fields require connector updates
- **Metadata Parsing**: Parse JSON string fields (`metadata`, `invoice_settings`) as needed for analysis

## Troubleshooting

### Common Issues

**Authentication Errors**:
- Verify API key is correct and properly formatted
- Ensure you're using the correct key type (test vs live mode)
- Check that the secret scope and key name match your configuration

**Missing Data**:
- Verify the customer exists in Stripe Dashboard
- Check if using correct mode (test vs live)
- Confirm incremental cursor is advancing properly

**Rate Limiting**:
- Reduce sync frequency
- Implement exponential backoff in connector code
- Contact Stripe support to increase rate limits

**Schema Mismatches**:
- Ensure connector version matches your Stripe API version
- Complex fields are stored as JSON strings - parse them in downstream transformations
- Check for null values in required fields

### Error Handling
The connector includes built-in error handling for:
- API authentication errors (returns detailed error messages)
- Rate limiting (basic delay between requests)
- Network connectivity issues
- Invalid table names

Check pipeline logs for detailed error information and stack traces.

## Future Enhancements

Potential future improvements:
- **Write Support**: Add write operations for creating/updating Stripe objects
- **Additional Objects**: Support for more Stripe objects like:
  - Tax rates and tax transactions
  - Connect accounts and transfers
  - Radar rules and reviews
  - Terminal readers and locations
  - Issuing cards and transactions

## API Documentation

For complete Stripe API documentation, see:
- [Stripe API Reference](https://docs.stripe.com/api)
- [Authentication](https://docs.stripe.com/api/authentication)
- [Pagination](https://docs.stripe.com/api/pagination)

### Object-Specific Documentation:
- [Customers API](https://docs.stripe.com/api/customers)
- [Charges API](https://docs.stripe.com/api/charges)
- [Payment Intents API](https://docs.stripe.com/api/payment_intents)
- [Subscriptions API](https://docs.stripe.com/api/subscriptions)
- [Invoices API](https://docs.stripe.com/api/invoices)
- [Products API](https://docs.stripe.com/api/products)
- [Prices API](https://docs.stripe.com/api/prices)
- [Balance Transactions API](https://docs.stripe.com/api/balance_transactions)
- [Payouts API](https://docs.stripe.com/api/payouts)
- [Events API](https://docs.stripe.com/api/events)

## Support

For issues or questions:
- Check Stripe API documentation
- Review connector logs in Databricks
- Verify API key permissions in Stripe Dashboard