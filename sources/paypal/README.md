# Lakeflow PayPal Community Connector

This documentation provides setup instructions and reference information for the PayPal source connector.

## Prerequisites

To use this connector, you need:

- A PayPal Business account with API access
- API credentials (Client ID and Client Secret) from the PayPal Developer Dashboard
- Access to the PayPal REST API (Transaction Search API v1)
- For production use: A verified PayPal Business account with appropriate API permissions
- For testing: Access to PayPal Sandbox environment

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connection options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `client_id` | string | Yes | OAuth 2.0 Client ID from PayPal Developer Dashboard | `"AYourClientIdHere..."` |
| `client_secret` | string | Yes | OAuth 2.0 Client Secret from PayPal Developer Dashboard | `"EYourClientSecretHere..."` |
| `environment` | string | No | API environment: `"sandbox"` or `"production"`. Defaults to `"sandbox"` | `"sandbox"` |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of table-specific options that can be configured per table: `"start_date,end_date,page_size"` | `"start_date,end_date,page_size"` |

**Note**: The `externalOptionsAllowList` parameter is **required** and must include: `"start_date,end_date,page_size"`. These options allow you to configure date ranges and pagination for each table.

### Obtaining PayPal API Credentials

Follow these steps to obtain your PayPal API credentials:

1. **Log in to PayPal Developer Dashboard**
   - Go to [https://developer.paypal.com/dashboard/](https://developer.paypal.com/dashboard/)
   - Sign in with your PayPal Business account credentials

2. **Navigate to Apps & Credentials**
   - Select the **Apps & Credentials** section
   - Choose **Sandbox** tab for testing or **Live** tab for production

3. **Create or Select an App**
   - Click **Create App** to create a new app, or select an existing one
   - Give your app a descriptive name (e.g., "Databricks Lakeflow Connector")

4. **Copy Credentials**
   - **Client ID**: Displayed at the top of the app details page
   - **Secret**: Click **Show** under "Secret" to reveal, then copy it
   - **Important**: Keep your Client Secret secure and never share it publicly

5. **API Permissions**
   - Ensure your app has access to the **Transaction Search API v1**
   - No additional permissions or scopes are required for basic transaction data access

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways:

1. **Via Lakeflow Community Connector UI**:
   - Navigate to the "Add Data" page in your Databricks workspace
   - Follow the Lakeflow Community Connector UI flow
   - Select PayPal as your source connector
   - Provide your `client_id`, `client_secret`, and `environment`
   - Set `externalOptionsAllowList` to: `"start_date,end_date,page_size"`

2. **Via Unity Catalog API**:
   ```python
   # Example using Databricks SDK
   from databricks.sdk import WorkspaceClient
   
   w = WorkspaceClient()
   
   connection = w.connections.create(
       name="paypal_connection",
       connection_type="LAKEFLOW",
       options={
           "client_id": "YOUR_CLIENT_ID",
           "client_secret": "YOUR_CLIENT_SECRET",
           "environment": "sandbox",
           "externalOptionsAllowList": "start_date,end_date,page_size"
       }
   )
   ```

## Supported Objects

### `transactions` Table

The PayPal connector currently supports the **`transactions`** table, which provides transaction history data from your PayPal account.

**Primary Key**: `transaction_info.transaction_id`

**Incremental Ingestion**: 
- **Strategy**: Snapshot-based ingestion with date range filtering
- **Cursor Field**: `transaction_info.transaction_initiation_date`
- **Ingestion Type**: `snapshot`

**Required Table Options**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `start_date` | string | Yes | Start of date range in ISO 8601 format (UTC). Must be within the last 3 years. | `"2024-01-01T00:00:00Z"` |
| `end_date` | string | Yes | End of date range in ISO 8601 format (UTC). Maximum 31-day range from start_date. | `"2024-01-31T23:59:59Z"` |
| `page_size` | integer | No | Number of transactions per page (default: 100, max: 500) | `100` |

**Schema Highlights**:

The `transactions` table includes nested structures for comprehensive transaction data:

- **`transaction_info`**: Core transaction details including transaction ID, dates, amounts, status, and fees
- **`payer_info`**: Payer account information including email, name, and country
- **`shipping_info`**: Shipping recipient name and address (when applicable)
- **`cart_info`**: Array of item details for transactions involving product purchases

**Key Fields**:
- `transaction_info.transaction_id` (string, not null): Unique transaction identifier
- `transaction_info.transaction_initiation_date` (string): Transaction creation timestamp (ISO 8601)
- `transaction_info.transaction_status` (string): Transaction status (S=Success, P=Pending, D=Denied, V=Voided, F=Failed)
- `transaction_info.transaction_amount` (struct): Amount with currency_code and value
- `payer_info.email_address` (string): Payer's email address

**Delete Synchronization**: Not supported. PayPal transactions are immutable once created.

**Important Limitations**:
- **31-Day Maximum**: PayPal enforces a strict 31-day maximum date range per query. For larger ranges, split into multiple 31-day windows.
- **3-Year Historical Limit**: Transaction data is only available for the last 3 years from the current date.
- **Immutable Transactions**: Transactions don't change after creation. Refunds and reversals appear as new transactions.

## Data Type Mapping

| PayPal API Type | Example Fields | Databricks Type | Notes |
|-----------------|----------------|-----------------|-------|
| string | `transaction_id`, `email_address`, `transaction_status` | STRING | Text identifiers, codes, and general strings |
| string (amount) | `value` in amount objects | STRING | Decimal values as strings to preserve precision. Can be cast to DECIMAL in downstream processing. |
| string (ISO 8601) | `transaction_initiation_date`, `transaction_updated_date` | STRING | ISO 8601 UTC timestamps (e.g., "2024-01-15T10:30:00Z"). Can be cast to TIMESTAMP in processing. |
| object | `transaction_amount`, `payer_info`, `shipping_info` | STRUCT | Nested records with named fields |
| array | `item_details` | ARRAY<STRUCT> | Arrays of nested objects |

**Type Conversion Notes**:
- **Currency Amounts**: Stored as STRING to avoid floating-point precision loss. Convert to DECIMAL(19,4) or similar in your queries.
- **Timestamps**: Stored as STRING in ISO 8601 format with 'Z' timezone. Convert using `to_timestamp()` function.
- **Status Codes**: Single-letter codes (S, P, D, V, F) - consider creating a lookup table for human-readable descriptions.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the PayPal source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in your main pipeline file (e.g., `ingest.py`):

```python
pipeline_spec = {
    "connection_name": "paypal_connection",
    "object": [
        {
            "table": {
                "source_table": "transactions",
                "start_date": "2024-01-01T00:00:00Z",
                "end_date": "2024-01-31T23:59:59Z",
                "page_size": 100
            }
        }
    ]
}
```

2. **Configure Date Ranges**:
   - Set `start_date` and `end_date` for each ingestion window
   - Ensure the range doesn't exceed 31 days
   - Use ISO 8601 format with UTC timezone (ending in 'Z')
   - For ongoing syncs, implement a rolling window strategy

3. **Example: Rolling Window for Incremental Sync**:

```python
from datetime import datetime, timedelta

# Sync the last 30 days of transactions
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=30)

pipeline_spec = {
    "connection_name": "paypal_connection",
    "object": [
        {
            "table": {
                "source_table": "transactions",
                "start_date": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_date": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "page_size": 500  # Maximum page size for better performance
            }
        }
    ]
}
```

4. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a short date range (e.g., 1-7 days) to validate your pipeline
- **Respect API Limits**: PayPal enforces a 31-day maximum date range - plan your queries accordingly
- **Use Maximum Page Size**: Set `page_size` to 500 (the maximum) to reduce API calls
- **Schedule Appropriately**: 
  - For recent transactions: Run daily with a 1-day window
  - For historical backfills: Run with 31-day windows sequentially
  - Avoid overlapping date ranges to prevent duplicate data
- **Monitor Rate Limits**: PayPal allows **50 requests per 10 seconds** - if you hit rate limits, the connector will raise an error with retry guidance
- **Timezone Awareness**: Always use UTC timestamps (ending in 'Z') to avoid timezone conversion issues

#### Troubleshooting

**Common Issues:**

1. **Authentication Errors (401 Unauthorized)**
   - **Cause**: Invalid or expired credentials
   - **Solution**: Verify your `client_id` and `client_secret` are correct. Ensure your PayPal app is active in the Developer Dashboard.

2. **Rate Limiting (429 Too Many Requests)**
   - **Cause**: Exceeded 50 requests per 10 seconds
   - **Solution**: The connector will report the retry-after time. Reduce pipeline frequency or implement exponential backoff in your scheduling.

3. **Date Range Error**
   - **Cause**: Date range exceeds 31 days or dates are outside the 3-year historical window
   - **Solution**: Split queries into 31-day windows. Verify dates are within the last 3 years.

4. **Empty Results**
   - **Cause**: No transactions in the specified date range, or dates in the future
   - **Solution**: Verify your account had activity during the specified period. Check that `start_date` is before `end_date` and both are in the past.

5. **Invalid Date Format**
   - **Cause**: Date strings not in ISO 8601 format
   - **Solution**: Use format `"YYYY-MM-DDTHH:MM:SSZ"` (e.g., `"2024-01-15T00:00:00Z"`)

6. **Missing Nested Fields**
   - **Cause**: Some transactions may not have all nested objects (e.g., no shipping info for certain transaction types)
   - **Solution**: This is expected behavior. The connector sets missing nested objects to `null`. Handle nulls in your downstream queries.

## References

- [PayPal REST API Documentation](https://developer.paypal.com/api/rest/)
- [PayPal Transaction Search API v1](https://developer.paypal.com/docs/api/transaction-search/v1/)
- [PayPal Developer Dashboard](https://developer.paypal.com/dashboard/)
- [PayPal REST API Current Resources](https://developer.paypal.com/api/rest/current-resources/)
- [OAuth 2.0 Client Credentials Flow](https://developer.paypal.com/api/rest/authentication/)
- [PayPal API Rate Limits](https://developer.paypal.com/api/rest/rate-limiting/)

