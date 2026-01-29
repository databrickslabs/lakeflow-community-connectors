# Lakeflow Zoho CRM Community Connector

This documentation provides setup instructions and reference information for the Zoho CRM source connector.

## Prerequisites

- **Zoho CRM Account**: An active Zoho CRM account with API access
- **Zoho CRM Edition**: Standard or higher (API access varies by edition)
- **OAuth Credentials**: Client ID, Client Secret, and Refresh Token from Zoho API Console
- **API Permissions**: Read access to CRM modules you want to sync

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `client_id` | string | Yes | OAuth Client ID from Zoho API Console | `1000.XXXXX...` |
| `client_value_tmp` | string | Yes | OAuth Client Secret from Zoho API Console | `abc123...` |
| `refresh_value_tmp` | string | Yes | OAuth Refresh Token (never expires) | `1000.YYYYY...` |
| `base_url` | string | No | Zoho accounts URL for your data center | `https://accounts.zoho.com` |
| `initial_load_start_date` | string | No | Starting point for the first sync. If omitted, syncs all historical data. (ISO 8601 format) | `2024-01-01T00:00:00Z` |

**Note:** This connector does not require any table-specific options. The `externalOptionsAllowList` connection parameter does not need to be included.

### Obtaining OAuth Credentials

> 💡 **Quick Start**: A Databricks notebook (`zoho_crm_oauth_setup.py`) is included with this connector to guide you through the OAuth setup interactively. Import and run it in your Databricks workspace - it handles URL generation, code extraction, and saves credentials automatically.

#### Step 1: Create OAuth Application in Zoho

1. Go to [Zoho API Console](https://api-console.zoho.com/)
2. Click **"Add Client"** → Select **"Server-based Applications"**
3. Fill in the application details:
   - **Client Name**: `Databricks Zoho CRM Connector`
   - **Homepage URL**: Your Databricks workspace URL (e.g., `https://your-workspace.cloud.databricks.com/`)
   - **Authorized Redirect URI**: `https://<your-workspace>/login/oauth/zoho_crm.html`
4. Click **"Create"**
5. Copy and save your **Client ID** and **Client Secret**

#### Step 2: Generate Authorization Code

Build and open this URL in your browser (replace the placeholder values):

```
https://{accounts-server}/oauth/v2/auth?response_type=code&client_id={CLIENT_ID}&scope=ZohoCRM.modules.ALL,ZohoCRM.settings.ALL,ZohoCRM.settings.modules.ALL&redirect_uri={REDIRECT_URI}&access_type=offline&prompt=consent
```

| Data Center | Accounts Server URL |
|-------------|---------------------|
| US | `https://accounts.zoho.com` |
| EU | `https://accounts.zoho.eu` |
| IN | `https://accounts.zoho.in` |
| AU | `https://accounts.zoho.com.au` |
| CN | `https://accounts.zoho.com.cn` |
| JP | `https://accounts.zoho.jp` |

After authorization, you'll be redirected with an authorization code in the URL.

> ⚠️ **Important**: The authorization code is valid for **2 minutes only**. Proceed to Step 3 immediately.

#### Step 3: Exchange Code for Refresh Token

Run this curl command within 2 minutes of getting the authorization code:

```bash
curl -X POST "https://{accounts-server}/oauth/v2/token" \
  -d "grant_type=authorization_code" \
  -d "client_id={CLIENT_ID}" \
  -d "client_secret={CLIENT_SECRET}" \
  -d "redirect_uri={REDIRECT_URI}" \
  -d "code={AUTHORIZATION_CODE}"
```

The response will contain your `refresh_token` - save this securely!

### Base URL by Data Center

Choose the appropriate `base_url` (Zoho accounts URL) based on your data center:

| Data Center | Base URL |
|-------------|----------|
| US | `https://accounts.zoho.com` |
| EU | `https://accounts.zoho.eu` |
| IN | `https://accounts.zoho.in` |
| AU | `https://accounts.zoho.com.au` |
| CN | `https://accounts.zoho.com.cn` |
| JP | `https://accounts.zoho.jp` |

The connector automatically derives the correct API endpoint from the accounts URL.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one

The connection can also be created using the standard Unity Catalog API.

## Code Structure

The connector is organized into modular components:

```
zoho_crm/
├── zoho_crm.py          # Main orchestrator (LakeflowConnect class)
├── zoho_client.py       # API client: auth, HTTP, pagination
├── zoho_types.py        # Spark type mappings and schema definitions
└── handlers/
    ├── base.py          # Abstract TableHandler interface
    ├── module.py        # Standard CRM modules (Leads, Contacts, etc.)
    ├── settings.py      # Org tables (Users, Roles, Profiles)
    ├── subform.py       # Line items (disabled by default)
    └── related.py       # Junction tables (Campaigns_Leads, etc.)
```

## Supported Objects

This connector supports **dynamic module discovery** - it automatically discovers all available CRM modules in your Zoho CRM instance, including custom modules.

### Standard Modules

| Module | Primary Key | Ingestion Type | Cursor Field |
|--------|-------------|----------------|--------------|
| `Leads` | `id` | CDC | `Modified_Time` |
| `Contacts` | `id` | CDC | `Modified_Time` |
| `Accounts` | `id` | CDC | `Modified_Time` |
| `Deals` | `id` | CDC | `Modified_Time` |
| `Tasks` | `id` | CDC | `Modified_Time` |
| `Events` | `id` | CDC | `Modified_Time` |
| `Calls` | `id` | CDC | `Modified_Time` |
| `Products` | `id` | CDC | `Modified_Time` |
| `Quotes`* | `id` | CDC | `Modified_Time` |
| `Sales_Orders`* | `id` | CDC | `Modified_Time` |
| `Purchase_Orders`* | `id` | CDC | `Modified_Time` |
| `Invoices`* | `id` | CDC | `Modified_Time` |
| `Campaigns` | `id` | CDC | `Modified_Time` |
| `Vendors` | `id` | CDC | `Modified_Time` |
| `Price_Books` | `id` | CDC | `Modified_Time` |
| `Cases` | `id` | CDC | `Modified_Time` |
| `Solutions` | `id` | CDC | `Modified_Time` |
| `Notes` | `id` | CDC | `Modified_Time` |
| `Attachments` | `id` | Append | - |

> **\*** *Quotes, Sales_Orders, Purchase_Orders, and Invoices require **Zoho Inventory** or **Zoho Books** integration. These modules return HTTP 400 errors if not enabled in your Zoho account.*

### Custom Modules

All custom modules created in your Zoho CRM instance are automatically discovered and supported. Custom modules:
- Use `id` as the primary key
- Support CDC ingestion with `Modified_Time` cursor
- Have dynamically discovered schemas

### Ingestion Types

| Type | Description |
|------|-------------|
| **CDC** (Change Data Capture) | Incremental sync using `Modified_Time` cursor. Captures new, updated, and deleted records. |
| **Append** | New records only (used for Attachments module). |
| **Snapshot** | Full table replacement (used for modules without `Modified_Time`). |

### Delete Handling

The connector captures deleted records using Zoho CRM's Deleted Records API. Deleted records include:
- `id`: The record ID
- `deleted_time`: When the record was deleted
- `deleted_by`: Who deleted the record
- `_zoho_deleted`: Boolean flag set to `true` for deleted records

> **Note**: Subform/line item tables (Quoted_Items, Ordered_Items, etc.) are **disabled by default** because they require Zoho Inventory or Zoho Books. To enable them, uncomment the relevant entries in `handlers/subform.py`.

### Special Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | Long | Primary key for all records (system-generated, immutable) |
| `Created_Time` | String (ISO 8601) | When the record was created |
| `Modified_Time` | String (ISO 8601) | When the record was last modified (cursor field for CDC) |
| `Created_By` | Struct | User who created the record |
| `Modified_By` | Struct | User who last modified the record |
| `Owner` | Struct | Record owner (user lookup) |

## Data Type Mapping

| Zoho CRM Type | Databricks Type | Notes |
|---------------|-----------------|-------|
| `bigint` | `LongType` | Used for ID fields |
| `text`, `textarea`, `email`, `phone`, `website` | `StringType` | Text fields |
| `picklist` | `StringType` | Single-select dropdown |
| `multiselectpicklist` | `ArrayType(StringType)` | Multi-select dropdown |
| `integer` | `LongType` | Whole numbers |
| `double`, `currency`, `percent` | `DoubleType` | Decimal numbers |
| `boolean` | `BooleanType` | True/false values |
| `date`, `datetime` | `StringType` | ISO 8601 format |
| `lookup`, `ownerlookup` | `StructType` | Contains `id`, `name`, `email` fields |
| `multiselectlookup` | `ArrayType(StructType)` | Array of lookup objects |
| `subform` | `ArrayType(StructType)` | Nested line items |
| `fileupload`, `imageupload` | `StringType` | File/image ID or URL |
| `autonumber` | `StringType` | Auto-generated sequence |

### Nested Structures

**Lookup fields** are represented as structs:
```json
{
  "Owner": {
    "id": "1234567000000098765",
    "name": "John Doe",
    "email": "john.doe@example.com"
  }
}
```

**Subform fields** are represented as arrays of structs:
```json
{
  "Product_Details": [
    {"id": "123", "Product_Name": "Widget A", "Quantity": 10},
    {"id": "124", "Product_Name": "Widget B", "Quantity": 5}
  ]
}
```

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Specify the tables you want to sync:

```json
{
  "pipeline_spec": {
    "connection_name": "zoho_crm_connection",
    "object": [
      {
        "table": {
          "source_table": "Leads"
        }
      },
      {
        "table": {
          "source_table": "Contacts"
        }
      },
      {
        "table": {
          "source_table": "Deals"
        }
      }
    ]
  }
}
```

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline
- **Use Incremental Sync**: The connector uses CDC by default, which reduces API calls and improves performance
- **Set Appropriate Schedules**: Balance data freshness requirements with API usage limits
- **Monitor API Credits**: Zoho CRM has daily API credit limits that vary by edition
- **Use Field Selection**: For large modules, consider selecting only needed fields to reduce API usage

#### API Rate Limits

| Edition | Daily API Calls | Concurrent Requests |
|---------|-----------------|---------------------|
| Free | 5,000 | N/A |
| Standard | 10,000 | N/A |
| Professional | 15,000 | N/A |
| Enterprise | 25,000 | 60/min |
| Ultimate | 100,000 | 120/min |

The connector implements automatic retry with exponential backoff for rate limit errors.

#### Troubleshooting

**Common Issues:**

| Error | Cause | Solution |
|-------|-------|----------|
| `invalid_code` | Authorization code expired (>2 min) or already used | Generate a new authorization code |
| `invalid_client` | Wrong Client ID/Secret or data center mismatch | Verify credentials in Zoho API Console |
| `invalid_redirect_uri` | Redirect URI doesn't match registered URI | Use exact URI from Zoho API Console |
| `RATE_LIMIT_EXCEEDED` | API credit limit reached | Wait for daily quota reset or reduce sync frequency |
| `AUTHENTICATION_FAILURE` | Expired or invalid access token | Connector auto-refreshes; check refresh_token validity |
| `OAUTH_SCOPE_MISMATCH` | Insufficient OAuth scopes | Re-authorize with required scopes |

**Token Refresh Failures:**

If you see "Token refresh response missing 'access_token'":
1. Verify your `client_id` and `client_secret` are correct
2. Check that your `refresh_token` is valid (hasn't been revoked)
3. Ensure you're using the correct `base_url` for your data center

**Empty Module Data:**

Some Zoho CRM modules (like `Visits`, `Actions_Performed`) may return empty responses. This is normal if these features are not enabled in your CRM instance.

## Security Recommendations

- **Credentials are stored securely** in the Unity Catalog connection - no need for separate secrets management
- **Rotate Client Secrets periodically** in Zoho API Console (update the UC connection afterward)
- **Never commit credentials** to version control
- **Use minimal OAuth scopes** if you don't need full access

## References

- [Zoho CRM API v8 Documentation](https://www.zoho.com/crm/developer/docs/api/v8/)
- [Zoho OAuth Authorization Guide](https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html)
- [Zoho API Console](https://api-console.zoho.com/)
- [Zoho CRM API Limits](https://www.zoho.com/crm/developer/docs/api/v8/api-limits.html)
- [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)

