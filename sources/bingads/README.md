# Lakeflow Bing Ads (Microsoft Advertising) Community Connector

This documentation provides setup instructions and reference information for the Bing Ads (Microsoft Advertising) source connector.

## Prerequisites

- A Microsoft Advertising account with API access
- An Azure AD application registered in the Azure Portal for OAuth authentication
- A Microsoft Advertising Developer Token (obtained from Microsoft Advertising web UI → Tools → Developer Token)
- Multi-factor authentication (MFA) completed during the initial OAuth consent flow (required since June 2022)

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `client_id` | string | Yes | Application (client) ID from Azure Portal App registrations. Identifies your application to Microsoft identity platform. |
| `client_secret` | string | No | Client secret from Azure Portal App registrations. Required for web applications. Optional for native/desktop applications. |
| `refresh_token` | string | Yes | Long-lived OAuth 2.0 refresh token obtained through the OAuth consent flow. Used to obtain access tokens at runtime. |
| `developer_token` | string | Yes | Developer token that identifies your application to the Microsoft Advertising API. |
| `customer_id` | string | No | Target customer ID for API operations. Can be overridden per-table via table options. |
| `account_id` | string | No | Target account ID for API operations. Can be overridden per-table via table options. |
| `env` | string | No | API environment to use. Valid values: `production` or `sandbox`. Defaults to `production`. |
| `externalOptionsAllowList` | string | Yes | A comma-separated list of table-specific options allowed in pipeline configuration. Must be set to: `account_id,campaign_id,ad_group_id,start_date,end_date,lookback_days` |

### Obtaining Required Credentials

#### 1. Register a Microsoft Entra ID Application

1. Go to the [Azure Portal](https://portal.azure.com/)
2. Navigate to **Microsoft Entra ID** → **App registrations** → **New registration**
3. Register your application and note the **Application (client) ID** — this is your `client_id`
4. For web applications, create a **Client secret** under **Certificates & secrets**

#### 2. Obtain a Developer Token

1. Sign in to [Microsoft Advertising](https://ads.microsoft.com/)
2. Go to **Tools** → **Developer Token**
3. Request a developer token if you don't have one

#### 3. Complete OAuth Consent Flow and Obtain Refresh Token

1. Direct users to the Microsoft authorization endpoint with the following parameters:
   - Authorization URL: `https://login.microsoftonline.com/common/oauth2/v2.0/authorize`
   - Scope: `https://ads.microsoft.com/msads.manage offline_access`
2. After consent, exchange the authorization code for tokens at:
   - Token URL: `https://login.microsoftonline.com/common/oauth2/v2.0/token`
3. Store the returned `refresh_token` securely

#### 4. Find Your Customer ID and Account ID

1. Sign in to [Microsoft Advertising](https://ads.microsoft.com/)
2. Your **Customer ID** and **Account ID** are displayed in the top navigation bar
3. Alternatively, find them under **Settings** → **Accounts & Billing**

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one
3. Set the `externalOptionsAllowList` connection option to: `account_id,campaign_id,ad_group_id,start_date,end_date,lookback_days`

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

This connector supports two categories of objects:

### Entity Tables (Snapshot Ingestion)

Entity tables are ingested as full snapshots on each pipeline run.

| Object | Primary Key | Description | Required Table Options |
|--------|-------------|-------------|------------------------|
| `accounts` | `Id` | Microsoft Advertising accounts associated with the customer | None |
| `campaigns` | `Id` | Advertising campaigns with budget and targeting settings | `account_id` |
| `ad_groups` | `Id` | Ad groups containing ads and keywords within campaigns | `account_id` |
| `ads` | `Id` | Individual advertisements (Text, Expanded Text, Responsive Search) | `account_id` |
| `keywords` | `Id` | Search keywords configured for ad groups | `account_id` |

**Optional Table Options for Entity Tables:**
- `account_id`: The account ID to read data from (required for all except `accounts`)
- `campaign_id`: Filter ad groups by specific campaign
- `ad_group_id`: Filter ads or keywords by specific ad group

### Performance Report Tables (Incremental Append)

Report tables support incremental ingestion using the `TimePeriod` cursor field. Data is aggregated daily.

| Object | Primary Keys | Cursor Field | Description |
|--------|--------------|--------------|-------------|
| `campaign_performance_report` | `TimePeriod`, `AccountId`, `CampaignId` | `TimePeriod` | Campaign-level performance metrics |
| `ad_group_performance_report` | `TimePeriod`, `AccountId`, `CampaignId`, `AdGroupId` | `TimePeriod` | Ad group-level performance metrics |
| `ad_performance_report` | `TimePeriod`, `AccountId`, `CampaignId`, `AdGroupId`, `AdId` | `TimePeriod` | Individual ad performance metrics |
| `keyword_performance_report` | `TimePeriod`, `AccountId`, `CampaignId`, `AdGroupId`, `KeywordId` | `TimePeriod` | Keyword-level performance metrics |

**Table Options for Report Tables:**
- `account_id`: (Required) The account ID to read data from
- `start_date`: Start date for the report in `YYYY-MM-DD` format. Defaults to 30 days ago.
- `end_date`: End date for the report in `YYYY-MM-DD` format. Defaults to yesterday.
- `lookback_days`: Number of days to look back from the cursor for data adjustments. Defaults to `3`.

> **Note:** The cursor state (max `TimePeriod` date) is automatically persisted by Spark Structured Streaming via checkpointing. On subsequent pipeline runs, the framework provides the stored cursor to the connector, which then fetches data starting from `cursor - lookback_days`.

### Report Metrics

All performance report tables include the following metrics:

| Column | Type | Description |
|--------|------|-------------|
| `Impressions` | Long | Number of times ads were displayed |
| `Clicks` | Long | Number of clicks on ads |
| `Ctr` | Double | Click-through rate |
| `AverageCpc` | Double | Average cost per click |
| `Spend` | Double | Total ad spend |
| `Conversions` | Long | Number of conversions |
| `ConversionRate` | Double | Conversion rate |
| `CostPerConversion` | Double | Cost per conversion |
| `Revenue` | Double | Revenue from conversions |
| `ReturnOnAdSpend` | Double | Return on ad spend |

The `keyword_performance_report` also includes `QualityScore`.

## Data Type Mapping

| Microsoft Advertising Type | Spark Type | Notes |
|----------------------------|------------|-------|
| `long` | `LongType` | 64-bit integer |
| `double` | `DoubleType` | Floating point number |
| `string` | `StringType` | Text values |
| `Date` (Day, Month, Year) | `StringType` | Converted to `YYYY-MM-DD` format |
| `ArrayOfString` | `ArrayType(StringType)` | List of strings |
| Enum types | `StringType` | Enum values converted to strings |
| `Bid` | `StructType` | Nested structure with `Amount` field |
| `BiddingScheme` | `StructType` | Nested structure with `Type`, `MaxCpc`, `TargetCpa`, `TargetRoas` fields |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).

2. For entity tables, specify the `account_id` and optional filters:

```json
{
  "pipeline_spec": {
    "connection_name": "your_bingads_connection",
    "object": [
      {
        "table": {
          "source_table": "accounts"
        }
      },
      {
        "table": {
          "source_table": "campaigns",
          "account_id": "123456789"
        }
      },
      {
        "table": {
          "source_table": "ad_groups",
          "account_id": "123456789",
          "campaign_id": "987654321"
        }
      }
    ]
  }
}
```

3. For performance reports, specify date ranges and lookback period:

```json
{
  "pipeline_spec": {
    "connection_name": "your_bingads_connection",
    "object": [
      {
        "table": {
          "source_table": "campaign_performance_report",
          "account_id": "123456789",
          "start_date": "2025-01-01",
          "end_date": "2025-01-31",
          "lookback_days": "3"
        }
      },
      {
        "table": {
          "source_table": "keyword_performance_report",
          "account_id": "123456789"
        }
      }
    ]
  }
}
```

4. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects (e.g., `accounts` and `campaigns`) to test your pipeline
- **Use Incremental Sync for Reports**: Performance reports support incremental ingestion, reducing API calls and improving performance
- **Set Appropriate Lookback Days**: Microsoft Advertising data can be adjusted retroactively; a 2-3 day lookback window is recommended
- **Data Freshness**: Performance data may have a delay of 2-3 hours; conversion data may take up to 24 hours
- **Use Account-Level Filtering**: When you have multiple accounts, configure pipelines per account for better manageability

#### API Rate Limits

- The Microsoft Advertising API has rate limits that vary by operation
- A maximum of 5 concurrent report requests per customer is enforced
- Implement appropriate scheduling intervals to avoid hitting rate limits

#### Troubleshooting

**Common Issues:**

| Issue | Cause | Solution |
|-------|-------|----------|
| Authentication errors | Expired refresh token | Re-authenticate through the OAuth consent flow to obtain a new refresh token |
| Missing developer token | Developer token not configured | Obtain a developer token from Microsoft Advertising UI → Tools → Developer Token |
| Account access denied | Insufficient permissions | Ensure your Microsoft Advertising account has access to the target customer/account |
| MFA required | Multi-factor authentication not completed | Complete MFA during the OAuth consent flow |
| Empty report data | No data for the specified date range | Verify the date range has data in your Microsoft Advertising account |
| Sandbox limitations | Using sandbox environment | Sandbox has limited functionality; switch to production for full features |

## References

- [Microsoft Advertising API Documentation](https://learn.microsoft.com/en-us/advertising/guides/?view=bingads-13)
- [OAuth Authentication Quick Start](https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-quick-start?view=bingads-13)
- [Campaign Management Service](https://learn.microsoft.com/en-us/advertising/campaign-management-service/campaign-management-service-reference?view=bingads-13)
- [Reporting Service](https://learn.microsoft.com/en-us/advertising/reporting-service/reporting-service-reference?view=bingads-13)
- [Python SDK Getting Started](https://learn.microsoft.com/en-us/advertising/guides/get-started-python?view=bingads-13)

