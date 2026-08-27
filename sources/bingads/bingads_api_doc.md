# **Bing Ads (Microsoft Advertising) API Documentation**

## **Authorization**

### Preferred Method: OAuth 2.0 with Refresh Token

The Bing Ads API (Microsoft Advertising API) uses OAuth 2.0 for authentication. The connector stores `client_id`, `client_secret` (optional for native apps), `refresh_token`, and `developer_token`, then exchanges the refresh token for an access token at runtime.

#### Required Credentials

| Parameter | Description | Where to Obtain |
|-----------|-------------|-----------------|
| `client_id` | Application (client) ID | Azure Portal - App registrations |
| `client_secret` | Client secret (required for web apps, optional for native apps) | Azure Portal - App registrations |
| `refresh_token` | Long-lived token for obtaining access tokens | OAuth consent flow |
| `developer_token` | Identifies your application to the API | Microsoft Advertising web UI → Tools → Developer Token |

#### OAuth Endpoints

| Endpoint | URL |
|----------|-----|
| Authorization | `https://login.microsoftonline.com/common/oauth2/v2.0/authorize` |
| Token | `https://login.microsoftonline.com/common/oauth2/v2.0/token` |

#### OAuth Scopes

```
https://ads.microsoft.com/msads.manage offline_access
```

#### Token Refresh Request

**Endpoint:** `POST https://login.microsoftonline.com/common/oauth2/v2.0/token`

**Headers:**
```
Content-Type: application/x-www-form-urlencoded
```

**Body Parameters:**
```
client_id={client_id}
&scope=https://ads.microsoft.com/msads.manage offline_access
&grant_type=refresh_token
&refresh_token={refresh_token}
```

**Example Response:**
```json
{
  "access_token": "eyJ0eXAi...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "scope": "https://ads.microsoft.com/msads.manage offline_access",
  "refresh_token": "M.R3_BAY..."
}
```

#### API Request Headers

All Bing Ads API requests (SOAP-based) require the following headers:

| Header | Description | Required |
|--------|-------------|----------|
| `DeveloperToken` | Your developer token | Yes |
| `AuthenticationToken` | OAuth access token | Yes |
| `CustomerAccountId` | Target account ID | Depends on operation |
| `CustomerId` | Target customer ID | Depends on operation |

#### Example SOAP Request (GetUser)

```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:v13="https://bingads.microsoft.com/Customer/v13">
  <soapenv:Header>
    <v13:DeveloperToken>{developer_token}</v13:DeveloperToken>
    <v13:AuthenticationToken>{access_token}</v13:AuthenticationToken>
  </soapenv:Header>
  <soapenv:Body>
    <v13:GetUserRequest>
      <v13:UserId xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="true"/>
    </v13:GetUserRequest>
  </soapenv:Body>
</soapenv:Envelope>
```

### Multi-Factor Authentication Requirement

As of June 2022, Microsoft Advertising requires multi-factor authentication (MFA) for all API access. Users must complete MFA during the initial OAuth consent flow.

---

## **Object List**

### Available API Services and Objects

The Bing Ads API v13 consists of five main services, each managing different object types:

| Service | Purpose | Key Objects |
|---------|---------|-------------|
| **Campaign Management** | Manage advertising entities | Campaigns, Ad Groups, Ads, Keywords, Ad Extensions |
| **Customer Management** | Manage accounts and users | Customers, Accounts, Users |
| **Bulk** | Large-scale upload/download | All campaign entities in CSV format |
| **Reporting** | Performance data | Multiple report types (see below) |
| **Ad Insight** | Optimization suggestions | Keywords, Bids, Budget suggestions |

### Object Hierarchy

```
Customer
└── Account
    └── Campaign
        └── Ad Group
            ├── Ads (Text Ads, Responsive Search Ads, etc.)
            ├── Keywords
            └── Ad Extensions
```

### Campaign Management Objects (Static List)

| Object | Description | Parent |
|--------|-------------|--------|
| `Campaign` | Advertising campaign | Account |
| `AdGroup` | Group of ads within a campaign | Campaign |
| `Ad` | Individual advertisement | Ad Group |
| `Keyword` | Search terms to trigger ads | Ad Group |
| `NegativeKeyword` | Terms to exclude | Campaign or Ad Group |
| `AdExtension` | Additional ad content (sitelinks, callouts, etc.) | Account |
| `Audience` | Target audience definitions | Account |
| `Budget` | Shared budget | Account |
| `Label` | Organizational tags | Account |

### Reporting Objects (Static List)

| Report Type | Description | Granularity |
|-------------|-------------|-------------|
| `AccountPerformanceReport` | Account-level metrics | Account |
| `CampaignPerformanceReport` | Campaign-level metrics | Campaign |
| `AdGroupPerformanceReport` | Ad group-level metrics | Ad Group |
| `AdPerformanceReport` | Individual ad metrics | Ad |
| `KeywordPerformanceReport` | Keyword-level metrics | Keyword |
| `SearchQueryPerformanceReport` | Actual search terms | Search Query |
| `AudiencePerformanceReport` | Audience targeting metrics | Audience |
| `GeographicPerformanceReport` | Location-based metrics | Geography |
| `AgeGenderAudienceReport` | Demographic metrics | Demographics |
| `DevicePerformanceReport` | Device type breakdown | Device |

### Bulk Download Entities (Static List)

The Bulk service supports downloading the following entity types:

- Campaigns
- AdGroups
- Ads
- Keywords
- NegativeKeywords
- CampaignNegativeKeywords
- AdGroupNegativeKeywords
- SitelinkAdExtensions
- CallAdExtensions
- CalloutAdExtensions
- StructuredSnippetAdExtensions
- Audiences
- RemarketingLists
- Budgets
- Labels

---

## **Object Schema**

### Schema Retrieval

Object schemas are defined in the WSDL (Web Services Description Language) files. The schema is static and defined in the API specification.

**WSDL Endpoints:**
- Campaign Management: `https://campaign.api.bingads.microsoft.com/Api/Advertiser/CampaignManagement/v13/CampaignManagementService.svc?wsdl`
- Customer Management: `https://clientcenter.api.bingads.microsoft.com/Api/CustomerManagement/v13/CustomerManagementService.svc?wsdl`
- Reporting: `https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc?wsdl`
- Bulk: `https://bulk.api.bingads.microsoft.com/Api/Advertiser/CampaignManagement/v13/BulkService.svc?wsdl`

### Campaign Schema

| Field | Type | Description |
|-------|------|-------------|
| `Id` | long | Unique campaign identifier |
| `Name` | string | Campaign name (max 128 characters) |
| `Status` | CampaignStatus | Active, Paused, or Deleted |
| `BudgetType` | BudgetLimitType | DailyBudgetAccelerated or DailyBudgetStandard |
| `DailyBudget` | double | Daily budget amount |
| `TimeZone` | string | Campaign time zone |
| `CampaignType` | CampaignType | Search, Shopping, Audience, etc. |
| `Languages` | string[] | Target languages |
| `BiddingScheme` | BiddingScheme | EnhancedCpc, ManualCpc, MaxClicks, etc. |
| `Settings` | Setting[] | Additional campaign settings |
| `TrackingUrlTemplate` | string | URL tracking template |
| `UrlCustomParameters` | CustomParameters | Custom URL parameters |
| `AudienceAdsBidAdjustment` | int | Bid adjustment percentage (-100 to 900) |
| `ExperimentId` | long | Associated experiment ID |
| `FinalUrlSuffix` | string | Final URL suffix |
| `MultimediaAdsBidAdjustment` | int | Multimedia ads bid adjustment |
| `TargetSetting` | TargetSetting | Targeting options |
| `BidStrategyId` | long | Shared bid strategy ID |

### Ad Group Schema

| Field | Type | Description |
|-------|------|-------------|
| `Id` | long | Unique ad group identifier |
| `Name` | string | Ad group name (max 256 characters) |
| `CampaignId` | long | Parent campaign ID |
| `Status` | AdGroupStatus | Active, Paused, Expired, or Deleted |
| `StartDate` | Date | Start date |
| `EndDate` | Date | End date |
| `CpcBid` | Bid | Cost-per-click bid |
| `ContentMatchBid` | Bid | Content network bid |
| `Language` | string | Target language |
| `Network` | Network | OwnedAndOperatedAndSyndicatedSearch, etc. |
| `AdRotation` | AdRotation | RotateAdsEvenly or OptimizeForClicks |
| `PrivacyStatus` | AdGroupPrivacyStatus | Privacy status |
| `TrackingUrlTemplate` | string | URL tracking template |
| `UrlCustomParameters` | CustomParameters | Custom URL parameters |
| `FinalUrlSuffix` | string | Final URL suffix |

### Keyword Schema

| Field | Type | Description |
|-------|------|-------------|
| `Id` | long | Unique keyword identifier |
| `AdGroupId` | long | Parent ad group ID |
| `Text` | string | Keyword text |
| `MatchType` | MatchType | Exact, Phrase, or Broad |
| `Status` | KeywordStatus | Active, Paused, or Deleted |
| `Bid` | Bid | Keyword bid amount |
| `BiddingScheme` | BiddingScheme | Bid strategy |
| `DestinationUrl` | string | Destination URL (deprecated) |
| `FinalUrls` | string[] | Final landing page URLs |
| `FinalMobileUrls` | string[] | Mobile landing page URLs |
| `TrackingUrlTemplate` | string | URL tracking template |
| `UrlCustomParameters` | CustomParameters | Custom URL parameters |
| `FinalUrlSuffix` | string | Final URL suffix |

### Account Schema

| Field | Type | Description |
|-------|------|-------------|
| `Id` | long | Unique account identifier |
| `Name` | string | Account name |
| `Number` | string | Account number |
| `AccountType` | AccountType | Prepay or Invoice |
| `AccountLifeCycleStatus` | AccountLifeCycleStatus | Draft, Active, Inactive, etc. |
| `PauseReason` | byte | Reason for pause (if applicable) |
| `CurrencyCode` | CurrencyCode | Account currency (e.g., USD) |
| `TimeZone` | TimeZone | Account time zone |
| `Language` | LanguageType | Primary language |
| `PaymentMethodId` | long | Payment method ID |
| `PaymentMethodType` | PaymentMethodType | Payment method type |
| `BillToCustomerId` | long | Billing customer ID |
| `SoldToPaymentInstrumentId` | long | Payment instrument ID |
| `TaxInformation` | KeyValuePairOfstringstring[] | Tax information |
| `BackUpPaymentInstrumentId` | long | Backup payment ID |
| `AutoTagType` | AutoTagType | Auto-tagging configuration |
| `LinkedAgencies` | CustomerInfo[] | Linked agency accounts |

---

## **Get Object Primary Keys**

Primary keys are static and defined by the API specification:

| Object | Primary Key Field(s) |
|--------|---------------------|
| `Campaign` | `Id` |
| `AdGroup` | `Id` |
| `Ad` | `Id` |
| `Keyword` | `Id` |
| `Account` | `Id` |
| `Customer` | `Id` |
| `User` | `Id` |
| `AdExtension` | `Id` |
| `Audience` | `Id` |
| `Budget` | `Id` |
| `Label` | `Id` |
| `NegativeKeyword` | `Id` |

For **Reports**, there is no single primary key. Reports are time-series data aggregated by:
- Time period (date, week, month, etc.)
- Account/Campaign/Ad Group/Keyword/Ad IDs
- Other dimensions specified in the report request

---

## **Object's Ingestion Type**

| Object | Ingestion Type | Rationale |
|--------|---------------|-----------|
| `Campaign` | `snapshot` | No reliable change feed; must re-read all campaigns |
| `AdGroup` | `snapshot` | No reliable change feed; must re-read all ad groups |
| `Ad` | `snapshot` | No reliable change feed |
| `Keyword` | `snapshot` | No reliable change feed |
| `Account` | `snapshot` | Accounts rarely change |
| `User` | `snapshot` | Users rarely change |
| `AdExtension` | `snapshot` | No reliable change feed |
| **Performance Reports** | `append` | Time-series data with date dimensions; can request date ranges |

### Incremental Strategy for Reports

Reports can be retrieved incrementally by date range:

1. **Cursor Field**: Report date dimension (e.g., `TimePeriod`)
2. **Ordering**: Reports are ordered by date ascending
3. **Lookback Window**: 2-3 days recommended (metrics can be adjusted retroactively)
4. **Delete Handling**: Reports don't have deletes; use lookback window to capture adjustments

---

## **Read API for Data Retrieval**

### Option 1: Campaign Management API (SOAP)

Best for: Small to medium accounts, real-time data

**Service Endpoint:**
```
https://campaign.api.bingads.microsoft.com/Api/Advertiser/CampaignManagement/v13/CampaignManagementService.svc
```

#### Get Campaigns by Account ID

**SOAP Request:**
```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:v13="https://bingads.microsoft.com/CampaignManagement/v13">
  <soapenv:Header>
    <v13:DeveloperToken>{developer_token}</v13:DeveloperToken>
    <v13:AuthenticationToken>{access_token}</v13:AuthenticationToken>
    <v13:CustomerAccountId>{account_id}</v13:CustomerAccountId>
    <v13:CustomerId>{customer_id}</v13:CustomerId>
  </soapenv:Header>
  <soapenv:Body>
    <v13:GetCampaignsByAccountIdRequest>
      <v13:AccountId>{account_id}</v13:AccountId>
      <v13:CampaignType>Search Shopping Audience</v13:CampaignType>
    </v13:GetCampaignsByAccountIdRequest>
  </soapenv:Body>
</soapenv:Envelope>
```

#### Get Ad Groups by Campaign ID

**SOAP Request:**
```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:v13="https://bingads.microsoft.com/CampaignManagement/v13">
  <soapenv:Header>
    <v13:DeveloperToken>{developer_token}</v13:DeveloperToken>
    <v13:AuthenticationToken>{access_token}</v13:AuthenticationToken>
    <v13:CustomerAccountId>{account_id}</v13:CustomerAccountId>
    <v13:CustomerId>{customer_id}</v13:CustomerId>
  </soapenv:Header>
  <soapenv:Body>
    <v13:GetAdGroupsByCampaignIdRequest>
      <v13:CampaignId>{campaign_id}</v13:CampaignId>
    </v13:GetAdGroupsByCampaignIdRequest>
  </soapenv:Body>
</soapenv:Envelope>
```

### Option 2: Bulk API

Best for: Large accounts, downloading all entities efficiently

**Service Endpoint:**
```
https://bulk.api.bingads.microsoft.com/Api/Advertiser/CampaignManagement/v13/BulkService.svc
```

#### Download Campaigns by Account IDs

**Process:**
1. Submit `DownloadCampaignsByAccountIds` request
2. Poll `GetBulkDownloadStatus` until complete
3. Download the result file (compressed CSV)

**SOAP Request (Submit Download):**
```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:v13="https://bingads.microsoft.com/CampaignManagement/v13">
  <soapenv:Header>
    <v13:DeveloperToken>{developer_token}</v13:DeveloperToken>
    <v13:AuthenticationToken>{access_token}</v13:AuthenticationToken>
    <v13:CustomerAccountId>{account_id}</v13:CustomerAccountId>
    <v13:CustomerId>{customer_id}</v13:CustomerId>
  </soapenv:Header>
  <soapenv:Body>
    <v13:DownloadCampaignsByAccountIdsRequest>
      <v13:AccountIds>
        <a1:long xmlns:a1="http://schemas.microsoft.com/2003/10/Serialization/Arrays">{account_id}</a1:long>
      </v13:AccountIds>
      <v13:CompressionType>Zip</v13:CompressionType>
      <v13:DataScope>EntityData</v13:DataScope>
      <v13:DownloadEntities>
        <v13:DownloadEntity>Campaigns</v13:DownloadEntity>
        <v13:DownloadEntity>AdGroups</v13:DownloadEntity>
        <v13:DownloadEntity>Ads</v13:DownloadEntity>
        <v13:DownloadEntity>Keywords</v13:DownloadEntity>
      </v13:DownloadEntities>
      <v13:DownloadFileType>Csv</v13:DownloadFileType>
      <v13:FormatVersion>6.0</v13:FormatVersion>
    </v13:DownloadCampaignsByAccountIdsRequest>
  </soapenv:Body>
</soapenv:Envelope>
```

### Option 3: Reporting API

Best for: Performance metrics, time-series data, analytics

**Service Endpoint:**
```
https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc
```

#### Submit Report Request

**Process:**
1. Submit `SubmitGenerateReport` request
2. Poll `PollGenerateReport` until complete
3. Download the report file from the provided URL

**SOAP Request (Campaign Performance Report):**
```xml
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:v13="https://bingads.microsoft.com/Reporting/v13">
  <soapenv:Header>
    <v13:DeveloperToken>{developer_token}</v13:DeveloperToken>
    <v13:AuthenticationToken>{access_token}</v13:AuthenticationToken>
    <v13:CustomerAccountId>{account_id}</v13:CustomerAccountId>
    <v13:CustomerId>{customer_id}</v13:CustomerId>
  </soapenv:Header>
  <soapenv:Body>
    <v13:SubmitGenerateReportRequest>
      <v13:ReportRequest xmlns:i="http://www.w3.org/2001/XMLSchema-instance" 
                         i:type="v13:CampaignPerformanceReportRequest">
        <v13:ExcludeColumnHeaders>false</v13:ExcludeColumnHeaders>
        <v13:ExcludeReportFooter>true</v13:ExcludeReportFooter>
        <v13:ExcludeReportHeader>true</v13:ExcludeReportHeader>
        <v13:Format>Csv</v13:Format>
        <v13:FormatVersion>2.0</v13:FormatVersion>
        <v13:ReportName>Campaign Performance Report</v13:ReportName>
        <v13:ReturnOnlyCompleteData>false</v13:ReturnOnlyCompleteData>
        <v13:Aggregation>Daily</v13:Aggregation>
        <v13:Columns>
          <v13:CampaignPerformanceReportColumn>TimePeriod</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>AccountId</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>CampaignId</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>CampaignName</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>Impressions</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>Clicks</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>Spend</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>Conversions</v13:CampaignPerformanceReportColumn>
        </v13:Columns>
        <v13:Scope>
          <v13:AccountIds>
            <a1:long xmlns:a1="http://schemas.microsoft.com/2003/10/Serialization/Arrays">{account_id}</a1:long>
          </v13:AccountIds>
        </v13:Scope>
        <v13:Time>
          <v13:CustomDateRangeStart>
            <v13:Day>1</v13:Day>
            <v13:Month>1</v13:Month>
            <v13:Year>2025</v13:Year>
          </v13:CustomDateRangeStart>
          <v13:CustomDateRangeEnd>
            <v13:Day>31</v13:Day>
            <v13:Month>1</v13:Month>
            <v13:Year>2025</v13:Year>
          </v13:CustomDateRangeEnd>
        </v13:Time>
      </v13:ReportRequest>
    </v13:SubmitGenerateReportRequest>
  </soapenv:Body>
</soapenv:Envelope>
```

### Python SDK Usage

**Installation:**
```bash
pip install bingads
```

**Authentication Example:**
```python
from bingads.authorization import AuthorizationData, OAuthDesktopMobileAuthCodeGrant

authorization_data = AuthorizationData(
    account_id=None,
    customer_id=None,
    developer_token='YOUR_DEVELOPER_TOKEN',
    authentication=None
)

authentication = OAuthDesktopMobileAuthCodeGrant(
    client_id='YOUR_CLIENT_ID',
    oauth_tokens=None
)

# Set refresh token for token exchange
authentication.request_oauth_tokens_by_refresh_token('YOUR_REFRESH_TOKEN')
authorization_data.authentication = authentication
```

**Get Campaigns Example:**
```python
from bingads.v13.services import ServiceClient

campaign_service = ServiceClient(
    service='CampaignManagementService',
    version=13,
    authorization_data=authorization_data
)

# Get campaigns
response = campaign_service.GetCampaignsByAccountId(
    AccountId=account_id,
    CampaignType='Search Shopping Audience'
)

for campaign in response.Campaigns.Campaign:
    print(f"Campaign ID: {campaign.Id}, Name: {campaign.Name}")
```

### Pagination

The Campaign Management API uses index-based pagination:

- `PageInfo.Index`: Page number (0-based)
- `PageInfo.Size`: Number of records per page (max 1000)

**Example:**
```xml
<v13:PageInfo>
  <v13:Index>0</v13:Index>
  <v13:Size>1000</v13:Size>
</v13:PageInfo>
```

### Deleted Records

**Handling Deletions:**
- Campaign Management API: Filter by `Status` to include `Deleted` entities
- Bulk API: Downloaded entities include status field with `Deleted` value
- Reports: No direct delete tracking; use snapshots and compare

### Rate Limits

Microsoft Advertising API has the following limits:

| Limit Type | Value | Notes |
|------------|-------|-------|
| Calls per minute | TBD: Not explicitly documented | Varies by operation |
| Concurrent report requests | 5 | Per customer |
| Bulk download concurrent requests | TBD: Not explicitly documented | - |
| Maximum entities per bulk upload | 4,000,000 | Per file |

**Rate Limit Headers:**
The API returns HTTP 429 when rate limited. Implement exponential backoff.

**Best Practices:**
- Use Bulk API for large data retrieval instead of many Campaign Management calls
- Cache access tokens (valid for ~1 hour)
- Use appropriate polling intervals for async operations (start at 5 seconds)

---

## **Field Type Mapping**

| API Type | Description | Spark Type |
|----------|-------------|------------|
| `long` | 64-bit integer | `LongType` |
| `int` | 32-bit integer | `IntegerType` |
| `double` | Floating point | `DoubleType` |
| `decimal` | Precise decimal | `DecimalType` |
| `string` | Text | `StringType` |
| `boolean` | True/False | `BooleanType` |
| `dateTime` | ISO 8601 datetime | `TimestampType` |
| `Date` | Date object (Day, Month, Year) | `DateType` |
| `ArrayOflong` | Array of longs | `ArrayType(LongType)` |
| `ArrayOfstring` | Array of strings | `ArrayType(StringType)` |

### Enumeration Types

| Enum | Values |
|------|--------|
| `CampaignStatus` | Active, Paused, Deleted |
| `AdGroupStatus` | Active, Paused, Expired, Deleted |
| `KeywordStatus` | Active, Paused, Deleted |
| `MatchType` | Exact, Phrase, Broad |
| `CampaignType` | Search, Shopping, Audience, DynamicSearchAds, Hotel, PerformanceMax |
| `BudgetLimitType` | DailyBudgetAccelerated, DailyBudgetStandard |
| `Network` | OwnedAndOperatedAndSyndicatedSearch, OwnedAndOperatedOnly, SyndicatedSearchOnly |

### Special Field Behaviors

| Field | Behavior |
|-------|----------|
| `Id` | Auto-generated on creation; immutable |
| `Status` | Can be updated; `Deleted` is permanent |
| `TrackingUrlTemplate` | Inherited from parent if not set |
| `TimeStamp` | Used for optimistic concurrency; must pass on updates |

---

## **Known Quirks and Limitations**

1. **SOAP-Only API**: Unlike most modern APIs, Bing Ads uses SOAP/XML, not REST/JSON. The Python SDK abstracts this complexity.

2. **Async Operations**: Bulk downloads and report generation are asynchronous. Must poll for completion.

3. **Time Zone Considerations**: Reports and metrics are based on the account's time zone setting.

4. **Data Freshness**: Performance data may have a delay of 2-3 hours. Conversion data may take up to 24 hours.

5. **Sandbox Limitations**: Sandbox environment has limited functionality and separate credentials.

6. **Multi-Factor Authentication**: Required since June 2022. Initial OAuth consent must include MFA.

7. **Developer Token**: Required for all API calls; separate from OAuth tokens.

---

## **Sources and References**

### Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://learn.microsoft.com/en-us/advertising/guides/?view=bingads-13 | 2026-01-06 | High | API overview, services, endpoints |
| Official Docs | https://learn.microsoft.com/en-us/advertising/guides/authentication-oauth-quick-start?view=bingads-13 | 2026-01-06 | High | OAuth flow, token endpoints, headers |
| Official Docs | https://learn.microsoft.com/en-us/advertising/guides/web-service-addresses?view=bingads-13 | 2026-01-06 | High | Service endpoints for all APIs |
| Official Docs | https://learn.microsoft.com/en-us/advertising/guides/client-libraries?view=bingads-13 | 2026-01-06 | High | SDK availability, Python SDK details |
| Official Docs | https://learn.microsoft.com/en-us/advertising/guides/get-started?view=bingads-13 | 2026-01-06 | High | Getting started, developer token |
| Official Docs | https://learn.microsoft.com/en-us/advertising/guides/get-started-python?view=bingads-13 | 2026-01-06 | High | Python SDK setup and usage |
| Official Docs | https://learn.microsoft.com/en-us/advertising/guides/technical-guides?view=bingads-13 | 2026-01-06 | High | Technical guides for all services |
| Official Docs | https://learn.microsoft.com/en-us/advertising/guides/code-examples?view=bingads-13 | 2026-01-06 | High | Code examples for common operations |

### Notes on Sources

- All information sourced exclusively from official Microsoft Learn documentation
- No third-party implementations (Airbyte, Singer, etc.) were referenced
- Schema details derived from WSDL specifications referenced in official docs
- Rate limits marked as `TBD:` where not explicitly documented in official sources

