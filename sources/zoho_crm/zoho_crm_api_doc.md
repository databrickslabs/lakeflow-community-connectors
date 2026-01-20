# **Zoho CRM API v8 Documentation**

## **Authorization**

- **Chosen method**: OAuth 2.0 with Client Credentials (Refresh Token flow)
- **Base URL**: `https://www.zohoapis.com/crm/v8`
- **Auth placement**:
  - HTTP header: `Authorization: Zoho-oauthtoken <access_token>`
  - The connector stores `client_id`, `client_secret`, and `refresh_token`, and exchanges them for a short-lived `access_token` at runtime
  - Access tokens are valid for 1 hour (3600 seconds)
  - Refresh tokens are long-lived and used to obtain new access tokens

**OAuth 2.0 Flow Overview**:
1. Register your application in the Zoho API Console (https://api-console.zoho.com/)
2. Generate a **Grant Token** (authorization code) with appropriate scopes through Zoho's OAuth consent screen
   - **Critical**: The grant token is only valid for **2 minutes** (per https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html) and must be exchanged quickly for a refresh token
3. Exchange the grant token for `access_token` and `refresh_token` immediately
4. Connector stores `client_id`, `client_secret`, and `refresh_token`
5. At runtime, connector exchanges `refresh_token` for a new `access_token` before making API calls

**Token endpoints**:
- **Authorization/Grant Token Generation**: `https://accounts.zoho.com/` (or region-specific: `.eu`, `.in`, `.com.au`, `.com.cn`, `.jp`)
- **Token Exchange**: `https://accounts.zoho.com/oauth/v2/token` (POST)

Example token refresh request:

```bash
curl -X POST "https://accounts.zoho.com/oauth/v2/token" \
  -d "refresh_token=<REFRESH_TOKEN>" \
  -d "client_id=<CLIENT_ID>" \
  -d "client_secret=<CLIENT_SECRET>" \
  -d "grant_type=refresh_token"
```

Example token refresh response:

```json
{
  "access_token": "1000.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "api_domain": "https://www.zohoapis.com",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

Example authenticated API request:

```bash
curl -X GET \
  -H "Authorization: Zoho-oauthtoken 1000.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" \
  "https://www.zohoapis.com/crm/v8/Leads?per_page=10"
```

**Required OAuth Scopes**:
- `ZohoCRM.modules.ALL` - Full access to all modules (read and write)
- `ZohoCRM.settings.ALL` - Access to settings and metadata
- `ZohoCRM.settings.modules.ALL` - Access to module metadata (required for dynamic schema discovery)
- For read-only access: `ZohoCRM.modules.READ`
- For specific modules: `ZohoCRM.modules.{module_name}.READ` or `ZohoCRM.modules.{module_name}.ALL`

**Recommended scope configuration for connectors**:
```
ZohoCRM.modules.ALL, ZohoCRM.settings.ALL, ZohoCRM.settings.modules.ALL
```

**Important**: The scope must be specified when generating the grant token. Make sure the scope covers all modules and metadata endpoints needed for schema discovery and data sync. Some modules may not be available if the scope is insufficient or if your Zoho CRM edition doesn't support them.

**Other supported methods (not used by this connector)**:
- Zoho CRM also supports self-client OAuth for internal apps, but the connector will use the standard OAuth 2.0 flow with client credentials and refresh tokens only.

Notes:
- Access tokens expire after 1 hour and must be refreshed using the refresh token
- Refresh tokens do not expire but can be revoked by users
- The `api_domain` in the token response indicates the correct API endpoint (may vary by data center: `.com`, `.eu`, `.in`, etc.)
- All API requests must include the current valid access token in the `Authorization` header


## **Object List**

Zoho CRM organizes data into **modules** (equivalent to tables/objects in other systems). The module list can be retrieved dynamically via the Metadata API.

### Supported Tables (29 total)

These are the tables available for data ingestion through this connector:

| Module Name | API Name | Description | Ingestion Type |
|------------|----------|-------------|----------------|
| **Core CRM Objects** ||||
| Leads | `Leads` | Potential customers or contacts | `cdc` |
| Contacts | `Contacts` | Individual people associated with accounts | `cdc` |
| Accounts | `Accounts` | Companies or organizations | `cdc` |
| Deals | `Deals` | Sales opportunities | `cdc` |
| **Activities** ||||
| Tasks | `Tasks` | Activity tasks | `cdc` |
| Events | `Events` | Calendar events and meetings | `cdc` |
| Calls | `Calls` | Call logs | `cdc` |
| Notes | `Notes` | Notes attached to records | `cdc` |
| Attachments | `Attachments` | File attachments | `append` |
| **Products & Pricing** ||||
| Products | `Products` | Product catalog items | `cdc` |
| Vendors | `Vendors` | Supplier contacts | `cdc` |
| Price_Books | `Price_Books` | Pricing lists | `cdc` |
| **Sales Documents** ||||
| Quotes | `Quotes` | Price quotes sent to customers | `cdc` |
| Sales_Orders | `Sales_Orders` | Confirmed sales orders | `cdc` |
| Purchase_Orders | `Purchase_Orders` | Purchase orders to vendors | `cdc` |
| Invoices | `Invoices` | Billing invoices | `cdc` |
| **Line Items (Derived - extracted from parent subforms)** ||||
| Quoted_Items | `Quoted_Items` | Line items within Quotes | `snapshot` |
| Ordered_Items | `Ordered_Items` | Line items within Sales_Orders | `snapshot` |
| Invoiced_Items | `Invoiced_Items` | Line items within Invoices | `snapshot` |
| Purchase_Items | `Purchase_Items` | Line items within Purchase_Orders | `snapshot` |
| **Marketing & Support** ||||
| Campaigns | `Campaigns` | Marketing campaigns | `cdc` |
| Cases | `Cases` | Customer support cases | `cdc` |
| Solutions | `Solutions` | Knowledge base solutions | `cdc` |
| **Junction Tables (Derived - fetched via Related Records API)** ||||
| Campaigns_Leads | `Campaigns_Leads` | Many-to-many: Campaigns ↔ Leads | `snapshot` |
| Campaigns_Contacts | `Campaigns_Contacts` | Many-to-many: Campaigns ↔ Contacts | `snapshot` |
| Contacts_X_Deals | `Contacts_X_Deals` | Many-to-many: Contacts ↔ Deals with roles | `snapshot` |
| **Organization & Settings (Derived - fetched via Settings API)** ||||
| Users | `Users` | CRM users (requires `ZohoCRM.users.READ` scope) | `cdc` |
| Roles | `Roles` | User roles hierarchy | `snapshot` |
| Profiles | `Profiles` | Permission profiles | `snapshot` |

> **Note on Derived Tables**: Line Items, Junction Tables, and Organization tables are not standalone Zoho CRM modules.
> The connector constructs these by:
> - **Line Items**: Extracting subform data from parent records (Quotes, Sales_Orders, Invoices, Purchase_Orders)
> - **Junction Tables**: Calling the Related Records API for each parent record
> - **Organization Tables**: Using dedicated settings endpoints (`/crm/v8/users`, `/crm/v8/settings/roles`, `/crm/v8/settings/profiles`)

---

### All Modules from API (with exclusion reasons)

The `GET /crm/v8/settings/modules` endpoint returns all modules in Zoho CRM. Below is the complete list with status and reasons for exclusion:

| API Name | Status | Reason |
|----------|--------|--------|
| **Supported Data Modules** |||
| `Accounts` | ✅ Supported | Core CRM data |
| `Attachments` | ✅ Supported | File attachment records |
| `Calls` | ✅ Supported | Call activity logs |
| `Campaigns` | ✅ Supported | Marketing campaigns |
| `Cases` | ✅ Supported | Support tickets |
| `Contacts` | ✅ Supported | Contact records |
| `Deals` | ✅ Supported | Sales opportunities |
| `Events` | ✅ Supported | Calendar events |
| `Invoices` | ✅ Supported | Billing invoices |
| `Leads` | ✅ Supported | Lead records |
| `Notes` | ✅ Supported | Note records |
| `Price_Books` | ✅ Supported | Pricing lists |
| `Products` | ✅ Supported | Product catalog |
| `Purchase_Orders` | ✅ Supported | Purchase orders |
| `Quotes` | ✅ Supported | Price quotes |
| `Sales_Orders` | ✅ Supported | Sales orders |
| `Solutions` | ✅ Supported | Knowledge base |
| `Tasks` | ✅ Supported | Task activities |
| `Vendors` | ✅ Supported | Supplier contacts |
| **Subform Modules (handled as Derived tables)** |||
| `Invoiced_Items` | ✅ Derived | Extracted from Invoices subform |
| `Ordered_Items` | ✅ Derived | Extracted from Sales_Orders subform |
| `Purchase_Items` | ✅ Derived | Extracted from Purchase_Orders subform |
| `Quoted_Items` | ✅ Derived | Extracted from Quotes subform |
| **Excluded - No Fields / Empty Schema** |||
| `Actions_Performed` | ❌ Excluded | No fields available from API |
| `Visits` | ❌ Excluded | No fields available from API |
| **Excluded - Analytics Modules (different API structure)** |||
| `Analytics` | ❌ Excluded | Dashboard/reporting module, not data storage |
| `Email_Analytics` | ❌ Excluded | Email tracking analytics, requires different API |
| `Email_Sentiment` | ❌ Excluded | AI sentiment analysis, requires different API |
| `Email_Template_Analytics` | ❌ Excluded | Template performance metrics, requires different API |
| **Excluded - System/Internal Modules** |||
| `Locking_Information__s` | ❌ Excluded | System module, returns 403 Forbidden |
| `Unknown__s` | ❌ Excluded | Internal system module |
| **Excluded - UI/Navigation Modules (not data tables)** |||
| `Activities` | ❌ Excluded | Aggregate view of Tasks/Events/Calls, not separate data |
| `Feeds` | ❌ Excluded | Activity stream/social feed, not structured data |
| `Home` | ❌ Excluded | Dashboard/home page module |
| `Reports` | ❌ Excluded | Reporting engine, not data storage |
| `SalesInbox` | ❌ Excluded | Email inbox integration UI |
| `Social` | ❌ Excluded | Social media integration UI |
| **Excluded - Email System Modules** |||
| `Emails` | ❌ Excluded | Email records - complex structure, often requires Zoho Mail integration |
| `Email_Drafts__s` | ❌ Excluded | Draft emails - transient data |
| `Email_Template__s` | ❌ Excluded | Email templates - configuration, not transactional data |
| **Excluded - Forecasting Modules** |||
| `Forecast_Groups` | ❌ Excluded | Forecasting configuration |
| `Forecast_Items` | ❌ Excluded | Forecast line items - complex relationships |
| `Forecast_Quotas` | ❌ Excluded | Quota definitions |
| `Forecasts` | ❌ Excluded | Forecast summaries - aggregate data |
| **Excluded - Other Specialized Modules** |||
| `CalendarBookings__s` | ❌ Excluded | Calendar booking integration |
| `DealHistory` | ❌ Excluded | Deal audit trail - uses History API instead |
| `Documents` | ❌ Excluded | Document storage - uses Files API |

### Adding Support for Excluded Modules

If you need data from an excluded module, consider:

1. **Custom modules**: The connector automatically discovers and supports custom modules with `generated_type: "custom"`
2. **Feature request**: Open an issue to add support for specific modules
3. **Fork and extend**: Add the module to the connector's supported list

For modules with different API structures (Analytics, Forecasts, Emails), additional implementation work is required to handle their specific endpoints and data formats

**Retrieving the module list dynamically**:

- **Endpoint**: `GET https://www.zohoapis.com/crm/v8/settings/modules`
- **Purpose**: Fetch all available modules including custom modules
- **Use case**: Connectors can use this API to discover modules dynamically

Example request:

```bash
curl -X GET \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  "https://www.zohoapis.com/crm/v8/settings/modules"
```

Example response (truncated):

```json
{
  "modules": [
    {
      "id": "1234567000000002175",
      "api_name": "Leads",
      "module_name": "Leads",
      "singular_label": "Lead",
      "plural_label": "Leads",
      "generated_type": "default",
      "creatable": true,
      "editable": true,
      "deletable": true,
      "viewable": true,
      "api_supported": true
    },
    {
      "id": "1234567000000002179",
      "api_name": "Contacts",
      "module_name": "Contacts",
      "singular_label": "Contact",
      "plural_label": "Contacts",
      "generated_type": "default",
      "creatable": true,
      "editable": true,
      "deletable": true,
      "viewable": true,
      "api_supported": true
    },
    {
      "id": "1234567000000998877",
      "api_name": "Custom_Module_1",
      "module_name": "Custom Module 1",
      "singular_label": "Custom Record",
      "plural_label": "Custom Records",
      "generated_type": "custom",
      "creatable": true,
      "editable": true,
      "deletable": true,
      "viewable": true,
      "api_supported": true
    }
  ]
}
```

**Notes**:
- Custom modules created by users will also appear in the module list
- The `api_name` field is used in API endpoints
- Not all modules are API-accessible (`api_supported: false` for some system modules)
- **Important**: The `generated_type` field indicates module type:
  - `default`: Standard Zoho CRM modules
  - `custom`: User-created custom modules
  - `web_tab`: Modules created from web tabs
  - `subform`: Subform modules (nested within other modules)
- **Best practice**: When filtering modules for sync, only include modules where `generated_type = "custom"` or `generated_type = "default"` and `api_supported = true`


## **Object Schema**

Field metadata for each module can be retrieved dynamically using the Fields Metadata API. This provides complete schema information including field types, constraints, and relationships.

**Endpoint**: `GET https://www.zohoapis.com/crm/v8/settings/fields?module={module_api_name}`

**Path parameters**:
- `module` (string, required): The API name of the module (e.g., `Leads`, `Contacts`, `Accounts`)

Example request:

```bash
curl -X GET \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  "https://www.zohoapis.com/crm/v8/settings/fields?module=Leads"
```

Example response (truncated for key fields):

```json
{
  "fields": [
    {
      "id": "1234567000000000001",
      "api_name": "id",
      "field_label": "Lead ID",
      "data_type": "bigint",
      "length": 19,
      "read_only": true,
      "system_mandatory": true
    },
    {
      "id": "1234567000000000003",
      "api_name": "Company",
      "field_label": "Company",
      "data_type": "text",
      "length": 100,
      "required": true
    },
    {
      "id": "1234567000000000005",
      "api_name": "First_Name",
      "field_label": "First Name",
      "data_type": "text",
      "length": 40
    },
    {
      "id": "1234567000000000007",
      "api_name": "Last_Name",
      "field_label": "Last Name",
      "data_type": "text",
      "length": 40,
      "required": true
    },
    {
      "id": "1234567000000000009",
      "api_name": "Email",
      "field_label": "Email",
      "data_type": "email",
      "length": 100
    },
    {
      "id": "1234567000000000011",
      "api_name": "Phone",
      "field_label": "Phone",
      "data_type": "phone",
      "length": 30
    },
    {
      "id": "1234567000000000013",
      "api_name": "Lead_Status",
      "field_label": "Lead Status",
      "data_type": "picklist",
      "pick_list_values": [
        {
          "display_value": "Attempted to Contact",
          "actual_value": "Attempted to Contact"
        },
        {
          "display_value": "Contact in Future",
          "actual_value": "Contact in Future"
        },
        {
          "display_value": "Contacted",
          "actual_value": "Contacted"
        },
        {
          "display_value": "Junk Lead",
          "actual_value": "Junk Lead"
        },
        {
          "display_value": "Lost Lead",
          "actual_value": "Lost Lead"
        },
        {
          "display_value": "Not Contacted",
          "actual_value": "Not Contacted"
        },
        {
          "display_value": "Pre-Qualified",
          "actual_value": "Pre-Qualified"
        },
        {
          "display_value": "Qualified",
          "actual_value": "Qualified"
        }
      ]
    },
    {
      "id": "1234567000000000015",
      "api_name": "Lead_Source",
      "field_label": "Lead Source",
      "data_type": "picklist",
      "pick_list_values": [
        {
          "display_value": "Advertisement",
          "actual_value": "Advertisement"
        },
        {
          "display_value": "Employee Referral",
          "actual_value": "Employee Referral"
        },
        {
          "display_value": "External Referral",
          "actual_value": "External Referral"
        },
        {
          "display_value": "Online Store",
          "actual_value": "Online Store"
        },
        {
          "display_value": "Partner",
          "actual_value": "Partner"
        },
        {
          "display_value": "Public Relations",
          "actual_value": "Public Relations"
        },
        {
          "display_value": "Sales Email Alias",
          "actual_value": "Sales Email Alias"
        },
        {
          "display_value": "Seminar Partner",
          "actual_value": "Seminar Partner"
        },
        {
          "display_value": "Trade Show",
          "actual_value": "Trade Show"
        },
        {
          "display_value": "Web Download",
          "actual_value": "Web Download"
        },
        {
          "display_value": "Web Research",
          "actual_value": "Web Research"
        },
        {
          "display_value": "Webinar",
          "actual_value": "Webinar"
        }
      ]
    },
    {
      "id": "1234567000000000017",
      "api_name": "Owner",
      "field_label": "Lead Owner",
      "data_type": "lookup",
      "lookup": {
        "module": "users",
        "id": "1234567000000000001",
        "name": "users"
      }
    },
    {
      "id": "1234567000000000019",
      "api_name": "Created_Time",
      "field_label": "Created Time",
      "data_type": "datetime",
      "read_only": true,
      "system_mandatory": true
    },
    {
      "id": "1234567000000000021",
      "api_name": "Modified_Time",
      "field_label": "Modified Time",
      "data_type": "datetime",
      "read_only": true,
      "system_mandatory": true
    },
    {
      "id": "1234567000000000023",
      "api_name": "Created_By",
      "field_label": "Created By",
      "data_type": "lookup",
      "lookup": {
        "module": "users"
      },
      "read_only": true
    },
    {
      "id": "1234567000000000025",
      "api_name": "Modified_By",
      "field_label": "Modified By",
      "data_type": "lookup",
      "lookup": {
        "module": "users"
      },
      "read_only": true
    },
    {
      "id": "1234567000000000027",
      "api_name": "Annual_Revenue",
      "field_label": "Annual Revenue",
      "data_type": "currency",
      "precision": 9
    },
    {
      "id": "1234567000000000029",
      "api_name": "Number_Of_Employees",
      "field_label": "No of Employees",
      "data_type": "integer"
    },
    {
      "id": "1234567000000000031",
      "api_name": "Website",
      "field_label": "Website",
      "data_type": "website",
      "length": 255
    },
    {
      "id": "1234567000000000033",
      "api_name": "Converted",
      "field_label": "Converted",
      "data_type": "boolean"
    },
    {
      "id": "1234567000000000035",
      "api_name": "Converted_Account",
      "field_label": "Converted Account",
      "data_type": "lookup",
      "lookup": {
        "module": "Accounts"
      },
      "read_only": true
    },
    {
      "id": "1234567000000000037",
      "api_name": "Converted_Contact",
      "field_label": "Converted Contact",
      "data_type": "lookup",
      "lookup": {
        "module": "Contacts"
      },
      "read_only": true
    },
    {
      "id": "1234567000000000039",
      "api_name": "Converted_Deal",
      "field_label": "Converted Deal",
      "data_type": "lookup",
      "lookup": {
        "module": "Deals"
      },
      "read_only": true
    },
    {
      "id": "1234567000000000041",
      "api_name": "Street",
      "field_label": "Street",
      "data_type": "text",
      "length": 250
    },
    {
      "id": "1234567000000000043",
      "api_name": "City",
      "field_label": "City",
      "data_type": "text",
      "length": 100
    },
    {
      "id": "1234567000000000045",
      "api_name": "State",
      "field_label": "State",
      "data_type": "text",
      "length": 100
    },
    {
      "id": "1234567000000000047",
      "api_name": "Zip_Code",
      "field_label": "Zip Code",
      "data_type": "text",
      "length": 30
    },
    {
      "id": "1234567000000000049",
      "api_name": "Country",
      "field_label": "Country",
      "data_type": "text",
      "length": 100
    },
    {
      "id": "1234567000000000051",
      "api_name": "Description",
      "field_label": "Description",
      "data_type": "textarea"
    },
    {
      "id": "1234567000000000053",
      "api_name": "Rating",
      "field_label": "Rating",
      "data_type": "picklist",
      "pick_list_values": [
        {
          "display_value": "Acquired",
          "actual_value": "Acquired"
        },
        {
          "display_value": "Active",
          "actual_value": "Active"
        },
        {
          "display_value": "Market Failed",
          "actual_value": "Market Failed"
        },
        {
          "display_value": "Project Cancelled",
          "actual_value": "Project Cancelled"
        },
        {
          "display_value": "Shut Down",
          "actual_value": "Shut Down"
        }
      ]
    },
    {
      "id": "1234567000000000055",
      "api_name": "Industry",
      "field_label": "Industry",
      "data_type": "picklist",
      "pick_list_values": [
        {
          "display_value": "ASP",
          "actual_value": "ASP"
        },
        {
          "display_value": "Data/Telecom OEM",
          "actual_value": "Data/Telecom OEM"
        },
        {
          "display_value": "ERP",
          "actual_value": "ERP"
        },
        {
          "display_value": "Government/Military",
          "actual_value": "Government/Military"
        },
        {
          "display_value": "Large Enterprise",
          "actual_value": "Large Enterprise"
        },
        {
          "display_value": "ManagementISV",
          "actual_value": "ManagementISV"
        },
        {
          "display_value": "MSP",
          "actual_value": "MSP"
        },
        {
          "display_value": "Network Equipment Enterprise",
          "actual_value": "Network Equipment Enterprise"
        },
        {
          "display_value": "Non-management ISV",
          "actual_value": "Non-management ISV"
        },
        {
          "display_value": "Optical Networking",
          "actual_value": "Optical Networking"
        },
        {
          "display_value": "Service Provider",
          "actual_value": "Service Provider"
        },
        {
          "display_value": "Small/Medium Enterprise",
          "actual_value": "Small/Medium Enterprise"
        },
        {
          "display_value": "Storage Equipment",
          "actual_value": "Storage Equipment"
        },
        {
          "display_value": "Storage Service Provider",
          "actual_value": "Storage Service Provider"
        },
        {
          "display_value": "Systems Integrator",
          "actual_value": "Systems Integrator"
        },
        {
          "display_value": "Wireless Industry",
          "actual_value": "Wireless Industry"
        }
      ]
    },
    {
      "id": "1234567000000000057",
      "api_name": "Skype_ID",
      "field_label": "Skype ID",
      "data_type": "text",
      "length": 50
    },
    {
      "id": "1234567000000000059",
      "api_name": "Secondary_Email",
      "field_label": "Secondary Email",
      "data_type": "email",
      "length": 100
    },
    {
      "id": "1234567000000000061",
      "api_name": "Twitter",
      "field_label": "Twitter",
      "data_type": "text",
      "length": 50
    },
    {
      "id": "1234567000000000063",
      "api_name": "Tag",
      "field_label": "Tags",
      "data_type": "text",
      "json_type": "jsonarray"
    }
  ]
}
```

**Key field attributes**:
- `api_name`: Field name used in API requests/responses
- `data_type`: Field type (see Field Type Mapping section)
- `required`: Whether the field is mandatory
- `read_only`: Whether the field is read-only
- `system_mandatory`: System field that cannot be modified
- `length`: Maximum length for text fields
- `precision`: Precision for numeric fields
- `pick_list_values`: Enumeration values for picklist fields
- `lookup`: Related module information for lookup fields
- `json_type`: Special JSON representation (e.g., arrays)

**Notes**:
- Custom fields created by users will also appear in the field list
- Field schemas can vary between Zoho CRM instances based on customizations
- The connector should dynamically retrieve and adapt to the schema


## **Get Object Primary Keys**

Zoho CRM uses a standard primary key field named `id` for all modules. This is a system-mandatory field that uniquely identifies each record.

**Primary key field**:
- **API name**: `id`
- **Data type**: `bigint` (64-bit integer represented as string in JSON)
- **Properties**: Read-only, system-mandatory, unique, auto-generated

The `id` field is:
- Automatically assigned when a record is created
- Globally unique within the module
- Immutable (never changes for the lifetime of the record)
- Used for all update and delete operations

Example record showing the primary key:

```json
{
  "data": [
    {
      "id": "1234567000000123456",
      "Company": "Acme Corp",
      "Last_Name": "Smith",
      "First_Name": "John",
      "Email": "john.smith@acme.com",
      "Created_Time": "2024-01-15T10:30:00+00:00",
      "Modified_Time": "2024-01-20T14:45:00+00:00"
    }
  ]
}
```

**Primary keys for all modules**:
- All standard and custom modules use `id` as the primary key
- The field is consistent across all modules
- No API call is needed to determine the primary key; it is always `id`

**Composite keys**:
- Zoho CRM does not use composite primary keys
- Each record has a single unique `id` field


## **Object's ingestion type**

Zoho CRM supports incremental data synchronization through the use of timestamp-based cursors and deleted records tracking.

**Ingestion types by module**:

| Module | Ingestion Type | Rationale |
|--------|----------------|-----------|
| Leads | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Contacts | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Accounts | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Deals | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Tasks | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Events | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Calls | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Products | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Quotes | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Sales_Orders | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Purchase_Orders | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Invoices | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Campaigns | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Vendors | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Price_Books | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Cases | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Solutions | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Notes | `cdc` | Supports incremental reads using `Modified_Time` cursor and deleted records API |
| Attachments | `append` | Attachments are typically append-only with limited update support |

**CDC (Change Data Capture) implementation**:

For modules with `cdc` ingestion type:

- **Primary key**: `id` (bigint)
- **Cursor field**: `Modified_Time` (datetime with timezone)
- **Sort order**: Records are retrieved sorted by `Modified_Time` in ascending order
- **Lookback window**: Recommended 5-minute lookback to account for clock skew and late-arriving updates
- **Deleted records**: Retrieved separately using the Deleted Records API

**Incremental sync strategy**:

1. **Initial sync**: 
   - Fetch all records without time filter, or
   - Use a configurable `start_date` as the initial cursor

2. **Subsequent syncs**:
   - Use the maximum `Modified_Time` from the previous sync (minus lookback window) as the new cursor
   - Query: `Modified_Time >= cursor_time`
   - Process all records with pagination until no more records are returned

3. **Deleted records**:
   - Call the Deleted Records API with time range to get IDs of deleted records
   - Process deletions separately in the CDC pipeline

**Example incremental query**:
```
GET https://www.zohoapis.com/crm/v8/Leads?fields=id,Company,Last_Name,First_Name,Email,Modified_Time&sort_order=asc&sort_by=Modified_Time&page=1&per_page=200&criteria=(Modified_Time:greater_equal:2024-01-20T14:45:00+00:00)
```


## **Read API for Data Retrieval**

### Primary read endpoint (Get Records)

- **HTTP method**: `GET`
- **Endpoint**: `/crm/v8/{module_api_name}`
- **Base URL**: `https://www.zohoapis.com`
- **Full URL example**: `https://www.zohoapis.com/crm/v8/Leads`

**Path parameters**:
- `module_api_name` (string, required): The API name of the module (e.g., `Leads`, `Contacts`, `Accounts`)

**Key query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `fields` | string (comma-separated) | no | all fields | Specify which fields to retrieve. Use comma-separated API names. If omitted, all fields are returned. |
| `page` | integer | no | 1 | Page number for pagination (1-indexed) |
| `per_page` | integer | no | 200 | Number of records per page. Maximum: 200. |
| `sort_order` | string | no | `desc` | Sort order: `asc` or `desc` |
| `sort_by` | string | no | `id` | Field name to sort by (e.g., `Modified_Time`, `Created_Time`, `id`) |
| `criteria` | string | no | none | Filter criteria in Zoho's filter syntax. Used for incremental reads. |
| `cvid` | string | no | none | Custom View ID to filter records by a pre-defined view |
| `territory_id` | string | no | none | Filter records by territory |
| `include_child` | boolean | no | false | Include records from child territories |
| `converted` | string | no | none | Filter by conversion status: `true`, `false`, or `both` (for Leads module) |
| `approved` | string | no | none | Filter by approval status: `true`, `false`, or `both` |

**Pagination strategy**:
- Zoho CRM uses page-based pagination with `page` and `per_page` parameters
- Maximum `per_page` value is 200
- The response includes an `info` object with pagination metadata:
  - `page`: Current page number
  - `per_page`: Records per page
  - `count`: Number of records in current page
  - `more_records`: Boolean indicating if there are more pages

**Pagination workflow**:
1. Start with `page=1` and `per_page=200`
2. Check `info.more_records` in the response
3. If `true`, increment `page` by 1 and fetch the next page
4. Continue until `info.more_records` is `false`

Example basic request:

```bash
curl -X GET \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  "https://www.zohoapis.com/crm/v8/Leads?per_page=200&page=1"
```

Example response:

```json
{
  "data": [
    {
      "id": "1234567000000123456",
      "Owner": {
        "name": "John Doe",
        "id": "1234567000000098765",
        "email": "john.doe@example.com"
      },
      "Company": "Acme Corporation",
      "Email": "contact@acme.com",
      "Description": "Potential enterprise client",
      "Rating": "Active",
      "Website": "https://www.acme.com",
      "Twitter": "acme_corp",
      "First_Name": "Jane",
      "Last_Name": "Smith",
      "Full_Name": "Jane Smith",
      "Lead_Status": "Qualified",
      "Industry": "Large Enterprise",
      "Lead_Source": "Web Research",
      "Phone": "+1-555-0100",
      "Mobile": "+1-555-0101",
      "Annual_Revenue": 5000000.0,
      "Number_Of_Employees": 250,
      "City": "San Francisco",
      "State": "California",
      "Country": "USA",
      "Zip_Code": "94102",
      "Street": "123 Market Street",
      "Created_Time": "2024-01-15T10:30:00+00:00",
      "Modified_Time": "2024-01-20T14:45:00+00:00",
      "Created_By": {
        "name": "Admin User",
        "id": "1234567000000098764",
        "email": "admin@example.com"
      },
      "Modified_By": {
        "name": "John Doe",
        "id": "1234567000000098765",
        "email": "john.doe@example.com"
      },
      "Tag": [
        "enterprise",
        "hot-lead"
      ]
    }
  ],
  "info": {
    "page": 1,
    "per_page": 200,
    "count": 1,
    "more_records": true
  }
}
```

**Incremental read using criteria parameter**:

For incremental syncs, use the `criteria` parameter with `Modified_Time`:

```bash
curl -X GET \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  "https://www.zohoapis.com/crm/v8/Leads?fields=id,Company,Last_Name,First_Name,Email,Modified_Time&sort_order=asc&sort_by=Modified_Time&per_page=200&page=1&criteria=(Modified_Time:greater_equal:2024-01-20T14:40:00%2B00:00)"
```

**Criteria syntax**:
- Format: `(field_name:operator:value)`
- Operators:
  - `equal`: Exact match
  - `not_equal`: Not equal
  - `greater_than`: Greater than
  - `greater_equal`: Greater than or equal
  - `less_than`: Less than
  - `less_equal`: Less than or equal
  - `contains`: String contains
  - `starts_with`: String starts with
- Date/time values must be URL-encoded and in ISO 8601 format with timezone
- Multiple criteria can be combined with `and` or `or`:
  - `(field1:equal:value1)and(field2:greater_than:value2)`

**Field selection**:

To retrieve specific fields only:

```bash
curl -X GET \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  "https://www.zohoapis.com/crm/v8/Leads?fields=id,Company,Last_Name,First_Name,Email,Phone,Modified_Time"
```

This improves performance and reduces bandwidth by fetching only required fields.

### Get a specific record by ID

- **HTTP method**: `GET`
- **Endpoint**: `/crm/v8/{module_api_name}/{record_id}`

Example request:

```bash
curl -X GET \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  "https://www.zohoapis.com/crm/v8/Leads/1234567000000123456"
```

### Get deleted records

- **HTTP method**: `GET`
- **Endpoint**: `/crm/v8/{module_api_name}/deleted`
- **Purpose**: Retrieve records that have been deleted (soft delete in recycle bin)

**Key query parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | no | `all` (default), `recycle`, or `permanent`. `recycle` shows records in recycle bin, `permanent` shows permanently deleted records. |
| `page` | integer | no | Page number (1-indexed) |
| `per_page` | integer | no | Number of records per page. Maximum: 200. |

Example request:

```bash
curl -X GET \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  "https://www.zohoapis.com/crm/v8/Leads/deleted?type=all&per_page=200&page=1"
```

Example response:

```json
{
  "data": [
    {
      "deleted_by": {
        "name": "John Doe",
        "id": "1234567000000098765"
      },
      "id": "1234567000000999888",
      "display_name": "Acme Lead",
      "type": "Leads",
      "created_by": {
        "name": "Admin User",
        "id": "1234567000000098764"
      },
      "deleted_time": "2024-01-21T09:15:00+00:00"
    }
  ],
  "info": {
    "page": 1,
    "per_page": 200,
    "count": 1,
    "more_records": false
  }
}
```

**Notes on deleted records**:
- Deleted records API returns only record IDs and metadata, not full record data
- Records remain in the recycle bin for a limited time before permanent deletion
- Connector should track deleted record IDs and process them as deletions in CDC pipeline
- Use `deleted_time` field to filter deleted records incrementally

### Alternative: Query API (COQL)

Zoho CRM also provides a Query API using COQL (CRM Object Query Language), which is SQL-like.

- **HTTP method**: `GET` or `POST`
- **Endpoint**: `/crm/v8/coql`
- **Use case**: Complex queries with multiple conditions, joins, and aggregations

Example COQL query:

```sql
SELECT id, Company, Last_Name, First_Name, Email, Modified_Time
FROM Leads
WHERE Modified_Time >= '2024-01-20T14:45:00+00:00'
ORDER BY Modified_Time ASC
LIMIT 200
OFFSET 0
```

POST request with COQL:

```bash
curl -X POST \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "select_query": "SELECT id, Company, Last_Name, First_Name, Email, Modified_Time FROM Leads WHERE Modified_Time >= '\''2024-01-20T14:45:00+00:00'\'' ORDER BY Modified_Time ASC LIMIT 200 OFFSET 0"
  }' \
  "https://www.zohoapis.com/crm/v8/coql"
```

**Notes on COQL**:
- More flexible than criteria-based filtering
- Supports JOIN operations for related modules
- Maximum 200 records per query (use OFFSET for pagination)
- Not all fields support filtering in COQL

### Rate Limits

Zoho CRM API has the following rate limits:

**API call limits** (varies by edition):

| Edition | API Calls per Day | API Calls per Minute |
|---------|-------------------|----------------------|
| Free | 5,000 | N/A |
| Standard | 10,000 | N/A |
| Professional | 15,000 | N/A |
| Enterprise | 25,000 | 60 |
| Ultimate | 100,000 | 120 |

**Additional limits**:
- Maximum 10 concurrent API requests per account
- Bulk Read API has separate limits (handled via job-based async processing)
- Rate limit headers are returned in API responses:
  - `X-RateLimit-Limit`: Maximum calls allowed
  - `X-RateLimit-Remaining`: Remaining calls in the current window
  - `X-RateLimit-Reset`: Timestamp when the limit resets

**Rate limit handling**:
- The connector should implement exponential backoff when rate limits are hit
- HTTP 429 (Too Many Requests) response indicates rate limit exceeded
- Track `X-RateLimit-Remaining` header to proactively throttle requests
- **Recommended strategy**: When API limit is exceeded, reschedule the sync by 1 hour to allow the daily quota to reset
- Error response when limit exceeded:
  ```json
  {
    "code": "RATE_LIMIT_EXCEEDED",
    "details": {},
    "message": "The API request limit has been exceeded.",
    "status": "error"
  }
  ```

**Understanding API Credits**:
- API calls in Zoho CRM consume **credits** from your daily quota (not just a simple call count)
- Each API operation may consume different amounts of credits:
  - Simple GET requests: 1 credit per call
  - Bulk operations: Credits based on number of records processed
  - Some metadata API calls: May be free or consume fewer credits
- **Critical**: If you exhaust your credits for a 24-hour window, all API access will be blocked until the quota resets
- **Credit reset timing**: Credits reset at midnight in your account's timezone
- Monitor the `X-RateLimit-Limit` and `X-RateLimit-Remaining` headers to track credit usage in real-time

**Best practices**:
- Use `per_page=200` to minimize API calls and credit consumption
- Use field selection (`fields` parameter) to reduce response size and processing overhead
- Implement request queuing and throttling based on edition limits
- Use Bulk Read API for large data exports (separate async API, not covered in detail here)
- Monitor daily credit consumption and adjust sync frequency accordingly
- Consider scheduling syncs during off-peak hours to preserve credits for interactive use


## **Field Type Mapping**

Zoho CRM field types map to standard data types as follows:

| Zoho CRM Data Type | Description | Connector Logical Type | JSON Representation | Notes |
|--------------------|-------------|------------------------|---------------------|-------|
| `bigint` | 64-bit integer | `long` | Number or String | Used for `id` fields. May be represented as string in JSON to prevent precision loss. |
| `text` | Short text string | `string` | String | Single-line text. Max length specified in field metadata. |
| `textarea` | Long text | `string` | String | Multi-line text. Max length specified in field metadata. |
| `email` | Email address | `string` | String | Validated email format. |
| `phone` | Phone number | `string` | String | Phone number format (not strictly validated). |
| `website` | URL | `string` | String | Website URL. |
| `picklist` | Single-select dropdown | `string` | String | One value from predefined list. |
| `multiselectpicklist` | Multi-select dropdown | `array<string>` | Array of strings | Multiple values from predefined list. |
| `integer` | 32-bit integer | `long` | Number | Whole numbers. Prefer `long` to avoid overflow. |
| `double` | Floating-point number | `double` | Number | Decimal numbers. |
| `currency` | Currency amount | `double` | Number | Monetary value. Precision specified in field metadata. |
| `percent` | Percentage | `double` | Number | Percentage value (0-100). |
| `boolean` | True/false | `boolean` | Boolean | `true` or `false`. |
| `date` | Date only | `date` | String (ISO 8601 date) | Format: `YYYY-MM-DD` |
| `datetime` | Date and time | `timestamp` | String (ISO 8601 datetime) | Format: `YYYY-MM-DDTHH:mm:ss+00:00` (UTC or with timezone) |
| `lookup` | Reference to another record | `struct` | Object | Contains `id` and `name` fields, plus other metadata. See nested structure below. |
| `ownerlookup` | Reference to user (owner) | `struct` | Object | Similar to lookup but references Users module. |
| `multiselectlookup` | Multi-reference lookup | `array<struct>` | Array of objects | Array of lookup objects. |
| `fileupload` | File attachment reference | `string` | String | File ID or URL. |
| `imageupload` | Image attachment reference | `string` | String | Image ID or URL. |
| `autonumber` | Auto-generated sequence | `string` | String | System-generated unique identifier (not the primary key). Note: Airbyte treats as big_integer when no prefix/suffix. |
| `formula` | Calculated field | varies | varies | Computed based on formula. Data type depends on formula result. |
| `subform` | Nested records | `array<struct>` | Array of objects | Array of nested objects with their own fields. |
| `consent_lookup` | GDPR consent reference | `struct` | Object | Reference to consent records. |
| `profileimage` | Profile image URL | `string` | String | URL to profile image. |
| `jsonarray` | JSON array | `array` | Array | Special JSON array type (e.g., Tags field). |
| `jsonobject` | JSON object | `struct` | Object | Embedded JSON object structure. |
| `multiselectlookup` | Multi-reference lookup | `array<struct>` | Array of objects | Array of lookup objects. |
| `event_reminder` | Event reminder | `string` | String | Event reminder configuration. |
| `RRULE` | Recurrence rule | `struct` | Object | iCalendar recurrence rule for recurring events. |
| `ALARM` | Event alarm | `struct` | Object | Event alarm/notification configuration. |

**Nested structures**:

**Lookup field structure**:
```json
{
  "Owner": {
    "name": "John Doe",
    "id": "1234567000000098765",
    "email": "john.doe@example.com"
  }
}
```

Connector schema for lookup fields:
- `struct` containing:
  - `id` (string/long): Record ID
  - `name` (string): Display name
  - Additional fields (e.g., `email` for user lookups)

**Multi-select picklist**:
```json
{
  "Categories": ["Category A", "Category B", "Category C"]
}
```

Connector schema: `array<string>`

**Subform structure**:
```json
{
  "Product_Details": [
    {
      "id": "1234567000000456789",
      "Product_Name": "Product A",
      "Quantity": 10,
      "List_Price": 100.00,
      "Total": 1000.00
    },
    {
      "id": "1234567000000456790",
      "Product_Name": "Product B",
      "Quantity": 5,
      "List_Price": 50.00,
      "Total": 250.00
    }
  ]
}
```

Connector schema: `array<struct>` with nested fields based on subform schema

**Tag field (special case)**:
```json
{
  "Tag": ["tag1", "tag2", "tag3"]
}
```

Connector schema: `array<string>` (even though metadata shows `json_type: jsonarray`)

**Handling null values**:
- Absent fields in JSON response should be treated as `null`
- Explicitly `null` fields should be preserved as `null`
- Do not convert `null` to empty string or empty object

**Special behaviors**:
- System fields (`id`, `Created_Time`, `Modified_Time`, `Created_By`, `Modified_By`) are read-only
- Lookup fields may contain partial data; only `id` and `name` are guaranteed
- Multi-currency fields may include additional currency metadata if multi-currency is enabled
- Formula fields are computed; values cannot be set via API


## **Write API**

Zoho CRM supports create, update, and upsert operations for records.

### Create records (Insert)

- **HTTP method**: `POST`
- **Endpoint**: `/crm/v8/{module_api_name}`
- **Request body**: JSON array of records to create

**Request body format**:

```json
{
  "data": [
    {
      "Company": "Example Corp",
      "Last_Name": "Johnson",
      "First_Name": "Alice",
      "Email": "alice.johnson@example.com",
      "Phone": "+1-555-0200",
      "Lead_Status": "Not Contacted",
      "Lead_Source": "Web Research"
    }
  ]
}
```

**Request parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `trigger` | array of strings | Workflow triggers to execute: `workflow`, `approval`, `blueprint` |

Example create request:

```bash
curl -X POST \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {
        "Company": "Example Corp",
        "Last_Name": "Johnson",
        "First_Name": "Alice",
        "Email": "alice.johnson@example.com",
        "Phone": "+1-555-0200",
        "Lead_Status": "Not Contacted",
        "Lead_Source": "Web Research"
      }
    ]
  }' \
  "https://www.zohoapis.com/crm/v8/Leads"
```

Example response:

```json
{
  "data": [
    {
      "code": "SUCCESS",
      "details": {
        "Modified_Time": "2024-01-22T10:15:00+00:00",
        "Modified_By": {
          "name": "API User",
          "id": "1234567000000098765"
        },
        "Created_Time": "2024-01-22T10:15:00+00:00",
        "id": "1234567000001111111",
        "Created_By": {
          "name": "API User",
          "id": "1234567000000098765"
        }
      },
      "message": "record added",
      "status": "success"
    }
  ]
}
```

**Batch create**:
- Up to 100 records can be created in a single API call
- Each record in the `data` array is processed independently
- Response includes status for each record

### Update records

- **HTTP method**: `PUT`
- **Endpoint**: `/crm/v8/{module_api_name}`
- **Request body**: JSON array of records to update (must include `id` field)

**Request body format**:

```json
{
  "data": [
    {
      "id": "1234567000001111111",
      "Lead_Status": "Contacted",
      "Phone": "+1-555-0201"
    }
  ]
}
```

Example update request:

```bash
curl -X PUT \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {
        "id": "1234567000001111111",
        "Lead_Status": "Contacted",
        "Phone": "+1-555-0201"
      }
    ]
  }' \
  "https://www.zohoapis.com/crm/v8/Leads"
```

Example response:

```json
{
  "data": [
    {
      "code": "SUCCESS",
      "details": {
        "Modified_Time": "2024-01-22T11:30:00+00:00",
        "Modified_By": {
          "name": "API User",
          "id": "1234567000000098765"
        },
        "Created_Time": "2024-01-22T10:15:00+00:00",
        "id": "1234567000001111111",
        "Created_By": {
          "name": "API User",
          "id": "1234567000000098765"
        }
      },
      "message": "record updated",
      "status": "success"
    }
  ]
}
```

**Batch update**:
- Up to 100 records can be updated in a single API call
- Only specified fields are updated; unspecified fields remain unchanged
- `id` field is required for each record

### Update a specific record

- **HTTP method**: `PUT`
- **Endpoint**: `/crm/v8/{module_api_name}/{record_id}`
- **Request body**: JSON object with fields to update

Example request:

```bash
curl -X PUT \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {
        "Lead_Status": "Qualified",
        "Rating": "Active"
      }
    ]
  }' \
  "https://www.zohoapis.com/crm/v8/Leads/1234567000001111111"
```

### Upsert records

Zoho CRM supports upsert operations based on duplicate check fields.

- **HTTP method**: `POST`
- **Endpoint**: `/crm/v8/{module_api_name}/upsert`
- **Request body**: JSON array of records
- **Query parameters**:
  - `duplicate_check_fields`: Comma-separated list of field API names to use for duplicate detection

Example upsert request:

```bash
curl -X POST \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {
        "Email": "alice.johnson@example.com",
        "Company": "Example Corp",
        "Last_Name": "Johnson",
        "First_Name": "Alice",
        "Phone": "+1-555-0202"
      }
    ]
  }' \
  "https://www.zohoapis.com/crm/v8/Leads/upsert?duplicate_check_fields=Email"
```

**Upsert behavior**:
- If a record with matching duplicate check fields exists, it is updated
- If no match is found, a new record is created
- Duplicate check fields must be marked as unique fields in Zoho CRM settings

### Delete records

- **HTTP method**: `DELETE`
- **Endpoint**: `/crm/v8/{module_api_name}?ids={comma_separated_ids}`
- **Query parameters**:
  - `ids`: Comma-separated list of record IDs to delete (up to 100)

Example delete request:

```bash
curl -X DELETE \
  -H "Authorization: Zoho-oauthtoken <ACCESS_TOKEN>" \
  "https://www.zohoapis.com/crm/v8/Leads?ids=1234567000001111111,1234567000001111112"
```

Example response:

```json
{
  "data": [
    {
      "code": "SUCCESS",
      "details": {
        "id": "1234567000001111111"
      },
      "message": "record deleted",
      "status": "success"
    }
  ]
}
```

**Delete behavior**:
- Deleted records are moved to the recycle bin (soft delete)
- Records can be restored from the recycle bin using the Restore API
- Permanently delete records by emptying the recycle bin

### Validation and error handling

**Validation**:
- Required fields must be provided for create operations
- Field values must match the field type and constraints
- Lookup fields must reference existing records
- Picklist values must be from the predefined list

**Common error responses**:

```json
{
  "data": [
    {
      "code": "MANDATORY_NOT_FOUND",
      "details": {
        "api_name": "Last_Name"
      },
      "message": "required field not found",
      "status": "error"
    }
  ]
}
```

**Error codes**:
- `MANDATORY_NOT_FOUND`: Required field missing
- `INVALID_DATA`: Field value violates constraints
- `DUPLICATE_DATA`: Duplicate record based on unique fields
- `INVALID_MODULE`: Module name is invalid
- `INVALID_REQUEST`: Malformed request
- `AUTHENTICATION_FAILURE`: Invalid or expired access token
- `OAUTH_SCOPE_MISMATCH`: Insufficient OAuth scopes


## **Known Quirks & Edge Cases**

- **Token expiration and grant token time limit**: 
  - Access tokens expire after 1 hour. The connector must handle token refresh transparently before making API calls. Monitor for HTTP 401 responses and refresh proactively based on `expires_in` value.
  - **Critical**: The initial grant token (authorization code) obtained from Zoho's OAuth console is only valid for **2 minutes** (per https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html). You must exchange it for a refresh token immediately. If the grant token expires, you'll need to generate a new one.
  - Best practice: Have your token exchange command ready before generating the grant token to complete the exchange within the 2-minute window.

- **Data center-specific domains**: Zoho CRM uses different API domains based on the data center and environment:

**Production environments**:
  - US: `https://www.zohoapis.com`
  - EU: `https://www.zohoapis.eu`
  - IN: `https://www.zohoapis.in`
  - AU: `https://www.zohoapis.com.au`
  - CN: `https://www.zohoapis.com.cn`
  - JP: `https://www.zohoapis.jp`

**Sandbox environments**:
  - US: `https://sandbox.zohoapis.com`
  - EU: `https://sandbox.zohoapis.eu`
  - IN: `https://sandbox.zohoapis.in`
  - AU: `https://sandbox.zohoapis.com.au`
  - CN: `https://sandbox.zohoapis.com.cn`
  - JP: `https://sandbox.zohoapis.jp`

**Developer environments**:
  - US: `https://developer.zohoapis.com`
  - EU: `https://developer.zohoapis.eu`
  - IN: `https://developer.zohoapis.in`
  - AU: `https://developer.zohoapis.com.au`
  - CN: `https://developer.zohoapis.com.cn`
  - JP: `https://developer.zohoapis.jp`

**Important notes**:
  - The correct domain is returned in the OAuth token response (`api_domain` field)
  - **Developer environment warning**: The Zoho Developer environment API is inconsistent with the production environment API. It contains approximately half of the modules supported in production. Use developer environment only for testing, not for production data sync.

- **Field visibility and permissions**: Not all fields may be visible to all users. Field access depends on:
  - User role and profile permissions
  - Field-level security settings
  - The connector should handle missing fields gracefully

- **Custom modules and fields**: Zoho CRM allows extensive customization:
  - Custom modules behave like standard modules
  - Custom field API names are typically prefixed with the module name or have a special format
  - The connector must dynamically discover and adapt to custom schemas
  - **Important**: When syncing custom modules, filter by `generated_type = "custom"` from the modules metadata API
  - Only custom modules with this flag should be treated as user-created data modules
  - Other `generated_type` values (like `web_tab` or `subform`) may require special handling
  - **Dynamic schema discovery**: The connector should build schemas dynamically using:
    - Modules API: `GET /crm/v8/settings/modules` - to discover available modules
    - Modules Metadata API: Module-level metadata including fields list
    - Fields Metadata API: `GET /crm/v8/settings/fields?module={module_name}` - detailed field schemas
  - Schema discovery typically takes 10-30 seconds depending on the number of modules and fields

- **Lookup field partial data**: Lookup fields in API responses may contain only `id` and `name`. Additional fields (like `email` for user lookups) may not always be present. Do not assume all lookup fields are fully populated.

- **Multi-currency**: If multi-currency is enabled in Zoho CRM:
  - Currency fields include additional metadata (currency code, exchange rate)
  - The connector should handle currency fields as structured objects if multi-currency is detected
  - Default behavior treats currency as simple numeric value

- **Subform limitations and deletion tracking**: Subforms (nested line items) have significant limitations:
  - Maximum 200 subform records per parent record
  - Subform fields have their own schema that must be retrieved separately
  - Updating subforms requires sending the complete subform array (partial updates not supported)
  - **Critical limitation**: Zoho CRM API does not provide a way to track deleted subform line items incrementally
  - When a subform line item is deleted from a parent record, only the updated parent record is returned with the remaining subform items
  - **Workaround for tracking subform deletes**:
    - Periodically re-import the entire subform data (e.g., weekly full refresh)
    - Compare the current subform items with previously synced items to detect deletions
    - Track subform items by their `id` field (each subform line item has a unique ID)
    - Mark missing subform items as deleted in your destination
  - This limitation applies to all subform fields in modules like Quotes, Sales Orders, Purchase Orders, and Invoices

- **Deleted records in recycle bin**: Deleted records are retained in the recycle bin for a limited time (typically 60 days, configurable). After this period, they are permanently deleted and no longer appear in the deleted records API.

- **Modified_Time precision**: The `Modified_Time` field has second-level precision. Multiple records updated within the same second will have identical `Modified_Time` values. To handle this:
  - Use a combination of `Modified_Time` and `id` for strict ordering
  - Implement a small lookback window (5 minutes) to catch late updates

- **Rate limit variability and credit exhaustion**: Rate limits vary by Zoho CRM edition and can be changed by administrators. The connector should:
  - Check rate limit headers in responses
  - Implement adaptive throttling based on remaining quota
  - Handle HTTP 429 responses with exponential backoff
  - **Critical**: If credits are exhausted, all API calls will fail with `RATE_LIMIT_EXCEEDED` error until the 24-hour quota resets
  - Monitor credit consumption throughout the day to avoid complete exhaustion
  - Consider implementing a "credit reserve" strategy (e.g., pause syncing when only 10% of credits remain)

- **COQL limitations**: The COQL query API has limitations:
  - Not all fields support filtering
  - JOIN operations are limited to certain module relationships
  - Aggregate functions have limited support
  - For best compatibility, prefer the standard Get Records API with `criteria` parameter

- **Blueprints and workflows**: Zoho CRM supports complex business processes:
  - Blueprint transitions may enforce field requirements and validations beyond normal field rules
  - Workflow rules can automatically modify records after creation/update
  - The connector should read back records after write operations to capture workflow-induced changes

- **API versioning**: Zoho CRM API v8 is the latest version. Previous versions (v2, v6, v7) are still available but may have different behavior. This documentation covers v8 only. Ensure all API calls use the `/crm/v8/` path prefix.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/ | 2024-12-19 | High | API v8 overview, categories (Metadata, Core, Composite, Bulk, Notification, Query APIs) |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/authentication.html | 2024-12-19 | High | OAuth 2.0 authentication flow, token endpoints, refresh token mechanism, scopes |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/get-records.html | 2024-12-19 | High | Get Records API endpoint, pagination parameters, response format, field selection |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/insert-records.html | 2024-12-19 | High | POST endpoint for creating records, batch create support, request/response format |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/update-records.html | 2024-12-19 | High | PUT endpoint for updating records, batch update support |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/upsert-records.html | 2024-12-19 | High | Upsert API with duplicate check fields |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/delete-records.html | 2024-12-19 | High | DELETE endpoint, soft delete behavior, recycle bin |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/get-deleted-records.html | 2024-12-19 | High | Deleted records API, type parameter (recycle/permanent), deleted_time field |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/modules-api.html | 2024-12-19 | High | Get Modules metadata API, module list structure, api_name field |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/field-meta.html | 2024-12-19 | High | Fields metadata API, field attributes, data types, picklist values, lookup structure |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/api-limits.html | 2024-12-19 | High | Rate limits by edition, concurrent request limits, rate limit headers |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/query-api.html | 2024-12-19 | High | COQL query API, SQL-like syntax, limitations |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/scopes.html | 2024-12-19 | High | OAuth scope definitions, module-level permissions |
| Official Docs | https://www.zoho.com/crm/developer/docs/api/v8/data-centers.html | 2024-12-19 | High | Data center-specific API domains, regional endpoints |
| Reference Implementation | https://fivetran.com/docs/connectors/applications/zoho-crm | 2024-12-19 | High | Fivetran connector: confirmed capture deletes for all modules, subform re-import strategy (weekly), custom module filtering (generated_type=custom), API credit exhaustion handling, 1-hour reschedule strategy for rate limits |
| Reference Implementation | https://docs.airbyte.com/integrations/sources/zoho-crm | 2024-12-19 | High | Airbyte connector: confirmed dynamic schema discovery via Metadata APIs, sandbox/developer environment URLs, grant token expiration warning (2 minutes per official docs), comprehensive scope requirements (modules.ALL + settings.ALL + settings.modules.ALL), data type mappings including jsonarray/jsonobject/RRULE/ALARM types, developer environment limitation (half the modules of production) |


## **Sources and References**

- **Official Zoho CRM API v8 Documentation** (highest confidence)
  - Main documentation: https://www.zoho.com/crm/developer/docs/api/v8/
  - Authentication: https://www.zoho.com/crm/developer/docs/api/v8/authentication.html
  - Get Records: https://www.zoho.com/crm/developer/docs/api/v8/get-records.html
  - Insert Records: https://www.zoho.com/crm/developer/docs/api/v8/insert-records.html
  - Update Records: https://www.zoho.com/crm/developer/docs/api/v8/update-records.html
  - Upsert Records: https://www.zoho.com/crm/developer/docs/api/v8/upsert-records.html
  - Delete Records: https://www.zoho.com/crm/developer/docs/api/v8/delete-records.html
  - Get Deleted Records: https://www.zoho.com/crm/developer/docs/api/v8/get-deleted-records.html
  - Modules API: https://www.zoho.com/crm/developer/docs/api/v8/modules-api.html
  - Fields Metadata: https://www.zoho.com/crm/developer/docs/api/v8/field-meta.html
  - Query API (COQL): https://www.zoho.com/crm/developer/docs/api/v8/query-api.html
  - API Limits: https://www.zoho.com/crm/developer/docs/api/v8/api-limits.html
  - OAuth Scopes: https://www.zoho.com/crm/developer/docs/api/v8/scopes.html
  - Data Centers: https://www.zoho.com/crm/developer/docs/api/v8/data-centers.html
  - API References: https://www.zoho.com/crm/developer/docs/api/v8/api-references.html

- **Fivetran Zoho CRM Connector Documentation** (high confidence - reference implementation)
  - Main documentation: https://fivetran.com/docs/connectors/applications/zoho-crm
  - Setup guide: https://fivetran.com/docs/connectors/applications/zoho-crm/setup-guide
  - Used to validate practical implementation patterns: custom module filtering, subform deletion tracking workarounds, API credit management, and error handling strategies

- **Airbyte Zoho CRM Connector Documentation** (high confidence - reference implementation)
  - Main documentation: https://docs.airbyte.com/integrations/sources/zoho-crm
  - GitHub implementation: https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-zoho-crm
  - Used to validate: dynamic schema discovery approach, OAuth scope requirements, environment URLs (production/sandbox/developer), data type mappings, grant token expiration timing, developer environment limitations

All information in this document is primarily sourced from the official Zoho CRM API v8 documentation. Fivetran's and Airbyte's connector documentation were used to validate real-world implementation patterns, identify edge cases, and confirm best practices. When conflicts or ambiguities arise, the official documentation at https://www.zoho.com/crm/developer/docs/api/v8/ is treated as the authoritative source.

