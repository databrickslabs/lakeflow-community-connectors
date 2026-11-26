# **Visma e-conomic (economic) API Documentation**

Scope (16 tables): batch 1 = `employees`, `customers`, `projects`, `project_time_entries`; batch 2 (added 2026-11-02) = `activities`, `activity_groups`, `employee_groups`, `project_groups`, `project_statuses`, `mileages`, `project_employees`, `project_customers`, `time_entry_prices`, `cost_types`, `customer_contacts`, `customer_delivery_locations`.
API family used: the newer **e-conomic OpenAPI** family (`https://apis.e-conomic.com/`), NOT the legacy REST API (`restapi.e-conomic.com`). All 16 tables are available in the new family, so the legacy API is not needed.

All 16 tables are **top-level cursor lists**: none is a child resource that requires iterating over a parent (no `/Projects/{n}/...` or `/Customers/{n}/...` paths exist in either spec). "Per customer" / "per project" access is only a `filter` (e.g. `customerNumber$eq:1`), never needed for full ingestion. Call count = ceil(rows / 1000) per table.

| Table | API | Spec version (verified 2026-11-02) | Base URL |
|---|---|---|---|
| employees | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| projects | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| project_time_entries | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| activities | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| activity_groups | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| employee_groups | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| project_groups | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| project_statuses | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| mileages | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| project_employees | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| project_customers | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| time_entry_prices | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| cost_types | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| customers | Customers | 3.1.0 | `https://apis.e-conomic.com/customersapi/v3.1.0/` |
| customer_contacts | Customers | 3.1.0 | `https://apis.e-conomic.com/customersapi/v3.1.0/` |
| customer_delivery_locations | Customers | 3.1.0 | `https://apis.e-conomic.com/customersapi/v3.1.0/` |

Table names were kept exactly as proposed; none is misleading. Two notes: `project_employees` is a richer view of the same people as `employees` (adds prices/user link, drops phone/email), and `project_customers` is a thin (number, name) projection of `customers`; both are separate endpoints with their own roles, so they are kept as independent tables (see Known Quirks below).

Important: there is no single global API. Each domain is a separate "resource API" (`projectsapi`, `customersapi`, `productsapi`, ...) with its own independent version in the URL path: `/{resource-api}/v{major}.{minor}.{patch}/{Resource}`. The version must be configured per API (make it a connector option with these defaults). Employees live in the **Projects** API (not a separate Employees API). Time entries live in the Projects API too (`/TimeEntries`).

## **Authorization**
- Single method: static API-key style headers (no OAuth flow at runtime).
  - `X-AppSecretToken`: identifies the app (the app's private/secret token).
  - `X-AgreementGrantToken`: identifies the grant issued by a customer agreement (the e-conomic company) to the app.
  - `Content-Type: application/json` (and `Accept: application/json`).
- Connector stores both tokens as credentials (`app_secret_token`, `agreement_grant_token`). Tokens are created per developer agreement at https://www.e-conomic.com/developer/connect (referenced from the API docs; the token-issuing UI itself was not researched).
- Demo tokens: both headers can be set to the literal word `demo`. The demo agreement is **GET only** and was verified working on 2026-10-30 against the first four tables and on 2026-11-02 against all 12 added tables (list + `/count`; the demo grant has all roles). Useful for live smoke tests with no credentials.
- Required app roles per resource group (`x-required-roles`, declared on the spec's `tags`, not on individual operations): Employees = `Sales` or `SuperUser`; EmployeeGroups = `Sales` or `SuperUser`; Customers / Contacts / DeliveryLocations = `Sales` or `SuperUser`; Projects, TimeEntries, Activities, ActivityGroups, ProjectGroups, ProjectStatuses, Mileages, ProjectEmployees, ProjectCustomers, TimeEntryPrices, CostTypes (and CostTypeGroups, ProjectActivities, ProjectEmployeeGroups, MileagePrices) = `ProjectEmployee` or `SuperUser`. A grant lacking the role yields `403`. Note the split: a grant with only `ProjectEmployee` can read the project-side tables but not `employees`, `employee_groups` or any customers-API table; a `Sales`-only grant is the reverse.
- Missing token returns 401 with `errorCode: ApplicationSecretTokenMissing`.

Example:
```
curl -H "X-AppSecretToken: demo" -H "X-AgreementGrantToken: demo" \
  "https://apis.e-conomic.com/projectsapi/v1.1.0/Employees"
```

## **Object List**
Static list (no API for object discovery).

| Connector table | Endpoint (cursor / "Retrieve all") | Other read endpoints |
|---|---|---|
| employees | `GET /projectsapi/v1.1.0/Employees` | `/Employees/paged`, `/Employees/count`, `/Employees/{number}` |
| customers | `GET /customersapi/v3.1.0/Customers` | `/Customers/paged`, `/Customers/count`, `/Customers/{number}` |
| projects | `GET /projectsapi/v1.1.0/Projects` | `/Projects/paged`, `/Projects/count`, `/Projects/{number}`, `/Projects/allowed` |
| project_time_entries | `GET /projectsapi/v1.1.0/TimeEntries` | `/TimeEntries/paged`, `/TimeEntries/count`, `/TimeEntries/{number}` |
| activities | `GET /projectsapi/v1.1.0/Activities` | `/Activities/paged`, `/Activities/count`, `/Activities/{number}`, `/Activities/allowed` |
| activity_groups | `GET /projectsapi/v1.1.0/ActivityGroups` | `/ActivityGroups/paged`, `/ActivityGroups/count`, `/ActivityGroups/{number}` |
| employee_groups | `GET /projectsapi/v1.1.0/EmployeeGroups` | `/EmployeeGroups/paged`, `/EmployeeGroups/count`, `/EmployeeGroups/{number}` |
| project_groups | `GET /projectsapi/v1.1.0/ProjectGroups` | `/ProjectGroups/paged`, `/ProjectGroups/count`, `/ProjectGroups/{number}` |
| project_statuses | `GET /projectsapi/v1.1.0/ProjectStatuses` | `/ProjectStatuses/paged`, `/ProjectStatuses/count`, `/ProjectStatuses/{number}` |
| mileages | `GET /projectsapi/v1.1.0/Mileages` | `/Mileages/paged`, `/Mileages/count`, `/Mileages/{number}` |
| project_employees | `GET /projectsapi/v1.1.0/project-employees` | `/project-employees/paged`, `/project-employees/count`, `/project-employees/{number}` |
| project_customers | `GET /projectsapi/v1.1.0/project-customers` | `/project-customers/paged`, `/project-customers/count`, `/project-customers/{number}` |
| time_entry_prices | `GET /projectsapi/v1.1.0/TimeEntryPrices` | `/TimeEntryPrices/paged`, `/TimeEntryPrices/{number}` (**no `/count`**, live 404) |
| cost_types | `GET /projectsapi/v1.1.0/CostTypes` | `/CostTypes/paged`, `/CostTypes/count`, `/CostTypes/{number}` |
| customer_contacts | `GET /customersapi/v3.1.0/Contacts` | `/Contacts/paged`, `/Contacts/count`, `/Contacts/{number}` |
| customer_delivery_locations | `GET /customersapi/v3.1.0/DeliveryLocations` | `/DeliveryLocations/paged`, `/DeliveryLocations/count`, `/DeliveryLocations/{number}` |

Note the path casing is inconsistent in the spec itself: most resources are PascalCase (`/Activities`) but `project-employees` and `project-customers` are lowercase-kebab. Paths were verified case-insensitive live (`/activities` returned the same data as `/Activities`), but use the spec casing anyway.

All 13 added endpoints above are top-level collections (no parent iteration). Other collections present in the Projects spec but NOT in scope: `CostTypeGroups` (2 demo rows; PK `number`; fields name, type, markup, accountOnGoing, accountClosed), `project-activities` (4 demo rows; PK `number`; has filterable `lastUpdated`; links project to activity), `project-employeegroups` (same 2 rows/fields as `EmployeeGroups` but gated by `ProjectEmployee` instead of `Sales`), `MileagePrices` (4 demo rows; derived prices per mileage `number`; no `/count`). See Deferred Tables.

## **Object Schema**
No schema discovery endpoint; schemas are static and published in the OpenAPI JSON (the download itself requires the demo/real tokens in headers):
- `https://apis.e-conomic.com/projectsapi/openapi-Projects.json`
- `https://apis.e-conomic.com/customersapi/openapi-Customers.json`

Response envelope for cursor endpoints (`EmployeeCursorResults`, `CustomerCursorResults`, `ProjectCursorResults`, `TimeEntryCursorResults`):
```json
{ "items": [ {...}, ... ], "cursor": "<opaque string, omitted when no more data>" }
```
`cursor` is read-only, max length 50. `/paged` endpoints return a bare JSON array instead.

General serialization rules:
- The API docs state "A false boolean is omitted from response body" (and equivalent nulling rules: null properties are omitted). Live demo shows `isBarred:false` and `isApproved:false` present in some responses, so behavior is not consistent; the connector must treat a missing boolean as false/null.
- Nullable properties are omitted rather than returned as `null` (e.g. Employee 4 has no `phone`/`email`). Treat missing keys as null.
- Date-times are ISO 8601 without timezone/offset (e.g. `2024-12-07T23:14:00`); timezone is not documented (TBD). Treat as naive timestamps.

### employees (`Employee`) - required: groupNumber, isBarred, name, number
| Field | Type | Notes |
|---|---|---|
| number | int32 | PK. Filterable (eq, ne, lt, lte, gt, gte, in, nin); sortable |
| groupNumber | int32 | Employee group number (FK to EmployeeGroups) |
| name | string (max 100) | Filterable incl. `like` |
| isBarred | boolean | True = cannot be registered on |
| phone | string, nullable (max 20) | |
| email | string, nullable (max 50) | |
| objectVersion | string, nullable | Optimistic-concurrency token (opaque hex, e.g. `e208e159c08bb729`); not filterable |

No `lastUpdated` field.

### customers (`Customer`) - no required fields on read
All fields are NOT filterable except `customerNumber` (eq, ne, lt, lte, gt, gte, in, nin; sortable).
| Field | Type | Notes |
|---|---|---|
| customerNumber | int32 | PK |
| customerGroupNumber | int32 | |
| zone | int32 | VAT zone |
| currency | string, nullable (max 3) | |
| access | boolean | true = not barred |
| name | string, nullable (255) | |
| address1 | string, nullable (255) | |
| address2 | string, nullable (255) | |
| postCode | string, nullable (30) | |
| city | string, nullable (50) | |
| country | string, nullable (50) | |
| county | string, nullable (50) | |
| contactPerson | string, nullable (50) | |
| phone | string, nullable (255) | |
| mobilePhone | string, nullable (50) | |
| email | string, nullable (255) | |
| homepage | string, nullable (255) | |
| paymentDays | int32, nullable | |
| paymentTermId | int32, nullable | |
| bankAccount | string, nullable (50) | |
| bankGiroNumber | string, nullable (20) | |
| iban | string, nullable (50) | |
| swift | string, nullable (20) | |
| cvrNo | string, nullable (40) | Danish company id |
| vatNo | string, nullable (50) | |
| pnr | string, nullable (40) | P-number |
| ourRef | int32, nullable | |
| layoutGroup | int32, nullable | |
| layoutGroupPrint | int32, nullable | |
| defaultTheirRef | int32, nullable | |
| defaultAttention | int32, nullable | |
| language | int32, nullable | |
| emailAttachmentLanguage | int32 | |
| discountGroupNumber | int32, nullable | |
| priceGroupNumber | int32, nullable | |
| defaultDiscountPct | double, nullable | |
| creditMaximum | double, nullable | |
| defaultDeliveryLocation | int32, nullable | |
| eanLocationNumber | string, nullable (13) | |
| voucherNumber | string, nullable (30) | Public entry number |
| recipientPeppol | string, nullable (30) | |
| nemHandelReceiverType | int32, nullable | |
| directDebitMandateDate | date-time, nullable | |
| defaultDisableEInvoicing | boolean | |
| defaultInvoiceRecipient, defaultOrderRecipient, defaultQuoteRecipient, defaultReminderRecipient, defaultStatementOfAccountRecipient | string, nullable (255 each) | |
| defaultInvoiceTemplate, defaultOrderTemplate, defaultQuoteTemplate, defaultReminderTemplate, defaultStatementOfAccountTemplate | int32, nullable | |
| objectVersion | string, nullable | |

No `lastUpdated` field and no filtering on anything but `customerNumber`.

### projects (`Project`) - required on write: name, projectGroupNumber
| Field | Type | Notes |
|---|---|---|
| number | int32 | PK; filterable; sortable |
| name | string (100) | filterable incl. like |
| projectGroupNumber | int32 | FK ProjectGroups |
| customerNumber | int32, nullable | FK customers (required for external project groups) |
| isClosed | boolean (read-only) | |
| mileage | double | default mileage |
| isBarred | boolean | |
| isMainProject | boolean | main projects group sub-projects |
| isMileageInvoiced | boolean | |
| lastUpdated | date-time, nullable (read-only) | **filterable (eq, ne, lt, lte, gt, gte)**; the change-tracking field |
| responsibleEmployeeNumber | int32, nullable | FK employees |
| otherResponsibleEmployeeNumber | int32, nullable | FK employees |
| mainProjectNumber | int32, nullable | FK projects |
| status | int32, nullable | 1 = Quote sent, 2 = Realised (ProjectStatuses resource) |
| description | string, nullable (2500) | not filterable |
| deliveryDate | date-time, nullable | |
| departmentNumber | int32, nullable | needs Dimension module |
| closedDate | date-time, nullable | |
| fixedPrice | double, nullable | |
| costPrice | double, nullable (read-only) | aggregate of registrations |
| salesPrice | double, nullable (read-only) | aggregate |
| invoicedTotal | double, nullable (read-only) | aggregate |
| contactPersonId | int32, nullable | customer contact |
| deliveryLocationNumber | int32, nullable | |
| otherReference | string, nullable (250) | |
| objectVersion | string, nullable | |

### project_time_entries (`TimeEntry`) - required on write: activityNumber, date, employeeNumber, projectNumber
| Field | Type | Notes |
|---|---|---|
| number | int32 | PK; filterable; sortable |
| projectNumber | int32 | FK projects |
| activityNumber | int32 | FK Activities |
| employeeNumber | int32 | FK employees |
| date | date-time | registration date (time part 00:00:00); filterable |
| text | string, nullable | filterable incl. like |
| numberOfHours | double, nullable | |
| isApproved | boolean (read-only) | approved entries are immutable |
| isReconciled | boolean, nullable (read-only) | |
| lastUpdated | date-time (read-only) | **filterable**; change-tracking field |
| objectVersion | string, nullable | |

Prices for time entries are in a separate resource `/TimeEntryPrices` (table `time_entry_prices`, below).

No nested objects/arrays in any of the 16 tables (flat structures).

Conventions for the batch-2 tables below: "Nullable" means the spec type is `["null", <type>]`; non-nullable non-required fields are still frequently omitted from responses (e.g. `salesAccountOngoing` absent on a group with `includeSalesPriceInFinance:false`), so treat every missing key as null. "F:" lists filterable operators from `x-filterable`; "not filterable" means a `filter` on it returns HTTP 400. `sort` is never usable on cursor endpoints. `objectVersion` (string, nullable, not filterable) is present on every table and is an opaque optimistic-lock token, not a change cursor.

### activities (`Activity`) - required on write: groupNumber, name, number
| Field | Type | Notes |
|---|---|---|
| number | int32 (1..999999999) | PK. F: eq, ne, lt, lte, gt, gte, in, nin |
| groupNumber | int32 | FK activity_groups.number. F: eq..nin |
| name | string (100) | F: eq..nin, like |
| isBarred | boolean | true = no time registration allowed. F: eq, ne, lt, lte, gt, gte |
| inLieuCode | int32 | time-in-lieu code. Not filterable |
| hideInSearch | boolean | hidden in UI time-entry search. Not filterable |
| cutoffDate | date-time, nullable | date when `salesPriceBefore` is replaced by `salesPriceAfter` |
| salesPriceBefore | double, nullable | |
| salesPriceAfter | double, nullable | |
| costPriceMarkupPercentage | double, nullable | |
| objectVersion | string, nullable | |

No `lastUpdated` (live: `filter=lastUpdated$gt:...` returns 400 "Filtering is not allowed on property 'lastUpdated'"). Demo rows: 10. Roles: ProjectEmployee/SuperUser.

### activity_groups (`ActivityGroup`) - required on write: name, type
| Field | Type | Notes |
|---|---|---|
| number | int32 | PK. F: eq..nin |
| name | string (100) | F: eq..nin, like |
| type | int32 enum | 1 ProductiveInvoiceable, 2 ProductiveNonInvoiceable, 3 Unproductive. F: eq, ne |
| includeCostPriceInFinance | boolean | F: eq..gte |
| costAccountClosed | int32, nullable | accounting account no. |
| costAccountOngoing | int32, nullable | |
| costAccountOngoingType | int32 enum | 0 Debit, 1 Credit. Not filterable |
| costContraAccountOngoing | int32, nullable | |
| includeSalesPriceInFinance | boolean | |
| salesAccountClosed | int32, nullable | |
| salesAccountOngoing | int32, nullable | |
| salesAccountOngoingType | int32 enum | 0 Debit, 1 Credit. Not filterable |
| salesContraAccountOngoing | int32, nullable | |
| objectVersion | string, nullable | |

Account fields are required when the matching `include...InFinance` flag is true, otherwise omitted. No `lastUpdated`. Demo rows: 3. Roles: ProjectEmployee/SuperUser.

### employee_groups (`EmployeeGroup`) - required on write: name, number
| Field | Type | Notes |
|---|---|---|
| number | int32 (1..999999999) | PK. F: eq..nin |
| name | string (100) | F: eq..nin, like |
| objectVersion | string, nullable | |

No `lastUpdated`. Demo rows: 2. **Roles: Sales/SuperUser** (differs from the other Projects-API tables except `employees`). `project-employeegroups` returns the same rows under the ProjectEmployee role if the grant lacks Sales.

### project_groups (`ProjectGroup`) - required on write: name, type
| Field | Type | Notes |
|---|---|---|
| number | int32 | PK. F: eq..nin |
| name | string (100) | F: eq..nin, like |
| type | int32 enum | 1 Internal, 2 ExternalInvoicable, 3 ExternalNonInvoicable. F: eq, ne |
| includeCostPriceInFinance | boolean | |
| costAccountOngoingType | int32 enum | 0 Debit, 1 Credit. Not filterable |
| costAccountOngoing | int32, nullable | |
| costContraAccountOngoing | int32, nullable | |
| costAccountClosed | int32, nullable | |
| includeSalesPriceInFinance | boolean | |
| salesAccountOngoingType | int32 enum | 0 Debit, 1 Credit. Not filterable |
| salesAccountOngoing | int32, nullable | |
| salesContraAccountOngoing | int32, nullable | |
| salesAccountClosed | int32, nullable | |
| objectVersion | string, nullable | |

No `lastUpdated`. Demo rows: 3. Roles: ProjectEmployee/SuperUser.

### project_statuses (`ProjectStatus`) - no required fields on read
| Field | Type | Notes |
|---|---|---|
| number | int32 | PK. F: eq..nin |
| name | string, nullable | F: eq..nin, like |
| type | int32 enum | 1 QuoteSent, 2 Realised. F: eq, ne |
| priority | int32 | F: eq..nin |
| objectVersion | string, nullable | |

Only two types exist; `projects.status` holds the `type`-style value 1/2 (the existing doc maps it as 1 = Quote sent, 2 = Realised; whether `projects.status` references `ProjectStatus.number` or `.type` is TBD, on demo both coincide: number 1 = type 1, number 2 = type 2). No `lastUpdated`. Demo rows: 2. Roles: ProjectEmployee/SuperUser.

### mileages (`Mileage`) - required on write: date, distance, employeeNumber, projectNumber
| Field | Type | Notes |
|---|---|---|
| number | int32 (read-only) | PK. F: eq..nin |
| projectNumber | int32 | FK projects. F: eq..nin |
| employeeNumber | int32 | FK employees. F: eq..nin |
| date | date-time | travel date (time part 00:00:00). F: eq, ne, lt, lte, gt, gte |
| distance | double | not filterable |
| from | string, nullable | start place (JSON key is literally `from`; reserved word in SQL/Python, quote or rename in the connector). Not filterable |
| to | string, nullable | destination. Not filterable |
| isApproved | boolean (read-only) | approved entries are immutable. F: eq..gte |
| objectVersion | string, nullable | |

No `lastUpdated` (no change field at all; `date` is the travel date, not an update stamp). Unapproved drafts can be edited/deleted, so incremental by `number` would miss edits. Demo rows: 4. Roles: ProjectEmployee/SuperUser. Prices per mileage are in `/MileagePrices` (deferred).

### project_employees (`ProjectEmployee`) - required: canApprove, canInvoice, groupNumber, isBarred, name, number
| Field | Type | Notes |
|---|---|---|
| number | int32 (1..999999) | PK (same numbering as `employees.number`). F: eq..nin |
| groupNumber | int32 | FK employee_groups. F: eq..nin |
| name | string (100) | F: eq..nin, like |
| isUser | boolean (read-only) | also an e-conomic user. F: eq..gte |
| userId | string, nullable (read-only) | linked user id. F: eq..nin |
| canApprove | boolean | not filterable |
| employeeType | int32 enum (read-only) | 0 NotEmployee, 1 TimeLogger, 2 ProjectManager, 3 MobileTimeLogger. F: eq, ne |
| canInvoice | boolean | F: eq..gte |
| isBarred | boolean | F: eq..gte |
| address | string, nullable (100) | |
| city | string, nullable (50) | |
| zipCode | string, nullable (50) | |
| cutOffDate | date, nullable | date (not date-time). Date after which `...After` prices apply |
| salesPriceBefore | double, nullable | |
| salesPriceAfter | double, nullable | |
| costPriceBefore | double, nullable | |
| costPriceAfter | double, nullable | |
| objectVersion | string, nullable | |

No `lastUpdated` (live: filter on it returns 400). Demo rows: 7 (same as `/Employees/count` = 7). Roles: ProjectEmployee/SuperUser. Contains price fields (sensitive) that `employees` does not expose; does not expose phone/email.

### project_customers (`ProjectCustomer`) - required: name, number
| Field | Type | Notes |
|---|---|---|
| number | int32 (1..999999999) | PK (= `customers.customerNumber`). F: eq..nin |
| name | string (255) | F: eq..nin, like |
| objectVersion | string, nullable | |

"Customers that can be used within Projects." No `lastUpdated`. Demo rows: 5. Roles: ProjectEmployee/SuperUser. This is the only way to read customer names with a ProjectEmployee-only grant (the Customers API needs Sales).

### time_entry_prices (`TimeEntryPrices`) - no required fields
| Field | Type | Notes |
|---|---|---|
| number | int32 | PK = `project_time_entries.number` (1:1 extension of time entries). F: eq..nin |
| costPrice | double, nullable (read-only) | (employee cost price + activity markup %) x hours. Not filterable |
| salesPrice | double, nullable (read-only) | activity sales price x hours; falls back to employee sales price. Not filterable |

Prices are **calculated** from activity/employee prices and hours, not stored with the entry, so they can change without the time entry's `lastUpdated` changing (TBD: whether e-conomic freezes prices at approval; not documented). No `lastUpdated`, no `/count` endpoint (404 live). One row per time entry (demo: 10 rows, matching `TimeEntries/count` = 10), so volume equals the time entry table. Roles: ProjectEmployee/SuperUser. `number` is filterable (`number$gt:N`), enabling append-only partial reads but not change detection.

### cost_types (`CostType`) - required on write: costGroupNumber, name, number
| Field | Type | Notes |
|---|---|---|
| number | int32 (1..999999999) | PK. F: eq..nin |
| name | string (100) | F: eq..nin, like |
| costGroupNumber | int32 | FK CostTypeGroups.number (CostTypeGroups is deferred). Not filterable |
| vatCode | string, nullable | e.g. `"I25  "`; **trailing spaces are significant/present in data** (fixed-width code), trim in the connector if used as a key. Not filterable |
| isBarred | boolean | F: eq..gte |
| lastUpdated | date-time (read-only) | **filterable (eq, ne, lt, lte, gt, gte)**; change field |
| objectVersion | string, nullable | |

Demo rows: 4. Roles: ProjectEmployee/SuperUser.

### customer_contacts (`Contact`, Customers API) - required on write: customerNumber, name
| Field | Type | Notes |
|---|---|---|
| number | int32 (read-only) | PK; globally unique across customers (not the UI number). F: eq..nin |
| customerNumber | int32 | FK customers.customerNumber. F: eq..nin |
| userInterfaceNumber | int32 (read-only) | per-customer UI number (1,2,3...). F: eq..nin |
| name | string (255) | F: eq..nin, like |
| email | string, nullable (255) | F: eq..nin, like |
| phone | string, nullable (50) | not filterable |
| notes | string, nullable (2000) | not filterable |
| eInvoiceId | string, nullable (50) | F: eq..gte |
| lastUpdated | date-time (read-only) | **filterable**; change field |
| receiveQuotes / receiveOrders / receiveInvoices / receiveEInvoices / receiveStatementOfAccounts / receiveReminders | boolean (each) | not filterable |
| objectVersion | string, nullable | |
| isDeleted | boolean (read-only) | F: eq..gte |
| active | boolean (read-only) | F: eq..gte |

Demo rows: 11 (numbers 1-8, 15, 16, 17; gaps 9-14 are not returned). `isDeleted` and `active` were `false`/`true` for every demo row; `filter=isDeleted$eq:true` and `active$eq:false` both returned zero rows, so whether the list ever returns soft-deleted rows is TBD (needs a real agreement; the spec only says "Flag indicating if the contact person was deleted"). Roles: Sales/SuperUser. All contacts for all customers come from this single top-level endpoint (filter `customerNumber$eq:N` for one customer).

### customer_delivery_locations (`DeliveryLocation`, Customers API) - required on write: customerNumber
| Field | Type | Notes |
|---|---|---|
| number | int32 (read-only) | PK; globally unique. F: eq..nin |
| customerNumber | int32 | FK customers.customerNumber. F: eq..nin |
| userInterfaceNumber | int32 (read-only) | per-customer UI number. F: eq..nin |
| address | string, nullable (255) | not filterable |
| city | string, nullable (50) | F: eq..gte |
| country | string, nullable (50) | F: eq..gte |
| postalCode | string, nullable (15) | not filterable |
| termsOfDelivery | string, nullable (100) | not filterable |
| eInvoiceId | string, nullable (50) | F: eq..gte |
| isBarred | boolean | F: eq..gte |
| lastUpdated | date-time (read-only) | **filterable**; change field |
| objectVersion | string, nullable | |

Demo rows: 5. Roles: Sales/SuperUser. Non-ASCII characters (e.g. the o-slash in the Danish city names) arrive as JSON unicode escapes on the wire; standard JSON decoding handles it.

## **Get Object Primary Keys**
Static (no API):
- employees: `number`
- customers: `customerNumber`
- projects: `number`
- project_time_entries: `number`
- activities, activity_groups, employee_groups, project_groups, project_statuses, mileages, project_employees, project_customers, cost_types, customer_contacts, customer_delivery_locations: `number` (all single-column; there are **no composite keys** in the 16 tables)
- time_entry_prices: `number` (= `project_time_entries.number`)
- Careful: `customer_contacts.number` and `customer_delivery_locations.number` are globally unique ids, NOT the per-customer `userInterfaceNumber`; `(customerNumber, userInterfaceNumber)` is a natural alternate key but `number` is the PK.

Each is a unique int32 within the agreement. One connection = one agreement (tenant).

## **Object's ingestion type**
| Table | Type | Cursor field | Rationale |
|---|---|---|---|
| employees | `snapshot` | n/a | no `lastUpdated`; `objectVersion` not filterable; small table |
| customers | `snapshot` | n/a | no `lastUpdated`; only `customerNumber` filterable |
| projects | `cdc` | `lastUpdated` | filterable `lastUpdated`; no delete feed |
| project_time_entries | `cdc` | `lastUpdated` | filterable `lastUpdated`; no delete feed |
| activities | `snapshot` | n/a | no `lastUpdated` (filter returns 400); small config table |
| activity_groups | `snapshot` | n/a | no `lastUpdated`; tiny config table |
| employee_groups | `snapshot` | n/a | no `lastUpdated`; tiny config table |
| project_groups | `snapshot` | n/a | no `lastUpdated`; tiny config table |
| project_statuses | `snapshot` | n/a | no `lastUpdated`; tiny (max 2 types) |
| mileages | `snapshot` | n/a | no `lastUpdated`; drafts editable/deletable so `number$gt` append-only would miss edits |
| project_employees | `snapshot` | n/a | no `lastUpdated`; prices/flags editable |
| project_customers | `snapshot` | n/a | no `lastUpdated`; only (number, name) |
| time_entry_prices | `snapshot` | n/a | no `lastUpdated`; prices are calculated and may change without the time entry changing; same row count as time entries |
| cost_types | `cdc` | `lastUpdated` | filterable `lastUpdated` (verified live with full date-time literal); tiny table, so snapshot is an acceptable simpler alternative that also picks up deletes |
| customer_contacts | `cdc` | `lastUpdated` | filterable `lastUpdated` (verified live); has `isDeleted` flag, see delete note |
| customer_delivery_locations | `cdc` | `lastUpdated` | filterable `lastUpdated` (verified live); no delete flag |

Delete handling: the APIs expose DELETE for writes, but there is **no tombstone/deleted-records feed** for any table. Deleted rows simply disappear, hence `cdc` (not `cdc_with_deletes`) everywhere; incremental reads leave stale rows, so a periodic full refresh is needed for delete sync (snapshot tables self-heal). Unapproved (draft) time entries and mileages can be deleted by users, and projects/employees/customers/cost types can be deleted if unreferenced. The sole exception candidate is `customer_contacts`, which exposes read-only `isDeleted` and `active` booleans (both filterable); the spec says contacts "in use" cannot be deleted (`CustomerContactIsInUse`), and the demo returns no row with `isDeleted=true`, so it is unproven whether the API returns soft-deleted rows. TBD: verify on a real agreement; if it does, `customer_contacts` could become `cdc_with_deletes` using `isDeleted`. Until then treat as `cdc` and keep `isDeleted`/`active` as ordinary columns.

Caveats for cdc:
- `lastUpdated` is documented as nullable on `Project`, and the docs say "not all resources have a LastUpdated property". Rows with null `lastUpdated` would not match `lastUpdated$gt:...` filters. TBD: confirm in a real agreement whether it is ever null on projects (demo sample projects had values, e.g. `2022-06-07T13:04:00`).
- `lastUpdated` granularity is whole minutes in observed data (`2024-12-07T23:14:00`). Docs say it is "considered updated when any property in the model has been changed" but that it can also be modified by changes to other models. Whether aggregate changes on a project (costPrice/salesPrice/invoicedTotal from new registrations) bump `lastUpdated` is TBD. Use an inclusive lower bound (`$gte`) with a lookback (e.g. 5-10 minutes) and de-dupe by PK.
- If freshness of project aggregates matters, treat `projects` as snapshot (small table) as a fallback.

## **Read API for Data Retrieval**

### Two pagination approaches (separate endpoints)
1. **Cursor-based (recommended, use this)**: `GET /{Resource}?cursor={CURSOR}&filter={FILTER}`.
   - Response: `{"items":[...], "cursor":"..."}`; max 1000 items per page (`x-cursor-page-size` = 1000 on these result types; docs say "usually 1000, sometimes smaller"). No `cursor` in response = last page.
   - The cursor is the identifier of the first item of the next page (opaque string, max 50 chars); items are returned in identifier order. **`sort` is NOT supported on cursor endpoints.**
   - No page-size parameter on cursor endpoints.
   - Cursor combined with `filter`: both are parameters of the same endpoint; send the same `filter` on each subsequent request together with the `cursor`. Not verified against a dataset over 1000 rows because demo data is small (TBD).
2. **Classic**: `GET /{Resource}/paged?pagesize=20&skippages=0&sort=...&filter=...`; `pageSize` 1-100 (default 20), `skipPages` 0-100; hard limit of the first 10,000 items. Do NOT use for ingestion.
- Count: `GET /{Resource}/count?filter=...` returns a bare integer - handy for validation.

### Filtering syntax (query param `filter`, URL-encode it)
MongoDB-inspired: `property$operator:value`; chain with `$and:` / `$or:`; parentheses for grouping.
- Operators: `$eq: $ne: $gt: $gte: $lt: $lte: $like: $in: $nin: $and: $or: $null:`.
- Docs example: `?filter=lastUpdated$gt:2024-02-24`; verified live on TimeEntries (returned 2 rows with 2024-12 timestamps). The full date-time literal form (`lastUpdated$gte:2023-12-07T13:39:00`) was verified live on 2026-11-02 against Contacts, DeliveryLocations and CostTypes (inclusive `$gte` returned the boundary row; `$gt` of the max value returned `{"items":[]}`). Date-only literals also work (`/Contacts/count?filter=lastUpdated$gte:2023-12-07` returned 1). Boolean (`isDeleted$eq:true`) and int (`type$eq:2`, `number$gt:8`) filters work. `$` must be URL-encoded when passed via a client (curl `--data-urlencode`).
- `$in:[1,2,3]` (max 200 values). Escape `$ ( ) * , [ ]` by prefixing `$`.
- Filtering on a non-filterable property returns 400 (live: `Filtering is not allowed on property 'foo'.`). Per-property filterable operators are in the schema (`x-filterable`). String filtering is case-insensitive.

### Incremental read strategy (projects, project_time_entries, cost_types, customer_contacts, customer_delivery_locations)
1. First run: no filter, follow `cursor` until absent.
2. Subsequent runs: `filter=lastUpdated$gte:{last_max_lastUpdated minus lookback}` and follow `cursor` until absent. Track `max(lastUpdated)` as offset (ISO string).
3. Upsert by PK (`number`). Results are ordered by PK, not lastUpdated, so compute the offset as the max over the whole run and commit only after the run finishes.

### Snapshot strategy (employees, customers, activities, activity_groups, employee_groups, project_groups, project_statuses, mileages, project_employees, project_customers, time_entry_prices)
Full cursor traversal each run; upsert/replace by PK. `time_entry_prices` is the only potentially large snapshot (one row per time entry); `number$gt:{max_seen}` can be used to fetch only new entries, but prices of existing entries are not re-read that way, so a full refresh is needed to refresh them.

Call-count implications: every table is a flat top-level list, so a full sync of the 16 tables is ceil(rows/1000) calls per table (16 calls on the demo agreement, each `x-callcost: 3`, so about 48 tokens of rate-limit budget). No per-parent fan-out is needed anywhere (do not iterate customers/projects to fetch contacts, delivery locations or project data).

### Example requests (verified against demo tokens)
```
GET https://apis.e-conomic.com/projectsapi/v1.1.0/Employees
-> {"items":[{"number":1,"groupNumber":1,"name":"Martin Crane","isBarred":false,"phone":"0763448762874","email":"martinc@mailinator.com","objectVersion":"e208e159c08bb729"}, ...]}

GET https://apis.e-conomic.com/customersapi/v3.1.0/Customers
-> {"items":[{"customerNumber":1,"customerGroupNumber":1,"zone":1,"currency":"DKK","access":true,"name":"Decathlon","address1":"Avenue des Arts No 5","postCode":"1040","city":"Brussels","country":"Belgium","phone":"08343242525","email":"customerone@mailinator.com","ourRef":1,"paymentTermId":1,"defaultTheirRef":2,"defaultAttention":1,"defaultDeliveryLocation":1,"nemHandelReceiverType":0,"mobilePhone":"066567657657575","emailAttachmentLanguage":0,"defaultDisableEInvoicing":false,"objectVersion":"f3ea6b50f1b8722e"}, ...]}

GET https://apis.e-conomic.com/projectsapi/v1.1.0/Projects
-> {"items":[{"number":1,"name":"Project number 1","projectGroupNumber":1,"customerNumber":1,"isClosed":false,"mileage":0.00,"isBarred":false,"isMainProject":false,"isMileageInvoiced":false,"lastUpdated":"2022-06-07T13:04:00","responsibleEmployeeNumber":5,"otherResponsibleEmployeeNumber":8,"status":1,"description":"...","deliveryDate":"2023-04-21T00:00:00","fixedPrice":10000.00,"costPrice":4080.00,"salesPrice":5146.00,"invoicedTotal":4640.00,"contactPersonId":2,"deliveryLocationNumber":1,"otherReference":"Leonard James","objectVersion":"35bd5d59583b6410"}, ...]}

GET https://apis.e-conomic.com/projectsapi/v1.1.0/TimeEntries?filter=lastUpdated$gt:2024-02-24
-> {"items":[{"number":13,"projectNumber":1,"activityNumber":1,"employeeNumber":1,"date":"2024-12-07T00:00:00","text":"Test at 07-12-2024 23:13:32 +01:00","isApproved":false,"isReconciled":false,"lastUpdated":"2024-12-07T23:14:00","objectVersion":"1e863f70b1bbe7d"}, ...]}
```
In these live samples `cursor` was absent (fewer than 1000 rows in demo data). Projects with `lastUpdated$gt:2024-02-24` returned `{"items":[]}` on demo because demo project data is older.

### Rate limits and errors
- Rate limiting is a **token bucket per app + agreement**; each call has a cost (response header `X-CallCost`, observed `x-callcost: 3` for list calls). Remaining/total budget is exposed in `X-RateLimiting` (e.g. `1450/2000` in the help article; not observed on demo responses). On HTTP 429 a `Retry-After` header gives seconds to wait. Recommended: honour `Retry-After`, otherwise exponential backoff starting at 1s then 2s. Bucket size and refill rate in the help article are examples (e.g. 30 tokens/sec, 2000 burst); real values per agreement are TBD. Source: search summary of the e-conomic help article, page not fetched directly.
- Status codes: 200, 400 (bad filter), 401 (bad/missing tokens or grant revoked), 403 (missing role), 404 (unknown id), 429, 500 (retry). Error body is ProblemDetails-style JSON, e.g. `{"type":"...","title":"Resource not found.","status":404,"detail":"...","errorCode":"ResourceNotFound","traceId":"...","traceTimeUtc":"..."}`.
- Single get: `GET /Employees/{number}`, `/Customers/{number}`, etc.; not needed for ingestion.

### API options comparison
- Cursor endpoint (chosen) vs `/paged`: cursor is fast and unlimited in volume, but has no sort and a fixed 1000 page size; `/paged` supports sort and `pagesize` up to 100 but is capped at 10,000 rows.
- Legacy REST API (`restapi.e-conomic.com`): not used and not researched here.

## **Field Type Mapping**
| API type | Spark/standard type |
|---|---|
| integer int32 | IntegerType (LongType acceptable) |
| number double | DoubleType (monetary values are JSON decimals like `4080.00`; consider DecimalType if exactness matters) |
| boolean | BooleanType (missing = false/null) |
| string | StringType |
| string date-time (no offset) | TimestampType (naive ISO-8601; timezone TBD) |
| objectVersion | StringType (opaque; not a change cursor) |
- Read-only fields: isClosed, lastUpdated, costPrice, salesPrice, invoicedTotal, isApproved, isReconciled; batch 2: mileages.number/isApproved, project_employees.isUser/userId/employeeType, time_entry_prices.costPrice/salesPrice, contacts and delivery locations `number`/`userInterfaceNumber`/`lastUpdated`, contacts `isDeleted`/`active`.
- Enumerations (all ints): `Project.status` / `project_statuses.type` (1 QuoteSent, 2 Realised); `activity_groups.type` (1 ProductiveInvoiceable, 2 ProductiveNonInvoiceable, 3 Unproductive); `project_groups.type` (1 Internal, 2 ExternalInvoicable, 3 ExternalNonInvoicable); `*.costAccountOngoingType` / `salesAccountOngoingType` (0 Debit, 1 Credit); `project_employees.employeeType` (0 NotEmployee, 1 TimeLogger, 2 ProjectManager, 3 MobileTimeLogger). Store as IntegerType; map to names downstream.
- Date vs date-time: `project_employees.cutOffDate` is a plain `date` (`YYYY-MM-DD`); all other date fields are naive date-times.
- Reserved-ish column name: `mileages.from` / `mileages.to` (keep JSON key; quote in SQL).
- Relationships: employees.groupNumber -> EmployeeGroups; projects.customerNumber -> customers.customerNumber; projects.responsibleEmployeeNumber / otherResponsibleEmployeeNumber -> employees.number; projects.mainProjectNumber -> projects.number; time entries projectNumber / employeeNumber / activityNumber -> projects / employees / Activities.
  Batch 2: activities.groupNumber -> activity_groups.number; employee_groups.number <- employees.groupNumber / project_employees.groupNumber; projects.projectGroupNumber -> project_groups.number; projects.status -> project_statuses (number or type, TBD); mileages.projectNumber / employeeNumber -> projects / employees; time_entry_prices.number -> project_time_entries.number (1:1); cost_types.costGroupNumber -> CostTypeGroups.number (deferred); customer_contacts.customerNumber and customer_delivery_locations.customerNumber -> customers.customerNumber; customers.defaultDeliveryLocation -> customer_delivery_locations.number; projects.contactPersonId -> customer_contacts.number (assumed, TBD); projects.deliveryLocationNumber -> customer_delivery_locations.number (assumed); project_customers.number = customers.customerNumber; project_employees.number = employees.number.
- Define explicit nullable columns and use `.get()` on records because absent keys are common.

## Sources and References**
| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official docs index | https://apis.e-conomic.com/ | 2026-10-30 | High | List of resource APIs |
| Official OpenAPI spec (Projects v2 1.1.0) | https://apis.e-conomic.com/projectsapi/openapi-Projects.json (needs demo headers) | 2026-10-30 | High | Endpoints, params, schemas, auth, filtering/sorting/pagination docs, lastUpdated, status codes, roles |
| Official docs page | https://apis.e-conomic.com/projectsapi/redoc.html | 2026-10-30 | High | Rendered Projects API doc |
| Official OpenAPI spec (Customers 3.1.0) | https://apis.e-conomic.com/customersapi/openapi-Customers.json (needs demo headers) | 2026-10-30 | High | Customer schema, only customerNumber filterable |
| Official docs page | https://apis.e-conomic.com/customersapi/redoc.html | 2026-10-30 | High | Rendered Customers API doc |
| Live API with demo tokens | https://apis.e-conomic.com/projectsapi/v1.1.0/ and https://apis.e-conomic.com/customersapi/v3.1.0/ | 2026-10-30 | High | Envelope, sample rows, filter behaviour and 400 error, x-callcost header, 404 shape |
| Official OpenAPI specs re-fetched with demo headers (batch 2: schemas, `x-filterable`, tag-level `x-required-roles`, paths) | https://apis.e-conomic.com/projectsapi/openapi-Projects.json , https://apis.e-conomic.com/customersapi/openapi-Customers.json | 2026-11-02 | High | Field lists/types/nullability, enums, filterable flags, roles for the 12 added tables; no child-resource paths; endpoint inventory incl. deferred ones |
| Live GET calls with demo tokens (batch 2) | list + `/count` on all 12 added endpoints; filter probes (`lastUpdated` date-time/date literal, `isDeleted`, `active`, `type`, `number$gt`, `customerNumber`); `/activities` lower-case path; `/TimeEntryPrices/count` | 2026-11-02 | High | Demo row counts, sample rows, which tables lack `lastUpdated` (400 on filter), 404 on `TimeEntryPrices/count`, case-insensitive paths, full date-time filter literals work |
| Official help article | https://www.e-conomic.dk/support/artikler/rate-limiting-on-the-e-conomic-public-api | 2026-10-30 | Medium | Token bucket, X-CallCost, X-RateLimiting, Retry-After (from search summary only) |
| Third-party blog | https://www.apideck.com/blog/how-to-integrate-with-the-e-conomic-api | 2026-10-30 | Low | General context only |
| Airbyte/Fivetran | not consulted (batch 1 and batch 2) | - | - | Official spec plus live verification judged sufficient; the user-specified scope for batch 2 was fixed, so vendor-coverage ranking was not needed |

Conflicts: none found.

## Deferred Tables
None deferred within the requested scope (all 16 tables documented). Additional endpoints found in the Projects spec and not requested (candidate future tables; all top-level cursor lists with PK `number`, ProjectEmployee/SuperUser roles, same envelope, verified live on 2026-11-02):

| Candidate table | Endpoint | Demo rows | Ingestion note |
|---|---|---|---|
| cost_type_groups | `/CostTypeGroups` | 2 | snapshot; fields number, name, type (CostGroupType int), markup, accountOnGoing, accountClosed; no `lastUpdated` |
| project_activities | `/project-activities` | 4 | cdc candidate: filterable `lastUpdated`; fields number, projectNumber, activityNumber, startDate, endDate, responsibleEmployeeNumber, completed |
| mileage_prices | `/MileagePrices` | 4 | snapshot; derived (number, costPrice, costRate, salesPrice, salesRate); no `/count` |
| project_employee_groups | `/project-employeegroups` | 2 | duplicate of employee_groups under ProjectEmployee role instead of Sales |

`/Setup` (Customers API) is a single-object endpoint, not a list; not researched.
