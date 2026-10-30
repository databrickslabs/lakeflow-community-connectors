# **Visma e-conomic (economic) API Documentation**

Scope of this batch: `employees`, `customers`, `projects`, `project_time_entries`.
API family used: the newer **e-conomic OpenAPI** family (`https://apis.e-conomic.com/`), NOT the legacy REST API (`restapi.e-conomic.com`). All four tables are available in the new family, so the legacy API is not needed.

| Table | API | Spec version (verified 2026-10-30) | Base URL |
|---|---|---|---|
| employees | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| projects | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| project_time_entries | Projects v2 | 1.1.0 | `https://apis.e-conomic.com/projectsapi/v1.1.0/` |
| customers | Customers | 3.1.0 | `https://apis.e-conomic.com/customersapi/v3.1.0/` |

Important: there is no single global API. Each domain is a separate "resource API" (`projectsapi`, `customersapi`, `productsapi`, ...) with its own independent version in the URL path: `/{resource-api}/v{major}.{minor}.{patch}/{Resource}`. The version must be configured per API (make it a connector option with these defaults). Employees live in the **Projects** API (not a separate Employees API). Time entries live in the Projects API too (`/TimeEntries`).

## **Authorization**
- Single method: static API-key style headers (no OAuth flow at runtime).
  - `X-AppSecretToken`: identifies the app (the app's private/secret token).
  - `X-AgreementGrantToken`: identifies the grant issued by a customer agreement (the e-conomic company) to the app.
  - `Content-Type: application/json` (and `Accept: application/json`).
- Connector stores both tokens as credentials (`app_secret_token`, `agreement_grant_token`). Tokens are created per developer agreement at https://www.e-conomic.com/developer/connect (referenced from the API docs; the token-issuing UI itself was not researched).
- Demo tokens: both headers can be set to the literal word `demo`. The demo agreement is **GET only** and was verified working on 2026-10-30 against all four tables. Useful for live smoke tests with no credentials.
- Required app roles per resource group (`x-required-roles` in the spec): Employees = `Sales` or `SuperUser`; Customers = `Sales` or `SuperUser`; Projects = `ProjectEmployee` or `SuperUser`; TimeEntries = `ProjectEmployee` or `SuperUser`. A grant lacking the role yields `403`.
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

Use the capitalised path casing exactly as in the spec (e.g. `/Employees`). Case-insensitivity of paths is not verified (TBD).

Related, NOT in scope (candidate future tables): `Activities`, `ActivityGroups`, `EmployeeGroups`, `ProjectGroups`, `ProjectStatuses`, `Mileages`, `project-employees`, `project-customers`, `TimeEntryPrices`, `CostTypes`, Customers API `Contacts`, `DeliveryLocations`.

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

Prices for time entries are in a separate resource `/TimeEntryPrices` (not in scope).

No nested objects/arrays in any of the four tables (flat structures).

## **Get Object Primary Keys**
Static (no API):
- employees: `number`
- customers: `customerNumber`
- projects: `number`
- project_time_entries: `number`

Each is a unique int32 within the agreement. One connection = one agreement (tenant).

## **Object's ingestion type**
| Table | Type | Cursor field | Rationale |
|---|---|---|---|
| employees | `snapshot` | n/a | no `lastUpdated`; `objectVersion` not filterable; small table |
| customers | `snapshot` | n/a | no `lastUpdated`; only `customerNumber` filterable |
| projects | `cdc` | `lastUpdated` | filterable `lastUpdated`; no delete feed |
| project_time_entries | `cdc` | `lastUpdated` | filterable `lastUpdated`; no delete feed |

Delete handling: the APIs expose DELETE for writes, but there is **no tombstone/deleted-records feed or soft-delete flag** for any of the four tables. Deleted rows simply disappear. Hence `cdc` (not `cdc_with_deletes`). Unapproved (draft) time entries can be deleted by users, and projects/employees/customers can be deleted if unreferenced, so incremental reads leave stale rows; a periodic full refresh is needed for delete sync.

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
- Docs example: `?filter=lastUpdated$gt:2024-02-24`; verified live on TimeEntries (returned 2 rows with 2024-12 timestamps). Full date-time literal form (`2024-02-24T10:00:00`) is not shown in docs (TBD, verify live before relying on it).
- `$in:[1,2,3]` (max 200 values). Escape `$ ( ) * , [ ]` by prefixing `$`.
- Filtering on a non-filterable property returns 400 (live: `Filtering is not allowed on property 'foo'.`). Per-property filterable operators are in the schema (`x-filterable`). String filtering is case-insensitive.

### Incremental read strategy (projects, project_time_entries)
1. First run: no filter, follow `cursor` until absent.
2. Subsequent runs: `filter=lastUpdated$gte:{last_max_lastUpdated minus lookback}` and follow `cursor` until absent. Track `max(lastUpdated)` as offset (ISO string).
3. Upsert by PK (`number`). Results are ordered by PK, not lastUpdated, so compute the offset as the max over the whole run and commit only after the run finishes.

### Snapshot strategy (employees, customers)
Full cursor traversal each run; upsert/replace by PK.

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
- Read-only fields: isClosed, lastUpdated, costPrice, salesPrice, invoicedTotal, isApproved, isReconciled.
- Enumerations: `Project.status` int (1 Quote sent, 2 Realised).
- Relationships: employees.groupNumber -> EmployeeGroups; projects.customerNumber -> customers.customerNumber; projects.responsibleEmployeeNumber / otherResponsibleEmployeeNumber -> employees.number; projects.mainProjectNumber -> projects.number; time entries projectNumber / employeeNumber / activityNumber -> projects / employees / Activities.
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
| Official help article | https://www.e-conomic.dk/support/artikler/rate-limiting-on-the-e-conomic-public-api | 2026-10-30 | Medium | Token bucket, X-CallCost, X-RateLimiting, Retry-After (from search summary only) |
| Third-party blog | https://www.apideck.com/blog/how-to-integrate-with-the-e-conomic-api | 2026-10-30 | Low | General context only |
| Airbyte/Fivetran | not consulted | - | - | Official spec plus live verification judged sufficient |

Conflicts: none found.

## Deferred Tables
None deferred within the requested scope. Out-of-scope tables are listed under Object List.
