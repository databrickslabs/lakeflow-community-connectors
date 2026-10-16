# **HiBob (Bob) API Documentation**

## **Authorization**

- **Preferred method**: HTTP Basic authentication with a **Service User** (created by a Bob admin under Settings > Integrations > Service users). Username = Service User ID, password = Service User token.
- Header: `Authorization: Basic base64("<SERVICE_USER_ID>:<SERVICE_USER_TOKEN>")`, plus `Accept: application/json` and (for POST) `Content-Type: application/json`.
- Alternative (not used): OAuth 2.0 Bearer, available only to registered marketplace partners. The connector should NOT implement OAuth.
- Service users do not count toward headcount. They have **no permissions by default**: the admin must assign a permission group granting, per category, View (and "View history" for historical tables) and an "Access data for" audience (by default employed people only; must include inactive/non-employed to read leavers).
- Base URLs: production `https://api.hibob.com/v1`; sandbox `https://api.sandbox.hibob.com/v1` (expose as config flag, e.g. `use_sandbox`).
- Connector connection params: `service_user_id`, `service_user_token`, optional `use_sandbox` (bool).

Example:
```
curl -X POST https://api.hibob.com/v1/people/search \
  -H "Authorization: Basic $(printf '%s:%s' "$ID" "$TOKEN" | base64)" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{"fields":["root.id","root.email"],"showInactive":true}'
```

## **Object List**

The object list is **static** for built-in tables (below); custom tables and custom people fields are **discoverable** via metadata endpoints.

Proposed tables (Airbyte supports profiles/payroll/employees via full refresh only; Fivetran syncs employee, work history, time off / out-of-office, custom table definitions). Chosen first batch:

| Table | Endpoint | Method | Notes |
|---|---|---|---|
| `employees` | `/people/search` | POST | Core people data, all employees incl. inactive, no pagination |
| `employee_work_history` | `/bulk/people/work` | GET | cursor-paginated |
| `employee_employment_history` | `/bulk/people/employment` | GET | cursor-paginated |
| `employee_lifecycle_history` | `/bulk/people/lifecycle` | GET | cursor-paginated |
| `time_off_request_changes` | `/timeoff/requests/changes` | GET | `since` incremental, 6 month window |
| `named_lists` | `/company/named-lists` | GET | reference data, nested items |
| `employee_fields` | `/company/people/fields` | GET | field metadata |
| `custom_tables_metadata` | `/people/custom-tables/metadata` | GET | table/column definitions |
| `company_report` | `/company/reports/{reportId}/download?format=csv` | GET | one saved report per table config (`report_id`); dynamic CSV schema |

### Deferred Tables

#### Not implemented: permission denied in production
The two tables below were researched and implemented in the first batch, then dropped. The production service user got **403 Forbidden** on both endpoints (it lacked the required permission), so they could not be validated. The research is kept here so they can be added back once a service user with the right permissions is available.

| Table | Endpoint | Method | Notes |
|---|---|---|---|
| `employee_salary_history` | `/bulk/people/salaries` | GET | cursor-paginated, sensitive. Needs Payroll/Salary View + View history. 403 in production. |
| `time_off_policy_types` | `/timeoff/policy-types` | GET | small reference list. 403 in production. |

- **employee_salary_history**: same bulk shared pattern as the work/employment/lifecycle tables (see "Bulk history tables" below; `GET /bulk/people/salaries`). Extra entry fields: `base` object {value: number, currency: string}, `payPeriod` string (Annual/Hourly/Daily/Weekly/Monthly), `payFrequency` string (Monthly/Semi Monthly/Weekly/Bi-Weekly), plus `endEffectiveDate` date. Primary key (`employeeId`, `id`); ingestion type `snapshot` (no date filter). Number type: `base.value` -> DoubleType; `base` -> struct {value: double, currency: string}.
- **time_off_policy_types**: response `{"policyTypes":["Holiday","Sick", ...]}` (array of strings). Primary key `name`; ingestion type `snapshot` (small reference data).

#### Other deferred tables
- `employee_equity_history` (`/bulk/people/equities`), `employee_variable_pay_history` (`/bulk/people/variable`), `employee_dependents` (`/bulk/people/dependents`), `employee_right_to_work` (`/bulk/people/right-to-work`), `employee_entitlements` (`/bulk/people/entitlement`), `employee_deductions` (`/bulk/people/deduction`): identical bulk cursor pattern to the work history table; same connector code applies, deferred only for permission/sensitivity and low demand. Easy follow-ups.
- `employee_training`, `employee_bank_accounts`: per-employee only (`/people/{id}/training`, `/people/{id}/bank-accounts`), N+1 calls; sensitive.
- Custom table entries (`GET /people/custom-tables/{employeeId}/{customTableId}`): per-employee, no bulk: N employees x M tables calls; rate limit risk. Deferred.
- `actual_payments` (`POST /people/actual-payments/search`), `timeoff whosout`/`outtoday`, time off balances (`/timeoff/employees/{id}/balance`, per-employee), policies (`/timeoff/policies`), calendar events search, tasks, onboarding wizards, workforce planning, `/payroll/history` (legacy, planned deprecation), `/profiles` (subset of people search; active only).

## **Object Schema**

No schema-introspection API for typed rows, except people fields: `GET /company/people/fields` returns `{ "fields": [ {id, categoryId, categoryDisplayName, name, description, jsonPath, type, typeData, historical} ] }` (no service-user permission needed for metadata). Field `type` values: text, number, date, list, etc.; `typeData` holds e.g. `listId`. Custom table columns: `GET /people/custom-tables/metadata` returns `{tables:[{id, category, name, description, columns:[{id,name,description,mandatory,type,typeData:{listId}}]}]}`; column types: text, text-area, number, date, list, multi-list, hierarchy-list, currency, employee-reference, document.

Static schemas for the proposed tables follow. Recommended approach for `employees`: fixed core columns plus a `raw`/nested struct for dynamic/custom fields; list-type fields return list item values (use `humanReadable` for labels).

### employees (`POST /people/search`)
Request body:
```json
{"fields": ["root.id","root.firstName","root.surname","root.email","root.displayName","root.creationDateTime","work.department","work.title","work.startDate","work.manager","work.site","work.reportsTo","internal.status","internal.lifecycleStatus","personal.birthDate"],
 "showInactive": true,
 "humanReadable": "APPEND"}
```
- `fields`: array of dot-notation field IDs, max 400. Omitted => default fields from `root`, `about`, `employment`, `work`. Custom fields are never returned unless explicitly listed. Unpermitted or invalid field IDs are **silently omitted** (200 OK).
- `filters`: optional, only ONE filter on `root.id` or `root.email` with `equals` and `values`. Must be omitted entirely (not `[]`, which gives 400).
- `showInactive` (default false): needs "Access data for" audience covering non-employed.
- `humanReadable`: `""` (default, machine values), `"APPEND"` (adds readable values), `"REPLACE"`.
- Response: `{"employees":[{ "id": "3332883884017713938", "firstName":..., "fullName":..., "email":..., "work": {...}, "about": {...}, ... }]}`. Values may appear as nested objects and/or flat slash keys such as `"/root/id": {"value": ...}`; handle both.
- Key fields (types): `id` string (numeric-like), `firstName`/`surname`/`email`/`displayName`/`fullName` string, `creationDateTime` datetime string, `work.startDate` date string, `work.department`/`work.title`/`work.site` string, `work.reportsTo` object {id, firstName, surname, email, displayName}, `internal.status`/`internal.lifecycleStatus` string (Active/Inactive, Employed, Terminated, ...), `personal.birthDate` date.
- Stale reads: up to ~20 seconds after writes.

### Bulk history tables (shared pattern)
`GET /bulk/people/{work|employment|lifecycle}`; query params: `limit` (1-200, default 50), `cursor` (URL-encode), `employeeIds` (comma-separated, max 200; omit for all accessible), `includeArchived` (default false).
Response:
```json
{"results":[{"employeeId":"...","values":[{...entries...}]}],
 "response_metadata":{"next_cursor":"string|null"},
 "errors":[{"employeeId":{"error":"MISSING_PERMISSION","message":"..."}}]}
```
Flatten `results[].values[]` into rows, adding `employeeId`. Common entry fields: `id` integer, `effectiveDate` date, `activeEffectiveDate` date, `endEffectiveDate` date, `isCurrent` boolean, `canBeDeleted` boolean, `creationDate` date (`YYYY-MM-DD`, often null), `modificationDate` date (`YYYY-MM-DD`), `workChangeType` string, `change` {reason, changedBy, changedById} object, `customColumns` object (keys are backend IDs like `column_1666178477233`; map to names via custom-tables / fields metadata).

**employee_work_history** extra: `department` string, `title` string, `site` string, `siteId` integer, `reportsTo` object {id, firstName, surname, email, displayName}.

**employee_employment_history** extra: `contract` string, `type` string, `salaryPayType` string, `weeklyHours` number, `fte` number, `hoursInDayNotWorked` number, `calendarName` string, `calendarId` integer, `flsaCode` string (Exempt/Non-Exempt), `actualWorkingPattern` object (pattern type hourly/fortnightly/flexible with per-day hours or weekly percentage; store as struct/JSON string).

**employee_lifecycle_history** extra: `status` string (Hired, Employed, Terminated, ...), `employeeStatus` string (Active/Inactive), `reasonType` string, `leaveReason` string; `endEffectiveDate` is string.

### time_off_request_changes (`GET /timeoff/requests/changes`)
Response `{"changes":[{...}]}`. Fields: `changeType` string (Created, Canceled, Deleted, Pending), `requestId` int64, `originalRequestId` int64|null, `previousRequestId` int64|null, `employeeId` string, `employeeDisplayName` string, `employeeEmail` string, `policyTypeDisplayName` string, `type` string (duration config, e.g. `days`, `specificHoursDayDurations`, `DifferentDayDurations`, ...), `createdOn` datetime string (change timestamp, e.g. `2025-08-17T18:29:10.408597`), `durationUnit` string (days/hours), `totalDuration` number, `totalCost` number, `changeReason` string, `visibility` string (Public/Private/custom), `startDate` date, `endDate` date, `timeZone` string. Additional type-specific fields (start/end time, portions, day durations) may appear depending on `type`; keep extras in a JSON/variant column.

### named_lists (`GET /company/named-lists`, `includeArchived` bool)
`{"lists":[{"name":"...","items":[{"id":int,"value":string,"name":string,"archived":bool,"children":[...]}]}]}`. Flatten to rows (list_name, item id, value, name, archived, parent_id); children are recursive.

### company_report (saved reports)

**List reports** — `GET /v1/company/reports` → 200, `{"views": [{"id": str, "name": str, "description": str, "createdBy": str, "configuration": object, "type": str, "modificationDate": str, "creationDate": str, "folderId": int, "modifiedBy": str, "isScheduled": bool, "isComparable": bool, "scheduleStatus": str}]}`. Returns only reports visible to (shared with) the calling service user (610 entries for the production account checked). Not paginated. The connector does not call it; use it to discover `report_id` values.

**Download a report** — `GET /v1/company/reports/{reportId}/download?format=csv` → 200, `Content-Type: text/csv; charset=UTF-8`.
- Body: UTF-8, possibly with a BOM (decode as `utf-8-sig`). First row is the header with the report's **human-readable column labels** (as defined in the Bob report builder), followed by one row per data row. Empty cells are `""`. Dates are `YYYY-MM-DD`.
- Example (production report "Work history report - BI", ~3 MB, ~15k rows) header: `Employee ID (bob)`, `Start date`, `Employment type`, `Termination date`, `Effective date`, `Job title`, `Level`, `Manager's ID`, `Site`, `Department`, `Finance Department Description`, `Group`, `Team`, `Product Line`, `Function`, `Reason`, `Leave and termination type`, `Reason for termination`. (`Employee ID (bob)`, `Effective date`) is unique in that report.
- The column set, order and row grain are entirely user-defined and change when the report is edited in Bob, so the schema must be derived from the header at read time.
- `format=json` also works (`{"employees": [{id, internal:{}, work:{}, payroll:{}}]}`, ~6x larger, nested per category) — not used; CSV is flatter and smaller.
- Rate-limit headers (`X-RateLimit-Limit: 50`, `-Remaining`, `-Reset`) are returned; 429 handling is the same as other endpoints.
- 404 when the report ID is unknown or not visible to the service user.
- **Future option (not implemented):** `GET /v1/company/reports/{reportId}/download-async` returns a polling URL for generating large reports asynchronously; consider it if synchronous downloads start timing out.

**Connector mapping** — table `company_report`, required option `report_id`. Every header label becomes a nullable `STRING` column; empty cells become `null`. Column names are normalised: strip accents (NFKD → ASCII), lowercase, replace each run of non-`[a-z0-9]` characters with `_`, strip leading/trailing `_` (e.g. `Employee ID (bob)` → `employee_id_bob`, `Manager's ID` → `manager_s_id`). A label with nothing left becomes `column_<position>`; duplicates get `_2`, `_3`, ... in header order. Optional `primary_keys` (comma-separated or JSON list of normalised names) is validated against the header.

## **Get Object Primary Keys**

Static (no API):
- `employees`: `id`
- `employee_work_history`, `employee_employment_history`, `employee_lifecycle_history`: (`employeeId`, `id`) (entry `id` is an integer row id; use the pair to be safe)
- `time_off_request_changes`: (`requestId`, `changeType`) (a request ID is regenerated on modification; same requestId can appear as Created then Canceled/Deleted). Hedge: also include `createdOn` if duplicates appear.
- `named_lists`: (`list_name`, `item_id`)
- `employee_fields`: `id`; `custom_tables_metadata`: `id` (columns: (`table_id`, `column_id`)).
- `company_report`: none inherent (report-defined). Supplied by the user via `primary_keys`; e.g. (`employee_id_bob`, `effective_date`) for a work-history report.

## **Object's ingestion type**

| Table | Type | Rationale |
|---|---|---|
| `employees` | `snapshot` | No modified-since filter on people search; full pull each run (filter by `root.id` batches if large). Not aware of hard deletes other than absence / `internal.status` inactive |
| `employee_work_history`, `employee_employment_history`, `employee_lifecycle_history` | `snapshot` | Bulk endpoints have no date filter. Entries carry `modificationDate`, which can be used client-side to filter, but the API still returns everything. Not `cdc` as deletes are undetectable incrementally. |
| `time_off_request_changes` | `append` | Change log filtered by `since` (change date); each row is an immutable snapshot of a change |
| `named_lists`, `employee_fields`, `custom_tables_metadata` | `snapshot` | Small reference data |
| `company_report` | `snapshot` | Report download is always the full current result; no filter or cursor |

## **Read API for Data Retrieval**

### People search (employees)
Single POST, no pagination; all employees are returned in one response. For big companies, do two steps: (1) `{"fields":["root.id"],"showInactive":true}`; (2) batches of 50-200 IDs via a `root.id` `equals` filter with desired fields. Rate limit: **`/people/search` 50 requests/minute**.

### Bulk tables
Loop: call with `limit=200`; read `response_metadata.next_cursor`; repeat with `cursor=<url-encoded next_cursor>` until null. Optionally shard with `employeeIds` (<=200) to bound response size. Check `errors[]` per employee (e.g. `MISSING_PERMISSION`); log and skip rather than fail. Requires "View history" permission on the category to get all entries; with only "View" you get just the current entry.

```
GET /v1/bulk/people/work?limit=200&cursor=<cursor>
```

### Time off changes
`GET /timeoff/requests/changes?since=2025-08-01T00:00:00Z&to=...&includePending=true`
- `since` required (ISO 8601); **error if `since` is older than 6 months**. `to` optional, max 6 months after `since`, defaults to now. `includePending` default false.
- Results are filtered by the date the change occurred. No pagination documented.
- Incremental: store max `createdOn` as cursor; next run `since = cursor` minus small lookback; dedupe on primary keys. For initial load, start at most 6 months back (history beyond that cannot be fetched via this endpoint). Fivetran additionally syncs future leave up to 18 months via who's-out style endpoint (`GET /timeoff/whosout?from&to`, optional `includeHourly`, `includePrivate`, `includePending`, `includeWorkingRequests`; only active users) - deferred.

### Deletes
No delete feed. Deleted time-off requests show as `changeType=Deleted`. Terminated employees stay visible only with `showInactive=true`.

### Rate limits
Per-endpoint, per service user; 429 on exceed. Headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` (Unix timestamp). Only published example: people search 50/min. Implement exponential backoff honoring `X-RateLimit-Reset` (and `Retry-After` if present); prefer bulk endpoints to avoid N+1 calls. Documents endpoints have no limits.

### Common errors
400 (bad field/filter), 401 (bad credentials), 403 (missing category permission or audience), 429.

## **Field Type Mapping**

| HiBob type | Spark/standard type |
|---|---|
| string / id (numeric-like, e.g. `3332883884017713938`) | StringType (employee IDs are strings; exceed safe JS ints) |
| integer (entry `id`, `siteId`, `calendarId`, `requestId`) | LongType |
| number (`fte`, `weeklyHours`, `totalDuration`, `totalCost`) | DoubleType (or Decimal) |
| boolean | BooleanType |
| date (`yyyy-MM-dd`) | DateType (or StringType to be safe) |
| datetime (ISO 8601, e.g. `createdOn`, `creationDateTime`) | TimestampType/StringType |
| object (`change`, `reportsTo`, `actualWorkingPattern`, `customColumns`) | StructType / MapType / JSON string |
| list / multi-list / hierarchy-list | list item value (string) or ID; use `humanReadable` for label |
| currency | struct {value: double, currency: string} |

Notes: `customColumns` keys are opaque backend IDs; resolve via metadata. Some dates are documented as "date" but may carry time; parse tolerantly. Absent permissions silently drop fields, so schema may vary per tenant - prefer nullable columns.

## Gotchas
- People search: POST, no pagination, 400 on `filters: []`, only `root.id`/`root.email` equals filters, 400 field cap, silent omission of unpermitted fields, `showInactive` needed for leavers.
- Each field category needs View permission; historical bulk tables need "View history".
- `/payroll/history` and `/profiles` are legacy/limited; use people search and the bulk endpoints instead (`/bulk/people/salaries` is deferred: 403 for the production service user).
- Time-off changes limited to 6 months lookback.
- Custom table entries are per-employee (no bulk), so costly.
- Sandbox and production use different hosts and different service users.

## Sources and References
- Official API docs (highest confidence): https://apidocs.hibob.com/docs/getting-started ; llms index https://apidocs.hibob.com/llms.txt ; People search https://apidocs.hibob.com/reference/post_people-search ; Bulk work https://apidocs.hibob.com/reference/get_bulk-people-work ; employment .../get_bulk-people-employment ; lifecycle .../get_bulk-people-lifecycle ; salaries .../get_bulk-people-salaries ; time off changes .../get_timeoff-requests-changes ; whosout .../get_timeoff-whosout ; fields .../get_company-people-fields ; named lists .../get_company-named-lists ; custom tables .../get_people-custom-tables-metadata ; payroll history .../get_payroll-history ; rate limiting https://apidocs.hibob.com/docs/rate-limit
- Airbyte source-hibob (https://docs.airbyte.com/integrations/sources/hibob): streams profiles, payroll, employees; full refresh only (medium-high confidence).
- Fivetran HiBob (https://fivetran.com/docs/connectors/applications/hibob): employee_work_history, out-of-office (incremental, plus 18 months future leave), custom table definitions (medium-high confidence; detail pages were not fully retrievable).
- Third-party (low-medium): dltHub, Apideck guides reported Basic auth and no public rate limit table.
- Conflicts: official rate-limit page gives only the 50/min people-search example; other limits undocumented. Field-level details for the salary/employment example JSON were taken from schema summaries, not verbatim samples; verify against a live tenant.
