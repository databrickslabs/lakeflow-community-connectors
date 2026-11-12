# **Adobe Analytics 2.0 API Documentation**

Scope of this document: read-only access to the Adobe Analytics 2.0 REST API (`analytics.adobe.io`). The connector is intended to work for any Adobe Analytics customer. All identifiers below (`examplersid`, `variables/evar1`, `metrics/visits`, `examplecompany`) are placeholders.

Confidence labels used in this doc:
- **[Spec]** stated in Adobe's published Analytics 2.0 OpenAPI (Swagger 2.0) specification ([swagger.json](https://github.com/AdobeDocs/analytics-2.0-apis/blob/main/static/swagger.json), rendered at https://developer.adobe.com/analytics-apis/docs/2.0/apis/).
- **[Docs]** stated in Adobe's public documentation (developer.adobe.com or experienceleague.adobe.com); see Sources and References for the pages.
- **[confirmed against a live Adobe Analytics account]** observed with the connector's read-only smoke check (`tests/unit/sources/adobe_analytics/live_smoke_check.py`).
- **[Unverified]** no public source found and not confirmed live; inferred or from community sources. Confirm against a live account before relying on it. The read-only smoke check covers only the behaviours it lists and does not settle the open items in "Items that must be verified against a live account" below (this connector does not use record mode).

## **Authorization**

**Chosen method: OAuth Server-to-Server (OAuth 2.0 `client_credentials` grant).** The connector stores `client_id` and `client_secret`, requests an access token at runtime, and re-requests it when it nears expiry. It does not run any user-facing OAuth flow and has no refresh token (the client-credentials grant simply issues a new token).

Not used: Service Account (JWT) credentials. Adobe has ended support for JWT credentials (no new JWT credentials after 2024-06-03; end of life 2025-06-30) [Docs]. Do not implement JWT.

### Token request

```bash
curl -X POST "https://ims-na1.adobelogin.com/ims/token/v3" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=$CLIENT_ID" \
  -d "client_secret=$CLIENT_SECRET" \
  -d "scope=openid,AdobeID,read_organizations,additional_info.projectedProductContext"
```

Response:
```json
{"access_token": "<token>", "token_type": "bearer", "expires_in": 86399}
```

- Default scopes (comma-separated, no spaces): `openid,AdobeID,read_organizations,additional_info.projectedProductContext`. Adobe's Developer Console authentication reference lists these scopes for Adobe Analytics [Docs], and signing in with this set is [confirmed against a live Adobe Analytics account]. Make the scope string overridable via an optional connection parameter `scopes`, because the scopes shown on a project's OAuth Server-to-Server credential page in Adobe Developer Console can differ (for example if Adobe adds `additional_info.roles`). [Unverified: exact scope requirements per project.]
- Token lifetime: access tokens usually expire in 24 hours, and `expires_in` is given in seconds [Docs]. Refresh when less than about 5 minutes remain, and also on any `401`.
- Token host `ims-na1.adobelogin.com` is the same for all regions; Adobe IMS is global for this flow [Unverified]. Make the token URL an internal constant, not a user option.
- Client secrets can be rotated in Developer Console; rotation invalidates the stored secret (surface the `401`/`invalid_client` error clearly).

### Request headers for every API call

```
Authorization: Bearer <access_token>
x-api-key: <client_id>
x-proxy-global-company-id: <globalCompanyId>      # optional; harmless duplicate of the path value
Content-Type: application/json                      # for POST /reports
Accept: application/json
```

`x-proxy-global-company-id` appears in community guidance for early 2.0 deployments (404s without it) [Unverified]; current Adobe docs only use the company id in the URL path [Docs]. Send both.

### Prerequisites on the Adobe side (document in the connector README)

1. Adobe Developer Console project with the **Adobe Analytics** API added and an **OAuth Server-to-Server** credential.
2. The credential's technical account must be added to an Analytics **product profile** (via Adobe Admin Console) that grants access to the report suites, metrics and dimensions wanted. Data returned is limited to what that product profile allows [Docs].
3. For metadata tables `segments`, `calculated_metrics` and `date_ranges`: the technical account only sees components it owns unless it has Analytics administrator rights, in which case `includeType=all` returns everyone's components (see those tables).

### Global company ID discovery

```bash
curl "https://analytics.adobe.io/discovery/me" \
  -H "Authorization: Bearer $TOKEN" -H "x-api-key: $CLIENT_ID"
```
```json
{
  "imsUserId": "1B...@AdobeID",
  "imsOrgs": [
    {
      "imsOrgId": "EA...@AdobeOrg",
      "companies": [
        {"globalCompanyId": "examplecompany0", "companyName": "Example Company", "apiRateLimitPolicy": "aa_api_tier10_tp"}
      ]
    }
  ]
}
```
[Docs]

Connector behavior:
- `global_company_id` is an optional connection parameter. When provided, skip discovery.
- When omitted, call discovery once per run and flatten `imsOrgs[].companies[]`.
  - Exactly one company: use it.
  - Zero companies: raise an error saying the credential has no Analytics access (check product profile).
  - More than one company: raise an error that lists the available `globalCompanyId` / `companyName` values and tells the user to set `global_company_id`. Do not guess.
- `apiRateLimitPolicy` in the response identifies the rate-limit tier of the company (informational; the documented limits below apply regardless).

### Base URL

`https://analytics.adobe.io/api/{globalCompanyId}/...` (all endpoints below are relative to this) [Spec/Docs].

## **Object List**

The object list is **static**. Metadata tables are fixed endpoints. The report table is user-defined: the user supplies a table name of their choice plus `table_options` (see Read API). Prebuilt/reserved names are used for the metadata tables.

| Table | Endpoint | Scope of one request | Ingestion type |
|---|---|---|---|
| `report_suites` | `GET /reportsuites/collections/suites` | whole company | `snapshot` |
| `virtual_report_suites` | `GET /reportsuites/virtualreportsuites` | whole company | `snapshot` |
| `dimensions` | `GET /dimensions?rsid=...` | one request per report suite | `snapshot` |
| `metrics` | `GET /metrics?rsid=...` | one request per report suite | `snapshot` |
| `segments` | `GET /segments` | whole company (paged) | `snapshot` |
| `calculated_metrics` | `GET /calculatedmetrics` | whole company (paged) | `snapshot` |
| `date_ranges` | `GET /dateranges` | whole company (paged) | `snapshot` |
| any other name (user-defined) | `POST /reports` | per date bucket (see Read API) | `cdc`, or `cdc_with_deletes` when a `dimension` is set and `delete_stale_rows` is on (the default) |

Layering: `dimensions` and `metrics` are children of `report_suites` (the list of rsids is enumerated first, unless the user restricts it with the `rsid` table option: one id or a comma-separated list). A report table is a child of one `rsid` and can reference `segments`, `metrics`/`calculated_metrics` and `dimensions` by id.

Reserved table names (the metadata tables above) must not be used for report tables.

Multiple report suites in one report table are not supported: one report table = one `rsid`. Users create one table entry per report suite.

Excluded from this batch (see Deferred Tables): annotations, projects, users, tags and shares, audit logs, real-time reports, `reports/topItems`, classifications datasets.

## **Object Schema**

### Schema discovery

- Metadata tables have static schemas (below). Adobe may add optional fields to responses at any time [Spec]; the connector should ignore unknown fields (and optionally keep complex ones as JSON strings).
- Report tables have a **dynamic** schema determined by `table_options` (dimension, metrics, granularity). Metric column types come from the static rule in Field Type Mapping; to name columns and validate the request the connector may call `GET /metrics?rsid=` and `GET /dimensions?rsid=` once per `get_table_schema`.

### Common metadata response rules

- `GET /dimensions` and `GET /metrics` return a **bare JSON array** [Spec]. No paging.
- `GET /reportsuites/collections/suites`, `/reportsuites/virtualreportsuites`, `/segments`, `/calculatedmetrics`, `/dateranges` are paged with `limit` and `page` (0-based) [Spec/Docs].
- The shape of the list response is inconsistent across sources and must be parsed tolerantly [Unverified]: Adobe's guide examples show a bare array; the OpenAPI spec for `/segments` and `/dateranges` shows a single object; the `aanalytics2` library and community code read a Spring-style envelope `{"content": [...], "totalElements": n, "totalPages": n, "numberOfElements": n, "number": p, "firstPage": bool, "lastPage": bool}`. All five list endpoints used here return the `{"content": [...], ...}` envelope [confirmed against a live Adobe Analytics account]. Implement: if the body is a list, treat it as the page; if it is an object with `content`, use `content` and `lastPage`. Stop when `lastPage` is true, or when a page returns fewer than `limit` items, or an empty page (needed for bare-array responses).
- Default `limit` for segments is 10 [Docs]; the maximum is not documented, but a `limit` of 1000 is accepted [confirmed against a live Adobe Analytics account]. Use an explicit `limit` of 100 and do not exceed it.

### `report_suites`

Source: `GET /reportsuites/collections/suites` with `expansion=name,parentRsid,currency,calendarType,timezoneZoneinfo` (expansion names from Adobe's single-suite example and the `adobeanalyticsr` package [Docs/Unverified]). Query params: `limit`, `page`, `rsids` (comma list), `rsidContains`, `expansion` [Docs].

Example (single suite shape) [Docs]:
```json
{"rsid": "examplersid", "id": "examplersid", "name": "Example Site", "parentRsid": null,
 "currency": "USD", "timezoneZoneinfo": "America/Los_Angeles",
 "calendarType": {"rsid": "examplersid", "anchorDate": "2024-01-01", "type": "gregorian"}}
```

| Column | Type | Notes |
|---|---|---|
| `rsid` | string | primary key. List items may expose it as `rsid` or `id`; coalesce |
| `name` | string | |
| `parent_rsid` | string | `parentRsid` |
| `currency` | string | ISO currency code |
| `timezone_zoneinfo` | string | IANA tz name; needed by the report strategy |
| `calendar_type` | struct<rsid, anchorDate, type> | `type` e.g. `gregorian`, `retail_445` |

Primary key: `rsid`.

### `virtual_report_suites`

Source: `GET /reportsuites/virtualreportsuites` [Docs]. Documented only as "returns an object containing all report suites that match the criteria"; the list parameters (`limit`, `page`, `idContains`, `segmentIds`, `rsids`, `expansion`) come from search-result summaries and the community libraries [Unverified]. Response field names below are from the Virtual Report Suite JSON reference [Unverified]; keep unknown fields out of the schema and keep a `raw` JSON string for the rest.

| Column | Type | Notes |
|---|---|---|
| `id` | string | primary key, e.g. `examplersid:vrs1` style id |
| `name` | string | |
| `parent_rsid` | string | |
| `timezone_zoneinfo` | string | |
| `description` | string | |
| `currency` | string | |
| `segment_list` | array<string> | segment ids that define the VRS |
| `modified` | timestamp | ISO 8601 |
| `raw` | string | full JSON of the item (forward compatibility) |

Primary key: `id`.

### `dimensions`

Source: `GET /dimensions?rsid={rsid}` [Spec]. Parameters: `rsid` (required), `locale`, `segmentable`, `reportable`, `classifiable`, `expansion` (documented values include `tags`, `allowedForReporting`, `attributionModel`, `categories`) [Spec]. Use `expansion=allowedForReporting,tags,categories` [Unverified: exact set; unknown expansion values may return 400, so fall back to no expansion on 400].

Response item (`AnalyticsDimension`) [Spec]: `id, title, name, type, category, categories[], support[], pathable, parent, extraTitleInfo, segmentable, reportable[], description, allowedForReporting, attributionModel{}, noneSettings{includeNoneByDefault, noneChangeable}, tags[]`.

| Column | Type | Notes |
|---|---|---|
| `rsid` | string | added by the connector (request parameter) |
| `id` | string | e.g. `variables/evar1`; use this value in `table_options.dimension` |
| `title`, `name`, `type`, `category`, `extra_title_info`, `description`, `parent` | string | `type` e.g. `string`, `int`, `time`, `enum` (case varies) |
| `categories`, `support` | array<string> | |
| `reportable` | array<string> | spec types it as an array |
| `pathable`, `segmentable`, `allowed_for_reporting` | boolean | |
| `attribution_model` | string (JSON) | object; stored as JSON text |
| `none_settings` | struct<include_none_by_default boolean, none_changeable boolean> | |
| `tags` | string (JSON) | array of objects; stored as JSON text |

Primary key: `(rsid, id)`. If a report suite returns `401`/`403` ("user does not have access to this report suite") while the connector is enumerating every report suite, skip that rsid with a warning rather than failing the table. Report suites listed explicitly in the `rsid` table option are not skipped: the error stops the table.

### `metrics`

Source: `GET /metrics?rsid={rsid}` [Spec]. Parameters: `rsid` (required), `locale`, `segmentable`, `expansion` (`tags`, `allowedForReporting`, `categories`) [Spec].

Response item (`AnalyticsMetric`) [Spec]: `id, title, name, type, extraTitleInfo, category, categories[], support[], allocation, precision, calculated, segmentable, description, polarity, helpLink, allowedForReporting, tags[]`.

| Column | Type | Notes |
|---|---|---|
| `rsid` | string | added by connector |
| `id` | string | e.g. `metrics/visits`; use in `table_options.metrics` |
| `title`, `name`, `type`, `extra_title_info`, `category`, `description`, `polarity`, `help_link` | string | `type` values: `INT`, `DECIMAL`, `CURRENCY`, `PERCENT`, `TIME`, `ENUM`, `ORDERED_ENUM`, `STRING` [Spec]; returned in lower case [confirmed against a live Adobe Analytics account] |
| `categories`, `support` | array<string> | |
| `allocation`, `calculated`, `segmentable`, `allowed_for_reporting` | boolean | |
| `precision` | long | decimal places |
| `tags` | string (JSON) | |

Primary key: `(rsid, id)`.

### `segments`

Source: `GET /segments` [Spec/Docs].

Query parameters [Spec]: `rsids` (comma list; note the public guide documents this as `rsid` [Docs]; the connector sends neither and lists segments for the whole company), `segmentFilter` (ids), `name`, `tagNames`, `filterByPublishedSegments` (`all`/`true`/`false`), `limit`, `page`, `sortDirection` (`ASC`/`DESC`), `sortProperty` (`name`, `modified_date`, `id`), `expansion`, `includeType` (`shared`, `templates`, `all`; `all` requires administrator rights, otherwise `403`) [Spec/Docs], `locale`.

Valid `expansion` values [Docs]: `reportSuiteName, ownerFullName, modified, tags, compatibility, definition, publishingStatus, definitionLastModified, categories`.

`includeType=all` is accepted and returns more than one page of results [confirmed against a live Adobe Analytics account]; how much smaller the result is without it is [Unverified]. The connector defaults to `all`, falls back to `shared` on `403`, and then to no `includeType` (only the technical account's own components) if `shared` is also refused. The `include_type` table option overrides this: any value Adobe accepts is sent as is with no fallback, and `none` sends no `includeType`. Request all expansion values except `compatibility` (large, not useful for analytics).

Response item (`AnalyticsSegmentResponseItem`) [Spec]: `id, name, description, rsid, reportSuiteName, owner{id,name,login}, definition{}, compatibility{}, definitionLastModified, categories[], siteTitle (deprecated), tags[], modified, created`.

| Column | Type | Notes |
|---|---|---|
| `id` | string | primary key, e.g. `s300000000_0123456789abcdef` |
| `name`, `description` | string | |
| `rsid`, `report_suite_name` | string | rsid the segment was built in |
| `owner_id` | long | `owner.id` |
| `owner_name`, `owner_login` | string | only populated with `ownerFullName` |
| `definition` | string (JSON) | segment container tree; stored as JSON text |
| `definition_last_modified`, `modified`, `created` | timestamp | ISO 8601 strings in the API |
| `categories` | array<string> | |
| `tags` | string (JSON) | |
| `publishing_status` | string | present only with that expansion [Unverified: field name] |

**Incremental decision: snapshot.** Evidence: (1) `sortProperty=modified_date` exists [Spec], so results can be ordered newest-modified first, but (2) there is no "modified after" filter on `/segments` or `/calculatedmetrics` (a `filterByModifiedAfter` parameter exists only on `/dateranges`) [Spec], and (3) there is no way to list deleted components. A cursor approach could stop paging once `modified` is older than the cursor, but would never detect deletes or visibility changes (sharing, ownership). The volume is small (hundreds to low thousands of rows), so a full snapshot per run is cheap. Use `snapshot`.

### `calculated_metrics`

Source: `GET /calculatedmetrics` [Spec/Docs].

Query parameters [Spec]: `rsids`, `ownerId` (login id), `filterByIds`, `toBeUsedInRsid`, `locale`, `name`, `tagNames`, `favorite`, `approved`, `limit`, `page`, `sortDirection`, `sortProperty` (`name`, `modified_date`, `id`), `expansion`, `includeType` (`shared`, `templates`, `all`).

Valid `expansion` values [Spec/Docs]: `reportSuiteName, ownerFullName, modified, tags, definition, compatibility, categories`. Default fields: `id, name, description, rsid, owner, polarity, precision, type`. Request all except `compatibility`.

Response item (`AnalyticsCalculatedMetric`) [Spec]: `id, name, description, rsid, reportSuiteName, owner{}, polarity, precision, type, definition{}, categories[], tags[], siteTitle, modified, created`. Docs example also shows `template` (boolean) on built-in metrics [Docs].

| Column | Type | Notes |
|---|---|---|
| `id` | string | primary key; saved ids look like `cm300000000_0123456789abcdef`; built-in templates like `cm_bouncerate_defaultmetric`. Usable directly as a metric id in report requests |
| `name`, `description`, `rsid`, `report_suite_name` | string | |
| `owner_id` | long | |
| `owner_name`, `owner_login` | string | with `ownerFullName` |
| `polarity` | string | `positive`/`negative` |
| `precision` | long | |
| `type` | string | `decimal`, `percent`, `currency`, `time` |
| `template` | boolean | |
| `definition` | string (JSON) | formula tree |
| `categories` | array<string> | |
| `tags` | string (JSON) | |
| `modified`, `created` | timestamp | |

Primary key: `id`. Ingestion: `snapshot` for the same reasons as segments.

### `date_ranges`

Source: `GET /dateranges` [Spec]. Parameters: `includeType`, `expansion`, `locale`, `filterByIds`, `newDefinition`, `filterByModifiedAfter` (ISO 8601), `curatedRsid`, `limit`, `page`. Response object (`ExpandedDateRange`) [Spec]: `id, name, description, modified, companyId, owner{}, definition, template, createDate, disabledDate, alternateVariableNames{}, curatedItem, imsOrgId, systemUserOwned, tags[], shares[], approved, favorite, usageSummary{}`.

| Column | Type | Notes |
|---|---|---|
| `id` | string | primary key |
| `name`, `description` | string | |
| `definition` | string | date-range formula text or JSON (type varies; stored as text/JSON string) [Unverified] |
| `owner_id` | long | from `owner` |
| `company_id` | long | |
| `ims_org_id` | string | |
| `template`, `curated_item`, `system_user_owned`, `approved`, `favorite` | boolean | |
| `modified`, `create_date`, `disabled_date` | timestamp | |
| `tags`, `shares`, `usage_summary`, `alternate_variable_names` | string (JSON) | |
| `raw` | string | full JSON of the item (forward compatibility) |

Ingestion: `snapshot`. `filterByModifiedAfter` would permit incremental reads, but nothing signals deletion and the table is small, so snapshot keeps behavior consistent. `includeType` follows the same default and fallback as `segments` (`all`, then `shared` on `403`, then none), and the `include_type` table option overrides it.

### Report table schema (dynamic)

For a report configured with `dimension` (optional) and `metrics`:

| Column | Type | Present when | Notes |
|---|---|---|---|
| `rsid` | string | always | constant for the table |
| `date` | date | always | first calendar date of the bucket (the day for `day`/`hour`, week start, first of month) in the report suite timezone |
| `hour` | long | `granularity=hour` | 0-23 |
| `dimension_value` | string | `dimension` set | human-readable value (`rows[].value`), includes the literal `Unspecified` when None rows are included |
| `dimension_item_id` | string | `dimension` set | Adobe `itemId` hash, stable per value |
| `segment_ids` | string | always | sorted, comma-joined `segment_ids` option; empty string when none (keeps the primary key non-null and separates tables that change the segment) |
| `retrieved_at` | timestamp | always | when the run that read the row started (UTC); the same for every row of one run. Declared as the table's `cursor_field`, so it sequences CDC upserts: a re-read row always replaces the older copy |
| one column per metric | double | always | column name = last path segment of the metric id, with `/` and non-alphanumeric characters replaced by `_` (for example `metrics/visits` becomes `visits`, `cm300000000_0123abcd` stays as is); collisions get a numeric suffix |

Primary key (`cdc`, MERGE): `["rsid", "date"]` + `["hour"]` if hourly + `["dimension_value"]` if a dimension is set + `["segment_ids"]`. Use `dimension_value` rather than `dimension_item_id` as the key since `itemId`s of time/derived dimensions may be unstable [Unverified].

## **Get Object Primary Keys**

There is no API for primary keys. They are static per table:

| Table | Primary key |
|---|---|
| `report_suites` | `rsid` |
| `virtual_report_suites` | `id` |
| `dimensions` | `rsid`, `id` |
| `metrics` | `rsid`, `id` |
| `segments` | `id` |
| `calculated_metrics` | `id` |
| `date_ranges` | `id` |
| report table | `rsid`, `date`, [`hour`], [`dimension_value`], `segment_ids` (see above); allow override via the framework-level `primary_keys` option |

## **Object's ingestion type**

| Table | Type | Reason |
|---|---|---|
| all metadata tables | `snapshot` | no change feed, no deletes, no reliable modified-since filter (see segments decision) |
| report table without a `dimension`, or with `delete_stale_rows=false` | `cdc` | data is keyed by date (and dimension); recent days are revised, so a lookback window is re-read and upserted by key. Without a dimension every bucket returns exactly one row, so no row can disappear. |
| report table with a `dimension` and `delete_stale_rows=true` (the default) | `cdc_with_deletes` | as `cdc`, plus `read_table_deletes`: dimension values that were seen in a lookback bucket but are missing when that bucket is re-read are deleted. The values seen per bucket are kept in the delete flow's own offset, capped at `max_tracked_keys` (default 10000); buckets beyond the cap are not tracked and a warning says how many. |

Report tables with no date axis are not offered (the table is always time-bucketed because the strategy always filters by date). Pure all-time totals are out of scope.

`read_table_metadata` declares `retrieved_at` as the `cursor_field` of a report table: it sequences upserts and is not used to choose dates. The dates to read come from the offset, whose `cursor` is the last date (bucket start) already ingested. Per run: `from = min(cursor - lookback_days, today)`, floored at `start_date`; `to = today` in the report-suite timezone. For `week` granularity the lookback is rounded up to whole weeks. On first run `from = start_date`, which defaults to 30 days before the first run. During a long backfill a batch can stop early (see `max_records_per_batch`), and the next batch resumes the day after the offset's cursor instead of applying the lookback. Do not advance the cursor beyond the report-suite-local "today" (an in-progress bucket is re-read on the next run through the lookback window).

## **Read API for Data Retrieval**

### Metadata tables

Simple GET loops as described above. For `dimensions` and `metrics`: obtain the rsid list from `report_suites` (paged) unless the `rsid` table option is set (one id or a comma-separated list), then issue one GET per rsid. Cost is one request per report suite per table. Honor the rate limit.

### Report table: `POST /reports`

`POST https://analytics.adobe.io/api/{globalCompanyId}/reports` [Spec]. Responses: `200`, `206` (partial; see errors), `400`.

#### `table_options` for a report table

| Option | Required | Default | Description |
|---|---|---|---|
| `rsid` | yes | | Report suite id (for example `examplersid`). Virtual report suite ids (`vrs_...`) work as rsid [confirmed against a live Adobe Analytics account] |
| `metrics` | yes | | Comma-separated list (or JSON array string) of metric ids, for example `metrics/visits,metrics/pageviews`; calculated metric ids (`cm...`) are allowed |
| `dimension` | no | none | One non-time dimension id, for example `variables/evar1` or `variables/page`. If omitted, one row per time bucket |
| `segment_ids` | no | none | Comma-separated segment ids applied as global filters (AND logic) |
| `start_date` | no | 30 days before the first run | First date to ingest, `YYYY-MM-DD`. A changed value only takes effect after a full refresh |
| `lookback_days` | no | 3 | Days before the cursor re-read each run (see Latency) |
| `granularity` | no | `day` | `hour`, `day`, `week`, `month`. `week` and `month` only without a `dimension` |
| `limit` | no | 50000 | Rows per page (maximum 50,000) |
| `max_rows_per_bucket` | no | unlimited | Optional safety cap on rows read per bucket for high-cardinality dimensions |
| `nones_behavior` | no | `return` | `return` (sent as `return-nones`) or `exclude` (sent as `exclude-nones`); the long forms are also accepted (see below) |
| `count_repeat_instances` | no | `true` | Sent as `settings.countRepeatInstances` on every request; `true` matches the Analysis Workspace default |
| `max_records_per_batch` | no | 100000 | Rows per micro-batch during a backfill with a `dimension`; the batch stops between whole days once this is reached. Reads without a dimension are bounded by their request size instead |
| `delete_stale_rows` | no | `true` | With a `dimension`, makes the table `cdc_with_deletes` (see Object's ingestion type) |
| `max_tracked_keys` | no | 10000 | Upper bound on the dimension values remembered for stale-row deletes |

All of these names, plus `include_type` (used by `segments`, `calculated_metrics` and `date_ranges`), must be added to `externalOptionsAllowList`, the same way as in `google_analytics_aggregated`.

Decisions recorded here: (1) one dimension max in the base table, because Adobe 2.0 allows only one dimension per `/reports` request; (2) one rsid per table; (3) `segment_ids` are applied as global filters only; (4) real-time reports (`/reports/realtime`) are not used because they do not support segments or calculated metrics and only cover the last 20 hours [Docs].

#### Strategy A: no `dimension` (trended report)

One request per chunk (paged if needed) covers the `[from, to]` window. `day` requests cover up to 366 days and `hour` requests up to 31 days; a longer backfill continues in the next batch. `week` and `month` requests cover the whole window, starting up to 7 or 42 days before `from` (never before `start_date`) so that buckets overlapping the lookback window are complete. A partial bucket at the start of the request is dropped and logged, so a `start_date` at the start of a week or month keeps the first bucket. Rows Adobe returns outside the requested dates are skipped with a warning.

```json
{
  "rsid": "examplersid",
  "globalFilters": [
    {"type": "dateRange", "dateRange": "2025-01-01T00:00:00.000/2025-02-01T00:00:00.000"}
  ],
  "metricContainer": {
    "metrics": [
      {"columnId": "0", "id": "metrics/visits"},
      {"columnId": "1", "id": "metrics/pageviews"}
    ]
  },
  "dimension": "variables/daterangeday",
  "settings": {"limit": 50000, "page": 0, "countRepeatInstances": true, "dimensionSort": "asc"}
}
```

`variables/daterange{minute|hour|day|week|month|quarter|year}` is the time dimension (`daterangeday` [Docs]; `daterangehour`, `daterangeweek` and `daterangemonth` [confirmed against a live Adobe Analytics account]; minute, quarter and year [Unverified]); map `granularity` to `daterangehour`, `daterangeday`, `daterangeweek`, `daterangemonth`. Each response row is one bucket: `rows[].value` is a display label (for example `Jan 1, 2025`) and `rows[].itemId` is a digit string: century digit, YY, zero-based MM (00 = January), DD, plus HH for hours (for example `1250114` is February 14, 2025, and `125011409` is 09:00 that day) [confirmed against a live Adobe Analytics account]. Decode buckets from `itemId`, since the label is locale-dependent, and fail loudly on an unknown format. Alternative that avoids label parsing: derive bucket boundaries locally (see below) and issue one non-dimension request per bucket, reading `summaryData.totals` (a request with no `dimension` returns only `summaryData` [Docs]); this costs one request per bucket.

For hourly granularity over long windows, chunk requests by 31 days so one page holds at most 744 rows.

#### Strategy B: with `dimension` (date x dimension, one ranked report per bucket)

Adobe cannot return date x dimension in one request without breakdowns, so issue **one ranked report per time bucket**, each with the bucket as a `dateRange` global filter and the user's dimension as the report dimension:

```json
{
  "rsid": "examplersid",
  "globalFilters": [
    {"type": "dateRange", "dateRange": "2025-01-15T00:00:00.000/2025-01-16T00:00:00.000"},
    {"type": "segment", "segmentId": "s300000000_0123456789abcdef"}
  ],
  "metricContainer": {
    "metrics": [
      {"columnId": "0", "id": "metrics/visits", "sort": "desc"},
      {"columnId": "1", "id": "metrics/pageviews"}
    ]
  },
  "dimension": "variables/evar1",
  "settings": {"limit": 50000, "page": 0, "countRepeatInstances": true, "nonesBehavior": "return-nones"}
}
```

Loop `settings.page` from 0 until `lastPage == true`. Each emitted row gets `date`/`hour` from the bucket the request was for (not from the response), `dimension_value = rows[].value`, `dimension_item_id = rows[].itemId`, metric columns from `rows[].data[i]` mapped through `columns.columnIds` (the order of `data` matches `columnIds`, which matches the request `columnId`s). The first metric should have `sort: "desc"` so that, if the optional `max_rows_per_bucket` cap is hit, the highest-volume values are kept; note that capped data is incomplete and totals will not reconcile.

Bucket boundaries (computed locally, all in the report suite timezone):
- `day`: `YYYY-MM-DDT00:00:00.000/<next day>T00:00:00.000` (the end is exclusive) [confirmed against a live Adobe Analytics account].
- `hour`: `YYYY-MM-DDTHH:00:00.000/<next hour>:00:00.000`. Date x dimension x hour multiplies requests by 24; warn in docs.
- `week` and `month`: **not supported with a dimension**; the connector rejects the combination with a clear error. Adobe weeks follow the report suite's calendar (`calendarType`) and can start on Sunday or Monday depending on that configuration [Unverified]; each week row carries its true start date [confirmed against a live Adobe Analytics account], so a week start must never be hardcoded. For `retail_445`-style calendars, month and week buckets do not match the Gregorian calendar [Unverified]. Week and month tables are therefore read only with Strategy A, which takes each bucket's start date from Adobe's `itemId`. A later release could derive Strategy B buckets from one Strategy A request.

Request cost for Strategy B: `buckets x pages-per-bucket`. For a daily table with a 3-day lookback this is roughly 4 requests per run per page, plus about as many again for the stale-row check when `delete_stale_rows` is on; a one-year backfill is about 365 requests per page, about 3 to 4 minutes at the connector's own limit of 10 requests per 6 seconds (one thread). High-cardinality dimensions (for example page URLs, search terms) can return up to the full cardinality; set `limit=50000` and `max_rows_per_bucket` if the table gets too large.

Optional optimization (not in the first implementation) [Unverified]: put several days into one request as separate metric columns, using one `metricFilters` entry of `type: "dateRange"` per day and `filters` on each metric. This returns dimension x (days x metrics) in a single call; the maximum number of metric columns per request is undocumented, so size it by test.

#### Additional dimensions via itemId breakdown (documented, not in the base table)

To add a second dimension, run the parent ranked report first, then one child request per parent row:

```json
{
  "rsid": "examplersid",
  "globalFilters": [
    {"type": "dateRange", "dateRange": "2025-01-15T00:00:00.000/2025-01-16T00:00:00.000"}
  ],
  "metricContainer": {
    "metrics": [{"columnId": "0", "id": "metrics/visits", "filters": ["0"]}],
    "metricFilters": [
      {"id": "0", "type": "breakdown", "dimension": "variables/evar1", "itemId": "3306266643"}
    ]
  },
  "dimension": "variables/evar2",
  "settings": {"limit": 50000, "page": 0}
}
```

Each breakdown level needs its own request per parent item [Docs]; `itemValue` can be given instead of `itemId` when the item's text is known [Docs], and whether either is faster is [Unverified]. Cost per bucket = `1 + N_parent_rows` requests (times pages), so a two-dimension daily table with 1,000 parent values costs about 1,000 requests per day (about 8 minutes of rate-limit budget per day of data). Recommendation: do not offer this in the base release; if added later, require an explicit `max_parent_items` (suggest 50 to 100, taking the top rows by the first metric) and document that the child data is then partial. Nested filters can also be written as `globalFilters` entries of `type: "breakdown"` [Spec: `ReportFilter` types are `dateRange`, `breakdown`, `segment`, `excludeItemIds`].

#### Response structure (`RankedReportData`) [Spec]

```json
{
  "totalPages": 1, "firstPage": true, "lastPage": true,
  "numberOfElements": 2, "number": 0, "totalElements": 2,
  "columns": {"dimension": {"id": "variables/evar1", "type": "string"}, "columnIds": ["0", "1"]},
  "rows": [
    {"itemId": "1000001", "value": "value-a", "data": [120.0, 340.0]},
    {"itemId": "1000002", "value": "value-b", "data": [80.0, 210.0]}
  ],
  "summaryData": {"totals": [200.0, 550.0], "filteredTotals": [200.0, 550.0]}
}
```

- `rows[].data` entries are JSON numbers (floats) [Spec]; whether individual entries can be `null` is [Unverified].
- `columns.columnErrors[]` may be present on `206` responses: `{columnId, errorCode, errorId, errorDescription}`. Error codes include `unauthorized_metric`, `unauthorized_dimension`, `not_enabled_metric`, `not_enabled_dimension` [Spec]. Treat any `columnErrors` as a hard failure for that table with the error text (silently emitting nulls for a metric would mislead users). Common cause: the metric or dimension is not enabled or not permitted for that report suite.
- Pagination fields: `totalPages`, `firstPage`, `lastPage`, `numberOfElements`, `number`, `totalElements` [Spec]. Paging is offset-based (`settings.page`, 0-based; `settings.limit` rows per page) [Spec]. A `limit` of 50,000 is accepted [confirmed against a live Adobe Analytics account]; the default and whether 50,000 is the maximum are [Unverified].
- `summaryData.totals` are for the whole report, not the page. They can be used for sanity checks. Metrics such as unique visitors are not additive across dimension values or buckets; never sum them in the connector.

#### Settings relevant to ingestion [Spec]

| Setting | Use |
|---|---|
| `limit`, `page` | paging |
| `dimensionSort` | `asc` for time dimensions |
| `nonesBehavior` | `return-nones` or `exclude-nones` [Docs]; `return-nones` adds and `exclude-nones` removes the "Unspecified" row [confirmed against a live Adobe Analytics account] |
| `countRepeatInstances` | boolean in the spec. Always sent, from the `count_repeat_instances` table option (default `true`, which matches the Analysis Workspace default) |
| `includeAnomalyDetection`, `includeForecasting`, `includePercentChange`, `includeLatLong`, `reflectRequest` | do not use |

**"Unspecified" / None rows.** For dimensions where an event happened without a value, Adobe returns a row whose `value` is `Unspecified` [Docs; also confirmed against a live Adobe Analytics account in English]; other labels such as "None" for some dimensions or locales are [Unverified]. With `return-nones` (the connector default) it is kept as a real row so that table totals reconcile with Workspace; with `exclude-nones` it is dropped. Dimension metadata exposes `noneSettings.includeNoneByDefault` and `noneChangeable` [Spec]. The literal string is not stable across locales; do not match on it in the connector. Which of the two settings Workspace's "Include unspecified" checkbox maps to is [Unverified].

#### Date handling and report-suite timezone

- `dateRange` strings are ISO 8601 `start/end` interpreted in the **report suite's configured timezone** [Docs for date formulas; Unverified for absolute dates]. The suite's zone is available from `report_suites.timezoneZoneinfo`; a virtual report suite uses its own zone, or its parent's when it has none. Compute "today", the cursor and bucket boundaries in that zone, not the Databricks cluster zone. If the zone cannot be read or is unknown, the connector uses UTC and logs a warning.
- Date formulas (`td-7d/td`) and mixed formats exist but only work in `globalFilters` of ranked reports [Docs]; prefer explicit absolute dates.
- End timestamp: the end of a `dateRange` is **exclusive**; use the start of the next bucket (`<next day>T00:00:00.000`), not `T23:59:59.999` [confirmed against a live Adobe Analytics account].
- DST days have 23 or 25 hours [Unverified for Adobe's output]. Hourly rows are keyed by `date` and `hour`: ranked (dimension) reads take them from the requested bucket, and trended reads decode them from `itemId`, never from the locale-dependent label. A missing local hour simply produces no row; if a trended read returns the same date and hour twice (the repeated hour when DST ends), the first row is kept and a warning is logged.
- Do not request future dates: cap `to` at today in the suite's zone.

#### Data latency and lookback

- Adobe data is processed continuously; recent data is available after a processing delay (Adobe's data availability technote gives about 90 minutes as normal and 2 hours for full processing [Docs]) and can still change after first being returned. Visit-level and visitor-level metrics (visits, unique visitors) for the latest day can shift because visits can span midnight and late-arriving hits are accepted [Unverified]. The exact Adobe figure for how long data settles is not stated in the sources reviewed; the default `lookback_days` of 3 is a conservative recommendation [Unverified]. Users with processing rules, bot rules, reprocessing jobs or classification uploads can see changes far beyond the lookback window; document that those require a manual full refresh.
- Do not run the pipeline more often than needed; Adobe recommends polling no more often than every 30 minutes [Docs].

#### Metric data types

- Report values arrive as JSON numbers (floats) regardless of metric type [Spec]. `GET /metrics` gives `type` (`INT`, `DECIMAL`, `CURRENCY`, `PERCENT`, `TIME`) and `precision`. Percent metrics are returned as fractions (0.25 for 25 percent) [confirmed against a live Adobe Analytics account]; time metrics are expected in seconds [Unverified; values seen live were consistent with seconds], and standard time metrics report `type` `int` although values have decimals [confirmed against a live Adobe Analytics account]; currency metrics are in the report suite's currency (`report_suites.currency`).
- Decision: all metric columns are `DoubleType`, nullable. Casting integer metrics to `LongType` is optional later using `metrics.type == INT && precision == 0`, but doubles avoid overflow and rounding surprises with calculated metrics.

#### Rate limits, timeouts, retries

- 12 requests every 6 seconds per user, about 120 per minute [Docs]; a limit of about 5 concurrent requests is [Unverified]. The throttled response is `429` with body `{"error_code": "429050", "message": "Too many requests"}` [Docs]; a `Retry-After` header (seconds) is [Unverified], so honor it when present.
- Request timeout: 60 seconds on the server side [Docs]. Large pages (50,000 rows with several metrics) may time out; on timeout, retry with a smaller `limit` (for example 10,000).
- Implement a client-side limiter (at most about 10 requests per 6 seconds for safety), honor `Retry-After` on `429` plus jitter, retry `500`, `502`, `503`, `504` with exponential backoff (up to about 5 attempts), and refresh the token once on `401` before failing.
- Use a single sequential worker per credential. Several simultaneous Spark partitions would multiply requests against the same per-user limit. Report tables should therefore be read in the driver or in a single partition per table.

#### Error formats [Docs/Spec]

- Typical body: `{"error_code": "429050", "message": "Too many requests"}`; others use `{"errorCode": "...", "errorDescription": "..."}` or `{"message": "..."}`. Parse defensively and include HTTP status, URL path and body text in the exception.
- `400`: invalid request (bad dimension or metric id, bad date range, invalid JSON). Do not retry, with one exception: a `400` on a list request that carries an `expansion` is retried once without the expansion (some columns are then empty, and a warning says so).
- `401`: bad or expired token, or no access to the report suite (`dimensions` documents "User does not have access to this report suite" as 401). Refresh the token once, then fail.
- `403`: no permission (for example `includeType=all` without administrator rights [Docs]; error `403025` "profile is not valid" [Unverified]). Do not retry.
- `206`: partial result with `columnErrors` (see above).
- Adobe error codes: `429050` [Docs]; `403025`, `500701`, `report_suite_invalid`, `segment_inaccessible`, `calculated_metric_invalid` [Unverified].

#### Things that commonly trip up a generic implementation

1. **One dimension per request.** There is no way to get date x dimension in a single ranked report without per-bucket requests or breakdown filters.
2. **Row order is not date order** in a ranked report; sort by the metric, not date. Always take the date from the bucket you requested.
3. **Non-additive metrics.** Unique visitors and ratios do not roll up. Never re-aggregate in the pipeline.
4. **`itemId` is a hash string for ordinary dimensions;** key dimension rows by the returned `value` (kept as `dimension_value`, with `itemId` stored as `dimension_item_id`). For time dimensions the `itemId` is a documented digit encoding of the bucket start (see Strategy A), and it is decoded instead of the locale-dependent label.
5. **List endpoints return only the technical account's own components** unless `includeType=all` (administrator) is given; metadata tables can look empty for non-admin credentials. Log which mode was used.
6. **Company ID is required in every path;** wrong company yields 404/403, not an empty list.
7. **`segments` rsid filter name differs between the spec (`rsids`) and the guide (`rsid`).** The connector does not filter segments by report suite, so it is unaffected.
8. **Dimension and metric ids differ between short form and full form:** report requests use `variables/evar1` and `metrics/visits`; the single-item endpoints (`/dimensions/{id}`, `/metrics/{id}`) take the short form (`evar1`, `visits`) [Spec]. The connector only uses the list endpoints.
9. **Metric and dimension availability is per report suite.** A metric valid in one rsid may be `not_enabled_metric` in another.
10. **Calculated metrics in report requests must be saved first.** Inline ad hoc calculated metric formulas are not supported in 2.0 [Unverified]; `metricDefinition` exists in the spec's `ReportMetric` but is out of scope here.
11. **Calculated metric and segment ids are tied to a company and often to an rsid;** applying a segment that is not accessible returns `segment_inaccessible`.
12. **The report-suite list may include report suites the credential cannot query;** skip with a warning.
13. **Spark nulls:** `data[i]` may be `null` for a bucket without data; keep as null, not zero.

## **Field Type Mapping**

| Adobe type / JSON | Spark type | Notes |
|---|---|---|
| string ids and names | StringType | |
| boolean | BooleanType | |
| integer (`precision`, `companyId`, `owner.id`) | LongType | all integers use LongType, following the repository's schema rules |
| ISO 8601 timestamp strings (`modified`, `created`, `createDate`) | TimestampType | parse with timezone offset when present; fall back to StringType if a source returns non-ISO text [Unverified] |
| arrays of strings (`categories`, `support`, `reportable`) | ArrayType(StringType) | |
| arrays/objects with variable shape (`definition`, `tags`, `shares`, `attributionModel`, `compatibility`) | StringType (JSON text) | avoid schema drift; users parse with `from_json` |
| report metric values | DoubleType | JSON numbers; nullable |
| report `date` | DateType | derived from the requested bucket |
| report `hour` | LongType | 0-23 |
| report `retrieved_at` | TimestampType | start of the run that read the row (UTC) |
| dimension `value` / `itemId` | StringType | |

Special behaviors:
- Metric/dimension ids in config use the full form (`metrics/...`, `variables/...`, `cm...`).
- Enumerations: dimension/metric `type` upper/lower-case varies by endpoint; normalize to lower case in output only if the framework needs it (default: keep as returned).
- Report column names are derived from the metric ids. A metric id listed twice in `metrics` is rejected; a derived name that clashes with another metric's name or with a fixed column (`rsid`, `date`, `hour`, `dimension_value`, `dimension_item_id`, `segment_ids`, `retrieved_at`) gets a numeric suffix, for example `visits_2`.

## **Sources and References**

| Source | Used for | Confidence |
|---|---|---|
| Adobe's published OpenAPI (Swagger 2.0) spec: https://github.com/AdobeDocs/analytics-2.0-apis/blob/main/static/swagger.json (rendered at https://developer.adobe.com/analytics-apis/docs/2.0/apis/) | endpoints, query parameters, response definitions, `ReportFilter` types, `RankedSettings`, `columnErrors` codes, metric `type` values | Highest. Note: it does not include the `reportsuites/*` endpoints and does not type the `/segments`, `/calculatedmetrics`, `/dateranges` list responses as arrays |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/faq | rate limit, `429` body, 60-second timeout, 30-minute polling guidance | High (official) |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/reports/overview, https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/reports/kpi and https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/reports/examples | `nonesBehavior` values, `daterangeday`, the `Unspecified` row | High (official) |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/reports/breakdowns | one request per parent item, `itemValue` | High (official) |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/reports/real-time | 20-hour window; no segments or calculated metrics | High (official) |
| https://developer.adobe.com/developer-console/docs/guides/authentication/ServerToServerAuthentication/implementation and https://developer.adobe.com/developer-console/docs/guides/authentication/UserAuthentication/ims | token lifetime; Adobe Analytics scopes | High (official) |
| https://experienceleague.adobe.com/en/docs/analytics/technotes/latency | data processing latency | High (official) |
| Read-only live smoke check (`tests/unit/sources/adobe_analytics/live_smoke_check.py`) | claims marked "confirmed against a live Adobe Analytics account" | High for the account tested; behavior can vary by report suite configuration |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/report-suites | report suite paths, params (`limit`, `page`, `rsids`, `rsidContains`, `expansion`), fields | High (official) |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/vrs/ | VRS list path only | High for path; params unverified |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/segments/ | segments params and expansion values | High (official) |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/calculatedmetrics/ | calculated metric params, expansions, sortProperty | High (official) |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/discovery | discovery URL and response | High (official) |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/reports/ | date range formats, no-dimension response | High (official) |
| https://developer.adobe.com/analytics-apis/docs/2.0/guides/jwt | JWT end of life 2025-06-30 | High (official) |
| https://fivetran.com/docs/connectors/applications/adobe-analytics | Fivetran's reporting connector builds custom tables from elements, metrics, calculated metrics and segments with `report_suite_id` and `datetime` keys, and queries elements sequentially because of API limits | Medium (vendor documentation) |
| `aanalytics2` (https://github.com/pitchmuc/adobe-analytics-api-2.0) and `adobeanalyticsr` packages | report suite expansion names; list envelope shape | Medium |
| Airbyte | No Adobe Analytics 2.0 source documentation was found during this research, so no stream list was used | n/a |

Conflicts and how they were resolved:
- `/segments` report suite filter: spec says `rsids`, guide says `rsid`. The connector sends neither, so the conflict does not affect it; prefer the spec if a filter is added later.
- List response envelope: guide examples show arrays, spec shows single objects, libraries read `content`. Parse all shapes.
- Token scope set: Adobe's authentication reference lists `openid, AdobeID, read_organizations, additional_info.projectedProductContext` for Adobe Analytics (plus `additional_info.job_function` for user authentication) [Docs]; the default set without `job_function` is [confirmed against a live Adobe Analytics account]. Keep `scopes` overridable because a credential's scopes can differ.
- Adobe's docs show the `x-api-key` and `Authorization` headers [Docs]; community guidance adds `x-proxy-global-company-id`. Both are sent.

### Items that must be verified against a live account before finalizing the implementation
1. ~~Exact list response envelope for each paged endpoint and the maximum `limit`.~~ Resolved, confirmed against a live Adobe Analytics account: an object with `content`, `lastPage`, `number`, `numberOfElements`, `size`, `totalElements` and `totalPages`; `limit=1000` is accepted.
2. ~~`itemId` formats for time dimensions.~~ Resolved, confirmed against a live Adobe Analytics account: century digit + YY + **zero-based** MM (00 = January) + DD, plus HH for hours; month rows use DD = 01. Each decoded date matched Adobe's label for that row. Week rows carry their true start date.
3. ~~Behavior of `nonesBehavior` values and the "Unspecified" literal.~~ Resolved, confirmed against a live Adobe Analytics account (English labels): `return-nones` adds one row labelled `Unspecified`.
4. Whether `expansion` names above are all accepted for report suites and VRS.
5. ~~Percent metric units.~~ Resolved, confirmed against a live Adobe Analytics account: percent metrics are fractions (0.25 = 25%). Time metrics are typed `int` and arrive with decimals [confirmed against a live Adobe Analytics account]; that they are in seconds is [Unverified].
6. ~~Whether a virtual report suite id works as `rsid` in `/reports`.~~ Resolved, confirmed against a live Adobe Analytics account: a daily report table read with a virtual report suite id as `rsid` returned rows that match the schema.
7. Maximum number of metrics per request.
8. Default token scopes against a freshly created OAuth Server-to-Server credential.
9. Units of time metrics: whether they are in seconds. Values seen live are consistent with seconds, and Adobe types them `int` although they arrive with decimals (see item 5).
10. Which `nonesBehavior` setting the "Include unspecified" option in Analysis Workspace maps to (see "Unspecified" / None rows above).

## **Deferred Tables**

| Table / endpoint | Why deferred |
|---|---|
| Multi-dimension report (itemId breakdown) | Request count grows with the number of parent rows; needs `max_parent_items` and a partial-data policy (see above) |
| Date x dimension at `week`/`month` granularity | Requires live verification of bucket boundaries and calendar types |
| `annotations` (`GET /annotations`) | Not requested; small snapshot table, straightforward later |
| `projects` (`GET /projects`) | Workspace project definitions; large JSON, not requested |
| `users`, `users/me` | Admin-only, not requested |
| `tags`, `shares` (`/componentmetadata/...`) | Component metadata, not requested |
| `auditlogs/usage` | Requires date filters and admin rights; separate pattern |
| `reports/realtime` | Segments and calculated metrics unsupported, 20-hour window; not suited for batch ingestion |
| `reports/topItems` | Redundant with ranked reports |
| Classification datasets, Data Warehouse, Data Feeds (raw hit data) | Different APIs and delivery mechanisms; Fivetran treats the data feed as a separate connector |
