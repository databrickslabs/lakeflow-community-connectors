# Lakeflow Adobe Analytics Community Connector

This documentation describes how to set up and use the **Adobe Analytics** Lakeflow community connector to bring Adobe Analytics report data and component definitions into Databricks through the Adobe Analytics 2.0 API.

The connector is read-only and produces two kinds of tables:

- **Reference tables** (seven fixed names) list the building blocks of your Adobe Analytics setup: report suites, virtual report suites, dimensions, metrics, segments, calculated metrics and date ranges.
- **Report tables** (as many as you need, with names you choose) hold metric values by date, and optionally by hour and by one dimension, for one report suite. You describe each report with table options, much like a freeform table in Analysis Workspace.

> **Read this before you sum anything.** Adobe Analytics report rows are not always additive. Unique Visitors, rates and some visit-based metrics give wrong answers if you add up daily rows into weekly, monthly or all-time totals. See [Summing metrics: what is safe and what is not](#summing-metrics-what-is-safe-and-what-is-not).

### Terms used in this guide

| Term | Meaning |
|---|---|
| Report suite | A separate data set in Adobe Analytics, usually one per website or app. Identified by a report suite id (`rsid`), for example `examplersid`. |
| Virtual report suite | A filtered view of a report suite defined by one or more segments. Its id usually starts with `vrs_`, for example `vrs_examplersid`. |
| Dimension | An attribute you break data down by, such as a page name or a custom variable. In the API it has an id like `variables/evar1` or `variables/page`. |
| Metric | A number you measure, such as visits or page views. In the API it has an id like `metrics/visits`. |
| Segment | A saved filter, for example "visits from mobile devices". Its id looks like `s000000000_000000000000000000000000`. |
| Calculated metric | A saved formula built from other metrics, for example a conversion rate. Its id starts with `cm`. |
| Technical account | The non-human "user" that Adobe creates for an API credential. It needs access to report suites just like a person does. |
| Product profile | A set of Adobe Analytics permissions (report suites, metrics, dimensions) assigned to users and technical accounts in Adobe Admin Console. |
| Global company ID | The id Adobe uses for your Analytics company in API addresses. Not the same as your organization id. |

## Prerequisites

- **Adobe Analytics** with access to the report suites you want to read.
- **Access to Adobe Developer Console** (`https://developer.adobe.com/console`) with permission to create a project in your Adobe organization. Your organization may require a System Administrator or Developer role for this.
- **Access to Adobe Admin Console** (`https://adminconsole.adobe.com`), or someone who has it, to check the product profile the credential uses.
- **An OAuth Server-to-Server credential** with the Adobe Analytics API added (created in the steps below).
- **Network access** from Databricks to `https://ims-na1.adobelogin.com` (sign-in) and `https://analytics.adobe.io` (data).
- **A Databricks workspace** where you can create a Unity Catalog connection and run Lakeflow pipelines.

> **JWT (Service Account) credentials are not supported.** Adobe stopped issuing JWT credentials on 2024-06-03 and ended support for them on 2025-06-30. If your organization still has a JWT credential for Analytics, add an OAuth Server-to-Server credential to the same Developer Console project (Adobe offers a migration option on the credential page) and use that with this connector.

## Setup

### Step 1: Create an OAuth Server-to-Server credential in Adobe Developer Console

An OAuth Server-to-Server credential lets the connector sign in as a technical account without a browser or a person present. You supply a client ID and client secret; the connector requests an access token from Adobe on every run and renews it automatically. You never paste a token.

1. Open [Adobe Developer Console](https://developer.adobe.com/console) and sign in to the Adobe organization that owns your Analytics data.
2. Select **Create new project** (or open an existing project you use for integrations). Give it a clear name, for example "Databricks Analytics ingestion".
3. Select **Add API**, choose **Adobe Analytics**, and select **Next**.
4. When asked for the authentication type, choose **OAuth Server-to-Server**.
5. Select one or more **product profiles**. The technical account gets exactly the access these profiles grant, so choose a profile that includes every report suite, metric and dimension you plan to ingest.
6. Select **Save configured API**.
7. Open the new **OAuth Server-to-Server** credential in the project and copy:
   - **Client ID**: becomes the `client_id` connection parameter.
   - **Client secret** (select **Retrieve client secret**): becomes the `client_secret` connection parameter. Store it in a secure place.
8. On the same page, note the listed **Scopes**. The connector requests `openid,AdobeID,read_organizations,additional_info.projectedProductContext` by default. If your credential lists a different set, put that list in the optional `scopes` connection parameter.

If you later rotate the client secret in Developer Console, update the connection with the new secret. The old one stops working.

### Step 2: Confirm the technical account's access in Adobe Admin Console

The connector can only read what the technical account is allowed to see. A missing permission shows up as an HTTP 401 or 403 error, or as a report column error.

1. Open [Adobe Admin Console](https://adminconsole.adobe.com), go to **Products**, then **Adobe Analytics**, and open the product profile you selected in Step 1.
2. On the profile's **API credentials** tab, confirm that the technical account from your Developer Console project is listed.
3. On the profile's **Permissions** tab, confirm that it includes:
   - every report suite (and virtual report suite) you want to read,
   - the metrics and dimensions you plan to use in report tables (including any custom variables such as eVars and success events).
4. **Segments and calculated metrics used in report tables** must be available to the technical account. A segment it cannot access fails with an error such as `segment_inaccessible`. Either share the segment with the technical account in Adobe Analytics, or give the technical account administrator rights (next step).
5. **Optional, for the complete component lists:** the `segments`, `calculated_metrics` and `date_ranges` reference tables return every component in the company only when the technical account has Adobe Analytics administrator rights. Without them, Adobe returns only components shared with or owned by the technical account (often very few). To grant administrator rights, add the technical account as a product administrator for Adobe Analytics in Admin Console, if your organization's policy allows it. Ask your Adobe administrator if you are unsure.

### Step 3: Find your global company ID (only if needed)

Most setups can skip this step. When `global_company_id` is left blank, the connector asks Adobe which Analytics companies the credential can reach and uses that company if there is exactly one.

Set `global_company_id` only when:

- the credential has access to **more than one** Analytics company. The connector then stops with an error that lists every available ID and company name; copy the one you want from that message; or
- you want to pin the connection to a specific company.

You can also find the ID in Adobe Analytics under **Admin > Company Settings > API Access** (the exact menu path can vary by Analytics version).

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `client_id` | string | yes | Client ID of the OAuth Server-to-Server credential (Step 1). | `0123456789abcdef0123456789abcdef` |
| `client_secret` | string | yes | Client secret of the same credential. Stored as a secret. | `p8e-XXXXXXXXXXXXXXXX` |
| `global_company_id` | string | no | Analytics company to read. Leave blank to detect it automatically (Step 3). | `examplecompany0` |
| `scopes` | string | no | Comma-separated OAuth scopes. Leave blank for the default `openid,AdobeID,read_organizations,additional_info.projectedProductContext`. Change it only if your credential page lists different scopes. | `openid,AdobeID,read_organizations,additional_info.projectedProductContext` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table options the connection passes through to the connector. Required, because report tables are configured entirely through table options. Use the exact value below. | see below |

Set `externalOptionsAllowList` to this exact value:

```
rsid,metrics,dimension,segment_ids,granularity,start_date,lookback_days,nones_behavior,count_repeat_instances,max_rows_per_bucket,limit,max_records_per_batch,delete_stale_rows,max_tracked_keys,include_type
```

> Table options such as `rsid`, `metrics` or `include_type` are **not** connection parameters. You set them per table in the pipeline spec (see [Table Configurations](#table-configurations)). They only reach the connector if their names are in `externalOptionsAllowList`.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Enter `client_id`, `client_secret` and, if needed, `global_company_id` and `scopes`. Set `externalOptionsAllowList` to the value shown above.

The connection can also be created using the standard Unity Catalog API.

Creating the connection does not open a browser or ask anyone to sign in. The connector authenticates machine-to-machine with the client ID and secret, so there is no redirect URI to register.

## Supported Objects

### Reference tables

These seven names are reserved. Each one is ingested as a **snapshot**: the whole list is read again on every run and replaces the previous copy, so items deleted in Adobe also disappear from the table.

| Table | What it holds | Primary key | Table options |
|---|---|---|---|
| `report_suites` | Every report suite the credential can see, with name, currency, time zone (`timezone_zoneinfo`) and calendar settings (`calendar_type`). | `rsid` | none |
| `virtual_report_suites` | Every virtual report suite, with its parent report suite, time zone, the segments that define it (`segment_list`), last-modified time and the full Adobe record as JSON (`raw`). | `id` | none |
| `dimensions` | The dimensions available in each report suite. The `id` column is the value to use in a report table's `dimension` option. | `rsid`, `id` | `rsid` (optional) |
| `metrics` | The standard metrics available in each report suite, with type, precision and polarity. The `id` column is the value to use in a report table's `metrics` option. | `rsid`, `id` | `rsid` (optional) |
| `segments` | Saved segments: name, owner, report suite, full definition as JSON text (`definition`), and when it was created, modified and last had its definition changed (`modified`, `definition_last_modified`). | `id` | `include_type` (optional) |
| `calculated_metrics` | Saved calculated metrics: name, owner, type (decimal, percent, currency, time), full formula as JSON text (`definition`), and `created` / `modified` times. Built-in templates have `template = true`. | `id` | `include_type` (optional) |
| `date_ranges` | Saved date range components, with their definition, owner and modified time. | `id` | `include_type` (optional) |

**Options for reference tables**

- **`rsid`** (for `dimensions` and `metrics`): one report suite id, or a comma-separated list such as `examplersid,vrs_examplersid`. When omitted, the connector reads every report suite in `report_suites`, sending one request per report suite. Report suites the credential cannot access are skipped with a warning in the pipeline log. When you list report suites explicitly, an access error stops the table instead, so you notice the problem.
- **`include_type`** (for `segments`, `calculated_metrics` and `date_ranges`): which components Adobe lists.
  - Not set (default): the connector first asks for `all`, which returns every component in the company. If Adobe refuses (HTTP 403, because `all` needs administrator rights), it logs a warning and tries `shared`. If that is also refused, it lists only the components the technical account owns.
  - `all`, `shared`, `templates`, or a comma-separated combination such as `shared,templates`: passed to Adobe as given, with no fallback.
  - `none`: list only components the technical account owns.

  Why `all` matters: without it, Adobe lists only components owned by the technical account (which usually owns nothing) or shared with it. A `segments` table with only a handful of rows usually means the technical account lacks administrator rights. The pipeline log records which mode was used.

### Report tables

Any `source_table` name that is **not** one of the seven reference table names defines a report table. Choose a descriptive name such as `daily_site_traffic` or `hourly_visits_by_evar1`. Each report table:

- reads one report suite (or virtual report suite),
- holds one or more metrics,
- is split by date (and by hour when `granularity` is `hour`),
- is optionally broken down by one dimension and filtered by one or more segments.

A report table needs at least the `rsid` and `metrics` options. All options are described in [Table Configurations](#report-table-options).

**Columns**

| Column | Type | Present | Meaning |
|---|---|---|---|
| `rsid` | STRING | always | The report suite id from the `rsid` option. |
| `date` | DATE | always | The day of the row, in the report suite's own time zone. For `week` and `month` tables, the first day of that week or month. |
| `hour` | BIGINT | hourly tables | Hour of the day, 0 to 23, in the report suite's time zone. |
| `dimension_value` | STRING | with a dimension | The dimension value as Adobe displays it. Includes an "Unspecified" row (Adobe may label it "None" for some dimensions) unless `nones_behavior` is `exclude`. |
| `dimension_item_id` | STRING | with a dimension | Adobe's internal id for the dimension value. |
| `segment_ids` | STRING | always | The segment ids from the `segment_ids` option, sorted and joined with commas. Empty string when no segment is used. |
| `retrieved_at` | TIMESTAMP | always | When the pipeline run that wrote this row started (UTC). Every row from one run has the same value. Use it to see how fresh a row is. |
| one column per metric | DOUBLE | always | The metric value. Empty (null) means Adobe returned no value, not zero. |

Metric columns are named after the last part of the metric id, in lower-case words separated by underscores: `metrics/visits` becomes `visits`, `metrics/pageviews` becomes `pageviews`, `metrics/event1` becomes `event1`, and a calculated metric id such as `cm000000000_000000000000000000000000` keeps its id as the column name. If two metrics would get the same name, or a name matches one of the columns above, the later one gets a numeric suffix such as `_2`.

**Primary key:** `rsid`, `date`, then `hour` (hourly tables), then `dimension_value` (tables with a dimension), then `segment_ids`.

**Ingestion type:** `cdc` (rows are updated in place by primary key). Report tables with a dimension and `delete_stale_rows` turned on (the default) use `cdc_with_deletes`, which also removes rows that no longer appear in Adobe. `retrieved_at` decides which copy of a row is newest, so a re-read always replaces the older copy.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system. Use a reference table name, or any other name for a report table. |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Reference table options

| Option | Applies to | Required | Default | Description |
|---|---|---|---|---|
| `rsid` | `dimensions`, `metrics` | no | every report suite | One report suite id or a comma-separated list. |
| `include_type` | `segments`, `calculated_metrics`, `date_ranges` | no | `all`, falling back to `shared`, then owned-only | Which components to list. See [Reference tables](#reference-tables). |

### Report table options

All values are strings in the pipeline spec. Only `rsid` and `metrics` are required.

| Option | Required | Default | Description |
|---|---|---|---|
| `rsid` | yes | none | Report suite id, for example `examplersid`. Virtual report suite ids such as `vrs_examplersid` also work (confirmed against a live Adobe Analytics account). The `report_suites` and `virtual_report_suites` tables list the ids your credential can use. |
| `metrics` | yes | none | Metric ids, as a comma-separated list (`metrics/visits,metrics/pageviews`) or a JSON array (`["metrics/visits","metrics/pageviews"]`). Use the full id form `metrics/...`. Saved calculated metric ids (`cm...`) are allowed. Each id may appear only once. |
| `dimension` | no | none | One dimension id, for example `variables/evar1`. Use the full id form `variables/...`. Leave it out to get one row per day (or hour, week, month) with totals for the whole report suite. Do not use time dimensions such as `variables/daterangeday`; set `granularity` instead. |
| `segment_ids` | no | none | Segment ids applied as filters to the whole report, comma-separated. When you list several, a hit must match all of them. |
| `granularity` | no | `day` | Size of each time bucket: `day`, `hour`, `week` or `month`. `week` and `month` work only without a dimension. Weeks and months follow the report suite's own calendar settings, so a week may start on Sunday or Monday. |
| `start_date` | no | 30 days before the first run | First date to ingest, as `YYYY-MM-DD`. Used on the first run only. For `week` and `month` tables, choose the first day of a week or month: a partial week or month at the start is skipped. A start date in the future reads nothing until that date arrives. |
| `lookback_days` | no | `3` | How many days before the last ingested date each run reads again. Adobe keeps revising recent data for a while (late-arriving hits, visits that are still open, processing delays), so re-reading the last few days keeps the table in line with Adobe. `0` re-reads only from the last ingested date onward. For `week` tables, the value is rounded up to whole weeks. |
| `nones_behavior` | no | `return` | `return` keeps the "Unspecified" row: hits where the dimension had no value. This keeps your totals in line with Analysis Workspace. `exclude` drops that row. Only used with a dimension. |
| `count_repeat_instances` | no | `true` | Matches the Analysis Workspace setting of the same name, which is on by default. With `true`, a dimension value that is recorded several times in a row (for example the same page reloaded) counts every time. With `false`, back-to-back repeats of the same value count once. This mainly changes instance-type metrics such as Occurrences and the instances of a dimension. Keep `true` unless your Workspace projects turn it off. |
| `max_rows_per_bucket` | no | no cap | Keep only the top N dimension values per day (or hour), ranked by the first metric in `metrics`. Use it to limit very large dimensions such as page URLs or search terms. Capped data is incomplete: rows outside the top N are missing and totals no longer match Adobe. |
| `limit` | no | `50000` | Rows per API page, from 1 to 50000. If pages of this size keep timing out, the connector retries automatically with 10000 rows per page. Rarely needs changing. |
| `max_records_per_batch` | no | `100000` | During a long backfill, the connector commits progress after roughly this many rows (always at the end of a whole day). If a run is interrupted, the next run continues from the last committed day. |
| `delete_stale_rows` | no | `true` | With a dimension, remove rows for dimension values that disappear from Adobe when recent days are re-read. Set `false` to keep rows once written and save some requests. See [Stale rows and full refresh](#stale-rows-and-full-refresh). Has no effect without a dimension. |
| `max_tracked_keys` | no | `10000` | Upper limit on how many dimension values the connector remembers for stale-row removal. When the limit is reached, the oldest days in the lookback window are not checked and a warning is logged; values that disappear from those days stay in the table until a full refresh. |

## How a Report Table Is Read

1. **Time zone.** The connector reads the report suite's time zone from Adobe (a virtual report suite uses its own time zone or its parent's). All dates and hours in the table, and the meaning of "today", use that time zone, not UTC and not the cluster's time zone. If the time zone cannot be read, UTC is used and a warning is logged.
2. **Date window.** The first run reads from `start_date` through today. Each later run reads from `lookback_days` before the last ingested date through today. Today is still in progress, so its numbers are partial; the next run reads it again and replaces them.
3. **Without a dimension**, the connector asks Adobe for a trended report (one row per time bucket) covering the whole window in one request. Hourly tables are requested in 31-day chunks, daily tables in chunks of up to 366 days.
4. **With a dimension**, Adobe cannot return "date by dimension" in one request, so the connector sends **one request per day** (or **one per hour** for hourly tables), with that day or hour as the date filter. Each request returns every value of the dimension for that bucket, sorted by the first metric. The row's `date` and `hour` come from the bucket that was requested.
5. **Paging.** Each request is read page by page (up to `limit` rows per page) until Adobe reports the last page.
6. **Dates are decoded from Adobe's internal item id**, not from the display label (such as "Jan 1, 2026"), because the label depends on the language and locale settings of the account. If Adobe returns an item id in a format the connector does not recognize, the table stops with a clear error rather than writing wrong dates.
7. **Daylight saving time.** On the day clocks change, an hourly table can have one hour with no data (spring) or a repeated hour (autumn). For a repeated hour without a dimension, the connector keeps the first row and logs a warning.
8. **Partial results are never written.** If Adobe cannot compute one of the metric columns (HTTP 206 with column errors), the table stops with Adobe's explanation instead of writing empty values.

## Request Volume and Rate Limits

Adobe allows **12 API requests per 6 seconds per user**, and the technical account counts as one user. The connector stays a little below that, at 10 requests per 6 seconds, which is about 100 requests per minute. It sends requests one at a time.

Request counts depend mostly on whether a report table has a dimension:

| Report table | First run (30-day backfill) | Each later run (`lookback_days` = 3) |
|---|---|---|
| Daily, no dimension | about 1 request | about 1 request |
| Hourly, no dimension | about 1 to 2 requests | about 1 request |
| Daily, with a dimension | about 31 requests (one per day) | about 4 requests, plus about 4 more for stale-row checks |
| Hourly, with a dimension | about 744 requests (31 days x 24 hours) | about 96 requests, plus about 96 more for stale-row checks |

These counts assume each bucket fits on one page. A dimension with more than 50000 values in a day needs more pages per bucket.

**Worked example.** A 30-day hourly backfill with a dimension is 31 days x 24 hours, or about 720 to 750 requests. At about 100 requests per minute, that takes **7 minutes or more** before any retries. A one-year hourly backfill with a dimension is about 8,800 requests, or roughly an hour and a half. By comparison, a daily incremental run with a dimension needs fewer than 10 requests and finishes in seconds.

The connector also makes a few small requests per run to look up the report suite's time zone and to check your metric and dimension ids.

**When Adobe answers 429 (too many requests)**, the connector waits for the time Adobe asks for and retries, up to 5 attempts. Server errors (500, 502, 503, 504) and timeouts are retried the same way with increasing waits.

**Several tables share one budget.** Adobe's limit applies to the credential as a whole. Each table paces itself, but several report tables, pipelines or other tools using the same credential at the same time compete for the same 12 requests per 6 seconds and will see more 429 responses and slower runs.

**Advice for large backfills**

- Start with a short `start_date` (a few days) to check the table's shape and numbers, then do a full refresh with the real `start_date`.
- Prefer `day` over `hour` for long history. If you need hourly data, consider an hourly table with a recent `start_date` next to a daily table with long history.
- For dimensions with very many values, set `max_rows_per_bucket` if the top values are enough for your use case.
- Run large backfills when no other jobs use the same credential, and do not start several large backfills at once.
- Leave `max_records_per_batch` at its default so long backfills commit progress regularly and can resume after an interruption.

## Summing Metrics: What Is Safe and What Is Not

Each row holds the value Adobe calculated for that exact day (or hour) and dimension value. Some metrics give correct totals when you add rows together; others do not.

> **Warning: deduplicated metrics cannot be summed across dates.** Metrics that count each person or visit once, such as **Unique Visitors** (`metrics/visitors`), are deduplicated within the time period Adobe calculated them for. A visitor who comes on Monday and Tuesday appears in both daily rows. Adding 7 daily rows counts that person twice and overstates weekly Unique Visitors, often by a lot.
>
> To get weekly, monthly or whole-period Unique Visitors, create a **separate report table at the coarser granularity** (for example `granularity` = `week` or `month`, without a dimension) and read the value from it. Do not add up daily or hourly rows.

Other effects to know about:

- **Visit-based metrics run slightly high when summed across days.** A visit that crosses midnight counts in both days, so the sum of daily Visits is a little higher than Visits for the whole period in Analysis Workspace. The same applies to summing hours into a day.
- **Visit-based metrics broken down by a dimension can add up to more than the total.** If a dimension can take several values within one visit (for example a page or a campaign variable that changes during the visit), the visit counts once for each value. The sum across dimension values is then higher than the report suite's total Visits. Visitor-based metrics behave the same way.
- **Rates and ratios must not be summed or averaged.** This covers bounce rate, conversion rates, averages such as time spent per visit, and any calculated metric that divides one metric by another. Adding rows gives a meaningless number, and a simple average of daily rates gives the wrong answer because days with little traffic count as much as busy days. Instead, ingest the parts of the formula (for example bounces and entries, or orders and visits), sum those, and divide afterwards. Only do this when the parts are themselves safe to sum.
- **Counts of hits are generally safe to sum** across dates and dimension values: page views, occurrences, success events (`metrics/event1` and similar), orders, units and revenue.
- **Capped tables are incomplete.** If `max_rows_per_bucket` is set, totals across dimension values are lower than Adobe's.

When in doubt, compare your SQL total with the same report in Analysis Workspace for the same dates, segment and time zone.

## Stale Rows and Full Refresh

### How stale rows are removed

When a recent day is read again, a dimension value that appeared before may be missing from the new result: for example after Adobe reprocesses data, after bot filtering removes the only hits for a value, or when `max_rows_per_bucket` keeps a different top N. Without cleanup, the old row would stay in the table.

For report tables with a dimension and `delete_stale_rows` = `true` (the default), the connector:

1. Remembers which dimension values it saw for every day (or hour) in the lookback window.
2. On the next run, reads those days again and compares.
3. Deletes the rows for values that are no longer returned.

Limits of this cleanup:

- It only checks days inside the lookback window. Older days are never re-checked automatically.
- It remembers at most `max_tracked_keys` dimension values (default 10000). For large dimensions or long lookback windows, the oldest days in the window are not checked once the limit is reached, and a warning is logged. Raise `max_tracked_keys`, lower `lookback_days`, or turn the cleanup off.
- It costs about one extra request per day (or hour) in the lookback window per run. See [Request Volume and Rate Limits](#request-volume-and-rate-limits).
- A value that changes in the few seconds between the two reads of a run can be missed until the next run.

To turn it off, set `delete_stale_rows` to `false`. Rows are then only added or updated, never removed. Tables without a dimension do not need it: every day always has exactly one row.

### When to run a full refresh

A full refresh clears a table and the connector's saved progress for it, so the next run reads everything again from `start_date`. In a Databricks pipeline, select the table and choose **Full refresh selection** (or refresh the whole pipeline).

Run a full refresh of the affected report tables when:

- **A segment used in `segment_ids` was edited.** Adobe applies segment changes to all past dates, but the connector only re-reads the lookback window, so older rows would still reflect the old definition.
- **A calculated metric used in `metrics` was edited**, for the same reason.
- **You changed `metrics` or `dimension`.** The table's columns or primary key change, and existing rows would not match the new definition.
- **You changed `start_date`.** The new date only takes effect after a full refresh.
- **You changed `rsid`, `segment_ids`, `granularity`, `nones_behavior` or `count_repeat_instances`.** Old rows were calculated differently or have different keys, and would remain next to the new ones.
- **Adobe data older than the lookback window changed**: for example data was reprocessed, processing rules or bot rules were changed and applied to history, or a virtual report suite's definition was edited.

Changing `lookback_days`, `limit`, `max_records_per_batch`, `delete_stale_rows` or `max_tracked_keys` does not require a full refresh.

**Spotting edits to segments and calculated metrics.** Ingest the `segments` and `calculated_metrics` reference tables in the same pipeline and watch their `modified` column (segments also have `definition_last_modified`). For example:

```sql
SELECT id, name, modified
FROM segments
WHERE id IN ('s000000000_000000000000000000000000')
  AND modified >= current_date() - INTERVAL 1 DAY;
```

If a segment or calculated metric used by a report table shows a recent `modified` time, check whether its definition changed and, if so, run a full refresh of the report tables that use it.

## Data Type Mapping

| Adobe Analytics value | Databricks type | Notes |
|---|---|---|
| Ids, names, descriptions, report suite ids | STRING | |
| True/false flags (`segmentable`, `template`, `allowed_for_reporting`) | BOOLEAN | |
| Whole numbers (`precision`, `owner_id`, `company_id`) | BIGINT | |
| Date-time values (`modified`, `created`, `definition_last_modified`) | TIMESTAMP | Converted from Adobe's ISO 8601 text. Text that cannot be read becomes null. |
| Lists of strings (`categories`, `support`, `reportable`, `segment_list`) | ARRAY&lt;STRING&gt; | |
| Fixed small objects (`calendar_type`, `none_settings`) | STRUCT | |
| Objects whose shape varies (`definition`, `tags`, `shares`, `attribution_model`, `usage_summary`, `publishing_status`, `raw`) | STRING containing JSON | Parse with `from_json` or `:` path syntax in SQL when needed. |
| Report `date` | DATE | In the report suite's time zone. |
| Report `hour` | BIGINT | 0 to 23. |
| Report metric values | DOUBLE | Adobe returns all report values as decimal numbers. Null means no value. Currency metrics use the report suite's currency (`report_suites.currency`). |
| Report `retrieved_at` | TIMESTAMP | Start of the pipeline run, UTC. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the Adobe Analytics source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Reference your Unity Catalog connection and add one `table` entry per table. Put table options under `table_configuration`. Give each report table its own unique `source_table` name.

The example below ingests one reference table and two report tables:

- `segments`: the segment list, used to watch for segment edits.
- `daily_site_traffic`: daily totals for the whole report suite, with no dimension.
- `daily_visits_by_evar1`: daily values for each value of a dimension, filtered by a segment.

```json
{
  "pipeline_spec": {
    "connection_name": "adobe_analytics_connection",
    "objects": [
      {
        "table": {
          "source_table": "segments"
        }
      },
      {
        "table": {
          "source_table": "daily_site_traffic",
          "table_configuration": {
            "rsid": "examplersid",
            "metrics": "metrics/visits,metrics/visitors,metrics/pageviews",
            "granularity": "day",
            "start_date": "2026-01-01"
          }
        }
      },
      {
        "table": {
          "source_table": "daily_visits_by_evar1",
          "table_configuration": {
            "rsid": "examplersid",
            "metrics": "metrics/visits,metrics/pageviews,metrics/event1",
            "dimension": "variables/evar1",
            "segment_ids": "s000000000_000000000000000000000000",
            "granularity": "day",
            "start_date": "2026-01-01",
            "lookback_days": "3"
          }
        }
      }
    ]
  }
}
```

`daily_site_traffic` has the columns `rsid`, `date`, `segment_ids`, `retrieved_at`, `visits`, `visitors` and `pageviews`, with one row per day. `daily_visits_by_evar1` adds `dimension_value` and `dimension_item_id` and has one row per day and dimension value. Remember that `visitors` (Unique Visitors) must not be summed across days.

To find valid ids, ingest the `metrics`, `dimensions`, `calculated_metrics` and `segments` reference tables first and query them, for example:

```sql
SELECT id, title FROM dimensions WHERE rsid = 'examplersid' ORDER BY id;
```

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

On the first run, report tables read from `start_date` (or the last 30 days) through today. Later runs read the lookback window and today. Reference tables are read in full every run.

**How to tell a run succeeded:** the pipeline finishes without errors, each report table has rows up to today's date in the report suite's time zone, and the newest `retrieved_at` matches the run's start time. Warnings in the pipeline log (for example an `include_type` fallback, a skipped report suite, or the `max_tracked_keys` limit) do not stop the run but are worth reading.

#### Best Practices

- **Start small**: begin with the reference tables and one daily report table with a recent `start_date`, and check the numbers against Analysis Workspace before adding more.
- **Schedule daily or a few times a day.** Adobe advises against polling more often than every 30 minutes, and recent data keeps changing for a while anyway.
- **Keep the default `lookback_days` of 3** unless you know your data settles faster or slower. A larger value costs more requests on tables with a dimension.
- **Prefer daily tables over hourly tables with a dimension**, which need 24 times as many requests.
- **Use one report table per question.** Each table has one report suite and at most one dimension. For a coarser time grain (weekly or monthly Unique Visitors), add a separate table rather than summing a daily one.
- **Limit `dimensions` and `metrics` with `rsid`** if your company has many report suites; otherwise those tables send one request per report suite every run.
- **Avoid running many large tables at once with the same credential**, because they share one rate limit.

#### Troubleshooting

**Sign-in fails: "Adobe rejected the OAuth credentials"**
- Check `client_id` and `client_secret` against the OAuth Server-to-Server credential in Developer Console. A rotated secret must be updated in the connection.
- If you set `scopes`, make sure it matches the scopes listed on the credential page. Clear it to use the default.
- JWT (Service Account) credentials do not work. Use an OAuth Server-to-Server credential.

**"This credential has no access to any Adobe Analytics company"**
- The technical account is not in any Adobe Analytics product profile. Add it in Admin Console (Setup, Step 2), or check that the Developer Console project selected a product profile.

**"This credential can access more than one Adobe Analytics company"**
- Copy one of the listed IDs into the `global_company_id` connection parameter.

**HTTP 401 or 403 on a table**
- The product profile does not include the report suite, metric or dimension. Check the profile's Permissions tab in Admin Console.
- For `dimensions` and `metrics` without `rsid`, report suites without access are skipped with a warning. With `rsid` set, the table stops so you can fix the access.
- For `segments`, `calculated_metrics` and `date_ranges`, a 403 for `includeType=all` is expected without administrator rights. The connector falls back automatically and logs a warning. If you set `include_type` yourself, there is no fallback; remove it or choose `shared`.
- `segment_inaccessible`: a segment in `segment_ids` is not shared with the technical account. Share it, or grant administrator rights.

**HTTP 429 (too many requests)**
- The connector waits and retries automatically. Repeated 429 errors usually mean other tables, pipelines or tools use the same credential at the same time. Stagger schedules, or use a separate credential for this pipeline.

**"Adobe could not compute some columns of report table ..." (HTTP 206)**
- One or more metrics or the dimension are not enabled in this report suite, or the product profile does not allow them. The message names the metric and Adobe's error code (for example `not_enabled_metric` or `unauthorized_dimension`).
- Check the `metrics` and `dimensions` reference tables for that report suite (including `allowed_for_reporting`) and the product profile permissions.

**"Report table ... uses ids that report suite ... does not offer"**
- The connector checks metric and dimension ids against the report suite before reading and suggests close matches, for example `unknown metric 'metrics/visit' (did you mean metrics/visits?)`.
- Metric and dimension availability differs between report suites. Query the `metrics` or `dimensions` table filtered by the report suite.

**"Metric id ... is not in the full form" or "Dimension id ... is not in the full form"**
- Use the full ids: `metrics/visits`, not `visits`; `variables/evar1`, not `evar1`.

**"granularity 'week' together with a dimension is not supported yet"**
- Use `day` or `hour` with a dimension, or remove the dimension to get weekly or monthly totals.

**Requests time out**
- The connector retries and switches to 10000 rows per page automatically. If timeouts continue, reduce the number of metrics in the table or set a smaller `limit`.

**Warning: "HTTP 200 response body is not valid JSON"**
- Adobe occasionally sends a large report page (several MB) that arrives cut off even though the request succeeded. The connector retries the page up to 3 times. If it keeps arriving cut off, the connector switches to 10000 rows per page. The warning says whether the `Content-Length` header matched the bytes received: a match means Adobe sent the page cut off, and a mismatch means bytes were lost on the network in between. It never includes the page content. If the warning appears often, set `max_rows_per_bucket` or a smaller `limit` so pages stay small.

**"Could not read the ... bucket from Adobe itemId"**
- Adobe returned a date format the connector does not recognize. The connector expects the formats seen in live responses (a century digit, then year, a month counted from 00 for January, and day, plus hour for hourly rows). Report the message to the connector maintainers; it never contains metric values.

**Warning: "skipped N row(s) dated ... because they fall outside the requested dates"**
- Adobe sometimes returns a row for a day just before the requested dates. This has been seen with `metrics/bouncerate`, probably because some metrics count a visit on the day it started. That row covers only part of its day, so the connector never writes it; otherwise it could replace a complete row from an earlier run. No action is needed. The day is read in full on the run that requests it.

**The first week or month of a `week` or `month` table is missing**
- `start_date` falls in the middle of a week or month, and partial periods are skipped. Set `start_date` to the first day of a week or month and run a full refresh.

**Numbers do not match Analysis Workspace**
- Compare the same report suite, dates, segments and metrics. Check `nones_behavior` (whether the "Unspecified" row is included) and `count_repeat_instances` against the settings of your Workspace project.
- Dates are in the report suite's time zone. Today's numbers are partial until the next run.
- Do not compare sums of Unique Visitors, rates or visit metrics across days. See [Summing Metrics](#summing-metrics-what-is-safe-and-what-is-not).
- If `max_rows_per_bucket` is set, the table is missing the smaller values.
- A segment or calculated metric may have been edited after older rows were written. Run a full refresh.

**Warning: "buckets in the lookback window were not remembered for stale-row removal"**
- `max_tracked_keys` was reached. Raise it, lower `lookback_days`, or set `delete_stale_rows` to `false`.

**A reference table has fewer rows than expected, or some columns are empty**
- For `segments`, `calculated_metrics` and `date_ranges`, see the `include_type` notes above.
- If Adobe rejects part of a request for extra details, the connector retries without them and logs a warning; some columns (such as owner names or tags) are then empty.

## Known Limitations

**Not supported in this version**

- **One dimension per report table.** Adobe's API returns one dimension per request; breakdowns by a second dimension (for example evar1 by evar2) are not offered.
- **One report suite per report table.** Create one table per report suite.
- **`week` or `month` granularity together with a dimension.**
- **Per-metric filters** (Adobe `metricFilters`), such as applying a segment to one metric column only. Segments in `segment_ids` apply to the whole table.
- **Classifications.** Classification (lookup) data is not ingested as its own table, and using classification dimensions in report tables has not been tested.
- **Unsaved calculated metrics.** Only saved calculated metrics (`cm...` ids) can be used.
- **Data Warehouse exports and Data Feeds** (raw hit-level data). These use different Adobe delivery mechanisms.
- **Real-time reports.**
- **Annotations, Workspace projects, users, and usage or audit logs.**
- **Writing to Adobe Analytics.** The connector is read-only.

**Still to be confirmed against a live account** (the live smoke check in [Testing This Connector](#testing-this-connector-for-contributors) does not settle these; they stay open until they are verified)

Confirmed against a live Adobe Analytics account: the date formats for hour, day, week and month buckets; that `nones_behavior` adds or removes one "Unspecified" row; that percent metrics such as `metrics/bouncerate` arrive as fractions (0.25 means 25%); the paging format of the list endpoints and that they accept 1,000 items per page; that `includeType=all` is accepted; and that a virtual report suite id works as `rsid` for a report table.

These behaviors follow Adobe's documentation and community sources but have not yet been verified end to end:

- How `nones_behavior` maps to the "Include unspecified" setting in Analysis Workspace. In an English-language account the row is labelled `Unspecified`; the label can differ by dimension and language.
- Units of time metrics. They are expected in seconds, and live values are consistent with that, but Adobe reports their type as `int` even though values arrive with decimals.
- The largest number of metrics Adobe accepts in one report request.
- Whether every optional detail field requested for report suites and virtual report suites is accepted. If not, the connector retries without it and some columns stay empty.
- Whether the default `scopes` match every newly created OAuth Server-to-Server credential.

## Testing This Connector (for Contributors)

This connector does **not** use the repository's record mode, and nothing from a live Adobe account is ever written to disk. Record mode saves live responses (company and report suite IDs, component names and owners, traffic numbers) into cassettes, coverage reports and simulator data. The OAuth token response also contains a live access token, which the recorder does not yet scrub: the simulator README lists "OAuth token refresh is not yet scrubbed" under "Limitations (v1)". Scrubbing also can't reliably cover every free-text field Adobe returns, so recording is turned off instead.

There are two kinds of tests:

1. **Offline simulator tests (default, no credentials).** Run these after every change:

   ```bash
   pytest tests/unit/sources/adobe_analytics/ -v
   ```

   They use the in-process simulator and placeholder data only. Setting `CONNECTOR_TEST_MODE` to `live` or `record` stops the run with an explanation before anything is written. That also means the record step of `/validate-connector` does not apply to this connector; use the smoke check below instead.

2. **Read-only live smoke check (on demand).** Run this to confirm the connector works against a real account, for example before a release or when Adobe changes its API:

   ```bash
   CONNECTOR_TEST_CONFIG_PATH=/path/outside/the/repo/adobe.json \
     python tests/unit/sources/adobe_analytics/live_smoke_check.py --dimension variables/page
   ```

   The JSON file holds the connection parameters (`client_id`, `client_secret`, and optionally `global_company_id` and `scopes`). Keep it outside the repository, or pass the same JSON inline with `CONNECTOR_TEST_CONFIG_JSON`. The check signs in, reads a few rows from every reference table and from small report tables (daily, hourly, a weekly table that starts mid-week, and a monthly table that starts mid-month), and confirms that the rows match the schemas and primary keys. With `--dimension`, it also runs the stale-row delete flow once.

   It does not settle the five items under "Still to be confirmed" in Known Limitations; those remain open. It then confirms the live behaviours listed in this table:

   | Section | What it reports |
   |---|---|
   | Item 1: date and time formats | The `itemId` layout for hour, day, week and month rows (for example `CYYMMDD`: a century digit, then year, a month counted from 00 for January, and day), whether every decoded date matches Adobe's display label for that row (any mismatch is a **Fail**), the shape of the display label (digits shown as `9`, letters as `A`/`a`), the number of hourly rows in a full day, which weekday full weeks start on, and how Adobe labels a week or month that began before the requested range. **Fail** here means the connector would keep a partial week or month as if it were complete. |
   | Item 2: `nones_behavior` | Row counts with `return` and `exclude`, and the label of the "Unspecified" row. Choose a `--dimension` that has unset values; otherwise this shows **Warning** (inconclusive). |
   | Item 3: percent and time metrics | The type and precision Adobe reports for one percent metric and one time metric, and whether values arrive as fractions (0.25 = 25%) or percentage points (25 = 25%). Values are shown only as an order-of-magnitude range. Pick the metrics with `--percent-metric` and `--time-metric` if the defaults aren't available. |
   | Item 4: list endpoints | The response shape and paging fields of each list endpoint, how many items Adobe returned when asked for 1,000 per page, whether `includeType=all` was accepted, and which `includeType` the connector actually used. |
   | Item 5: virtual report suite | Whether a daily report table reads correctly when its `rsid` is a virtual report suite id. The check uses the first virtual report suite the credential can see and never prints its id. **Warning** (inconclusive) means the credential can't see any virtual report suites. |

   A full run sends about 45 requests and takes several minutes. Run with `--help` to see every option; `--skip-open-items` runs only the table checks and skips the five live-behaviour checks in the table above (not the "Still to be confirmed" items, which the smoke check does not cover).

   **Reading the result:** each check prints `Pass ✅`, `Fail ❌` or `Warning ⚠️` (inconclusive, usually because there was no data to test with). The last line counts each. The output never shows row contents, metric values, report suite IDs, component IDs or names. The one exception is the "Unspecified" label in Item 2, which is printed only when exactly one value differs between `return` and `exclude` and the label is 30 characters or fewer. Adobe's own standard metrics (such as `metrics/bouncerate`) are named; custom metrics are not. Connector warnings are counted but hidden unless you pass `--show-warnings`, because some name a report suite or dimension value. A failed check prints the first line of its error, which can name a report suite or component ID, so don't paste the output anywhere public or save it in the repository.

   The smoke check keeps everything in memory. It does not update the simulator data, so the simulator is maintained by hand from Adobe's documentation, with placeholder values only.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/adobe_analytics/adobe_analytics.py`
- Connector API notes: `src/databricks/labs/community_connector/sources/adobe_analytics/adobe_analytics_api_doc.md`
- [Adobe Analytics 2.0 API overview](https://developer.adobe.com/analytics-apis/docs/2.0/)
- [Reports API guide](https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/reports/)
- [Discovery API (global company ID)](https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/discovery)
- [Segments API](https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/segments/)
- [Calculated metrics API](https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/calculatedmetrics/)
- [Report suites API](https://developer.adobe.com/analytics-apis/docs/2.0/guides/endpoints/report-suites)
- [Adobe Developer Console](https://developer.adobe.com/console)
- [Adobe Admin Console](https://adminconsole.adobe.com)
