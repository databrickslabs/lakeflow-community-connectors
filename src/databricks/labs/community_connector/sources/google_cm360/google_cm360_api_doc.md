# Google Campaign Manager 360 Reporting API

Technical reference for the connector's fixed reporting contract. The public
[README](README.md) covers setup, output fields and operation. This community
copy needs fresh authenticated validation; simulator results do not establish
source permissions, locale compatibility or business metric semantics.

## Authorization

- Scope: `https://www.googleapis.com/auth/dfareporting`
- Authorization endpoint: `https://accounts.google.com/o/oauth2/v2/auth`
- Token endpoint: `https://oauth2.googleapis.com/token`
- API base: `https://dfareporting.googleapis.com/dfareporting/v5`
- Header: `Authorization: Bearer {access_token}`

Unity Catalog manages interactive OAuth and token refresh. One connection
identifies one CM360 user profile. The connector consumes the injected token;
it does not implement service-account authentication or require a Google Ads
developer token. Report definitions are temporary source resources. Campaigns,
ads, placements and advertiser settings are not modified.

## Object List and Schema

These are report-backed tables, not CM360 resources. Spark schemas are fixed in
`google_cm360_schemas.py`; they are not discovered from compatible-field results.

| Table | Dimensions / grain | Metrics | Ingestion |
|---|---|---|---|
| `ad_report_daily` | Profile, request day, delivery country, advertiser, campaign | Performance metrics below | CDC |
| `reach_report_daily` | Profile, returned day, delivery country, advertiser, campaign | `uniqueReachImpressionReach` | CDC |
| `ad_report_placement_daily` | Profile, request day, advertiser, campaign, placement; all countries | Performance metrics plus `dv360Cost` | CDC |
| `reach_report_period` | Profile, delivery country, advertiser, campaign | Native cumulative `uniqueReachImpressionReach` | Snapshot |

Performance metrics are `impressions`, `clicks`, `mediaCost`,
`richMediaVideoPlays`, `richMediaVideoViews`, `richMediaVideoMidpoints` and
`richMediaVideoCompletions`. The service's midpoint CSV label is
`Video Mid-points`. `qualified_video_views` is an output alias for
`richMediaVideoViews`, not a verified business-qualified-view definition.
Costs are preserved without conversion; `media_cost` has no currency column.

Every row includes profile/account identity and optional paired subaccount
ID/name from UserProfile, advertiser/campaign identity and the report-file
revision timestamp. Placement adds placement identity and no country column.
Daily reach preserves numeric strings and `-`, converting empty cells to NULL.
Period reach also preserves empty strings. Never add reach across reports.

### Output Columns

All tables contain `profile_id`, `account_id`, `account_name`, `subaccount_id`,
`subaccount_name`, `advertiser_id`, `advertiser_name`, `campaign_id`,
`campaign_name` and `report_file_last_modified_at`.

- `ad_report_daily` adds `report_date`, `delivery_country`, `impressions`,
  `clicks`, `media_cost`, `video_starts`, `qualified_video_views`,
  `video_views_50_percent` and `video_views_100_percent`.
- `ad_report_placement_daily` adds the same date and performance metrics but
  no `delivery_country`. It also adds `placement_id`, `placement_name` and
  `dv360_cost`.
- `reach_report_daily` adds `report_date`, `delivery_country` and
  `unique_reach_impression_reach`.
- `reach_report_period` adds `delivery_country`, `unique_reach_impression_reach`,
  `campaign_start_date`, `campaign_end_date`, `period_start`, `period_end`,
  `source_cutoff_date`, `metadata_anchor`, `reporting_timezone` and
  `coverage_status`. It has no `report_date` column.

Types follow the mapping below and the README's source-field mapping. Schema
nullability is explicit in `google_cm360_schemas.py`; optional subaccount pairs,
campaign/placement names, metrics and campaign/period bounds can be NULL.

## Primary Keys and Ingestion Metadata

- Daily campaign/reach: `profile_id`, `report_date`, `delivery_country`,
  `advertiser_id`, `campaign_id`.
- Daily placement: `profile_id`, `report_date`, `advertiser_id`, `campaign_id`,
  `placement_id`.
- Period reach: `profile_id`, `delivery_country`, `advertiser_id`, `campaign_id`.
- Daily cursor: `report_file_last_modified_at`, from File `lastModifiedTime`
  (epoch milliseconds converted to UTC).
- Daily offset: independent ISO date state, not the revision timestamp.
- No daily delete feed. Snapshot ingestion reconciles absent period rows.

## Read API and Lifecycle

All paths below are relative to the v5 API base.

| Operation | Method and path | Contract |
|---|---|---|
| Profile | `GET /userprofiles/{profileId}` | Returned profile must match; require account identity and a consistent optional subaccount pair. |
| Country lookup | `POST /userprofiles/{profileId}/dimensionvalues/query` | Resolve one exact delivery-country value using reporting scope. |
| Field validation | `POST /userprofiles/{profileId}/reports/compatiblefields/query` | Validate the entire STANDARD or REACH definition; returned compatible additions need not echo selected fields. |
| Insert | `POST /userprofiles/{profileId}/reports` | Create a temporary CSV report with explicit completed date bounds and advertiser/country filters. |
| Run | `POST /userprofiles/{profileId}/reports/{reportId}/run` | Obtain file ID. |
| Poll | `GET /userprofiles/{profileId}/reports/{reportId}/files/{fileId}` | Require `REPORT_AVAILABLE`; fail on `FAILED` / `CANCELLED`; bounded polling. |
| Download | Same File path with `alt=media` | Download CSV through the authenticated REST client. |
| Cleanup | `DELETE /userprofiles/{profileId}/reports/{reportId}` | Attempt in `finally`; daily cleanup is best effort, period cleanup failure fails the snapshot. |

STANDARD uses `criteria`; REACH uses `reachCriteria`. The single-day report
has equal `dateRange.startDate` and `endDate`, `format: CSV`, fixed dimensions
and metric names. Advertiser filters use `dimensionName: advertiser`; country
filters use the resolved reporting dimension ID. Placement ignores a valid
shared country filter because its grain has no delivery-country dimension.

## CSV Contract

English metadata and exact configured header names are required. Columns may
be reordered, but duplicate/missing/unexpected columns are rejected. Data starts
after `Report Fields`. The native `Grand Total:` footer marks completion;
footerless prefixes must not advance a daily checkpoint. Numeric campaign IDs
distinguish report data from footer-like names. Data after a footer is rejected.
The public API-team discussion linked below demonstrates both the marker and
the native footer; validate current exports during live testing.

Performance reports omit the localized Date dimension and use the request day.
Daily reach includes Date and currently requires ISO `YYYY-MM-DD` matching the
request day. Period reach requires English metadata, a
`DD/MM/YYYY - DD/MM/YYYY` Date Range, ISO campaign flight dates, and an exact
IANA timezone match. Other locales/formats are unsupported, not guessed.

## Incremental Reads

Each daily call drains one complete day, even above `max_records_per_batch`.
The default initial history is 12 calendar months ending yesterday in the
configured timezone. A later connector instance replays 30 inclusive days once,
bounded by the original start. Empty complete reports advance the date state;
failed reports return no successful next offset. A terminal call returns no
rows and the unchanged completed offset.

Completed state: `{"high_watermark":"2026-08-23"}`. In-progress state also
contains `next_date`, `upper_bound` and `initial_start`. Recent source revisions
are upserted using the file revision timestamp. Disappeared daily rows remain
because there is no delete feed.

A nonempty one-day replay whose completed checkpoint would equal its input
returns the existing in-progress form with `next_date` one day beyond the
bound. The next call returns an empty completed checkpoint, followed by stable
empty termination. Resuming that in-progress checkpoint does not re-read the day.

### Request Telemetry

`emit_request_telemetry` defaults to the string `false`; only lowercase strings
`true` and `false` are valid. When enabled, daily checkpoints add
`request_telemetry` with `http_attempts`, `http_retries`, `file_poll_calls`,
`transport_errors`, `response_status_counts`, `retry_status_counts` and
`daily_reports_completed` (reports whose cleanup succeeded). Counters persist
across in-progress reads and reset on a new read from a completed checkpoint.
An instrumented daily checkpoint cannot be resumed with the option disabled.
Enabling it on an uninstrumented checkpoint is supported. Counters contain no
identifiers, credentials, rows or response bodies and are not sent to a separate
service. No destination columns change. Period snapshots ignore the option and
never emit request telemetry.

## Campaign Period Snapshots

Require explicit metadata anchor (`sync_start_date`), completed cutoff
(`sync_end_date`) and one supported country. Discover campaigns with delivery
inside that window using a STANDARD metadata report, then request native
cumulative reach from campaign start through completed campaign end/cutoff.
The anchor is not a substitute for campaign start. No delivery in the discovery
window means the campaign is absent.

Spans over 92 inclusive days retain metadata with NULL reach and `WINDOW_LIMIT`.
Unknown starts produce NULL reach with `UNPROVEN_START`. Otherwise coverage is
`COMPLETE`. Each row carries campaign bounds, effective period, source cutoff,
metadata anchor and timezone. Materialize the whole snapshot before returning:
a late campaign failure cannot publish an earlier prefix. Never sum daily reach.

## Types, Limits and Errors

IDs/names remain strings; counts are nullable BIGINT; cost is DECIMAL(38,9);
dates are ISO DATE; file revisions are UTC TIMESTAMP; reach is STRING.
Reject non-integral counts, invalid identifiers, duplicate keys, localized
decimal commas and malformed framing. No silent metric substitution.

HTTP timeout is 30 seconds. HTTP 429 and selected 5xx responses have at most
five attempts with capped 30-second delays; File polling is bounded to 60
calls. Transport exceptions fail the read. CSV and parsed rows are held in
memory; the batch target does not split a large day or snapshot. Quota limits
and metric availability depend on the source account.

Explicit country filters currently support one of US/DE/CH. Unfiltered daily
campaign reads can return other countries. Period snapshots require a filter;
placement remains global. New countries/locales require a deliberate contract
change and representative source validation, not a simulator-only claim.

## Public References

- [REST API v5](https://developers.google.com/doubleclick-advertisers/rest/v5)
- [Discovery document](https://www.googleapis.com/discovery/v1/apis/dfareporting/v5/rest)
- [Create reports and compatible fields](https://developers.google.com/doubleclick-advertisers/guides/create_reports)
- [Run reports](https://developers.google.com/doubleclick-advertisers/guides/run_reports)
- [Download reports](https://developers.google.com/doubleclick-advertisers/guides/download_reports)
- [API-team CSV framing and language discussion](https://groups.google.com/g/google-doubleclick-for-advertisers-api/c/tDNtgg2fzFc), retrieved 2026-10-19.
- [Reporting fields](https://developers.google.com/doubleclick-advertisers/v5/dimensions)
- [OAuth authorization](https://developers.google.com/doubleclick-advertisers/authorizing)
- [Data freshness](https://support.google.com/campaignmanager/answer/6110224)
- [Lakeflow contribution workflow](https://github.com/databrickslabs/lakeflow-community-connectors/blob/master/CONTRIBUTING.md)

Fresh record-mode validation must confirm permissions, filtering, selected
fields, CSV framing, locale, cutoff/timezone behavior, costs, reach markers and
cleanup. Keep raw credentials/cassettes private and sanitize any evidence.
