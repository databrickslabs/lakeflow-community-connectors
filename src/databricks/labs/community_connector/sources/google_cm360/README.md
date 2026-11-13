# Lakeflow Google Campaign Manager 360 Community Connector

This connector ingests Campaign Manager 360 (CM360) performance and reach reports into Databricks. It exposes four fixed report tables through the CM360 Reporting API v5. It does not ingest individual ad events or provide complete coverage of the CM360 API.

> Development status: this contribution has offline simulator coverage. Authenticated source validation and an end-to-end Databricks pipeline run are still required before production use. See [contribution instructions](CONTRIBUTING.md).

## Prerequisites

- A Databricks workspace with Unity Catalog and support for Lakeflow community connectors.
- Permission to create or use a Unity Catalog connection and write to the destination catalog and schema. Custom installation also requires access to the volume containing the connector packages.
- A CM360 user with access to the reporting profile and advertisers to ingest, including permission to create, run, download and delete reports.
- A Google Cloud project with the Campaign Manager 360 API enabled.
- A Google OAuth Web application client, with the reporting scope `https://www.googleapis.com/auth/dfareporting`.
- Network access to `dfareporting.googleapis.com`, `accounts.google.com` and `oauth2.googleapis.com`.
- A reporting profile that exports the English CSV field names expected by the connector. Additional date-format restrictions for reach reports are listed under Troubleshooting.

The connector creates and deletes temporary reporting definitions. It does not change campaigns, ads, placements or advertiser settings.

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `client_id` | String | Yes | Google OAuth Web application client ID. |
| `client_secret` | Secret string | Yes | Secret for that OAuth application. Enter it in the connection form. |
| `profile_id` | String | Yes | Numeric CM360 user-profile ID accessible to the Google user who authorizes the connection. |
| `externalOptionsAllowList` | String | Yes | Complete source-specific allowlist shown below. The connector specification supplies it when the setup tool uses that specification. |

The complete source-specific value for `externalOptionsAllowList` is:

```text
sync_start_date,sync_end_date,initial_lookback_months,lookback_window_days,reporting_timezone,advertiser_ids,delivery_country_codes,max_records_per_batch,refresh_ended_within_days,emit_request_telemetry
```

Creating the connection opens a browser for Google sign-in and consent. Databricks stores and refreshes the credentials. Users supply the OAuth application identity and profile ID; they do not paste access or refresh tokens.

### Register the Google OAuth Application

1. In [Google Cloud Console](https://console.cloud.google.com/apis/library/dfareporting.googleapis.com), enable the Campaign Manager 360 API for the project.
2. Configure the Google Auth Platform consent screen and add `https://www.googleapis.com/auth/dfareporting` under Data Access. Add the authorizing user as a test user when using an External application in Testing.
3. Create an OAuth client with application type **Web application**.
4. For connection creation through Databricks, register the workspace callback `https://<workspace-host>/login/oauth/http.html` as an authorized redirect URI. Confirm that it matches the exact `redirect_uri` sent by the workspace. For CLI setup, register the exact loopback callback used by that CLI version as well; it is a different callback.
5. Enter the client ID and secret in the Databricks connection form.

Use an Internal application where appropriate, or a properly configured External application in Production for scheduled ingestion. Google's External Testing configuration can expire refresh tokens after seven days. See [Google's expiration rules](https://developers.google.com/identity/protocols/oauth2#expiration).

A Google Ads developer token is not required. Service-account authentication is not implemented by this connector.

### Find the Profile ID

Use [`userProfiles.list`](https://developers.google.com/doubleclick-advertisers/rest/v5/userProfiles/list) with the same Google identity that will authorize the connection. Select the appropriate profile and copy `profileId`. The profile ID is different from the CM360 account ID, advertiser ID and Google Ads customer ID.

### Create a Unity Catalog Connection

1. Open the Databricks Add Data experience and select the CM360 community connector when available. Until upstream acceptance and availability, install it as a custom community connector from the vetted release packages and `connector_spec.yaml`.
2. Create a connection, enter the three required parameters and complete Google sign-in and consent.
3. Ensure the connection permits the source-specific table options listed above.
4. Create a separate connection for each CM360 profile to ingest.

For custom installation, build and provide both the framework wheel and CM360 source wheel. A GitHub repository URL by itself is not a built connector package. The [community connector CLI documentation](https://github.com/databrickslabs/lakeflow-community-connectors/tree/master/tools/community_connector) describes building, publishing and creating pipelines.

The official [custom connector guide](https://docs.databricks.com/aws/en/ingestion/custom-connectors#deploy-in-the-databricks-ui) describes registering a Custom tile with the source name `google_cm360`, wheel paths in a Unity Catalog volume and the connection specification. Publishing that tile makes the connector available in your workspace; it does not contribute the connector to the shared upstream repository.

## Supported Objects

| Source table | Report and grain | Ingestion | Primary key |
|---|---|---|---|
| `ad_report_daily` | STANDARD performance report; one row per day, campaign and delivery country. | CDC upserts | `profile_id`, `report_date`, `delivery_country`, `advertiser_id`, `campaign_id` |
| `reach_report_daily` | REACH report; daily campaign reach by delivery country. | CDC upserts | `profile_id`, `report_date`, `delivery_country`, `advertiser_id`, `campaign_id` |
| `ad_report_placement_daily` | STANDARD performance report; one row per day, campaign and placement, across all delivery countries. | CDC upserts | `profile_id`, `report_date`, `advertiser_id`, `campaign_id`, `placement_id` |
| `reach_report_period` | Native cumulative campaign reach for a campaign's completed flight or the supplied cutoff, by delivery country. | Complete snapshot | `profile_id`, `delivery_country`, `advertiser_id`, `campaign_id` |
| `reach_report_period_incremental` | The same native cumulative campaign reach, checkpointed so one cutoff can load across several pipeline updates. | CDC upserts | `profile_id`, `delivery_country`, `advertiser_id`, `campaign_id`, `source_cutoff_date` |

Despite its name, `ad_report_daily` aggregates across ads at campaign grain. Separate ad, creative, device, hourly, conversion and placement-reach tables are not exposed.

### Performance Fields

Both performance tables contain profile, account, optional subaccount, advertiser and campaign IDs and names, a `report_date`, and these metrics:

| Output | CM360 field | Databricks type |
|---|---|---|
| `impressions` | `impressions` | BIGINT |
| `clicks` | `clicks` | BIGINT |
| `media_cost` | `mediaCost` | DECIMAL(38,9) |
| `video_starts` | `richMediaVideoPlays` | BIGINT |
| `qualified_video_views` | `richMediaVideoViews` | BIGINT |
| `video_views_50_percent` | `richMediaVideoMidpoints` | BIGINT |
| `video_views_100_percent` | `richMediaVideoCompletions` | BIGINT |

`ad_report_daily` also contains `delivery_country`. `ad_report_placement_daily` instead adds `placement_id`, `placement_name` and `dv360_cost` (`dv360Cost`, DECIMAL(38,9)). Placement data has no delivery-country column and remains global when read alongside a country-filtered table. Campaign and placement totals describe overlapping data and must not be added together.

Cost values are preserved without conversion. The connector does not emit a currency code for `media_cost`; determine units and currency from the source account before combining costs. The name `qualified_video_views` is an output alias for CM360 Video Views, not a guarantee of a particular qualified-view business definition. Define video rates downstream using the verified source metric and an explicit denominator.

### Reach Fields

All reach tables return `unique_reach_impression_reach`, sourced from `uniqueReachImpressionReach`, as STRING. Numeric strings remain strings and the literal `-` remains `-`. In daily reach, an empty value becomes NULL. Period reach preserves an empty string separately from NULL.

Reach is non-additive. Do not sum it across dates, campaigns, countries or platforms to calculate deduplicated reach.

Both period tables also include:

| Output | Meaning |
|---|---|
| `campaign_start_date`, `campaign_end_date` | Native campaign flight dates; nullable. |
| `period_start`, `period_end` | Effective cumulative report interval. |
| `source_cutoff_date` | Explicit final complete reporting day. |
| `metadata_anchor` | Start of the delivery window used to discover campaigns. |
| `reporting_timezone` | Configured source reporting timezone. |
| `coverage_status` | `COMPLETE`, `UNPROVEN_START`, `WINDOW_LIMIT`, `START_AFTER_CUTOFF`, or `NOT_REFRESHED` (incremental table only). |
| `snapshot_campaign_count` | `reach_report_period_incremental` only: non-null BIGINT expected size of the cutoff's frozen campaign cohort. |

Period snapshots discover campaigns with delivery between the metadata anchor and cutoff. The anchor does not replace the native campaign start. A campaign with no delivery in that discovery window is absent. Campaign spans above 92 inclusive days return `WINDOW_LIMIT` and NULL reach; unknown starts return `UNPROVEN_START` and NULL reach; campaigns starting after the cutoff return `START_AFTER_CUTOFF` and NULL reach. The connector does not construct cumulative reach by adding daily values. It materializes the entire snapshot before returning any rows, so a late campaign failure fails the snapshot.

Campaigns with identical exact period bounds share one native REACH export that keeps the campaign and country dimensions; each campaign's returned reach is copied unchanged. Different periods use separate reports, and exports run one at a time. Duplicate or unexpected campaign rows reject the whole result.

The `reach_report_period` snapshot has a fixed 45-minute cooperative execution deadline. Requests, retry waits and polling use the remaining budget. The deadline cannot interrupt CPU work or a socket that keeps streaming, so the platform run timeout remains the outer bound. Expiry raises `Cm360SnapshotTimeout` and returns no partial snapshot. HTTP 401 raises `Cm360AuthenticationError` without retrying the supplied token. Both are subclasses of `Cm360ApiError`. Period reads log steps, request status, durations and completed group counts, never credentials, request URLs or bodies, or report values.

### Resumable Period Reach

`reach_report_period_incremental` is an opt-in alternative to the snapshot for cutoffs that need several shorter pipeline updates. It uses the same native period bounds and reach values. `report_file_last_modified_at` sequences SCD type 1 upserts. Load it into its own destination; do not migrate an existing snapshot table in place.

The first batch checkpoints the cutoff's campaign membership and exact-period groups and returns no rows. Later batches export whole groups in order, at most ten groups or 30 minutes of source work per query. A failed interval returns no rows and replays in full; previously committed intervals remain. The 30-minute limit stops new intervals from starting but does not shrink one already running. If a single interval cannot finish within one query's budget, each later update retries the same interval and fails again; narrow `advertiser_ids` so each interval has less work. A later pipeline update resumes the checkpoint with credentials resolved by Unity Catalog; the connector does not refresh tokens itself.

Supply explicit `sync_start_date`, `sync_end_date`, `reporting_timezone` and one delivery country, and keep them fixed until the cutoff finishes. A completed cutoff is a no-op; a later `sync_end_date` starts a new cohort. Empty discovery, duplicate keys, incompatible checkpoints and checkpoint manifests over 1 MiB fail without resetting progress.

Rows of an unfinished cutoff can already be in the destination. Expose a cutoff to consumers only after its pipeline update completes and it holds exactly `snapshot_campaign_count` distinct campaign keys. Keep the previous complete cutoff visible while a newer one loads.

`refresh_ended_within_days` (whole days, `0`–`3650`) skips the native report for a campaign whose completed period ended more than that many days before both `sync_end_date` and the stream's previous completed cutoff, when that previous cutoff already held a final value for the same period. Skipped campaigns stay in the cohort and in `snapshot_campaign_count`, but arrive with `coverage_status = NOT_REFRESHED`, NULL reach and no CM360 report. Read their value from the campaign's latest earlier row whose status is not `NOT_REFRESHED`. New campaigns, changed periods, campaigns after a gap and the stream's first cutoff always get a real report. The decision is frozen at discovery, so a changed value applies from the next cutoff. Without the option every complete campaign is refreshed. `reach_report_period` rejects the option, because a snapshot would replace stored reach with NULL.

### Incremental Reads and Deletes

Daily tables initially read 12 calendar months through yesterday by default, then replay the latest 30 inclusive days on later runs to capture reporting revisions. Each batch reads one day, including days with no rows. `report_file_last_modified_at`, converted from the CM360 File resource's epoch-millisecond timestamp, orders upserts. The checkpoint tracks dates separately from that timestamp.

Daily tables do not emit delete records. If a previously reported aggregate row disappears from CM360, an upsert replay does not remove the old destination row. `reach_report_period` uses snapshot ingestion, which lets the framework reconcile rows absent from a subsequent complete snapshot. `reach_report_period_incremental` does not emit deletes.

## Table Configurations

### Source and Destination

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | One of the five exact table names above. |
| `destination_catalog` | No | Destination catalog; otherwise use the pipeline default. |
| `destination_schema` | No | Destination schema; otherwise use the pipeline default. |
| `destination_table` | No | Destination name; defaults to the source table name. |

### Common `table_configuration` Options

| Option | Description |
|---|---|
| `scd_type` | `SCD_TYPE_1` by default; retain this for ordinary current-value reporting. |
| `primary_keys` | Optional framework override; keep the source grain unless an alternative has been verified. |
| `sequence_by` | For daily tables, keep `report_file_last_modified_at`. |
| `cluster_by` | Optional destination clustering columns. |

### Source-specific Options

Values passed to the connector are strings. JSON lists are encoded as strings.

| Option | Default | Meaning |
|---|---|---|
| `sync_start_date` | 12 calendar months before the upper bound | Inclusive first date, `YYYY-MM-DD`. Required metadata discovery anchor for period reach. |
| `sync_end_date` | Yesterday for daily tables | Inclusive last date, `YYYY-MM-DD`. Required cutoff for period reach. Current and future reporting days are rejected. |
| `initial_lookback_months` | `12` | Initial history when no explicit start date is supplied; positive integer. |
| `lookback_window_days` | `30` | Inclusive revision window replayed by daily tables; positive integer. |
| `reporting_timezone` | `UTC` | IANA timezone used to determine complete days. Set the source account's actual timezone; period exports must match it. |
| `advertiser_ids` | `[]` | JSON list of advertiser ID strings. An empty list reads all advertisers accessible through the profile. |
| `delivery_country_codes` | Unfiltered for daily tables | Optional single-item JSON list containing `US`, `DE` or `CH`. Required for period reach. Other filters are currently rejected. Placement validates the option but does not apply it. |
| `max_records_per_batch` | `100000` | Best-effort daily row target. The whole day's rows are returned even above the target. Period snapshots also return all rows together. |
| `emit_request_telemetry` | `false` | Set the string `true` to include sanitized request counters in daily-table checkpoints. Only lowercase strings `true` and `false` are accepted. Period tables do not emit these counters. |
| `refresh_ended_within_days` | Unset | `reach_report_period_incremental` only. Whole days `0`–`3650`; see [Resumable Period Reach](#resumable-period-reach). Rejected by `reach_report_period`. |

Omitting `delivery_country_codes` allows the daily campaign tables to return other delivery countries. The three-country restriction applies to explicit filters, not to all source data. A different country filter or multiple countries requires an implementation change.

### Request Telemetry

With `emit_request_telemetry` set to `true`, daily offsets include a `request_telemetry` object containing HTTP attempt/retry counts, response/retry counts by status, file-poll calls, transport errors, and daily reports completed with successful cleanup. These are counters only, not report rows, identifiers, credentials or response bodies. The connector does not send them to a separate telemetry service or add columns to destination tables.

Counters accumulate while resuming an in-progress checkpoint and reset when a new read starts from a completed checkpoint. Once a daily checkpoint contains `request_telemetry`, keep the option enabled when resuming it; changing it to `false` raises an error. Enabling the option on a checkpoint that has no counters is supported. Both period tables ignore this option and return no telemetry.

## Data Type Mapping

| Source value | Databricks value |
|---|---|
| IDs and names | STRING; IDs retain their exact digits. |
| Daily request date | DATE; daily reach additionally validates its returned Date field. |
| Count | BIGINT, nullable for empty values. |
| Cost decimal | DECIMAL(38,9), without currency conversion. |
| Reach | STRING, preserving source suppression markers. |
| Campaign and reporting bounds | DATE. |
| File revision milliseconds | TIMESTAMP, converted to UTC. |

## How to Run

### Step 1: Obtain the Connector

Use the upstream CM360 tile when the contribution has been accepted and made available. For a custom release, install the vetted framework/source packages and connector specification using the community connector tooling or the custom-connector setup in the workspace.

### Step 2: Configure Your Pipeline

Start with one daily source table, one advertiser and a short completed date range. This is a repository pipeline-spec example; managed Pipelines API definitions use a different shape.

```json
{
  "connection_name": "cm360_connection",
  "objects": [
    {
      "table": {
        "source_table": "ad_report_daily",
        "destination_catalog": "marketing",
        "destination_schema": "cm360",
        "destination_table": "campaign_performance",
        "table_configuration": {
          "scd_type": "SCD_TYPE_1",
          "sync_start_date": "2026-07-01",
          "sync_end_date": "2026-07-07",
          "reporting_timezone": "UTC",
          "advertiser_ids": "[\"123456\"]",
          "max_records_per_batch": "100000"
        }
      }
    }
  ]
}
```

Replace the example connection, destinations, dates, timezone and advertiser ID with your values. Remove the fixed `sync_end_date` for daily ingestion that should advance with new completed days. Period reach always needs an explicit cutoff; update it for each new snapshot.

For managed ingestion, use the installed tooling to construct the ingestion definition, or configure the source options in the UI. Use each source table once per pipeline and keep options consistent within a managed pipeline. Separate pipelines are appropriate for different country filters or incompatible options.

### Step 3: Run and Schedule the Pipeline

Run the bounded sample and compare its rows and totals with a report from the same CM360 profile, filters and dates. Then expand the history and configure a schedule, commonly daily. Each daily source creates a report per date, plus compatible-field checks, polling, download and cleanup requests. Period reach adds one metadata report and one report per distinct eligible campaign period. Set the schedule and history window with source quotas in mind.

### Troubleshooting

- **OAuth redirect error:** Register the exact callback URI sent to Google for the selected setup path.
- **Expired or revoked authorization:** Reauthorize the connection. Check the OAuth application's publishing status and the authorizing user's continued profile access.
- **403 or rejected fields:** Confirm reporting permissions and access to the profile, then verify that CM360 supports the report's fixed field combination. Metrics are not substituted automatically.
- **CSV locale:** All tables expect English headers. Daily reach currently requires an ISO `Date`; period reach requires English metadata labels and `DD/MM/YYYY - DD/MM/YYYY` range text, with ISO campaign dates. Other export formats fail. Validate the actual profile export before enabling reach tables.
- **Country filter:** Only single `US`, `DE` or `CH` filters are implemented. An accepted country option does not constrain placement rows.
- **Period coverage:** Check `coverage_status`, the metadata discovery window and the source cutoff. NULL reach is different from zero and from the source's `-` marker.
- **Retries and quotas:** HTTP 429 and selected 5xx responses use up to five attempts with capped delays. File polling is bounded to 60 calls. Transport exceptions fail the current read. A later run retries from the last committed checkpoint.
- **Period timeouts:** For `reach_report_period`, `Cm360SnapshotTimeout` means the snapshot used its 45-minute budget; narrow the advertiser scope or use `reach_report_period_incremental`. For `reach_report_period_incremental`, `Cm360SnapshotTimeout` or `TimeoutError` means an interval exceeded the query's 30-minute budget; it is retried on the next update, and repeated failures need a narrower advertiser scope. Temporary reports are still deleted after a timeout.
- **Large exports:** CSV responses and parsed rows are held in memory. The batch target does not split a large day or a period snapshot into smaller allocations.

## References

- [CM360 REST API v5](https://developers.google.com/doubleclick-advertisers/rest/v5)
- [Authorize requests](https://developers.google.com/doubleclick-advertisers/authorizing)
- [Create reports](https://developers.google.com/doubleclick-advertisers/guides/create_reports)
- [Run reports](https://developers.google.com/doubleclick-advertisers/guides/run_reports)
- [Download reports](https://developers.google.com/doubleclick-advertisers/guides/download_reports)
- [Lakeflow Community Connectors](https://github.com/databrickslabs/lakeflow-community-connectors)
- [Use community connectors](https://docs.databricks.com/aws/en/ingestion/community-connectors)
- [Install a custom connector](https://docs.databricks.com/aws/en/ingestion/custom-connectors)

Community connectors are maintained by contributors and do not carry official Databricks SLAs, certification or guaranteed compatibility. Distribution retains the upstream LICENSE and NOTICE.
