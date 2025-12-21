# Lakeflow OSIPI Community Connector

This documentation provides setup instructions and reference information for the **OSI PI (PI Web API)** source connector.

## Prerequisites

- Access to a PI Web API deployment.
- An authentication setup that can mint **Bearer** access tokens accepted by PI Web API (preferred).
  See: [Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html).
- A Databricks workspace with permission to create a Unity Catalog connection and run pipelines.

## Setup

### Required Connection Parameters

Provide these parameters in your connector options.

| Name | Type | Required | Description | Example |
|---|---|---:|---|---|
| `pi_base_url` | string | yes | Base URL of the PI Web API host (no trailing slash). Requests are made under `${pi_base_url}/piwebapi/...`. | `https://my-pi-web-api.company.com` |
| `access_token` | string | yes | OAuth/OIDC access token used as `Authorization: Bearer <token>`. | `eyJ...` |
| `externalOptionsAllowList` | string | yes | Comma-separated allowlist of table-specific read options. | `dataserver_webid,nameFilter,maxCount,startIndex,maxTotalCount,tag_webids,default_tags,lookback_minutes,lookback_days,prefer_streamset,time,startTime,endTime,summaryType,calculationBasis,timeType,summaryDuration,sampleType,sampleInterval,interval,calculation_type,calculationType,intervals,element_webids,default_elements,event_frame_webids,default_event_frames,searchMode,selectedFields,tags_per_request,window_seconds,point_webids,default_points,elementtemplate_webid,eventframe_template_webid,assetdatabase_webid,default_tables,table_webids,table_webid,verify_ssl` |

Supported table-specific options (**this is the full definitive list**):

- `dataserver_webid,nameFilter,maxCount,startIndex,maxTotalCount,tag_webids,default_tags,lookback_minutes,lookback_days,prefer_streamset,time,startTime,endTime,summaryType,calculationBasis,timeType,summaryDuration,sampleType,sampleInterval,interval,calculation_type,calculationType,intervals,element_webids,default_elements,event_frame_webids,default_event_frames,searchMode,selectedFields,tags_per_request,window_seconds,point_webids,default_points,elementtemplate_webid,eventframe_template_webid,assetdatabase_webid,default_tables,table_webids,table_webid,verify_ssl`
- `nameFilter`
- `maxCount`
- `tag_webids`
- `default_tags`
- `lookback_minutes`
- `lookback_days`

### How to obtain the required parameters

#### `pi_base_url`

This is the hostname (and scheme) where PI Web API is reachable.

#### `access_token`

PI Web API supports Bearer authentication where the client includes `Authorization: Bearer <access_token>`.
The PI docs describe how to:
- discover the OpenID configuration (`/piwebapi/.well-known/openid-configuration`)
- request an access token from the identity provider using client credentials

See: [Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html).

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to:

`dataserver_webid,nameFilter,maxCount,startIndex,maxTotalCount,tag_webids,default_tags,lookback_minutes,lookback_days,prefer_streamset,time,startTime,endTime,summaryType,calculationBasis,timeType,summaryDuration,sampleType,sampleInterval,interval,calculation_type,calculationType,intervals,element_webids,default_elements,event_frame_webids,default_event_frames,searchMode,selectedFields,tags_per_request,window_seconds,point_webids,default_points,elementtemplate_webid,eventframe_template_webid,assetdatabase_webid,default_tables,table_webids,table_webid,verify_ssl`

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

### Table taxonomy (classification)

| Category | Tables | Analytics usage |
|---|---|---|
| **Discovery & inventory** | `pi_dataservers`, `pi_points`, `pi_point_attributes`, `pi_point_type_catalog` | Build a governed tag catalog, standardize naming, derive coverage metrics, and drive downstream table selection. |
| **Time-series (recorded / sampled / derived)** | `pi_timeseries`, `pi_streamset_recorded`, `pi_interpolated`, `pi_streamset_interpolated`, `pi_plot`, `pi_streamset_plot`, `pi_summary`, `pi_streamset_summary`, `pi_value_at_time`, `pi_recorded_at_time`, `pi_end`, `pi_streamset_end`, `pi_calculated` | Power time-series analytics (SPC, forecasting, anomaly detection), feature extraction, and KPI computation with scalable multi-tag ingestion using StreamSet. |
| **Asset Framework (AF)** | `pi_assetservers`, `pi_assetdatabases`, `pi_af_hierarchy`, `pi_element_attributes`, `pi_categories`, `pi_element_templates`, `pi_attribute_templates`, `pi_element_template_attributes`, `pi_analyses`, `pi_analysis_templates` | Model- and template-driven analytics: join time-series to governed assets, enforce schema via templates, and support hierarchy-based rollups (site/unit/equipment). |
| **Event Frames** | `pi_event_frames`, `pi_eventframe_templates`, `pi_eventframe_attributes`, `pi_eventframe_template_attributes`, `pi_eventframe_referenced_elements`, `pi_eventframe_acknowledgements`, `pi_eventframe_annotations` | Operational analytics (downtime, batch analysis, alarm review): join events to assets, apply template semantics, and enrich with annotations/ack metadata. |
| **AF Tables (reference data)** | `pi_af_tables`, `pi_af_table_rows` | Enrich metrics with slowly changing business/operational reference data (shift calendars, product masters) maintained in AF. |
| **Reference / lookup** | `pi_units_of_measure` | Normalize and validate units for consistent analytics and cross-tag comparisons. |
| **Governance & diagnostics** | `pi_links`, `pi_errors` | Improve navigability and observability: materialize link relationships for exploration; capture connector-side failures as data for monitoring. |

| Object (tableName) | Description | Primary keys | Ingestion type | Notes |
|---|---|---|---|---|
| `pi_dataservers` | List PI Data Archives (DataServers) | `webid` | snapshot | Based on DataServer List API |
| `pi_points` | List points (tags) on a DataServer | `webid` | snapshot | Supports `nameFilter`, pagination parameters exist in PI Web API docs |
| `pi_point_attributes` | Point (tag) attributes | (`point_webid`, `name`) | snapshot | Reads point attributes for a set of point WebIds |
| `pi_timeseries` | Recorded/compressed values for tags | (`tag_webid`, `timestamp`) | append | Uses Stream GetRecorded time windows |
| `pi_af_hierarchy` | AF elements | `element_webid` | snapshot | Uses AssetServer/AssetDatabase element listing |
| `pi_event_frames` | Event Frames in a time window | (`event_frame_webid`, `start_time`) | append | Uses AssetDatabase GetEventFrames |
| `pi_current_value` | Current (snapshot) value per tag | `tag_webid` | snapshot | Uses Stream GetValue |
| `pi_summary` | Summary stats per tag and summary type | (`tag_webid`, `summary_type`) | snapshot | Uses Stream GetSummary |
| `pi_streamset_recorded` | Recorded values via StreamSet (multi-tag) | (`tag_webid`, `timestamp`) | append | Uses StreamSet GetRecordedAdHoc |
| `pi_interpolated` | Interpolated values for tags | (`tag_webid`, `timestamp`) | append | Uses Stream/StreamSet GetInterpolated |
| `pi_plot` | Plot (downsampled) values for tags | (`tag_webid`, `timestamp`) | append | Uses Stream GetPlot |
| `pi_streamset_interpolated` | Interpolated values via StreamSet (multi-tag) | (`tag_webid`, `timestamp`) | append | Uses StreamSet GetInterpolatedAdHoc |
| `pi_streamset_summary` | Summary stats via StreamSet (multi-tag) | (`tag_webid`, `summary_type`, `timestamp`) | append | Uses StreamSet GetSummaryAdHoc |
| `pi_assetservers` | List AF servers | `webid` | snapshot | Uses AssetServer List |
| `pi_assetdatabases` | List AF databases (per server) | `webid` | snapshot | Uses AssetServer GetAssetDatabases |
| `pi_element_templates` | List AF element templates (per DB) | `webid` | snapshot | Uses AssetDatabase GetElementTemplates |
| `pi_categories` | AF categories | `webid` | snapshot | Uses AssetDatabase GetCategories |
| `pi_attribute_templates` | AF attribute templates per element template | `webid` | snapshot | Uses ElementTemplate GetAttributeTemplates |
| `pi_analyses` | AF analyses metadata | `webid` | snapshot | Uses AssetDatabase GetAnalyses |
| `pi_eventframe_templates` | Event Frame templates | `webid` | snapshot | Uses AssetDatabase GetEventFrameTemplates |
| `pi_end` | Last archived value per tag | `tag_webid` | snapshot | Uses Stream GetEnd |
| `pi_value_at_time` | Value at a specific time per tag | (`tag_webid`, `timestamp`) | snapshot | Uses Stream GetValue with `time` |
| `pi_streamset_plot` | Plot values via StreamSet (multi-tag) | (`tag_webid`, `timestamp`) | append | Uses StreamSet GetPlotAdHoc |
| `pi_units_of_measure` | Units of measure reference | `webid` | snapshot | Uses UOM list |
| `pi_analysis_templates` | AF analysis templates | `webid` | snapshot | Uses AssetDatabase GetAnalysisTemplates |
| `pi_eventframe_template_attributes` | Event Frame template attribute templates | `webid` | snapshot | Uses EventFrameTemplate GetAttributeTemplates |
| `pi_streamset_end` | Last archived values via StreamSet (multi-tag) | `tag_webid` | snapshot | Uses StreamSet GetEndAdHoc |
| `pi_element_template_attributes` | AF element template attribute templates (richer) | `webid` | snapshot | Uses ElementTemplate GetAttributeTemplates |
| `pi_eventframe_referenced_elements` | EventFrame ↔ referenced elements | (`event_frame_webid`, `element_webid`) | append | Uses EventFrame GetReferencedElements (or derived) |
| `pi_af_tables` | AF Tables inventory | `webid` | snapshot | Uses AssetDatabase GetTables |
| `pi_af_table_rows` | AF table contents (rows) | (`table_webid`, `row_index`) | snapshot | Uses Table GetRows |
| `pi_eventframe_acknowledgements` | Event Frame acknowledgements | (`event_frame_webid`, `ack_id`) | snapshot | Uses EventFrame GetAcknowledgements |
| `pi_eventframe_annotations` | Event Frame annotations | (`event_frame_webid`, `annotation_id`) | snapshot | Uses EventFrame GetAnnotations |
| `pi_recorded_at_time` | Recorded value at a specific time per tag | (`tag_webid`, `query_time`) | snapshot | Uses Stream GetRecordedAtTime |
| `pi_calculated` | Calculated values over time per tag | (`tag_webid`, `timestamp`, `calculation_type`) | append | Uses Stream GetCalculated |
| `pi_point_type_catalog` | Derived point-type catalog (grouped) | (`point_type`, `engineering_units`) | snapshot | Derived from pi_points |
| `pi_links` | Links materialization (rel → href) | (`entity_type`, `webid`, `rel`) | snapshot | Derived from Links fields / generated links |
| `pi_errors` | Connector diagnostics/errors captured during reads | (`table_name`, `endpoint`) | snapshot | In-memory best-effort diagnostics |
| `pi_element_attributes` | AF element attributes | (`element_webid`, `attribute_webid`) | snapshot | Uses Element GetAttributes |
| `pi_eventframe_attributes` | Event Frame attributes | (`event_frame_webid`, `attribute_webid`) | snapshot | Uses EventFrame GetAttributes |

## Data Type Mapping

| Source (PI Web API) | Example fields | Databricks type |
|---|---|---|
| Identifiers | `WebId` | string |
| Names/descriptions | `Name`, `Description`, `Descriptor` | string |
| Timestamps | `Timestamp`, `StartTime`, `EndTime` | timestamp |
| Values | `Value` | double |
| Quality | `Good` | boolean |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Sample output script (validation utility)

`sources/osipi/test/print_osipi_samples.py` runs the connector directly (no Spark required) and prints a small sample for each table. Use it to validate connectivity, permissions, and expected payload shapes.

### Step 2: Configure Your Pipeline

Example pipeline spec showing how to select objects and pass table-specific options:

```json
{
  "pipeline_spec": {
    "connection_name": "<YOUR_CONNECTION_NAME>",
    "object": [
      {"table": {"source_table": "pi_dataservers"}},
      {"table": {"source_table": "pi_points", "nameFilter": "Sydney_*", "maxCount": "10000"}},
      {"table": {"source_table": "pi_timeseries", "tag_webids": "<WEBID1>,<WEBID2>", "lookback_minutes": "60", "maxCount": "1000"}}
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- Start small: begin with `pi_dataservers` and a narrow `nameFilter` for `pi_points`.
- For `pi_timeseries`, keep `lookback_minutes` modest for examples.

#### Troubleshooting

**Common issues:**
- Authentication failures: ensure the token is valid and PI Web API is configured for Bearer authentication. See [Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html).
- Not enough data: increase `lookback_minutes` or confirm tags exist.

## References

- PI Web API Reference: https://docs.aveva.com/bundle/pi-web-api-reference/
- Connector API doc: `sources/osipi/osipi_api_doc.md`
