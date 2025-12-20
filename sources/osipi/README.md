# Lakeflow osipi Community Connector

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
| `externalOptionsAllowList` | string | yes | Comma-separated allowlist of table-specific read options. | `dataserver_webid,nameFilter,maxCount,startIndex,maxTotalCount,tag_webids,default_tags,lookback_minutes,lookback_days,prefer_streamset,time,startTime,endTime,summaryType,calculationBasis,timeType,summaryDuration,sampleType,sampleInterval,element_webids,default_elements,event_frame_webids,default_event_frames,searchMode,selectedFields,tags_per_request,window_seconds,point_webids,default_points,verify_ssl` |

Supported table-specific options (**this is the full definitive list**):

- `dataserver_webid,nameFilter,maxCount,startIndex,maxTotalCount,tag_webids,default_tags,lookback_minutes,lookback_days,prefer_streamset,time,startTime,endTime,summaryType,calculationBasis,timeType,summaryDuration,sampleType,sampleInterval,element_webids,default_elements,event_frame_webids,default_event_frames,searchMode,selectedFields,tags_per_request,window_seconds,point_webids,default_points,verify_ssl`
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

`dataserver_webid,nameFilter,maxCount,startIndex,maxTotalCount,tag_webids,default_tags,lookback_minutes,lookback_days,prefer_streamset,time,startTime,endTime,summaryType,calculationBasis,timeType,summaryDuration,sampleType,sampleInterval,element_webids,default_elements,event_frame_webids,default_event_frames,searchMode,selectedFields,tags_per_request,window_seconds,point_webids,default_points,verify_ssl`

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

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

### Sample output script (demo helper)

For demos, you can print a small sample of what each connector table returns using:

- `sources/osipi/test/print_osipi_samples.py`
- Notes: `sources/osipi/test/README_PRINT_SAMPLES.md` (temporary; safe to delete later)

### Hackathon Demo (no PI Web API required)

If you (the reviewer/judge) do not have access to a real PI Web API deployment, you can still run an end-to-end demo against a **Databricks App** mock PI Web API hosted by the submitter.

- **Set `pi_base_url`** to the provided Databricks App URL (no trailing slash), for example:
  - `https://<submitter-provided-app>.databricksapps.com`
  - `https://osipi-webserver-1444828305810485.aws.databricksapps.com` (submitter demo App)
- **Auth (`access_token`)**:
  - If the App requires auth, request a temporary access token from the submitter and set it as `access_token`.
  - If the App is unauthenticated, `access_token` can be left blank.

This connector will call PI Web API paths under that base URL, e.g. `${pi_base_url}/piwebapi/dataservers`, `${pi_base_url}/piwebapi/batch`.

#### Demo token minting (submitter workspace)

For the demo App above, the submitter mints a short-lived Databricks OIDC access token (client-credentials) and passes it as `access_token` (`Authorization: Bearer <token>`).

- Helper script: `sources/osipi/osipi_databricks_app_token_setup.py`
- Service principal: `sp-osipi` (must have **Can Use** on the App)
- Secrets (in submitter workspace): `sp-osipi/sp-client-id`, `sp-osipi/sp-client-secret`

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
- For `pi_timeseries`, keep `lookback_minutes` modest for demos.

#### Troubleshooting

**Common issues:**
- Authentication failures: ensure the token is valid and PI Web API is configured for Bearer authentication. See [Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html).
- Not enough data: increase `lookback_minutes` or confirm tags exist.

## References

- PI Web API Reference: https://docs.aveva.com/bundle/pi-web-api-reference/
- Connector API doc: `sources/osipi/osipi_api_doc.md`
