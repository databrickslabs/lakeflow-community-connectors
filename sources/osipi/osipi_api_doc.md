# **OSI PI Web API API Documentation**

## **Authorization**

### Preferred method: Bearer authentication (OAuth / OIDC access token)

PI Web API supports supplying an access token in the HTTP `Authorization` header as `Bearer <token>` ([Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html)).

- **Header**: `Authorization: Bearer <access_token>` ([Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html))
- **Discover OIDC configuration**: `GET https://{piwebapifqdn}/piwebapi/.well-known/openid-configuration` ([Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html))
- **Client credentials grant**: `POST` to your IdP `token_endpoint` with `grant_type=client_credentials` and appropriate `scope` ([Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html))

Example (from the PI Web API docs) showing how to pass the Bearer token:

```bash
curl --location --request GET 'https://{piwebapifqdn}/piwebapi'   --header 'Authorization: Bearer xxxxxxxxxxx'
```

([Bearer Authentication](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html))

---

## **Object List**

This connector exposes PI Web API resources as a small set of **tables** (objects). Object names are **static** and returned by the connector’s `list_tables()`.

| Connector object (tableName) | Description | PI Web API controller/actions used |
|---|---|---|
| `pi_dataservers` | List PI Data Archives (DataServers) | DataServer **List** (`GET dataservers`) ([DataServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html)) |
| `pi_points` | List points (tags) on a DataServer | DataServer **GetPoints** (`GET dataservers/{webId}/points`) ([DataServer GetPoints](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html)) |
| `pi_point_attributes` | Point (tag) attributes | Point **GetAttributes** (`GET points/{webId}/attributes`) (PI Web API Point controller) |
| `pi_timeseries` | Recorded/compressed values for one or more tags | Stream **GetRecorded** (`GET streams/{webId}/recorded`) ([Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html)); optionally Batch **Execute** (`POST batch`) ([Batch Execute](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/batch/actions/execute.html)) |
| `pi_af_hierarchy` | AF elements (hierarchy) | AssetServer **List** (`GET assetservers`) ([AssetServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetserver/actions/list.html)); AssetServer **GetDatabases** (`GET assetservers/{webId}/assetdatabases`) ([AssetServer GetDatabases](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetserver/actions/getdatabases.html)); AssetDatabase **GetElements** (`GET assetdatabases/{webId}/elements`) ([AssetDatabase GetElements](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/getelements.html)) |
| `pi_event_frames` | Event Frames in a time window | AssetDatabase **GetEventFrames** (`GET assetdatabases/{webId}/eventframes`) ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html)) |

Notes:

| `pi_current_value` | Current (snapshot) value per tag | Stream **GetValue** (`GET streams/{webId}/value`) ([Stream GetValue](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getvalue.html)) |
| `pi_summary` | Summary stats per tag | Stream **GetSummary** (`GET streams/{webId}/summary`) ([Stream GetSummary](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getsummary.html)) |
| `pi_streamset_recorded` | Recorded values via StreamSet (multi-tag) | StreamSet **GetRecordedAdHoc** (`GET streamsets/recorded`) ([StreamSet GetRecordedAdHoc](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/streamset/actions/getrecordedadhoc.html)) |
| `pi_element_attributes` | AF element attributes | Element **GetAttributes** (`GET elements/{webId}/attributes`) ([Element GetAttributes](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/element/actions/getattributes.html)) |
| `pi_eventframe_attributes` | Event Frame attributes | EventFrame **GetAttributes** (`GET eventframes/{webId}/attributes`) ([EventFrame GetAttributes](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/eventframe/actions/getattributes.html)) |

- The PI Web API docs show request paths relative to the PI Web API root. In practice, requests are made under the server’s PI Web API base path, typically `https://{piwebapifqdn}/piwebapi/...` (see sample `Links.Self` values in action pages like [DataServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html)).

---

## **Object Schema**

This section documents the **output schemas** produced by the connector tables.

### `pi_dataservers`

Source call: DataServer **List** (`GET dataservers`) ([DataServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html))

Output schema:
- `webid` (string, non-null)
- `name` (string, nullable)

PI Web API sample response includes fields like `WebId`, `Id`, `Name`, `Path`, and link metadata ([DataServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html)).

### `pi_points`

Source call: DataServer **GetPoints** (`GET dataservers/{webId}/points`) ([DataServer GetPoints](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html))

Output schema:
- `webid` (string, non-null)
- `name` (string, nullable)
- `descriptor` (string, nullable)
- `engineering_units` (string, nullable)
- `path` (string, nullable)
- `dataserver_webid` (string, nullable)

The PI Web API sample response for GetPoints contains fields such as `WebId`, `Id`, `Name`, `Path`, `Descriptor`, `PointType`, `EngineeringUnits`, etc. ([DataServer GetPoints](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html)).

### `pi_timeseries`

Primary source call: Stream **GetRecorded** (`GET streams/{webId}/recorded`) ([Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html))

Output schema:
- `tag_webid` (string, non-null)
- `timestamp` (timestamp, non-null)
- `value` (double, nullable)
- `good` (boolean, nullable)
- `units` (string, nullable)
- `ingestion_timestamp` (timestamp, non-null)

PI Web API GetRecorded sample response includes `Items[]` with `Timestamp`, `UnitsAbbreviation`, `Good`, `Questionable`, `Substituted`, `Annotated`, and `Value` ([Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html)).

### `pi_af_hierarchy`

Source calls:
- AssetServer **List** (`GET assetservers`) ([AssetServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetserver/actions/list.html))
- AssetServer **GetDatabases** (`GET assetservers/{webId}/assetdatabases`) ([AssetServer GetDatabases](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetserver/actions/getdatabases.html))
- AssetDatabase **GetElements** (`GET assetdatabases/{webId}/elements`) ([AssetDatabase GetElements](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/getelements.html))

Output schema:
- `element_webid` (string, non-null)
- `name` (string, nullable)
- `template_name` (string, nullable)
- `description` (string, nullable)
- `path` (string, nullable)
- `parent_webid` (string, nullable)
- `depth` (int, nullable)
- `category_names` (array<string>, nullable)
- `ingestion_timestamp` (timestamp, non-null)

The PI Web API GetElements sample response includes `WebId`, `Id`, `Name`, `Description`, `Path`, `TemplateName`, `HasChildren`, `CategoryNames`, etc. ([AssetDatabase GetElements](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/getelements.html)).

### `pi_event_frames`

Source call: AssetDatabase **GetEventFrames** (`GET assetdatabases/{webId}/eventframes`) ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html))

Output schema:
- `event_frame_webid` (string, non-null)
- `name` (string, nullable)
- `template_name` (string, nullable)
- `start_time` (timestamp, nullable)
- `end_time` (timestamp, nullable)
- `primary_referenced_element_webid` (string, nullable)
- `description` (string, nullable)
- `category_names` (array<string>, nullable)
- `attributes` (map<string,string>, nullable)
- `ingestion_timestamp` (timestamp, non-null)

The PI Web API GetEventFrames sample response includes fields such as `WebId`, `Name`, `Description`, `Path`, `TemplateName`, time fields, severity/ack fields, `RefElementWebIds`, and more ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html)).

---

## **Get Object Primary Keys**

Primary keys are **static** for this connector:

| Object | Primary key columns |
|---|---|
| `pi_dataservers` | `webid` |
| `pi_points` | `webid` |
| `pi_timeseries` | (`tag_webid`, `timestamp`) |
| `pi_af_hierarchy` | `element_webid` |
| `pi_event_frames` | (`event_frame_webid`, `start_time`) |

---

## **Object's ingestion type**

| Object | ingestion_type | Cursor field |
|---|---|---|
| `pi_dataservers` | `snapshot` | none |
| `pi_points` | `snapshot` | none |
| `pi_timeseries` | `append` | `timestamp` |
| `pi_af_hierarchy` | `snapshot` | none |
| `pi_event_frames` | `append` | `start_time` |

Rationale:
- Recorded stream values are naturally append-like over time and are retrieved for a time window via `startTime`/`endTime` in Stream GetRecorded ([Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html)).
- Event Frames are retrieved over time windows via `startTime`/`endTime` in AssetDatabase GetEventFrames ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html)).

---

## **Read API for Data Retrieval**

### Base URL

PI Web API endpoints are served under:

- `https://{piwebapifqdn}/piwebapi/...`

This is visible in sample `Links.Self` values (e.g., [DataServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html)).

### `pi_dataservers` read path

- **Request**: `GET dataservers` ([DataServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html))
- **Relevant parameters**:
  - `selectedFields` (optional) ([DataServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html))
  - `webIdType` (optional) ([DataServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html))

### `pi_points` read path

- **Request**: `GET dataservers/{webId}/points` ([DataServer GetPoints](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html))
- **Relevant parameters**:
  - `nameFilter` (optional) ([DataServer GetPoints](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html))
  - `startIndex` (optional, pagination offset) ([DataServer GetPoints](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html))
  - `maxCount` (optional, page size) ([DataServer GetPoints](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html))

### `pi_timeseries` read path (recorded values)

- **Request**: `GET streams/{webId}/recorded` ([Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html))
- **Required parameter**:
  - `webId` (in path) ([Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html))
- **Key parameters**:
  - `startTime` / `endTime` (optional; defaults documented) ([Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html))
  - `maxCount` (optional; default 1000) ([Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html))

Batch optimization:
- PI Web API supports batching multiple logical REST requests into one call via `POST batch` ([Batch Execute](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/batch/actions/execute.html)).

### `pi_af_hierarchy` read path (AF elements)

- **Request**: `GET assetservers` ([AssetServer List](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetserver/actions/list.html))
- **Then**: `GET assetservers/{webId}/assetdatabases` ([AssetServer GetDatabases](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetserver/actions/getdatabases.html))
- **Then**: `GET assetdatabases/{webId}/elements` with optional `searchFullHierarchy=true` to traverse beyond immediate children ([AssetDatabase GetElements](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/getelements.html)).

### `pi_event_frames` read path

- **Request**: `GET assetdatabases/{webId}/eventframes` ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html))
- **Key parameters**:
  - `startTime` (default `*-8h`) ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html))
  - `endTime` (default `*`) ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html))
  - `searchMode` (default `Overlapped`) ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html))
  - `startIndex` / `maxCount` for pagination ([AssetDatabase GetEventFrames](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html))

Rate limits:
- The PI Web API reference bundle used here does not define a single global numeric rate limit in the sections cited above; deployments may impose network/load constraints. (No explicit numeric limit found during this documentation pass.)

Deleted records:
- This connector does not currently retrieve deleted records.

---

## **Field Type Mapping**

| PI Web API concept | PI sample field(s) | Connector output type |
|---|---|---|
| WebId | `WebId` | string |
| Name | `Name` | string |
| Description/Descriptor | `Description`, `Descriptor` | string |
| Time | `Timestamp`, `StartTime`, `EndTime` | timestamp |
| Numeric value | `Value` | double |
| Boolean quality | `Good` | boolean |
| Units | `UnitsAbbreviation`, `EngineeringUnits` | string |

Example fields are shown in PI Web API sample responses for [DataServer GetPoints](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html) and [Stream GetRecorded](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html).

---

## **Write API**

This connector is **read-only**.

(PI Web API supports write operations for some controllers, e.g., Stream UpdateValue / UpdateValues are listed under the Stream controller actions, but they are not implemented by this connector.)

Stream controller actions include `UpdateValue` / `UpdateValues` ([Stream controller](https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream.html)).

---

## **10. Sources and References**

### Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---:|---|---|
| Official Docs (bundle index) | https://docs.aveva.com/bundle/pi-web-api-reference/ | 2025-12-18 | High | Canonical documentation bundle location |
| Official Docs (DataServer List) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/list.html | 2025-12-18 | High | Dataserver list request path + params + sample response |
| Official Docs (DataServer GetPoints) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/dataserver/actions/getpoints.html | 2025-12-18 | High | Points listing request path, nameFilter, pagination params |
| Official Docs (Stream GetRecorded) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/stream/actions/getrecorded.html | 2025-12-18 | High | Recorded values endpoint + time window params + maxCount |
| Official Docs (Batch Execute) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/batch/actions/execute.html | 2025-12-18 | High | Batch endpoint exists + request structure narrative |
| Official Docs (AssetServer List) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetserver/actions/list.html | 2025-12-18 | High | Asset server discovery |
| Official Docs (AssetServer GetDatabases) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetserver/actions/getdatabases.html | 2025-12-18 | High | Asset database discovery |
| Official Docs (AssetDatabase GetElements) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/getelements.html | 2025-12-18 | High | AF element listing endpoint + traversal flags |
| Official Docs (AssetDatabase GetEventFrames) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/controllers/assetdatabase/actions/geteventframes.html | 2025-12-18 | High | EventFrames endpoint + start/end/searchMode + pagination |
| Official Docs (Bearer Authentication) | https://docs.aveva.com/bundle/pi-web-api-reference-1.19.1/page/help/topics/bearer-authentication.html | 2025-12-18 | High | How to obtain/access token + Authorization header usage |

Coverage note:
- We enumerated PI Web API reference pages via the public sitemap and downloaded rendered page HTML via the docs backend API.
  The raw inventories are stored under `sources/osipi/research/` (e.g., `aveva_pi_web_api_reference_links_all.txt`, `aveva_topic_html_full_navpaths.txt`).

### Sources

- Official PI Web API reference bundle: https://docs.aveva.com/bundle/pi-web-api-reference/
- PI Web API Reference v1.19.1 controller/action pages and topics (see Research Log links above).

Confidence / precedence:
- Official docs (AVEVA) are treated as highest confidence.
