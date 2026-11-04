# **Apis REST Service API Documentation**

> Source spec: `src/databricks/labs/community_connector/sources/apis/apis_openapi.json` (OpenAPI 3.1.0, `info.title = "Apis REST Service"`, `info.version = "0.4.0"`). This is the **sole authoritative source** used for this document — see [Sources and References](#sources-and-references) for why external documentation could not be directly verified in this environment.

## **Overview**

This is a small, purpose-built REST API in front of a "Hive" — an industrial data-historian / SCADA-style runtime that hosts pluggable **modules**, each of which exposes **items** (points/tags). The API supports:
- Browsing available Hive **instances**, their **modules**, and **items** (catalog/metadata)
- Reading and writing **current (real-time) values** of items
- Reading **historical timeseries** for items, optionally aggregated

`TBD` / assumption: web research (see Sources) strongly suggests this spec matches the REST API shipped with **Prediktor's "Apis" / "Apis Foundation" Hive platform** (a process historian/SCADA product where "Hive" = real-time data hub, "modules" = plug-ins such as `ApisLogger`/`ApisOpcUa`, and historical values are stored in a companion time-series store referred to as "HoneyStore"). This external context is used only for narrative framing (e.g., why `timeseries` requires items to be "logged") — it is **not** used to assert any field name, schema, or parameter not present in the spec itself.

## **Live Validation Findings (manual spot-check, 2026-09-22 to 2026-09-23)**

A developer ran a handful of authenticated calls against a real Apis Hive instance before the full `/validate-connector` record-mode pass. This corrects several assumptions made when this document was first written from the spec alone:

| Assumption in this doc | Reality (confirmed live) |
|---|---|
| `values`/`timeseries` identifying field is `item_name` (per the spec's `single_value_t`) | Real server sends `"item"` on both `values` and `timeseries`, and `"name"` on `items`. The spec's own declared schema was wrong, not just incomplete. The connector's tolerant `_ITEM_NAME_KEYS` list already covered all three, so this needed no code fix — only correcting this document's and the schema's assumption. |
| `items` schema has no `type`/similar field | Real `items` rows include `"type"` (observed: `"Signal"`, `"Function item"`, `"Status"`). Added to the connector's `ITEMS_SCHEMA` as `item_type`. |
| `module` derivation (split `item_name` on first `.`) is "observed but not guaranteed" | Confirmed: reproduces the real `modules` endpoint's module names exactly, even for deeply nested item paths like `PumpHouse.PumpHouse.PumpTrain1.PumpLoop1.DensitySensor.Density`. |
| `value.t` "presumed ISO-8601" | **Not ISO-8601.** Observed: `"2026-09-22 20:38:02.545"` — space-separated, millisecond precision, no timezone. Does not affect connector logic (offsets are self-generated, never parsed from this field), only downstream casting guidance. |
| `value.q` open string, enum unconfirmed | Confirmed open and mixed-case: observed `"Good"` (capital), while the `quality` *filter* parameter only accepts lowercase `good`/`uncertain`/`bad`. |
| 206 truncation is a theoretical risk implied by the spec | Confirmed real: an unfiltered `item=*` call against an instance with ~100 items already returned `206 Partial Content`. |
| `Authorization` header form (`Bearer <token>` vs. bare token) unconfirmed | Confirmed both work identically (`200 OK` either way) against this instance. |
| `timeseries` per-item bundle shape (`{item, values: [...]}`), per-point shape | **Confirmed (2026-09-23)**, both outer bundle and inner `{v, q, t}` point: `[{"item":"ApisOT.Signal19","values":[{"v":423.99,"q":"Good","t":"2026-09-23 00:05:00.344"}, ...]}]`. Whether `endtime` is inclusive or exclusive remains unconfirmed (the confirmed window had continuous data, so the boundary wasn't exercised). |
| Request timestamps (`updatedSince`/`starttime`/`endtime`) sent as ISO-8601 (matching the spec's own example, e.g. `2023-06-01T12:00:00Z`) | **Wrong — found and fixed (2026-09-23).** Every ISO-8601-formatted request for a 2026 date returned a raw `500 Internal Server Error` / `"Invalid time string: ..."`, regardless of whether the timestamp was hours in the past or seconds in the future (ruling out a past/future validation rule). The real required format is space-separated with no timezone suffix (e.g. `2026-09-23 00:05:00`) — identical in style to the response `value.t` field. Fixed in `apis.py`/`apis_schemas.py` via a new `WIRE_TIMESTAMP_FORMAT` constant and `_to_wire_timestamp()` conversion applied only at the point of building the HTTP request; the connector's internal cursor/offset representation and the user-facing `start_timestamp` option remain ISO-8601. |
| `item` wildcard patterns (`Work*.Sig*`) work identically on every endpoint | **Wrong — found and fixed (2026-09-23).** `values`/`timeseries` reject wildcard patterns outright: a bare `*` fails with `HTTP 500: "<instance>: module * not found"`, and even a real module prefix like `ApisOT.*` fails with `"<instance>: item ApisOT.* not found"`. Only `items` genuinely supports wildcards (confirmed earlier via `items_wildcard.txt`). Separately confirmed: multiple **exact** item names CAN be batched into one `values`/`timeseries` request via repeated `item=` params (the spec's own `array`/`explode: true` style) — tested with 2 real item names in one call, both returned. Fixed in `apis.py` via a new `_resolve_exact_items()` method: any wildcard pattern given to `values`/`timeseries` is resolved to real names via one `items` lookup first, then batched into groups of `items_per_request` (new option, default `50` — a placeholder; the real upper limit on batch size is unconfirmed). |

## **Authorization**

- **Method**: Bearer token, declared in the spec as an `apiKey`-style security scheme named `BearerAuth`:
  ```json
  "BearerAuth": { "type": "apiKey", "name": "Authorization", "in": "header" }
  ```
- This scheme is applied globally (top-level `security: [{"BearerAuth": []}]`) and again per-operation on every path, so **every endpoint in this API requires the `Authorization` header**.
- **Assumption / ambiguity**: the spec models this as a raw `apiKey` header named `Authorization` rather than an `http`/`bearer` scheme. It does not state whether the header value must be sent as `Bearer <token>` or as the raw token only. The connector should default to the conventional `Authorization: Bearer <token>` form (matching the task description's characterization of this as "Bearer token, apiKey-style") and treat this as configurable/overridable if live testing shows otherwise. Flag this explicitly in Known Quirks.
- No OAuth2/refresh-token flow is defined anywhere in the spec — there is no token endpoint, no scopes, no `flows` object. The connector should treat the bearer token as a **static, pre-issued credential** supplied by the user (e.g., via connector config), not something the connector obtains through an OAuth exchange.
- No API key placement in query string or body is defined — header only.

**Example API request (assumed header format):**
```bash
curl -X GET "http://<host>:<port>/hive?format=json" \
  -H "Authorization: Bearer <token>"
```

## **Object List**

The spec exposes exactly six operations across four resource "shapes" nested under a Hive **instance**. There is no dedicated schema/discovery endpoint beyond these — the object list below **is** the full set of resources in the spec (static list; not separately retrievable via a meta-API).

| # | Endpoint | Method | Tag | Purpose |
|---|----------|--------|-----|---------|
| 1 | `/hive` | GET | Browse | List all Hive **instances** |
| 2 | `/hive/{instance}/runstate` | GET | Browse | Runstate of one instance |
| 3 | `/hive/{instance}/modules` | GET | Browse | List modules within a running instance |
| 4 | `/hive/{instance}/items` | GET | Browse | List items (points/tags) within an instance, filterable by name/wildcard, across one or more modules |
| 5 | `/hive/{instance}/values` | GET, POST | Real-time | GET: latest/current values for named items. POST (`operationId: writeItemValues`): write values to items |
| 6 | `/hive/{instance}/timeseries` | GET | Timeseries | Historical (raw or aggregated) values for named items over a time range |

**Nesting**: every resource except `/hive` itself is scoped under a specific `{instance}` path segment, and `items`/`values`/`timeseries` are further scoped implicitly by `module` (item names look like `Worker.Signal1`, i.e. `<Module>.<Item>`). There is no separate `/hive/{instance}/modules/{module}/items` endpoint — module-level filtering of items happens by wildcard pattern on the flat `item` query parameter (e.g. `Work*.Sig*`) rather than through path nesting.

**`/hive` is a required upstream lookup, not an ingestible table.** It only returns the list of instance names needed to parameterize every other call (`{instance}` path segment). It has no incremental semantics, no meaningful schema of its own beyond instance identifiers, and would typically be called once per connector run (or cached in connector configuration) to enumerate/validate configured instances — **do not treat it as a row-producing table in the connector.** It should be modeled as a config-time or discovery-time call.

**Recommended table scope for this connector** (all objects share the same host, same Bearer auth, same `{instance}` path-parameter pattern, and the same thin/undocumented response-body style, so they are handled together in a single research batch — no deferral needed):

| Candidate table | Ingest? | Recommended ingestion type | Notes |
|---|---|---|---|
| `hive` (instances) | **No** — upstream/config lookup only | n/a | Drives the `{instance}` value(s) used by every other call; see above |
| `runstate` | Optional / low priority | `snapshot` | Single scalar-ish status value per instance; more useful as a pre-flight health check than a table (see Known Quirks) |
| `modules` | **Yes** | `snapshot` | Slowly-changing catalog of modules per instance |
| `items` | **Yes** | `snapshot` | Slowly-changing catalog of points/tags (dimension-like); no update/delete markers in spec |
| `values` | **Yes** | `cdc` (upsert-only, via `updatedSince`) | One row per item = current value; `updatedSince` is a genuine incremental cursor signal |
| `timeseries` | **Yes** | `append` | Historical, immutable readings per item over `[starttime, endtime)`; windowed incremental strategy |

## **Object Schema**

The spec is intentionally thin: **most operations declare only `"200": {"description": "Success"}` with no `content`/`schema` block at all.** Only the `values` GET (and its POST request body) has a declared JSON schema. This is a material gap in the spec itself, not an omission by this document — every place a schema is "unknown" below is unknown in the source spec, not guessed.

### `hive` (instances) — response schema: **not defined in spec**
- No `content` schema on the `200` response. Only inference available: the endpoint "returns a list of all hive instances" and instance names elsewhere look like plain strings (e.g. `ApisHive`, used as the `{instance}` path parameter).
- `TBD`: whether the payload is a bare JSON array of strings (`["ApisHive", ...]`) or an array of objects (`[{"name": "ApisHive", ...}]`) — must be confirmed against a live instance during `/validate-connector`.

### `runstate` — response schema: **not defined in spec**
- No `content` schema on `200`. Description states it "Returns \"none\" if the instance is not running," implying the payload is (or contains) a short state string, e.g. `"running"` / `"none"`. Exact field name/wrapper object is `TBD`.

### `modules` — response schema: **not defined in spec**
- No `content` schema on `200`. Described only as "Current list of all modules in an instance, which must be running." No field names (e.g. module name, type, status) are declared anywhere in the spec. `TBD`: full field list — must be captured from a live sample response.

### `items` — response schema: **not defined in spec**
- No `content` schema on `200`/`206`. Query parameters imply the item's identity is at minimum a name string (e.g. `Worker.Signal1`), and the optional `attrib` parameter (array of numbers = "Attribute ID") implies each item can carry a variable, numerically-indexed set of extra attributes when requested — but the attribute-ID-to-meaning mapping is **not defined anywhere in the spec** (`TBD`, likely requires an out-of-band attribute registry from the underlying product).

### `values` — response schema: **defined** (`single_value_t`, referenced by the array response)

```json
{
  "type": "array",
  "items": { "$ref": "#/components/schemas/single_value_t" }
}
```

`single_value_t`:

| Field | Type (per spec) | Notes |
|---|---|---|
| `item_name` | `string` | Name of the item, e.g. `Worker.Signal1` |
| `value.v` | untyped (`{}` — no `type` constraint in spec) | The actual value; type varies per underlying point (numeric, string, boolean are all plausible — spec places no constraint) |
| `value.q` | `string` | Quality indicator. The spec types this as an open string, while the `quality` *query* parameter only offers the enum `good`/`uncertain`/`bad` — it is `TBD`/ambiguous whether `q` in the response is restricted to those same three values or can carry richer/raw quality codes (e.g. OPC quality codes) |
| `value.t` | `string` | Timestamp of the value; format not explicitly constrained (presumed ISO 8601 based on `apis_datetime_format`, but not confirmed for the *response* — only request-side datetime params reference that schema) |

This same `single_value_t` schema is reused, unmodified, as the **POST request body** for `/hive/{instance}/values` (`operationId: writeItemValues`) — i.e., writes use the exact same shape as reads. This is a **write** operation and is out of scope for this read-focused connector, documented here only for completeness/context.

### `timeseries` — response schema: **not defined in spec at all**
- Neither `200` nor `206` declares a `content` schema — this is the largest schema gap in the spec. Given `aggregate`/`interval` parameters exist, the response almost certainly returns, per requested item, either raw `(v, q, t)` triples (structurally similar to `single_value_t`) or aggregated value(s) per resample `interval`, but **this is an assumption, not a confirmed fact** — mark as `TBD: confirm exact timeseries response shape against a live instance before finalizing the connector schema/StructType.`

## **Get Object Primary Keys**

No endpoint in the spec declares a primary key, unique identifier field, or `id`-style field of any kind — all "keys" below are inferred from the parameters used to address a row, not from a declared schema field:

| Table | Inferred primary key | Basis |
|---|---|---|
| `hive` (instances, not ingested) | instance name | Used as the `{instance}` path parameter everywhere else; n/a since not ingested |
| `runstate` | `instance` | One state per instance per call; not really record-shaped |
| `modules` | (`instance`, module name) | Module name field itself is undeclared (`TBD`) — assume the returned list contains a name-like field per module, composited with the queried `instance` |
| `items` | (`instance`, item name) | Item name is the addressable unit everywhere (`item` query param, `item_name` field in `single_value_t`); composite with `instance` since item names are only unique within an instance |
| `values` | (`instance`, `item_name`) for a snapshot/current-value read; add `value.t` if treating each poll as an appended reading rather than an upserted snapshot | `item_name` is explicit in `single_value_t`; `instance` is required for scoping since names are not globally unique across instances |
| `timeseries` | (`instance`, `item_name`, timestamp) | Standard historian primary key shape: item identity + point-in-time; timestamp field name itself is `TBD` per the schema gap noted above, presumed `t` by analogy with `single_value_t` |

## **Object's Ingestion Type**

| Table | Ingestion type | Rationale |
|---|---|---|
| `hive` (instances) | n/a (not ingested) | Upstream/config lookup, called out explicitly above |
| `runstate` | `snapshot` | A live status check, not an accumulating dataset; no timestamp/cursor field; full refresh (single row per instance) each poll if ingested at all |
| `modules` | `snapshot` | No `updatedSince`/cursor parameter exists on this endpoint; no created/modified timestamp field is declared; must be fully re-listed each run. Module membership is expected to change rarely (config-time changes), so `snapshot` with periodic full refresh is appropriate. No native delete signal — a module disappearing from the list is the only way to detect a delete, i.e., **no `cdc_with_deletes` support** |
| `items` | `snapshot` | Same reasoning as `modules`: no incremental parameter, no timestamp field, no delete feed. Wildcard filtering (`item` param) narrows scope but does not provide a change cursor. Treat as a slowly-changing dimension/catalog table, fully re-pulled each run (optionally scoped by a stable wildcard pattern per table to bound response size) |
| `values` | `cdc` (upsert-only) | The `updatedSince` query parameter (typed as `apis_datetime_format`) is an explicit incremental-read signal: "return only values updated after this time." Each poll returns the current value per item (one row per item, keyed by `item_name`), so this behaves as an **upsert stream** keyed by `(instance, item_name)`, cursored on `updatedSince`/`value.t`. No delete endpoint or tombstone field exists, so this cannot be `cdc_with_deletes` — an item being retired would simply stop appearing, not appear with a delete marker |
| `timeseries` | `append` | Historical values in a time-series store are immutable once recorded (standard historian semantics); the endpoint takes required `starttime`/`endtime` window parameters, which is a textbook append/incremental-windowing pattern (advance `starttime` to the previous run's `endtime` on each subsequent read). No update/delete semantics apply to already-recorded history in this model |

## **Read API for Data Retrieval**

All six operations require the `instance` path parameter (except `/hive` itself) and all support an optional `format` query parameter (`json` default, or `csv`) — this documentation assumes **`format=json`** throughout for connector implementation, since CSV would require a separate parser and the spec gives no CSV schema either.

### 1. `GET /hive` — list instances (upstream lookup, not an ingested table)
- **Parameters**: `format` (optional, default `json`)
- **Pagination**: none defined
- **Example**:
  ```bash
  GET /hive?format=json
  ```
- **Response**: `200` (schema undefined, see Object Schema); `500` on server error. No `400`/`404` (no path params to be invalid).

### 2. `GET /hive/{instance}/runstate`
- **Parameters**: `instance` (path, required); `format` (optional)
- **Pagination**: none (single-value response)
- **Example**:
  ```bash
  GET /hive/ApisHive/runstate?format=json
  ```
- **Response codes**: `200`, `400` (bad request), `404` (instance not found), `500`

### 3. `GET /hive/{instance}/modules`
- **Parameters**: `instance` (path, required); `format` (optional)
- **Precondition**: per spec description, the instance "must be running" for this to return data
- **Pagination**: none defined — no `limit`/`offset`/cursor parameters exist; the endpoint is documented as returning the "current list of all modules," implying a single, complete response
- **Example**:
  ```bash
  GET /hive/ApisHive/modules?format=json
  ```
- **Response codes**: `200`, `400`, `404`, `500`

### 4. `GET /hive/{instance}/items`
- **Parameters**:
  - `instance` (path, required)
  - `item` (query, **required**, array of strings, `style: form, explode: true` — i.e. repeated `item=...&item=...`); supports wildcards, e.g. `Work*.Sig*` matches all items starting with `Sig` in modules starting with `Work`
  - `attrib` (query, optional, array of numbers) — attribute IDs to include as extra fields per item; meaning of specific IDs is undocumented in the spec (`TBD`)
  - `format` (optional)
- **Pagination**: none defined. Notably this endpoint (like `values` and `timeseries`) supports a `206 Partial Content` response in addition to `200`, which strongly suggests the server can truncate large result sets — **but the spec defines no continuation token, `Link` header, or offset parameter for resuming/paging through a `206` response.** This is a real limitation: **flag as `TBD`/Known Quirk** — the connector may need to narrow `item` wildcard patterns (e.g., per-module fan-out) to avoid truncation rather than relying on a documented pagination mechanism.
- **Example**:
  ```bash
  GET /hive/ApisHive/items?item=Worker.Signal1&format=json
  GET /hive/ApisHive/items?item=Work*.Sig*&attrib=1&attrib=2&format=json
  ```
- **Response codes**: `200`, `206` (partial content — see pagination note above), `400`, `404`, `500`

### 5a. `GET /hive/{instance}/values` — current values (READ)
- **Parameters**:
  - `instance` (path, required)
  - `item` (query, **required**, array of strings, repeated `item=` params)
  - `updatedSince` (query, optional, `apis_datetime_format` — ISO 8601 or "OPC time" relative expression such as `DAY-1D`) — **this is the incremental-read cursor**: "return only values updated after this time"
  - `quality` (query, optional, enum `good`/`uncertain`/`bad`) — minimum quality filter
  - `format` (optional)
- **Pagination**: none defined beyond the `206 Partial Content` status (same caveat as `items` above — no documented continuation mechanism)
- **Incremental strategy**: poll with `updatedSince = <max value.t (or run timestamp) from previous poll>`; response is an array of `single_value_t` keyed by `item_name`; treat as upsert (`cdc`) keyed on `(instance, item_name)`
- **Example request**:
  ```bash
  GET /hive/ApisHive/values?item=Worker.Signal1&item=Worker.Signal2&updatedSince=2023-06-01T12:00:00Z&quality=good&format=json
  ```
- **Example response** (per declared schema):
  ```json
  [
    {
      "item_name": "Worker.Signal1",
      "value": { "v": 42.7, "q": "good", "t": "2023-06-01T12:03:11Z" }
    },
    {
      "item_name": "Worker.Signal2",
      "value": { "v": 17, "q": "good", "t": "2023-06-01T12:03:09Z" }
    }
  ]
  ```
- **Response codes**: `200` (schema as above), `206`, `400`, `404`, `500`

### 5b. `POST /hive/{instance}/values` — write values (**NOT a read operation — out of scope for ingestion**)
- `operationId: writeItemValues`. Request body is an array of `single_value_t` (same shape as the GET response). Documented here only because it shares the endpoint with the read path; the connector should never call this operation as part of ingestion. Per the skill's "Read operations only" principle, no further design detail is captured for this operation.

### 6. `GET /hive/{instance}/timeseries` — historical values
- **Parameters**:
  - `instance` (path, required)
  - `item` (query, **required**, array of strings)
  - `starttime` (query, **required**, `apis_datetime_format`)
  - `endtime` (query, **required**, `apis_datetime_format`)
  - `aggregate` (query, optional) — large enum of aggregation functions (`interpolative`, `total`, `average`, `timeaverage`, `count`, `minimum`, `maximum`, `sum`, `median`, `delta`, `range`, `percent_good`, `percent_bad`, `worst_quality`, `movingaveragebytime`, etc. — see spec for the full ~35-value enum). If omitted, presumed raw/recorded values are returned (not stated explicitly in spec — `TBD`)
  - `interval` (query, optional, integer, `1`–`86400` seconds, default `3600`) — resample interval for aggregated data; relationship to `aggregate` (e.g., whether `interval` is ignored when `aggregate` is absent) is not stated in the spec (`TBD`)
  - `quality` (query, optional, enum `good`/`uncertain`/`bad`)
  - `format` (optional)
- **Pagination**: none defined; only `200`/`206` status codes, same "possible truncation with no documented continuation token" caveat as above
- **Incremental strategy**: window-based — on each run, set `starttime` = previous run's `endtime` (or last successfully-read timestamp), and `endtime` = current time (or a fixed lookback window); this is a standard time-windowed append pattern for historian APIs. Because the response schema is entirely undeclared (see Object Schema), the exact cursor field to persist (e.g., last row's timestamp vs. simply the requested `endtime`) should default to the requested `endtime` boundary until a live response is inspected.
- **Example request**:
  ```bash
  GET /hive/ApisHive/timeseries?item=Worker.Signal1&starttime=2023-06-01T00:00:00Z&endtime=2023-06-02T00:00:00Z&aggregate=average&interval=3600&quality=good&format=json
  ```
- **Example response**: **not defined in spec** — `TBD: must capture from a live instance.` A plausible (unconfirmed) shape by analogy with `single_value_t`, given `item`, `aggregate`, and `interval` are all per-item/per-bucket concepts:
  ```json
  [
    {
      "item_name": "Worker.Signal1",
      "values": [
        { "v": 41.9, "q": "good", "t": "2023-06-01T00:00:00Z" },
        { "v": 44.2, "q": "good", "t": "2023-06-01T01:00:00Z" }
      ]
    }
  ]
  ```
  This shape is a **best-effort inference, not a documented fact** — do not hard-code field names from it without live confirmation.
- **Response codes**: `200`, `206`, `400`, `404`, `500`

### Rate limits
- **Not documented anywhere in the spec.** No `429` response is declared on any operation, no rate-limit headers are mentioned, and no `x-ratelimit*` extensions exist in the spec. `TBD: rate limits are unknown; must be determined empirically during live validation (Phase 2) and handled defensively (e.g., conservative concurrency/backoff) until then.`

### Comparing read options for the same data
- **Current value** (`values`) vs. **history** (`timeseries`) are not interchangeable: `values` only returns the latest value per item (optionally filtered to only-if-updated-since a cursor), while `timeseries` requires an explicit `[starttime, endtime]` window and can aggregate/resample. There is no "point-in-time historical value" endpoint (equivalent of PI Web API's `value?time=`) — retrieving a single historical value would require calling `timeseries` with a narrow window around the desired time.
- `items` vs. `modules`: `items` is the finer-grained catalog (tags/points) and can be scoped to specific modules via wildcard; `modules` only enumerates the containers, with no way to retrieve item counts or per-module item lists directly (must call `items` with a module-matching wildcard, e.g. `ModuleName.*`, to get that module's items).

## **Field Type Mapping**

| API concept | Spec type | Standard type mapping | Notes |
|---|---|---|---|
| `apis_datetime_format` (used for `updatedSince`, `starttime`, `endtime`) | `string` | `timestamp` (when absolute) | Accepts **either** ISO 8601 basic combined date-time (e.g. `2023-06-01T12:00:00Z`) **or** an "OPC time" relative expression (e.g. `DAY-1D`, per the spec's own description referencing OPC UA Historical Access relative-time syntax). For deterministic, replayable incremental reads, the connector should always send **absolute ISO 8601** timestamps it computes itself, not relative OPC expressions, even though the API accepts both. |
| `single_value_t.item_name` | `string` | `string` | Fully-qualified item name, `Module.Item` convention observed in examples (e.g. `Worker.Signal1`) — not formally declared as a naming rule in the spec, just observed in parameter descriptions |
| `single_value_t.value.v` | untyped (empty schema `{}`) | variant — cast defensively (try numeric, fall back to string/boolean) | No type constraint at all in the spec; the underlying point's real data type is only knowable from the source system (e.g. via `attrib` metadata on `items`, itself undocumented), not from this schema |
| `single_value_t.value.q` | `string` | `string` (quality code) | The `quality` filter *parameters* on `values`/`timeseries` only enumerate `good`/`uncertain`/`bad`, but the *response* `q` field has no such enum constraint — treat as an open string and do not assume only those three values appear |
| `single_value_t.value.t` | `string` | `timestamp` | Presumed ISO 8601 by analogy with `apis_datetime_format`, but the response field itself carries no explicit `format` or schema reference confirming this — `TBD` |
| `items.attrib` (Attribute ID) | `number` (query param) | n/a (request-only) | Numeric attribute IDs requested to enrich `items` responses; no ID-to-meaning registry exists in this spec |
| `timeseries.aggregate` | `string` enum (~35 values) | n/a (request-only) | Selects an aggregation function server-side; full literal enum list is in the spec (`interpolative`, `total`, `average`, `timeaverage`, `count`, `regstddev`, `minimumactualtime`, `minimum`, `maximumactualtime`, `maximum`, `start`, `end`, `delta`, `regslope`, `regconst`, `regdev`, `variance`, `range`, `duration_good`, `duration_bad`, `percent_good`, `percent_bad`, `worst_quality`, `annotation_count`, `sum`, `interpolative_0`, `median`, `ua_minimumactualtime2`, `ua_maximumactualtime2`, `ua_range2`, `vec_sum`, `vec_aver`, `vec_min`, `vec_max`, `displayvalues`, `lowpassfilter`, `movingaveragebycount`, `movingaveragebytime`, `ua_percentgood`, `ua_percentbad`) |
| `timeseries.interval` | `integer` (1–86400, default 3600) | `integer` (seconds) | Resample bucket size in seconds; only meaningful in combination with `aggregate` (relationship not explicitly stated) |
| `format` (all endpoints) | `string` enum `json`/`csv`, default `json` | n/a | Connector should always use `json`; CSV response shape is entirely undefined in the spec |

## Known Quirks / Assumptions

1. **Auth header value format is unconfirmed.** Spec declares a raw `apiKey` header named `Authorization`, not a formal `bearer`/`oauth2` scheme — assumed `Bearer <token>` convention; verify during live testing.
2. **Most response bodies have no declared schema at all** (`hive`, `runstate`, `modules`, `items`, `timeseries` all lack a `content`/`schema` block on their `200` responses). Only `values` (GET response + POST body) has a declared schema (`single_value_t`). All field names for the other five endpoints are inferred/assumed and must be confirmed against a live instance before finalizing the connector's output schema.
3. **No pagination mechanism is documented anywhere**, despite `items`, `values`, and `timeseries` all supporting a `206 Partial Content` response (implying server-side truncation is possible). There is no `limit`, `offset`, `cursor`, `nextPageToken`, or `Link` header defined. Mitigation: scope `item` wildcard patterns narrowly and/or window `timeseries` requests tightly to avoid truncation, since there's no documented way to detect or resume a truncated response.
4. **No rate limits are documented.** No `429` responses, headers, or extensions anywhere in the spec.
5. **No delete-tracking mechanism exists for any object** (`modules`, `items`, or otherwise) — deletions can only be inferred by diffing successive full snapshots.
6. **`interval`'s interaction with `aggregate`** on `timeseries` is not explicitly specified (e.g., whether `interval` is ignored when `aggregate` is omitted).
7. **`attrib` (Attribute ID) values on `items`** have no documented ID-to-meaning mapping in this spec.
8. **Server URL in the spec (`http://localhost:8080`) is a local/dev placeholder** — real deployments will use a customer-specific host/port; this is expected/normal for a self-hosted, on-prem product and not itself a spec defect, but the connector must not hard-code this URL.
9. Timeseries data, per general knowledge of this class of historian product (see Overview), is typically only available for items that have been explicitly configured for logging into a companion time-series store — an item existing in `items` does not necessarily guarantee it has historical data in `timeseries`. This is external context, not something stated in the spec itself, and should be treated as a soft assumption only.

## Sources and References

| Source Type | URL / Location | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Provided OpenAPI spec (primary/authoritative) | `src/databricks/labs/community_connector/sources/apis/apis_openapi.json` (local repo file, OpenAPI 3.1.0, title "Apis REST Service", version 0.4.0) | 2026-09-21 | Highest | All endpoints, parameters, security scheme, and the one declared schema (`single_value_t`, `apis_datetime_format`) used throughout this document |
| Web search | Search for `"Apis REST Service" Hive instance modules items runstate historian API` | 2026-09-21 | Low (context only) | No direct hit on this exact product's public docs |
| Web search | Search for `"apis_datetime_format" OR "single_value_t" Hive REST API historian OPC` | 2026-09-21 | Medium (context only) | Surfaced `docs.prediktor.com` pages (e.g. "OPC UA Catch-Up - Apis Foundation 8", "Apis Hive Modules") strongly suggesting this spec belongs to Prediktor's "Apis Foundation" Hive product family; search-result snippets described Hive as a "real-time data hub and container for plug-in modules" and referenced a companion "HoneyStore" time-series database and `ApisLogger`/`ApisOpcUa` modules |
| Web search | Search for `Prediktor Apis Foundation 8 REST API Hive items values timeseries documentation` | 2026-09-21 | Medium (context only) | Snippet indicating historical/timeseries reads on a "regular item" require that item to be logged into HoneyStore via an `ApisLogger` module (`LoggerName` attribute) — used only as soft framing in Known Quirks, not as a schema source |
| Attempted direct fetch | `https://docs.prediktor.com/docs/foundation8/APIS_Hive_Modules/ApisOpcUa/CatchUp.html` and `https://docs.prediktor.com/docs/foundation8/APIS_Hive_Modules/APIS_Modules.html` | 2026-09-21 | n/a | **Fetch failed** (`getaddrinfo ENOTFOUND docs.prediktor.com`) — this environment's network access could not resolve/reach `docs.prediktor.com` directly, so no official Prediktor documentation could be directly read or cited beyond search-result snippets. This is a documented limitation, not an omission. |
| Airbyte / Fivetran / Singer check | General web search for known connectors to this API | 2026-09-21 | n/a | No existing Airbyte, Fivetran, or Singer connector was found for this niche industrial-historian product; no cross-reference implementation exists to validate against |

**Conflicts / resolution**: There were no conflicting sources to reconcile — the OpenAPI spec is the only source with concrete, verifiable detail; all secondary web-search context was used solely for high-level product framing and is explicitly called out wherever it informed a statement in this document (see Overview and Known Quirks item 9). No field name, parameter, or schema claim in this document originates from anything other than the spec file itself.
