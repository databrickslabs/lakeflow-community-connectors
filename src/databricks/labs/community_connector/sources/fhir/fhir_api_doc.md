# FHIR R4 API Documentation

## Authorization

Three supported auth methods (connector selects at runtime via `auth_type` option):

### Token Discovery

`token_url` is optional for `jwt_assertion` and `client_secret` auth types. If omitted, the connector
auto-discovers the token endpoint from the FHIR server's SMART configuration:

```
GET {base_url}/.well-known/smart-configuration
Accept: application/json
```

The connector reads `token_endpoint` from the response. Raises `RuntimeError` if discovery fails or
`token_endpoint` is absent. Provide `token_url` explicitly to skip discovery.

### Method A: SMART Backend Services — JWT Assertion (`auth_type: jwt_assertion`)
Per official SMART on FHIR Backend Services spec. Uses asymmetric key JWT (RFC 7523).

Token request (POST to token_url, `Content-Type: application/x-www-form-urlencoded`):
- `grant_type=client_credentials`
- `client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer`
- `client_assertion=<signed one-time JWT>`
- `scope=<scope>` (if provided)

JWT header fields: `alg`=`private_key_algorithm` (default `RS384`), `kid`=user-provided key ID (required), `typ`=`JWT`.
JWT payload claims: `iss`=client_id, `sub`=client_id, `aud`=token_url, `jti`=uuid4(), `iat`=now, `exp`=now+5min.
Sign with PEM private key using `private_key_algorithm` (`RS384` default; `ES384` also supported; RS256 not recommended).

If `kid` is not provided, a `ValueError` is raised — `kid` must match the key registered in the FHIR server's JWK Set.

Required connection parameters: `base_url`, `client_id`, `private_key_pem`, `kid`
(plus `token_url` or auto-discovery).

Optional connection parameters: `scope` (strongly recommended — FHIR servers typically require it),
`private_key_algorithm` (default: `RS384`).

Token response: `{"access_token": "...", "token_type": "bearer", "expires_in": 300}`

### Method B: Client Secret (`auth_type: client_secret`)
For servers supporting symmetric client credentials (SMART v1 confidential-symmetric clients).

Token request (POST to token_url, `Content-Type: application/x-www-form-urlencoded`):
- POST body: `grant_type=client_credentials` (and `scope=<scope>` if provided)
- HTTP Basic auth header: `Authorization: Basic B64Encode(client_id:client_secret)`

`client_id` and `client_secret` are NOT sent in the POST body — they are passed exclusively via
HTTP Basic authentication, per the SMART client-confidential-symmetric spec.

Required connection parameters: `base_url`, `client_id`, `client_secret`
(plus `token_url` or auto-discovery).

### Method C: No Auth (`auth_type: none`)
For open/dev FHIR servers (e.g. HAPI FHIR public server). No Authorization header sent.

**Token expiry:** Tokens are short-lived (recommended max 300s). Re-fetch 30s before expiry.

**Auth placement:** `Authorization: Bearer <token>` header on all requests.
**Accept header:** `Accept: application/fhir+json`

---

## Object List

FHIR resources are the "tables". The list is static (not discoverable via API).

Default resources (configurable via `resource_types` option, comma-separated):
Patient, Observation, Condition, Encounter, Procedure, MedicationRequest,
DiagnosticReport, AllergyIntolerance, Immunization, Coverage, CarePlan,
Goal, Device, DocumentReference

Each resource type is its own table. Primary key: `resource_id`, the FHIR logical
id (unique per resource type per server, per FHIR spec). Cursor: `last_updated`
(`meta.lastUpdated`).

---

## Object Schema

No schema discovery is needed: every resource-type table shares one schema, with
the whole resource stored as a VARIANT. Any FHIR R4 resource type works without
schema code.

| Column | Spark type | Source |
|---|---|---|
| `resource_type` | StringType, not null | `resourceType` (constant per table) |
| `resource_id` | StringType, not null | `id` (FHIR logical id, up to 64 chars, case-sensitive) |
| `last_updated` | TimestampType | `meta.lastUpdated`, nullable (spec: 0..1 cardinality) |
| `resource` | VariantType | the whole resource. Bulk NDJSON lines are stored verbatim, so decimals keep full precision; REST re-serialises the parsed JSON, so decimals round to float64 |
| `fhir_version` | StringType | `fhirVersion` from the server's CapabilityStatement (`GET /metadata`) |
| `ingested_at` | TimestampType | connector read time |

---

## Object Ingestion Type

| Mode | `ingestion_type` | `cursor_field` | `primary_keys` |
|---|---|---|---|
| REST, or bulk `since` / `typefilter` (default) | `cdc` | `last_updated` | `["resource_id"]` |
| Same, with `track_deletes=true` | `cdc_with_deletes` | `last_updated` | `["resource_id"]` |
| Bulk `full` | `snapshot` | none | `["resource_id"]` |

### Deletes via history (`track_deletes=true`)
```
GET {base_url}/{ResourceType}/_history?_since={cursor}&_count={page_size}
```
Returns a FHIR Bundle of type `history`, **newest first**. A deletion is an entry
with `request.method = "DELETE"` and `request.url = "{Type}/{id}/_history/{n}"`.
On public HAPI R4 (measured 2026-11-01), DELETE entries carry no resource and no
`response.lastModified`. Each deletion is therefore sequenced by the closest newer
entry's timestamp. The whole history since the cursor is read once per trigger;
the next cursor is the trigger start minus 5 minutes. Servers without type-level
`_history` support (e.g. Epic) cannot use `track_deletes`.

---

## Read API for Data Retrieval

### Search endpoint
```
GET {base_url}/{ResourceType}?_count={page_size}&_lastUpdated=gt{cursor}
Accept: application/fhir+json
Authorization: Bearer {token}
```

**Response format:** FHIR Bundle (searchset):
```json
{
  "resourceType": "Bundle",
  "type": "searchset",
  "entry": [
    {"resource": {"resourceType": "Patient", "id": "...", "meta": {"lastUpdated": "..."}, ...}},
    ...
  ],
  "link": [
    {"relation": "self", "url": "..."},
    {"relation": "next", "url": "..."}
  ]
}
```

### Pagination (FHIR R4 spec-verified)
Follow `Bundle.link[relation="next"].url` until no "next" link exists.
Links are opaque — follow as-is, never parse or modify.
`_count` parameter: server SHALL NOT return more than requested, but may return fewer.

### Incremental sync via `_lastUpdated` (FHIR R4 spec-verified)
- Standard search parameter, available on all resource types
- **CAVEAT:** Servers are NOT required to implement it (only `_id` is mandatory)
- Prefix `gt` = strictly greater than: `_lastUpdated=gt2024-01-01T00:00:00Z`
- First run (no cursor): omit `_lastUpdated`, fetch all
- Subsequent runs: `_lastUpdated=gt{last_cursor}`
- Cursor = max `meta.lastUpdated` across fetched records (ISO8601 string comparison is correct)

### `meta.lastUpdated` handling
- Type: FHIR `instant` (ISO8601 with timezone, e.g. `2024-01-15T10:30:00+00:00`)
- Nullable: 0..1 cardinality — not guaranteed present
- On REST write ops, server SHALL update it (so present on server-stored resources in practice)
- If absent: the record cannot advance the cursor; a batch where no record has it ends the trigger

### Error handling
- `429`, `500`, `502`, `503`: retry with exponential backoff (max 5 retries, initial 5s; 5→10→20→40→80s)
- `429`: honor `Retry-After` header if present
- `400`, `401`, `403`, `404`: raise immediately with descriptive message
- Every HTTP call has a timeout (60s REST, 30s token, 300s NDJSON) to prevent hangs

### Rate limits
HAPI FHIR public server: no documented limit but use small `_count` (5-10) in tests.
Production EHRs (Epic, Cerner): check server's CapabilityStatement. Use `max_records_per_batch`.

### Table options (passed via table_options dict)
- `page_size` (default: "100"): `_count` sent to FHIR server
- `max_records_per_batch` (default: "1000"): max records per read_table() call
- `page_delay` (default: "0"): seconds to sleep between pages
- `resource_types`: comma-separated resource types; each becomes a table
- `track_deletes` (default: "false"): read deletions from `_history`

---

## Field Type Mapping

FHIR datatypes stay inside the `resource` VARIANT; the connector does not map
them to Spark types. Cast with VARIANT paths at query time, e.g.
`resource:birthDate::string` (FHIR `date` may be partial, like `"1956"`, which
`::date` turns into `1956-01-01`) or `resource:valueQuantity.value::decimal(38,18)`.

---

## Sources and References
- FHIR R4 HTTP spec: https://hl7.org/fhir/R4/http.html (verified)
- FHIR R4 Search spec: https://hl7.org/fhir/R4/search.html (verified)
- FHIR R4 Bundle spec: https://hl7.org/fhir/R4/bundle.html (verified)
- FHIR R4 Resource spec: https://hl7.org/fhir/R4/resource.html (verified)
- SMART Backend Services: https://www.hl7.org/fhir/smart-app-launch/backend-services.html (verified)
- Test server: https://hapi.fhir.org/baseR4 (HAPI FHIR R4, no auth, public)
