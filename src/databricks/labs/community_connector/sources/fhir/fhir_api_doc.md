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

Primary key for all resources: `id` (unique per resource type per server, per FHIR spec).
All resources use CDC ingestion via `_lastUpdated` cursor.

---

## Object Schema

No schema discovery API. Schemas are hardcoded per resource type.

All resources share common fields:
- `id` (string): FHIR logical id, up to 64 chars, case-sensitive
- `resourceType` (string): e.g. "Patient"
- `lastUpdated` (timestamp): from `meta.lastUpdated`, nullable (spec: 0..1 cardinality)
- `raw_json` (string): full FHIR resource as JSON string

Resource-specific typed fields:

**Patient:** gender (string), birthDate (string), active (boolean), name_text (string), name_family (string)
**Observation:** status (string), code_text (string), code_system (string), code_code (string), subject_reference (string), effective_datetime (timestamp), value_quantity_value (double), value_quantity_unit (string), value_string (string), issued (timestamp)
**Condition:** clinical_status (string), verification_status (string), code_text (string), subject_reference (string), onset_datetime (timestamp), recorded_date (string)
**Encounter:** status (string), class_code (string), subject_reference (string), period_start (timestamp), period_end (timestamp)
**Procedure:** status (string), code_text (string), subject_reference (string), performed_datetime (timestamp)
**MedicationRequest:** status (string), intent (string), medication_text (string), subject_reference (string), authored_on (string)
**DiagnosticReport:** status (string), code_text (string), subject_reference (string), effective_datetime (timestamp), issued (timestamp)
**AllergyIntolerance:** clinical_status (string), verification_status (string), code_text (string), patient_reference (string), recorded_date (string)
**Immunization:** status (string), vaccine_code_text (string), patient_reference (string), occurrence_datetime (timestamp)
**Coverage:** status (string), beneficiary_reference (string), payor_reference (string), period_start (string), period_end (string)
**CarePlan:** status (string), intent (string), subject_reference (string), period_start (timestamp), period_end (timestamp)
**Goal:** lifecycle_status (string), description_text (string), subject_reference (string), start_date (string)
**Device:** status (string), device_name (string), type_text (string), patient_reference (string)
**DocumentReference:** status (string), type_text (string), subject_reference (string), date (timestamp)

Unknown resource types → fallback to common fields + raw_json only.

---

## Object Ingestion Type

All resources: `ingestion_type=cdc`, `cursor_field=lastUpdated`, `primary_keys=["id"]`

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
- If absent: yield record but don't advance cursor from it

### Error handling
- `429`, `500`, `502`, `503`: retry with exponential backoff (max 5 retries, initial 5s; 5→10→20→40→80s)
- `429`: honor `Retry-After` header if present
- `400`, `401`, `403`, `404`: raise immediately with descriptive message
- All HTTP calls must have `timeout=60` (prevents hangs on slow servers)

### Rate limits
HAPI FHIR public server: no documented limit but use small `_count` (5-10) in tests.
Production EHRs (Epic, Cerner): check server's CapabilityStatement. Use `max_records_per_batch`.

### Table options (passed via table_options dict)
- `page_size` (default: "100"): `_count` sent to FHIR server
- `max_records_per_batch` (default: "1000"): max records per read_table() call
- `resource_types` (connection-level): comma-separated resource type override

---

## Field Type Mapping

| FHIR type | Spark type |
|-----------|-----------|
| string / code / uri | StringType |
| instant / dateTime | TimestampType (stored as ISO string, framework converts) |
| date | StringType (FHIR date is not a full timestamp) |
| boolean | BooleanType |
| decimal / integer / positiveInt | DoubleType / LongType |
| Reference.reference | StringType |

---

## Sources and References
- FHIR R4 HTTP spec: https://hl7.org/fhir/R4/http.html (verified)
- FHIR R4 Search spec: https://hl7.org/fhir/R4/search.html (verified)
- FHIR R4 Bundle spec: https://hl7.org/fhir/R4/bundle.html (verified)
- FHIR R4 Resource spec: https://hl7.org/fhir/R4/resource.html (verified)
- SMART Backend Services: https://www.hl7.org/fhir/smart-app-launch/backend-services.html (verified)
- Test server: https://hapi.fhir.org/baseR4 (HAPI FHIR R4, no auth, public)
