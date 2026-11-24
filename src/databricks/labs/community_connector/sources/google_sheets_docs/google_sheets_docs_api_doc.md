# Google Sheets and Google Docs API Documentation

This document covers **read-only** usage of the Google Sheets API v4, Google Docs API v1, and Google Drive API v3 for building a connector that lists and reads spreadsheets and documents. Listing is done via the Drive API; sheet/document content is read via the Sheets and Docs APIs (and optionally Drive export for Docs).

---

## Authorization

### OAuth 2.0 (Preferred Method)

Google Sheets, Docs, and Drive use OAuth 2.0. The connector **stores** `client_id`, `client_secret`, and `refresh_token`, and exchanges the refresh token for an access token at runtime. The connector **does not** run user-facing OAuth flows; tokens must be provisioned out-of-band (e.g., via a one-time auth script or Google Cloud Console).

**Required credentials:**
- `client_id`: OAuth 2.0 client ID from Google Cloud Console
- `client_secret`: OAuth 2.0 client secret
- `refresh_token`: Long-lived refresh token (obtained with `access_type=offline` during authorization)

**Required scopes (read-only):**
- `https://www.googleapis.com/auth/spreadsheets.readonly` — read spreadsheets and sheet values
- `https://www.googleapis.com/auth/documents.readonly` — read document content
- `https://www.googleapis.com/auth/drive.readonly` — list and get file metadata (required to discover spreadsheets and docs)

**Token exchange request:**
```http
POST https://oauth2.googleapis.com/token
Content-Type: application/x-www-form-urlencoded

client_id={client_id}&client_secret={client_secret}&refresh_token={refresh_token}&grant_type=refresh_token
```

**Token exchange response:**
```json
{
  "access_token": "ya29.a0AfH6SM...",
  "expires_in": 3599,
  "scope": "https://www.googleapis.com/auth/spreadsheets.readonly ...",
  "token_type": "Bearer"
}
```

**Using the access token in API requests:**
```http
GET https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.spreadsheet'&pageSize=100
Authorization: Bearer {access_token}
```

### Alternative: Service Account

For server-to-server or domain-wide access, use a Google Cloud **service account**. The connector can store the service account JSON key and use it to obtain access tokens. Service accounts do not use refresh_token; they use JWT or the metadata server. For accessing user-owned Sheets/Docs, the spreadsheet or document must be **shared** with the service account’s client email. Document this in Known Quirks if supporting service accounts.

---

## Object List

Objects are **not** enumerated by the Sheets or Docs APIs. Spreadsheets and Docs are **files** in Google Drive. The object list is therefore **retrieved via the Drive API** by filtering on `mimeType`.

**How to obtain the list:**
- **Spreadsheets:** Drive API `files.list` with `q=mimeType='application/vnd.google-apps.spreadsheet'`
- **Documents:** Drive API `files.list` with `q=mimeType='application/vnd.google-apps.document'`
- Optionally combine: `q="mimeType='application/vnd.google-apps.spreadsheet' or mimeType='application/vnd.google-apps.document'"`

**Object hierarchy:**

| Object            | Description                          | API that lists it | Sub-objects / usage |
|-------------------|--------------------------------------|-------------------|----------------------|
| **files** (Drive) | Drive files (spreadsheets or docs)   | Drive `files.list`| Filter by mimeType   |
| **spreadsheets**  | Spreadsheet metadata and structure   | Sheets `spreadsheets.get` | Requires `spreadsheetId` (Drive file `id`) |
| **sheets**        | Tabs within a spreadsheet           | From `spreadsheets.get` response (`sheets[]`) | No separate list API |
| **values**        | Cell data in a range                 | Sheets `spreadsheets.values.get` or `values.batchGet` | Requires `spreadsheetId` and `range`(s) |
| **documents**     | Document metadata and content       | Docs `documents.get` | Requires `documentId` (Drive file `id`) |

**Example: list spreadsheets (Drive API):**
```http
GET https://www.googleapis.com/drive/v3/files?q=mimeType%3D'application%2Fvnd.google-apps.spreadsheet'&pageSize=100&fields=nextPageToken,files(id,name,mimeType,modifiedTime,createdTime)&trashed=false
Authorization: Bearer {access_token}
```

**Example response:**
```json
{
  "nextPageToken": "~!!~AI9...",
  "files": [
    {
      "id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
      "name": "My Sheet",
      "mimeType": "application/vnd.google-apps.spreadsheet",
      "modifiedTime": "2025-03-01T12:00:00.000Z",
      "createdTime": "2024-01-15T08:00:00.000Z"
    }
  ]
}
```

**Example: list documents (Drive API):**
```http
GET https://www.googleapis.com/drive/v3/files?q=mimeType%3D'application%2Fvnd.google-apps.document'&pageSize=100&fields=nextPageToken,files(id,name,mimeType,modifiedTime,createdTime)&trashed=false
Authorization: Bearer {access_token}
```

**Notes:**
- Use `trashed=false` to exclude trashed files.
- For spreadsheets, the Drive file `id` is the `spreadsheetId` for the Sheets API. For docs, the Drive file `id` is the `documentId` for the Docs API.
- Sheet names (tabs) and ranges are obtained from `spreadsheets.get`; there is no separate “list sheets” endpoint.

---

## Object Schema

### Drive File (from `files.list` / `files.get`)

| Field          | Type    | Description |
|----------------|---------|-------------|
| `id`           | string  | Stable file ID (use as spreadsheetId or documentId) |
| `name`         | string  | Display name |
| `mimeType`     | string  | e.g. `application/vnd.google-apps.spreadsheet`, `application/vnd.google-apps.document` |
| `createdTime`  | string  | RFC 3339 datetime |
| `modifiedTime` | string  | RFC 3339 datetime (last modified by anyone) |
| `modifiedByMeTime` | string | RFC 3339 (last modified by current user) |
| `trashed`      | boolean | Whether file is in trash |
| `parents`      | array[string] | Parent folder IDs |
| `webViewLink`  | string  | URL to open in browser (if requested in `fields`) |
| `size`         | string  | Size in bytes (often not set for native Google files) |

Default `files.list` returns only `kind`, `id`, `name`, `mimeType`, and `resourceKey`. Use the `fields` query parameter to request additional fields.

### Spreadsheet (from `spreadsheets.get`)

| Field               | Type    | Description |
|---------------------|---------|-------------|
| `spreadsheetId`     | string  | Read-only ID |
| `properties`        | object  | SpreadsheetProperties (see below) |
| `sheets`            | array   | Sheet objects (see below) |
| `namedRanges`       | array   | Named range definitions |
| `spreadsheetUrl`    | string  | Read-only URL |
| `developerMetadata` | array   | Developer metadata |
| `dataSources`       | array   | External data sources |
| `dataSourceSchedules` | array | Output-only refresh schedules |

**SpreadsheetProperties:** `title`, `locale`, `autoRecalc`, `timeZone`, `defaultFormat`, `iterativeCalculationSettings`, `spreadsheetTheme`, `importFunctionsExternalUrlAccessAllowed`.

### Sheet (element of `spreadsheets.get` → `sheets[]`)

| Field             | Type   | Description |
|-------------------|--------|-------------|
| `properties`      | object | SheetProperties (see below) |
| `data`            | array  | GridData (only when `includeGridData=true`) |
| `merges`          | array  | Merge ranges |
| `conditionalFormats` | array | Conditional format rules |
| `filterViews`     | array  | Filter view definitions |
| `protectedRanges` | array | Protected range definitions |
| `basicFilter`     | object | Basic filter (if any) |
| `charts`          | array  | Embedded charts |
| `developerMetadata` | array | Developer metadata |

**SheetProperties:** `sheetId` (integer, stable ID), `title` (string), `index` (integer), `sheetType` (e.g. GRID), `gridProperties` (rowCount, columnCount, frozenRowCount, frozenColumnCount, etc.), `tabColor`, `hidden`, etc.

### ValueRange (from `spreadsheets.values.get` or `values.batchGet`)

| Field            | Type   | Description |
|-----------------|--------|-------------|
| `range`         | string | A1 or R1C1 range that was requested (e.g. `Sheet1!A1:D10`) |
| `majorDimension`| string | `ROWS` or `COLUMNS` |
| `values`        | array  | Array of arrays; inner arrays are rows (if ROWS) or columns (if COLUMNS). Cell values are string, number, or boolean. |

Empty trailing rows/columns are omitted from `values`. Nulls are skipped; empty cells may be missing from inner arrays.

### Document (from `documents.get`)

| Field                    | Type   | Description |
|--------------------------|--------|-------------|
| `documentId`             | string | Output only; document ID |
| `title`                  | string | Document title |
| `revisionId`             | string | Output only; revision ID (opaque, valid ~24h) |
| `suggestionsViewMode`    | enum   | Output only |
| `tabs`                   | array  | Tab objects (multi-tab docs); use `includeTabsContent=true` for full tab content |
| `body`                   | object | Body (StructuralElements); legacy, first tab only unless using tabs |
| `headers`                | object | Keyed by header ID |
| `footers`                | object | Keyed by footer ID |
| `footnotes`              | object | Keyed by footnote ID |
| `documentStyle`          | object | Global document style |
| `namedStyles`            | object | Named paragraph/character styles |
| `lists`                  | object | List definitions |
| `namedRanges`            | object | Named ranges |
| `inlineObjects`         | object | Inline images etc. |
| `positionedObjects`      | object | Positioned objects |

**Body** contains a sequence of **StructuralElement** objects (paragraphs, tables, etc.). For tab-based content, use `Document.tabs[].documentTab.body` and request `includeTabsContent=true` on `documents.get`.

---

## Get Object Primary Keys

Primary keys are defined by the APIs:

| Object / concept      | Primary key | Notes |
|-----------------------|-------------|--------|
| Drive file            | `id`        | Same as spreadsheetId or documentId |
| Spreadsheet           | `spreadsheetId` | Same as Drive file `id` for that spreadsheet |
| Sheet (tab)           | `sheetId`   | Integer in SheetProperties; stable even if title changes |
| Range / values        | Composite  | `spreadsheetId` + `range` (e.g. sheet title + A1 notation) |
| Document              | `documentId`| Same as Drive file `id` |
| Document revision     | `revisionId`| Opaque string; use for “has this doc changed?” (short-lived validity) |

There is no API that returns “primary key columns” for a sheet; the connector should treat (spreadsheetId, sheet title or sheetId, range) as the natural key for value data.

---

## Object's Ingestion Type

| Object / stream      | Recommended type | Rationale |
|----------------------|------------------|-----------|
| **Spreadsheet list** (Drive files, type spreadsheet) | `snapshot` or `append` | Drive has `modifiedTime`; can re-list and only ingest new/changed file IDs. No row-level CDC. |
| **Sheet values** (ranges) | `snapshot` | Sheets API returns current cell state only; no change feed per range. Full re-read per sync. |
| **Document list** (Drive files, type document) | `snapshot` or `append` | Same as spreadsheets; Drive `modifiedTime` can drive “which docs to re-read.” |
| **Document content**  | `snapshot` | Docs API returns current content; `revisionId` can detect change but not incremental content. |

- **`snapshot`**: Full read each time; no native incremental.
- **`append`**: If the connector uses Drive `modifiedTime` (or similar) as a cursor to only re-read files modified after last run, that is an append-like strategy at the “file” level; row-level or cell-level CDC is not provided by the APIs.
- **Deletes**: Trashed files can be excluded with `trashed=false`. Deleted rows/cells in a sheet are not reported as deletes; re-reading the range gives current state. Document deletions are reflected by the file no longer appearing in Drive list (or appearing with `trashed=true`).

---

## Read API for Data Retrieval

### List files (Drive API) — spreadsheets and/or documents

**Endpoint:** `GET https://www.googleapis.com/drive/v3/files`

**Query parameters:**

| Parameter   | Type    | Required | Description |
|-------------|---------|----------|-------------|
| `q`         | string  | Yes      | Search query. Use `mimeType='application/vnd.google-apps.spreadsheet'` or `mimeType='application/vnd.google-apps.document'`, or both with `or`. Use `trashed=false` to exclude trashed. |
| `pageSize`  | integer | No       | Max files per page (partial pages possible). |
| `pageToken` | string  | No       | Token from previous response’s `nextPageToken`. |
| `fields`    | string  | No       | Comma-separated field list, e.g. `nextPageToken,files(id,name,mimeType,modifiedTime,createdTime)`. Include `nextPageToken` for pagination. |
| `orderBy`   | string  | No       | e.g. `modifiedTime desc`, `name`. |
| `corpora`   | string  | No       | `user` (default), `drive`, `allDrives`. |
| `supportsAllDrives` | boolean | No | Set true when querying shared drives. |

**Example (spreadsheets only):**
```http
GET https://www.googleapis.com/drive/v3/files?q=mimeType%3D'application%2Fvnd.google-apps.spreadsheet'&trashed%3Dfalse&pageSize=100&fields=nextPageToken,files(id,name,mimeType,modifiedTime,createdTime)&orderBy=modifiedTime%20desc
Authorization: Bearer {access_token}
```

**Pagination:** Use `nextPageToken` from the response as `pageToken` in the next request. Continue while `nextPageToken` is present.

---

### Get spreadsheet metadata and sheet list (Sheets API)

**Endpoint:** `GET https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}`

**Path parameters:** `spreadsheetId` (string) — from Drive file `id`.

**Query parameters:**

| Parameter   | Type    | Required | Description |
|-------------|---------|----------|-------------|
| `ranges`   | string[]| No       | A1 ranges to include in response when using `includeGridData`. |
| `includeGridData` | boolean | No | If true, includes grid data for requested ranges (heavy). Prefer `values.get`/`values.batchGet` for large data. |
| `fields`   | string  | No       | Field mask to limit response (e.g. `spreadsheetId,properties.title,sheets.properties(sheetId,title,index,gridProperties)`). |

**Example:**
```http
GET https://sheets.googleapis.com/v4/spreadsheets/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms?fields=spreadsheetId,properties.title,sheets.properties(sheetId,title,index,gridProperties)
Authorization: Bearer {access_token}
```

---

### Get sheet values (Sheets API)

**Single range:** `GET https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values/{range}`  

**Multiple ranges:** `GET https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values:batchGet`

Path: `spreadsheetId` (from Drive file `id`). For single-range get, `range` is A1 notation (e.g. `Sheet1!A1:D100` or `A1:D100`).

**Query parameters (both get and batchGet):**

| Parameter             | Type   | Required | Description |
|-----------------------|--------|----------|-------------|
| `ranges[]`           | string | Yes (batchGet) | One or more A1 or R1C1 ranges. |
| `majorDimension`      | string | No       | `ROWS` (default) or `COLUMNS`. |
| `valueRenderOption`  | string | No       | `FORMATTED_VALUE` (default), `UNFORMATTED_VALUE`, or `FORMULA`. |
| `dateTimeRenderOption` | string | No     | How to render dates/times; ignored if valueRenderOption is FORMATTED_VALUE. e.g. `SERIAL_NUMBER` (default) or `FORMATTED_STRING`. |

**Single-range example:**
```http
GET https://sheets.googleapis.com/v4/spreadsheets/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/values/Sheet1!A1:D100?valueRenderOption=UNFORMATTED_VALUE&majorDimension=ROWS
Authorization: Bearer {access_token}
```

**BatchGet example:**
```http
GET https://sheets.googleapis.com/v4/spreadsheets/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/values:batchGet?ranges=Sheet1!A1:D10&ranges=Sheet2!A1:C5&valueRenderOption=UNFORMATTED_VALUE
Authorization: Bearer {access_token}
```

**BatchGet response:**
```json
{
  "spreadsheetId": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
  "valueRanges": [
    { "range": "Sheet1!A1:D10", "majorDimension": "ROWS", "values": [ ["a","b","c"], [1, 2, 3] ] },
    { "range": "Sheet2!A1:C5", "majorDimension": "ROWS", "values": [ ... ] }
  ]
}
```

**Pagination:** The Values API does not support cursor-based pagination. For large sheets, request multiple ranges (e.g. row chunks like `Sheet1!A1:Z1000`, `Sheet1!A1001:Z2000`) or use a single large range; empty trailing rows/columns are omitted from `values`.

---

### Get document (Docs API)

**Endpoint:** `GET https://docs.googleapis.com/v1/documents/{documentId}`

**Path parameters:** `documentId` (string) — from Drive file `id`.

**Query parameters:**

| Parameter             | Type    | Required | Description |
|-----------------------|---------|----------|-------------|
| `suggestionsViewMode` | string  | No       | How suggestions are shown; default `DEFAULT_FOR_CURRENT_ACCESS`. |
| `includeTabsContent`  | boolean | No       | If true, populates `Document.tabs` with full content; if false, first tab content is in `body` and top-level content fields. |

**Example:**
```http
GET https://docs.googleapis.com/v1/documents/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms?includeTabsContent=true
Authorization: Bearer {access_token}
```

Response is a **Document** object (see Object Schema). To get plain text or HTML instead, use Drive **files.export** (see below).

---

### Export document (Drive API) — optional for simple text/HTML

**Endpoint:** `GET https://www.googleapis.com/drive/v3/files/{fileId}/export`

**Query parameters:** `mimeType` (required) — e.g. `text/plain`, `text/html`, `application/pdf`. Export is limited to **10 MB**.

**Example:**
```http
GET https://www.googleapis.com/drive/v3/files/{documentId}/export?mimeType=text/plain
Authorization: Bearer {access_token}
```

Response body is the exported content (bytes), not JSON. Use when the connector needs plain text or HTML rather than the Docs JSON structure.

---

### Rate limits

**Sheets API (v4):**
- 300 requests per minute per project
- 60 requests per minute per user per project  
- Exceeding returns **429 Too Many Requests**. Quota refills each minute. Batch requests (e.g. `values.batchGet`) count as one request.

**Drive API (v3):**
- 12,000 requests per 60 seconds per project (and per user)
- 403 “User rate limit exceeded” or 429 when exceeded
- Sustained write/insert: avoid more than ~3 per second per account

**Docs API (v1):**
- TBD: Not found in quick search; assume similar to other Google APIs. Use exponential backoff on 429/403.

**Best practices:**
- Use `values.batchGet` to reduce read calls when reading multiple ranges.
- Use `fields` to limit response size.
- Implement exponential backoff for 429/403.

---

## Field Type Mapping

| API / response type | Spark SQL / standard type | Notes |
|---------------------|---------------------------|--------|
| string              | StringType                | Most text and IDs |
| number (JSON)       | DoubleType or DecimalType | Sheets values with UNFORMATTED_VALUE |
| boolean (JSON)      | BooleanType               | Sheets boolean cells |
| integer (sheetId etc.) | IntegerType            | Sheet properties |
| RFC 3339 datetime   | TimestampType             | Drive `modifiedTime`, `createdTime` |
| array of arrays     | ArrayType(ArrayType(...)) or flattened rows | Sheets `values`; inner arrays are rows or columns by majorDimension |
| Body / StructuralElement | StructType or string (extracted text) | Docs; connector may flatten to text or preserve structure |
| object (nested)     | StructType                | Nested JSON objects |

**Special behaviors:**
- **Sheets values:** With `FORMATTED_VALUE`, numbers and dates often come as strings (e.g. `"$1.23"`). With `UNFORMATTED_VALUE`, types are preserved (number, boolean). Date/time serial numbers (Sheets epoch) need conversion if using `dateTimeRenderOption=SERIAL_NUMBER`.
- **Docs body:** Content is a tree of StructuralElements (paragraphs, tables, etc.); text is in runs. Connector may expose raw JSON, or a flattened “plain text” field from extraction.

---

## Known Quirks

1. **No native list for Sheets/Docs:** Listing is via Drive `files.list` with mimeType filter. Sheet tabs are only available from `spreadsheets.get`, not a separate list endpoint.
2. **SpreadsheetId = Drive file id:** The same ID is used in Drive and in the Sheets API.
3. **DocumentId = Drive file id:** Same for Docs API.
4. **Service account access:** To read user-owned Sheets/Docs, the file must be shared with the service account email (e.g. in the Google Sheet “Share” dialog).
5. **Docs revisionId:** Opaque and only guaranteed valid for ~24 hours; use for “has doc changed?” within a sync window, not as a long-term cursor.
6. **Sheets values pagination:** No pageToken; request multiple ranges or one large range; empty trailing rows/columns are omitted.
7. **Drive list default fields:** Default response includes only a few fields; always request `fields=nextPageToken,files(...)` with the fields you need.
8. **Export size:** Drive `files.export` is limited to 10 MB per export.

---

## Sources and References

| Source type       | URL | Confidence | What it confirmed |
|-------------------|-----|------------|--------------------|
| Official (Sheets) | https://developers.google.com/sheets/api/reference/rest | High | Base URL, spreadsheets.get, values |
| Official (Sheets) | https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get | High | spreadsheets.get params, Spreadsheet response |
| Official (Sheets) | https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchGet | High | values.batchGet params, ValueRange response |
| Official (Sheets) | https://developers.google.com/workspace/sheets/api/limits | High | Rate limits 300/min project, 60/min user |
| Official (Sheets) | https://developers.google.com/workspace/sheets/api/reference/rest/v4/ValueRenderOption | High | valueRenderOption enum |
| Official (Docs)   | https://developers.google.com/workspace/docs/api/reference/rest/v1/documents/get | High | documents.get, Document response |
| Official (Docs)   | https://developers.google.com/workspace/docs/api/reference/rest/v1/documents#Document | High | Document schema, body, tabs |
| Official (Drive)  | https://developers.google.com/workspace/drive/api/reference/rest/v3/files/list | High | files.list params, pagination, q, fields |
| Official (Drive)  | https://developers.google.com/drive/api/guides/about-files | High | MIME types for spreadsheet, document |
| Official (Drive)  | https://developers.google.com/drive/api/guides/limits | High | Drive rate limits 12000/60s |
| Official (Drive)  | https://developers.google.com/workspace/drive/api/reference/rest/v3/files/export | High | files.export, 10 MB limit |
| Official (OAuth)  | https://developers.google.com/identity/protocols/oauth2/web-server | High | Refresh token exchange |
| Airbyte           | https://docs.airbyte.com/integrations/sources/google-sheets (and connector) | Medium | OAuth, discovery pattern |

---

## Research Log

| Source type   | URL | Accessed (UTC) | Confidence | What it confirmed |
|---------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get | 2026-03-04 | High | spreadsheets.get, query params, Spreadsheet response |
| Official Docs | https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchGet | 2026-03-04 | High | values.batchGet, ValueRange, params |
| Official Docs | https://developers.google.com/workspace/docs/api/reference/rest/v1/documents/get | 2026-03-04 | High | documents.get, documentId, scopes |
| Official Docs | https://developers.google.com/workspace/docs/api/reference/rest/v1/documents#Document | 2026-03-04 | High | Document schema, body, tabs, revisionId |
| Official Docs | https://developers.google.com/workspace/drive/api/reference/rest/v3/files/list | 2026-03-04 | High | files.list, pageSize, pageToken, q, fields, nextPageToken |
| Official Docs | https://developers.google.com/workspace/sheets/api/limits | 2026-03-04 | High | Sheets 300/min, 60/min per user, 429 |
| Official Docs | https://developers.google.com/drive/api/guides/limits | 2026-03-04 | High | Drive 12000/60s |
| Official Docs | https://developers.google.com/workspace/drive/api/reference/rest/v3/files/export | 2026-03-04 | High | files.export, mimeType, 10 MB |
| Web search     | ValueRange, ValueRenderOption, Sheet schema, Drive File fields | 2026-03-04 | Medium | ValueRange fields, FORMATTED_VALUE/UNFORMATTED_VALUE, SheetProperties, File id/name/mimeType/modifiedTime |
