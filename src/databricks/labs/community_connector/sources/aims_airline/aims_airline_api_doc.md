# **AIMS Airline API Documentation**

AIMS (Airline Information Management System) is a crew scheduling and management system used by airlines such as easyJet, developed by Histon (now part of Jeppesen/Boeing). **There is no public REST API.** Data is accessed by web scraping the AIMS Crew Portal HTML pages. This document describes the authentication flow, data endpoints, data model, and implementation recommendations for building a Lakeflow Community Connector.

---

## **Authorization**

- **Chosen method**: Username/password authentication against the AIMS web server.
- **Base URL**: Airline-specific AIMS server URL (e.g. `https://ezy-crew.aims.aero` for easyJet).
- **Auth placement**:
  - Credentials are POSTed to the server's `wtouch.exe/verify` endpoint.
  - Username is Base64-encoded; password is MD5-hashed before transmission.
  - A session-based cookie is returned and used for all subsequent requests.

### Connection Parameters (dev_config.json)

| Parameter    | Type   | Required | Default | Description |
|-------------|--------|----------|---------|-------------|
| `server_url` | string | Yes | - | URL of the AIMS server (e.g. `https://ezy-crew.aims.aero`). Airline-specific. |
| `username`   | string | Yes | - | AIMS username / employee ID (e.g. `001234` for easyJet). |
| `password`   | string | Yes | - | AIMS password. Expunged after use; never stored. |

**Example dev_config.json**:
```json
{
  "server_url": "https://ezy-crew.aims.aero",
  "username": "001234",
  "password": "YOUR_PASSWORD_HERE"
}
```

### Authentication Flow

Two authentication approaches exist in the community tools; the connector should use the **direct AIMS login** (simpler, from `aimslib`):

#### Direct AIMS Login (Recommended)

1. **POST** `{server_url}` — Populates session cookies.
2. **POST** `{server_url}/wtouch/wtouch.exe/verify` — Sends encoded credentials:
   - Body: `Crew_Id={base64_encoded_username}`, `Crm={md5_hashed_password}`
   - On success: response URL contains the AIMS base URL. Extract by splitting on `wtouch.exe` and taking the prefix.
   - On failure: response contains `"Please re-enter your Credentials"` or `"Please swipe your card"`.
3. **Session** — After successful login, session cookies and the base URL are sufficient to access all AIMS pages. No token exchange; maintain a `requests.Session()`.

#### SSO Login (Alternative — easyJet-specific)

Used by the `aims_extract` tool; involves a SAML handshake through `connected.easyjet.com`. More complex and airline-specific.

1. **GET** `https://connected.easyjet.com` — Populates SSO cookies. If `g-recaptcha` appears, CAPTCHA is active.
2. **POST** `https://connected.easyjet.com/my.policy` — Sends `username`, `password`, `vhost=standard`.
3. **SAML handshake** — Extract `SAMLRequest` from response form → POST to IdP → Extract `SAMLResponse` → POST to SP.
4. **Auto-login** — GET the redirect URL (`cms/redirect/ecrew`). Extract AIMS base URL from response URL.

### Example (conceptual)

```python
import requests, base64, hashlib

session = requests.Session()
session.post(server_url)  # get cookies
encoded_id = base64.b64encode(username.encode()).decode()
encoded_pw = hashlib.md5(password.encode()).hexdigest()
r = session.post(f"{server_url}/wtouch/wtouch.exe/verify",
                 {"Crew_Id": encoded_id, "Crm": encoded_pw})
base_url = r.url.split("wtouch.exe")[0]  # e.g. https://ezy-crew.aims.aero/.../
```

### Notes

- Password must not be stored; expunge after use.
- CAPTCHA or MFA may block automated access; no official support for headless flows.
- If already logged in, AIMS returns `"Please log out and try again"` — logout via `perinfo.exe/AjAction?LOGOUT=1` then retry.
- Request timeout: 60 seconds recommended.

---

## **Object List**

The object list is **static**. There is no API to enumerate objects. The connector exposes these four logical tables, derived from HTML parsing of the AIMS Crew Portal:

| Object Name     | Description | Primary Endpoint | Ingestion Type |
|----------------|-------------|------------------|----------------|
| `duties`        | Duty periods (work shifts) containing flight sectors, standbys, and ground duties | `POST perinfo.exe/schedule` → `GET perinfo.exe/schedule` (trip details) | `snapshot` |
| `sectors`       | Individual flight segments and quasi-sectors (standby, positioning) with times, airports, aircraft | `GET perinfo.exe/schedule` (trip details) | `snapshot` |
| `crew_members`  | Crew assignments per sector (name, role) | `GET perinfo.exe/getlegmem` | `snapshot` |
| `all_day_events`| Non-flying events: days off, leave, rest, sick, etc. | `POST perinfo.exe/schedule` (brief roster) | `snapshot` |

**Total Supported Tables**: 4 (all snapshot)

### Object Hierarchy

- **Duties** contain **Sectors** (one duty has one or more sectors).
- **Sectors** have **CrewMembers** (crew on that sector, accessed via separate crew list request).
- **AllDayEvents** are standalone (extracted from the brief roster, no sectors).

### Data Retrieval Flow

```
Brief Roster (POST schedule)
  ├── All-Day Events (day-off codes: D/O, REST, SICK, etc.)
  ├── Standby/Ground Duties (text + start/end times)
  └── Trip IDs (e.g. B089, K1234)
        │
        ▼
Trip Details (GET schedule?FltInf=1&ORGDAY=...&CROUTE=...)
  └── Sectors (flight number, airports, times, aircraft reg)
        │
        ▼
Crew List (GET getlegmem?LegInfo=...)
  └── Crew Members (name, role)
```

---

## **Object Schema**

### General notes

- There is no schema API. All schemas are static, derived from the AIMS HTML structure and the `aimslib`/`aims_extract` data models.
- All times are **UTC**.
- The `aims_day` field is an AIMS internal date: the number of elapsed days since 1 January 1980.

### `duties` object

**Source**: Brief Roster + Trip Details pages

| Column Name    | Type      | Nullable | Description |
|---------------|-----------|----------|-------------|
| `aims_day`     | string    | No       | Days since 1980-01-01 (AIMS internal date). Combined with `trip_id` to form the duty identifier. |
| `trip_id`      | string    | Yes      | Trip identifier from the brief roster (e.g. `B089`, `K1234`). Null for standalone standby/ground duties. |
| `start`        | timestamp | No       | Duty start / report time (UTC). |
| `finish`       | timestamp | Yes      | Duty end time (UTC). Null for all-day events. |
| `sector_count` | integer   | No       | Number of sectors in the duty. 0 for all-day events or duties without trip details. |
| `date`         | date      | No       | Calendar date of the duty, derived from `aims_day`. |

### `sectors` object

**Source**: Trip Details page

| Column Name   | Type      | Nullable | Description |
|--------------|-----------|----------|-------------|
| `sector_id`   | string    | Yes      | AIMS sector identifier used to look up crew lists (e.g. `14262,138549409849,14262,401,brs,1, ,gla,320`). Null for quasi-sectors. |
| `aims_day`    | string    | No       | Days since 1980-01-01 for the trip start date. |
| `trip_id`     | string    | No       | Trip identifier (e.g. `B089`). |
| `flight_num`  | string    | No       | Flight number (e.g. `EZY6021`) or quasi-sector type (e.g. `[esby]`, `STBY`, `ADTY`). Square brackets indicate a quasi-sector recorded within a trip. |
| `from_`       | string    | Yes      | Origin airport (3-letter IATA code, e.g. `BRS`). Null for quasi-sectors. |
| `to`          | string    | Yes      | Destination airport (3-letter IATA code, e.g. `GLA`). Null for quasi-sectors. |
| `sched_off`   | timestamp | No       | Scheduled off-blocks / departure time (UTC). |
| `sched_on`    | timestamp | No       | Scheduled on-blocks / arrival time (UTC). |
| `act_off`     | timestamp | Yes      | Actual off-blocks time (UTC). Null for future flights, quasi-sectors, and ground positioning. |
| `act_on`      | timestamp | Yes      | Actual on-blocks time (UTC). Null for future flights, quasi-sectors, and ground positioning. |
| `reg`         | string    | Yes      | Aircraft registration (e.g. `G-EZAA`). Format: `XX-XXX` or `X-XXXX`. Null for quasi-sectors and ground positioning. |
| `type_`       | string    | Yes      | Aircraft type (e.g. `A320`). Available from Crew Schedule HTML but not always from trip details. |
| `is_quasi`    | boolean   | No       | True if non-aircraft duty (standby, SEP training, etc.). Identified by flight number in `[square brackets]` or missing sector_id. |
| `is_positioning` | boolean | No    | True if positioning (ground or air). |
| `is_pax`      | boolean   | No       | True if deadheading as passenger. |
| `duty_start`  | timestamp | Yes      | Parent duty start time (UTC). For grouping sectors into duties. |
| `duty_finish`  | timestamp | Yes     | Parent duty finish time (UTC). |
| `date`        | date      | No       | Calendar date of the sector, derived from aims_day + trip day offset. |

### `crew_members` object

**Source**: Crew List page

| Column Name   | Type   | Nullable | Description |
|--------------|--------|----------|-------------|
| `name`        | string | No       | Crew member's name. |
| `role`        | string | No       | Crew role. Common values: `CP` (Captain), `FO` (First Officer), `PU` (Purser), `FA` (Flight Attendant). |
| `sector_id`   | string | No       | AIMS sector identifier (links to `sectors.sector_id`). |
| `aims_day`    | string | No       | Days since 1980-01-01 for the trip. |
| `trip_id`     | string | No       | Trip identifier. |

### `all_day_events` object

**Source**: Brief Roster page

| Column Name | Type   | Nullable | Description |
|------------|--------|----------|-------------|
| `date`      | date   | No       | Date of the event. |
| `code`      | string | No       | AIMS event code. Known values: `D/O` (Day Off), `D/OR` (Day Off Requested), `WD/O` (Weekend Day Off), `LVE` (Leave), `P/T` (Part Time), `REST`, `FTGD` (Fatigued), `SICK`, `SIDO` (Sick Day Off), `SILN` (Sick Leave Night), `EXPD`, `GDO`. |
| `aims_day`  | string | No       | Days since 1980-01-01. |

---

## **Get Object Primary Keys**

There is no API for primary keys. Use these static definitions:

| Object          | Primary Key Columns | Notes |
|-----------------|---------------------|-------|
| `duties`        | `aims_day`, `trip_id`, `start` | `trip_id` may be null for standalone duties; use `aims_day` + `start` as fallback composite. |
| `sectors`       | `aims_day`, `trip_id`, `sector_id`, `sched_off` | `sector_id` may be null for quasi-sectors; use `sched_off` as tie-breaker. |
| `crew_members`  | `sector_id`, `name`, `role` | Composite of sector + crew member identity. |
| `all_day_events`| `aims_day`, `code` | One event code per day. |

---

## **Object's Ingestion Type**

| Object          | Ingestion Type | Cursor Field | Rationale |
|-----------------|----------------|-------------|-----------|
| `duties`        | `snapshot`     | None        | Roster data is published in discrete periods; no change feed or cursor. Each sync re-scrapes the selected period. |
| `sectors`       | `snapshot`     | None        | Same as duties — derived from trip details within a roster period. |
| `crew_members`  | `snapshot`     | None        | Crew lists are point-in-time; planned crew changes frequently before a duty. |
| `all_day_events`| `snapshot`     | None        | Day-off codes come from the brief roster with no change tracking. |

All tables use **snapshot** ingestion. AIMS provides no incremental change feed, cursor field, or CDC mechanism. Each connector run re-reads the current roster period(s) and replaces downstream table contents.

---

## **Read API for Data Retrieval**

Data is retrieved by **web scraping** HTML pages from the AIMS Crew Portal. All endpoints below are relative to the AIMS base URL (e.g. `https://ezy-crew.aims.aero/.../`).

### Endpoint 0: Index Page (Roster Change Check)

- **URL**: `{base_url}perinfo.exe/index`
- **Method**: GET
- **Returns**: HTML index page.
- **Use**: Check for roster changes ("red writing"). If `var notification = Trim("");` is **not** found in the response, changes exist and the user must acknowledge them through official channels before data can be accessed.

```python
r = session.get(f"{base_url}perinfo.exe/index")
no_changes_marker = '\r\nvar notification = Trim("");\r\n'
has_changes = r.text.find(no_changes_marker) == -1
```

### Endpoint 1: Brief Roster

- **URL**: `{base_url}perinfo.exe/schedule`
- **Method**: POST
- **Body (current roster)**: `{}` (empty form data)
- **Body (navigate to adjacent roster)**:
  - `Direc=2` (next period) or `Direc=1` (previous period)
  - `_flagy=2`
- **Returns**: HTML with a table-based brief roster.

**Parsing logic**:
- Find `<div id="main_div">` → all `<table class="duties_table">` elements.
- Each table's parent has `id="myday_{aims_day}"`.
- Table contents are stripped strings: trip IDs, day-off codes, and standby times.

**Roster entry categories**:
1. **Day-off codes** (filter out): `D/O`, `D/OR`, `WD/O`, `P/T`, `LVE`, `FTGD`, `REST`, `SICK`, `SIDO`, `SILN`, `EXPD`, `GDO`, `==>`
2. **Standby/ground duties**: Three consecutive strings — text label, start time (`H:MM` or `HH:MM`), end time
3. **Trip identifiers**: Mix of letters and numbers (e.g. `B089`, `K1234s`)

**Pagination**: There is no random access. Stepping to a distant roster requires one POST per period step. To access the roster 3 periods in the past, 3 sequential requests are needed.

**Example**:
```python
# Get current roster
r = session.post(f"{base_url}perinfo.exe/schedule", data={})

# Navigate to next roster period
r = session.post(f"{base_url}perinfo.exe/schedule",
                 data={"Direc": "2", "_flagy": "2"})
```

### Endpoint 2: Trip Details

- **URL**: `{base_url}perinfo.exe/schedule`
- **Method**: GET
- **Query params**:
  - `FltInf=1`
  - `ORGDAY={aims_day}` — Days since 1980-01-01
  - `CROUTE={trip_id}` — Trip identifier from brief roster
- **Returns**: HTML trip sheet with sector rows.

**Parsing logic**:
- Find first `<tr class="mono_rows_ctrl_f3">` element.
- Each such row is a sector. Extract the `id` attribute (sector_id) and split text content.
- Sector fields (positional from text):
  - `[0]`: Sector identifier (from `id` attr; may be None for quasi-sectors)
  - `[1]`: Flight number
  - `[2]`: Departure airport (3-letter code)
  - `[3]`: Arrival airport (3-letter code)
  - `[4]`: Scheduled departure (HHMM UTC, may have `+N` day offset suffix)
  - `[5]`: Scheduled arrival (HHMM UTC, may have `+N` day offset suffix)
  - Optional fields in remaining positions:
    - Actual departure: `A` + 4 digits (e.g. `A0612`)
    - Actual arrival: `A` + 4 digits (e.g. `A0735`)
    - Aircraft registration: pattern `XX-XXX` or `X-XXXX` (e.g. `G-EZAA`)
    - `PAX`: indicates positioning/deadheading
    - Trip day number: single digit
    - Various `HH:MM` formatted times (duty start/end)
    - Date string (e.g. `Fri18Jan`)

**Duty boundaries**: Consecutive sector rows belong to the same duty. Non-`mono_rows_ctrl_f3` rows signal a duty boundary.

**Example**:
```python
r = session.get(f"{base_url}perinfo.exe/schedule",
                params={"FltInf": "1", "ORGDAY": "16436", "CROUTE": "B089"})
```

### Endpoint 3: Crew List

- **URL**: `{base_url}perinfo.exe/getlegmem`
- **Method**: GET
- **Query params**:
  - `LegInfo={sector_id}` — The sector identifier from the trip details page
- **Returns**: HTML crew sheet with crew members (name, role).

**Notes**:
- `sector_id` format example: `14262,138549409849,14262,401,brs,1, ,gla,320`
- Crew lists are only meaningful for **past** sectors with actual times. For future sectors, crew is in flux and the list may not be useful.
- This endpoint is **slow** relative to other AIMS pages. Each sector requires a separate request.

**Example**:
```python
r = session.get(f"{base_url}perinfo.exe/getlegmem",
                params={"LegInfo": "14262,138549409849,14262,401,brs,1, ,gla,320"})
```

### Endpoint 4: Logout

- **URL**: `{base_url}perinfo.exe/AjAction?LOGOUT=1`
- **Method**: POST
- **Body**: `AjaxOperation=0`
- **Use**: Clean session termination. Call after data retrieval is complete.

---

## **Table Configuration Parameters**

These parameters control how much roster data is retrieved per sync.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `roster_periods` | integer | No | `0` | Number of additional roster periods to retrieve. `0` = current only. Positive = forward, negative = backward. E.g. `-2` retrieves current + 2 previous periods. |
| `include_crew` | boolean | No | `false` | Whether to fetch crew lists for each sector. Significantly increases request count and sync time. |
| `include_future_crew` | boolean | No | `false` | If `include_crew` is true, whether to also fetch crew for future (unflown) sectors. |

---

## **Field Type Mapping**

| AIMS / Source Type | Spark Type | Example | Notes |
|-------------------|------------|---------|-------|
| Flight number, trip ID, event codes | `StringType` | `EZY6021`, `B089`, `D/O` | Free-form text identifiers |
| Airport codes | `StringType` | `BRS`, `GLA`, `AMS` | 3-letter IATA codes |
| Aircraft registration | `StringType` | `G-EZAA`, `OE-LKF` | Format: `XX-XXX` or `X-XXXX` |
| Aircraft type | `StringType` | `A320`, `A319` | Aircraft type designator |
| Crew role | `StringType` | `CP`, `FO`, `PU`, `FA` | Crew role abbreviation |
| Crew name | `StringType` | `Smith J` | Name as displayed in AIMS |
| aims_day | `StringType` | `16436` | Days since 1980-01-01. Convert: `datetime(1980,1,1) + timedelta(int(aims_day))` |
| Scheduled/actual times (HHMM) | `TimestampType` | `2024-02-11T06:00:00Z` | Combine aims_day date + HHMM time. All UTC. |
| Duty start/finish | `TimestampType` | `2024-02-11T06:00:00Z` | UTC datetime |
| Event date | `DateType` | `2024-02-27` | Calendar date |
| Boolean flags (quasi, positioning, pax) | `BooleanType` | `true`, `false` | Derived from parsing: quasi = no sector_id or `[brackets]`; positioning = `PAX` keyword; ground = no registration |

### Special Behaviors

- **aims_day conversion**: `date = datetime(1980, 1, 1) + timedelta(days=int(aims_day))`. E.g. aims_day `16436` = `2024-12-31`.
- **Time `+N` suffix**: Scheduled times may have a day offset suffix (e.g. `0130+1` means 01:30 on the following day).
- **AIMS bug**: End-of-duty time may be `24:00` (non-existent); treat as `00:00` of the next day.
- **Actual times**: Format `A` + 4 digits (e.g. `A0612` = 06:12 UTC). No day offset — use proximity to scheduled time to determine the correct date.
- **Quasi-sector naming**: Flight numbers wrapped in `[square brackets]` (e.g. `[esby]`) indicate standby or ground duties recorded as part of a trip.

---

## **Rate Limiting**

AIMS is not designed for bulk automated access. There are no documented rate limits, but the following best practices apply:

| Concern | Recommendation |
|---------|---------------|
| Request frequency | Add 1-2 second delays between requests to avoid server-side throttling. |
| Request timeout | 60 seconds per request (as used by both community tools). |
| Roster navigation | Each period step requires a separate POST. Minimize `roster_periods` to avoid excessive requests. |
| Crew list fetching | One request per sector. A duty with 4 sectors = 4 crew list requests. Consider limiting to past-only or disabling by default. |
| Session lifetime | Login once per sync run. Logout cleanly after completion. |
| Error handling | Handle `requests.HTTPError`, `requests.Timeout`, and `requests.ConnectionError` with retries and exponential backoff. |

### Estimated Request Counts

| Scenario | Requests |
|----------|----------|
| Current roster, no crew | ~1 (brief roster) + N (trip details, one per trip) |
| Current roster, with crew | ~1 + N trips + M sectors (crew lists) |
| 3 roster periods, no crew | ~3 + 3×N trips |
| 3 roster periods, with crew | ~3 + 3×N trips + 3×M sectors |

A typical monthly roster with 15-20 duty days, 2-4 sectors per duty ≈ 20-30 trips + 40-80 sectors.

---

## **Known Quirks & Edge Cases**

- **No REST API**: All data is obtained by scraping HTML pages. HTML structure is subject to change without notice. The connector is inherently brittle.
- **CAPTCHA / MFA**: AIMS may deploy CAPTCHA or multi-factor authentication at any time, which would break automated access.
- **Red writing / pending changes**: If roster changes are pending, AIMS may block access until the user acknowledges changes through official channels. The connector should check the index page first.
- **Session conflicts**: If already logged in elsewhere, AIMS returns `"Please log out and try again"`. The connector must handle this by logging out and retrying.
- **aims_day date system**: AIMS uses an internal date format (days since 1980-01-01). All date handling must account for this conversion.
- **24:00 time bug**: AIMS occasionally records `24:00` as an end-of-duty time. This must be treated as `00:00` of the next day.
- **Quasi-sectors**: Standbys and ground duties within trips are recorded as pseudo-sectors with flight numbers in `[square brackets]`. These have no actual times, no registration, and no crew list.
- **Ground positioning**: Positioning duties with no aircraft registration are identified by the `PAX` flag with a null `reg`. AIMS does not update these with actual times.
- **Crew list availability**: Crew data is only reliably available for **past** sectors. Future crew assignments are in flux. The `crewlist_id` field may be null for quasi-sectors.
- **Trip caching**: In practice, trip details don't change after a duty is completed. Community tools cache trip details to minimize requests. The connector should consider caching strategies.
- **Airline-specific**: AIMS is deployed differently per airline. The easyJet implementation (`ezy-crew.aims.aero`) is the reference. Other airlines using AIMS may have different URLs, SSO flows, and HTML structures.
- **Roster period size**: A roster period is typically one month but may vary by airline. Data can only be downloaded in chunks of one roster period at a time.
- **All-day event codes**: The known set (`D/O`, `D/OR`, `WD/O`, `LVE`, `P/T`, `REST`, `FTGD`, `SICK`, `SIDO`, `SILN`, `EXPD`, `GDO`) may be incomplete. Unknown codes should be preserved rather than filtered.

---

## **Connector Implementation Recommendations**

### Recommended Table Priority

| Priority | Table           | Rationale |
|----------|----------------|-----------|
| 1        | `sectors`       | Core flight data — flight numbers, airports, times, aircraft. Primary analytics use case. |
| 2        | `duties`        | Duty-level view — needed for roster analysis and flight time compliance. |
| 3        | `all_day_events`| Days off, leave, sick — complements roster for crew availability analysis. |
| 4        | `crew_members`  | Crew assignments — useful but expensive to fetch (1 request per sector). |

### Implementation Approach

1. **Authenticate** → connect to AIMS server, get session + base URL.
2. **Check for changes** → GET index page, verify no pending notifications.
3. **Retrieve brief roster** → POST to schedule endpoint; optionally navigate to adjacent periods.
4. **Parse roster entries** → extract trip IDs, day-off codes, standby duties.
5. **For each trip** → GET trip details, parse sector data.
6. **(Optional) For each sector** → GET crew list, parse crew members.
7. **Build records** → convert parsed data to connector record format.
8. **Logout** → POST to logout endpoint.

### Alternative: HTML File Ingestion

For simpler deployments or when SAML auth is not feasible, the connector could support ingesting manually exported HTML files:

| Source           | Pros | Cons |
|------------------|------|------|
| Crew Schedule HTML | Accurate duty times, full crew, non-flying duties | Max 31 days per export; no aircraft registration |
| Pilot Logbook HTML | Aircraft registration; up to 2 years per export | Approximate times (standard report/debrief); no all-day events |
| Web scraping     | Automated; no manual export | Auth complexity; rate limits; CAPTCHA risk |

---

## **Research Log**

| Source Type | URL | Confidence | What it confirmed |
|------------|-----|------------|-------------------|
| OSS Tool (aims_extract) | https://github.com/aviationio/aims_extract | High | SSO authentication flow, endpoint structure (schedule, getlegmem, index), HTML parsing patterns, data types |
| OSS Library (aimslib) | https://github.com/JonHurst/aimslib | High | Direct AIMS login (wtouch.exe/verify with base64 username + MD5 password), PostFunc pattern, TripID/Sector/Duty types, SectorFlags enum, crew caching strategy |
| Documentation (aims-convert) | https://hursts.org.uk/aimsdocs/ | High | Canonical data structures (Sector, Duty, CrewMember, AllDayEvent), parse functions, HTML export formats, output formats |
| Data Structures | https://hursts.org.uk/aimsdocs/data_structures.html | High | NamedTuple field definitions for Sector, Duty, CrewMember, AllDayEvent |
| Processing Functions | https://hursts.org.uk/aimsdocs/functions.html | High | `parse()`, `roster.duties()`, `logbook_report.duties()` function signatures |
| Web App Docs | https://hursts.org.uk/aimsdocs/webapp.html | Medium | HTML export workflow, Crew Schedule vs Pilot Logbook trade-offs, 31-day/2-year limits |
| CLI Docs | https://hursts.org.uk/aimsdocs/command_line.html | Medium | Output formats (eFJ, iCalendar, CSV, roster), usage examples |

## **Sources and References**

- **aimslib** (highest confidence for authentication and data model)
  - https://github.com/JonHurst/aimslib — Refactored library with clean types, direct AIMS login, caching
- **aims_extract** (high confidence for SSO flow and parsing)
  - https://github.com/aviationio/aims_extract — CLI tool with easyJet SSO auth, HTML parsing
- **aims-convert documentation** (high confidence for data structures)
  - https://hursts.org.uk/aimsdocs/ — Data structures, functions, output formats

When conflicts arise between the three sources, **aimslib** is preferred (most recent, cleanest API). **aims-convert** is used for canonical data structure definitions. **aims_extract** is used for SSO flow details.

**No official API documentation exists.** All information is from reverse engineering by the aviation open-source community.
