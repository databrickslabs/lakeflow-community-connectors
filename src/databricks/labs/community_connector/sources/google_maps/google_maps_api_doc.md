# **Google Maps Platform API Documentation**

## **Authorization**

- **Chosen method**: API Key for all Google Maps Platform web service APIs.
- **Base URLs** (vary by API product):
  - Places API (New): `https://places.googleapis.com/v1`
  - Routes API: `https://routes.googleapis.com`
  - Roads API: `https://roads.googleapis.com/v1`
  - Geocoding API: `https://maps.googleapis.com/maps/api/geocode`
  - Elevation API: `https://maps.googleapis.com/maps/api/elevation`
  - Time Zone API: `https://maps.googleapis.com/maps/api/timezone`
  - Air Quality API: `https://airquality.googleapis.com/v1`
  - Address Validation API: `https://addressvalidation.googleapis.com/v1`
- **Auth placement**:
  - For Places API (New) and Routes API: HTTP header `X-Goog-Api-Key: <API_KEY>`
  - For legacy-style APIs (Geocoding, Elevation, Time Zone, Roads): Query parameter `key=<API_KEY>`
  - For Air Quality and Address Validation APIs: HTTP header `X-Goog-Api-Key: <API_KEY>` or query parameter `key=<API_KEY>`
- **OAuth alternative**: Google Maps Platform also supports OAuth 2.0 with service accounts for server-side applications. The connector stores the API key and passes it with every request; it does not run user-facing OAuth flows. If OAuth is used instead, the connector would store service account JSON credentials and exchange them for short-lived access tokens at runtime, passing `Authorization: Bearer <access_token>` in the header.
- **Required setup**:
  1. Create a Google Cloud project
  2. Enable the specific Google Maps Platform APIs needed (Places API New, Routes API, etc.)
  3. Create an API key in the Google Cloud Console
  4. Restrict the API key to only the required APIs

Example authenticated request (Places API New):

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-Goog-Api-Key: YOUR_API_KEY" \
  -H "X-Goog-FieldMask: places.id,places.displayName,places.formattedAddress,places.location,places.types,places.rating" \
  -d '{
    "textQuery": "restaurants in Mountain View, CA",
    "pageSize": 10
  }' \
  "https://places.googleapis.com/v1/places:searchText"
```

Example authenticated request (Geocoding API):

```bash
curl "https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key=YOUR_API_KEY"
```

Notes:
- All requests must use HTTPS.
- API keys are project-scoped; rate limits apply per project.
- Rate limits are enforced per minute (QPM) and vary by API method. There are no maximum daily request limits, only QPM limits and billing-based cost controls.
- Google provides a $200 monthly credit that automatically applies to eligible Google Maps Platform SKUs.
- API key restrictions (by API, by IP address, or by HTTP referrer) are strongly recommended for security.


## **Object List**

The Google Maps Platform encompasses multiple distinct APIs, each serving different data retrieval needs. For connector purposes, we treat specific API endpoints as objects/tables. The object list is **static** (defined by the connector), not discovered dynamically from an API.

### Places API (New) Objects

| Object Name | Description | Primary Endpoint | HTTP Method | Ingestion Type |
|-------------|-------------|------------------|-------------|----------------|
| `places_text_search` | Places matching a text query (e.g., "pizza in NYC") | `POST /v1/places:searchText` | POST | `snapshot` |
| `places_nearby_search` | Places near a geographic point within a radius | `POST /v1/places:searchNearby` | POST | `snapshot` |
| `place_details` | Detailed information about a specific place by Place ID | `GET /v1/places/{placeId}` | GET | `snapshot` |

### Routes API Objects

| Object Name | Description | Primary Endpoint | HTTP Method | Ingestion Type |
|-------------|-------------|------------------|-------------|----------------|
| `routes_compute` | Computed route between origin and destination | `POST /directions/v2:computeRoutes` | POST | `snapshot` |
| `routes_matrix` | Distance/duration matrix for origin-destination pairs | `POST /distanceMatrix/v2:computeRouteMatrix` | POST | `snapshot` |

### Roads API Objects

| Object Name | Description | Primary Endpoint | HTTP Method | Ingestion Type |
|-------------|-------------|------------------|-------------|----------------|
| `roads_snap` | GPS points snapped to nearest road segments | `GET /v1/snapToRoads` | GET | `snapshot` |
| `roads_nearest` | Nearest road segment for each GPS point | `GET /v1/nearestRoads` | GET | `snapshot` |
| `roads_speed_limits` | Posted speed limits for road segments | `GET /v1/speedLimits` | GET | `snapshot` |

### Geocoding API Objects

| Object Name | Description | Primary Endpoint | HTTP Method | Ingestion Type |
|-------------|-------------|------------------|-------------|----------------|
| `geocode` | Convert addresses to coordinates | `GET /json` | GET | `snapshot` |
| `reverse_geocode` | Convert coordinates to addresses | `GET /json` | GET | `snapshot` |

### Utility API Objects

| Object Name | Description | Primary Endpoint | HTTP Method | Ingestion Type |
|-------------|-------------|------------------|-------------|----------------|
| `elevation` | Elevation data for geographic coordinates | `GET /json` | GET | `snapshot` |
| `timezone` | Time zone information for a location and timestamp | `GET /json` | GET | `snapshot` |

### Environment API Objects

| Object Name | Description | Primary Endpoint | HTTP Method | Ingestion Type |
|-------------|-------------|------------------|-------------|----------------|
| `air_quality_current` | Current hourly air quality conditions for a location | `POST /v1/currentConditions:lookup` | POST | `snapshot` |
| `air_quality_history` | Historical hourly air quality (up to 30 days) | `POST /v1/history:lookup` | POST | `append` |

### Address Validation API Objects

| Object Name | Description | Primary Endpoint | HTTP Method | Ingestion Type |
|-------------|-------------|------------------|-------------|----------------|
| `address_validation` | Validated and standardized address with component-level results | `POST /v1:validateAddress` | POST | `snapshot` |

**Connector scope for initial implementation**:
- Phase 1 focuses on the **Places API (New)** objects (`places_text_search`, `places_nearby_search`, `place_details`) and the **Geocoding API** (`geocode`, `reverse_geocode`) as these are the most commonly used for data extraction.
- Phase 2 would add Environment APIs (`air_quality_current`, `air_quality_history`) and utility APIs (`elevation`, `timezone`).
- Phase 3 would add Routes and Roads APIs for specialized use cases.

**Important design note**: Unlike traditional CRUD APIs (Zendesk, GitHub, etc.), Google Maps Platform APIs are **query-based** rather than **resource-listing** APIs. There is no "list all places" endpoint. Each request requires input parameters (a search query, coordinates, place IDs, addresses, etc.). The connector must accept these parameters as `table_options` to drive data retrieval.


## **Object Schema**

### General Notes

- Google Maps Platform APIs return JSON responses.
- The Places API (New) uses **field masks** (`X-Goog-FieldMask` header) to control which fields are returned. If no field mask is specified, the API returns an error.
- Field masks also determine billing tier (Essentials, Pro, Enterprise, Enterprise + Atmosphere).
- Nested JSON objects are modeled as nested structures, not flattened.
- All schemas below are defined statically based on the API documentation.

---

### `places_text_search` / `places_nearby_search` Object

**Source endpoints**:
- Text Search: `POST https://places.googleapis.com/v1/places:searchText`
- Nearby Search: `POST https://places.googleapis.com/v1/places:searchNearby`

Both endpoints return an array of Place objects. The schema is identical.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique place identifier (Place ID) |
| `name` | string | Resource name in `places/{placeId}` format |
| `displayName` | struct | Localized place name (`text`, `languageCode`) |
| `formattedAddress` | string | Complete human-readable address |
| `shortFormattedAddress` | string | Abbreviated address |
| `location` | struct | Geographic coordinates (`latitude`, `longitude`) |
| `viewport` | struct | Recommended map viewport (`low.latitude`, `low.longitude`, `high.latitude`, `high.longitude`) |
| `types` | array\<string\> | Place type tags (e.g., `["restaurant", "food"]`) |
| `primaryType` | string | Main category of the place |
| `primaryTypeDisplayName` | struct | Localized display name of primary type |
| `nationalPhoneNumber` | string | Phone in national format |
| `internationalPhoneNumber` | string | Phone in international format |
| `websiteUri` | string | Official business website URL |
| `googleMapsUri` | string | Link to Google Maps page |
| `businessStatus` | string | Operational status enum: `OPERATIONAL`, `CLOSED_TEMPORARILY`, `CLOSED_PERMANENTLY` |
| `priceLevel` | string | Price category enum: `PRICE_LEVEL_FREE`, `PRICE_LEVEL_INEXPENSIVE`, `PRICE_LEVEL_MODERATE`, `PRICE_LEVEL_EXPENSIVE`, `PRICE_LEVEL_VERY_EXPENSIVE` |
| `rating` | double | Average user rating (1.0 - 5.0) |
| `userRatingCount` | integer | Total number of user reviews |
| `utcOffsetMinutes` | integer | Timezone offset from UTC in minutes |
| `timeZone` | string | IANA timezone identifier |
| `regularOpeningHours` | struct | Standard operating hours |
| `currentOpeningHours` | struct | Operating hours for next 7 days |
| `editorialSummary` | struct | Editorial overview (`text`, `languageCode`) |
| `reviews` | array\<struct\> | Up to 5 user reviews |
| `photos` | array\<struct\> | Up to 10 photo references |
| `addressComponents` | array\<struct\> | Structured address components |
| `plusCode` | struct | Plus Code location reference (`globalCode`, `compoundCode`) |
| `postalAddress` | struct | Structured postal address |
| `takeout` | boolean | Offers takeout |
| `delivery` | boolean | Offers delivery |
| `dineIn` | boolean | Offers dine-in |
| `curbsidePickup` | boolean | Offers curbside pickup |
| `reservable` | boolean | Accepts reservations |
| `servesBreakfast` | boolean | Serves breakfast |
| `servesLunch` | boolean | Serves lunch |
| `servesDinner` | boolean | Serves dinner |
| `servesBeer` | boolean | Serves beer |
| `servesWine` | boolean | Serves wine |
| `servesBrunch` | boolean | Serves brunch |
| `servesVegetarianFood` | boolean | Serves vegetarian food |
| `outdoorSeating` | boolean | Has outdoor seating |
| `liveMusic` | boolean | Has live music |
| `goodForChildren` | boolean | Good for children |
| `allowsDogs` | boolean | Allows dogs |
| `goodForGroups` | boolean | Good for groups |
| `goodForWatchingSports` | boolean | Good for watching sports |
| `paymentOptions` | struct | Accepted payment methods |
| `parkingOptions` | struct | Available parking types |
| `accessibilityOptions` | struct | Wheelchair access features |
| `fuelOptions` | struct | Fuel types (gas stations) |
| `evChargeOptions` | struct | EV charging details |
| `priceRange` | struct | Price range with start and end values |

**Nested `displayName` / `editorialSummary` struct (LocalizedText)**:

| Field | Type | Description |
|-------|------|-------------|
| `text` | string | The localized text content |
| `languageCode` | string | BCP-47 language code |

**Nested `location` struct (LatLng)**:

| Field | Type | Description |
|-------|------|-------------|
| `latitude` | double | Latitude in decimal degrees |
| `longitude` | double | Longitude in decimal degrees |

**Nested `review` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Review resource name |
| `relativePublishTimeDescription` | string | Human-readable relative time |
| `rating` | integer | Star rating (1-5) |
| `text` | struct (LocalizedText) | Review text |
| `originalText` | struct (LocalizedText) | Original language review text |
| `authorAttribution` | struct | Author info (`displayName`, `uri`, `photoUri`) |
| `publishTime` | string (RFC3339 timestamp) | Publication timestamp |
| `flagContentUri` | string | URI to flag inappropriate content |
| `googleMapsUri` | string | Link to the review on Google Maps |

**Nested `addressComponent` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `longText` | string | Full text description of the component |
| `shortText` | string | Abbreviated text |
| `types` | array\<string\> | Component types |
| `languageCode` | string | Language code |

**Example Text Search request**:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-Goog-Api-Key: YOUR_API_KEY" \
  -H "X-Goog-FieldMask: places.id,places.displayName,places.formattedAddress,places.location,places.types,places.rating,places.userRatingCount,places.businessStatus" \
  -d '{
    "textQuery": "pizza restaurants in San Francisco",
    "pageSize": 5
  }' \
  "https://places.googleapis.com/v1/places:searchText"
```

**Example response (truncated)**:

```json
{
  "places": [
    {
      "id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
      "types": ["pizza_restaurant", "restaurant", "food", "point_of_interest", "establishment"],
      "formattedAddress": "123 Main St, San Francisco, CA 94105, USA",
      "location": {
        "latitude": 37.7749295,
        "longitude": -122.4194155
      },
      "rating": 4.5,
      "userRatingCount": 1234,
      "displayName": {
        "text": "Joe's Pizza",
        "languageCode": "en"
      },
      "businessStatus": "OPERATIONAL"
    }
  ],
  "nextPageToken": "AeJbb3eFR..."
}
```

---

### `place_details` Object

**Source endpoint**: `GET https://places.googleapis.com/v1/places/{placeId}`

Uses the same Place object schema as text/nearby search above, but for a single place. Returns all fields specified in the field mask.

**Example request**:

```bash
curl -X GET \
  -H "X-Goog-Api-Key: YOUR_API_KEY" \
  -H "X-Goog-FieldMask: id,displayName,formattedAddress,location,rating,regularOpeningHours,reviews" \
  "https://places.googleapis.com/v1/places/ChIJN1t_tDeuEmsRUsoyG83frY4"
```

---

### `geocode` / `reverse_geocode` Object

**Source endpoint**: `GET https://maps.googleapis.com/maps/api/geocode/json`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `place_id` | string | Unique identifier for the result |
| `formatted_address` | string | Complete human-readable address |
| `geometry_location_lat` | double | Geocoded latitude |
| `geometry_location_lng` | double | Geocoded longitude |
| `geometry_location_type` | string | Accuracy enum: `ROOFTOP`, `RANGE_INTERPOLATED`, `GEOMETRIC_CENTER`, `APPROXIMATE` |
| `geometry_viewport` | struct | Display-suitable viewport (`northeast`, `southwest` with `lat`, `lng`) |
| `geometry_bounds` | struct or null | Bounding box (not always present) |
| `address_components` | array\<struct\> | Structured address components |
| `types` | array\<string\> | Address type classifications |
| `plus_code` | struct or null | Plus code (`global_code`, `compound_code`) |
| `partial_match` | boolean | Whether the result is an incomplete match |

**Nested `address_component` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `long_name` | string | Full name of the component |
| `short_name` | string | Abbreviated name |
| `types` | array\<string\> | Component types (e.g., `street_number`, `route`, `locality`) |

**Example geocoding request**:

```bash
curl "https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key=YOUR_API_KEY"
```

**Example response (truncated)**:

```json
{
  "status": "OK",
  "results": [
    {
      "place_id": "ChIJtYuu0V25j4ARwu5e4wwRYgE",
      "formatted_address": "1600 Amphitheatre Pkwy, Mountain View, CA 94043, USA",
      "geometry": {
        "location": { "lat": 37.4224764, "lng": -122.0842499 },
        "location_type": "ROOFTOP",
        "viewport": {
          "northeast": { "lat": 37.4238253802915, "lng": -122.0829009197085 },
          "southwest": { "lat": 37.4211274197085, "lng": -122.0855988802915 }
        }
      },
      "address_components": [
        { "long_name": "1600", "short_name": "1600", "types": ["street_number"] },
        { "long_name": "Amphitheatre Parkway", "short_name": "Amphitheatre Pkwy", "types": ["route"] },
        { "long_name": "Mountain View", "short_name": "Mountain View", "types": ["locality", "political"] }
      ],
      "types": ["street_address"],
      "partial_match": false
    }
  ]
}
```

---

### `routes_compute` Object

**Source endpoint**: `POST https://routes.googleapis.com/directions/v2:computeRoutes`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `distanceMeters` | integer | Total route distance in meters |
| `duration` | string | Travel time with traffic (e.g., `"1234s"`) |
| `staticDuration` | string | Travel time without traffic |
| `polyline` | struct | Route geometry (encoded polyline or GeoJSON) |
| `description` | string | Route description text |
| `warnings` | array\<string\> | Route warnings/alerts |
| `viewport` | struct | Bounding box for the route |
| `routeLabels` | array\<string\> | Route classifications (e.g., `DEFAULT_ROUTE`, `FUEL_EFFICIENT`) |
| `legs` | array\<struct\> | Route segments between waypoints |
| `travelAdvisory` | struct | Toll info, fuel consumption, speed reading intervals |
| `optimizedIntermediateWaypointIndex` | array\<integer\> | Reordered waypoint indices |
| `routeToken` | string | Opaque token for Navigation SDK |

**Nested `leg` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `distanceMeters` | integer | Leg distance in meters |
| `duration` | string | Leg travel time |
| `staticDuration` | string | Leg travel time without traffic |
| `polyline` | struct | Leg geometry |
| `startLocation` | struct | Start coordinates (`latLng`) |
| `endLocation` | struct | End coordinates (`latLng`) |
| `steps` | array\<struct\> | Turn-by-turn navigation steps |

---

### `routes_matrix` Object

**Source endpoint**: `POST https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `originIndex` | integer | Zero-indexed position in origins array |
| `destinationIndex` | integer | Zero-indexed position in destinations array |
| `status` | struct | Error status (code, message) |
| `condition` | string | Route status: `ROUTE_EXISTS`, `ROUTE_NOT_FOUND` |
| `distanceMeters` | integer | Route distance in meters |
| `duration` | string | Travel duration (e.g., `"160s"`) |
| `staticDuration` | string | Duration without traffic |
| `travelAdvisory` | struct | Toll info, speed reading intervals |
| `fallbackInfo` | struct | Fallback computation details |
| `localizedValues` | struct | Text representations of distance/duration |

---

### `roads_snap` Object

**Source endpoint**: `GET https://roads.googleapis.com/v1/snapToRoads`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `latitude` | double | Snapped latitude on road |
| `longitude` | double | Snapped longitude on road |
| `placeId` | string | Unique road segment Place ID |
| `originalIndex` | integer or null | Index of the original input point (null for interpolated points) |

---

### `roads_nearest` Object

**Source endpoint**: `GET https://roads.googleapis.com/v1/nearestRoads`

Same schema as `roads_snap`.

---

### `roads_speed_limits` Object

**Source endpoint**: `GET https://roads.googleapis.com/v1/speedLimits`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `placeId` | string | Unique road segment Place ID |
| `speedLimit` | double | Posted speed limit value |
| `units` | string | Speed unit: `KPH` or `MPH` |

Note: The Speed Limits endpoint additionally returns `snappedPoints` (same schema as `roads_snap`) when the `path` parameter is used instead of `placeId`.

---

### `elevation` Object

**Source endpoint**: `GET https://maps.googleapis.com/maps/api/elevation/json`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `elevation` | double | Height in meters relative to local mean sea level |
| `location_lat` | double | Latitude of the elevation point |
| `location_lng` | double | Longitude of the elevation point |
| `resolution` | double | Maximum distance between data points in meters |

---

### `timezone` Object

**Source endpoint**: `GET https://maps.googleapis.com/maps/api/timezone/json`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `timeZoneId` | string | IANA timezone identifier (e.g., `America/Los_Angeles`) |
| `timeZoneName` | string | Full timezone display name |
| `dstOffset` | integer | Daylight saving offset in seconds |
| `rawOffset` | integer | UTC offset in seconds (excluding DST) |

---

### `air_quality_current` Object

**Source endpoint**: `POST https://airquality.googleapis.com/v1/currentConditions:lookup`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `dateTime` | string (RFC3339) | UTC timestamp of the air quality data (rounded down to the hour) |
| `regionCode` | string | ISO 3166-1 alpha-2 country/region code |
| `indexes` | array\<struct\> | Air quality index values (up to 2: Universal AQI and/or Local AQI) |
| `pollutants` | array\<struct\> | List of pollutant measurements |
| `healthRecommendations` | struct | Health guidance by population group |

**Nested `index` struct (AirQualityIndex)**:

| Field | Type | Description |
|-------|------|-------------|
| `code` | string | Index code (e.g., `uaqi`, `us_aqi`) |
| `displayName` | string | Human-readable index name |
| `aqi` | integer | Index value |
| `aqiDisplay` | string | Display string for the value |
| `color` | struct | Display color (RGBA) |
| `category` | string | Category label (e.g., "Good", "Moderate") |
| `dominantPollutant` | string | Primary pollutant |

---

### `air_quality_history` Object

**Source endpoint**: `POST https://airquality.googleapis.com/v1/history:lookup`

Returns the same per-hour data structure as `air_quality_current`, but with multiple hourly records.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `dateTime` | string (RFC3339) | UTC timestamp for the hourly record |
| `indexes` | array\<struct\> | Air quality index values |
| `pollutants` | array\<struct\> | Pollutant measurements |
| `healthRecommendations` | struct | Health guidance |
| `regionCode` | string | ISO 3166-1 alpha-2 country/region code |

---

### `address_validation` Object

**Source endpoint**: `POST https://addressvalidation.googleapis.com/v1:validateAddress`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `inputGranularity` | string | Granularity level of the input address |
| `validationGranularity` | string | Granularity level of validation achievable |
| `geocode` | struct | Geocoding result with coordinates |
| `verdict` | struct | Overall validation result |
| `address` | struct | Standardized and validated address |
| `uspsData` | struct or null | USPS-specific data (US/Puerto Rico only) |


## **Get Object Primary Keys**

Google Maps Platform APIs do not provide a dedicated metadata endpoint for primary keys. Primary keys are defined **statically** based on resource schemas and the nature of each API.

### Primary Keys by Object

| Object | Primary Key(s) | Type | Notes |
|--------|----------------|------|-------|
| `places_text_search` | `id` | string | Place ID, globally unique |
| `places_nearby_search` | `id` | string | Place ID, globally unique |
| `place_details` | `id` | string | Place ID, globally unique |
| `geocode` | `place_id` | string | Unique geocoding result identifier |
| `reverse_geocode` | `place_id` | string | Unique geocoding result identifier |
| `routes_compute` | (synthetic) | - | No natural PK; use hash of origin + destination + travel mode |
| `routes_matrix` | `originIndex`, `destinationIndex` | integer, integer | Composite key within a single request |
| `roads_snap` | `placeId`, `originalIndex` | string, integer | Composite: road segment + input point |
| `roads_nearest` | `placeId`, `originalIndex` | string, integer | Composite: road segment + input point |
| `roads_speed_limits` | `placeId` | string | Unique road segment identifier |
| `elevation` | `location_lat`, `location_lng` | double, double | Composite: coordinates of the point |
| `timezone` | (synthetic) | - | No natural PK; use composite of input `location` + `timestamp` |
| `air_quality_current` | (synthetic) | - | No natural PK; use composite of input `location` + `dateTime` |
| `air_quality_history` | (synthetic) | - | Composite of input `location` + `dateTime` |
| `address_validation` | (synthetic) | - | No natural PK; use hash of input address |

**Key observations**:
- Places API objects have a natural primary key (`id` / Place ID), which is stable and globally unique.
- Geocoding results have a `place_id` that serves as a natural primary key.
- Query-based APIs (Routes, Elevation, Time Zone, Air Quality) do not have inherent primary keys. The connector must synthesize keys from input parameters.
- For connector implementation, synthetic primary keys should be generated by combining input parameters (e.g., origin + destination for routes, coordinates for elevation).


## **Object's Ingestion Type**

### Supported Ingestion Types

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `places_text_search` | `snapshot` | Query-based API with no incremental cursor. Results change as places open, close, or update. Full re-query required. |
| `places_nearby_search` | `snapshot` | Query-based API with no change tracking. Results reflect current state of places near a point. |
| `place_details` | `snapshot` | Returns current state of a place. No `updated_at` or change tracking field exposed. Place data can change (hours, rating, etc.) without notification. |
| `geocode` | `snapshot` | Address-to-coordinate mapping is essentially static. No incremental capabilities. |
| `reverse_geocode` | `snapshot` | Coordinate-to-address mapping is essentially static. |
| `routes_compute` | `snapshot` | Routes are computed on-demand based on current traffic. No historical state. |
| `routes_matrix` | `snapshot` | Distance matrices are computed on-demand. Results vary with traffic conditions. |
| `roads_snap` | `snapshot` | Road snapping is a stateless computation. |
| `roads_nearest` | `snapshot` | Nearest road lookup is stateless. |
| `roads_speed_limits` | `snapshot` | Speed limits are relatively static but have no change tracking. |
| `elevation` | `snapshot` | Elevation data is essentially static geographic data. |
| `timezone` | `snapshot` | Time zone data rarely changes (only during policy updates). |
| `air_quality_current` | `snapshot` | Returns current conditions only. Each call returns the latest hourly data. |
| `air_quality_history` | `append` | Historical data is immutable once created. New hourly records can be appended incrementally by advancing the time window. |
| `address_validation` | `snapshot` | Validation results are computed on-demand. |

**Key design considerations**:

1. **No CDC support**: Google Maps Platform APIs do not provide change data capture, webhooks, or `updated_at` timestamps on resources. There is no way to ask "what changed since my last sync?"

2. **Snapshot is the primary pattern**: Almost all objects are best served as snapshots because:
   - Place data changes unpredictably (a restaurant may update hours, close permanently, or get new reviews)
   - Routes and matrices are ephemeral computations
   - Geocoding results are essentially stable lookups

3. **Air Quality History is the exception**: The history endpoint returns immutable hourly records that can be fetched incrementally by advancing the time window. This makes it suitable for `append` ingestion with a date/time-based cursor.

4. **No delete tracking**: Google Maps APIs do not track deletions. Places may become unavailable (returning `NOT_FOUND`), but there is no dedicated endpoint or flag for deleted places. The `businessStatus` field on places can indicate `CLOSED_PERMANENTLY`, which is the closest equivalent to a soft delete.


## **Read API for Data Retrieval**

### Places API (New): Text Search

- **HTTP method**: POST
- **Endpoint**: `https://places.googleapis.com/v1/places:searchText`

**Required headers**:

| Header | Value | Description |
|--------|-------|-------------|
| `Content-Type` | `application/json` | Request body format |
| `X-Goog-Api-Key` | `YOUR_API_KEY` | Authentication |
| `X-Goog-FieldMask` | comma-separated field paths | Controls which fields are returned and billing tier |

**Request body parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `textQuery` | string | yes | - | Search text (e.g., "pizza in NYC", "123 Main St") |
| `pageSize` | integer | no | 20 | Results per page (1-20) |
| `pageToken` | string | no | - | Pagination token from previous response |
| `includedType` | string | no | - | Filter to a specific place type |
| `languageCode` | string | no | `en` | Response language |
| `regionCode` | string | no | - | CLDR region code for formatting |
| `locationBias` | object | no | - | Bias results toward a location (circle or rectangle) |
| `locationRestriction` | object | no | - | Restrict results to a rectangular area |
| `minRating` | number | no | - | Minimum user rating (0.0-5.0 in 0.5 increments) |
| `openNow` | boolean | no | - | Only return currently open places |
| `rankPreference` | string | no | `RELEVANCE` | Sort by `RELEVANCE` or `DISTANCE` |
| `priceLevels` | array\<string\> | no | - | Filter by price level |
| `strictTypeFiltering` | boolean | no | false | Enforce type filtering strictly |

**Pagination strategy**:
- Text Search returns a maximum of **60 results across all pages**.
- Each page contains up to 20 results (`pageSize` max is 20).
- The response includes a `nextPageToken` field if more results are available.
- Pass `nextPageToken` as `pageToken` in the next request to get subsequent pages.
- Pagination ends when `nextPageToken` is absent from the response.

**Example paginated read**:

```python
def fetch_all_text_search_results(api_key, text_query, field_mask):
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": field_mask,
    }
    all_places = []
    page_token = None

    while True:
        body = {"textQuery": text_query, "pageSize": 20}
        if page_token:
            body["pageToken"] = page_token

        response = requests.post(url, headers=headers, json=body)
        data = response.json()

        places = data.get("places", [])
        all_places.extend(places)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return all_places
```

---

### Places API (New): Nearby Search

- **HTTP method**: POST
- **Endpoint**: `https://places.googleapis.com/v1/places:searchNearby`

**Request body parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `locationRestriction` | object | yes | - | Circle with `center` (`latitude`, `longitude`) and `radius` (0.0-50000.0 meters) |
| `includedTypes` | array\<string\> | no | - | Filter by place types (up to 50) |
| `excludedTypes` | array\<string\> | no | - | Exclude place types (up to 50) |
| `includedPrimaryTypes` | array\<string\> | no | - | Filter by primary type |
| `excludedPrimaryTypes` | array\<string\> | no | - | Exclude by primary type |
| `languageCode` | string | no | `en` | Response language |
| `maxResultCount` | integer | no | 20 | Results to return (1-20) |
| `rankPreference` | string | no | `POPULARITY` | Sort by `POPULARITY` or `DISTANCE` |
| `regionCode` | string | no | - | CLDR region code |

**Pagination**: Nearby Search does **not** support pagination. All results are returned in a single response, limited by `maxResultCount` (max 20).

---

### Places API (New): Place Details

- **HTTP method**: GET
- **Endpoint**: `https://places.googleapis.com/v1/places/{placeId}`

**Path parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `placeId` | string | yes | The Place ID to look up |

**Query parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `languageCode` | string | no | Response language |
| `regionCode` | string | no | CLDR region code |
| `sessionToken` | string | no | Autocomplete session tracking token |

---

### Geocoding API

- **HTTP method**: GET
- **Endpoint**: `https://maps.googleapis.com/maps/api/geocode/json`

**Query parameters (geocoding)**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `address` | string | yes (or `components`) | The address to geocode |
| `components` | string | no | Component filter (e.g., `country:US`) |
| `bounds` | string | no | Viewport bias (`southwest\|northeast` coordinates) |
| `language` | string | no | Response language |
| `region` | string | no | Country code for biasing |
| `key` | string | yes | API key |

**Query parameters (reverse geocoding)**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `latlng` | string | yes (or `place_id`) | Coordinates as `lat,lng` |
| `place_id` | string | yes (or `latlng`) | Place ID to reverse geocode |
| `result_type` | string | no | Filter by address type |
| `location_type` | string | no | Filter by geometry location type |
| `language` | string | no | Response language |
| `key` | string | yes | API key |

**Pagination**: Geocoding API does **not** support pagination. All results are returned in a single response (typically 1-5 results).

---

### Routes API: Compute Routes

- **HTTP method**: POST
- **Endpoint**: `https://routes.googleapis.com/directions/v2:computeRoutes`

**Required headers**:

| Header | Value |
|--------|-------|
| `Content-Type` | `application/json` |
| `X-Goog-Api-Key` | `YOUR_API_KEY` |
| `X-Goog-FieldMask` | Comma-separated field paths (e.g., `routes.duration,routes.distanceMeters,routes.polyline`) |

**Request body parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `origin` | Waypoint | yes | Starting location (coordinates or Place ID) |
| `destination` | Waypoint | yes | Ending location |
| `intermediates` | array\<Waypoint\> | no | Up to 25 intermediate waypoints |
| `travelMode` | string | no | `DRIVE`, `BICYCLE`, `WALK`, `TRANSIT`, `TWO_WHEELER` |
| `routingPreference` | string | no | `TRAFFIC_UNAWARE`, `TRAFFIC_AWARE`, `TRAFFIC_AWARE_OPTIMAL` |
| `departureTime` | string (RFC3339) | no | When to depart |
| `arrivalTime` | string (RFC3339) | no | When to arrive (TRANSIT only) |
| `computeAlternativeRoutes` | boolean | no | Return up to 3 routes |
| `routeModifiers` | object | no | Avoid tolls, highways, ferries, indoor |
| `languageCode` | string | no | Response language |
| `units` | string | no | `IMPERIAL` or `METRIC` |
| `optimizeWaypointOrder` | boolean | no | Reorder intermediates for efficiency |
| `extraComputations` | array\<string\> | no | `TOLLS`, `FUEL_CONSUMPTION`, `TRAFFIC_ON_POLYLINE`, `HTML_FORMATTED_NAVIGATION_INSTRUCTIONS` |

**Pagination**: Routes API does **not** support pagination. Returns up to 3 routes in a single response.

---

### Routes API: Compute Route Matrix

- **HTTP method**: POST
- **Endpoint**: `https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix`

**Request body parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `origins` | array\<RouteMatrixOrigin\> | yes | Array of origin waypoints with optional route modifiers |
| `destinations` | array\<RouteMatrixDestination\> | yes | Array of destination waypoints |
| `travelMode` | string | no | `DRIVE`, `BICYCLE`, `WALK`, `TRANSIT`, `TWO_WHEELER` |
| `routingPreference` | string | no | `TRAFFIC_UNAWARE`, `TRAFFIC_AWARE`, `TRAFFIC_AWARE_OPTIMAL` |
| `departureTime` | string (RFC3339) | no | Departure time |
| `languageCode` | string | no | Response language |

**Element limits**:
- Standard routes: Maximum **625 elements** (origins x destinations)
- Transit routes: Maximum **100 elements**
- `TRAFFIC_AWARE_OPTIMAL`: Maximum **100 elements**
- Address/Place ID waypoints: Up to **50 total** across origins and destinations

**Pagination**: No pagination. All elements returned in a single response.

---

### Roads API

All Roads API endpoints use GET requests with query parameters.

**Snap to Roads**: `GET https://roads.googleapis.com/v1/snapToRoads`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | yes | Pipe-separated lat,lng pairs (max 100 points) |
| `interpolate` | boolean | no | Insert interpolated points (default: false) |
| `key` | string | yes | API key |

**Nearest Roads**: `GET https://roads.googleapis.com/v1/nearestRoads`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `points` | string | yes | Pipe-separated lat,lng pairs (max 100 points) |
| `key` | string | yes | API key |

**Speed Limits**: `GET https://roads.googleapis.com/v1/speedLimits`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | string | one required | Pipe-separated lat,lng pairs (max 100) |
| `placeId` | string | one required | One or more road segment Place IDs (max 100, repeated param) |
| `units` | string | no | `KPH` (default) or `MPH` |
| `key` | string | yes | API key |

Note: Speed Limits requires an **Asset Tracking license**.

---

### Air Quality API: History

- **HTTP method**: POST
- **Endpoint**: `https://airquality.googleapis.com/v1/history:lookup`

**Request body parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `location` | LatLng | yes | Coordinates for the query |
| `hours` | integer | one required | Number of hours of history (1-720, i.e., up to 30 days) |
| `dateTime` | string (RFC3339) | one required | Specific timestamp to query |
| `period` | object | one required | Custom time range (`startTime`, `endTime`) |
| `pageSize` | integer | no | Records per page (default 72, max 168) |
| `pageToken` | string | no | Pagination token |
| `extraComputations` | array | no | Additional data features |
| `universalAqi` | boolean | no | Include Universal AQI (default: true) |
| `languageCode` | string | no | Response language |

**Pagination**: Uses cursor-based pagination with `pageToken` / `nextPageToken`. All request parameters except `pageToken` must remain identical across pages.

**Incremental strategy for `air_quality_history`**:
1. On first run, fetch history for a configurable lookback period (e.g., 720 hours / 30 days)
2. Store the latest `dateTime` as the cursor
3. On subsequent runs, use the `period` parameter with `startTime` = last cursor and `endTime` = now
4. Historical air quality data is immutable once published

---

### Handling Deleted Records

Google Maps Platform APIs **do not support delete tracking**:
- Places can disappear (return `NOT_FOUND` on detail lookup) or have `businessStatus` set to `CLOSED_PERMANENTLY`
- No dedicated deleted records endpoint or audit log exists
- The connector should not implement `read_table_deletes()`
- For places, periodic full snapshot re-sync is the only way to detect disappearances

---

### Rate Limits

| API | Rate Limit (QPM) | Notes |
|-----|-------------------|-------|
| Places API (New) - Text Search | Per-method per-project | Exact QPM not publicly documented; configurable in Cloud Console |
| Places API (New) - Nearby Search | Per-method per-project | Same as above |
| Places API (New) - Place Details | Per-method per-project | Same as above |
| Geocoding API | 3,000 QPM | Sum of client-side and server-side queries |
| Routes API - Compute Routes | 3,000 QPM | Per project |
| Routes API - Route Matrix | 3,000 EPM | Elements per minute (origins x destinations) |
| Roads API | Not publicly documented | Configurable in Cloud Console |
| Elevation API | 3,000 QPM | Per project |
| Time Zone API | 3,000 QPM | Per project |
| Air Quality API | 6,000 QPM | Per API method |
| Address Validation API | 6,000 QPM | Per method |

**Rate limit handling**:
- HTTP 429 (Too Many Requests) is returned when limits are exceeded
- Implement exponential backoff with jitter
- Google recommends waiting and retrying with increasing intervals
- No daily request limits exist; only QPM limits and cost controls
- QPM can be adjusted via the Google Cloud Console quota management page


## **Field Type Mapping**

### General Mapping (Google Maps JSON to Connector Logical Types)

| Google Maps JSON Type | Example Fields | Connector Logical Type (Spark) | Notes |
|-----------------------|----------------|-------------------------------|-------|
| string | `id`, `formattedAddress`, `name` | `StringType` | UTF-8 text |
| number (integer) | `userRatingCount`, `utcOffsetMinutes` | `LongType` | 64-bit integer |
| number (float) | `rating`, `latitude`, `longitude`, `elevation` | `DoubleType` | 64-bit floating point |
| boolean | `takeout`, `delivery`, `dineIn` | `BooleanType` | Standard true/false |
| string (RFC3339 timestamp) | `publishTime`, `dateTime` | `StringType` | ISO 8601 timestamp strings; parse downstream |
| string (duration) | `duration`, `staticDuration` | `StringType` | Format: `"1234s"` (seconds as string) |
| string (enum) | `businessStatus`, `priceLevel`, `condition` | `StringType` | Enum values represented as strings |
| object | `location`, `displayName`, `viewport` | `StructType` | Nested record |
| array | `types`, `reviews`, `photos`, `addressComponents` | `ArrayType` | Array of strings or structs |
| null | Any nullable field | Corresponding type + nullable | Fields may be absent or null |

### Special Field Behaviors

- **Place IDs** (`id`, `placeId`, `place_id`): Opaque strings, globally unique. Should not be parsed or modified. Can be used as stable identifiers across API calls.
- **LatLng fields**: Always returned as doubles with sufficient decimal precision (typically 7+ digits). Store as `DoubleType`.
- **Duration fields**: Returned as strings in the format `"<seconds>s"` (e.g., `"1234s"`). Parse the numeric portion if a numeric type is needed.
- **Enum fields**: `businessStatus`, `priceLevel`, `locationRestriction.type`, `travelMode`, etc. Represented as uppercase strings. Keep as `StringType` for flexibility.
- **Field masks**: The Places API (New) and Routes API only return fields specified in the `X-Goog-FieldMask` header. Missing fields in the response should be treated as `null`, not empty.
- **Monetary values**: `priceRange` contains `startPrice` and `endPrice` with `currencyCode` and `units`/`nanos` fields. Store monetary values as nested structs.
- **Photo references**: `photos` contains `name` fields that are photo resource names, not URLs. To get actual photo bytes, a separate request to `GET https://places.googleapis.com/v1/{name}/media` is needed.
- **Review text**: `reviews[].text` is a LocalizedText struct with `text` and `languageCode` fields, not a plain string.
- **Geocoding status codes**: Legacy-style APIs (Geocoding, Elevation, Time Zone) wrap responses in a top-level object with a `status` field. The connector should check `status == "OK"` before processing results. Common status values: `OK`, `ZERO_RESULTS`, `OVER_QUERY_LIMIT`, `REQUEST_DENIED`, `INVALID_REQUEST`, `UNKNOWN_ERROR`.

### Constraints and Validation

- **Coordinate ranges**: Latitude: -90 to 90; Longitude: -180 to 180
- **Rating range**: 1.0 to 5.0 (inclusive)
- **URL length limits**: Legacy-style API URLs are limited to 16,384 characters
- **Input limits per request**:
  - Text Search: 20 results per page, 60 total
  - Nearby Search: 20 results max, no pagination
  - Place Details: 1 place per request
  - Geocoding: Typically returns 1-5 results
  - Roads Snap/Nearest: 100 GPS points per request
  - Roads Speed Limits: 100 segments per request
  - Routes Matrix: 625 elements max (standard), 100 (transit/optimal)
  - Elevation: 512 locations per request
  - Air Quality History: 168 hourly records per page, 720 hours max per query


## **Connector Implementation Notes**

### Input-Driven Architecture

Unlike traditional data source APIs (e.g., Zendesk, GitHub) where you list resources and iterate, Google Maps Platform APIs are **input-driven**. Each API call requires specific input parameters:

- **Places Text Search**: requires a `textQuery` string
- **Places Nearby Search**: requires `latitude`, `longitude`, and `radius`
- **Place Details**: requires a `placeId`
- **Geocoding**: requires an `address` or `latlng`
- **Routes**: requires `origin` and `destination`
- **Roads**: requires `path` (GPS coordinates)
- **Elevation/Time Zone**: requires `location` coordinates
- **Air Quality**: requires `location` coordinates

**Connector design implications**:
- The connector must accept input parameters via `table_options`
- `list_tables()` returns a static list of supported table types
- `read_table()` uses `table_options` to construct API requests
- Multiple calls with different `table_options` values are needed to build up a dataset
- The connector acts more like a "query executor" than a "data puller"

### Recommended `table_options` Parameters

For the connector's `read_table()` method, the following `table_options` should be supported:

**For `places_text_search`**:
- `text_query` (required): Search query string
- `field_mask` (optional): Comma-separated field paths
- `location_bias_lat`, `location_bias_lng`, `location_bias_radius` (optional): Location bias
- `included_type` (optional): Place type filter
- `language_code` (optional): Response language
- `min_rating` (optional): Minimum rating filter
- `open_now` (optional): Only open places

**For `places_nearby_search`**:
- `latitude` (required): Center latitude
- `longitude` (required): Center longitude
- `radius` (required): Search radius in meters
- `field_mask` (optional): Comma-separated field paths
- `included_types` (optional): Comma-separated place types
- `max_result_count` (optional): Max results (1-20)

**For `place_details`**:
- `place_id` (required): Place ID to look up
- `field_mask` (optional): Comma-separated field paths
- `language_code` (optional): Response language

**For `geocode`**:
- `address` (required for forward geocoding): Address to geocode
- `latlng` (required for reverse geocoding): Coordinates
- `language` (optional): Response language
- `region` (optional): Region bias

**For `air_quality_history`**:
- `latitude` (required): Location latitude
- `longitude` (required): Location longitude
- `hours` (optional): Number of hours of history (1-720)

### Batch Processing Pattern

Since many Google Maps APIs process individual items (one address, one place, one coordinate), the connector should support batch processing by:

1. Accepting a list of inputs (e.g., a list of addresses to geocode)
2. Making individual API calls for each input
3. Respecting rate limits between calls
4. Returning all results as a single table

This can be implemented by accepting comma-separated or newline-separated inputs in `table_options`, or by integrating with an external input source.

### Field Mask Best Practices

For the Places API (New) and Routes API:
- Always specify a field mask to control costs and response size
- Provide a sensible default field mask for common use cases
- Allow users to override via `table_options`
- Field mask directly impacts billing tier (Essentials < Pro < Enterprise < Enterprise + Atmosphere)

### Error Handling

| HTTP Status | Meaning | Connector Action |
|-------------|---------|------------------|
| 200 | Success | Process response |
| 400 | Invalid request | Log error, skip record |
| 401 | Authentication failure | Raise configuration error |
| 403 | Permission denied / API not enabled | Raise configuration error |
| 404 | Resource not found | Return null/empty for the item |
| 429 | Rate limit exceeded | Exponential backoff and retry |
| 500/503 | Server error | Retry with backoff |

For legacy-style APIs (Geocoding, Elevation, Time Zone), check the `status` field in the response body:
- `OK`: Process results
- `ZERO_RESULTS`: Return empty result set
- `OVER_QUERY_LIMIT`: Backoff and retry
- `REQUEST_DENIED`: Raise configuration error
- `INVALID_REQUEST`: Log error and skip


## **Known Quirks and Edge Cases**

### Query-Based vs. Resource-Based API

- **Issue**: Google Maps APIs are fundamentally query-based, not resource-listing APIs. There is no "list all places" or "list all addresses" endpoint.
- **Impact**: The connector cannot perform a traditional full table scan. Every read requires input parameters.
- **Solution**: Design the connector to accept input parameters via `table_options`. Document that users must provide search criteria.

### Field Mask Requirement (Places and Routes APIs)

- **Issue**: The Places API (New) and Routes API require an `X-Goog-FieldMask` header. Without it, the API returns an error.
- **Impact**: The connector must always include a field mask. Different field masks trigger different billing tiers.
- **Solution**: Provide a default field mask covering commonly needed fields at the Pro tier. Allow override via `table_options`.

### No Incremental Sync for Places

- **Issue**: Place data has no `updated_at` timestamp or change feed. A restaurant's hours, rating, or status can change at any time without notification.
- **Impact**: CDC or incremental ingestion is not possible for place data.
- **Solution**: Use snapshot ingestion. Re-run queries periodically to capture changes.

### Place ID Stability

- **Issue**: Place IDs are generally stable but can occasionally change (e.g., when Google merges duplicate place entries). The `movedPlaceId` field may point to a new Place ID for permanently closed/moved places.
- **Impact**: Historical references to Place IDs may become invalid.
- **Solution**: Always re-validate Place IDs when using cached data. Handle `NOT_FOUND` responses gracefully.

### Text Search Result Limit

- **Issue**: Text Search returns a maximum of 60 results across all pages, regardless of how many matching places exist.
- **Impact**: Large-scale place data collection requires multiple queries with different parameters.
- **Solution**: Use specific queries (narrow geographic areas, specific types) and combine results. Use Nearby Search for geographic grid-based coverage.

### Nearby Search: No Pagination

- **Issue**: Nearby Search returns a maximum of 20 results with no pagination support.
- **Impact**: Dense areas may have many more relevant places than can be returned.
- **Solution**: Use smaller search radii and tile the area into a grid of overlapping circles. Deduplicate results by Place ID.

### Speed Limits: License Requirement

- **Issue**: The Roads API Speed Limits endpoint requires a separate Asset Tracking license, which is not included in standard Google Maps Platform access.
- **Impact**: Most users will not have access to speed limit data.
- **Solution**: Document the license requirement. Handle 403 errors gracefully. Make speed limits an optional table.

### Geocoding Ambiguity

- **Issue**: Geocoding API may return multiple results for ambiguous addresses, with `partial_match: true`.
- **Impact**: The "correct" result is not always the first one.
- **Solution**: Return all results and let the user decide. Include the `partial_match` flag in the schema.

### Air Quality Data Availability

- **Issue**: Air Quality data coverage varies by region. Some locations may have no data available.
- **Impact**: Queries for unsupported locations return empty results.
- **Solution**: Handle empty responses gracefully. Document coverage limitations.

### Duration Format

- **Issue**: Routes API returns durations as strings in the format `"1234s"` (seconds suffix) rather than numeric seconds.
- **Impact**: Requires string parsing to extract numeric values.
- **Solution**: Store as-is as StringType. Provide documentation on parsing. Alternatively, parse to LongType in the connector.

### Photo URLs Require Additional Request

- **Issue**: Place photos return a `name` (resource name), not a direct image URL. A separate API call to `GET https://places.googleapis.com/v1/{name}/media` is needed to get the actual image.
- **Impact**: Photo data cannot be fully resolved in a single API call.
- **Solution**: Store photo resource names. Optionally implement a separate photo resolution step.

### Cost Implications

- **Issue**: Google Maps Platform charges per API call. High-volume data extraction can be expensive.
- **Impact**: Uncontrolled querying can generate significant costs.
- **Solution**: Implement configurable rate limiting. Use field masks to minimize billing tier. Document expected costs per query pattern. Respect the $200/month free credit.

### Legacy vs. New API Inconsistencies

- **Issue**: Legacy-style APIs (Geocoding, Elevation, Time Zone) use query parameters and wrap responses in `{status, results}`. New-style APIs (Places, Routes) use POST with JSON bodies and `X-Goog-FieldMask` headers.
- **Impact**: The connector must handle two different API patterns.
- **Solution**: Implement separate request/response handlers for legacy and new-style APIs.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.google.com/maps/documentation | 2026-02-19 | High | Complete list of Google Maps Platform APIs, categories, and products |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/overview | 2026-02-19 | High | Places API (New) overview, legacy deprecation, base URL, auth methods |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/get-api-key | 2026-02-19 | High | API key and OAuth authentication, setup requirements |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/text-search | 2026-02-19 | High | Text Search endpoint, request/response format, pagination (60 result max, pageToken), all parameters |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/nearby-search | 2026-02-19 | High | Nearby Search endpoint, no pagination, max 20 results, locationRestriction requirement |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/place-details | 2026-02-19 | High | Place Details endpoint, field mask tiers (Essentials, Pro, Enterprise), GET method |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/reference/rest/v1/places | 2026-02-19 | High | Complete Place resource schema with all fields and types |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/place-types | 2026-02-19 | High | Complete place type taxonomy (Table A for filtering, Table B for responses) |
| Official Docs | https://developers.google.com/maps/documentation/places/web-service/usage-and-billing | 2026-02-19 | High | Billing model, field mask tiers, $200 monthly credit, QPM per-method per-project |
| Official Docs | https://developers.google.com/maps/documentation/routes/overview | 2026-02-19 | High | Routes API overview, Compute Routes and Route Matrix endpoints |
| Official Docs | https://developers.google.com/maps/documentation/routes/reference/rest/v2/TopLevel/computeRoutes | 2026-02-19 | High | Compute Routes REST reference, all request/response fields, waypoint structures |
| Official Docs | https://developers.google.com/maps/documentation/routes/compute_route_matrix | 2026-02-19 | High | Route Matrix endpoint, element limits (625/100), response schema |
| Official Docs | https://developers.google.com/maps/documentation/roads/overview | 2026-02-19 | High | Roads API overview, three endpoints (Snap, Nearest, Speed Limits) |
| Official Docs | https://developers.google.com/maps/documentation/roads/snap | 2026-02-19 | High | Snap to Roads endpoint, path parameter, interpolation, response schema |
| Official Docs | https://developers.google.com/maps/documentation/roads/nearest | 2026-02-19 | High | Nearest Roads endpoint, points parameter, response schema |
| Official Docs | https://developers.google.com/maps/documentation/roads/speed-limits | 2026-02-19 | High | Speed Limits endpoint, Asset Tracking license requirement, units parameter |
| Official Docs | https://developers.google.com/maps/documentation/geocoding/overview | 2026-02-19 | High | Geocoding API overview, forward and reverse geocoding |
| Official Docs | https://developers.google.com/maps/documentation/geocoding/requests-geocoding | 2026-02-19 | High | Geocoding request/response format, all parameters, complete response schema |
| Official Docs | https://developers.google.com/maps/documentation/elevation/overview | 2026-02-19 | High | Elevation API overview, base URL, capabilities |
| Official Docs | https://developers.google.com/maps/documentation/elevation/requests-elevation | 2026-02-19 | High | Elevation request/response format, 512 point limit, resolution field |
| Official Docs | https://developers.google.com/maps/documentation/timezone/overview | 2026-02-19 | High | Time Zone API overview, base URL, capabilities |
| Official Docs | https://developers.google.com/maps/documentation/timezone/requests-timezone | 2026-02-19 | High | Time Zone request/response format, timestamp parameter, DST/raw offset fields |
| Official Docs | https://developers.google.com/maps/documentation/air-quality/overview | 2026-02-19 | High | Air Quality API overview, 4 endpoints, 30-day history, 500m resolution |
| Official Docs | https://developers.google.com/maps/documentation/air-quality/reference/rest/v1/currentConditions/lookup | 2026-02-19 | High | Current conditions endpoint, request/response schema, OAuth scope |
| Official Docs | https://developers.google.com/maps/documentation/air-quality/reference/rest/v1/history/lookup | 2026-02-19 | High | History endpoint, pagination with pageToken, 720 hours max |
| Official Docs | https://developers.google.com/maps/documentation/address-validation/overview | 2026-02-19 | High | Address Validation API overview, CASS support, component-level validation |
| Official Docs | https://developers.google.com/maps/documentation/pollen/overview | 2026-02-19 | High | Pollen API overview, forecast and heatmap endpoints, 65+ country coverage |
| Official Docs | https://developers.google.com/maps/api-security-best-practices | 2026-02-19 | High | API key security, restriction best practices, OAuth recommendations |
| Web Search | Google Maps Platform rate limits search | 2026-02-19 | High | QPM limits: Geocoding 3000, Routes 3000, Air Quality 6000, Address Validation 6000 |
| Web Search | Google Maps connectors search | 2026-02-19 | Low | No existing Airbyte/Fivetran/dlt connectors found for Google Maps Platform |


## **Sources and References**

### Official Documentation (Highest Confidence)

- Google Maps Platform overview: https://developers.google.com/maps/documentation
- Places API (New) overview: https://developers.google.com/maps/documentation/places/web-service/overview
- Places API authentication: https://developers.google.com/maps/documentation/places/web-service/get-api-key
- Places API Text Search: https://developers.google.com/maps/documentation/places/web-service/text-search
- Places API Nearby Search: https://developers.google.com/maps/documentation/places/web-service/nearby-search
- Places API Place Details: https://developers.google.com/maps/documentation/places/web-service/place-details
- Places API Place Resource reference: https://developers.google.com/maps/documentation/places/web-service/reference/rest/v1/places
- Places API Place Types: https://developers.google.com/maps/documentation/places/web-service/place-types
- Places API Usage and Billing: https://developers.google.com/maps/documentation/places/web-service/usage-and-billing
- Routes API overview: https://developers.google.com/maps/documentation/routes/overview
- Routes API Compute Routes reference: https://developers.google.com/maps/documentation/routes/reference/rest/v2/TopLevel/computeRoutes
- Routes API Route Matrix: https://developers.google.com/maps/documentation/routes/compute_route_matrix
- Routes API Usage and Billing: https://developers.google.com/maps/documentation/routes/usage-and-billing
- Roads API overview: https://developers.google.com/maps/documentation/roads/overview
- Roads API Snap to Roads: https://developers.google.com/maps/documentation/roads/snap
- Roads API Nearest Roads: https://developers.google.com/maps/documentation/roads/nearest
- Roads API Speed Limits: https://developers.google.com/maps/documentation/roads/speed-limits
- Geocoding API overview: https://developers.google.com/maps/documentation/geocoding/overview
- Geocoding API requests: https://developers.google.com/maps/documentation/geocoding/requests-geocoding
- Geocoding API Usage and Billing: https://developers.google.com/maps/documentation/geocoding/usage-and-billing
- Elevation API overview: https://developers.google.com/maps/documentation/elevation/overview
- Elevation API requests: https://developers.google.com/maps/documentation/elevation/requests-elevation
- Time Zone API overview: https://developers.google.com/maps/documentation/timezone/overview
- Time Zone API requests: https://developers.google.com/maps/documentation/timezone/requests-timezone
- Air Quality API overview: https://developers.google.com/maps/documentation/air-quality/overview
- Air Quality API current conditions reference: https://developers.google.com/maps/documentation/air-quality/reference/rest/v1/currentConditions/lookup
- Air Quality API history reference: https://developers.google.com/maps/documentation/air-quality/reference/rest/v1/history/lookup
- Air Quality API Usage and Billing: https://developers.google.com/maps/documentation/air-quality/usage-and-billing
- Address Validation API overview: https://developers.google.com/maps/documentation/address-validation/overview
- Pollen API overview: https://developers.google.com/maps/documentation/pollen/overview
- Pollen API Usage and Billing: https://developers.google.com/maps/documentation/pollen/usage-and-billing
- Google Maps Platform security best practices: https://developers.google.com/maps/api-security-best-practices
- Google Cloud API credentials: https://support.google.com/googleapi/answer/6158857
- Google Cloud capping API usage: https://docs.cloud.google.com/apis/docs/capping-api-usage

### Existing Connector Implementations (No Matches Found)

No existing community connectors (Airbyte, Fivetran, dlt, Singer) were found for Google Maps Platform APIs as of 2026-02-19. This connector would be a novel implementation.

### Confidence Assessment

All information in this document is sourced from **official Google Maps Platform documentation** accessed on 2026-02-19. The documentation is authoritative and implementation-ready.

**Key uncertainties**:
- Exact QPM limits for Places API (New) are described as "per-method per-project" but specific numbers are not publicly documented. They are configurable in the Cloud Console.
- Roads API QPM limits are not explicitly documented.
- The Places API (New) field mask billing tiers may evolve. Refer to the usage and billing page for current pricing.
- Air Quality API OAuth scope requirement (`cloud-platform`) differs from Places API (which uses API keys). This may affect authentication design.

**Rationale for design choices**:
- **API Key authentication chosen over OAuth**: API keys are the standard and simplest method for Google Maps Platform web services. OAuth with service accounts is also supported but adds complexity. The connector should support API key as the primary method.
- **Snapshot ingestion for most objects**: Google Maps APIs are query-based with no change tracking. Snapshot is the only viable pattern.
- **Static object list**: The API surface is well-defined. A static list with `table_options` for parameterization is the correct approach.
- **No delete tracking**: Google Maps APIs have no delete mechanism. `read_table_deletes()` should not be implemented.
